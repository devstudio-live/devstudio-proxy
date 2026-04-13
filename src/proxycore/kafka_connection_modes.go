package proxycore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

// kafkaModeEntry holds the resolved broker addresses and the underlying
// tunnel/port-forward handles that keep those addresses alive.
type kafkaModeEntry struct {
	rewrittenBrokers []string
	sshListeners     []net.Listener // ssh mode — one local-forward per broker
	k8sStopCh        chan struct{}  // k8s-portforward mode — stop channel for pf session
	lastUsed         time.Time
}

// kafkaModeKey derives a stable key for a mode-pool entry.
// For the same (mode + auth params + original broker list), we reuse tunnels.
func kafkaModeKey(req KafkaRequest) string {
	var raw string
	switch req.ConnectionMode {
	case "ssh":
		raw = "ssh|"
		if req.SSHConnection != nil {
			raw += sshConnectionKey(*req.SSHConnection)
		}
		raw += "|" + strings.Join(req.Connection.Brokers, ",")
	case "k8s-portforward":
		raw = fmt.Sprintf("k8spf|%s|%s|%s|%s|%d",
			req.Kubeconfig, req.Context, req.Namespace, req.ServiceName, req.ServicePort)
	default:
		raw = req.ConnectionMode
	}
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// getPooledKafkaClientForReq returns a pooled Kafka client for req, applying
// SSH-tunnel or K8s-port-forward rewriting of broker addresses when requested.
// For direct mode (default) it delegates to getPooledKafkaClient.
func (s *Server) getPooledKafkaClientForReq(req KafkaRequest) (*kafkaPoolEntry, error) {
	switch req.ConnectionMode {
	case "", "direct":
		return s.getPooledKafkaClient(req.Connection)
	case "ssh":
		return s.getKafkaClientViaSSH(req)
	case "k8s-portforward":
		return s.getKafkaClientViaK8sPortForward(req)
	default:
		return nil, fmt.Errorf("unsupported connectionMode: %q (use direct, ssh, or k8s-portforward)", req.ConnectionMode)
	}
}

// getKafkaClientViaSSH opens a local SSH port-forward for each broker, then
// hands off to the direct pool using 127.0.0.1:<localPort> broker addresses.
func (s *Server) getKafkaClientViaSSH(req KafkaRequest) (*kafkaPoolEntry, error) {
	if req.SSHConnection == nil {
		return nil, fmt.Errorf("sshConnection is required for ssh connectionMode")
	}
	if len(req.Connection.Brokers) == 0 {
		return nil, fmt.Errorf("at least one broker is required")
	}

	modeEntry, err := s.ensureKafkaModeEntry(req, func() (*kafkaModeEntry, error) {
		sshClient, err := s.getPooledSSHClient(*req.SSHConnection)
		if err != nil {
			return nil, fmt.Errorf("ssh: %w", err)
		}

		stopCh := make(chan struct{})
		listeners := make([]net.Listener, 0, len(req.Connection.Brokers))
		rewritten := make([]string, 0, len(req.Connection.Brokers))

		for _, broker := range req.Connection.Brokers {
			ln, err := startLocalForward(sshClient, "127.0.0.1:0", broker, stopCh)
			if err != nil {
				close(stopCh)
				for _, l := range listeners {
					l.Close() //nolint:errcheck
				}
				return nil, fmt.Errorf("ssh tunnel for %s: %w", broker, err)
			}
			listeners = append(listeners, ln)
			addr := ln.Addr().(*net.TCPAddr)
			rewritten = append(rewritten, fmt.Sprintf("127.0.0.1:%d", addr.Port))
		}

		return &kafkaModeEntry{
			rewrittenBrokers: rewritten,
			sshListeners:     listeners,
			lastUsed:         time.Now(),
		}, nil
	})
	if err != nil {
		return nil, err
	}

	rewrittenConn := req.Connection
	rewrittenConn.Brokers = modeEntry.rewrittenBrokers
	return s.getPooledKafkaClient(rewrittenConn)
}

// getKafkaClientViaK8sPortForward starts a kubectl-style port-forward to a pod
// backing the target service, then hands off to the direct pool using the
// 127.0.0.1:<localPort> address.
func (s *Server) getKafkaClientViaK8sPortForward(req KafkaRequest) (*kafkaPoolEntry, error) {
	if req.ServiceName == "" || req.ServicePort == 0 {
		return nil, fmt.Errorf("serviceName and servicePort are required for k8s-portforward connectionMode")
	}

	ns := req.Namespace
	if ns == "" {
		ns = "default"
	}

	modeEntry, err := s.ensureKafkaModeEntry(req, func() (*kafkaModeEntry, error) {
		cs, cfg, err := s.getPooledK8sClient(req.Kubeconfig, req.Context)
		if err != nil {
			return nil, fmt.Errorf("k8s: %w", err)
		}

		pod, err := pickPodForService(cs, ns, req.ServiceName)
		if err != nil {
			return nil, err
		}

		localPort, err := findFreePort()
		if err != nil {
			return nil, fmt.Errorf("find free port: %w", err)
		}

		transport, upgrader, err := spdy.RoundTripperFor(cfg)
		if err != nil {
			return nil, fmt.Errorf("spdy transport: %w", err)
		}
		pfURL := buildPortForwardURL(cfg, ns, pod)
		dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, "POST", pfURL)

		stopCh := make(chan struct{})
		readyCh := make(chan struct{})
		ports := []string{fmt.Sprintf("%d:%d", localPort, req.ServicePort)}

		pf, err := portforward.New(dialer, ports, stopCh, readyCh, discardWriter{}, discardWriter{})
		if err != nil {
			return nil, fmt.Errorf("portforward setup: %w", err)
		}

		errCh := make(chan error, 1)
		go func() { errCh <- pf.ForwardPorts() }()

		select {
		case <-readyCh:
			// ready
		case err := <-errCh:
			return nil, fmt.Errorf("portforward failed: %w", err)
		case <-time.After(10 * time.Second):
			close(stopCh)
			return nil, fmt.Errorf("portforward timed out waiting for ready")
		}

		if forwarded, err := pf.GetPorts(); err == nil && len(forwarded) > 0 {
			localPort = int(forwarded[0].Local)
		}

		return &kafkaModeEntry{
			rewrittenBrokers: []string{fmt.Sprintf("127.0.0.1:%d", localPort)},
			k8sStopCh:        stopCh,
			lastUsed:         time.Now(),
		}, nil
	})
	if err != nil {
		return nil, err
	}

	rewrittenConn := req.Connection
	rewrittenConn.Brokers = modeEntry.rewrittenBrokers
	return s.getPooledKafkaClient(rewrittenConn)
}

// ensureKafkaModeEntry returns a cached mode entry for req or creates one via
// the factory fn, handling the double-checked-locking pattern.
func (s *Server) ensureKafkaModeEntry(req KafkaRequest, factory func() (*kafkaModeEntry, error)) (*kafkaModeEntry, error) {
	key := kafkaModeKey(req)

	if v, ok := s.kafkaModePool.Load(key); ok {
		entry := v.(*kafkaModeEntry)
		entry.lastUsed = time.Now()
		return entry, nil
	}

	s.kafkaModePoolMu.Lock()
	defer s.kafkaModePoolMu.Unlock()

	if v, ok := s.kafkaModePool.Load(key); ok {
		entry := v.(*kafkaModeEntry)
		entry.lastUsed = time.Now()
		return entry, nil
	}

	entry, err := factory()
	if err != nil {
		return nil, err
	}
	s.kafkaModePool.Store(key, entry)
	return entry, nil
}

// pickPodForService returns the name of a ready pod backing the named Service
// (by looking at the Service's Endpoints subsets). Mirrors what kubectl
// port-forward svc/<name> does internally.
func pickPodForService(cs kubernetes.Interface, namespace, serviceName string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ep, err := cs.CoreV1().Endpoints(namespace).Get(ctx, serviceName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get endpoints %s/%s: %w", namespace, serviceName, err)
	}
	for _, subset := range ep.Subsets {
		for _, addr := range subset.Addresses {
			if addr.TargetRef != nil && addr.TargetRef.Kind == "Pod" && addr.TargetRef.Name != "" {
				return addr.TargetRef.Name, nil
			}
		}
	}
	return "", fmt.Errorf("no ready pods for service %s/%s", namespace, serviceName)
}

// startKafkaModePoolReaper tears down idle SSH tunnels / port-forwards.
func (s *Server) startKafkaModePoolReaper() {
	ticker := time.NewTicker(reaperInterval)
	for range ticker.C {
		s.reapIdleKafkaModeEntries()
	}
}

func (s *Server) reapIdleKafkaModeEntries() {
	now := time.Now()
	var toEvict []string
	s.kafkaModePool.Range(func(k, v any) bool {
		entry := v.(*kafkaModeEntry)
		if now.Sub(entry.lastUsed) > idleExpiry {
			toEvict = append(toEvict, k.(string))
		}
		return true
	})
	if len(toEvict) == 0 {
		return
	}

	s.kafkaModePoolMu.Lock()
	defer s.kafkaModePoolMu.Unlock()

	for _, key := range toEvict {
		v, ok := s.kafkaModePool.Load(key)
		if !ok {
			continue
		}
		entry := v.(*kafkaModeEntry)
		if now.Sub(entry.lastUsed) <= idleExpiry {
			continue
		}
		for _, ln := range entry.sshListeners {
			ln.Close() //nolint:errcheck
		}
		if entry.k8sStopCh != nil {
			close(entry.k8sStopCh)
		}
		s.kafkaModePool.Delete(key)
	}
}
