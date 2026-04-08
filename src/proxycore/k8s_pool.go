package proxycore

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	maxK8sPoolSize    = 10
	k8sIdleExpiry     = 10 * time.Minute
	k8sReaperInterval = 30 * time.Second
)

type k8sPoolEntry struct {
	clientset *kubernetes.Clientset
	config    *rest.Config
	lastUsed  time.Time
}

// k8sConnectionKey returns a deterministic hash for kubeconfig path + context +
// file mtime/size. Including the file's modification time means that when the
// kubeconfig is regenerated (e.g. after k3s restarts with new TLS certs), the
// old pool entry is not reused — a fresh client with the new CA is created
// automatically instead of failing with "certificate signed by unknown authority".
func k8sConnectionKey(kubeconfig, context string) string {
	raw := kubeconfig + "\x00" + context
	if fi, err := os.Stat(kubeconfig); err == nil {
		raw += "\x00" + fi.ModTime().UTC().String() + "\x00" + fmt.Sprint(fi.Size())
	}
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// resolveKubeconfig resolves an empty kubeconfig path to ~/.kube/config.
func resolveKubeconfig(path string) string {
	if path != "" {
		return path
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".kube", "config")
}

// getPooledK8sClient returns a cached or newly created Kubernetes clientset.
func (s *Server) getPooledK8sClient(kubeconfig, context string) (*kubernetes.Clientset, *rest.Config, error) {
	kubeconfig = resolveKubeconfig(kubeconfig)
	if kubeconfig == "" {
		return nil, nil, fmt.Errorf("could not resolve kubeconfig path")
	}

	key := k8sConnectionKey(kubeconfig, context)

	// Fast path
	if v, ok := s.k8sPool.Load(key); ok {
		entry := v.(*k8sPoolEntry)
		entry.lastUsed = time.Now()
		return entry.clientset, entry.config, nil
	}

	s.k8sPoolMu.Lock()
	defer s.k8sPoolMu.Unlock()

	// Double-check after lock
	if v, ok := s.k8sPool.Load(key); ok {
		entry := v.(*k8sPoolEntry)
		entry.lastUsed = time.Now()
		return entry.clientset, entry.config, nil
	}

	if s.k8sPoolSize >= maxK8sPoolSize {
		return nil, nil, fmt.Errorf("k8s connection pool full (%d/%d): close unused connections first", s.k8sPoolSize, maxK8sPoolSize)
	}

	// Build config from kubeconfig with optional context override
	loadingRules := &clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfig}
	overrides := &clientcmd.ConfigOverrides{}
	if context != "" {
		overrides.CurrentContext = context
	}

	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides).ClientConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("load kubeconfig: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("create k8s client: %w", err)
	}

	entry := &k8sPoolEntry{clientset: clientset, config: cfg, lastUsed: time.Now()}
	s.k8sPool.Store(key, entry)
	s.k8sPoolSize++

	return clientset, cfg, nil
}

func (s *Server) startK8sPoolReaper() {
	ticker := time.NewTicker(k8sReaperInterval)
	for range ticker.C {
		s.reapIdleK8sConnections()
	}
}

func (s *Server) reapIdleK8sConnections() {
	now := time.Now()
	var toEvict []string

	s.k8sPool.Range(func(k, v any) bool {
		entry := v.(*k8sPoolEntry)
		if now.Sub(entry.lastUsed) > k8sIdleExpiry {
			toEvict = append(toEvict, k.(string))
		}
		return true
	})

	if len(toEvict) == 0 {
		return
	}

	s.k8sPoolMu.Lock()
	defer s.k8sPoolMu.Unlock()

	for _, key := range toEvict {
		if v, ok := s.k8sPool.Load(key); ok {
			entry := v.(*k8sPoolEntry)
			if now.Sub(entry.lastUsed) > k8sIdleExpiry {
				// kubernetes.Clientset has no Close — just evict from pool
				s.k8sPool.Delete(key)
				s.k8sPoolSize--
			}
		}
	}
}
