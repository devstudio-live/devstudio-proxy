package proxycore

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

// KafkaConnection holds the connection parameters for a Kafka cluster.
type KafkaConnection struct {
	Brokers           []string `json:"brokers"`
	SASL              string   `json:"sasl,omitempty"`
	User              string   `json:"user,omitempty"`
	Password          string   `json:"password,omitempty"`
	TLS               bool     `json:"tls,omitempty"`
	TLSSkipVerify     bool     `json:"tlsSkipVerify,omitempty"`
	SchemaRegistryURL string   `json:"schemaRegistryUrl,omitempty"`
	ConnectURL        string   `json:"connectUrl,omitempty"`
}

// KafkaRequest is the JSON body for all Kafka gateway endpoints.
type KafkaRequest struct {
	Connection     KafkaConnection   `json:"connection"`
	ConnectionMode string            `json:"connectionMode,omitempty"`
	SSHConnection  *SSHConnection    `json:"sshConnection,omitempty"`
	Kubeconfig     string            `json:"kubeconfig,omitempty"`
	Context        string            `json:"context,omitempty"`
	Topic          string            `json:"topic,omitempty"`
	Group          string            `json:"group,omitempty"`
	Partition      int               `json:"partition"`
	Offset         int64             `json:"offset"`
	Timestamp      int64             `json:"timestamp,omitempty"`
	Limit          int               `json:"limit,omitempty"`
	Search         string            `json:"search,omitempty"`
	Query          string            `json:"query,omitempty"`
	Config         map[string]string `json:"config,omitempty"`
	Partitions     int               `json:"partitions,omitempty"`
	Replicas       int               `json:"replicas,omitempty"`
	Key            string            `json:"key,omitempty"`
	Value          string            `json:"value,omitempty"`
	Headers        map[string]string `json:"headers,omitempty"`
	Deserializer   string            `json:"deserializer,omitempty"`
	Format         string            `json:"format,omitempty"`
	Connector      string            `json:"connector,omitempty"`
	ConnectorConfig map[string]any   `json:"connectorConfig,omitempty"`
	TaskID         int               `json:"taskId,omitempty"`
	ResourceType   string            `json:"resourceType,omitempty"`
	ResourceName   string            `json:"resourceName,omitempty"`
	Principal      string            `json:"principal,omitempty"`
	Host           string            `json:"host,omitempty"`
	Operation      string            `json:"operation,omitempty"`
	Permission     string            `json:"permission,omitempty"`
	TraceKey       string            `json:"traceKey,omitempty"`
	TraceValue     string            `json:"traceValue,omitempty"`
	TraceTopics    []string          `json:"traceTopics,omitempty"`
	DiffTopic2     string            `json:"diffTopic2,omitempty"`
	DiffPartition2 int               `json:"diffPartition2,omitempty"`
	DiffOffset2    int64             `json:"diffOffset2,omitempty"`
	ResetStrategy  string            `json:"resetStrategy,omitempty"`
	Subject        string            `json:"subject,omitempty"`
	SchemaVersion  int               `json:"schemaVersion,omitempty"`
	SchemaContent  string            `json:"schemaContent,omitempty"`
	SchemaType     string            `json:"schemaType,omitempty"`
	Compatibility  string            `json:"compatibility,omitempty"`
	Namespace      string            `json:"namespace,omitempty"`
	ServiceName    string            `json:"serviceName,omitempty"`
	ServicePort    int               `json:"servicePort,omitempty"`
}

// KafkaResponse is the standard JSON response for Kafka gateway endpoints.
type KafkaResponse struct {
	Error      string      `json:"error,omitempty"`
	DurationMs float64     `json:"durationMs"`
	Data       interface{} `json:"data,omitempty"`
}

// kafkaPoolEntry holds a pooled Kafka transport and dialer.
type kafkaPoolEntry struct {
	transport *kafka.Transport
	dialer    *kafka.Dialer
	brokers   []string
	lastUsed  time.Time
}

// buildKafkaSASL returns a SASL mechanism for the given connection, or nil if no auth.
func buildKafkaSASL(conn KafkaConnection) (sasl.Mechanism, error) {
	switch conn.SASL {
	case "", "none":
		return nil, nil
	case "plain":
		return &plain.Mechanism{
			Username: conn.User,
			Password: conn.Password,
		}, nil
	case "scram-sha-256":
		m, err := scram.Mechanism(scram.SHA256, conn.User, conn.Password)
		if err != nil {
			return nil, fmt.Errorf("SCRAM-SHA-256 setup: %w", err)
		}
		return m, nil
	case "scram-sha-512":
		m, err := scram.Mechanism(scram.SHA512, conn.User, conn.Password)
		if err != nil {
			return nil, fmt.Errorf("SCRAM-SHA-512 setup: %w", err)
		}
		return m, nil
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s", conn.SASL)
	}
}

// buildKafkaTLSConfig returns a *tls.Config if TLS is enabled, or nil.
func buildKafkaTLSConfig(conn KafkaConnection) *tls.Config {
	if !conn.TLS {
		return nil
	}
	return &tls.Config{InsecureSkipVerify: conn.TLSSkipVerify} //nolint:gosec
}

// kafkaConnectionKey returns a SHA-256 hash of the connection config for pool keying.
func kafkaConnectionKey(conn KafkaConnection) string {
	b, _ := json.Marshal(conn)
	sum := sha256.Sum256(b)
	return "kafka:" + hex.EncodeToString(sum[:])
}

// getPooledKafkaClient returns a pooled kafkaPoolEntry, creating one if needed.
func (s *Server) getPooledKafkaClient(conn KafkaConnection) (*kafkaPoolEntry, error) {
	key := kafkaConnectionKey(conn)

	if v, ok := s.kafkaPool.Load(key); ok {
		entry := v.(*kafkaPoolEntry)
		entry.lastUsed = time.Now()
		return entry, nil
	}

	s.kafkaPoolMu.Lock()
	defer s.kafkaPoolMu.Unlock()

	// Double-check after acquiring the lock.
	if v, ok := s.kafkaPool.Load(key); ok {
		entry := v.(*kafkaPoolEntry)
		entry.lastUsed = time.Now()
		return entry, nil
	}

	if s.kafkaPoolSize >= maxPoolSize {
		return nil, fmt.Errorf("Kafka connection pool full (%d/%d)", s.kafkaPoolSize, maxPoolSize)
	}

	mechanism, err := buildKafkaSASL(conn)
	if err != nil {
		return nil, err
	}

	tlsCfg := buildKafkaTLSConfig(conn)

	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		SASLMechanism: mechanism,
	}
	if tlsCfg != nil {
		dialer.TLS = tlsCfg
	}

	transport := &kafka.Transport{
		DialTimeout: 10 * time.Second,
		IdleTimeout: idleExpiry,
		SASL:        mechanism,
	}
	if tlsCfg != nil {
		transport.TLS = tlsCfg
	}

	entry := &kafkaPoolEntry{
		transport: transport,
		dialer:    dialer,
		brokers:   conn.Brokers,
		lastUsed:  time.Now(),
	}
	s.kafkaPool.Store(key, entry)
	s.kafkaPoolSize++

	return entry, nil
}

// kafkaClient returns a configured kafka.Client from a pool entry.
func (e *kafkaPoolEntry) kafkaClient() *kafka.Client {
	return &kafka.Client{
		Addr:      kafka.TCP(e.brokers...),
		Transport: e.transport,
	}
}

// kafkaDial dials the first available broker to verify connectivity.
func (e *kafkaPoolEntry) kafkaDial() (net.Conn, error) {
	for _, broker := range e.brokers {
		conn, err := e.dialer.Dial("tcp", broker)
		if err == nil {
			return conn, nil
		}
	}
	return nil, fmt.Errorf("could not connect to any broker: %v", e.brokers)
}

// startKafkaPoolReaper starts a background goroutine that closes idle Kafka clients.
func (s *Server) startKafkaPoolReaper() {
	ticker := time.NewTicker(reaperInterval)
	for range ticker.C {
		s.reapIdleKafkaConnections()
	}
}

func (s *Server) reapIdleKafkaConnections() {
	now := time.Now()
	var toEvict []string

	s.kafkaPool.Range(func(k, v any) bool {
		entry := v.(*kafkaPoolEntry)
		if now.Sub(entry.lastUsed) > idleExpiry {
			toEvict = append(toEvict, k.(string))
		}
		return true
	})

	if len(toEvict) == 0 {
		return
	}

	s.kafkaPoolMu.Lock()
	defer s.kafkaPoolMu.Unlock()

	for _, key := range toEvict {
		if v, ok := s.kafkaPool.Load(key); ok {
			entry := v.(*kafkaPoolEntry)
			if now.Sub(entry.lastUsed) > idleExpiry {
				entry.transport.CloseIdleConnections()
				s.kafkaPool.Delete(key)
				s.kafkaPoolSize--
			}
		}
	}
}
