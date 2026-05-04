package proxycore

import (
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Options configures how the Server is constructed.
type Options struct {
	Port        int
	MCPRefresh  time.Duration
	MCPFallback bool
}

// Server encapsulates all proxy state that was previously held in package-level
// globals. A single Server instance is shared by every handler.
type Server struct {
	// Handler is the core HTTP handler (proxy router).
	Handler http.Handler

	// LogBuf is the ring buffer for log lines with SSE subscriptions.
	LogBuf *LogRing

	// EventBuf is the ring buffer for proxy events with SSE subscriptions.
	EventBuf *EventRing

	// TrafficBuf is the ring buffer for request/response traffic records.
	TrafficBuf *TrafficRing

	// VerboseRevertTimer cancels an in-progress verbose TTL.
	VerboseRevertTimer *time.Timer
	verboseRevertMu    sync.Mutex

	// LogEnabled / VerboseEnabled are toggled at runtime by /admin/config.
	LogEnabled     atomic.Bool
	VerboseEnabled atomic.Bool

	// AdminPort is the listening port (readable by admin endpoints).
	AdminPort int

	// AdminServer is the current *http.Server reference for restart support.
	AdminServer *http.Server

	// TLSAvailable indicates whether TLS is active for the current listen cycle.
	TLSAvailable bool

	// TLSCAPath is the path to the CA certificate file.
	TLSCAPath string

	// ServerStartTime is used for uptime tracking in health events.
	ServerStartTime time.Time

	// MCPFallbackEnabled controls remote MCP forward fallback.
	MCPFallbackEnabled bool

	// mcpRuntime holds the Goja VM state (protected by its own mutex).
	mcpRuntime struct {
		mu     sync.Mutex
		vm     interface{} // *goja.Runtime — kept as interface to avoid import in this file
		script string
		ready  bool
	}

	// Connection pools (all use sync.Map internally)
	sqlPool     sync.Map
	sqlPoolMu   sync.Mutex
	sqlPoolSize int

	mongoPool     sync.Map
	mongoPoolMu   sync.Mutex
	mongoPoolSize int

	redisPool     sync.Map
	redisPoolMu   sync.Mutex
	redisPoolSize int

	elasticPool     sync.Map
	elasticPoolMu   sync.Mutex
	elasticPoolSize int

	// Context cache
	contextCache     sync.Map
	contextTotalSize atomic.Int64
	contextCount     atomic.Int32

	// Kubernetes client pool
	k8sPool     sync.Map
	k8sPoolMu   sync.Mutex
	k8sPoolSize int

	// Kubernetes port-forward sessions (Phase 2)
	k8sPortForwards sync.Map

	// Kubernetes SSH port-forward sessions
	k8sSSHPortForwards sync.Map

	// HPROF parse jobs
	hprofJobs sync.Map

	// HPROF session cache (Phase 3)
	hprofSessions *HprofSessionStore

	// SSH connection pool
	sshPool     sync.Map
	sshPoolMu   sync.Mutex
	sshPoolSize int

	// SSH terminal sessions
	sshSessions sync.Map

	// SSH tunnels (Phase 3)
	sshTunnels sync.Map

	// Kafka connection pool
	kafkaPool     sync.Map
	kafkaPoolMu   sync.Mutex
	kafkaPoolSize int

	// Kafka connection-mode pool: SSH tunnels + K8s port-forwards that sit
	// between the Kafka pool and the actual brokers.
	kafkaModePool   sync.Map
	kafkaModePoolMu sync.Mutex

	// DAG executions registry (Phase 16A). Keyed by execution_id
	// (UUID string) → *dagExecution. The reaper goroutine is started
	// lazily on first registration via dagReaperOnce.
	dagExecutions sync.Map
	dagReaperOnce sync.Once

	// DAG large-result dedup (Phase 16D). Content hash (hex) → UUID
	// already stored in the /mcp/context cache. Keeps repeat node
	// results from multiplying cache pressure. Entries self-heal: a
	// stale pointer (cache entry expired via TTL) is dropped on next
	// lookup and a fresh store proceeds.
	dagResultDedup sync.Map
}

// NewServer creates a Server with default ring buffers and wires the handler.
func NewServer(opts Options) *Server {
	s := &Server{
		AdminPort:          opts.Port,
		MCPFallbackEnabled: opts.MCPFallback,
		ServerStartTime:    time.Now(),
		LogBuf:             NewLogRing(200),
		EventBuf:           NewEventRing(100),
		TrafficBuf:         NewTrafficRing(500),
		hprofSessions:      NewHprofSessionStore(),
	}

	// The Handler wraps the proxy router with logging middleware.
	s.Handler = s.NewLoggingMiddleware(&handler{s: s})
	return s
}

// Start launches background goroutines (reapers, health ticker, MCP runtime).
func (s *Server) Start(mcpRefresh time.Duration) {
	s.initLogOutput()
	s.initMCPRuntime(mcpRefresh)

	go s.startPoolReaper()
	go s.startMongoPoolReaper()
	go s.startElasticPoolReaper()
	go s.startRedisPoolReaper()
	go s.startK8sPoolReaper()
	go s.startContextReaper()
	go s.startHprofJobReaper()
	go s.startSessionReaper()
	go s.startSSHPoolReaper()
	go s.startSSHSessionReaper()
	go s.startSSHTunnelReaper()
	go s.startKafkaPoolReaper()
	go s.startKafkaModePoolReaper()
	go s.startHealthTicker()
}

// Shutdown performs a graceful shutdown of the server.
func (s *Server) Shutdown() {
	if s.AdminServer != nil {
		s.AdminServer.Close()
	}
}
