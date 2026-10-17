package models

import "time"

// ── Sandbox ──────────────────────────────────────────────────────────────────

type IsolationTier string

const (
	IsolationStandard IsolationTier = "standard" // Docker + seccomp
	IsolationSecure   IsolationTier = "secure"   // gVisor (runsc)
)

type SandboxStatus string

const (
	StatusProvisioning SandboxStatus = "provisioning"
	StatusRunning      SandboxStatus = "running"
	StatusPaused       SandboxStatus = "paused"
	StatusResuming     SandboxStatus = "resuming"
	StatusDestroyed    SandboxStatus = "destroyed"
	StatusFailed       SandboxStatus = "failed"
)

type Sandbox struct {
	ID          string        `json:"id"`
	TenantID    string        `json:"tenant_id"`
	Status      SandboxStatus `json:"status"`
	PodName     string        `json:"pod_name,omitempty"`
	Template    string        `json:"template"`
	Isolation   IsolationTier `json:"isolation"`
	Memory      string        `json:"memory"`
	CPU         string        `json:"cpu"`
	Persistent  bool          `json:"persistent"`
	DiskGB      int           `json:"disk_gb,omitempty"`
	Secrets     []string      `json:"secrets,omitempty"`
	Region      string        `json:"region,omitempty"`
	MCPEnabled  bool          `json:"mcp_enabled"`
	MCPUrl      string        `json:"mcp_url,omitempty"`
	PreviewURLs []string      `json:"preview_urls,omitempty"`
	TTLExpires  *time.Time    `json:"ttl_expires,omitempty"`
	CreatedAt   time.Time     `json:"created_at"`
	UpdatedAt   time.Time     `json:"updated_at"`
}

type CreateSandboxRequest struct {
	Template   string            `json:"template" binding:"required"`
	Memory     string            `json:"memory"`
	CPU        string            `json:"cpu"`
	TTL        string            `json:"ttl"`
	Isolation  IsolationTier     `json:"isolation"`
	Env        map[string]string `json:"env"`
	Secrets    []string          `json:"secrets"`
	Persistent bool              `json:"persistent"`
	DiskGB     int               `json:"disk_gb"`
	Region     string            `json:"region"`
	Network    *NetworkConfig    `json:"network"`
	MCP        *MCPConfig        `json:"mcp"`
}

type NetworkConfig struct {
	Expose        []PortExpose `json:"expose"`
	BlockOutbound bool         `json:"block_outbound"`
	AllowDomains  []string     `json:"allowed_domains"`
}

type PortExpose struct {
	Port int    `json:"port"`
	Name string `json:"name"`
}

type MCPConfig struct {
	Enabled bool `json:"enabled"`
}

type ExecRequest struct {
	Cmd     string            `json:"cmd" binding:"required"`
	Env     map[string]string `json:"env"`
	Timeout int               `json:"timeout_seconds"`
}

type ExecResult struct {
	Stdout     string `json:"stdout"`
	Stderr     string `json:"stderr"`
	ExitCode   int    `json:"exit_code"`
	DurationMs int64  `json:"duration_ms"`
}

type ExecFrame struct {
	Type string `json:"type"` // stdout | stderr | exit
	Data string `json:"data"`
	Code int    `json:"code,omitempty"`
}

// ── REPL ─────────────────────────────────────────────────────────────────────

type REPLKernel string

const (
	KernelPython REPLKernel = "python"
	KernelNode   REPLKernel = "node"
	KernelBash   REPLKernel = "bash"
	KernelRuby   REPLKernel = "ruby"
)

type REPLSession struct {
	ID        string     `json:"id"`
	SandboxID string     `json:"sandbox_id"`
	Kernel    REPLKernel `json:"kernel"`
	CreatedAt time.Time  `json:"created_at"`
}

type REPLSendRequest struct {
	Code string `json:"code" binding:"required"`
}

type REPLResult struct {
	Stdout string `json:"stdout"`
	Stderr string `json:"stderr"`
}

// ── PTY ──────────────────────────────────────────────────────────────────────

type PTYFrame struct {
	Type string `json:"type"` // input | resize | output | exit
	Data string `json:"data,omitempty"`
	Cols uint16 `json:"cols,omitempty"`
	Rows uint16 `json:"rows,omitempty"`
	Code int    `json:"code,omitempty"`
}

// ── Snapshot ─────────────────────────────────────────────────────────────────

type Snapshot struct {
	ID        string    `json:"id"`
	SandboxID string    `json:"sandbox_id"`
	Name      string    `json:"name"`
	SizeBytes int64     `json:"size_bytes"`
	CreatedAt time.Time `json:"created_at"`
}

// ── Checkpoint ───────────────────────────────────────────────────────────────

type Checkpoint struct {
	ID         string    `json:"id"`
	SandboxID  string    `json:"sandbox_id"`
	SnapshotID string    `json:"snapshot_id"`
	Name       string    `json:"name"`
	CreatedAt  time.Time `json:"created_at"`
}

// ── Webhook ──────────────────────────────────────────────────────────────────

type WebhookEndpoint struct {
	ID        string    `json:"id"`
	TenantID  string    `json:"tenant_id"`
	URL       string    `json:"url"`
	Events    []string  `json:"events"`
	Secret    string    `json:"secret"`
	CreatedAt time.Time `json:"created_at"`
}

type WebhookEvent struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Payload   interface{} `json:"payload"`
	CreatedAt time.Time   `json:"created_at"`
}

// ── Audit ────────────────────────────────────────────────────────────────────

type AuditEntry struct {
	ID          string                 `json:"id"`
	Action      string                 `json:"action"`
	ActorType   string                 `json:"actor_type"` // api_key | user
	ActorID     string                 `json:"actor_id"`
	SandboxID   string                 `json:"sandbox_id,omitempty"`
	Meta        map[string]interface{} `json:"meta,omitempty"`
	SecretsUsed []string               `json:"secrets_used,omitempty"`
	CreatedAt   time.Time              `json:"created_at"`
}

// ── Quota ────────────────────────────────────────────────────────────────────

type QuotaUsage struct {
	TenantID            string  `json:"tenant_id"`
	ConcurrentSandboxes int     `json:"concurrent_sandboxes"`
	CPUHoursThisMonth   float64 `json:"cpu_hours_this_month"`
	StorageGB           float64 `json:"storage_gb"`
}

type QuotaLimits struct {
	TenantID            string  `json:"tenant_id"`
	ConcurrentSandboxes int     `json:"concurrent_sandboxes"`
	CPUHoursThisMonth   float64 `json:"cpu_hours_this_month"`
	StorageGB           float64 `json:"storage_gb"`
}

// ── Task ─────────────────────────────────────────────────────────────────────

type TaskDefinition struct {
	ID               string        `json:"id"`
	Name             string        `json:"name"`
	Version          int           `json:"version"`
	Template         string        `json:"template"`
	Memory           string        `json:"memory"`
	CPU              string        `json:"cpu"`
	Isolation        IsolationTier `json:"isolation"`
	Handler          []byte        `json:"-"`
	RetryMax         int           `json:"retry_max"`
	RetryBackoffMs   int           `json:"retry_backoff_ms"`
	ConcurrencyLimit int           `json:"concurrency_limit"`
	ConcurrencyKey   string        `json:"concurrency_key,omitempty"`
	QueueName        string        `json:"queue_name"`
	CreatedAt        time.Time     `json:"created_at"`
}

type TaskRunStatus string

const (
	TaskRunQueued       TaskRunStatus = "queued"
	TaskRunStarting     TaskRunStatus = "starting"
	TaskRunRunning      TaskRunStatus = "running"
	TaskRunCompleted    TaskRunStatus = "completed"
	TaskRunFailed       TaskRunStatus = "failed"
	TaskRunCancelled    TaskRunStatus = "cancelled"
	TaskRunTimedOut     TaskRunStatus = "timed_out"
	TaskRunRetrying     TaskRunStatus = "retrying"
	TaskRunWaitDuration TaskRunStatus = "waiting_for_duration"
	TaskRunWaitToken    TaskRunStatus = "waiting_for_token"
)

type TaskRun struct {
	ID             string        `json:"id"`
	TaskID         string        `json:"task_id"`
	TaskVersion    int           `json:"task_version"`
	Status         TaskRunStatus `json:"status"`
	Payload        []byte        `json:"payload"`
	IdempotencyKey string        `json:"idempotency_key,omitempty"`
	Priority       int           `json:"priority"`
	Attempt        int           `json:"attempt"`
	MaxAttempts    int           `json:"max_attempts"`
	DelayUntil     time.Time     `json:"delay_until"`
	ResumeAt       *time.Time    `json:"resume_at,omitempty"`
	CheckpointID   string        `json:"checkpoint_id,omitempty"`
	WaitTokenID    string        `json:"wait_token_id,omitempty"`
	SandboxID      string        `json:"sandbox_id,omitempty"`
	Output         []byte        `json:"output,omitempty"`
	ErrorMsg       string        `json:"error_msg,omitempty"`
	Tags           []string      `json:"tags,omitempty"`
	StartedAt      *time.Time    `json:"started_at,omitempty"`
	CompletedAt    *time.Time    `json:"completed_at,omitempty"`
	CreatedAt      time.Time     `json:"created_at"`
}

type WaitToken struct {
	ID          string     `json:"id"`
	RunID       string     `json:"run_id"`
	Description string     `json:"description"`
	ExpiresAt   time.Time  `json:"expires_at"`
	ResolvedAt  *time.Time `json:"resolved_at,omitempty"`
	Data        []byte     `json:"data,omitempty"`
}

type TaskSchedule struct {
	ID        string    `json:"id"`
	TaskName  string    `json:"task_name"`
	Cron      string    `json:"cron"`
	Timezone  string    `json:"timezone"`
	Payload   []byte    `json:"payload"`
	Enabled   bool      `json:"enabled"`
	NextRunAt time.Time `json:"next_run_at"`
	CreatedAt time.Time `json:"created_at"`
}

type Queue struct {
	Name             string `json:"name"`
	Depth            int    `json:"depth"`
	ActiveRuns       int    `json:"active_runs"`
	ConcurrencyLimit int    `json:"concurrency_limit"`
	Paused           bool   `json:"paused"`
}
