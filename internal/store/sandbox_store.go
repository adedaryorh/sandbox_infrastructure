package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
	"github.com/yourorg/sandboxing/pkg/models"
)

var ErrBadTransition = errors.New("sandbox not in expected state for this transition")
var ErrNotFound = errors.New("not found")

type DB struct {
	db *sql.DB
}

func New(databaseURL string) (*DB, error) {
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)
	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}
	return &DB{db: db}, nil
}

func (s *DB) Raw() *sql.DB { return s.db }

// ── Migrations ────────────────────────────────────────────────────────────────

func (s *DB) Migrate(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "select pg_advisory_xact_lock(527061210)"); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, schema); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "create table if not exists schema_versions(version integer primary key,applied_at timestamptz not null default now())"); err != nil {
		return err
	}
	var applied bool
	if err = tx.QueryRowContext(ctx, "select exists(select 1 from schema_versions where version=2)").Scan(&applied); err != nil {
		return err
	}
	if !applied {
		if _, err = tx.ExecContext(ctx, securitySchema); err != nil {
			return err
		}
	}
	return tx.Commit()
}

const schema = `
CREATE TABLE IF NOT EXISTS sandboxes (
	id           TEXT PRIMARY KEY,
	tenant_id    TEXT NOT NULL,
	status       TEXT NOT NULL DEFAULT 'provisioning',
	pod_name     TEXT,
	template     TEXT NOT NULL,
	isolation    TEXT NOT NULL DEFAULT 'standard',
	memory       TEXT NOT NULL DEFAULT '512mb',
	cpu          TEXT NOT NULL DEFAULT '0.5',
	persistent   BOOLEAN DEFAULT false,
	disk_gb      INT DEFAULT 0,
	secrets      TEXT[] DEFAULT '{}',
	region       TEXT DEFAULT 'default',
	mcp_enabled  BOOLEAN DEFAULT false,
	mcp_url      TEXT,
	ttl_expires  TIMESTAMPTZ,
	created_at   TIMESTAMPTZ DEFAULT NOW(),
	updated_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS warm_pool (
	id          TEXT PRIMARY KEY,
	template    TEXT NOT NULL,
	pod_name    TEXT NOT NULL,
	isolation   TEXT NOT NULL DEFAULT 'standard',
	status      TEXT NOT NULL DEFAULT 'ready',
	created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS snapshots (
	id          TEXT PRIMARY KEY,
	sandbox_id  TEXT NOT NULL,
	name        TEXT NOT NULL,
	pvc_snap    TEXT,
	size_bytes  BIGINT DEFAULT 0,
	created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS checkpoints (
	id          TEXT PRIMARY KEY,
	sandbox_id  TEXT NOT NULL,
	snapshot_id TEXT,
	name        TEXT NOT NULL,
	created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS repl_sessions (
	id          TEXT PRIMARY KEY,
	sandbox_id  TEXT NOT NULL,
	kernel      TEXT NOT NULL,
	created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS webhook_endpoints (
	id          TEXT PRIMARY KEY,
	tenant_id   TEXT NOT NULL,
	url         TEXT NOT NULL,
	events      TEXT[] DEFAULT '{}',
	secret      TEXT NOT NULL,
	created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit_log (
	id           TEXT PRIMARY KEY,
	action       TEXT NOT NULL,
	actor_type   TEXT NOT NULL DEFAULT 'api_key',
	actor_id     TEXT NOT NULL,
	sandbox_id   TEXT,
	meta         JSONB,
	secrets_used TEXT[] DEFAULT '{}',
	created_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS quota_usage (
	tenant_id              TEXT PRIMARY KEY,
	concurrent_sandboxes   INT DEFAULT 0,
	cpu_hours_this_month   FLOAT DEFAULT 0,
	storage_gb             FLOAT DEFAULT 0,
	updated_at             TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS quota_limits (
	tenant_id              TEXT PRIMARY KEY,
	concurrent_sandboxes   INT DEFAULT 5,
	cpu_hours_this_month   FLOAT DEFAULT 100,
	storage_gb             FLOAT DEFAULT 50
);

CREATE TABLE IF NOT EXISTS task_definitions (
	id                  TEXT PRIMARY KEY,
	name                TEXT NOT NULL,
	version             INT NOT NULL DEFAULT 1,
	template            TEXT NOT NULL,
	memory              TEXT NOT NULL DEFAULT '512mb',
	cpu                 TEXT NOT NULL DEFAULT '0.5',
	isolation           TEXT NOT NULL DEFAULT 'secure',
	handler             BYTEA NOT NULL,
	retry_max           INT DEFAULT 3,
	retry_backoff_ms    INT DEFAULT 1000,
	concurrency_limit   INT DEFAULT 10,
	concurrency_key     TEXT,
	queue_name          TEXT NOT NULL DEFAULT 'default',
	created_at          TIMESTAMPTZ DEFAULT NOW(),
	UNIQUE(name, version)
);

CREATE TABLE IF NOT EXISTS task_runs (
	id               TEXT PRIMARY KEY,
	task_id          TEXT NOT NULL,
	task_version     INT NOT NULL,
	status           TEXT NOT NULL DEFAULT 'queued',
	payload          JSONB,
	idempotency_key  TEXT UNIQUE,
	priority         INT DEFAULT 5,
	attempt          INT DEFAULT 0,
	max_attempts     INT DEFAULT 3,
	delay_until      TIMESTAMPTZ DEFAULT NOW(),
	resume_at        TIMESTAMPTZ,
	checkpoint_id    TEXT,
	wait_token_id    TEXT,
	sandbox_id       TEXT,
	output           JSONB,
	error_msg        TEXT,
	tags             TEXT[] DEFAULT '{}',
	started_at       TIMESTAMPTZ,
	completed_at     TIMESTAMPTZ,
	created_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wait_tokens (
	id           TEXT PRIMARY KEY,
	run_id       TEXT NOT NULL,
	description  TEXT,
	expires_at   TIMESTAMPTZ NOT NULL,
	resolved_at  TIMESTAMPTZ,
	data         JSONB,
	created_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS task_schedules (
	id          TEXT PRIMARY KEY,
	task_name   TEXT NOT NULL,
	cron        TEXT NOT NULL,
	timezone    TEXT DEFAULT 'UTC',
	payload     JSONB,
	enabled     BOOLEAN DEFAULT true,
	next_run_at TIMESTAMPTZ,
	created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS queues (
	name              TEXT PRIMARY KEY,
	concurrency_limit INT DEFAULT 10,
	paused            BOOLEAN DEFAULT false,
	created_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sandboxes_tenant ON sandboxes(tenant_id);
CREATE INDEX IF NOT EXISTS idx_sandboxes_status ON sandboxes(status);
CREATE INDEX IF NOT EXISTS idx_warm_pool_template_status ON warm_pool(template, status);
CREATE INDEX IF NOT EXISTS idx_task_runs_status ON task_runs(status);
CREATE INDEX IF NOT EXISTS idx_task_runs_delay ON task_runs(delay_until) WHERE status='queued';
CREATE INDEX IF NOT EXISTS idx_audit_sandbox ON audit_log(sandbox_id);

CREATE TABLE IF NOT EXISTS secrets (
	name       TEXT NOT NULL,
	value      TEXT NOT NULL,
	tenant_id  TEXT NOT NULL,
	created_at TIMESTAMPTZ DEFAULT NOW(),
	PRIMARY KEY (name, tenant_id)
);
`

// ── Sandbox CRUD ──────────────────────────────────────────────────────────────

func (s *DB) InsertSandbox(ctx context.Context, sb *models.Sandbox) error {
	secrets := pq.Array(sb.Secrets)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO sandboxes
			(id, tenant_id, status, pod_name, template, isolation,
			 memory, cpu, persistent, disk_gb, secrets, region,
			 mcp_enabled, ttl_expires)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
		sb.ID, sb.TenantID, sb.Status, sb.PodName, sb.Template, sb.Isolation,
		sb.Memory, sb.CPU, sb.Persistent, sb.DiskGB, secrets, sb.Region,
		sb.MCPEnabled, sb.TTLExpires,
	)
	return err
}

func (s *DB) GetSandbox(ctx context.Context, id string) (*models.Sandbox, error) {
	tenant, err := models.Tenant(ctx)
	if err != nil {
		return nil, err
	}
	row := s.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, status, pod_name, template, isolation,
		       memory, cpu, persistent, disk_gb, region,
		       mcp_enabled, COALESCE(mcp_url,''), ttl_expires, created_at, updated_at, secrets, preview_urls
		FROM sandboxes WHERE id=$1 AND tenant_id=$2`, id, tenant)

	sb := &models.Sandbox{}
	var previews []byte
	err = row.Scan(
		&sb.ID, &sb.TenantID, &sb.Status, &sb.PodName, &sb.Template, &sb.Isolation,
		&sb.Memory, &sb.CPU, &sb.Persistent, &sb.DiskGB, &sb.Region,
		&sb.MCPEnabled, &sb.MCPUrl, &sb.TTLExpires, &sb.CreatedAt, &sb.UpdatedAt, pq.Array(&sb.Secrets), &previews,
	)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err == nil {
		err = json.Unmarshal(previews, &sb.PreviewURLs)
	}
	return sb, err
}

func (s *DB) ListSandboxes(ctx context.Context, tenantID string, limit, offset int) ([]*models.Sandbox, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, tenant_id, status, pod_name, template, isolation,
		       memory, cpu, persistent, region, mcp_enabled, created_at
		FROM sandboxes
		WHERE tenant_id=$1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3`, tenantID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sandboxes []*models.Sandbox
	for rows.Next() {
		sb := &models.Sandbox{}
		if err := rows.Scan(
			&sb.ID, &sb.TenantID, &sb.Status, &sb.PodName, &sb.Template, &sb.Isolation,
			&sb.Memory, &sb.CPU, &sb.Persistent, &sb.Region, &sb.MCPEnabled, &sb.CreatedAt,
		); err != nil {
			return nil, err
		}
		sandboxes = append(sandboxes, sb)
	}
	return sandboxes, rows.Err()
}

// Transition atomically moves a sandbox from one status to another.
// If the sandbox is not in `from` status the update is a no-op and ErrBadTransition is returned.
func (s *DB) Transition(ctx context.Context, id, from, to string) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE sandboxes
		SET status=$1, updated_at=NOW()
		WHERE id=$2 AND status=$3 AND tenant_id=$4`,
		to, id, from, mustTenant(ctx),
	)
	if err != nil {
		return err
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return ErrBadTransition
	}
	return nil
}

func (s *DB) UpdatePodName(ctx context.Context, id, podName string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE sandboxes SET pod_name=$1, updated_at=NOW() WHERE id=$2 AND tenant_id=$3`,
		podName, id, mustTenant(ctx),
	)
	return err
}

func (s *DB) UpdateMCPUrl(ctx context.Context, id, mcpURL string, enabled bool) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE sandboxes SET mcp_url=$1, mcp_enabled=$2, updated_at=NOW() WHERE id=$3 AND tenant_id=$4`,
		mcpURL, enabled, id, mustTenant(ctx),
	)
	return err
}

func (s *DB) CountRunningSandboxes(ctx context.Context, tenantID string) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sandboxes WHERE tenant_id=$1 AND status='running'`,
		tenantID,
	).Scan(&count)
	return count, err
}

// ── Warm Pool ─────────────────────────────────────────────────────────────────

type WarmPod struct {
	ID      string
	PodName string
}

func (s *DB) ClaimWarmPod(ctx context.Context, template string) (*WarmPod, error) {
	var pod WarmPod
	err := s.db.QueryRowContext(ctx, `
		UPDATE warm_pool SET status='claimed'
		WHERE id = (
			SELECT id FROM warm_pool
			WHERE template=$1 AND status='ready'
			ORDER BY created_at ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, pod_name`,
		template,
	).Scan(&pod.ID, &pod.PodName)
	if err == sql.ErrNoRows {
		return nil, nil // pool empty
	}
	return &pod, err
}

func (s *DB) InsertWarmPod(ctx context.Context, id, template, podName, isolation string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO warm_pool (id, template, pod_name, isolation) VALUES ($1,$2,$3,$4)`,
		id, template, podName, isolation,
	)
	return err
}

func (s *DB) CountReadyWarmPods(ctx context.Context, template string) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM warm_pool WHERE template=$1 AND status='ready'`,
		template,
	).Scan(&count)
	return count, err
}

func (s *DB) DeleteWarmPod(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM warm_pool WHERE id=$1`, id)
	return err
}

// ── Snapshots ─────────────────────────────────────────────────────────────────

func (s *DB) InsertSnapshot(ctx context.Context, snap *models.Snapshot) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO snapshots (id, sandbox_id, name, size_bytes,pvc_snap) VALUES ($1,$2,$3,$4,$5)`,
		snap.ID, snap.SandboxID, snap.Name, snap.SizeBytes, snap.VolumeSnapshot,
	)
	return err
}

func (s *DB) ListSnapshots(ctx context.Context, sandboxID string) ([]*models.Snapshot, error) {
	if _, err := s.GetSandbox(ctx, sandboxID); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, sandbox_id, name, size_bytes, created_at FROM snapshots WHERE sandbox_id=$1`,
		sandboxID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var snaps []*models.Snapshot
	for rows.Next() {
		sn := &models.Snapshot{}
		rows.Scan(&sn.ID, &sn.SandboxID, &sn.Name, &sn.SizeBytes, &sn.CreatedAt)
		snaps = append(snaps, sn)
	}
	return snaps, rows.Err()
}

// ── Checkpoints ───────────────────────────────────────────────────────────────

func (s *DB) InsertCheckpoint(ctx context.Context, ckpt *models.Checkpoint) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO checkpoints (id, sandbox_id, snapshot_id, name) VALUES ($1,$2,$3,$4)`,
		ckpt.ID, ckpt.SandboxID, ckpt.SnapshotID, ckpt.Name,
	)
	return err
}

func (s *DB) GetCheckpoint(ctx context.Context, id string) (*models.Checkpoint, error) {
	tenant, err := models.Tenant(ctx)
	if err != nil {
		return nil, err
	}
	v := &models.Checkpoint{}
	err = s.db.QueryRowContext(ctx, `select c.id,c.sandbox_id,coalesce(c.snapshot_id,''),c.name,c.created_at from checkpoints c join sandboxes s on s.id=c.sandbox_id where c.id=$1 and s.tenant_id=$2`, id, tenant).Scan(&v.ID, &v.SandboxID, &v.SnapshotID, &v.Name, &v.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	return v, err
}

// ── Webhook Endpoints ─────────────────────────────────────────────────────────

func (s *DB) InsertWebhookEndpoint(ctx context.Context, ep *models.WebhookEndpoint) error {
	events := pq.Array(ep.Events)
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO webhook_endpoints (id, tenant_id, url, events, secret) VALUES ($1,$2,$3,$4,$5)`,
		ep.ID, ep.TenantID, ep.URL, events, ep.Secret,
	)
	return err
}

func (s *DB) GetWebhookEndpointsForEvent(ctx context.Context, tenantID, event string) ([]*models.WebhookEndpoint, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, tenant_id, url, events, secret FROM webhook_endpoints
		 WHERE tenant_id=$1 AND (events @> ARRAY[$2]::text[] OR events @> ARRAY['*']::text[])`,
		tenantID, event,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var eps []*models.WebhookEndpoint
	for rows.Next() {
		ep := &models.WebhookEndpoint{}
		if err := rows.Scan(&ep.ID, &ep.TenantID, &ep.URL, pq.Array(&ep.Events), &ep.Secret); err != nil {
			return nil, err
		}
		eps = append(eps, ep)
	}
	return eps, rows.Err()
}

// ── Audit Log ─────────────────────────────────────────────────────────────────

func (s *DB) InsertAuditEntry(ctx context.Context, e *models.AuditEntry) error {
	meta, _ := json.Marshal(e.Meta)
	secrets := pq.Array(e.SecretsUsed)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO audit_log (id, action, actor_type, actor_id, sandbox_id, meta, secrets_used)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		e.ID, e.Action, e.ActorType, e.ActorID, e.SandboxID, meta, secrets,
	)
	return err
}

func (s *DB) ListAuditEntries(ctx context.Context, sandboxID string, from, to time.Time, limit int) ([]*models.AuditEntry, error) {
	tenant, err := models.Tenant(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, action, actor_type, actor_id, sandbox_id, meta, created_at
		FROM audit_log
		WHERE ($1='' OR sandbox_id=$1) AND actor_id=$5
		  AND created_at >= $2
		  AND created_at <= $3
		ORDER BY created_at DESC
		LIMIT $4`,
		sandboxID, from, to, limit, tenant,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var entries []*models.AuditEntry
	for rows.Next() {
		e := &models.AuditEntry{}
		var meta []byte
		rows.Scan(&e.ID, &e.Action, &e.ActorType, &e.ActorID, &e.SandboxID, &meta, &e.CreatedAt)
		json.Unmarshal(meta, &e.Meta)
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// ── Quota ─────────────────────────────────────────────────────────────────────

func (s *DB) GetQuotaUsage(ctx context.Context, tenantID string) (*models.QuotaUsage, error) {
	u := &models.QuotaUsage{TenantID: tenantID}
	err := s.db.QueryRowContext(ctx,
		`SELECT (select count(*) from sandboxes where tenant_id=$1 and status in ('provisioning','running','resuming')), case when usage_month=date_trunc('month',now())::date then cpu_hours_this_month else 0 end, (select coalesce(sum(disk_gb),0) from sandboxes where tenant_id=$1 and persistent and status<>'failed') FROM quota_usage WHERE tenant_id=$1`,
		tenantID,
	).Scan(&u.ConcurrentSandboxes, &u.CPUHoursThisMonth, &u.StorageGB)
	if err == sql.ErrNoRows {
		err = s.db.QueryRowContext(ctx, `select count(*),coalesce(sum(disk_gb) filter(where persistent),0) from sandboxes where tenant_id=$1 and status in ('provisioning','running','resuming')`, tenantID).Scan(&u.ConcurrentSandboxes, &u.StorageGB)
		return u, err
	}
	return u, err
}

func (s *DB) GetQuotaLimits(ctx context.Context, tenantID string) (*models.QuotaLimits, error) {
	l := &models.QuotaLimits{
		TenantID:            tenantID,
		ConcurrentSandboxes: 5,
		CPUHoursThisMonth:   100,
		StorageGB:           50,
	}
	err := s.db.QueryRowContext(ctx,
		`SELECT concurrent_sandboxes, cpu_hours_this_month, storage_gb
		 FROM quota_limits WHERE tenant_id=$1`,
		tenantID,
	).Scan(&l.ConcurrentSandboxes, &l.CPUHoursThisMonth, &l.StorageGB)
	if err == sql.ErrNoRows {
		return l, nil
	}
	return l, err
}

func (s *DB) IncrementCPUSeconds(ctx context.Context, tenantID string, seconds float64) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO quota_usage (tenant_id, cpu_hours_this_month)
		VALUES ($1, $2/3600.0)
		ON CONFLICT (tenant_id) DO UPDATE
		SET cpu_hours_this_month = case when quota_usage.usage_month=date_trunc('month',now())::date then quota_usage.cpu_hours_this_month else 0 end + EXCLUDED.cpu_hours_this_month, usage_month=date_trunc('month',now())::date,
		    updated_at = NOW()`,
		tenantID, seconds,
	)
	return err
}

// ── Task Definitions ──────────────────────────────────────────────────────────

func (s *DB) InsertTaskDefinition(ctx context.Context, def *models.TaskDefinition) error {
	tenant, err := models.Tenant(ctx)
	if err != nil {
		return err
	}
	def.TenantID = tenant
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO task_definitions
			(id, name, version, template, memory, cpu, isolation, handler,
			 retry_max, retry_backoff_ms, concurrency_limit, concurrency_key, queue_name,tenant_id,checkpoints,disk_gb)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
		def.ID, def.Name, def.Version, def.Template, def.Memory, def.CPU, def.Isolation,
		def.Handler, def.RetryMax, def.RetryBackoffMs, def.ConcurrencyLimit,
		def.ConcurrencyKey, def.QueueName, tenant, def.Checkpoints, def.DiskGB,
	)
	return err
}

func (s *DB) GetLatestTaskDefinition(ctx context.Context, name string) (*models.TaskDefinition, error) {
	tenant, err := models.Tenant(ctx)
	if err != nil {
		return nil, err
	}
	def := &models.TaskDefinition{}
	err = s.db.QueryRowContext(ctx, `
		SELECT id, name, version, template, memory, cpu, isolation, handler,
		       retry_max, retry_backoff_ms, concurrency_limit,
		       COALESCE(concurrency_key,''), queue_name, created_at,checkpoints,disk_gb
		FROM task_definitions
		WHERE name=$1 AND tenant_id=$2
		ORDER BY version DESC LIMIT 1`,
		name, tenant,
	).Scan(
		&def.ID, &def.Name, &def.Version, &def.Template, &def.Memory, &def.CPU, &def.Isolation,
		&def.Handler, &def.RetryMax, &def.RetryBackoffMs, &def.ConcurrencyLimit,
		&def.ConcurrencyKey, &def.QueueName, &def.CreatedAt, &def.Checkpoints, &def.DiskGB,
	)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	def.TenantID = tenant
	return def, err
}

// ── Task Runs ─────────────────────────────────────────────────────────────────

// ── Wait Tokens ───────────────────────────────────────────────────────────────

func (s *DB) InsertWaitToken(ctx context.Context, t *models.WaitToken) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO wait_tokens (id, run_id, description, expires_at) VALUES ($1,$2,$3,$4)`,
		t.ID, t.RunID, t.Description, t.ExpiresAt,
	)
	return err
}

func (s *DB) ResolveWaitToken(ctx context.Context, tokenID string, data []byte) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE wait_tokens SET resolved_at=NOW(), data=$1 WHERE id=$2`,
		data, tokenID,
	)
	return err
}

// ── Schedules ─────────────────────────────────────────────────────────────────

func (s *DB) InsertSchedule(ctx context.Context, sched *models.TaskSchedule) error {
	if len(sched.Payload) == 0 {
		sched.Payload = []byte("null")
	}
	tenant, err := models.Tenant(ctx)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO task_schedules (id, task_name, cron, timezone, payload, next_run_at,tenant_id)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		sched.ID, sched.TaskName, sched.Cron, sched.Timezone, string(sched.Payload), sched.NextRunAt, tenant,
	)
	return err
}

func (s *DB) GetDueSchedules(ctx context.Context, now time.Time) ([]*models.TaskSchedule, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, task_name, cron, timezone, payload, next_run_at,tenant_id
		 FROM task_schedules WHERE enabled=true AND next_run_at <= $1 AND tenant_id<>'__legacy_unassigned__'`,
		now,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var scheds []*models.TaskSchedule
	for rows.Next() {
		sc := &models.TaskSchedule{}
		rows.Scan(&sc.ID, &sc.TaskName, &sc.Cron, &sc.Timezone, &sc.Payload, &sc.NextRunAt, &sc.TenantID)
		scheds = append(scheds, sc)
	}
	return scheds, rows.Err()
}

func (s *DB) UpdateScheduleNextRun(ctx context.Context, id string, nextRun time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE task_schedules SET next_run_at=$1 WHERE id=$2 AND tenant_id=$3`,
		nextRun, id, mustTenant(ctx),
	)
	return err
}

// ── Secrets table (added to migration above) ─────────────────────────────────
// This ensures the secrets table exists. It is included in the main schema
// const but repeated here for clarity — the IF NOT EXISTS guard makes it safe.

const secretsTable = `
CREATE TABLE IF NOT EXISTS secrets (
	name       TEXT NOT NULL,
	value      TEXT NOT NULL,
	tenant_id  TEXT NOT NULL,
	created_at TIMESTAMPTZ DEFAULT NOW(),
	PRIMARY KEY (name, tenant_id)
);
`
