package store

import "context"

const securitySchema = `
CREATE TABLE IF NOT EXISTS schema_versions(version integer PRIMARY KEY,applied_at timestamptz NOT NULL DEFAULT now());
ALTER TABLE quota_usage ADD COLUMN IF NOT EXISTS usage_month date NOT NULL DEFAULT date_trunc('month',now())::date;
CREATE TABLE IF NOT EXISTS api_keys(key_hash text PRIMARY KEY,tenant_id text NOT NULL,revoked boolean NOT NULL DEFAULT false,created_at timestamptz NOT NULL DEFAULT now());
ALTER TABLE task_definitions ADD COLUMN IF NOT EXISTS checkpoints boolean NOT NULL DEFAULT false;
ALTER TABLE task_definitions ADD COLUMN IF NOT EXISTS disk_gb integer NOT NULL DEFAULT 0;
ALTER TABLE task_definitions ADD COLUMN IF NOT EXISTS tenant_id text NOT NULL DEFAULT '__legacy_unassigned__';
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS tenant_id text NOT NULL DEFAULT '__legacy_unassigned__';
ALTER TABLE task_schedules ADD COLUMN IF NOT EXISTS tenant_id text NOT NULL DEFAULT '__legacy_unassigned__';
ALTER TABLE queues ADD COLUMN IF NOT EXISTS tenant_id text NOT NULL DEFAULT '__legacy_unassigned__';
ALTER TABLE task_definitions DROP CONSTRAINT IF EXISTS task_definitions_name_version_key;
CREATE UNIQUE INDEX IF NOT EXISTS task_definitions_tenant_name_version ON task_definitions(tenant_id,name,version);
ALTER TABLE task_runs DROP CONSTRAINT IF EXISTS task_runs_idempotency_key_key;
UPDATE task_runs SET idempotency_key=NULL WHERE idempotency_key='';
CREATE UNIQUE INDEX IF NOT EXISTS task_runs_tenant_idempotency ON task_runs(tenant_id,idempotency_key) WHERE idempotency_key IS NOT NULL;
ALTER TABLE queues DROP CONSTRAINT IF EXISTS queues_pkey;
CREATE UNIQUE INDEX IF NOT EXISTS queues_tenant_name ON queues(tenant_id,name);
ALTER TABLE sandboxes ADD COLUMN IF NOT EXISTS route_cleanup_pending boolean NOT NULL DEFAULT false;
ALTER TABLE sandboxes ADD COLUMN IF NOT EXISTS preview_urls jsonb NOT NULL DEFAULT '[]';
ALTER TABLE sandboxes ADD COLUMN IF NOT EXISTS runtime_config jsonb NOT NULL DEFAULT '{}';
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS lease_until timestamptz;
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS lease_token text;
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS heartbeat_at timestamptz;
CREATE TABLE IF NOT EXISTS webhook_outbox (
 id text PRIMARY KEY,endpoint_id text NOT NULL REFERENCES webhook_endpoints(id) ON DELETE CASCADE,
 tenant_id text NOT NULL,event_type text NOT NULL,payload jsonb NOT NULL,
 status text NOT NULL DEFAULT 'queued',attempts integer NOT NULL DEFAULT 0,
 available_at timestamptz NOT NULL DEFAULT now(),locked_until timestamptz,lease_token text,
 last_error text,created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS webhook_outbox_claim ON webhook_outbox(status,available_at);
CREATE INDEX IF NOT EXISTS task_runs_tenant_status ON task_runs(tenant_id,status);
INSERT INTO schema_versions(version) VALUES (2) ON CONFLICT DO NOTHING;
`

func (s *DB) migrateSecurity(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "select pg_advisory_xact_lock(527061210)"); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, securitySchema); err != nil {
		return err
	}
	return tx.Commit()
}
