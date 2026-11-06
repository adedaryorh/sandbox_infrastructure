package store

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/lib/pq"
	"github.com/yourorg/sandboxing/pkg/models"
)

var ErrQuotaExceeded = errors.New("quota exceeded")

func (s *DB) ReserveSandbox(ctx context.Context, sb *models.Sandbox) error {
	tenant, err := models.Tenant(ctx)
	if err != nil || tenant != sb.TenantID {
		return errors.New("authenticated tenant required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "select pg_advisory_xact_lock(hashtextextended($1,0))", tenant); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "insert into quota_limits(tenant_id) values($1) on conflict do nothing", tenant); err != nil {
		return err
	}
	var limit int
	var hours, storage float64
	if err = tx.QueryRowContext(ctx, "select concurrent_sandboxes,cpu_hours_this_month,storage_gb from quota_limits where tenant_id=$1", tenant).Scan(&limit, &hours, &storage); err != nil {
		return err
	}
	var active int
	var disk float64
	if err = tx.QueryRowContext(ctx, `select count(*) filter(where status in ('provisioning','running','resuming')),coalesce(sum(disk_gb) filter(where persistent and status<>'failed'),0) from sandboxes where tenant_id=$1`, tenant).Scan(&active, &disk); err != nil {
		return err
	}
	var used float64
	if err = tx.QueryRowContext(ctx, "select coalesce((select case when usage_month=date_trunc('month',now())::date then cpu_hours_this_month else 0 end from quota_usage where tenant_id=$1),0)", tenant).Scan(&used); err != nil {
		return err
	}
	if active >= limit || used >= hours || disk+float64(sb.DiskGB) > storage {
		return ErrQuotaExceeded
	}
	_, err = tx.ExecContext(ctx, `insert into sandboxes(id,tenant_id,status,pod_name,template,isolation,memory,cpu,persistent,disk_gb,secrets,region,mcp_enabled,ttl_expires)
 values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`, sb.ID, tenant, sb.Status, sb.PodName, sb.Template, sb.Isolation, sb.Memory, sb.CPU, sb.Persistent, sb.DiskGB, pq.Array(sb.Secrets), sb.Region, sb.MCPEnabled, sb.TTLExpires)
	if err != nil {
		return err
	}
	return tx.Commit()
}
func (s *DB) SaveRuntimeConfig(ctx context.Context, id string, req *models.CreateSandboxRequest) error {
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, "update sandboxes set runtime_config=$1 where id=$2 and tenant_id=$3", string(data), id, mustTenant(ctx))
	return err
}
func (s *DB) RuntimeConfig(ctx context.Context, id string) (*models.CreateSandboxRequest, error) {
	var data []byte
	err := s.db.QueryRowContext(ctx, "select runtime_config from sandboxes where id=$1 and tenant_id=$2", id, mustTenant(ctx)).Scan(&data)
	if err != nil {
		return nil, err
	}
	var v models.CreateSandboxRequest
	err = json.Unmarshal(data, &v)
	if v.Template == "" {
		return nil, errors.New("legacy sandbox lacks persisted runtime configuration")
	}
	return &v, err
}
func (s *DB) SavePreviewURLs(ctx context.Context, id string, urls []string) error {
	data, err := json.Marshal(urls)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, "update sandboxes set preview_urls=$1 where id=$2 and tenant_id=$3", string(data), id, mustTenant(ctx))
	return err
}

func (s *DB) MarkRouteCleanup(ctx context.Context, id string, pending bool) error {
	_, err := s.db.ExecContext(ctx, "update sandboxes set route_cleanup_pending=$1 where id=$2 and tenant_id=$3", pending, id, mustTenant(ctx))
	return err
}
