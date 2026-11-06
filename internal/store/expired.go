package store

import (
	"context"
	"github.com/yourorg/sandboxing/pkg/models"
)

func (s *DB) ExpiredRuns(ctx context.Context) ([]*models.TaskRun, error) {
	rows, err := s.db.QueryContext(ctx, `select id,tenant_id,coalesce(sandbox_id,''),coalesce(lease_token,'') from task_runs where status='running' and lease_until<now() limit 100`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []*models.TaskRun{}
	for rows.Next() {
		v := &models.TaskRun{}
		if err = rows.Scan(&v.ID, &v.TenantID, &v.SandboxID, &v.LeaseToken); err != nil {
			return nil, err
		}
		items = append(items, v)
	}
	return items, rows.Err()
}
func (s *DB) FinishExpiredRun(ctx context.Context, id, lease string) error {
	_, err := s.db.ExecContext(ctx, `update task_runs set status='failed',error_msg='Worker lease expired. Workload was stopped; inspect effects before replaying.',completed_at=now() where id=$1 and tenant_id=$2 and lease_token=$3 and lease_until<now() and status='running'`, id, mustTenant(ctx), lease)
	return err
}
