package store

import (
	"context"
	"github.com/lib/pq"
	"github.com/yourorg/sandboxing/pkg/models"
)

func (s *DB) ListWebhookEndpoints(ctx context.Context) ([]*models.WebhookEndpoint, error) {
	rows, err := s.db.QueryContext(ctx, "select id,tenant_id,url,events,created_at from webhook_endpoints where tenant_id=$1 order by created_at", mustTenant(ctx))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*models.WebhookEndpoint{}
	for rows.Next() {
		v := &models.WebhookEndpoint{}
		if err = rows.Scan(&v.ID, &v.TenantID, &v.URL, pq.Array(&v.Events), &v.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (s *DB) DeleteWebhookEndpoint(ctx context.Context, id string) error {
	r, e := s.db.ExecContext(ctx, "delete from webhook_endpoints where id=$1 and tenant_id=$2", id, mustTenant(ctx))
	if e != nil {
		return e
	}
	n, _ := r.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}
