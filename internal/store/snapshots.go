package store

import (
	"context"
	"database/sql"
	"github.com/yourorg/sandboxing/pkg/models"
)

func (s *DB) GetSnapshot(ctx context.Context, id string) (*models.Snapshot, error) {
	v := &models.Snapshot{}
	err := s.db.QueryRowContext(ctx, `select p.id,p.sandbox_id,p.name,p.size_bytes,p.created_at,coalesce(p.pvc_snap,'') from snapshots p join sandboxes s on s.id=p.sandbox_id where p.id=$1 and s.tenant_id=$2`, id, mustTenant(ctx)).Scan(&v.ID, &v.SandboxID, &v.Name, &v.SizeBytes, &v.CreatedAt, &v.VolumeSnapshot)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	return v, err
}
