package store

import (
	"context"
	"fmt"
	"github.com/yourorg/sandboxing/pkg/models"
	"time"
)

func (s *DB) GetResumeInput(ctx context.Context, id string) ([]byte, error) {
	var data []byte
	err := s.db.QueryRowContext(ctx, `select coalesce(w.data,'null') from task_runs r left join wait_tokens w on w.id=r.wait_token_id where r.id=$1 and r.tenant_id=$2`, id, mustTenant(ctx)).Scan(&data)
	return data, err
}
func (s *DB) PromoteWaitingRuns(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `update task_runs r set status='timed_out',completed_at=now(),error_msg='Approval wait expired' where status='waiting_for_token' and exists(select 1 from wait_tokens w where w.id=r.wait_token_id and w.resolved_at is null and w.expires_at<=now())`)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `update task_runs r set status='queued',delay_until=now() where checkpoint_id is not null and ((status='waiting_for_duration' and resume_at<=now()) or(status='waiting_for_token' and exists(select 1 from wait_tokens w where w.id=r.wait_token_id and w.resolved_at is not null and w.resolved_at<w.expires_at)))`)
	return err
}
func (s *DB) ResolveRunToken(ctx context.Context, id string, data []byte) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var token string
	err = tx.QueryRowContext(ctx, `select coalesce(wait_token_id,'') from task_runs where id=$1 and tenant_id=$2 and status='waiting_for_token' for update`, id, mustTenant(ctx)).Scan(&token)
	if err != nil {
		return ErrNotFound
	}
	result, err := tx.ExecContext(ctx, `update wait_tokens set resolved_at=now(),data=$1 where id=$2 and resolved_at is null and expires_at>now()`, string(data), token)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return fmt.Errorf("wait token is expired or already resolved")
	}
	return tx.Commit()
}
func (s *DB) SetRunWait(ctx context.Context, id, checkpoint, status, token string, resume *time.Time) error {
	result, err := s.db.ExecContext(ctx, `update task_runs set status=$1,checkpoint_id=$2,wait_token_id=nullif($3,''),resume_at=$4,lease_until=null where id=$5 and tenant_id=$6 and status='running' and lease_token=$7`, status, checkpoint, token, resume, id, mustTenant(ctx), models.Lease(ctx))
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return fmt.Errorf("run changed while checkpointing")
	}
	return nil
}
