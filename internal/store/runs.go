package store

import (
	"context"
	"database/sql"
	"github.com/lib/pq"
	"github.com/yourorg/sandboxing/pkg/models"
	"time"
)

func mustTenant(ctx context.Context) string {
	v, err := models.Tenant(ctx)
	if err != nil {
		return "__no_tenant_context__"
	}
	return v
}
func (s *DB) InsertTaskRun(ctx context.Context, run *models.TaskRun) error {
	tenant, err := models.Tenant(ctx)
	if err != nil {
		return err
	}
	run.TenantID = tenant
	var key any
	if run.IdempotencyKey != "" {
		key = run.IdempotencyKey
	}
	result, err := s.db.ExecContext(ctx, `insert into task_runs(id,tenant_id,task_id,task_version,status,payload,idempotency_key,priority,max_attempts,delay_until,tags)
 select $1,$2,id,version,'queued',$5,$6,$7,$8,$9,$10 from task_definitions where id=$3 and version=$4 and tenant_id=$2
 on conflict (tenant_id,idempotency_key) where idempotency_key is not null do nothing`, run.ID, tenant, run.TaskID, run.TaskVersion, string(run.Payload), key, run.Priority, run.MaxAttempts, run.DelayUntil, pq.Array(run.Tags))
	if err != nil {
		return err
	}
	if key == nil {
		n, e := result.RowsAffected()
		if e != nil {
			return e
		}
		if n == 0 {
			return ErrNotFound
		}
	}
	if key != nil {
		var id string
		if err = s.db.QueryRowContext(ctx, "select id from task_runs where tenant_id=$1 and idempotency_key=$2", tenant, key).Scan(&id); err != nil {
			return err
		}
		existing, e := s.GetTaskRun(ctx, id)
		if e != nil {
			return e
		}
		*run = *existing
	}
	return nil
}
func (s *DB) GetTaskRun(ctx context.Context, id string) (*models.TaskRun, error) {
	tenant, err := models.Tenant(ctx)
	if err != nil {
		return nil, err
	}
	run := &models.TaskRun{TenantID: tenant}
	err = s.db.QueryRowContext(ctx, `select id,task_id,task_version,status,coalesce(payload,'null'),coalesce(idempotency_key,''),priority,attempt,max_attempts,delay_until,resume_at,
 coalesce(checkpoint_id,''),coalesce(wait_token_id,''),coalesce(sandbox_id,''),coalesce(output,'null'),coalesce(error_msg,''),tags,started_at,completed_at,created_at,coalesce(lease_token,'')
 from task_runs where id=$1 and tenant_id=$2`, id, tenant).Scan(&run.ID, &run.TaskID, &run.TaskVersion, &run.Status, &run.Payload, &run.IdempotencyKey, &run.Priority, &run.Attempt, &run.MaxAttempts, &run.DelayUntil, &run.ResumeAt, &run.CheckpointID, &run.WaitTokenID, &run.SandboxID, &run.Output, &run.ErrorMsg, pq.Array(&run.Tags), &run.StartedAt, &run.CompletedAt, &run.CreatedAt, &run.LeaseToken)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	return run, err
}
func (s *DB) DequeueTaskRun(ctx context.Context) (*models.TaskRun, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	// Serialize admission across different candidate rows to enforce aggregate concurrency limits.
	if _, err = tx.ExecContext(ctx, "select pg_advisory_xact_lock(527061211)"); err != nil {
		return nil, err
	}
	var id, tenant string
	err = tx.QueryRowContext(ctx, `select tr.id,tr.tenant_id from task_runs tr join task_definitions td on td.id=tr.task_id and td.tenant_id=tr.tenant_id
 left join queues q on q.name=td.queue_name and q.tenant_id=tr.tenant_id
 where tr.status='queued' and tr.tenant_id<>'__legacy_unassigned__' and tr.delay_until<=now() and coalesce(q.paused,false)=false
 and (select count(*) from task_runs active join task_definitions d on d.id=active.task_id where active.tenant_id=tr.tenant_id and active.status='running' and d.name=td.name)<td.concurrency_limit
 and (select count(*) from task_runs active join task_definitions d on d.id=active.task_id where active.tenant_id=tr.tenant_id and active.status='running' and d.queue_name=td.queue_name)<coalesce(q.concurrency_limit,10)
 and (coalesce(td.concurrency_key,'')='' or not exists(select 1 from task_runs active join task_definitions d on d.id=active.task_id where active.tenant_id=tr.tenant_id and active.status='running' and d.name=td.name and active.payload->>td.concurrency_key=tr.payload->>td.concurrency_key))
 order by tr.priority desc,tr.created_at,tr.id limit 1 for update of tr skip locked`).Scan(&id, &tenant)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	token := models.NewID("lease")
	if _, err = tx.ExecContext(ctx, `update task_runs set status='running',attempt=case when checkpoint_id is null then attempt+1 else attempt end,started_at=now(),lease_until=now()+interval '90 seconds',heartbeat_at=now(),lease_token=$2 where id=$1`, id, token); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return s.GetTaskRun(models.WithTenant(ctx, tenant), id)
}
func (s *DB) HeartbeatRun(ctx context.Context, id, token string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `update task_runs set lease_until=now()+interval '90 seconds',heartbeat_at=now() where id=$1 and tenant_id=$2 and status='running' and lease_token=$3 and lease_until>now()`, id, mustTenant(ctx), token)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}
func (s *DB) AttachRunSandbox(ctx context.Context, id, sandbox string) error {
	_, err := s.db.ExecContext(ctx, `update task_runs set sandbox_id=$1 where id=$2 and tenant_id=$3 and lease_token=$4 and status='running'`, sandbox, id, mustTenant(ctx), models.Lease(ctx))
	return err
}
func (s *DB) CompleteTaskRun(ctx context.Context, id string, output []byte) error {
	if len(output) == 0 {
		output = []byte("null")
	}
	_, err := s.db.ExecContext(ctx, `update task_runs set status='completed',output=$1,completed_at=now(),lease_until=null where id=$2 and tenant_id=$3 and status='running' and lease_token=$4`, string(output), id, mustTenant(ctx), models.Lease(ctx))
	return err
}
func (s *DB) FailTaskRun(ctx context.Context, id, msg string) error {
	_, err := s.db.ExecContext(ctx, `update task_runs set status='failed',error_msg=$1,completed_at=now(),lease_until=null where id=$2 and tenant_id=$3 and status='running' and lease_token=$4`, msg, id, mustTenant(ctx), models.Lease(ctx))
	return err
}
func (s *DB) RequeueTaskRun(ctx context.Context, id string, delay time.Time) error {
	_, err := s.db.ExecContext(ctx, `update task_runs set status='queued',delay_until=$1,error_msg=null,lease_until=null where id=$2 and tenant_id=$3 and status='running' and lease_token=$4`, delay, id, mustTenant(ctx), models.Lease(ctx))
	return err
}
func (s *DB) SuspendTaskRun(ctx context.Context, id, status, checkpoint string, resume *time.Time) error {
	_, err := s.db.ExecContext(ctx, `update task_runs set status=$1,checkpoint_id=$2,resume_at=$3,lease_until=null where id=$4 and tenant_id=$5 and status='running' and lease_token=$6`, status, checkpoint, resume, id, mustTenant(ctx), models.Lease(ctx))
	return err
}
func (s *DB) GetRunsReadyToResume(ctx context.Context) ([]*models.TaskRun, error) {
	rows, err := s.db.QueryContext(ctx, `select id,tenant_id,checkpoint_id from task_runs where status='waiting_for_duration' and resume_at<=now()`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*models.TaskRun
	for rows.Next() {
		v := &models.TaskRun{}
		if err = rows.Scan(&v.ID, &v.TenantID, &v.CheckpointID); err != nil {
			return nil, err
		}
		result = append(result, v)
	}
	return result, rows.Err()
}
func (s *DB) GetTaskDefinition(ctx context.Context, id string, version int) (*models.TaskDefinition, error) {
	d := &models.TaskDefinition{TenantID: mustTenant(ctx)}
	err := s.db.QueryRowContext(ctx, `select id,name,version,template,memory,cpu,isolation,handler,retry_max,retry_backoff_ms,concurrency_limit,coalesce(concurrency_key,''),queue_name,checkpoints,disk_gb from task_definitions where id=$1 and version=$2 and tenant_id=$3`, id, version, mustTenant(ctx)).Scan(&d.ID, &d.Name, &d.Version, &d.Template, &d.Memory, &d.CPU, &d.Isolation, &d.Handler, &d.RetryMax, &d.RetryBackoffMs, &d.ConcurrencyLimit, &d.ConcurrencyKey, &d.QueueName, &d.Checkpoints, &d.DiskGB)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	return d, err
}
