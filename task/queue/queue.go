package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/yourorg/sandboxing/internal/store"
	"github.com/yourorg/sandboxing/pkg/models"
)

// Queue manages task run enqueueing with idempotency, priority, and delay support.
type Queue struct {
	store *store.DB
}

func New(store *store.DB) *Queue {
	return &Queue{store: store}
}

// TriggerOptions controls how a task run is enqueued.
type TriggerOptions struct {
	IdempotencyKey string
	Priority       int    // higher = dequeued sooner
	Delay          string // e.g. "10m", "1h"
	TTL            string // expire from queue if not started within this duration
	Tags           []string
	Metadata       map[string]interface{}
}

// Trigger enqueues a task run and returns immediately.
// If idempotency_key already exists and the run is complete, returns the existing run.
func (q *Queue) Trigger(ctx context.Context, taskName string, payload interface{}, opts TriggerOptions) (*models.TaskRun, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	if opts.IdempotencyKey != "" {
		existing, e := q.getByIdempotencyKey(ctx, opts.IdempotencyKey)
		if e == nil {
			def, e := q.store.GetTaskDefinition(ctx, existing.TaskID, existing.TaskVersion)
			if e != nil {
				return nil, e
			}
			var a, b any
			json.Unmarshal(existing.Payload, &a)
			json.Unmarshal(payloadBytes, &b)
			if def.Name != taskName || !reflect.DeepEqual(a, b) {
				return nil, fmt.Errorf("idempotency key was already used for different work")
			}
			return existing, nil
		}
		if !errors.Is(e, store.ErrNotFound) {
			return nil, e
		}
	}

	// load task definition
	def, err := q.store.GetLatestTaskDefinition(ctx, taskName)
	if err != nil {
		return nil, fmt.Errorf("task %q not found: %w", taskName, err)
	}

	priority := opts.Priority
	if priority == 0 {
		priority = 5 // default normal priority
	}

	delayUntil := time.Now()
	if opts.Delay != "" {
		d, err := time.ParseDuration(opts.Delay)
		if err != nil || d < 0 {
			return nil, fmt.Errorf("invalid delay")
		}
		delayUntil = time.Now().Add(d)
	}

	run := &models.TaskRun{
		ID:             models.NewID("run"),
		TaskID:         def.ID,
		TaskVersion:    def.Version,
		Status:         models.TaskRunQueued,
		Payload:        payloadBytes,
		IdempotencyKey: opts.IdempotencyKey,
		Priority:       priority,
		MaxAttempts:    def.RetryMax,
		DelayUntil:     delayUntil,
		Tags:           opts.Tags,
		CreatedAt:      time.Now(),
	}

	if err := q.store.InsertTaskRun(ctx, run); err != nil {
		return nil, fmt.Errorf("insert task run: %w", err)
	}

	// Recheck the winner after an INSERT conflict: another request may have raced the lookup.
	if opts.IdempotencyKey != "" {
		actual, e := q.store.GetTaskDefinition(ctx, run.TaskID, run.TaskVersion)
		if e != nil {
			return nil, e
		}
		var a, b any
		if json.Unmarshal(run.Payload, &a) != nil || json.Unmarshal(payloadBytes, &b) != nil || actual.Name != taskName || !reflect.DeepEqual(a, b) {
			return nil, fmt.Errorf("idempotency key was already used for different work")
		}
	}
	return run, nil
}

// TriggerAndWait enqueues a run and blocks until it completes or ctx is cancelled.
func (q *Queue) TriggerAndWait(ctx context.Context, taskName string, payload interface{}, opts TriggerOptions) (*models.TaskRun, error) {
	run, err := q.Trigger(ctx, taskName, payload, opts)
	if err != nil {
		return nil, err
	}

	// poll for completion
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			current, err := q.store.GetTaskRun(ctx, run.ID)
			if err != nil {
				return nil, err
			}
			switch current.Status {
			case models.TaskRunCompleted, models.TaskRunFailed,
				models.TaskRunCancelled, models.TaskRunTimedOut:
				return current, nil
			}
		}
	}
}

// BatchTrigger enqueues multiple runs in a single call.
func (q *Queue) BatchTrigger(ctx context.Context, taskName string, payloads []interface{}, opts TriggerOptions) ([]*models.TaskRun, error) {
	runs := make([]*models.TaskRun, 0, len(payloads))
	for _, payload := range payloads {
		run, err := q.Trigger(ctx, taskName, payload, opts)
		if err != nil {
			return runs, err
		}
		runs = append(runs, run)
	}
	return runs, nil
}

// Cancel marks a queued or running run as cancelled.
func (q *Queue) Cancel(ctx context.Context, id string) error {
	result, err := q.store.Raw().ExecContext(ctx, `update task_runs set status='cancelled',completed_at=now() where id=$1 and tenant_id=$2 and status in ('queued','running','waiting_for_duration','waiting_for_token')`, id, tenant(ctx))
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return store.ErrBadTransition
	}
	return nil
}

// Replay re-enqueues a failed or cancelled run with the same payload.
func (q *Queue) Replay(ctx context.Context, runID string) (*models.TaskRun, error) {
	original, err := q.store.GetTaskRun(ctx, runID)
	if err != nil {
		return nil, err
	}

	newRun := &models.TaskRun{
		ID:          models.NewID("run"),
		TaskID:      original.TaskID,
		TaskVersion: original.TaskVersion,
		Status:      models.TaskRunQueued,
		Payload:     original.Payload,
		Priority:    original.Priority,
		MaxAttempts: original.MaxAttempts,
		DelayUntil:  time.Now(),
		Tags:        original.Tags,
		CreatedAt:   time.Now(),
	}

	if err := q.store.InsertTaskRun(ctx, newRun); err != nil {
		return nil, err
	}
	return newRun, nil
}

// PauseQueue stops new runs from being dequeued for a named queue.
func (q *Queue) PauseQueue(ctx context.Context, name string) error {
	_, err := q.store.Raw().ExecContext(ctx,
		`INSERT INTO queues (name,tenant_id, paused) VALUES ($1,$2, true)
		 ON CONFLICT (tenant_id,name) DO UPDATE SET paused=true`, name, tenant(ctx))
	return err
}

// ResumeQueue allows dequeuing to resume for a named queue.
func (q *Queue) ResumeQueue(ctx context.Context, name string) error {
	_, err := q.store.Raw().ExecContext(ctx,
		`UPDATE queues SET paused=false WHERE name=$1 AND tenant_id=$2`, name, tenant(ctx))
	return err
}

// GetQueue returns depth and status of a named queue.
func (q *Queue) GetQueue(ctx context.Context, name string) (*models.Queue, error) {
	v := &models.Queue{Name: name, TenantID: tenant(ctx)}
	var exists bool
	err := q.store.Raw().QueryRowContext(ctx, `select coalesce((select paused from queues where name=$1 and tenant_id=$2),false),exists(select 1 from queues where name=$1 and tenant_id=$2) or exists(select 1 from task_definitions where queue_name=$1 and tenant_id=$2)`, name, tenant(ctx)).Scan(&v.Paused, &exists)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, store.ErrNotFound
	}
	err = q.store.Raw().QueryRowContext(ctx, `select count(*) filter(where r.status='queued'),count(*) filter(where r.status='running') from task_runs r join task_definitions d on d.id=r.task_id where r.tenant_id=$1 and d.queue_name=$2`, tenant(ctx), name).Scan(&v.Depth, &v.ActiveRuns)
	return v, err
}

func (q *Queue) getByIdempotencyKey(ctx context.Context, key string) (*models.TaskRun, error) {
	var id string
	err := q.store.Raw().QueryRowContext(ctx, "select id from task_runs where tenant_id=$1 and idempotency_key=$2", tenant(ctx), key).Scan(&id)
	if err == sql.ErrNoRows {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return q.store.GetTaskRun(ctx, id)
}

func tenant(ctx context.Context) string { v, _ := models.Tenant(ctx); return v }
