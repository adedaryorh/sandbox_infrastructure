package queue

import (
	"context"
	"encoding/json"
	"fmt"
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
	Priority       int        // higher = dequeued sooner
	Delay          string     // e.g. "10m", "1h"
	TTL            string     // expire from queue if not started within this duration
	Tags           []string
	Metadata       map[string]interface{}
}

// Trigger enqueues a task run and returns immediately.
// If idempotency_key already exists and the run is complete, returns the existing run.
func (q *Queue) Trigger(ctx context.Context, taskName string, payload interface{}, opts TriggerOptions) (*models.TaskRun, error) {
	// if idempotency key provided, check for existing run
	if opts.IdempotencyKey != "" {
		existing, err := q.getByIdempotencyKey(ctx, opts.IdempotencyKey)
		if err == nil && existing != nil {
			// deduplicate — return the existing run
			return existing, nil
		}
	}

	// load task definition
	def, err := q.store.GetLatestTaskDefinition(ctx, taskName)
	if err != nil {
		return nil, fmt.Errorf("task %q not found: %w", taskName, err)
	}

	// marshal payload
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}

	priority := opts.Priority
	if priority == 0 {
		priority = 5 // default normal priority
	}

	delayUntil := time.Now()
	if opts.Delay != "" {
		d, err := time.ParseDuration(opts.Delay)
		if err == nil {
			delayUntil = time.Now().Add(d)
		}
	}

	run := &models.TaskRun{
		ID:             fmt.Sprintf("run-%d", time.Now().UnixNano()),
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
func (q *Queue) Cancel(ctx context.Context, runID string) error {
	_, err := q.store.Raw().ExecContext(ctx,
		`UPDATE task_runs SET status='cancelled', completed_at=NOW()
		 WHERE id=$1 AND status IN ('queued','running','waiting_for_duration','waiting_for_token')`,
		runID,
	)
	return err
}

// Replay re-enqueues a failed or cancelled run with the same payload.
func (q *Queue) Replay(ctx context.Context, runID string) (*models.TaskRun, error) {
	original, err := q.store.GetTaskRun(ctx, runID)
	if err != nil {
		return nil, err
	}

	newRun := &models.TaskRun{
		ID:          fmt.Sprintf("run-%d", time.Now().UnixNano()),
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
		`INSERT INTO queues (name, paused) VALUES ($1, true)
		 ON CONFLICT (name) DO UPDATE SET paused=true`, name)
	return err
}

// ResumeQueue allows dequeuing to resume for a named queue.
func (q *Queue) ResumeQueue(ctx context.Context, name string) error {
	_, err := q.store.Raw().ExecContext(ctx,
		`UPDATE queues SET paused=false WHERE name=$1`, name)
	return err
}

// GetQueue returns depth and status of a named queue.
func (q *Queue) GetQueue(ctx context.Context, name string) (*models.Queue, error) {
	queue := &models.Queue{Name: name}
	q.store.Raw().QueryRowContext(ctx,
		`SELECT COALESCE(paused, false) FROM queues WHERE name=$1`, name,
	).Scan(&queue.Paused)

	q.store.Raw().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_runs tr
		 JOIN task_definitions td ON tr.task_id=td.id
		 WHERE td.queue_name=$1 AND tr.status='queued'`, name,
	).Scan(&queue.Depth)

	q.store.Raw().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_runs tr
		 JOIN task_definitions td ON tr.task_id=td.id
		 WHERE td.queue_name=$1 AND tr.status='running'`, name,
	).Scan(&queue.ActiveRuns)

	return queue, nil
}

func (q *Queue) getByIdempotencyKey(ctx context.Context, key string) (*models.TaskRun, error) {
	run := &models.TaskRun{}
	err := q.store.Raw().QueryRowContext(ctx,
		`SELECT id, task_id, task_version, status, output FROM task_runs WHERE idempotency_key=$1`,
		key,
	).Scan(&run.ID, &run.TaskID, &run.TaskVersion, &run.Status, &run.Output)
	if err != nil {
		return nil, err
	}
	return run, nil
}
