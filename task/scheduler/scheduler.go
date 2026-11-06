package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/yourorg/sandboxing/internal/store"
	"github.com/yourorg/sandboxing/pkg/logger"
	"github.com/yourorg/sandboxing/pkg/models"
	"github.com/yourorg/sandboxing/task/queue"
)

// Scheduler fires task runs on cron schedules.
// It polls the task_schedules table every minute and enqueues due tasks.
type Scheduler struct {
	store *store.DB
	queue *queue.Queue
}

func New(store *store.DB, queue *queue.Queue) *Scheduler {
	return &Scheduler{store: store, queue: queue}
}

// Run starts the scheduler loop. Blocks until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	logger.Info("cron scheduler started")

	// fire immediately on start to catch any missed runs
	s.tick(ctx)

	for {
		select {
		case <-ctx.Done():
			logger.Info("cron scheduler stopped")
			return
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

func (s *Scheduler) tick(ctx context.Context) {
	schedules, err := s.store.GetDueSchedules(ctx, time.Now())
	if err != nil {
		logger.Error("get due schedules", "err", err)
		return
	}

	for _, sched := range schedules {
		go s.fire(ctx, sched)
	}
}

func (s *Scheduler) fire(ctx context.Context, sched *models.TaskSchedule) {
	// idempotency key prevents duplicate runs if scheduler fires twice
	// format: <schedule_id>-<minute>
	ctx = models.WithTenant(ctx, sched.TenantID)
	now := sched.NextRunAt.UTC()
	idempotencyKey := fmt.Sprintf("%s-%s", sched.ID, now.Format("2006-01-02T15:04"))

	_, err := s.queue.Trigger(ctx, sched.TaskName, json.RawMessage(sched.Payload), queue.TriggerOptions{
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		logger.Error("fire scheduled task", "schedule_id", sched.ID, "task", sched.TaskName, "err", err)
		return
	}

	// update next run time
	next, err := Next(sched.Cron, sched.Timezone, time.Now())
	if err != nil {
		logger.Error("invalid stored schedule", "schedule", sched.ID)
		return
	}
	s.store.UpdateScheduleNextRun(ctx, sched.ID, next)

	logger.Info("fired scheduled task", "task", sched.TaskName, "next_run", next)
}

// Compatibility helper retained for existing callers; new API paths use Next and propagate validation errors.
func nextCronTime(expr string, after time.Time) time.Time { v, _ := Next(expr, "UTC", after); return v }
