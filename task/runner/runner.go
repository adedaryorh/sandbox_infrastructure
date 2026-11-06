package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/yourorg/sandboxing/pkg/logger"
	"github.com/yourorg/sandboxing/pkg/models"
	"math"
	"time"
)

type Persistence interface {
	GetTaskRun(context.Context, string) (*models.TaskRun, error)
	GetResumeInput(context.Context, string) ([]byte, error)
	PromoteWaitingRuns(context.Context) error
	SetRunWait(context.Context, string, string, string, string, *time.Time) error
	InsertWaitToken(context.Context, *models.WaitToken) error
	DequeueTaskRun(context.Context) (*models.TaskRun, error)
	GetTaskDefinition(context.Context, string, int) (*models.TaskDefinition, error)
	HeartbeatRun(context.Context, string, string) (bool, error)
	AttachRunSandbox(context.Context, string, string) error
	CompleteTaskRun(context.Context, string, []byte) error
	FailTaskRun(context.Context, string, string) error
	RequeueTaskRun(context.Context, string, time.Time) error
	ExpiredRuns(context.Context) ([]*models.TaskRun, error)
	FinishExpiredRun(context.Context, string, string) error
}
type Executor interface {
	Create(context.Context, string, *models.CreateSandboxRequest) (*models.Sandbox, error)
	Destroy(context.Context, string, string) error
	ExecInSandbox(context.Context, *models.Sandbox, string) (*models.ExecResult, error)
	WriteFileInSandbox(context.Context, *models.Sandbox, string, []byte) error
	ReadFileInSandbox(context.Context, *models.Sandbox, string) ([]byte, error)
}
type Runner struct {
	store     Persistence
	orch      Executor
	workers   int
	heartbeat time.Duration
}

func New(s Persistence, o Executor, workers int) *Runner {
	return &Runner{store: s, orch: o, workers: workers, heartbeat: time.Second}
}
func (r *Runner) Start(ctx context.Context) {
	for i := 0; i < r.workers; i++ {
		go r.worker(ctx)
	}
	go r.reap(ctx)
	<-ctx.Done()
}
func (r *Runner) worker(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		r.store.PromoteWaitingRuns(ctx)
		run, err := r.store.DequeueTaskRun(ctx)
		if err != nil {
			logger.Error("task dequeue failed")
		}
		if run == nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
		r.execute(ctx, run)
	}
}
func (r *Runner) execute(parent context.Context, run *models.TaskRun) {
	scoped := models.WithLease(models.WithTenant(parent, run.TenantID), run.LeaseToken)
	ctx, cancel := context.WithTimeout(scoped, 30*time.Minute)
	defer cancel()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(r.heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				valid, e := r.store.HeartbeatRun(ctx, run.ID, run.LeaseToken)
				if e != nil || !valid {
					cancel()
					return
				}
			}
		}
	}()
	defer func() { close(stop); cancel(); <-done }()
	definition, err := r.store.GetTaskDefinition(ctx, run.TaskID, run.TaskVersion)
	if err != nil {
		r.failure(scoped, run, nil, "task definition unavailable")
		return
	}
	var sb *models.Sandbox
	if run.CheckpointID != "" {
		restorer, ok := r.orch.(CheckpointExecutor)
		if !ok {
			r.failure(scoped, run, nil, "checkpoint runtime unavailable")
			return
		}
		input, e := r.store.GetResumeInput(ctx, run.ID)
		if e != nil {
			r.failure(scoped, run, nil, "resume input unavailable")
			return
		}
		sb, err = restorer.RestoreCheckpointWithInput(ctx, run.TenantID, run.CheckpointID, input)
	} else {
		sb, err = r.orch.Create(ctx, run.TenantID, &models.CreateSandboxRequest{Template: definition.Template, Memory: definition.Memory, CPU: definition.CPU, Isolation: definition.Isolation, TTL: "30m", Persistent: definition.Checkpoints, DiskGB: definition.DiskGB})
	}

	if err != nil {
		r.failure(scoped, run, definition, "sandbox creation failed")
		return
	}
	defer func() {
		cleanup, end := context.WithTimeout(context.WithoutCancel(scoped), 30*time.Second)
		defer end()
		if e := r.orch.Destroy(cleanup, run.TenantID, sb.ID); e != nil {
			logger.Error("task workload cleanup failed", "sandbox_id", sb.ID)
		}
	}()
	if err = r.store.AttachRunSandbox(ctx, run.ID, sb.ID); err != nil {
		r.failure(scoped, run, definition, "sandbox association failed")
		return
	}
	if run.CheckpointID == "" {
		if err = r.orch.WriteFileInSandbox(ctx, sb, "/task/handler.py", definition.Handler); err != nil {
			r.failure(scoped, run, definition, "handler upload failed")
			return
		}
		if err = r.orch.WriteFileInSandbox(ctx, sb, "/task/payload.json", run.Payload); err != nil {
			r.failure(scoped, run, definition, "payload upload failed")
			return
		}
		result, e := r.orch.ExecInSandbox(ctx, sb, `python /task/handler.py & child=$!; printf '%s' "$child" > /workspace/.checkpoint.pid; wait "$child"`)
		if e != nil || result == nil || result.ExitCode != 0 {
			r.failure(scoped, run, definition, "task process failed or was cancelled")
			return
		}
	} else {
		for {
			check, e := r.orch.ExecInSandbox(ctx, sb, "kill -0 $(cat /workspace/.checkpoint.pid)")
			if e != nil {
				r.failure(scoped, run, nil, "resumed task monitor failed")
				return
			}
			if check != nil && check.ExitCode != 0 {
				break
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	}

	output, err := r.orch.ReadFileInSandbox(ctx, sb, "/task/output.json")
	if err != nil || !json.Valid(output) {
		r.failure(scoped, run, definition, "task output must be valid JSON")
		return
	}
	if ctx.Err() != nil {
		return
	}
	if err = r.store.CompleteTaskRun(ctx, run.ID, output); err != nil {
		logger.Error("persist task completion failed", "run_id", run.ID)
	}
}
func (r *Runner) failure(ctx context.Context, run *models.TaskRun, def *models.TaskDefinition, message string) {
	save, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if def != nil && run.Attempt < run.MaxAttempts {
		delay := float64(def.RetryBackoffMs) * math.Pow(2, float64(run.Attempt-1))
		if delay > 300000 {
			delay = 300000
		}
		if delay < 100 {
			delay = 100
		}
		r.store.RequeueTaskRun(save, run.ID, time.Now().Add(time.Duration(delay)*time.Millisecond))
		return
	}
	r.store.FailTaskRun(save, run.ID, message)
}
func (r *Runner) reap(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.reapOnce(ctx)
		}
	}
}
func (r *Runner) reapOnce(ctx context.Context) {
	items, e := r.store.ExpiredRuns(ctx)
	if e != nil {
		return
	}
	for _, run := range items {
		scope := models.WithTenant(ctx, run.TenantID)
		if run.SandboxID != "" {
			if e = r.orch.Destroy(scope, run.TenantID, run.SandboxID); e != nil {
				continue
			}
		}
		r.store.FinishExpiredRun(scope, run.ID, run.LeaseToken)
	}
}

type CheckpointExecutor interface {
	Checkpoint(context.Context, string, string, string) (*models.Checkpoint, error)
	RestoreCheckpointWithInput(context.Context, string, string, []byte) (*models.Sandbox, error)
}

func (r *Runner) Wait(ctx context.Context, runID string, duration, tokenTTL time.Duration, description string) error {
	run, err := r.store.GetTaskRun(ctx, runID)
	if err != nil {
		return err
	}
	if run.Status != models.TaskRunRunning || run.SandboxID == "" {
		return fmt.Errorf("run is not executing")
	}
	definition, err := r.store.GetTaskDefinition(ctx, run.TaskID, run.TaskVersion)
	if err != nil || !definition.Checkpoints {
		return fmt.Errorf("task was not deployed with qualified checkpoint support")
	}
	cp, ok := r.orch.(CheckpointExecutor)
	if !ok {
		return fmt.Errorf("checkpoint runtime unavailable")
	}
	scope := models.WithLease(ctx, run.LeaseToken)
	checkpoint, err := cp.Checkpoint(scope, run.TenantID, run.SandboxID, "wait-"+runID)
	if err != nil {
		return err
	}
	status := "waiting_for_duration"
	var resume *time.Time
	token := ""
	if tokenTTL > 0 {
		status = "waiting_for_token"
		v := &models.WaitToken{ID: models.NewID("wait"), RunID: runID, Description: description, ExpiresAt: time.Now().Add(tokenTTL)}
		if err = r.store.InsertWaitToken(scope, v); err != nil {
			return err
		}
		token = v.ID
	} else {
		v := time.Now().Add(duration)
		resume = &v
	}
	if err = r.store.SetRunWait(scope, runID, checkpoint.ID, status, token, resume); err != nil {
		return err
	}
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(scope), 30*time.Second)
	defer cancel()
	return r.orch.Destroy(cleanup, run.TenantID, run.SandboxID)
}
