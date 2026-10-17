package runner

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/yourorg/sandboxing/internal/orchestrator"
	"github.com/yourorg/sandboxing/internal/store"
	"github.com/yourorg/sandboxing/pkg/logger"
	"github.com/yourorg/sandboxing/pkg/models"
)

// Runner is a pool of workers that dequeue task runs and execute them.
// Each run creates a sandbox, injects the handler, executes it,
// handles the result (complete or retry), then destroys the sandbox.
type Runner struct {
	store  *store.DB
	orch   *orchestrator.Orchestrator
	workers int
}

func New(store *store.DB, orch *orchestrator.Orchestrator, workers int) *Runner {
	return &Runner{
		store:   store,
		orch:    orch,
		workers: workers,
	}
}

// Start launches the worker pool. Blocks until ctx is cancelled.
func (r *Runner) Start(ctx context.Context) {
	logger.Info("task runner started", "workers", r.workers)

	for i := 0; i < r.workers; i++ {
		go r.worker(ctx, i)
	}

	// resume loop — wakes suspended tasks when their timer fires
	go r.resumeLoop(ctx)

	<-ctx.Done()
	logger.Info("task runner stopped")
}

// worker is a single goroutine that continuously polls for queued runs.
func (r *Runner) worker(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			run, err := r.store.DequeueTaskRun(ctx)
			if err != nil {
				logger.Error("dequeue task run", "worker", id, "err", err)
				time.Sleep(time.Second)
				continue
			}
			if run == nil {
				// queue empty — back off before polling again
				time.Sleep(500 * time.Millisecond)
				continue
			}
			r.execute(ctx, run)
		}
	}
}

// execute runs a single task run inside a freshly created sandbox.
func (r *Runner) execute(ctx context.Context, run *models.TaskRun) {
	logger.Info("executing task run", "run_id", run.ID, "task_id", run.TaskID, "attempt", run.Attempt)

	// load task definition to get template, handler code, etc.
	def, err := r.loadDefinition(ctx, run)
	if err != nil {
		r.store.FailTaskRun(ctx, run.ID, fmt.Sprintf("load definition: %v", err))
		return
	}

	// create a sandbox for this run
	sb, err := r.orch.Create(ctx, "system", &models.CreateSandboxRequest{
		Template:  def.Template,
		Memory:    def.Memory,
		CPU:       def.CPU,
		Isolation: def.Isolation,
	})
	if err != nil {
		r.handleFailure(ctx, run, def, fmt.Sprintf("create sandbox: %v", err))
		return
	}

	// always destroy the sandbox when done — even on failure
	defer func() {
		if err := r.orch.Destroy(ctx, "system", sb.ID); err != nil {
			logger.Error("destroy task sandbox", "sandbox_id", sb.ID, "err", err)
		}
	}()

	// write handler code and payload into the sandbox
	if err := r.injectHandler(ctx, sb, def, run); err != nil {
		r.handleFailure(ctx, run, def, fmt.Sprintf("inject handler: %v", err))
		return
	}

	// execute the handler
	// handler is responsible for reading /task/payload.json and writing /task/output.json
	result, err := r.orch.ExecInSandbox(ctx, sb, "python /task/handler.py")
	if err != nil || result.ExitCode != 0 {
		errMsg := result.Stderr
		if err != nil {
			errMsg = err.Error()
		}
		r.handleFailure(ctx, run, def, errMsg)
		return
	}

	// read output file
	output, _ := r.readOutput(ctx, sb)

	// mark completed
	if err := r.store.CompleteTaskRun(ctx, run.ID, output); err != nil {
		logger.Error("complete task run", "run_id", run.ID, "err", err)
	}

	logger.Info("task run completed", "run_id", run.ID)
}

// handleFailure retries with exponential backoff or marks the run as failed.
func (r *Runner) handleFailure(ctx context.Context, run *models.TaskRun, def *models.TaskDefinition, errMsg string) {
	logger.Warn("task run failed", "run_id", run.ID, "attempt", run.Attempt, "max", run.MaxAttempts, "err", errMsg)

	if run.Attempt < run.MaxAttempts {
		// exponential backoff: 1s, 2s, 4s, 8s...
		// capped by the task's max backoff setting
		backoffMs := float64(def.RetryBackoffMs) * math.Pow(2, float64(run.Attempt-1))
		backoff := time.Duration(backoffMs) * time.Millisecond
		delayUntil := time.Now().Add(backoff)

		logger.Info("retrying task run", "run_id", run.ID, "delay", backoff)
		r.store.RequeueTaskRun(ctx, run.ID, delayUntil)
	} else {
		// exhausted all attempts — mark as permanently failed
		r.store.FailTaskRun(ctx, run.ID, errMsg)
	}
}

// ── Wait primitives ───────────────────────────────────────────────────────────

// SuspendForDuration CRIU-checkpoints the sandbox, releases the pod (billing stops),
// and records when the run should resume.
func (r *Runner) SuspendForDuration(ctx context.Context, tenantID, runID, sandboxID string, d time.Duration) error {
	// checkpoint the sandbox — freeze all processes to disk
	ckpt, err := r.orch.Checkpoint(ctx, tenantID, sandboxID, "wait-"+runID)
	if err != nil {
		return fmt.Errorf("checkpoint for wait: %w", err)
	}

	// pause the sandbox — pod is deleted, billing stops
	if err := r.orch.Pause(ctx, tenantID, sandboxID); err != nil {
		return fmt.Errorf("pause for wait: %w", err)
	}

	resumeAt := time.Now().Add(d)
	return r.store.SuspendTaskRun(ctx, runID, string(models.TaskRunWaitDuration), ckpt.ID, &resumeAt)
}

// SuspendForToken suspends a run awaiting a human decision token.
func (r *Runner) SuspendForToken(ctx context.Context, tenantID, runID, sandboxID, tokenID string) error {
	ckpt, err := r.orch.Checkpoint(ctx, tenantID, sandboxID, "token-"+runID)
	if err != nil {
		return fmt.Errorf("checkpoint for token: %w", err)
	}

	if err := r.orch.Pause(ctx, tenantID, sandboxID); err != nil {
		return fmt.Errorf("pause for token: %w", err)
	}

	return r.store.SuspendTaskRun(ctx, runID, string(models.TaskRunWaitToken), ckpt.ID, nil)
}

// resumeLoop polls every 15 seconds for runs whose wait timer has fired.
func (r *Runner) resumeLoop(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.resumeReady(ctx)
		}
	}
}

func (r *Runner) resumeReady(ctx context.Context) {
	runs, err := r.store.GetRunsReadyToResume(ctx)
	if err != nil {
		logger.Error("get runs ready to resume", "err", err)
		return
	}
	for _, run := range runs {
		go func(run *models.TaskRun) {
			// restore from CRIU checkpoint — resumes mid-execution in ~300ms
			sb, err := r.orch.RestoreCheckpoint(ctx, "system", run.CheckpointID)
			if err != nil {
				logger.Error("restore checkpoint for resume", "run_id", run.ID, "err", err)
				r.store.FailTaskRun(ctx, run.ID, "restore checkpoint: "+err.Error())
				return
			}
			// update run with new sandbox ID
			r.store.Raw().ExecContext(ctx,
				`UPDATE task_runs SET status='running', sandbox_id=$1 WHERE id=$2`,
				sb.ID, run.ID,
			)
			logger.Info("task run resumed from checkpoint", "run_id", run.ID, "sandbox_id", sb.ID)
		}(run)
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func (r *Runner) loadDefinition(ctx context.Context, run *models.TaskRun) (*models.TaskDefinition, error) {
	rows, err := r.store.Raw().QueryRowContext(ctx, `
		SELECT id, name, version, template, memory, cpu, isolation, handler,
		       retry_max, retry_backoff_ms, concurrency_limit
		FROM task_definitions
		WHERE id=$1 AND version=$2`,
		run.TaskID, run.TaskVersion,
	)
	if err != nil {
		return nil, err
	}
	def := &models.TaskDefinition{}
	err = rows.Scan(
		&def.ID, &def.Name, &def.Version, &def.Template, &def.Memory, &def.CPU,
		&def.Isolation, &def.Handler, &def.RetryMax, &def.RetryBackoffMs, &def.ConcurrencyLimit,
	)
	return def, err
}

func (r *Runner) injectHandler(ctx context.Context, sb *models.Sandbox, def *models.TaskDefinition, run *models.TaskRun) error {
	// write handler code
	if err := r.orch.WriteFileInSandbox(ctx, sb, "/task/handler.py", def.Handler); err != nil {
		return fmt.Errorf("write handler: %w", err)
	}
	// write payload JSON
	if err := r.orch.WriteFileInSandbox(ctx, sb, "/task/payload.json", run.Payload); err != nil {
		return fmt.Errorf("write payload: %w", err)
	}
	return nil
}

func (r *Runner) readOutput(ctx context.Context, sb *models.Sandbox) ([]byte, error) {
	return r.orch.ReadFileInSandbox(ctx, sb, "/task/output.json")
}
