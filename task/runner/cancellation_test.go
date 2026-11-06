package runner

import (
	"context"
	"errors"
	"github.com/yourorg/sandboxing/pkg/logger"
	"github.com/yourorg/sandboxing/pkg/models"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type fakePersistence struct {
	Persistence
	cancel    atomic.Bool
	completed atomic.Bool
	failed    atomic.Bool
}

func (f *fakePersistence) GetTaskDefinition(ctx context.Context, id string, version int) (*models.TaskDefinition, error) {
	return &models.TaskDefinition{Template: "python", Memory: "512Mi", CPU: "500m", Isolation: models.IsolationStandard, Handler: []byte("code")}, nil
}
func (f *fakePersistence) HeartbeatRun(context.Context, string, string) (bool, error) {
	return !f.cancel.Load(), nil
}
func (f *fakePersistence) AttachRunSandbox(context.Context, string, string) error { return nil }
func (f *fakePersistence) CompleteTaskRun(context.Context, string, []byte) error {
	f.completed.Store(true)
	return nil
}
func (f *fakePersistence) FailTaskRun(context.Context, string, string) error {
	f.failed.Store(true)
	return nil
}
func (f *fakePersistence) RequeueTaskRun(context.Context, string, time.Time) error { return nil }

type fakeExecutor struct {
	Executor
	t         *testing.T
	started   chan struct{}
	destroyed chan struct{}
	nilResult bool
}

func (f *fakeExecutor) Create(ctx context.Context, tenant string, req *models.CreateSandboxRequest) (*models.Sandbox, error) {
	actual, _ := models.Tenant(ctx)
	if tenant != "tenant-a" || actual != tenant {
		f.t.Error("worker escaped tenant scope")
	}
	return &models.Sandbox{ID: "sandbox-unit"}, nil
}
func (f *fakeExecutor) Destroy(ctx context.Context, tenant, id string) error {
	if ctx.Err() != nil {
		f.t.Error("cleanup inherited cancellation")
	}
	close(f.destroyed)
	return nil
}
func (f *fakeExecutor) WriteFileInSandbox(context.Context, *models.Sandbox, string, []byte) error {
	return nil
}
func (f *fakeExecutor) ExecInSandbox(ctx context.Context, sb *models.Sandbox, command string) (*models.ExecResult, error) {
	if strings.Contains(command, `\"$child\"`) {
		f.t.Error("PID command contains escaped quote literals")
	}
	close(f.started)
	if f.nilResult {
		return nil, errors.New("runtime failed")
	}
	<-ctx.Done()
	return nil, ctx.Err()
}
func TestCancellationStopsExecutionAndStillCleansUp(t *testing.T) {
	logger.Init()
	s := &fakePersistence{}
	o := &fakeExecutor{t: t, started: make(chan struct{}), destroyed: make(chan struct{})}
	r := New(s, o, 1)
	r.heartbeat = 5 * time.Millisecond
	done := make(chan struct{})
	go func() {
		r.execute(context.Background(), &models.TaskRun{ID: "run", TenantID: "tenant-a", LeaseToken: "lease", Attempt: 1, MaxAttempts: 1})
		close(done)
	}()
	<-o.started
	s.cancel.Store(true)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation did not interrupt execution")
	}
	select {
	case <-o.destroyed:
	default:
		t.Fatal("workload was not cleaned up")
	}
	if s.completed.Load() {
		t.Fatal("cancelled work marked completed")
	}
}
func TestNilRuntimeResultCannotPanic(t *testing.T) {
	logger.Init()
	s := &fakePersistence{}
	o := &fakeExecutor{t: t, started: make(chan struct{}), destroyed: make(chan struct{}), nilResult: true}
	r := New(s, o, 1)
	r.execute(context.Background(), &models.TaskRun{TenantID: "tenant-a", LeaseToken: "lease", Attempt: 1, MaxAttempts: 1})
	if !s.failed.Load() {
		t.Fatal("runtime failure was not recorded")
	}
}
