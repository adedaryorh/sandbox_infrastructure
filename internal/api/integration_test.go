package api_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/yourorg/sandboxing/internal/api"
	"github.com/yourorg/sandboxing/internal/audit"
	"github.com/yourorg/sandboxing/internal/auth"
	"github.com/yourorg/sandboxing/internal/orchestrator"
	"github.com/yourorg/sandboxing/internal/provider"
	"github.com/yourorg/sandboxing/internal/repl"
	"github.com/yourorg/sandboxing/internal/store"
	"github.com/yourorg/sandboxing/internal/webhook"
	"github.com/yourorg/sandboxing/pkg/logger"
	"github.com/yourorg/sandboxing/pkg/models"
	"github.com/yourorg/sandboxing/task/queue"
	"github.com/yourorg/sandboxing/task/registry"
	"github.com/yourorg/sandboxing/task/scheduler"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
)

type runtimeFake struct {
	provider.Runtime
	calls atomic.Int32
}

func (f *runtimeFake) CreatePod(context.Context, *models.CreateSandboxRequest, string) (string, error) {
	return "pod-test", nil
}
func (f *runtimeFake) WaitForPodRunning(context.Context, string) error               { return nil }
func (f *runtimeFake) DeletePod(context.Context, string) error                       { return nil }
func (f *runtimeFake) CreateSecret(context.Context, string, map[string]string) error { return nil }
func (f *runtimeFake) DeleteSecret(context.Context, string) error                    { return nil }
func (f *runtimeFake) Exec(context.Context, string, string) (*models.ExecResult, error) {
	f.calls.Add(1)
	return &models.ExecResult{Stdout: "ok"}, nil
}
func (f *runtimeFake) ReadFile(context.Context, string, string) ([]byte, error) {
	f.calls.Add(1)
	return []byte("owned file"), nil
}
func (f *runtimeFake) WriteFile(context.Context, string, string, []byte) error {
	f.calls.Add(1)
	return nil
}
func (f *runtimeFake) GetMetrics(context.Context, string) (*models.ResourceMetrics, error) {
	f.calls.Add(1)
	return &models.ResourceMetrics{Source: "test"}, nil
}
func TestPostgresTenantIsolationAndTaskPersistence(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required for PostgreSQL integration; not a verified integration result")
	}
	logger.Init()
	db, e := store.New(dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Raw().Close()
	if e = db.Migrate(context.Background()); e != nil {
		t.Fatal(e)
	}
	a, b := models.NewID("tenant"), models.NewID("tenant")
	keyA := strings.Repeat("samehead", 4) + a
	keyB := strings.Repeat("samehead", 4) + b
	for tenant, key := range map[string]string{a: keyA, b: keyB} {
		if e = db.ProvisionAPIKey(context.Background(), tenant, key); e != nil {
			t.Fatal(e)
		}
	}
	t.Cleanup(func() {
		for _, tenant := range []string{a, b} {
			for _, table := range []string{"task_runs", "task_schedules", "task_definitions", "queues", "webhook_endpoints", "sandboxes", "quota_limits", "quota_usage", "api_keys"} {
				db.Raw().Exec("delete from "+table+" where tenant_id=$1", tenant)
			}
		}
	})
	fake := &runtimeFake{}
	events := webhook.NewEmitter(db)
	orch := orchestrator.New(db, fake, events, audit.New(db), nil)
	handler := api.NewHandler(db, orch, fake, repl.NewManager(fake), events, audit.New(db))
	router := gin.New()
	router.Use(auth.Middleware(db))
	handler.RegisterRoutes(router)
	tasks := api.NewTaskHandler(db, registry.New(db), queue.New(db), scheduler.New(db, queue.New(db)))
	tasks.RegisterTaskRoutes(router)
	perform := func(method, path, key, body string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		return w
	}
	create := perform("POST", "/v1/sandboxes", keyA, `{"template":"python","isolation":"standard","ttl":"10m"}`)
	if create.Code != 201 {
		t.Fatalf("create: %d %s", create.Code, create.Body)
	}
	var sb models.Sandbox
	if e = json.Unmarshal(create.Body.Bytes(), &sb); e != nil {
		t.Fatal(e)
	}
	checks := []struct{ method, suffix, body string }{{"GET", "", ""}, {"DELETE", "", ""}, {"POST", "/exec", `{"cmd":"id"}`}, {"GET", "/files/workspace/a", ""}, {"PUT", "/files/workspace/a", "a"}, {"POST", "/mcp/enable", "{}"}, {"GET", "/metrics", ""}, {"POST", "/snapshots", `{"name":"test"}`}, {"POST", "/pause", "{}"}, {"GET", "/pty", ""}}
	for _, check := range checks {
		path := "/v1/sandboxes/" + sb.ID + check.suffix
		if w := perform(check.method, path, "", check.body); w.Code != 401 {
			t.Errorf("anonymous %s: %d", path, w.Code)
		}
		if w := perform(check.method, path, keyB, check.body); w.Code != 404 {
			t.Errorf("foreign %s: %d %s", path, w.Code, w.Body)
		}
	}
	if fake.calls.Load() != 0 {
		t.Fatal("foreign caller reached execution backend")
	}
	if w := perform("POST", "/v1/sandboxes/"+sb.ID+"/exec", keyA, `{"cmd":"id"}`); w.Code != 200 {
		t.Fatal(w.Code, w.Body)
	}
	if w := perform("POST", "/v1/sandboxes/"+sb.ID+"/mcp/enable", keyA, "{}"); w.Code != 200 {
		t.Fatal(w.Body)
	}
	if w := perform("POST", "/v1/sandboxes/"+sb.ID+"/mcp/rpc", keyA, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"exec","arguments":{"cmd":"id"}}}`); w.Code != 200 || strings.Contains(w.Body.String(), `"isError":true`) {
		t.Fatal(w.Code, w.Body)
	}
	handlerCode := base64.StdEncoding.EncodeToString([]byte("print('test')"))
	definition := fmt.Sprintf(`{"name":"shared-name","template":"python","isolation":"standard","handler":"%s"}`, handlerCode)
	for _, key := range []string{keyA, keyB} {
		if w := perform("POST", "/v1/tasks", key, definition); w.Code != 201 {
			t.Fatalf("definition: %d %s", w.Code, w.Body)
		}
	}
	var runA, runB models.TaskRun
	for key, result := range map[string]*models.TaskRun{keyA: &runA, keyB: &runB} {
		w := perform("POST", "/v1/tasks/shared-name/trigger", key, `{"payload":{"a":1},"idempotency_key":"same-key"}`)
		if w.Code != 202 {
			t.Fatalf("trigger: %d %s", w.Code, w.Body)
		}
		if e = json.Unmarshal(w.Body.Bytes(), result); e != nil {
			t.Fatal(e)
		}
	}
	if runA.ID == runB.ID {
		t.Fatal("idempotency crossed tenant boundary")
	}
	if w := perform("GET", "/v1/runs/"+runA.ID, keyB, ""); w.Code != 404 {
		t.Fatal("foreign run read", w.Code, w.Body)
	}
	if w := perform("POST", "/v1/runs/"+runA.ID+"/cancel", keyB, "{}"); w.Code != 404 {
		t.Fatal("foreign run cancelled", w.Code)
	}
	if w := perform("POST", "/v1/runs/"+runA.ID+"/cancel", keyA, "{}"); w.Code != 200 {
		t.Fatal(w.Body)
	}
	for i := 0; i < 2; i++ {
		if w := perform("POST", "/v1/tasks/shared-name/trigger", keyA, `{"payload":{"a":2}}`); w.Code != 202 {
			t.Fatal("empty idempotency key conflicts", w.Code, w.Body)
		}
	}
	if w := perform("POST", "/v1/schedules", keyA, `{"task_name":"shared-name","cron":"*/15 * * * *","timezone":"UTC","payload":{"schedule":true}}`); w.Code != 201 {
		t.Fatal("schedule persistence", w.Code, w.Body)
	}
	ctxB := models.WithTenant(context.Background(), b)
	if _, e = db.GetSandbox(ctxB, sb.ID); e != store.ErrNotFound {
		t.Fatal("store bypassed tenant boundary", e)
	}
	// Verify cancellation fencing against a stale worker lease.
	claimed, e := db.DequeueTaskRun(context.Background())
	if e != nil || claimed == nil {
		t.Fatal("dequeue", e)
	}
	scope := models.WithLease(models.WithTenant(context.Background(), claimed.TenantID), claimed.LeaseToken)
	if e = queue.New(db).Cancel(scope, claimed.ID); e != nil {
		t.Fatal(e)
	}
	if e = db.CompleteTaskRun(scope, claimed.ID, []byte(`{"late":true}`)); e != nil {
		t.Fatal(e)
	}
	after, e := db.GetTaskRun(scope, claimed.ID)
	if e != nil || after.Status != models.TaskRunCancelled {
		t.Fatal("stale worker overwrote cancellation", e)
	}
}
