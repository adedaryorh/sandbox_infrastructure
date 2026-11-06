# sandboxing_go

A Go control plane for running code in Kubernetes sandboxes.

The server creates execution environments, runs commands inside them, manages workspace files, and tracks their lifecycle in PostgreSQL. Clients can use the HTTP API or CLI to work with a sandbox. A separate task subsystem handles versioned Python jobs, queued runs, and retries.

The project is aimed at AI-agent tooling, code evaluation, and background execution: applications that need a workspace and a process to run, without managing Kubernetes pods directly.

**Status:** Development prototype. Authentication and tenant isolation are incomplete, so use a trusted development environment. The [current limitations](#current-limitations) section separates the working code paths from integrations that still need work.

## What it does

- Creates sandbox pods from a template or container image.
- Runs shell commands and returns stdout, stderr, exit codes, and execution duration.
- Streams command output through server-sent events and exposes an interactive terminal over WebSocket.
- Provides file upload, download, directory listing, and deletion endpoints.
- Manages interpreter sessions for Python, Node.js, Bash, and Ruby, provided the interpreter is installed in the selected image.
- Pauses a sandbox by deleting its pod, then resumes it by creating a new pod. Persistent workspaces use a PVC; temporary workspaces do not survive pod deletion.
- Tracks sandbox state and runs a cleanup loop for running sandboxes whose requested TTL has expired.
- Stores task definitions and runs in PostgreSQL, with worker polling, delayed execution, idempotency-key handling, and retry logic.
- Records audit events and delivers HMAC-signed webhooks for supported events.

Warm pools, preview routing, checkpoints, GPU configuration, and region selection also have source files in the repository. Their integration status is described below rather than counted as finished features.

## How it fits together

```mermaid
flowchart TD
    Clients["CLI and HTTP clients"] --> API["Gin API server"]
    API --> Orchestrator["Sandbox orchestrator"]
    Orchestrator --> Kubernetes["Kubernetes API"]
    Kubernetes --> Sandboxes["Sandbox pods and workspaces"]
    API --> Database["PostgreSQL"]
    Workers["Task workers"] --> Database
    Workers --> Orchestrator
    Orchestrator --> Database
```

The API handles requests and passes lifecycle operations to the orchestrator. The orchestrator creates or removes Kubernetes resources and updates the corresponding database records. Execution and file operations use the Kubernetes client to communicate with the sandbox container.

Task workers poll PostgreSQL for queued runs. Each run creates a sandbox, writes its handler and input, executes the handler, records the result, and attempts cleanup. Run failures can be requeued with exponential backoff.

The server also starts background loops for warm-pool refill, usage accounting, TTL cleanup, expiry warnings, and scheduled tasks. Warm-pool claims and task dequeue operations use PostgreSQL row locking with `FOR UPDATE SKIP LOCKED`.

## Stack

| Component | Technology |
|---|---|
| Control plane | Go; root module declares Go 1.25.0 |
| HTTP API | Gin |
| CLI | Cobra |
| Interactive terminal | Gorilla WebSocket |
| Execution infrastructure | Kubernetes client-go |
| Persistence | PostgreSQL; local Compose uses PostgreSQL 16 |
| Optional routing infrastructure | Caddy |
| Telemetry scaffolding | OpenTelemetry; optional Jaeger Compose profile |

The Go module currently uses the placeholder path `github.com/yourorg/sandboxing`.

## Run locally

The current server entry point requires Kubernetes. Docker Compose starts the supporting services; its Go server service is commented out. Setting `SANDBOX_DEV_MODE=true` does not switch the running server to the Docker development helper.

### 1. Prepare the environment

You need a Go toolchain compatible with `go.mod`, Docker Compose, and a running Kubernetes cluster with a working kubeconfig.

From the repository root, check the cluster and create the sandbox namespace if needed:

```bash
kubectl config current-context
kubectl get nodes
kubectl create namespace sandboxes --dry-run=client -o yaml | kubectl apply -f -
```

Start the local database and Caddy:

```bash
docker compose up -d --wait postgres caddy
```

The Compose configuration uses local development database credentials. Do not run `docker compose down -v` as a routine restart command: it removes the named volumes, including the database volume.

### 2. Configure the server

```bash
export PORT=8080
export DATABASE_URL='postgres://sandboxing:sandboxing@localhost:5432/sandboxing?sslmode=disable'
export KUBECONFIG="$HOME/.kube/config"
export SANDBOX_NAMESPACE=sandboxes
export CADDY_ADMIN_URL='http://localhost:2019'
export PLATFORM_DOMAIN=localhost

# Start with cold provisioning. See the warm-pool limitation below.
export WARM_PYTHON=0
export WARM_NODE=0
export WARM_UBUNTU=0
```

The server creates its database tables during startup. The PostgreSQL database itself must already exist; the Compose service creates `sandboxing` on its initial database setup.

### 3. Start the API

```bash
go run ./cmd/server
```

In another terminal:

```bash
curl http://localhost:8080/health
```

The health endpoint reports that the HTTP handler is responding. It does not validate sandbox provisioning or cluster capacity.

### 4. Create a sandbox

Provision a key with `DATABASE_URL=... go run ./cmd/key --tenant local`, then export the printed key below. The middleware validates its stored hash and resolves the owning tenant.

```bash
export API='http://localhost:8080'
export SANDBOX_API_KEY='<paste-your-provisioned-key>'

curl -sS -X POST "$API/v1/sandboxes" \
  -H "Authorization: Bearer $SANDBOX_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
    "template": "python",
    "memory": "512Mi",
    "cpu": "500m",
    "ttl": "15m",
    "isolation": "standard"
  }'
```

Creation waits for the pod to reach Kubernetes `Running` phase. Copy the returned sandbox ID for the next commands:

```bash
export SANDBOX_ID='replace-with-the-returned-id'
```

### 5. Execute a command

```bash
curl -sS -X POST "$API/v1/sandboxes/$SANDBOX_ID/exec" \
  -H "Authorization: Bearer $SANDBOX_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
    "cmd": "python -c \"print(2 + 2)\"",
    "timeout_seconds": 30
  }'
```

The response contains `stdout`, `stderr`, `exit_code`, and `duration_ms`.

To receive output as an SSE stream:

```bash
curl -N -G "$API/v1/sandboxes/$SANDBOX_ID/exec/stream" \
  -H "Authorization: Bearer $SANDBOX_API_KEY" \
  --data-urlencode 'cmd=python -u -c "print(2 + 2)"'
```

### 6. Work with files

```bash
printf 'print("hello from the workspace")\n' > hello.py

curl -sS -X PUT "$API/v1/sandboxes/$SANDBOX_ID/files/workspace/hello.py" \
  -H "Authorization: Bearer $SANDBOX_API_KEY" \
  --data-binary @hello.py

curl -sS "$API/v1/sandboxes/$SANDBOX_ID/files/workspace/hello.py" \
  -H "Authorization: Bearer $SANDBOX_API_KEY"

curl -sS -G "$API/v1/sandboxes/$SANDBOX_ID/files" \
  -H "Authorization: Bearer $SANDBOX_API_KEY" \
  --data-urlencode 'path=/workspace'
```

### 7. Clean up

```bash
curl -sS -X DELETE "$API/v1/sandboxes/$SANDBOX_ID" \
  -H "Authorization: Bearer $SANDBOX_API_KEY"
```

Persistent sandboxes retain their PVC when destroyed. Account for those volumes separately when cleaning up a development cluster.

## CLI

Build the server and CLI using the existing Makefile targets:

```bash
make build-all
```

This produces `bin/server` and `bin/sandbox`.

Configure the CLI:

```bash
export SANDBOX_API_URL='http://localhost:8080'
export SANDBOX_API_KEY='<paste-your-provisioned-key>'
```

Common commands:

```bash
./bin/sandbox create --template python --memory 512Mi --ttl 15m --isolation standard
./bin/sandbox list
./bin/sandbox exec "$SANDBOX_ID" -- python --version
./bin/sandbox ls "$SANDBOX_ID" /workspace
./bin/sandbox cp hello.py "$SANDBOX_ID:/workspace/hello.py"
./bin/sandbox cp "$SANDBOX_ID:/workspace/hello.py" downloaded-hello.py
./bin/sandbox pause "$SANDBOX_ID"
./bin/sandbox resume "$SANDBOX_ID"
./bin/sandbox destroy "$SANDBOX_ID"
```

Use `./bin/sandbox --help` and the relevant subcommand's `--help` for the complete command list. The CLI also registers snapshot, checkpoint, and task commands; registration does not establish that every underlying integration is complete.

## Templates and sandbox settings

The Kubernetes client maps these template names to images:

| Template | Image in the current source |
|---|---|
| `python` | `python:3.11-slim` |
| `node` | `node:20-slim` |
| `ubuntu` | `ubuntu:22.04` |
| `ruby` | `ruby:3.3-slim` |
| `go` | `golang:1.22-bookworm` |
| `python-cuda` | `nvidia/cuda:12.3.0-runtime-ubuntu22.04` |

An unrecognized template value is treated as a container image reference. The image needs `/bin/sh` and `sleep` for the current pod startup command, plus the tools required by the workload.

Sandbox creation accepts resource settings, environment variables, a TTL, isolation, secret references, and persistent-workspace settings. Persistent storage requires both `persistent: true` and a positive `disk_gb` value, together with a usable cluster storage configuration.

The `secure` cold-provisioning path sets `runtimeClassName: gvisor`. That runtime class and its handler must be installed on the cluster. Keep warm pools disabled when testing isolation or custom sandbox configuration; the current claim path only matches the template.

The pod builder requests a non-root user, drops Linux capabilities, disables privilege escalation, and sets CPU and memory requests and limits. These settings are parts of the prototype's pod specification, not a guarantee that arbitrary workloads are safe to execute.

## API overview

All routes below are registered by the current handlers. `/health` is unauthenticated; the application routes pass through the development authentication middleware.

### Sandboxes and execution

| Method | Path | Operation |
|---|---|---|
| `POST` | `/v1/sandboxes` | Create a sandbox |
| `GET` | `/v1/sandboxes` | List sandboxes; accepts `limit` and `offset` |
| `GET` | `/v1/sandboxes/:id` | Read sandbox details |
| `DELETE` | `/v1/sandboxes/:id` | Destroy a sandbox |
| `POST` | `/v1/sandboxes/:id/pause` | Delete the pod and mark the sandbox paused |
| `POST` | `/v1/sandboxes/:id/resume` | Create a replacement pod |
| `POST` | `/v1/sandboxes/:id/exec` | Run a command |
| `GET` | `/v1/sandboxes/:id/exec/stream` | Stream execution output with SSE |
| `GET` | `/v1/sandboxes/:id/pty` | Open a WebSocket terminal |
| `GET` | `/v1/sandboxes/:id/files` | List a directory |
| `PUT`, `GET`, `DELETE` | `/v1/sandboxes/:id/files/*path` | Write, read, or delete a file |
| `POST` | `/v1/sandboxes/:id/repls` | Open an interpreter session |
| `POST`, `DELETE` | `/v1/sandboxes/:id/repls/:rid` | Send code or close a session |

To open a Python session, send `{"kernel":"python"}` to the REPL creation endpoint. Subsequent requests to that session use `{"code":"..."}`. Sessions are held in the server process and do not survive a server restart.

### Tasks, runs, and queues

| Method | Path | Operation |
|---|---|---|
| `POST`, `GET` | `/v1/tasks` | Deploy a definition or list tasks |
| `GET`, `DELETE` | `/v1/tasks/:id` | Read or delete a task definition by name |
| `GET` | `/v1/tasks/:id/versions` | List versions for a task name |
| `POST` | `/v1/tasks/:id/trigger` | Queue a run for a task name |
| `POST` | `/v1/tasks/:id/trigger-and-wait` | Queue a run and wait for its result |
| `POST` | `/v1/tasks/:id/batch` | Queue multiple payloads |
| `GET` | `/v1/runs` | List recent runs; accepts `task` and `status` filters |
| `GET` | `/v1/runs/:id` | Read a run |
| `GET` | `/v1/runs/:id/stream` | Stream run-status changes with SSE |
| `POST` | `/v1/runs/:id/cancel`, `/replay`, `/resume` | Change a run's state or request replay/resume |
| `POST` | `/v1/runs/bulk-cancel`, `/v1/runs/bulk-replay` | Apply bulk run operations |
| `GET` | `/v1/queues`, `/v1/queues/:name` | Read queue information |
| `POST` | `/v1/queues/:name/pause`, `/resume` | Pause or resume a queue |
| `POST`, `GET` | `/v1/schedules` | Create or list schedules |
| `PUT`, `DELETE` | `/v1/schedules/:id` | Update or delete a schedule |
| `POST` | `/v1/schedules/:id/disable`, `/enable` | Disable or enable a schedule |

The task handlers use a task **name** for several `:id` parameters. The task runner currently executes Python handlers from `/task/handler.py`, reads input from `/task/payload.json`, and expects output at `/task/output.json`. Its image must provide Python and a writable `/task` directory.

Other registered routes cover snapshots, checkpoints, MCP metadata, sandbox status/uptime, webhook registration, secrets, quotas, and audit records. The implementation notes for those areas are in the limitations section.

## Configuration

Values below come from `pkg/config/config.go`.

| Variable | Default | Purpose |
|---|---|---|
| `PORT` | `8080` | HTTP listening port |
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/sandboxing?sslmode=disable` | Database connection; override for the Compose credentials |
| `KUBECONFIG` | Empty | Kubeconfig path; empty selects in-cluster configuration |
| `SANDBOX_NAMESPACE` | `sandboxes` | Namespace used by the Kubernetes client |
| `DEFAULT_CPU` | `500m` | Pod-builder fallback; orchestration also applies request defaults |
| `DEFAULT_MEMORY` | `512Mi` | Pod-builder fallback; orchestration also applies request defaults |
| `WARM_PYTHON` | `10` | Python warm-pool target |
| `WARM_NODE` | `10` | Node.js warm-pool target |
| `WARM_UBUNTU` | `5` | Ubuntu warm-pool target |
| `WARM_POOL_REFILL_SEC` | `10` | Warm-pool refill interval |
| `CADDY_ADMIN_URL` | `http://localhost:2019` | Routing configuration value |
| `PLATFORM_DOMAIN` | `sandbox.yourplatform.com` | Routing configuration value |
| `TASK_WORKERS` | `10` | Task-runner worker count |
| `BILLING_TICK_SEC` | `1` | Parsed setting; the accounting loop currently uses a fixed one-second ticker |
| `WEBHOOK_WORKERS` | `5` | Parsed setting; the emitter currently launches delivery goroutines directly |

Without an explicit `ttl` in a sandbox creation request, the orchestrator does not set an expiry. Use a duration such as `15m` or `1h` when you want automatic expiry cleanup.

## Repository layout

| Path | Contents |
|---|---|
| `cmd/server/` | Server startup, middleware, and background-service wiring |
| `cli/cmd/` | CLI and HTTP client |
| `internal/api/` | HTTP handlers and OpenAPI document |
| `internal/orchestrator/` | Lifecycle operations, warm pools, and region helpers |
| `internal/k8s/` | Pod construction, execution, file operations, PVCs, and GPU helpers |
| `internal/provider/` | Infrastructure-provider interface |
| `internal/store/` | PostgreSQL schema and queries |
| `internal/repl/` | Interpreter-session manager |
| `internal/quota/` | Creation-time quota checks and usage accounting |
| `internal/audit/`, `internal/webhook/` | Audit records and event delivery |
| `internal/router/`, `internal/devmode/` | Routing and Docker development helpers |
| `task/` | Task registry, queue, runner, and scheduler |
| `pkg/` | Shared models, configuration, logging, and tracing |
| `k8s/` | Cluster, deployment, networking, runtime, and storage manifests |
| `sdk/` | Generated Go, Python, and TypeScript client files |

## Development

```bash
make build-all
make test
make test-race
go vet ./...
```

`make lint` requires `golangci-lint`. Tests are present for configuration and Kubernetes pod construction. The commands above describe how to run checks; this README does not report an executed end-to-end test or benchmark result.

The Go, Python, and TypeScript SDK directories are generated from `internal/api/openapi.yaml`:

```bash
make generate-sdk
```

The specification currently lacks complete request and response schemas. Expand and align it with the handlers before treating generated clients as a complete API contract. SDK regeneration rewrites generated files.

## Hardening update

Authentication now validates hashed API keys and scopes resources to tenants. Task workers use leases, cancellation stops the workload, schedules evaluate cron expressions, and webhook retries are persisted. Preview routing, authenticated MCP tools, measured runtime metrics, explicit GPU requests, and Kubernetes VolumeSnapshot operations are connected to the server.

See [HARDENING.md](HARDENING.md) for migration, updated startup instructions, deployment prerequisites, and verification limits. Those instructions supersede older development examples in this document. Checkpoint recovery remains experimental and disabled by default; production infrastructure qualification is still required.

## License

Apache License 2.0. See [LICENSE](LICENSE).
