# Implementation and deployment notes

The hardening update replaces prefix-based authentication with hashed API keys, scopes resources and tasks by tenant, adds worker leases and cancellation cleanup, evaluates five-field cron expressions, and persists webhook deliveries and retries. Warm-pool claims only accept compatible configurations. Preview requests pass through an authenticated gateway. MCP exposes authenticated execution and file tools. Kubernetes snapshot operations now call the VolumeSnapshot API.

CPU accounting samples runtime metrics; it is an estimate, not invoice-grade metering. Docker is a development backend and rejects persistent volumes and secure isolation. Kubernetes requires a network-policy-capable CNI, metrics-server, an installed gVisor runtime for secure isolation, and a CSI driver with snapshot support for snapshots. GPU requests require device plugins and compatible nodes.

Checkpoint recovery remains experimental and disabled by default. CRIU, kernel, container permissions, storage consistency, and actual restore behavior require qualification on the target cluster. Do not enable checkpoints merely because compilation passes. Webhook deliveries are durable after enqueue; enqueue is not in the same transaction as every originating business operation, so exactly-once delivery is not promised.

## Migration

Back up PostgreSQL before starting the updated server. Startup applies versioned schema changes under an advisory lock. Existing globally scoped task records are quarantined under `__legacy_unassigned__`; an operator must establish their correct ownership before assigning them to tenants. Old prefix-derived API tokens no longer authenticate.

Provision an API key using `DATABASE_URL=... go run ./cmd/key --tenant local`, or supply a random key of at least 32 characters through SANDBOX_BOOTSTRAP_API_KEY and set SANDBOX_BOOTSTRAP_TENANT. Send it as an Authorization Bearer header. Keep keys and the database URL out of source control.

## Local development

Copy `.env.example` to `.env`, replace credentials, and set a generated bootstrap key. Export the variables in your shell before running the server; the Go process does not automatically load this file. Start PostgreSQL and Caddy with `docker compose up -d`, then run `go run ./cmd/server`. Docker mode requires the Docker CLI and daemon permissions on the host. Use standard isolation. Do not use `docker compose down -v` unless intentionally deleting database data.

## Kubernetes

`k8s/base` is the canonical starting point; older manifests remain as historical alternatives and must not be applied alongside it. Build and load or publish the server image, then update its image reference. Create namespace `sandboxes` and a `sandboxing-config` Secret containing DATABASE_URL, PUBLIC_API_URL, PLATFORM_DOMAIN, and the provisioning credentials if used. Use a managed or separately provisioned PostgreSQL database. Apply with `kubectl apply -k k8s/base`.

Configure TLS ingress for the API and wildcard preview hostnames. Set PREVIEW_SCHEME=https when TLS is installed. Caddy's admin Service is internal and restricted by network policy. Sandbox ingress is limited to the API proxy; sandbox egress is denied. Install and verify runtime classes and storage drivers independently; the base does not pretend to install a cluster-specific runtime or CSI driver.

## Verification

Run `go test -race ./... -count=1`, `go vet ./...`, and `go build ./cmd/... ./cli/...`.

The API integration suite requires TEST_DATABASE_URL pointing to a disposable PostgreSQL database and skips when it is absent. CI provisions PostgreSQL 16 for it. Unit coverage includes authentication, warm-pool compatibility, cron/DST handling, cancellation, pod security, route construction, and webhook signatures/destination restrictions.

This editing environment cannot access Docker or bind a PostgreSQL socket. Database-backed API tests, live Caddy traffic, Kubernetes isolation, GPU execution, CSI snapshots, and CRIU recovery have not been verified here. Before public deployment, run those checks on your infrastructure, including tenant-crossing attempts and cancellation of a real long-running process. No startup-latency benchmark is claimed.

## Follow-up review

The second review added a post-insert idempotency conflict check, timeout handling for expired approval waits, validation of task retry/concurrency limits, propagation of task-definition lookup failures, and rejection of additional reserved webhook address ranges. Race-enabled unit tests, vet, and builds passed with VCS stamping disabled for the prepared copy. PostgreSQL 18 standalone execution accepted the schema and migration SQL; PostgreSQL 16 API integration remains a separate CI check.

Snapshot restore now retains source storage-class metadata and checks readiness and minimum restore capacity. Snapshots created by older builds without this metadata require operator migration. Live verification remains blocked in this session: socket creation returns Operation not permitted, Docker access is denied, and kubectl, GPU devices, and CRIU are unavailable.
