package orchestrator

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/yourorg/sandboxing/internal/audit"
	"github.com/yourorg/sandboxing/internal/provider"
	"github.com/yourorg/sandboxing/internal/store"
	"github.com/yourorg/sandboxing/internal/webhook"
	"github.com/yourorg/sandboxing/pkg/logger"
	"github.com/yourorg/sandboxing/pkg/models"
)

// Orchestrator manages the full lifecycle of sandbox pods.
// It sits between the REST API and the Kubernetes client.
type PreviewRouter interface {
	RegisterPorts(string, string, []models.PortExpose) ([]string, error)
	Deregister(string) error
}
type Orchestrator struct {
	router             PreviewRouter
	LocalRegion        string
	CheckpointsEnabled bool

	store    *store.DB
	k8s      provider.Provider
	webhooks *webhook.Emitter
	auditor  *audit.Auditor
	pool     *WarmPool
}

func New(
	store *store.DB,
	provider provider.Provider,
	webhooks *webhook.Emitter,
	auditor *audit.Auditor,
	pool *WarmPool,
) *Orchestrator {
	return &Orchestrator{
		store:    store,
		k8s:      provider,
		webhooks: webhooks,
		auditor:  auditor,
		pool:     pool,
	}
}

// ── Create ────────────────────────────────────────────────────────────────────

// Create provisions a new sandbox.
// It checks the warm pool first for a sub-100ms start.
// Falls back to a cold start if the pool is exhausted.
func (o *Orchestrator) Create(ctx context.Context, tenantID string, req *models.CreateSandboxRequest) (*models.Sandbox, error) {
	if existing, e := models.Tenant(ctx); e == nil && existing != tenantID {
		return nil, fmt.Errorf("tenant mismatch")
	}
	ctx = models.WithTenant(ctx, tenantID)
	// apply defaults
	if req.Memory == "" {
		req.Memory = "512mb"
	}
	if req.CPU == "" {
		req.CPU = "0.5"
	}
	if req.Isolation == "" {
		req.Isolation = models.IsolationStandard
	}
	if req.Region == "" {
		req.Region = "default"
	}

	if err := ValidateRequest(req); err != nil {
		return nil, err
	}
	sandboxID := newID()

	sb := &models.Sandbox{
		ID:         sandboxID,
		TenantID:   tenantID,
		Status:     models.StatusProvisioning,
		Template:   req.Template,
		Isolation:  req.Isolation,
		Memory:     req.Memory,
		CPU:        req.CPU,
		Persistent: req.Persistent,
		DiskGB:     req.DiskGB,
		Secrets:    req.Secrets,
		Region:     o.region(),
	}

	// set TTL expiry if requested
	if req.TTL != "" {
		d, err := time.ParseDuration(req.TTL)
		if err == nil {
			t := time.Now().Add(d)
			sb.TTLExpires = &t
		}
	}

	// write initial record to postgres before touching k8s
	if err := o.store.ReserveSandbox(ctx, sb); err != nil {
		return nil, fmt.Errorf("insert sandbox: %w", err)
	}

	var podName string
	created := false
	defer func() {
		if !created {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			if podName != "" {
				o.k8s.DeletePod(cleanup, podName)
			}
			o.k8s.DeleteSecret(cleanup, "secrets-"+sandboxID)
			if req.Persistent {
				o.k8s.DeletePVC(cleanup, sandboxID)
			}
			o.store.Transition(cleanup, sandboxID, string(models.StatusProvisioning), string(models.StatusFailed))
		}
	}()
	// inject secrets — create k8s Secret object before pod
	if len(req.Secrets) > 0 {
		if err := o.injectSecrets(ctx, sandboxID, req.Secrets); err != nil {
			o.store.Transition(ctx, sandboxID, string(models.StatusProvisioning), string(models.StatusFailed))
			return nil, fmt.Errorf("inject secrets: %w", err)
		}
	}

	if err := o.store.SaveRuntimeConfig(ctx, sandboxID, req); err != nil {
		return nil, err
	}

	// create PVC for persistent sandboxes
	if req.Persistent && req.DiskGB > 0 {
		if err := o.k8s.CreatePVC(ctx, sandboxID, req.DiskGB); err != nil {
			o.store.Transition(ctx, sandboxID, string(models.StatusProvisioning), string(models.StatusFailed))
			return nil, fmt.Errorf("create pvc: %w", err)
		}
	}

	// Only claim never-used pods whose complete configuration matches the request.
	var warmPod *store.WarmPod
	var err error
	if o.pool != nil && WarmCompatible(req) {
		warmPod, err = o.pool.Claim(ctx, req.Template)
	}
	if err == nil && warmPod != nil {
		// warm pool hit — pod is already Running
		podName = warmPod.PodName
		logger.Info("warm pool hit", "sandbox", sandboxID, "pod", podName)
	} else {
		// cold start — create pod now
		logger.Info("cold start", "sandbox", sandboxID)
		podName, err = o.k8s.CreatePod(ctx, req, sandboxID)
		if err == nil {
			o.store.UpdatePodName(ctx, sandboxID, podName)
		}
		if err != nil {
			o.store.Transition(ctx, sandboxID, string(models.StatusProvisioning), string(models.StatusFailed))
			return nil, fmt.Errorf("create pod: %w", err)
		}

		// wait until pod is Running (blocks until ready)
		waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		if err := o.k8s.WaitForPodRunning(waitCtx, podName); err != nil {
			o.store.Transition(ctx, sandboxID, string(models.StatusProvisioning), string(models.StatusFailed))
			return nil, fmt.Errorf("wait pod running: %w", err)
		}
	}

	// update pod name and transition to running
	o.store.UpdatePodName(ctx, sandboxID, podName)
	if err := o.store.Transition(ctx, sandboxID, string(models.StatusProvisioning), string(models.StatusRunning)); err != nil {
		return nil, fmt.Errorf("transition to running: %w", err)
	}

	sb.Status = models.StatusRunning
	sb.PodName = podName

	if req.Network != nil && len(req.Network.Expose) > 0 {
		if o.router == nil {
			return nil, fmt.Errorf("preview routing is not configured")
		}
		ip, e := o.k8s.GetPodIP(ctx, podName)
		if e != nil {
			return nil, e
		}
		urls, e := o.router.RegisterPorts(sandboxID, ip, req.Network.Expose)
		if e != nil {
			return nil, e
		}
		sb.PreviewURLs = urls
		if e = o.store.SavePreviewURLs(ctx, sandboxID, urls); e != nil {
			o.router.Deregister(sandboxID)
			return nil, e
		}
	}

	// fire webhook and audit
	o.webhooks.Emit(ctx, tenantID, "sandbox.running", sb)
	o.auditor.Log(ctx, "sandbox.create", "api_key", tenantID, sandboxID, map[string]interface{}{
		"template":  req.Template,
		"isolation": req.Isolation,
		"memory":    req.Memory,
	}, req.Secrets)

	created = true
	return sb, nil
}

// ── Destroy ───────────────────────────────────────────────────────────────────

// Destroy stops and removes a sandbox pod and its associated resources.
func (o *Orchestrator) Destroy(ctx context.Context, tenantID, sandboxID string) error {
	if existing, e := models.Tenant(ctx); e == nil && existing != tenantID {
		return fmt.Errorf("tenant mismatch")
	}
	ctx = models.WithTenant(ctx, tenantID)
	sb, err := o.store.GetSandbox(ctx, sandboxID)
	if err != nil {
		return err
	}
	if sb.Region != o.region() {
		return fmt.Errorf("sandbox belongs to another regional control plane")
	}
	if sb.TenantID != tenantID {
		return fmt.Errorf("sandbox not found")
	}
	if sb.Status == models.StatusDestroyed {
		return nil // already destroyed, idempotent
	}

	// Stop workload before attempting external routing cleanup.
	// delete the kubernetes pod
	if sb.PodName != "" {
		if err := o.k8s.DeletePod(ctx, sb.PodName); err != nil {
			return fmt.Errorf("delete workload: %w", err)
		}
	}

	if o.router != nil {
		if e := o.router.Deregister(sandboxID); e != nil {
			o.store.MarkRouteCleanup(ctx, sandboxID, true)
		}
	}
	// clean up k8s secret
	o.k8s.DeleteSecret(ctx, "secrets-"+sandboxID)

	// delete PVC only for non-persistent sandboxes
	// persistent sandboxes keep their volume unless explicitly deleted
	if !sb.Persistent {
		o.k8s.DeletePVC(ctx, sandboxID)
	}

	// transition state
	if err := o.store.Transition(ctx, sandboxID, string(sb.Status), string(models.StatusDestroyed)); err != nil {
		return fmt.Errorf("transition destroyed: %w", err)
	}

	o.webhooks.Emit(ctx, tenantID, "sandbox.destroyed", map[string]string{"id": sandboxID})
	o.auditor.Log(ctx, "sandbox.destroy", "api_key", tenantID, sandboxID, nil, nil)

	return nil
}

// ── Pause ─────────────────────────────────────────────────────────────────────

// Pause stops billing by stopping the pod but keeping the volume attached.
// The sandbox can be resumed later — all files and installed packages survive.
func (o *Orchestrator) Pause(ctx context.Context, tenantID, sandboxID string) error {
	if existing, e := models.Tenant(ctx); e == nil && existing != tenantID {
		return fmt.Errorf("tenant mismatch")
	}
	ctx = models.WithTenant(ctx, tenantID)
	sb, err := o.store.GetSandbox(ctx, sandboxID)
	if err != nil {
		return err
	}
	if sb.Region != o.region() {
		return fmt.Errorf("sandbox belongs to another regional control plane")
	}
	if sb.TenantID != tenantID {
		return fmt.Errorf("sandbox not found")
	}
	if sb.Status != models.StatusRunning {
		return fmt.Errorf("sandbox must be running to pause, current status: %s", sb.Status)
	}

	// delete the pod — this stops CPU/memory billing
	// the PVC (if persistent) stays attached and survives
	if sb.PodName != "" {
		if err := o.k8s.DeletePod(ctx, sb.PodName); err != nil {
			return fmt.Errorf("delete pod for pause: %w", err)
		}
	}

	if o.router != nil {
		if e := o.router.Deregister(sandboxID); e != nil {
			o.store.MarkRouteCleanup(ctx, sandboxID, true)
		}
	}
	if err := o.store.Transition(ctx, sandboxID, string(models.StatusRunning), string(models.StatusPaused)); err != nil {
		return err
	}

	o.webhooks.Emit(ctx, tenantID, "sandbox.paused", map[string]string{"id": sandboxID})
	o.auditor.Log(ctx, "sandbox.pause", "api_key", tenantID, sandboxID, nil, nil)

	return nil
}

// ── Resume ────────────────────────────────────────────────────────────────────

// Resume starts a paused sandbox by creating a new pod.
// For persistent sandboxes the volume is re-attached — all files are intact.
func (o *Orchestrator) Resume(ctx context.Context, tenantID, sandboxID string) error {
	if existing, e := models.Tenant(ctx); e == nil && existing != tenantID {
		return fmt.Errorf("tenant mismatch")
	}
	ctx = models.WithTenant(ctx, tenantID)
	sb, err := o.store.GetSandbox(ctx, sandboxID)
	if err != nil {
		return err
	}
	if sb.Region != o.region() {
		return fmt.Errorf("sandbox belongs to another regional control plane")
	}
	if sb.TenantID != tenantID {
		return fmt.Errorf("sandbox not found")
	}
	if sb.Status != models.StatusPaused {
		return fmt.Errorf("sandbox must be paused to resume, current status: %s", sb.Status)
	}

	if err := o.store.Transition(ctx, sandboxID, string(models.StatusPaused), string(models.StatusResuming)); err != nil {
		return err
	}

	// rebuild the create request from stored sandbox data
	req := &models.CreateSandboxRequest{
		Template:   sb.Template,
		Memory:     sb.Memory,
		CPU:        sb.CPU,
		Isolation:  sb.Isolation,
		Persistent: sb.Persistent,
		DiskGB:     sb.DiskGB,
		Secrets:    sb.Secrets,
	}

	if saved, e := o.store.RuntimeConfig(ctx, sandboxID); e == nil {
		req = saved
	} else {
		return e
	}

	// create a fresh pod — PVC re-attaches automatically
	podName, err := o.k8s.CreatePod(ctx, req, sandboxID)
	if err != nil {
		o.store.Transition(ctx, sandboxID, string(models.StatusResuming), string(models.StatusFailed))
		return fmt.Errorf("create pod for resume: %w", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if err := o.k8s.WaitForPodRunning(waitCtx, podName); err != nil {
		o.store.Transition(ctx, sandboxID, string(models.StatusResuming), string(models.StatusFailed))
		return fmt.Errorf("wait pod running on resume: %w", err)
	}

	o.store.UpdatePodName(ctx, sandboxID, podName)
	if err := o.store.Transition(ctx, sandboxID, string(models.StatusResuming), string(models.StatusRunning)); err != nil {
		return err
	}

	if req.Network != nil && len(req.Network.Expose) > 0 {
		if o.router == nil {
			return fmt.Errorf("preview routing unavailable")
		}
		ip, e := o.k8s.GetPodIP(ctx, podName)
		if e != nil {
			return e
		}
		urls, e := o.router.RegisterPorts(sandboxID, ip, req.Network.Expose)
		if e != nil {
			return e
		}
		if e = o.store.SavePreviewURLs(ctx, sandboxID, urls); e != nil {
			return e
		}
	}
	o.webhooks.Emit(ctx, tenantID, "sandbox.resumed", map[string]string{"id": sandboxID})
	o.auditor.Log(ctx, "sandbox.resume", "api_key", tenantID, sandboxID, nil, nil)

	return nil
}

// ── Snapshot ──────────────────────────────────────────────────────────────────

// Snapshot captures disk state using Kubernetes VolumeSnapshot.
// It creates a new sandbox from the snapshot — fresh boot from a known baseline.

func (o *Orchestrator) Snapshot(ctx context.Context, tenant, id, name string) (*models.Snapshot, error) {
	if current, e := models.Tenant(ctx); e == nil && current != tenant {
		return nil, fmt.Errorf("tenant mismatch")
	}
	ctx = models.WithTenant(ctx, tenant)
	sb, e := o.store.GetSandbox(ctx, id)
	if e != nil {
		return nil, e
	}
	if !sb.Persistent || sb.DiskGB < 1 {
		return nil, fmt.Errorf("snapshots require a persistent workspace")
	}
	backend, ok := o.k8s.(provider.Snapshotter)
	if !ok {
		return nil, fmt.Errorf("this backend does not support volume snapshots")
	}
	if len(name) < 1 || len(name) > 120 {
		return nil, fmt.Errorf("snapshot name must be 1-120 characters")
	}
	snapshot := &models.Snapshot{ID: models.NewID("snapshot"), SandboxID: id, Name: name}
	snapshot.VolumeSnapshot = snapshot.ID
	size, e := backend.CreateVolumeSnapshot(ctx, "pvc-"+id, snapshot.VolumeSnapshot)
	if e != nil {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		backend.DeleteVolumeSnapshot(cleanup, snapshot.VolumeSnapshot)
		return nil, e
	}
	snapshot.SizeBytes = size
	if e = o.store.InsertSnapshot(ctx, snapshot); e != nil {
		backend.DeleteVolumeSnapshot(context.WithoutCancel(ctx), snapshot.VolumeSnapshot)
		return nil, e
	}
	o.auditor.Log(ctx, "snapshot.create", "api_key", tenant, id, map[string]any{"snapshot_id": snapshot.ID}, nil)
	o.webhooks.Emit(ctx, tenant, "snapshot.created", snapshot)
	return snapshot, nil
}

// Checkpoints are explicitly gated: CRIU/kernel/image support must be qualified by the operator.
// The workload writes the non-init process PID to /workspace/.checkpoint.pid.
func (o *Orchestrator) Checkpoint(ctx context.Context, tenant, id, name string) (*models.Checkpoint, error) {
	if !o.CheckpointsEnabled {
		return nil, fmt.Errorf("checkpoint runtime is disabled; qualify CRIU support before enabling ENABLE_CHECKPOINTS")
	}
	ctx = models.WithTenant(ctx, tenant)
	sb, e := o.store.GetSandbox(ctx, id)
	if e != nil {
		return nil, e
	}
	if sb.Status != models.StatusRunning || !sb.Persistent {
		return nil, fmt.Errorf("checkpoint requires a running persistent sandbox")
	}
	probe, e := o.k8s.Exec(ctx, sb.PodName, "criu check --unprivileged")
	if e != nil || probe == nil || probe.ExitCode != 0 {
		return nil, fmt.Errorf("runtime cannot checkpoint unprivileged processes")
	}
	raw, e := o.k8s.ReadFile(ctx, sb.PodName, "/workspace/.checkpoint.pid")
	if e != nil {
		return nil, fmt.Errorf("workload must publish /workspace/.checkpoint.pid")
	}
	pid, e := strconv.Atoi(strings.TrimSpace(string(raw)))
	if e != nil || pid < 2 {
		return nil, fmt.Errorf("checkpoint target must be a non-init process")
	}
	ck := &models.Checkpoint{ID: models.NewID("checkpoint"), SandboxID: id, Name: name}
	dir := "/workspace/.checkpoints/" + ck.ID
	result, e := o.k8s.Exec(ctx, sb.PodName, fmt.Sprintf("mkdir -p %s && criu dump --unprivileged --tree %d --images-dir %s --shell-job --leave-running && sync", dir, pid, dir))
	if e != nil || result == nil || result.ExitCode != 0 {
		return nil, fmt.Errorf("CRIU dump failed; no checkpoint was recorded")
	}
	snapshot, e := o.Snapshot(ctx, tenant, id, "checkpoint-"+ck.ID)
	if e != nil {
		return nil, e
	}
	ck.SnapshotID = snapshot.ID
	if e = o.store.InsertCheckpoint(ctx, ck); e != nil {
		return nil, e
	}
	o.auditor.Log(ctx, "checkpoint.create", "api_key", tenant, id, map[string]any{"checkpoint_id": ck.ID, "pid": pid}, nil)
	return ck, nil
}
func (o *Orchestrator) RestoreCheckpoint(ctx context.Context, tenant, id string) (*models.Sandbox, error) {
	return o.RestoreCheckpointWithInput(ctx, tenant, id, nil)
}
func (o *Orchestrator) RestoreCheckpointWithInput(ctx context.Context, tenant, id string, input []byte) (sb *models.Sandbox, err error) {
	if !o.CheckpointsEnabled {
		return nil, fmt.Errorf("checkpoint runtime is disabled")
	}
	ctx = models.WithTenant(ctx, tenant)
	checkpoint, e := o.store.GetCheckpoint(ctx, id)
	if e != nil {
		return nil, e
	}
	snapshot, e := o.store.GetSnapshot(ctx, checkpoint.SnapshotID)
	if e != nil {
		return nil, e
	}
	if snapshot.VolumeSnapshot == "" {
		return nil, fmt.Errorf("legacy checkpoint has no actual volume snapshot")
	}
	original, e := o.store.GetSandbox(ctx, checkpoint.SandboxID)
	if e != nil {
		return nil, e
	}
	req, e := o.store.RuntimeConfig(ctx, original.ID)
	if e != nil {
		return nil, e
	}
	backend, ok := o.k8s.(provider.Snapshotter)
	if !ok {
		return nil, fmt.Errorf("backend cannot restore snapshots")
	}
	sb = &models.Sandbox{ID: models.NewID("sb"), TenantID: tenant, Status: models.StatusProvisioning, Template: req.Template, Memory: req.Memory, CPU: req.CPU, Isolation: req.Isolation, Persistent: true, DiskGB: original.DiskGB, Secrets: req.Secrets, Region: req.Region}
	if e = o.store.ReserveSandbox(ctx, sb); e != nil {
		return nil, e
	}
	completed := false
	defer func() {
		if !completed {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			if sb != nil && sb.PodName != "" {
				o.k8s.DeletePod(cleanup, sb.PodName)
			}
			o.k8s.DeletePVC(cleanup, sb.ID)
			o.k8s.DeleteSecret(cleanup, "secrets-"+sb.ID)
			o.store.Transition(cleanup, sb.ID, string(models.StatusProvisioning), string(models.StatusFailed))
		}
	}()
	req.Persistent = true
	req.DiskGB = original.DiskGB
	if e = o.store.SaveRuntimeConfig(ctx, sb.ID, req); e != nil {
		return sb, e
	}
	if e = backend.RestoreVolume(ctx, sb.ID, snapshot.VolumeSnapshot, sb.DiskGB); e != nil {
		return sb, e
	}
	if len(req.Secrets) > 0 {
		if e = o.injectSecrets(ctx, sb.ID, req.Secrets); e != nil {
			return sb, e
		}
	}
	sb.PodName, e = o.k8s.CreatePod(ctx, req, sb.ID)
	if e != nil {
		return sb, e
	}
	o.store.UpdatePodName(ctx, sb.ID, sb.PodName)
	wait, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if e = o.k8s.WaitForPodRunning(wait, sb.PodName); e != nil {
		return sb, e
	}
	if len(input) > 0 {
		if e = o.k8s.WriteFile(ctx, sb.PodName, "/task/resume.json", input); e != nil {
			return sb, e
		}
	}
	result, e := o.k8s.Exec(ctx, sb.PodName, "criu restore --unprivileged --images-dir /workspace/.checkpoints/"+checkpoint.ID+" --shell-job --restore-detached")
	if e != nil || result == nil || result.ExitCode != 0 {
		return sb, fmt.Errorf("CRIU restore failed; sandbox is not marked running")
	}
	if e = o.store.Transition(ctx, sb.ID, string(models.StatusProvisioning), string(models.StatusRunning)); e != nil {
		return sb, e
	}
	sb.Status = models.StatusRunning
	completed = true
	o.auditor.Log(ctx, "checkpoint.restore", "api_key", tenant, sb.ID, map[string]any{"checkpoint_id": id}, nil)
	return sb, nil
}

// ── TTL expiry ────────────────────────────────────────────────────────────────

// ExpireTTLSandboxes finds sandboxes past their TTL and destroys them.
// Runs as a background goroutine.
func (o *Orchestrator) ExpireTTLSandboxes(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.expireOnce(ctx)
		}
	}
}

func (o *Orchestrator) expireOnce(ctx context.Context) {
	// find sandboxes whose TTL has passed
	rows, err := o.store.Raw().QueryContext(ctx, `
		SELECT id, tenant_id FROM sandboxes
		WHERE status='running'
		  AND ttl_expires IS NOT NULL
		  AND ttl_expires < NOW() AND region=$1
	`, o.region())
	if err != nil {
		logger.Error("expire query", "err", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var id, tenantID string
		rows.Scan(&id, &tenantID)
		logger.Info("expiring sandbox", "id", id)
		if err := o.Destroy(ctx, tenantID, id); err != nil {
			logger.Error("expire destroy", "id", id, "err", err)
		}
	}
}

// ── TTL warning ───────────────────────────────────────────────────────────────

// WarnTTLSandboxes fires a webhook 5 minutes before TTL expiry.
func (o *Orchestrator) WarnTTLSandboxes(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rows, err := o.store.Raw().QueryContext(ctx, `
				SELECT id, tenant_id FROM sandboxes
				WHERE status='running'
				  AND ttl_expires IS NOT NULL
				  AND ttl_expires BETWEEN NOW() AND NOW() + INTERVAL '5 minutes'
				  AND id NOT IN (
				    SELECT sandbox_id FROM audit_log
				    WHERE action='sandbox.ttl_warning'
				      AND created_at > NOW() - INTERVAL '10 minutes'
				  )
			`)
			if err != nil {
				continue
			}
			defer rows.Close()
			for rows.Next() {
				var id, tenantID string
				rows.Scan(&id, &tenantID)
				o.webhooks.Emit(ctx, tenantID, "sandbox.ttl_warning", map[string]string{"id": id})
				o.auditor.Log(ctx, "sandbox.ttl_warning", "system", tenantID, id, nil, nil)
			}
		}
	}
}

// ── Secrets ───────────────────────────────────────────────────────────────────

// injectSecrets fetches secret values from the internal store and creates
// a Kubernetes Secret object. Values are never logged or returned in API responses.
func (o *Orchestrator) injectSecrets(ctx context.Context, sandboxID string, secretNames []string) error {
	// In production: fetch values from Vault, AWS Secrets Manager, etc.
	// Here we read from our internal secrets table.
	values, err := o.fetchSecretValues(ctx, secretNames)
	if err != nil {
		return err
	}

	secretName := "secrets-" + sandboxID
	return o.k8s.CreateSecret(ctx, secretName, values)
}

func (o *Orchestrator) fetchSecretValues(ctx context.Context, names []string) (map[string]string, error) {
	result := make(map[string]string)
	for _, name := range names {
		var value string
		err := o.store.Raw().QueryRowContext(ctx,
			`SELECT value FROM secrets WHERE name=$1 AND tenant_id=$2`, name, tenantFor(ctx),
		).Scan(&value)
		if err != nil {
			return nil, fmt.Errorf("secret %q not found", name)
		}
		result[name] = value
	}
	return result, nil
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func newID() string {
	return models.NewID("sb")
}

func tenantFor(ctx context.Context) string { v, _ := models.Tenant(ctx); return v }

func (o *Orchestrator) SetRouter(r PreviewRouter) { o.router = r }

func (o *Orchestrator) ReconcileRoutes(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if o.router == nil {
				continue
			}
			rows, err := o.store.Raw().QueryContext(ctx, "select id,tenant_id from sandboxes where route_cleanup_pending=true and status in ('destroyed','paused','failed')")
			if err != nil {
				continue
			}
			type pending struct{ id, tenant string }
			items := []pending{}
			for rows.Next() {
				var v pending
				if rows.Scan(&v.id, &v.tenant) == nil {
					items = append(items, v)
				}
			}
			rows.Close()
			for _, v := range items {
				if o.router.Deregister(v.id) == nil {
					o.store.MarkRouteCleanup(models.WithTenant(ctx, v.tenant), v.id, false)
				}
			}
		}
	}
}

func (o *Orchestrator) region() string {
	if o.LocalRegion == "" {
		return "default"
	}
	return o.LocalRegion
}
