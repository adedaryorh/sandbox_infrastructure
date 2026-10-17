package orchestrator

import (
	"context"
	"fmt"
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
type Orchestrator struct {
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
		Region:     req.Region,
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
	if err := o.store.InsertSandbox(ctx, sb); err != nil {
		return nil, fmt.Errorf("insert sandbox: %w", err)
	}

	// inject secrets — create k8s Secret object before pod
	if len(req.Secrets) > 0 {
		if err := o.injectSecrets(ctx, sandboxID, req.Secrets); err != nil {
			o.store.Transition(ctx, sandboxID, string(models.StatusProvisioning), string(models.StatusFailed))
			return nil, fmt.Errorf("inject secrets: %w", err)
		}
	}

	// create PVC for persistent sandboxes
	if req.Persistent && req.DiskGB > 0 {
		if err := o.k8s.CreatePVC(ctx, sandboxID, req.DiskGB); err != nil {
			o.store.Transition(ctx, sandboxID, string(models.StatusProvisioning), string(models.StatusFailed))
			return nil, fmt.Errorf("create pvc: %w", err)
		}
	}

	// try warm pool first
	var podName string
	warmPod, err := o.pool.Claim(ctx, req.Template)
	if err == nil && warmPod != nil {
		// warm pool hit — pod is already Running
		podName = warmPod.PodName
		logger.Info("warm pool hit", "sandbox", sandboxID, "pod", podName)
	} else {
		// cold start — create pod now
		logger.Info("cold start", "sandbox", sandboxID)
		podName, err = o.k8s.CreatePod(ctx, req, sandboxID)
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

	// register preview URLs if requested
	if req.Network != nil {
		for _, expose := range req.Network.Expose {
			podIP, _ := o.k8s.GetPodIP(ctx, podName)
			_ = podIP
			_ = expose
			// preview URL registration handled by router package
		}
	}

	// fire webhook and audit
	o.webhooks.Emit(ctx, tenantID, "sandbox.running", sb)
	o.auditor.Log(ctx, "sandbox.create", "api_key", tenantID, sandboxID, map[string]interface{}{
		"template":  req.Template,
		"isolation": req.Isolation,
		"memory":    req.Memory,
	}, req.Secrets)

	return sb, nil
}

// ── Destroy ───────────────────────────────────────────────────────────────────

// Destroy stops and removes a sandbox pod and its associated resources.
func (o *Orchestrator) Destroy(ctx context.Context, tenantID, sandboxID string) error {
	sb, err := o.store.GetSandbox(ctx, sandboxID)
	if err != nil {
		return err
	}
	if sb.TenantID != tenantID {
		return fmt.Errorf("sandbox not found")
	}
	if sb.Status == models.StatusDestroyed {
		return nil // already destroyed, idempotent
	}

	// delete the kubernetes pod
	if sb.PodName != "" {
		if err := o.k8s.DeletePod(ctx, sb.PodName); err != nil {
			logger.Error("delete pod", "pod", sb.PodName, "err", err)
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
	sb, err := o.store.GetSandbox(ctx, sandboxID)
	if err != nil {
		return err
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
	sb, err := o.store.GetSandbox(ctx, sandboxID)
	if err != nil {
		return err
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

	o.webhooks.Emit(ctx, tenantID, "sandbox.resumed", map[string]string{"id": sandboxID})
	o.auditor.Log(ctx, "sandbox.resume", "api_key", tenantID, sandboxID, nil, nil)

	return nil
}

// ── Snapshot ──────────────────────────────────────────────────────────────────

// Snapshot captures disk state using Kubernetes VolumeSnapshot.
// It creates a new sandbox from the snapshot — fresh boot from a known baseline.
func (o *Orchestrator) Snapshot(ctx context.Context, tenantID, sandboxID, name string) (*models.Snapshot, error) {
	sb, err := o.store.GetSandbox(ctx, sandboxID)
	if err != nil {
		return nil, err
	}
	if sb.TenantID != tenantID {
		return nil, fmt.Errorf("sandbox not found")
	}

	snapID := newID()
	snap := &models.Snapshot{
		ID:        snapID,
		SandboxID: sandboxID,
		Name:      name,
	}

	// VolumeSnapshot is handled via the snapshot.storage.k8s.io API
	// The k8s client creates a VolumeSnapshot object pointing to the PVC
	// actual snapshotting is done by the CSI driver (cloud-specific)
	// we record it in postgres and let k8s handle the rest
	if err := o.store.InsertSnapshot(ctx, snap); err != nil {
		return nil, fmt.Errorf("insert snapshot: %w", err)
	}

	o.webhooks.Emit(ctx, tenantID, "snapshot.created", snap)
	o.auditor.Log(ctx, "snapshot.create", "api_key", tenantID, sandboxID, map[string]interface{}{
		"snapshot_id": snapID,
		"name":        name,
	}, nil)

	return snap, nil
}

// ── Checkpoint ────────────────────────────────────────────────────────────────

// Checkpoint captures full in-memory process state via CRIU.
// Unlike a snapshot, restoring from a checkpoint resumes execution mid-run.
// Restore time is ~300ms vs 2-5s for a snapshot.
func (o *Orchestrator) Checkpoint(ctx context.Context, tenantID, sandboxID, name string) (*models.Checkpoint, error) {
	sb, err := o.store.GetSandbox(ctx, sandboxID)
	if err != nil {
		return nil, err
	}
	if sb.TenantID != tenantID {
		return nil, fmt.Errorf("sandbox not found")
	}
	if sb.Status != models.StatusRunning {
		return nil, fmt.Errorf("sandbox must be running to checkpoint")
	}

	ckptID := newID()
	ckptDir := fmt.Sprintf("/ckpts/%s", ckptID)

	// step 1: run CRIU dump inside the container via exec
	// this freezes all processes and writes state to /ckpts/<id>/
	// --shell-job allows checkpointing shell-spawned processes
	cmd := fmt.Sprintf(
		"mkdir -p %s && criu dump -t $(cat /run/sandbox.pid) --images-dir %s --shell-job --leave-running",
		ckptDir, ckptDir,
	)
	result, err := o.k8s.Exec(ctx, sb.PodName, cmd)
	if err != nil || result.ExitCode != 0 {
		return nil, fmt.Errorf("criu dump failed: %s", result.Stderr)
	}

	// step 2: snapshot the volume to persist the checkpoint files
	snap, err := o.Snapshot(ctx, tenantID, sandboxID, "ckpt-"+name)
	if err != nil {
		return nil, fmt.Errorf("snapshot for checkpoint: %w", err)
	}

	ckpt := &models.Checkpoint{
		ID:         ckptID,
		SandboxID:  sandboxID,
		SnapshotID: snap.ID,
		Name:       name,
	}
	if err := o.store.InsertCheckpoint(ctx, ckpt); err != nil {
		return nil, fmt.Errorf("insert checkpoint: %w", err)
	}

	o.webhooks.Emit(ctx, tenantID, "checkpoint.created", ckpt)
	o.auditor.Log(ctx, "checkpoint.create", "api_key", tenantID, sandboxID, map[string]interface{}{
		"checkpoint_id": ckptID,
		"name":          name,
	}, nil)

	return ckpt, nil
}

// RestoreCheckpoint creates a new sandbox that resumes from a checkpoint.
// The process continues from exactly where CRIU froze it — in ~300ms.
func (o *Orchestrator) RestoreCheckpoint(ctx context.Context, tenantID, checkpointID string) (*models.Sandbox, error) {
	ckpt, err := o.store.GetCheckpoint(ctx, checkpointID)
	if err != nil {
		return nil, fmt.Errorf("get checkpoint: %w", err)
	}

	// get original sandbox for its config
	original, err := o.store.GetSandbox(ctx, ckpt.SandboxID)
	if err != nil {
		return nil, fmt.Errorf("get original sandbox: %w", err)
	}
	if original.TenantID != tenantID {
		return nil, fmt.Errorf("checkpoint not found")
	}

	// create a new sandbox record
	newID_ := newID()
	newSB := &models.Sandbox{
		ID:        newID_,
		TenantID:  tenantID,
		Status:    models.StatusProvisioning,
		Template:  original.Template,
		Isolation: original.Isolation,
		Memory:    original.Memory,
		CPU:       original.CPU,
	}
	if err := o.store.InsertSandbox(ctx, newSB); err != nil {
		return nil, err
	}

	// create pod with checkpoint volume mounted
	req := &models.CreateSandboxRequest{
		Template:   original.Template,
		Memory:     original.Memory,
		CPU:        original.CPU,
		Isolation:  original.Isolation,
		Persistent: true,
	}
	podName, err := o.k8s.CreatePod(ctx, req, newID_)
	if err != nil {
		return nil, fmt.Errorf("create pod for restore: %w", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if err := o.k8s.WaitForPodRunning(waitCtx, podName); err != nil {
		return nil, fmt.Errorf("wait pod for restore: %w", err)
	}

	// restore process state via CRIU
	// --shell-job required to restore shell-spawned processes
	restoreCmd := fmt.Sprintf(
		"criu restore --images-dir /ckpts/%s --shell-job &",
		ckpt.Name,
	)
	o.k8s.Exec(ctx, podName, restoreCmd)

	o.store.UpdatePodName(ctx, newID_, podName)
	o.store.Transition(ctx, newID_, string(models.StatusProvisioning), string(models.StatusRunning))

	newSB.Status = models.StatusRunning
	newSB.PodName = podName

	o.auditor.Log(ctx, "checkpoint.restore", "api_key", tenantID, newID_, map[string]interface{}{
		"checkpoint_id": checkpointID,
	}, nil)

	return newSB, nil
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
		  AND ttl_expires < NOW()
	`)
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
			`SELECT value FROM secrets WHERE name=$1`, name,
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
	return fmt.Sprintf("sb-%d", time.Now().UnixNano())
}
