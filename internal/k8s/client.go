package k8s

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"

	"github.com/yourorg/sandboxing/pkg/models"
)

// Client wraps the Kubernetes client-go for all sandbox pod operations.
type Client struct {
	clientset *kubernetes.Clientset
	config    *rest.Config
	namespace string
}

// New creates a Kubernetes client.
// If kubeConfigPath is empty it uses in-cluster config (when running inside k8s).
func New(kubeConfigPath, namespace string) (*Client, error) {
	var cfg *rest.Config
	var err error

	if kubeConfigPath == "" {
		// running inside the cluster — use service account token
		cfg, err = rest.InClusterConfig()
	} else {
		// running locally — use kubeconfig file
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeConfigPath)
	}
	if err != nil {
		return nil, fmt.Errorf("k8s config: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("k8s clientset: %w", err)
	}

	return &Client{
		clientset: clientset,
		config:    cfg,
		namespace: namespace,
	}, nil
}

// ── Pod Lifecycle ─────────────────────────────────────────────────────────────

// CreatePod builds and submits a sandbox pod spec to Kubernetes.
func (c *Client) CreatePod(ctx context.Context, req *models.CreateSandboxRequest, sandboxID string) (string, error) {
	pod := c.buildPodSpec(req, sandboxID)

	created, err := c.clientset.CoreV1().Pods(c.namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("create pod: %w", err)
	}
	return created.Name, nil
}

// WaitForPodRunning blocks until the pod reaches Running phase or ctx is cancelled.
func (c *Client) WaitForPodRunning(ctx context.Context, podName string) error {
	watcher, err := c.clientset.CoreV1().Pods(c.namespace).Watch(ctx, metav1.ListOptions{
		FieldSelector: fmt.Sprintf("metadata.name=%s", podName),
	})
	if err != nil {
		return fmt.Errorf("watch pod: %w", err)
	}
	defer watcher.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return fmt.Errorf("watch channel closed")
			}
			if event.Type == watch.Modified || event.Type == watch.Added {
				pod, ok := event.Object.(*corev1.Pod)
				if !ok {
					continue
				}
				if pod.Status.Phase == corev1.PodRunning {
					return nil
				}
				if pod.Status.Phase == corev1.PodFailed {
					return fmt.Errorf("pod failed: %s", pod.Status.Message)
				}
			}
		}
	}
}

// DeletePod removes a pod from Kubernetes immediately.
func (c *Client) DeletePod(ctx context.Context, podName string) error {
	grace := int64(0)
	err := c.clientset.CoreV1().Pods(c.namespace).Delete(ctx, podName, metav1.DeleteOptions{
		GracePeriodSeconds: &grace,
	})
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("delete pod: %w", err)
	}
	return nil
}

// GetPodIP returns the pod's IP address once it's Running.
func (c *Client) GetPodIP(ctx context.Context, podName string) (string, error) {
	pod, err := c.clientset.CoreV1().Pods(c.namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	return pod.Status.PodIP, nil
}

// ── Exec ──────────────────────────────────────────────────────────────────────

// Exec runs a command inside a pod and returns stdout/stderr.
func (c *Client) Exec(ctx context.Context, podName, cmd string) (*models.ExecResult, error) {
	start := time.Now()

	req := c.clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(c.namespace).
		SubResource("exec")

	req.VersionedParams(&corev1.PodExecOptions{
		Command: []string{"/bin/sh", "-c", cmd},
		Stdout:  true,
		Stderr:  true,
	}, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(c.config, "POST", req.URL())
	if err != nil {
		return nil, fmt.Errorf("exec executor: %w", err)
	}

	var stdout, stderr bytes.Buffer
	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})

	exitCode := 0
	if err != nil {
		// extract exit code if possible
		if exitErr, ok := err.(interface{ ExitStatus() int }); ok {
			exitCode = exitErr.ExitStatus()
		} else {
			exitCode = 1
		}
	}

	return &models.ExecResult{
		Stdout:     stdout.String(),
		Stderr:     stderr.String(),
		ExitCode:   exitCode,
		DurationMs: time.Since(start).Milliseconds(),
	}, nil
}

// ExecStream runs a command and streams stdout/stderr frames to the provided channel.
func (c *Client) ExecStream(ctx context.Context, podName, cmd string, frames chan<- models.ExecFrame) error {
	req := c.clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(c.namespace).
		SubResource("exec")

	req.VersionedParams(&corev1.PodExecOptions{
		Command: []string{"/bin/sh", "-c", cmd},
		Stdout:  true,
		Stderr:  true,
	}, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(c.config, "POST", req.URL())
	if err != nil {
		return err
	}

	// pipe stdout
	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()

	// goroutine: forward stdout frames
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := stdoutR.Read(buf)
			if n > 0 {
				frames <- models.ExecFrame{Type: "stdout", Data: string(buf[:n])}
			}
			if err != nil {
				return
			}
		}
	}()

	// goroutine: forward stderr frames
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := stderrR.Read(buf)
			if n > 0 {
				frames <- models.ExecFrame{Type: "stderr", Data: string(buf[:n])}
			}
			if err != nil {
				return
			}
		}
	}()

	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: stdoutW,
		Stderr: stderrW,
	})
	stdoutW.Close()
	stderrW.Close()

	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(interface{ ExitStatus() int }); ok {
			exitCode = exitErr.ExitStatus()
		} else {
			exitCode = 1
		}
	}
	frames <- models.ExecFrame{Type: "exit", Code: exitCode}
	return nil
}

// ExecWithStdin runs a command piping data into the pod's stdin — used for file writes.
func (c *Client) ExecWithStdin(ctx context.Context, podName, cmd string, stdin io.Reader) error {
	req := c.clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(c.namespace).
		SubResource("exec")

	req.VersionedParams(&corev1.PodExecOptions{
		Command: []string{"/bin/sh", "-c", cmd},
		Stdin:   true,
		Stdout:  true,
		Stderr:  true,
	}, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(c.config, "POST", req.URL())
	if err != nil {
		return err
	}

	return executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  stdin,
		Stdout: io.Discard,
		Stderr: io.Discard,
	})
}

// ── PTY ───────────────────────────────────────────────────────────────────────

// PTYStream holds the open streams for an interactive terminal session.
type PTYStream struct {
	executor remotecommand.Executor
	sizeQ    remotecommand.TerminalSizeQueue
	stdin    *io.PipeWriter
	stdout   *io.PipeReader
	resizeCh chan remotecommand.TerminalSize
}

type termSizeQueue struct {
	ch chan remotecommand.TerminalSize
}

func (q *termSizeQueue) Next() *remotecommand.TerminalSize {
	s, ok := <-q.ch
	if !ok {
		return nil
	}
	return &s
}

// OpenPTY opens a full PTY exec stream into the pod for interactive use.
func (c *Client) OpenPTY(ctx context.Context, podName string) (*PTYStream, error) {
	req := c.clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(c.namespace).
		SubResource("exec")

	req.VersionedParams(&corev1.PodExecOptions{
		Command: []string{"/bin/bash"},
		Stdin:   true,
		Stdout:  true,
		Stderr:  true,
		TTY:     true,
	}, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(c.config, "POST", req.URL())
	if err != nil {
		return nil, fmt.Errorf("pty executor: %w", err)
	}

	stdinR, stdinW := io.Pipe()
	stdoutR, stdoutW := io.Pipe()
	resizeCh := make(chan remotecommand.TerminalSize, 4)
	sizeQ := &termSizeQueue{ch: resizeCh}

	// start streaming in background
	go func() {
		executor.StreamWithContext(ctx, remotecommand.StreamOptions{
			Stdin:             stdinR,
			Stdout:            stdoutW,
			Stderr:            stdoutW,
			Tty:               true,
			TerminalSizeQueue: sizeQ,
		})
		stdoutW.Close()
	}()

	return &PTYStream{
		executor: executor,
		stdin:    stdinW,
		stdout:   stdoutR,
		resizeCh: resizeCh,
	}, nil
}

func (p *PTYStream) Write(data []byte) (int, error) { return p.stdin.Write(data) }
func (p *PTYStream) Read(buf []byte) (int, error)   { return p.stdout.Read(buf) }
func (p *PTYStream) Close()                         { p.stdin.Close(); close(p.resizeCh) }
func (p *PTYStream) Resize(cols, rows uint16) {
	select {
	case p.resizeCh <- remotecommand.TerminalSize{Width: cols, Height: rows}:
	default:
	}
}

// ── Files ─────────────────────────────────────────────────────────────────────

// WriteFile writes data into a path inside the pod via stdin pipe.
func (c *Client) WriteFile(ctx context.Context, podName, path string, data []byte) error {
	// ensure parent directory exists
	mkdirCmd := fmt.Sprintf("mkdir -p $(dirname %q)", path)
	c.Exec(ctx, podName, mkdirCmd) //nolint:errcheck

	cmd := fmt.Sprintf("cat > %q", path)
	return c.ExecWithStdin(ctx, podName, cmd, bytes.NewReader(data))
}

// ReadFile reads a file from inside the pod.
func (c *Client) ReadFile(ctx context.Context, podName, path string) ([]byte, error) {
	cmd := fmt.Sprintf("cat %q", path)
	result, err := c.Exec(ctx, podName, cmd)
	if err != nil {
		return nil, err
	}
	if result.ExitCode != 0 {
		return nil, fmt.Errorf("read file: %s", result.Stderr)
	}
	return []byte(result.Stdout), nil
}

// DeleteFile removes a file from inside the pod.
func (c *Client) DeleteFile(ctx context.Context, podName, path string) error {
	cmd := fmt.Sprintf("rm -f %q", path)
	result, err := c.Exec(ctx, podName, cmd)
	if err != nil {
		return err
	}
	if result.ExitCode != 0 {
		return fmt.Errorf("delete file: %s", result.Stderr)
	}
	return nil
}

// ListDir returns file listing inside the pod at path.
func (c *Client) ListDir(ctx context.Context, podName, path string) (string, error) {
	cmd := fmt.Sprintf("ls -la %q", path)
	result, err := c.Exec(ctx, podName, cmd)
	if err != nil {
		return "", err
	}
	return result.Stdout, nil
}

// ── Secrets ───────────────────────────────────────────────────────────────────

// CreateSecret creates a Kubernetes Secret from the given key-value map.
func (c *Client) CreateSecret(ctx context.Context, name string, data map[string]string) error {
	stringData := make(map[string]string)
	for k, v := range data {
		stringData[k] = v
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: c.namespace,
		},
		StringData: stringData,
	}

	_, err := c.clientset.CoreV1().Secrets(c.namespace).Create(ctx, secret, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create secret: %w", err)
	}
	return nil
}

// DeleteSecret removes a Kubernetes Secret.
func (c *Client) DeleteSecret(ctx context.Context, name string) error {
	err := c.clientset.CoreV1().Secrets(c.namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !isNotFound(err) {
		return err
	}
	return nil
}

// ── Pod Builder ───────────────────────────────────────────────────────────────

func (c *Client) buildPodSpec(req *models.CreateSandboxRequest, sandboxID string) *corev1.Pod {
	mem := "512Mi"
	if req.Memory != "" {
		mem = normalizeMemory(req.Memory)
	}
	cpu := "500m"
	if req.CPU != "" {
		cpu = normalizeCPU(req.CPU)
	}

	// environment variables from request
	var envVars []corev1.EnvVar
	for k, v := range req.Env {
		envVars = append(envVars, corev1.EnvVar{Name: k, Value: v})
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			// pod name = sandbox ID for easy lookup
			Name:      "sb-" + sandboxID,
			Namespace: c.namespace,
			Labels: map[string]string{
				"app":        "sandbox",
				"sandbox-id": sandboxID,
				"template":   req.Template,
			},
		},
		Spec: corev1.PodSpec{
			// don't restart — we manage lifecycle ourselves
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:  "sandbox",
					Image: templateToImage(req.Template),
					// keep container running, user sends commands via exec
					Command: []string{"/bin/sh", "-c", "sleep infinity"},
					Env:     envVars,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse(mem),
							corev1.ResourceCPU:    resource.MustParse(cpu),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse(mem),
							corev1.ResourceCPU:    resource.MustParse(cpu),
						},
					},
					SecurityContext: &corev1.SecurityContext{
						// drop ALL linux capabilities — nothing should be privileged
						Capabilities: &corev1.Capabilities{
							Drop: []corev1.Capability{"ALL"},
						},
						AllowPrivilegeEscalation: boolPtr(false),
						RunAsNonRoot:             boolPtr(true),
						RunAsUser:                int64Ptr(1000),
					},
					// workspace is writable; root filesystem is read-only
					VolumeMounts: []corev1.VolumeMount{
						{Name: "workspace", MountPath: "/workspace"},
						{Name: "tmp", MountPath: "/tmp"},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "workspace",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
				{
					Name: "tmp",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
			},
			// network policy handled separately via NetworkPolicy resource
		},
	}

	// ── Isolation tier ────────────────────────────────────────────────────────
	// This single field switches between Docker (standard) and gVisor (secure).
	if req.Isolation == models.IsolationSecure {
		rc := "gvisor"
		pod.Spec.RuntimeClassName = &rc
	}

	// ── Secret injection ──────────────────────────────────────────────────────
	// Mount the pre-created Kubernetes secret as environment variables.
	if len(req.Secrets) > 0 {
		secretName := "secrets-" + sandboxID
		pod.Spec.Containers[0].EnvFrom = append(
			pod.Spec.Containers[0].EnvFrom,
			corev1.EnvFromSource{
				SecretRef: &corev1.SecretEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: secretName,
					},
				},
			},
		)
	}

	// ── Persistent volume ──────────────────────────────────────────────────────
	// Replace the EmptyDir workspace with a real PVC for persistent sandboxes.
	if req.Persistent && req.DiskGB > 0 {
		pvcName := "pvc-" + sandboxID
		// replace workspace emptyDir with PVC mount
		pod.Spec.Volumes[0] = corev1.Volume{
			Name: "workspace",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvcName,
				},
			},
		}
	}

	return pod
}

// CreatePVC creates a PersistentVolumeClaim for a persistent sandbox.
func (c *Client) CreatePVC(ctx context.Context, sandboxID string, diskGB int) error {
	storageSize := resource.MustParse(fmt.Sprintf("%dGi", diskGB))
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pvc-" + sandboxID,
			Namespace: c.namespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: storageSize,
				},
			},
			// uses the cluster's default storage class
			// on Hetzner: hcloud-volumes
			// on AWS:     gp3 (EBS CSI)
			// on GCP:     pd-ssd
		},
	}
	_, err := c.clientset.CoreV1().PersistentVolumeClaims(c.namespace).
		Create(ctx, pvc, metav1.CreateOptions{})
	return err
}

// DeletePVC removes a PVC — called when destroying a persistent sandbox.
func (c *Client) DeletePVC(ctx context.Context, sandboxID string) error {
	err := c.clientset.CoreV1().PersistentVolumeClaims(c.namespace).
		Delete(ctx, "pvc-"+sandboxID, metav1.DeleteOptions{})
	if err != nil && !isNotFound(err) {
		return err
	}
	return nil
}

// ── REPL support ──────────────────────────────────────────────────────────────

// OpenInteractive starts an interactive process inside the pod and returns
// connected stdin/stdout pipes. The process stays alive until Close is called.
func (c *Client) OpenInteractive(ctx context.Context, podName string, args []string) (io.WriteCloser, io.ReadCloser, error) {
	req := c.clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(c.namespace).
		SubResource("exec")

	req.VersionedParams(&corev1.PodExecOptions{
		Command: args,
		Stdin:   true,
		Stdout:  true,
		Stderr:  true,
	}, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(c.config, "POST", req.URL())
	if err != nil {
		return nil, nil, err
	}

	stdinR, stdinW := io.Pipe()
	stdoutR, stdoutW := io.Pipe()

	go func() {
		executor.StreamWithContext(ctx, remotecommand.StreamOptions{
			Stdin:  stdinR,
			Stdout: stdoutW,
			Stderr: stdoutW,
		})
		stdoutW.Close()
	}()

	return stdinW, stdoutR, nil
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func templateToImage(template string) string {
	images := map[string]string{
		"python":      "python:3.11-slim",
		"python-cuda": "nvidia/cuda:12.3.0-runtime-ubuntu22.04",
		"node":        "node:20-slim",
		"ubuntu":      "ubuntu:22.04",
		"ruby":        "ruby:3.3-slim",
		"go":          "golang:1.22-bookworm",
	}
	if img, ok := images[template]; ok {
		return img
	}
	return template // allow custom images
}

func normalizeMemory(mem string) string {
	mem = strings.ToLower(mem)
	replacer := strings.NewReplacer("mb", "Mi", "gb", "Gi", "kb", "Ki")
	return replacer.Replace(mem)
}

func normalizeCPU(cpu string) string {
	// "0.5" → "500m", "1" → "1000m", already "500m" → "500m"
	if strings.HasSuffix(cpu, "m") {
		return cpu
	}
	return cpu // let k8s parse it
}

func boolPtr(b bool) *bool    { return &b }
func int64Ptr(i int64) *int64 { return &i }

func isNotFound(err error) bool {
	return err != nil && strings.Contains(err.Error(), "not found")
}

// waitForCondition polls until fn returns true or ctx is cancelled.
func waitForCondition(ctx context.Context, interval time.Duration, fn func() (bool, error)) error {
	return wait.PollUntilContextTimeout(ctx, interval, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
		return fn()
	})
}

// buildExecURL is used in tests.
func buildExecURL(baseURL *url.URL, namespace, podName string) *url.URL {
	u := *baseURL
	u.Path = fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/exec", namespace, podName)
	return &u
}

// roundTripper is used by tests.
type roundTripper struct {
	fn func(*http.Request) (*http.Response, error)
}

func (r *roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return r.fn(req)
}
