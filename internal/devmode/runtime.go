package devmode

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/creack/pty"
	"github.com/yourorg/sandboxing/internal/provider"
	"github.com/yourorg/sandboxing/pkg/models"
	"io"
	"k8s.io/apimachinery/pkg/api/resource"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Runtime struct {
	root    string
	mu      sync.Mutex
	secrets map[string]map[string]string
}

func NewRuntime(root string) (*Runtime, error) {
	if root == "" {
		root = filepath.Join(os.TempDir(), "sandboxing-workspaces")
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	return &Runtime{root: root, secrets: map[string]map[string]string{}}, nil
}
func (d *Runtime) directory(id string) (string, error) {
	if !regexp.MustCompile(`^[A-Za-z0-9-]{1,100}$`).MatchString(id) {
		return "", fmt.Errorf("invalid sandbox identifier")
	}
	return filepath.Join(d.root, id), nil
}
func (d *Runtime) docker(ctx context.Context, args ...string) ([]byte, error) {
	var out, stderr bytes.Buffer
	c := exec.CommandContext(ctx, "docker", args...)
	c.Stdout = &out
	c.Stderr = &stderr
	if err := c.Run(); err != nil {
		return nil, fmt.Errorf("docker operation failed: %w", err)
	}
	return out.Bytes(), nil
}
func (d *Runtime) CreatePod(ctx context.Context, req *models.CreateSandboxRequest, id string) (string, error) {
	if req.Isolation == models.IsolationSecure {
		return "", fmt.Errorf("Docker backend does not implement gVisor isolation; use Kubernetes")
	}
	if req.Persistent {
		return "", fmt.Errorf("Docker persistent disk quotas require a storage driver; use Kubernetes for persistent sandboxes")
	}
	if req.Region != "" && req.Region != "default" {
		return "", fmt.Errorf("Docker backend cannot select a Kubernetes region")
	}
	dir, e := d.directory(id)
	if e != nil {
		return "", e
	}
	if e = os.MkdirAll(dir, 0777); e != nil {
		return "", e
	}
	if e = os.Chmod(dir, 0777); e != nil {
		return "", e
	}
	if _, e = d.docker(ctx, "network", "inspect", "sandbox-net"); e != nil {
		if _, e = d.docker(ctx, "network", "create", "--internal", "sandbox-net"); e != nil {
			return "", e
		}
	}
	name := "sb-" + id
	memory := strings.ToLower(req.Memory)
	memory = strings.NewReplacer("mi", "m", "gi", "g", "ki", "k", "mb", "m", "gb", "g").Replace(memory)
	args := []string{"run", "-d", "--name", name, "--label", "sandbox.id=" + id, "--label", "sandbox.persistent=" + strconv.FormatBool(req.Persistent), "--memory", memory, "--cpus", req.CPU, "--user", "1000:1000", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true", "--read-only", "--pids-limit", "256", "--network", "sandbox-net", "--mount", "type=bind,src=" + dir + ",dst=/workspace", "--mount", "type=bind,src=" + dir + ",dst=/task", "--tmpfs", "/tmp:rw,nosuid,nodev,mode=1777"}
	if req.GPU != nil {
		if req.GPU.Resource != "" && req.GPU.Resource != "nvidia.com/gpu" {
			return "", fmt.Errorf("Docker GPU mode supports NVIDIA device requests only")
		}
		args = append(args, "--gpus", strconv.Itoa(req.GPU.Count))
	}
	for k, v := range req.Env {
		args = append(args, "-e", k+"="+v)
	}
	d.mu.Lock()
	for _, secret := range req.Secrets {
		_ = secret
	}
	for k, v := range d.secrets["secrets-"+id] {
		args = append(args, "-e", k+"="+v)
	}
	d.mu.Unlock()
	args = append(args, templateToDockerImage(req.Template), "/bin/sh", "-c", "sleep infinity")
	if _, e = d.docker(ctx, args...); e != nil {
		if !req.Persistent {
			os.RemoveAll(dir)
		}
		return "", e
	}
	return name, nil
}
func (d *Runtime) WaitForPodRunning(ctx context.Context, pod string) error {
	b, e := d.docker(ctx, "inspect", "--format", "{{.State.Running}}", pod)
	if e != nil {
		return e
	}
	if strings.TrimSpace(string(b)) != "true" {
		return fmt.Errorf("container is not running")
	}
	return nil
}
func (d *Runtime) GetPodIP(ctx context.Context, pod string) (string, error) {
	b, e := d.docker(ctx, "inspect", "--format", "{{(index .NetworkSettings.Networks \"sandbox-net\").IPAddress}}", pod)
	if e != nil {
		return "", e
	}
	ip := strings.TrimSpace(string(b))
	if ip == "" {
		return "", fmt.Errorf("container has no sandbox-network address")
	}
	return ip, nil
}
func (d *Runtime) DeletePod(ctx context.Context, pod string) error {
	b, e := d.docker(ctx, "inspect", "--format", "{{json .Config.Labels}}", pod)
	if e != nil {
		if _, listErr := d.docker(ctx, "info"); listErr != nil {
			return e
		}
		return nil
	}
	var labels map[string]string
	if e = json.Unmarshal(b, &labels); e != nil {
		return e
	}
	if _, e = d.docker(ctx, "rm", "-f", pod); e != nil {
		return e
	}
	if labels["sandbox.persistent"] != "true" {
		dir, e := d.directory(labels["sandbox.id"])
		if e != nil {
			return e
		}
		return os.RemoveAll(dir)
	}
	return nil
}
func (d *Runtime) CreatePVC(ctx context.Context, id string, gb int) error {
	dir, e := d.directory(id)
	if e != nil {
		return e
	}
	if gb < 1 {
		return fmt.Errorf("positive disk size required")
	}
	if e = os.MkdirAll(dir, 0777); e != nil {
		return e
	}
	return os.Chmod(dir, 0777)
}
func (d *Runtime) DeletePVC(ctx context.Context, id string) error {
	dir, e := d.directory(id)
	if e != nil {
		return e
	}
	return os.RemoveAll(dir)
}
func (d *Runtime) CreateSecret(ctx context.Context, name string, values map[string]string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	copy := map[string]string{}
	for k, v := range values {
		copy[k] = v
	}
	d.secrets[name] = copy
	return nil
}
func (d *Runtime) DeleteSecret(ctx context.Context, name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.secrets, name)
	return nil
}
func (d *Runtime) Exec(ctx context.Context, pod, command string) (*models.ExecResult, error) {
	start := time.Now()
	cmd := exec.CommandContext(ctx, "docker", "exec", pod, "/bin/sh", "-c", command)
	var stdout, stderr models.OutputBuffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	e := cmd.Run()
	result := &models.ExecResult{Stdout: stdout.String(), Stderr: stderr.String(), DurationMs: time.Since(start).Milliseconds()}
	if e != nil {
		if exit, ok := e.(*exec.ExitError); ok {
			result.ExitCode = exit.ExitCode()
			return result, nil
		}
		return nil, e
	}
	return result, nil
}
func (d *Runtime) ExecStream(ctx context.Context, pod, command string, frames chan<- models.ExecFrame) error {
	cmd := exec.CommandContext(ctx, "docker", "exec", pod, "/bin/sh", "-c", command)
	stdout, e := cmd.StdoutPipe()
	if e != nil {
		return e
	}
	stderr, e := cmd.StderrPipe()
	if e != nil {
		return e
	}
	if e = cmd.Start(); e != nil {
		return e
	}
	var wg sync.WaitGroup
	pipe := func(r io.Reader, kind string) {
		defer wg.Done()
		buf := make([]byte, 4096)
		for {
			n, e := r.Read(buf)
			if n > 0 {
				select {
				case frames <- models.ExecFrame{Type: kind, Data: string(buf[:n])}:
				case <-ctx.Done():
					return
				}
			}
			if e != nil {
				return
			}
		}
	}
	wg.Add(2)
	go pipe(stdout, "stdout")
	go pipe(stderr, "stderr")
	wg.Wait()
	e = cmd.Wait()
	code := 0
	if exit, ok := e.(*exec.ExitError); ok {
		code = exit.ExitCode()
	} else if e != nil {
		return e
	}
	select {
	case frames <- models.ExecFrame{Type: "exit", Code: code}:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
func (d *Runtime) ExecWithStdin(ctx context.Context, pod, command string, input io.Reader) error {
	cmd := exec.CommandContext(ctx, "docker", "exec", "-i", pod, "/bin/sh", "-c", command)
	cmd.Stdin = input
	return cmd.Run()
}
func quote(v string) string { return "'" + strings.ReplaceAll(v, "'", "'\"'\"'") + "'" }
func (d *Runtime) WriteFile(ctx context.Context, pod, path string, data []byte) error {
	result, e := d.Exec(ctx, pod, "mkdir -p "+quote(filepath.Dir(path))+" && printf %s "+quote(base64.StdEncoding.EncodeToString(data))+" | base64 -d > "+quote(path))
	if e == nil && result.ExitCode != 0 {
		return fmt.Errorf("file write failed")
	}
	return e
}
func (d *Runtime) ReadFile(ctx context.Context, pod, path string) ([]byte, error) {
	v, e := d.Exec(ctx, pod, "cat -- "+quote(path))
	if e != nil {
		return nil, e
	}
	if v.ExitCode != 0 {
		return nil, fmt.Errorf("file read failed")
	}
	return []byte(v.Stdout), nil
}
func (d *Runtime) DeleteFile(ctx context.Context, pod, path string) error {
	v, e := d.Exec(ctx, pod, "rm -f -- "+quote(path))
	if e == nil && v.ExitCode != 0 {
		return fmt.Errorf("file delete failed")
	}
	return e
}
func (d *Runtime) ListDir(ctx context.Context, pod, path string) (string, error) {
	v, e := d.Exec(ctx, pod, "ls -la -- "+quote(path))
	if e != nil {
		return "", e
	}
	if v.ExitCode != 0 {
		return "", fmt.Errorf("directory listing failed")
	}
	return v.Stdout, nil
}

type processPipe struct {
	io.ReadCloser
	command *exec.Cmd
	once    sync.Once
}

func (p *processPipe) Close() error {
	var e error
	p.once.Do(func() {
		e = p.ReadCloser.Close()
		if p.command.Process != nil {
			p.command.Process.Kill()
		}
		p.command.Wait()
	})
	return e
}
func (d *Runtime) OpenInteractive(ctx context.Context, pod string, args []string) (io.WriteCloser, io.ReadCloser, error) {
	all := append([]string{"exec", "-i", pod}, args...)
	cmd := exec.CommandContext(ctx, "docker", all...)
	in, e := cmd.StdinPipe()
	if e != nil {
		return nil, nil, e
	}
	out, e := cmd.StdoutPipe()
	if e != nil {
		in.Close()
		return nil, nil, e
	}
	cmd.Stderr = cmd.Stdout
	if e = cmd.Start(); e != nil {
		in.Close()
		out.Close()
		return nil, nil, e
	}
	return in, &processPipe{ReadCloser: out, command: cmd}, nil
}

type dockerTerminal struct {
	file *os.File
	cmd  *exec.Cmd
	once sync.Once
}

func (t *dockerTerminal) Read(b []byte) (int, error)  { return t.file.Read(b) }
func (t *dockerTerminal) Write(b []byte) (int, error) { return t.file.Write(b) }
func (t *dockerTerminal) Close() {
	t.once.Do(func() {
		t.file.Close()
		if t.cmd.Process != nil {
			t.cmd.Process.Kill()
		}
		t.cmd.Wait()
	})
}
func (t *dockerTerminal) Resize(cols, rows uint16) {
	pty.Setsize(t.file, &pty.Winsize{Cols: cols, Rows: rows})
}
func (d *Runtime) OpenPTY(ctx context.Context, pod string) (provider.Terminal, error) {
	cmd := exec.CommandContext(ctx, "docker", "exec", "-it", pod, "/bin/sh")
	f, e := pty.Start(cmd)
	if e != nil {
		return nil, e
	}
	return &dockerTerminal{file: f, cmd: cmd}, nil
}
func (d *Runtime) GetMetrics(ctx context.Context, pod string) (*models.ResourceMetrics, error) {
	b, e := d.docker(ctx, "stats", "--no-stream", "--format", "{{json .}}", pod)
	if e != nil {
		return nil, e
	}
	var item struct {
		CPUPerc  string
		MemUsage string
	}
	if e = json.Unmarshal(b, &item); e != nil {
		return nil, e
	}
	percent, e := strconv.ParseFloat(strings.TrimSuffix(item.CPUPerc, "%"), 64)
	if e != nil {
		return nil, e
	}
	part := strings.TrimSpace(strings.Split(item.MemUsage, "/")[0])
	part = strings.NewReplacer("KiB", "Ki", "MiB", "Mi", "GiB", "Gi", "kB", "k", "MB", "M", "GB", "G", "B", "").Replace(part)
	memory, e := resource.ParseQuantity(part)
	if e != nil {
		return nil, e
	}
	return &models.ResourceMetrics{CPUCoreUsage: percent / 100, MemoryBytes: memory.Value(), SampledAt: time.Now().UTC(), Source: "docker-stats"}, nil
}
