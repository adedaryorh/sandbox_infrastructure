package repl

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/yourorg/sandboxing/internal/k8s"
	"github.com/yourorg/sandboxing/pkg/models"
)

// Session holds a live interpreter process inside a sandbox pod.
// Variables, imports and in-memory state persist between Send() calls.
type Session struct {
	ID        string
	SandboxID string
	Kernel    models.REPLKernel
	stdin     io.WriteCloser
	stdout    *bufio.Scanner
	mu        sync.Mutex // serialise sends — one at a time
	CreatedAt time.Time
}

// Manager owns all active REPL sessions across all sandboxes.
type Manager struct {
	k8s      *k8s.Client
	sessions map[string]*Session // replID → session
	mu       sync.RWMutex
}

func NewManager(k8s *k8s.Client) *Manager {
	return &Manager{
		k8s:      k8s,
		sessions: make(map[string]*Session),
	}
}

// Open starts a new REPL session inside the given sandbox pod.
// The interpreter process stays alive until Close() is called.
func (m *Manager) Open(ctx context.Context, sandboxID, podName string, kernel models.REPLKernel) (*Session, error) {
	cmd := kernelCommand(kernel)
	if cmd == nil {
		return nil, fmt.Errorf("unsupported kernel: %s", kernel)
	}

	// OpenInteractive keeps stdin open — the interpreter reads from it indefinitely
	stdin, stdout, err := m.k8s.OpenInteractive(ctx, podName, cmd)
	if err != nil {
		return nil, fmt.Errorf("open interactive: %w", err)
	}

	id := fmt.Sprintf("repl-%d", time.Now().UnixNano())
	session := &Session{
		ID:        id,
		SandboxID: sandboxID,
		Kernel:    kernel,
		stdin:     stdin,
		stdout:    bufio.NewScanner(stdout),
		CreatedAt: time.Now(),
	}

	m.mu.Lock()
	m.sessions[id] = session
	m.mu.Unlock()

	// consume the interpreter's startup banner so it doesn't pollute results
	session.drainStartup()

	return session, nil
}

// Send writes code to the interpreter and reads back the output.
// State (variables, imports) persists across calls.
func (m *Manager) Send(replID, code string) (*models.REPLResult, error) {
	m.mu.RLock()
	session, ok := m.sessions[replID]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("repl session %q not found", replID)
	}

	// serialise — only one send at a time per session
	session.mu.Lock()
	defer session.mu.Unlock()

	// unique sentinel marks the end of this send's output
	sentinel := fmt.Sprintf("__REPL_END_%d__", time.Now().UnixNano())

	// write code + sentinel print to stdin
	// the interpreter will execute the code then print the sentinel
	input := code + "\n" + sentinelPrint(session.Kernel, sentinel) + "\n"
	if _, err := io.WriteString(session.stdin, input); err != nil {
		return nil, fmt.Errorf("write to repl: %w", err)
	}

	// read output lines until sentinel appears
	var stdout, stderr strings.Builder
	for session.stdout.Scan() {
		line := session.stdout.Text()
		if line == sentinel {
			break
		}
		// Python/Node prefix stderr lines with specific patterns
		if isStderr(line) {
			stderr.WriteString(strings.TrimPrefix(line, "STDERR:") + "\n")
		} else {
			stdout.WriteString(line + "\n")
		}
	}

	return &models.REPLResult{
		Stdout: strings.TrimRight(stdout.String(), "\n"),
		Stderr: strings.TrimRight(stderr.String(), "\n"),
	}, nil
}

// Close terminates the interpreter process and removes the session.
func (m *Manager) Close(replID string) error {
	m.mu.Lock()
	session, ok := m.sessions[replID]
	if ok {
		delete(m.sessions, replID)
	}
	m.mu.Unlock()

	if !ok {
		return fmt.Errorf("repl session %q not found", replID)
	}

	// close stdin — the interpreter will exit when stdin closes
	return session.stdin.Close()
}

// CloseAll terminates all sessions for a given sandbox — called on destroy.
func (m *Manager) CloseAll(sandboxID string) {
	m.mu.Lock()
	var toClose []string
	for id, s := range m.sessions {
		if s.SandboxID == sandboxID {
			toClose = append(toClose, id)
		}
	}
	for _, id := range toClose {
		delete(m.sessions, id)
	}
	m.mu.Unlock()

	for _, id := range toClose {
		m.Close(id)
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// kernelCommand returns the argv for starting each supported interpreter.
func kernelCommand(kernel models.REPLKernel) []string {
	switch kernel {
	case models.KernelPython:
		// -u = unbuffered stdout so we see output immediately
		return []string{"python3", "-u", "-i"}
	case models.KernelNode:
		return []string{"node", "--interactive"}
	case models.KernelBash:
		return []string{"bash", "-i"}
	case models.KernelRuby:
		return []string{"irb", "--noecho"}
	}
	return nil
}

// sentinelPrint returns the interpreter-specific command to print the sentinel.
func sentinelPrint(kernel models.REPLKernel, sentinel string) string {
	switch kernel {
	case models.KernelPython:
		return fmt.Sprintf("print(%q)", sentinel)
	case models.KernelNode:
		return fmt.Sprintf("console.log(%q)", sentinel)
	case models.KernelBash:
		return fmt.Sprintf("echo %q", sentinel)
	case models.KernelRuby:
		return fmt.Sprintf("puts %q", sentinel)
	}
	return fmt.Sprintf("echo %q", sentinel)
}

func isStderr(line string) bool {
	return strings.HasPrefix(line, "STDERR:")
}

// drainStartup reads and discards the interpreter's startup banner.
func (s *Session) drainStartup() {
	// give interpreter 100ms to write its banner then drain
	done := make(chan struct{})
	go func() {
		defer close(done)
		for s.stdout.Scan() {
			line := s.stdout.Text()
			// Python banner ends when we see ">>> "
			if strings.HasPrefix(line, ">>> ") || line == "" {
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
	}
}
