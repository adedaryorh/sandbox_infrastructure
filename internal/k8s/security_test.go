package k8s

import (
	"github.com/yourorg/sandboxing/pkg/models"
	core "k8s.io/api/core/v1"
	"testing"
)

func TestPodSecurityTaskWorkspaceAndGPUResources(t *testing.T) {
	c := &Client{namespace: "sandboxes", defaultCPU: "500m", defaultMemory: "512Mi"}
	pod := c.buildPodSpec(&models.CreateSandboxRequest{Template: "python", Isolation: models.IsolationSecure, GPU: &models.GPUConfig{Count: 2, Resource: "nvidia.com/gpu"}}, "unit")
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Fatal("service account token is mounted")
	}
	container := pod.Spec.Containers[0]
	if !*container.SecurityContext.ReadOnlyRootFilesystem || *container.SecurityContext.AllowPrivilegeEscalation {
		t.Fatal("unsafe security context")
	}
	if pod.Spec.RuntimeClassName == nil || *pod.Spec.RuntimeClassName != "gvisor" {
		t.Fatal("GPU request downgraded secure runtime")
	}
	gpu := container.Resources.Limits[core.ResourceName("nvidia.com/gpu")]
	if gpu.Value() != 2 {
		t.Fatal("GPU resource request missing")
	}
	task := false
	for _, m := range container.VolumeMounts {
		if m.MountPath == "/task" {
			task = true
		}
	}
	if !task || pod.Spec.SecurityContext.FSGroup == nil {
		t.Fatal("task directory is not prepared")
	}
}
