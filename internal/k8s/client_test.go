package k8s

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/yourorg/sandboxing/pkg/models"
)

func TestBuildPodSpec_DefaultResources(t *testing.T) {
	client := &Client{
		namespace:     "test-ns",
		defaultCPU:    "250m",
		defaultMemory: "256Mi",
	}

	req := &models.CreateSandboxRequest{
		Template: "python",
	}

	pod := client.buildPodSpec(req, "test-sb-123")
	if pod == nil {
		t.Fatal("expected pod to be created, got nil")
	}

	if len(pod.Spec.Containers) == 0 {
		t.Fatal("expected container in pod spec")
	}

	container := pod.Spec.Containers[0]
	memReq := container.Resources.Requests[corev1.ResourceMemory]
	if memReq.String() != "256Mi" {
		t.Errorf("expected memory request 256Mi, got %s", memReq.String())
	}

	cpuReq := container.Resources.Requests[corev1.ResourceCPU]
	if cpuReq.String() != "250m" {
		t.Errorf("expected CPU request 250m, got %s", cpuReq.String())
	}

	memLimit := container.Resources.Limits[corev1.ResourceMemory]
	if memLimit.String() != "256Mi" {
		t.Errorf("expected memory limit 256Mi, got %s", memLimit.String())
	}

	cpuLimit := container.Resources.Limits[corev1.ResourceCPU]
	if cpuLimit.String() != "250m" {
		t.Errorf("expected CPU limit 250m, got %s", cpuLimit.String())
	}
}

func TestBuildPodSpec_CustomResources(t *testing.T) {
	client := &Client{
		namespace:     "test-ns",
		defaultCPU:    "500m",
		defaultMemory: "512Mi",
	}

	req := &models.CreateSandboxRequest{
		Template: "python",
		CPU:      "2",
		Memory:   "1Gi",
	}

	pod := client.buildPodSpec(req, "test-sb-456")
	if pod == nil {
		t.Fatal("expected pod to be created, got nil")
	}

	container := pod.Spec.Containers[0]
	memReq := container.Resources.Requests[corev1.ResourceMemory]
	if memReq.String() != "1Gi" {
		t.Errorf("expected memory request 1Gi, got %s", memReq.String())
	}

	cpuReq := container.Resources.Requests[corev1.ResourceCPU]
	if cpuReq.String() != "2" {
		t.Errorf("expected CPU request 2, got %s", cpuReq.String())
	}
}
