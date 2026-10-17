package k8s

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"github.com/yourorg/sandboxing/pkg/models"
)

// GPUResources defines a GPU resource request for a sandbox pod.
type GPUResources struct {
	Type  string // e.g. "nvidia-t4", "nvidia-a100"
	Count int    // number of GPUs to allocate
}

// ApplyGPUResources mutates a pod spec to add GPU resource requests.
// GPU sandboxes:
//   - always use the secure isolation tier (gVisor)
//   - run on dedicated GPU nodes via nodeSelector
//   - get NVIDIA environment variables injected automatically
func ApplyGPUResources(pod *corev1.Pod, gpu *GPUResources) {
	if gpu == nil || gpu.Count == 0 {
		return
	}

	gpuQuantity := resource.MustParse(fmt.Sprintf("%d", gpu.Count))

	// add GPU limit to the sandbox container
	if pod.Spec.Containers[0].Resources.Limits == nil {
		pod.Spec.Containers[0].Resources.Limits = corev1.ResourceList{}
	}
	if pod.Spec.Containers[0].Resources.Requests == nil {
		pod.Spec.Containers[0].Resources.Requests = corev1.ResourceList{}
	}

	pod.Spec.Containers[0].Resources.Limits["nvidia.com/gpu"] = gpuQuantity
	pod.Spec.Containers[0].Resources.Requests["nvidia.com/gpu"] = gpuQuantity

	// schedule only on GPU nodes
	if pod.Spec.NodeSelector == nil {
		pod.Spec.NodeSelector = map[string]string{}
	}
	pod.Spec.NodeSelector["node-type"] = "gpu"

	// NVIDIA container toolkit environment variables
	pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env,
		corev1.EnvVar{
			Name:  "NVIDIA_VISIBLE_DEVICES",
			Value: "all",
		},
		corev1.EnvVar{
			Name:  "NVIDIA_DRIVER_CAPABILITIES",
			Value: "compute,utility",
		},
	)

	// GPU sandboxes always use gVisor (secure tier)
	rc := "gvisor"
	pod.Spec.RuntimeClassName = &rc
}

// ParseGPURequest extracts GPU config from a CreateSandboxRequest.
// Returns nil if no GPU is requested.
func ParseGPURequest(req *models.CreateSandboxRequest) *GPUResources {
	// GPU requests come in via template name convention: "python-cuda", "ubuntu-cuda"
	// or via a future explicit GPU field on the request
	templates := map[string]bool{
		"python-cuda":  true,
		"ubuntu-cuda":  true,
		"pytorch":      true,
		"tensorflow":   true,
	}

	if templates[req.Template] {
		return &GPUResources{
			Type:  "nvidia-t4", // default GPU type
			Count: 1,
		}
	}

	return nil
}
