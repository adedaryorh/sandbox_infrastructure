package k8s

import (
	"fmt"
	"github.com/yourorg/sandboxing/pkg/models"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

type GPUResources = models.GPUConfig

func ApplyGPUResources(pod *corev1.Pod, gpu *models.GPUConfig) {
	if gpu == nil || gpu.Count < 1 {
		return
	}
	key := gpu.Resource
	if key == "" {
		key = "nvidia.com/gpu"
	}
	q := resource.MustParse(fmt.Sprint(gpu.Count))
	pod.Spec.Containers[0].Resources.Requests[corev1.ResourceName(key)] = q
	pod.Spec.Containers[0].Resources.Limits[corev1.ResourceName(key)] = q
	if pod.Spec.NodeSelector == nil {
		pod.Spec.NodeSelector = map[string]string{}
	}
	for k, v := range gpu.NodeSelector {
		pod.Spec.NodeSelector[k] = v
	}
	if gpu.RuntimeClass != "" {
		pod.Spec.RuntimeClassName = &gpu.RuntimeClass
	}
}
func ParseGPURequest(req *models.CreateSandboxRequest) *GPUResources { return req.GPU }
