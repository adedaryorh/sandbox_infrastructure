package k8s

import (
	"context"
	"fmt"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"os"
	"time"
)

var snapshots = schema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshots"}

func (c *Client) CreateVolumeSnapshot(ctx context.Context, pvc, name string) (int64, error) {
	source, err := c.clientset.CoreV1().PersistentVolumeClaims(c.namespace).Get(ctx, pvc, meta.GetOptions{})
	if err != nil {
		return 0, fmt.Errorf("read source volume: %w", err)
	}
	class := ""
	if source.Spec.StorageClassName != nil {
		class = *source.Spec.StorageClassName
	}
	spec := map[string]any{"source": map[string]any{"persistentVolumeClaimName": pvc}}
	if class := os.Getenv("VOLUME_SNAPSHOT_CLASS"); class != "" {
		spec["volumeSnapshotClassName"] = class
	}
	object := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshot", "metadata": map[string]any{"name": name, "namespace": c.namespace, "annotations": map[string]any{"sandboxing.storage-class": class}}, "spec": spec}}
	if _, err := c.dynamic.Resource(snapshots).Namespace(c.namespace).Create(ctx, object, meta.CreateOptions{}); err != nil {
		return 0, err
	}
	timeout, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		current, err := c.dynamic.Resource(snapshots).Namespace(c.namespace).Get(timeout, name, meta.GetOptions{})
		if err != nil {
			return 0, err
		}
		if failure, found, _ := unstructured.NestedMap(current.Object, "status", "error"); found {
			return 0, fmt.Errorf("volume snapshot controller failed: %v", failure["message"])
		}
		ready, _, _ := unstructured.NestedBool(current.Object, "status", "readyToUse")
		if ready {
			value, _, _ := unstructured.NestedString(current.Object, "status", "restoreSize")
			size, err := resource.ParseQuantity(value)
			if err != nil {
				return 0, err
			}
			return size.Value(), nil
		}
		select {
		case <-timeout.Done():
			return 0, timeout.Err()
		case <-ticker.C:
		}
	}
}
func (c *Client) DeleteVolumeSnapshot(ctx context.Context, name string) error {
	err := c.dynamic.Resource(snapshots).Namespace(c.namespace).Delete(ctx, name, meta.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}
func (c *Client) RestoreVolume(ctx context.Context, id, snapshot string, gb int) error {
	if gb < 1 {
		return fmt.Errorf("restore size must be positive")
	}
	snap, err := c.dynamic.Resource(snapshots).Namespace(c.namespace).Get(ctx, snapshot, meta.GetOptions{})
	if err != nil {
		return err
	}
	ready, _, _ := unstructured.NestedBool(snap.Object, "status", "readyToUse")
	if !ready {
		return fmt.Errorf("snapshot is not ready to restore")
	}
	class, ok := snap.GetAnnotations()["sandboxing.storage-class"]
	if !ok {
		return fmt.Errorf("snapshot lacks source storage class metadata; operator migration required")
	}
	sizeText, _, _ := unstructured.NestedString(snap.Object, "status", "restoreSize")
	size, err := resource.ParseQuantity(sizeText)
	if err != nil {
		return fmt.Errorf("invalid snapshot restore size: %w", err)
	}
	requested := resource.MustParse(fmt.Sprintf("%dGi", gb))
	if requested.Cmp(size) < 0 {
		return fmt.Errorf("requested volume is smaller than snapshot")
	}
	group := "snapshot.storage.k8s.io"
	pvc := &core.PersistentVolumeClaim{ObjectMeta: meta.ObjectMeta{Name: "pvc-" + id, Namespace: c.namespace}, Spec: core.PersistentVolumeClaimSpec{StorageClassName: &class, AccessModes: []core.PersistentVolumeAccessMode{core.ReadWriteOnce}, Resources: core.VolumeResourceRequirements{Requests: core.ResourceList{core.ResourceStorage: resource.MustParse(fmt.Sprintf("%dGi", gb))}}, DataSource: &core.TypedLocalObjectReference{APIGroup: &group, Kind: "VolumeSnapshot", Name: snapshot}}}
	_, err = c.clientset.CoreV1().PersistentVolumeClaims(c.namespace).Create(ctx, pvc, meta.CreateOptions{})
	return err
}
