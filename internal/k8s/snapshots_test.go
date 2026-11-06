package k8s

import (
	"context"
	"io"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"net/http"
	"strings"
	"testing"
)

type snapshotTransport func(*http.Request) (*http.Response, error)

func (f snapshotTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestRestoreRejectsUnsafeSnapshots(t *testing.T) {
	for _, body := range []string{
		`{"apiVersion":"snapshot.storage.k8s.io/v1","kind":"VolumeSnapshot","status":{"readyToUse":false}}`,
		`{"apiVersion":"snapshot.storage.k8s.io/v1","kind":"VolumeSnapshot","status":{"readyToUse":true,"restoreSize":"1Gi"}}`,
		`{"apiVersion":"snapshot.storage.k8s.io/v1","kind":"VolumeSnapshot","metadata":{"annotations":{"sandboxing.storage-class":"fast"}},"status":{"readyToUse":true,"restoreSize":"10Gi"}}`,
	} {
		client, err := dynamic.NewForConfigAndClient(&rest.Config{Host: "https://cluster.invalid"}, &http.Client{Transport: snapshotTransport(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
		})})
		if err != nil {
			t.Fatal(err)
		}
		c := &Client{dynamic: client, namespace: "sandboxes"}
		// No PVC client is supplied: invalid snapshots must fail before creating a volume.
		if err = c.RestoreVolume(context.Background(), "test", "snapshot", 1); err == nil {
			t.Fatal("invalid snapshot accepted")
		}
	}
}
