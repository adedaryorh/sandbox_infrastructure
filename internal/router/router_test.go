package router

import (
	"encoding/json"
	"github.com/yourorg/sandboxing/pkg/models"
	"io"
	"net/http"
	"strings"
	"testing"
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestRoutesHaveStableIDsAndUseAuthenticatedGateway(t *testing.T) {
	r := New("http://caddy.invalid", "localhost")
	var methods []string
	r.client = &http.Client{Transport: transportFunc(func(req *http.Request) (*http.Response, error) {
		methods = append(methods, req.Method)
		code := 200
		if req.Method == "PATCH" {
			code = 404
		}
		if req.Method == "PUT" {
			data, _ := io.ReadAll(req.Body)
			var object map[string]any
			if e := json.Unmarshal(data, &object); e != nil {
				t.Fatal(e)
			}
			if object["@id"] != "sandbox-sb-unit" {
				t.Fatal("missing stable route ID")
			}
			if !strings.Contains(string(data), "/v1/sandboxes/sb-unit/preview/8000") {
				t.Fatal("missing authenticated gateway rewrite")
			}
			if strings.Contains(string(data), "10.0.0.7:8000") {
				t.Fatal("route bypasses tenant authorization")
			}
		}
		return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	})}
	urls, e := r.RegisterPorts("sb-unit", "10.0.0.7", []models.PortExpose{{Port: 8000}, {Port: 9000}})
	if e != nil || len(urls) != 2 {
		t.Fatal(urls, e)
	}
	if strings.Join(methods, ",") != "PATCH,PUT" {
		t.Fatal(methods)
	}
	if e = r.Deregister("sb-unit"); e != nil {
		t.Fatal(e)
	}
}
