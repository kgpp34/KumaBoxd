//go:build integration

// Package e2e checks KumaBoxd against an operator-selected real runtime snapshot.
// It creates only uniquely named test clones; the supplied snapshot is not deleted.
package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/kgpp34/kumaboxd/internal/metadata"
	"github.com/kgpp34/kumaboxd/internal/node"
	"github.com/kgpp34/kumaboxd/internal/runtimeclient"
	"github.com/kgpp34/kumaboxd/internal/template"
)

func TestRuntimeAndTemplateRegistration(t *testing.T) {
	socket := os.Getenv("KUMABOXD_E2E_SOCKET")
	snapshot := os.Getenv("KUMABOXD_E2E_SNAPSHOT")
	if socket == "" || snapshot == "" {
		t.Fatal("set KUMABOXD_E2E_SOCKET and KUMABOXD_E2E_SNAPSHOT to a real eBPF golden snapshot")
	}
	client, err := runtimeclient.New(socket, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := client.Check(ctx); err != nil {
		t.Fatal(err)
	}
	capture, err := client.InspectSnapshot(ctx, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var shape runtimeclient.GuestConfig
	if err := json.Unmarshal(capture.Config, &shape); err != nil {
		t.Fatal(err)
	}
	if shape.NICs != 1 || shape.NetworkBackend != "ebpf" {
		t.Fatalf("test requires one eBPF NIC, got %+v", shape)
	}
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	name := "kbd-e2e-" + hex.EncodeToString(random[:])
	// Register cleanup before the mutation: a lost acknowledgment may still create a VM.
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		record, err := client.InspectSandbox(cleanupCtx, name)
		if err != nil {
			var remote *runtimeclient.RemoteError
			if !errors.As(err, &remote) || remote.Code != "NOT_FOUND" {
				t.Errorf("inspect test clone for cleanup: %v", err)
			}
			return
		}
		if record.Config.Name != name {
			t.Error("refusing to clean a sandbox with a different identity")
			return
		}
		if _, err := client.Stop(cleanupCtx, record.ID, true); err != nil {
			t.Errorf("stop test clone: %v", err)
			return
		}
		if _, err := client.Remove(cleanupCtx, record.ID); err != nil {
			t.Errorf("remove test clone: %v", err)
		}
	})
	started := time.Now()
	clone, err := client.Clone(ctx, capture.ID, runtimeclient.CloneOptions{Name: name, WaitForNetwork: true})
	if err != nil {
		t.Fatal(err)
	}
	if clone.State != "running" || clone.Config.NetworkBackend != "ebpf" {
		t.Fatalf("unexpected clone: %+v", clone)
	}
	var stdout, stderr bytes.Buffer
	code, err := client.Exec(ctx, clone.ID, runtimeclient.Command{Args: []string{"/bin/true"}}, &stdout, &stderr)
	if err != nil || code != 0 {
		t.Fatalf("first exec: exit %d error %v stderr %q", code, err, stderr.String())
	}
	t.Logf("real clone + network initialization + first exec: %s (one observation, not a benchmark)", time.Since(started))
	code, err = client.Exec(ctx, clone.ID, runtimeclient.Command{Args: []string{
		"/bin/sh", "-ec", "ip -4 -o addr show dev eth0 scope global | grep -q inet; ip -4 route show default | grep -q via; getent ahostsv4 example.com; curl -fsSI --max-time 15 https://example.com",
	}}, &stdout, &stderr)
	if err != nil || code != 0 {
		t.Fatalf("network readiness: exit %d error %v stdout %q stderr %q", code, err, stdout.String(), stderr.String())
	}
	t.Logf("guest DNS and HTTPS passed: %s", stdout.String())
	listed, err := client.ListSandboxes(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, record := range listed {
		found = found || record.ID == clone.ID
	}
	if !found {
		t.Fatal("created sandbox missing from runtime listing")
	}

	directory := filepath.Join(t.TempDir(), "node-state")
	endpoint, stop := startNode(t, client, directory)
	request := template.RegisterRequest{Name: "e2e-template", Snapshot: snapshot}
	var registered template.Template
	apiCall(t, http.MethodPost, endpoint+"/v1/templates", request, 201, &registered)
	if registered.SnapshotID != capture.ID || registered.ImageDigest != capture.ImageDigest {
		t.Fatalf("template does not pin resolved artifacts: %+v", registered)
	}
	apiCall(t, http.MethodPost, endpoint+"/v1/templates", request, 200, new(template.Template))
	apiCall(t, http.MethodGet, endpoint+"/v1/templates?limit=1", nil, 200, new(template.Page))
	stop()
	endpoint, _ = startNode(t, client, directory)
	var recovered template.Template
	apiCall(t, http.MethodGet, endpoint+"/v1/templates/e2e-template", nil, 200, &recovered)
	if recovered.SnapshotID != registered.SnapshotID || !recovered.CreatedAt.Equal(registered.CreatedAt) {
		t.Fatalf("registration did not survive node restart: %+v", recovered)
	}
	t.Log("template create/retry/list/restart recovery passed")
	if _, err := client.Stop(ctx, clone.ID, false); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Remove(ctx, clone.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := client.InspectSandbox(ctx, clone.ID); err == nil {
		t.Fatal("removed test sandbox still exists")
	} else {
		var remote *runtimeclient.RemoteError
		if !errors.As(err, &remote) || remote.Code != "NOT_FOUND" {
			t.Fatalf("removed sandbox did not return NOT_FOUND: %v", err)
		}
	}
	t.Log("runtime stop/remove and classified NOT_FOUND passed")
}

func startNode(t *testing.T, runtime *runtimeclient.Client, directory string) (string, func()) {
	t.Helper()
	store, err := metadata.Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	templates, err := template.New(runtime, store)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	server, err := node.New(runtime, templates, node.Options{
		ProbeTimeout: 2 * time.Second, OperationTimeout: time.Minute, ShutdownTimeout: 5 * time.Second,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { finished <- server.Run(ctx, listener) }()
	stop := sync.OnceFunc(func() {
		cancel()
		if err := errors.Join(<-finished, store.Close()); err != nil {
			t.Error(err)
		}
	})
	t.Cleanup(stop)
	return "http://" + listener.Addr().String(), stop
}

func apiCall(t *testing.T, method, endpoint string, input any, status int, output any) {
	t.Helper()
	var body bytes.Buffer
	if input != nil {
		if err := json.NewEncoder(&body).Encode(input); err != nil {
			t.Fatal(err)
		}
	}
	request, err := http.NewRequestWithContext(context.Background(), method, endpoint, &body)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 30 * time.Second}
	defer client.CloseIdleConnections()
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != status {
		t.Fatalf("%s %s: HTTP %d, want %d: %s", method, endpoint, response.StatusCode, status, data)
	}
	if err := json.Unmarshal(data, output); err != nil {
		t.Fatal(err)
	}
}
