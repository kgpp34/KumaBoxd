package runtimeclient

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCheckUsesRuntimeRPCAndReusesConnection(t *testing.T) {
	var connections atomic.Int32
	socket := startRuntime(t, &connections, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/rpc/v1/node.info" || r.ProtoMajor != 2 {
			t.Errorf("unexpected request: %s %s %s", r.Method, r.URL.Path, r.Proto)
		}
		var size [4]byte
		if _, err := io.ReadFull(r.Body, size[:]); err != nil {
			t.Error(err)
			return
		}
		length := binary.BigEndian.Uint32(size[:])
		if length == 0 || length > 1<<20 {
			t.Errorf("invalid metadata length %d", length)
			return
		}
		metadata := make([]byte, length)
		if _, err := io.ReadFull(r.Body, metadata); err != nil {
			t.Error(err)
			return
		}
		var request struct {
			Params   json.RawMessage `json:"params"`
			Deadline time.Time       `json:"deadline"`
		}
		if err := json.Unmarshal(metadata, &request); err != nil {
			t.Error(err)
			return
		}
		if string(request.Params) != "{}" || request.Deadline.IsZero() {
			t.Errorf("missing params/deadline: %s", metadata)
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		// The runtime currently encodes NodeInfo fields with Go's default casing.
		_, _ = io.WriteString(w, "{\"type\":\"result\",\"data\":{\"Version\":\"test-build\"}}\n")
	})
	client, err := New(socket, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for range 2 {
		if err := client.Check(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if got := connections.Load(); got != 1 {
		t.Fatalf("opened %d connections for sequential probes, want 1", got)
	}
}

func TestCheckHonorsCancellation(t *testing.T) {
	entered := make(chan struct{})
	socket := startRuntime(t, new(atomic.Int32), func(_ http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
	})
	client, err := New(socket, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- client.Check(ctx) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("runtime probe did not arrive")
	}
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("probe did not stop after cancellation")
	}
}

func TestCheckRejectsUnavailableAndOversizedResponses(t *testing.T) {
	for _, test := range []struct {
		name   string
		status int
		body   string
	}{
		{"unavailable", http.StatusServiceUnavailable, "busy"},
		{"oversized", http.StatusOK, strings.Repeat("x", maxFrame+1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			socket := startRuntime(t, new(atomic.Int32), func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.status)
				_, _ = io.WriteString(w, test.body)
			})
			client, err := New(socket, 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = client.Close() }()
			if err := client.Check(context.Background()); err == nil {
				t.Fatal("invalid runtime response passed readiness")
			}
		})
	}
}

func TestCheckRejectsIncompleteAndFailedResults(t *testing.T) {
	for _, data := range []string{
		``, `null`, `{`, `{"type":"progress"}`,
		`{"type":"result","data":null}`,
		`{"type":"result","data":{"Version":" "}}`,
		`{"type":"result","failure":{"message":"unavailable","class":4,"code":"ARTIFACT_UNAVAILABLE"}}`,
	} {
		t.Run(data, func(t *testing.T) {
			socket := startRuntime(t, new(atomic.Int32), func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, data) })
			client, err := New(socket, 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = client.Close() }()
			if err := client.Check(context.Background()); err == nil {
				t.Errorf("accepted invalid probe %q", data)
			}
		})
	}
}

func startRuntime(t *testing.T, connections *atomic.Int32, handler http.HandlerFunc) string {
	t.Helper()
	// Short paths also fit macOS's Unix socket path limit.
	dir, err := os.MkdirTemp("", "kbd-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "rpc.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	server := &http.Server{
		Handler: handler, Protocols: protocols, ReadHeaderTimeout: time.Second,
		ConnState: func(_ net.Conn, state http.ConnState) {
			if state == http.StateNew {
				connections.Add(1)
			}
		},
	}
	finished := make(chan error, 1)
	go func() { finished <- server.Serve(listener) }()
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Error(err)
		}
		if err := <-finished; !errors.Is(err, http.ErrServerClosed) {
			t.Error(err)
		}
	})
	return socket
}
