//go:build integration

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/kgpp34/kumaboxd/internal/metadata"
	"github.com/kgpp34/kumaboxd/internal/node"
	"github.com/kgpp34/kumaboxd/internal/pool"
	"github.com/kgpp34/kumaboxd/internal/runtimeclient"
	"github.com/kgpp34/kumaboxd/internal/template"
)

// TestWarmPool uses an existing golden and a fresh private database. Its inventory
// is fully reclaimed; the input image, snapshot and other node resources survive.
func TestWarmPool(t *testing.T) {
	socket, snapshot := os.Getenv("KUMABOXD_E2E_SOCKET"), os.Getenv("KUMABOXD_E2E_SNAPSHOT")
	if socket == "" || snapshot == "" {
		t.Fatal("set KUMABOXD_E2E_SOCKET and KUMABOXD_E2E_SNAPSHOT")
	}
	runtime, err := runtimeclient.New(socket, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = runtime.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	capture, err := runtime.InspectSnapshot(ctx, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var shape runtimeclient.GuestConfig
	if err := json.Unmarshal(capture.Config, &shape); err != nil || shape.NICs != 1 || shape.NetworkBackend != "ebpf" {
		t.Fatalf("requires a one-eBPF-NIC golden: %+v %v", shape, err)
	}
	state := filepath.Join(t.TempDir(), "pool-state")
	endpoint, stop := startPoolNode(t, runtime, state)
	transport := &http.Transport{MaxIdleConnsPerHost: 32}
	httpClient := &http.Client{Transport: transport, Timeout: 30 * time.Second}
	defer transport.CloseIdleConnections()
	call := func(method, path string, body any, key string, want int, output any) time.Duration {
		t.Helper()
		elapsed, err := poolCall(ctx, httpClient, endpoint, method, path, body, key, want, output)
		if err != nil {
			t.Fatal(err)
		}
		return elapsed
	}
	var binding template.Template
	call("POST", "/v1/templates", template.RegisterRequest{Name: "warm-test", Snapshot: snapshot}, "", 201, &binding)
	configure := func(target, maxTotal int) {
		t.Helper()
		call("POST", "/v1/pools", pool.Spec{Name: "warm", Template: "warm-test", TargetReady: target, MaxTotal: maxTotal}, "", 200, nil)
	}
	call("POST", "/v1/pools", pool.Spec{Name: "warm", Template: "warm-test", TargetReady: 4, MaxTotal: 8}, "", 201, nil)
	wait := func(condition func(pool.Status) bool) {
		t.Helper()
		until := time.Now().Add(90 * time.Second)
		for time.Now().Before(until) {
			var status pool.Status
			call("GET", "/v1/pools/warm", nil, "", 200, &status)
			if status.ControllerReady && condition(status) {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatal("pool did not converge")
	}
	// This cleanup runs before helper cleanups. It drains leases and spare stock
	// through the controller while the durable store is still open.
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		stop()
		observed, err := runtime.ListSandboxes(cleanupCtx, true)
		if err != nil {
			t.Error(err)
			return
		}
		db, err := metadata.Open(state)
		if err != nil {
			t.Error(err)
			return
		}
		prefix := "kbd-" + db.NodeID()[:16] + "-"
		_ = db.Close()
		for _, vm := range observed {
			if len(vm.Config.Name) > len(prefix) && vm.Config.Name[:len(prefix)] == prefix {
				if _, err := runtime.Stop(cleanupCtx, vm.ID, true); err != nil {
					t.Error(err)
					continue
				}
				if _, err := runtime.Remove(cleanupCtx, vm.ID); err != nil {
					t.Error(err)
				}
			}
		}
	}()
	wait(func(v pool.Status) bool { return v.Counts.Ready == 4 })
	var lease pool.Lease
	elapsed := call("POST", "/v1/pools/warm/claims", pool.ClaimRequest{Owner: "e2e", TTLSeconds: 300}, "first", 201, &lease)
	t.Logf("warm claim HTTP response including durable receipt: %s (one functional observation)", elapsed)
	var output bytes.Buffer
	code, err := runtime.Exec(ctx, lease.SandboxID, runtimeclient.Command{Args: []string{"/bin/sh", "-ec", "ip -4 route show default; getent ahostsv4 example.com; curl -fsSI --max-time 15 https://example.com"}}, &output, &output)
	if err != nil || code != 0 {
		t.Fatalf("leased guest network: exit %d %v %s", code, err, output.String())
	}
	t.Log("leased VM: guest exec, eBPF route, DNS and HTTPS passed")
	// Restart with one live lease and spare inventory; neither may be allocated twice.
	stop()
	endpoint, stop = startPoolNode(t, runtime, state)
	wait(func(v pool.Status) bool { return v.Counts.Ready >= 3 })
	var retry pool.Lease
	call("POST", "/v1/pools/warm/claims", pool.ClaimRequest{Owner: "e2e", TTLSeconds: 300}, "first", 200, &retry)
	if retry.ID != lease.ID || retry.SandboxID != lease.SandboxID {
		t.Fatal("restart/retry changed allocation")
	}
	call("POST", "/v1/pools/warm/claims", pool.ClaimRequest{Owner: "different", TTLSeconds: 300}, "first", 409, nil)
	var renewed pool.Lease
	deadline := lease.ExpiresAt.Add(time.Second)
	call("PUT", "/v1/leases/"+lease.ID+"/expiry", map[string]any{"expires_at": deadline}, "", 200, &renewed)
	call("PUT", "/v1/leases/"+lease.ID+"/expiry", map[string]any{"expires_at": deadline}, "", 200, &retry)
	if !retry.ExpiresAt.Equal(renewed.ExpiresAt) {
		t.Fatal("renewal retry extended the absolute deadline")
	}
	call("DELETE", "/v1/leases/"+lease.ID, nil, "", 202, nil)
	wait(func(v pool.Status) bool { return v.Counts.Leased == 0 && v.Counts.Retiring == 0 && v.Counts.Ready == 4 })
	if _, err := runtime.InspectSandbox(ctx, lease.SandboxID); err == nil {
		t.Fatal("released VM still exists")
	}
	var expiring pool.Lease
	call("POST", "/v1/pools/warm/claims", pool.ClaimRequest{Owner: "ttl", TTLSeconds: 1}, "ttl", 201, &expiring)
	wait(func(v pool.Status) bool { return v.Counts.Leased == 0 && v.Counts.Retiring == 0 && v.Counts.Ready == 4 })
	call("POST", "/v1/pools/warm/claims", pool.ClaimRequest{Owner: "ttl", TTLSeconds: 1}, "ttl", 410, nil)
	t.Log("node restart, idempotent retry, conflict, renewal, release and TTL passed")
	if os.Getenv("KUMABOXD_POOL_BENCH") == "1" {
		for _, maxTotal := range []int{16, 32} {
			configure(16, maxTotal)
			wait(func(v pool.Status) bool { return v.Counts.Ready == 16 && v.Counts.Total == 16 })
			samples := make([]time.Duration, 0, 80)
			execSamples := make([]time.Duration, 0, 80)
			for round := range 5 {
				receipts := make([]pool.Lease, 16)
				durations := make([]time.Duration, 16)
				execDurations := make([]time.Duration, 16)
				failures := make([]error, 16)
				var group sync.WaitGroup
				for index := range 16 {
					group.Add(1)
					go func() {
						defer group.Done()
						started := time.Now()
						durations[index], failures[index] = poolCall(ctx, httpClient, endpoint, "POST", "/v1/pools/warm/claims", pool.ClaimRequest{Owner: "bench", TTLSeconds: 300}, fmt.Sprintf("batch-%d-%d-%d", maxTotal, round, index), 201, &receipts[index])
						if failures[index] == nil {
							code, err := runtime.Exec(ctx, receipts[index].SandboxID, runtimeclient.Command{Args: []string{"/bin/true"}}, io.Discard, io.Discard)
							if err != nil || code != 0 {
								failures[index] = fmt.Errorf("first guest exec exited %d: %w", code, err)
							}
						}
						execDurations[index] = time.Since(started)
					}()
				}
				group.Wait()
				for _, err := range failures {
					if err != nil {
						t.Fatal(err)
					}
				}
				seen := map[string]bool{}
				for _, v := range receipts {
					if seen[v.SandboxID] {
						t.Fatal("concurrent duplicate allocation")
					}
					seen[v.SandboxID] = true
				}
				if maxTotal == 16 {
					call("POST", "/v1/pools/warm/claims", pool.ClaimRequest{Owner: "bench", TTLSeconds: 300}, fmt.Sprintf("empty-%d", round), 503, nil)
				}
				samples = append(samples, durations...)
				execSamples = append(execSamples, execDurations...)
				if maxTotal == 16 && round == 0 && os.Getenv("KUMABOXD_POOL_EXEC_BENCH") == "1" {
					measureReadyExec(t, ctx, runtime, receipts)
				}
				slices.Sort(durations)
				t.Logf("max_total=%d batch %d: n=16 claim p50=%s p95=%s max=%s (HTTP + durable receipt)", maxTotal, round+1, durations[7], durations[15], durations[15])
				for _, v := range receipts {
					call("DELETE", "/v1/leases/"+v.ID, nil, "", 202, nil)
				}
				wait(func(v pool.Status) bool { return v.Counts.Ready == 16 && v.Counts.Total == 16 })
			}
			slices.Sort(samples)
			t.Logf("max_total=%d pooled n=%d warm claim: p50=%s p95=%s p99=%s max=%s", maxTotal, len(samples), samples[39], samples[75], samples[79], samples[79])
			slices.Sort(execSamples)
			t.Logf("max_total=%d pooled n=%d claim-to-first-exec: p50=%s p95=%s p99=%s max=%s", maxTotal, len(execSamples), execSamples[39], execSamples[75], execSamples[79], execSamples[79])
			readyRaw := make([]int64, len(execSamples))
			for i, d := range execSamples {
				readyRaw[i] = d.Nanoseconds()
			}
			readyJSON, _ := json.Marshal(readyRaw)
			t.Logf("max_total=%d first_exec_samples_ns=%s", maxTotal, readyJSON)
			raw := make([]int64, len(samples))
			for i, d := range samples {
				raw[i] = d.Nanoseconds()
			}
			encoded, _ := json.Marshal(raw)
			t.Logf("max_total=%d samples_ns=%s", maxTotal, encoded)
		}
	}
	configure(0, 16)
	wait(func(v pool.Status) bool { return v.Counts.Total == 0 })
	t.Log("pool drained; no owned VMs remain")
}

func startPoolNode(t *testing.T, runtime *runtimeclient.Client, directory string) (string, func()) {
	t.Helper()
	store, err := metadata.Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	templates, err := template.New(runtime, store)
	if err != nil {
		t.Fatal(err)
	}
	options := pool.DefaultOptions()
	options.MaxVMs = 40
	options.MaxVCPUs = 80
	options.MemoryBudget = 20 << 30
	options.StorageBudget = 512 << 30
	options.Workers = 4
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	inventory, err := pool.New(store, runtime, templates, pool.FixedPolicy{}, options, logger)
	if err != nil {
		t.Fatal(err)
	}
	server, err := node.New(runtime, templates, node.Options{ProbeTimeout: 2 * time.Second, OperationTimeout: 2 * time.Minute, ShutdownTimeout: 5 * time.Second}, logger, node.WithPools(inventory))
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 2)
	go func() { finished <- inventory.Run(ctx) }()
	go func() { finished <- server.Run(ctx, listener) }()
	stop := sync.OnceFunc(func() {
		cancel()
		for range 2 {
			if err := <-finished; err != nil {
				t.Error(err)
			}
		}
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	t.Cleanup(stop)
	return "http://" + listener.Addr().String(), stop
}

func poolCall(ctx context.Context, client *http.Client, endpoint, method, path string, body any, key string, want int, output any) (time.Duration, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return 0, err
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint+path, bytes.NewReader(data))
	if err != nil {
		return 0, err
	}
	request.Header.Set("Content-Type", "application/json")
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}
	started := time.Now()
	response, err := client.Do(request)
	if err != nil {
		return 0, err
	}
	defer func() { _ = response.Body.Close() }()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	elapsed := time.Since(started)
	if err != nil {
		return elapsed, err
	}
	if response.StatusCode != want {
		return elapsed, fmt.Errorf("%s %s: %d want %d: %s", method, path, response.StatusCode, want, payload)
	}
	if output != nil {
		err = json.Unmarshal(payload, output)
	}
	return elapsed, err
}
