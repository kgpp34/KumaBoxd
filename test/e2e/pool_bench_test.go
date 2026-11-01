//go:build integration

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kgpp34/kumaboxd/internal/pool"
	"github.com/kgpp34/kumaboxd/internal/runtimeclient"
)

// measureWarmClaims measures allocation separately from downstream guest work.
// Every batch starts with 16 Ready VMs; MaxTotal prevents refill until release.
func measureWarmClaims(t *testing.T, ctx context.Context, runtime *runtimeclient.Client, client *http.Client, endpoint string,
	configure func(int, int), wait func(func(pool.Status) bool),
) {
	t.Helper()
	var responseProtocol string
	probe, err := client.Get(endpoint + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	responseProtocol = probe.Proto
	_ = probe.Body.Close()
	t.Logf("claim-only transport=%s", responseProtocol)
	configure(16, 16)
	ready := func(v pool.Status) bool { return v.Counts.Ready == 16 && v.Counts.Total == 16 }
	wait(ready)
	for _, concurrent := range []bool{false, true} {
		samples := make([]int64, 0, 160)
		for round := range 10 {
			leases := make([]pool.Lease, 16)
			durations := make([]int64, 16)
			failures := make([]error, 16)
			reused := make([]bool, 16)
			claim := func(index int) {
				var reusing atomic.Bool
				traced := httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { reusing.Store(info.Reused) }})
				elapsed, err := poolCall(traced, client, endpoint, "POST", "/v1/pools/warm/claims",
					pool.ClaimRequest{Owner: "claim-only", TTLSeconds: 300}, fmt.Sprintf("claim-only-%t-%d-%d", concurrent, round, index), 201, &leases[index])
				durations[index], failures[index] = elapsed.Nanoseconds(), err
				reused[index] = reusing.Load()
			}
			var group sync.WaitGroup
			for index := range leases {
				if concurrent {
					group.Add(1)
					go func() { defer group.Done(); claim(index) }()
				} else {
					claim(index)
				}
			}
			group.Wait()
			seen := map[string]bool{}
			for i, lease := range leases {
				if failures[i] != nil {
					t.Fatal(failures[i])
				}
				if seen[lease.SandboxID] {
					t.Fatal("duplicate warm VM allocation")
				}
				seen[lease.SandboxID] = true
			}
			samples = append(samples, durations...)
			batch := append([]int64(nil), durations...)
			slices.Sort(batch)
			reusedCount := 0
			for _, v := range reused {
				if v {
					reusedCount++
				}
			}
			t.Logf("claim-only concurrent=%t batch=%d p50=%s max=%s reused_connections=%d/16", concurrent, round+1, time.Duration(batch[7]), time.Duration(batch[15]), reusedCount)
			// Verify usability after all claim clocks have stopped. Successful exec
			// does not become part of the reported allocation latency.
			for _, lease := range leases {
				code, err := runtime.Exec(ctx, lease.SandboxID, runtimeclient.Command{Args: []string{"/bin/true"}}, io.Discard, io.Discard)
				if err != nil || code != 0 {
					t.Fatalf("claimed VM unusable: exit=%d err=%v", code, err)
				}
				if _, err := poolCall(ctx, client, endpoint, "DELETE", "/v1/leases/"+lease.ID, nil, "", 202, nil); err != nil {
					t.Fatal(err)
				}
			}
			wait(ready)
		}
		slices.Sort(samples)
		raw, _ := json.Marshal(samples)
		t.Logf("claim-only concurrent=%t n=%d p50=%s p95=%s p99=%s max=%s raw_ns=%s", concurrent, len(samples),
			time.Duration(samples[79]), time.Duration(samples[151]), time.Duration(samples[158]), time.Duration(samples[159]), raw)
	}
}
