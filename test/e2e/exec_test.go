//go:build integration

package e2e

import (
	"context"
	"fmt"
	"io"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/kgpp34/kumaboxd/internal/pool"
	"github.com/kgpp34/kumaboxd/internal/runtimeclient"
)

// measureReadyExec excludes allocation entirely. Both cases use the same leased
// VMs while max_total prevents refill, so exec costs cannot be called pool costs.
func measureReadyExec(t *testing.T, ctx context.Context, runtime *runtimeclient.Client, leases []pool.Lease) {
	t.Helper()
	for _, concurrent := range []bool{false, true} {
		samples := make([]time.Duration, 0, 5*len(leases))
		for range 5 {
			durations := make([]time.Duration, len(leases))
			failures := make([]error, len(leases))
			execute := func(index int) {
				start := time.Now()
				code, err := runtime.Exec(ctx, leases[index].SandboxID,
					runtimeclient.Command{Args: []string{"/bin/true"}}, io.Discard, io.Discard)
				durations[index] = time.Since(start)
				if err != nil || code != 0 {
					failures[index] = fmt.Errorf("exec exit=%d: %v", code, err)
				}
			}
			var group sync.WaitGroup
			for index := range leases {
				if concurrent {
					group.Add(1)
					go func() {
						defer group.Done()
						execute(index)
					}()
				} else {
					execute(index)
				}
			}
			group.Wait()
			for _, err := range failures {
				if err != nil {
					t.Fatal(err)
				}
			}
			samples = append(samples, durations...)
		}
		slices.Sort(samples)
		t.Logf("already leased exec only: concurrent=%t n=%d p50=%s p95=%s max=%s samples=%v",
			concurrent, len(samples), samples[(len(samples)-1)/2], samples[(95*len(samples)+99)/100-1], samples[len(samples)-1], samples)
	}
}
