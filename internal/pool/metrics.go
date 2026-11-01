package pool

import (
	"sync"
	"time"
)

// Histogram uses fixed cumulative buckets, bounding memory regardless of traffic.
// Counts and durations reset on process restart; leases and inventory do not.
type Histogram struct {
	Count      uint64   `json:"count"`
	SumSeconds float64  `json:"sum_seconds"`
	Buckets    []Bucket `json:"buckets"`
}
type Bucket struct {
	UpperSeconds float64 `json:"upper_seconds"`
	Count        uint64  `json:"count"`
}

// Metrics separates incoming attempts, idempotent retries, misses and new claims.
type Metrics struct {
	// ProvisionRuntimeLatency isolates successful clone/probe RPC time.
	ProvisionRuntimeLatency Histogram `json:"provision_runtime_latency"`
	Since                   time.Time `json:"since"`
	ClaimAttempts           uint64    `json:"claim_attempts"`
	Claimed                 uint64    `json:"claimed"`
	Replays                 uint64    `json:"replays"`
	Misses                  uint64    `json:"misses"`
	Provisioned             uint64    `json:"provisioned"`
	ProvisionFailures       uint64    `json:"provision_failures"`
	ClaimLatency            Histogram `json:"claim_latency"`
	ProvisionLatency        Histogram `json:"provision_latency"`
}
type measurements struct {
	mu    sync.Mutex
	pools map[string]*Metrics
}

func histogram() Histogram {
	h := Histogram{}
	for _, v := range []float64{.0001, .00025, .0005, .001, .002, .005, .01, .025, .05, .1, .25, .5, 1, 2, 5, 10, 30, 60, 120, 3600, 86400} {
		h.Buckets = append(h.Buckets, Bucket{UpperSeconds: v})
	}
	return h
}
func (m *measurements) update(name string, f func(*Metrics)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pools == nil {
		m.pools = make(map[string]*Metrics)
	}
	v := m.pools[name]
	if v == nil {
		v = &Metrics{Since: time.Now().UTC(), ClaimLatency: histogram(), ProvisionRuntimeLatency: histogram(), ProvisionLatency: histogram()}
		m.pools[name] = v
	}
	f(v)
}
func (m *measurements) snapshot(name string) Metrics {
	var out Metrics
	m.update(name, func(v *Metrics) {
		out = *v
		out.ProvisionRuntimeLatency.Buckets = append([]Bucket(nil), v.ProvisionRuntimeLatency.Buckets...)
		out.ClaimLatency.Buckets = append([]Bucket(nil), v.ClaimLatency.Buckets...)
		out.ProvisionLatency.Buckets = append([]Bucket(nil), v.ProvisionLatency.Buckets...)
	})
	return out
}
func observe(h *Histogram, d time.Duration) {
	h.Count++
	s := d.Seconds()
	h.SumSeconds += s
	for i := range h.Buckets {
		if s <= h.Buckets[i].UpperSeconds {
			h.Buckets[i].Count++
		}
	}
}
