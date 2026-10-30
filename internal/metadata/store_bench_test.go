package metadata

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkTemplateStore measures this adapter, not VM claim or startup latency.
// Writes retain synchronous durability; reads include decoding and validation.
func BenchmarkTemplateStore(b *testing.B) {
	b.Run("Read", func(b *testing.B) {
		store := openStore(b)
		if _, _, err := store.Put(context.Background(), storedTemplate("python")); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		samples := make([]int64, 0, 10000)
		for b.Loop() {
			started := time.Now()
			if _, err := store.Get(context.Background(), "python"); err != nil {
				b.Fatal(err)
			}
			if len(samples) < cap(samples) {
				samples = append(samples, time.Since(started).Nanoseconds())
			}
		}
		reportLatency(b, samples)
	})
	b.Run("DurableWrite", func(b *testing.B) {
		store := openStore(b)
		record := storedTemplate("python")
		index := 0
		b.ReportAllocs()
		samples := make([]int64, 0, 10000)
		for b.Loop() {
			record.Name = fmt.Sprintf("template-%08d", index)
			index++
			started := time.Now()
			if _, _, err := store.Put(context.Background(), record); err != nil {
				b.Fatal(err)
			}
			if len(samples) < cap(samples) {
				samples = append(samples, time.Since(started).Nanoseconds())
			}
		}
		reportLatency(b, samples)
	})
}

// BenchmarkTemplateStoreConcurrentWrite includes waiting for the durable writer.
// Each request inserts a distinct immutable record; no operation is acknowledged
// before the transaction commits. It is not a lease or warm-pool benchmark.
func BenchmarkTemplateStoreConcurrentWrite(b *testing.B) {
	for _, concurrency := range []int{4, 8, 16} {
		b.Run(fmt.Sprintf("requests=%d", concurrency), func(b *testing.B) {
			store := openStore(b)
			var next atomic.Int64
			var workers sync.WaitGroup
			samples := make([]int64, min(b.N, 10000))
			b.ReportAllocs()
			b.ResetTimer()
			for range concurrency {
				workers.Add(1)
				go func() {
					defer workers.Done()
					record := storedTemplate("python")
					for {
						index := next.Add(1) - 1
						if index >= int64(b.N) {
							return
						}
						record.Name = fmt.Sprintf("template-%08d", index)
						started := time.Now()
						if _, _, err := store.Put(context.Background(), record); err != nil {
							b.Error(err)
							return
						}
						if index < int64(len(samples)) {
							samples[index] = time.Since(started).Nanoseconds()
						}
					}
				}()
			}
			workers.Wait()
			b.StopTimer()
			reportLatency(b, samples)
		})
	}
}

func reportLatency(b *testing.B, samples []int64) {
	b.Helper()
	if len(samples) == 0 {
		return
	}
	slices.Sort(samples)
	for _, p := range []int{50, 95, 99} {
		b.ReportMetric(float64(samples[(len(samples)-1)*p/100])/1000, fmt.Sprintf("p%d-us", p))
	}
}
