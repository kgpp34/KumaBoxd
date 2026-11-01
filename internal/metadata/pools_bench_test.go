package metadata

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/kgpp34/kumaboxd/internal/pool"
)

// BenchmarkPoolAllocation measures the transaction adapter only. Synthetic Ready
// slots avoid VM work; each operation consumes a distinct slot and lease key.
func BenchmarkPoolAllocation(b *testing.B) {
	for _, concurrent := range []bool{false, true} {
		b.Run(fmt.Sprintf("concurrent=%t", concurrent), func(b *testing.B) {
			store, err := OpenWithOptions(privateDirectory(b), Options{PoolDurability: Durability(os.Getenv("KUMABOXD_POOL_DURABILITY"))})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if err := store.Close(); err != nil {
					b.Error(err)
				}
			})
			now := time.Now().UTC()
			ctx := context.Background()
			err = store.Write(ctx, func(tx pool.WriteTx) error {
				tx.SavePool(pool.Record{Spec: pool.Spec{Name: "test", Template: "test", TargetReady: 1, MaxTotal: b.N + 1}, Template: storedTemplate("test"), CreatedAt: now})
				for i := range b.N {
					id := fmt.Sprintf("%032x", i+1)
					tx.SaveSlot(pool.Slot{ID: id, Pool: "test", Name: "vm-" + id, SandboxID: "vm-" + id, State: pool.Ready, CreatedAt: now, UpdatedAt: now})
				}
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
			allocate := func() error {
				return store.Write(ctx, func(tx pool.WriteTx) error {
					if _, err := tx.Pool("test"); err != nil {
						return err
					}
					slot, err := tx.Ready("test")
					if err != nil {
						return err
					}
					hash := "00000000000000000000000000000000" + slot.ID
					lease := pool.Lease{ID: slot.ID, Pool: "test", SlotID: slot.ID, SandboxID: slot.SandboxID, Owner: "bench", State: pool.Active, KeyHash: hash, RequestHash: hash, CreatedAt: now, UpdatedAt: now, ExpiresAt: now.Add(time.Hour)}
					slot.State = pool.Leased
					slot.LeaseID = lease.ID
					tx.SaveSlot(slot)
					tx.SaveLease(lease)
					return nil
				})
			}
			b.ReportAllocs()
			b.ResetTimer()
			if concurrent {
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if err := allocate(); err != nil {
							b.Error(err)
						}
					}
				})
			} else {
				for range b.N {
					if err := allocate(); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
