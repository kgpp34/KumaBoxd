package pool

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/kgpp34/kumaboxd/internal/runtimeclient"
)

type reservation struct {
	vms     int
	cpus    uint64
	memory  uint64
	storage uint64
}

func (r *reservation) add(shape runtimeclient.GuestConfig, n int) {
	r.vms += n
	if n > 0 {
		unit := diskBytes(shape)
		if unit > ^uint64(0)/uint64(n) || r.storage > ^uint64(0)-unit*uint64(n) {
			r.storage = ^uint64(0)
		} else {
			r.storage += unit * uint64(n)
		}
	}
	cpus := uint64(shape.CPUs) * uint64(n)
	mem := uint64(shape.Memory)
	if n > 0 && mem > ^uint64(0)/uint64(n) {
		r.memory = ^uint64(0)
	} else if ^uint64(0)-r.memory < mem*uint64(n) {
		r.memory = ^uint64(0)
	} else {
		r.memory += mem * uint64(n)
	}
	if ^uint64(0)-r.cpus < cpus {
		r.cpus = ^uint64(0)
	} else {
		r.cpus += cpus
	}
}
func (s *Service) fits(r reservation, shape runtimeclient.GuestConfig) bool {
	return diskBytes(shape) <= s.options.StorageBudget &&
		r.storage <= s.options.StorageBudget-diskBytes(shape) &&
		r.vms < s.options.MaxVMs &&
		uint64(shape.CPUs) <= s.options.MaxVCPUs &&
		r.cpus <= s.options.MaxVCPUs-uint64(shape.CPUs) &&
		uint64(shape.Memory) <= s.options.MemoryBudget &&
		r.memory <= s.options.MemoryBudget-uint64(shape.Memory)
}

func (s *Service) plan(ctx context.Context, active map[string]string, round int, gc bool) ([]work, error) {
	jobs := make([]work, 0, s.options.Workers+s.options.ReclaimWorkers)
	now := time.Now().UTC()
	observation := Observation{At: now, Limits: s.options}
	var snapshot []Record
	err := s.store.Read(ctx, func(tx ReadTx) error {
		var err error
		snapshot, err = tx.Pools()
		if err != nil {
			return err
		}
		var used reservation
		for _, r := range snapshot {
			slots, err := tx.Slots(r.Spec.Name)
			if err != nil {
				return err
			}
			var shape runtimeclient.GuestConfig
			if err := json.Unmarshal(r.Template.GuestConfig, &shape); err != nil {
				return err
			}
			used.add(shape, len(slots))
			observation.Pools = append(observation.Pools, PoolObservation{CPUs: shape.CPUs, Memory: uint64(shape.Memory), Storage: diskBytes(shape), Spec: r.Spec, Counts: count(slots), Metrics: s.metrics.snapshot(r.Spec.Name)})
		}
		observation.ReservedVMs = used.vms
		observation.ReservedVCPUs = used.cpus
		observation.ReservedMemory = used.memory
		observation.ReservedStorage = used.storage
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Policy receives owned values; expensive prediction never holds a DB lock.
	policyCtx, cancelPolicy := context.WithTimeout(ctx, s.options.PolicyTimeout)
	decisions, policyErr := s.policy.Decide(policyCtx, observation)
	if policyErr == nil {
		policyErr = policyCtx.Err()
	}
	cancelPolicy()
	if policyErr != nil {
		s.logger.WarnContext(ctx, "pool policy failed; using fixed inventory", "error", policyErr)
		decisions, _ = (FixedPolicy{}).Decide(ctx, observation)
	}

	moreGC := false
	err = s.store.Write(ctx, func(tx WriteTx) error {
		records, err := tx.Pools()
		if err != nil {
			return err
		}
		sort.Slice(records, func(i, j int) bool { return records[i].Spec.Name < records[j].Spec.Name })
		var used reservation
		for _, r := range records {
			slots, err := tx.Slots(r.Spec.Name)
			if err != nil {
				return err
			}
			var shape runtimeclient.GuestConfig
			if err := json.Unmarshal(r.Template.GuestConfig, &shape); err != nil {
				return err
			}
			used.add(shape, len(slots))
		}
		// Rotate pools across passes so one popular template cannot monopolize workers.
		for index := range records {
			r := records[(index+round)%len(records)]
			name := r.Spec.Name
			slots, err := tx.Slots(name)
			if err != nil {
				return err
			}
			counts := count(slots)
			decision, ok := decisions[name]
			if !ok {
				decision = Decision{TargetReady: r.Spec.TargetReady, RefillConcurrency: s.options.Workers}
			}
			target := min(r.Spec.MaxTotal, max(0, decision.TargetReady))
			refillLimit := min(s.options.Workers, max(0, decision.RefillConcurrency))
			if r.Spec.TargetReady == 0 {
				target = 0
				refillLimit = 0
			}
			excess := max(counts.Ready+counts.Checking+counts.Creating-target, counts.Total-r.Spec.MaxTotal)
			cooldown := time.Time{}
			for i, slot := range slots {
				if slot.State == Leased {
					lease, err := tx.Lease(slot.LeaseID)
					if err != nil {
						return err
					}
					if !now.Before(lease.ExpiresAt) {
						lease.State = Releasing
						lease.UpdatedAt = now
						slot.State = Retiring
						slot.NextAttempt = time.Time{}
						slot.UpdatedAt = now
						tx.SaveLease(lease)
						tx.SaveSlot(slot)
						slots[i] = slot
					}
				}
				if excess > 0 && slot.State == Ready && active[slot.ID] == "" {
					slot.State = Retiring
					slot.UpdatedAt = now
					tx.SaveSlot(slot)
					slots[i] = slot
					excess--
				}
				if slot.State == Retiring && slot.NextAttempt.After(cooldown) {
					cooldown = slot.NextAttempt
				}
			}
			for _, state := range []string{Retiring, Checking, Creating} {
				for _, slot := range slots {
					if !s.room(state, active, jobs) || state != Retiring && (poolWorkers(name, active, jobs, slots) >= refillLimit || !s.healthy.Load()) {
						break
					}
					if slot.State == state && active[slot.ID] == "" && !now.Before(slot.NextAttempt) {
						jobs = append(jobs, work{slot, r})
					}
				}
			}
			counts = count(slots)
			needed := min(target-counts.Ready-counts.Creating-counts.Checking, r.Spec.MaxTotal-counts.Total)
			var shape runtimeclient.GuestConfig
			if err := json.Unmarshal(r.Template.GuestConfig, &shape); err != nil {
				return err
			}
			for s.healthy.Load() && needed > 0 && s.room(Creating, active, jobs) && poolWorkers(name, active, jobs, slots) < refillLimit && s.fits(used, shape) && !now.Before(cooldown) {
				id := newID()
				slot := Slot{ID: id, Pool: name, Name: s.vmName(id), State: Creating, CreatedAt: now, UpdatedAt: now}
				tx.SaveSlot(slot)
				jobs = append(jobs, work{slot, r})
				used.add(shape, 1)
				needed--
			}
		}
		if !gc {
			return nil
		}
		const receiptBatch = 256
		receipts, err := tx.ReceiptsBefore(now.Add(-s.options.ReceiptRetention), receiptBatch)
		if err != nil {
			return err
		}
		moreGC = len(receipts) == receiptBatch
		for _, lease := range receipts {
			if (lease.State == Released || lease.State == Lost) && now.Sub(lease.UpdatedAt) > s.options.ReceiptRetention {
				if _, err := tx.Slot(lease.Pool, lease.SlotID); errors.Is(err, ErrNotFound) {
					tx.RemoveLease(lease)
				} else if err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err == nil && moreGC {
		s.moreGC.Store(true)
		s.notify()
	}
	return jobs, err
}

func poolWorkers(name string, active map[string]string, jobs []work, slots []Slot) int {
	n := 0
	for _, slot := range slots {
		if state := active[slot.ID]; state != "" && state != Retiring {
			n++
		}
	}
	for _, job := range jobs {
		if job.record.Spec.Name == name && job.slot.State != Retiring {
			n++
		}
	}
	return n
}

// diskBytes includes inherited data disks, not just the root writable disk.
func diskBytes(shape runtimeclient.GuestConfig) uint64 {
	if shape.Storage <= 0 {
		return ^uint64(0)
	}
	total := uint64(shape.Storage)
	for _, disk := range shape.DataDisks {
		if disk.Size <= 0 || uint64(disk.Size) > ^uint64(0)-total {
			return ^uint64(0)
		}
		total += uint64(disk.Size)
	}
	return total
}
