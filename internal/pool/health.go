package pool

import (
	"context"
	"errors"
	"time"

	"github.com/kgpp34/kumaboxd/internal/runtimeclient"
)

// Ready reports controller recovery and the last successful runtime observation.
// It does not promise that a particular template has available inventory.
func (s *Service) Ready() bool { return s.running.Load() && s.healthy.Load() }

// checkHealth batches live VMM observation outside metadata transactions. Runtime
// events can replace this polling boundary later without changing lease rules.
// A VM may still fail after observation; allocation does not claim atomicity with
// an external process's lifetime.
func (s *Service) checkHealth(parent context.Context) error {
	var candidates []Slot
	err := s.store.Read(parent, func(tx ReadTx) error {
		all, err := tx.Pools()
		if err != nil {
			return err
		}
		for _, r := range all {
			slots, err := tx.Slots(r.Spec.Name)
			if err != nil {
				return err
			}
			for _, slot := range slots {
				if slot.State == Ready || slot.State == Leased {
					candidates = append(candidates, slot)
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(candidates) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(parent, min(s.options.OperationTimeout, 2*time.Second))
	defer cancel()
	references := make([]string, len(candidates))
	for i, slot := range candidates {
		references[i] = slot.SandboxID
	}
	observed, err := s.runtime.Status(ctx, references...)
	if err != nil {
		// A VM can leave inventory while a batch observation is in flight. Retry
		// only still-eligible IDs; a retiring VM must not block unrelated claims.
		observed = nil
		for _, slot := range candidates {
			eligible, checkErr := s.eligible(ctx, slot)
			if checkErr != nil {
				return checkErr
			}
			if !eligible {
				continue
			}
			one, observeErr := s.runtime.Status(ctx, slot.SandboxID)
			if notFound(observeErr) {
				one = []runtimeclient.SandboxStatus{{Sandbox: runtimeclient.Sandbox{ID: slot.SandboxID}, Runtime: "absent", Stale: true}}
				observeErr = nil
			}
			if observeErr != nil {
				eligible, checkErr = s.eligible(ctx, slot)
				if checkErr != nil {
					return checkErr
				}
				if !eligible {
					continue
				}
				return observeErr
			}
			observed = append(observed, one...)
		}
	}
	statuses := make(map[string]runtimeclient.SandboxStatus, len(observed))
	for _, status := range observed {
		statuses[status.Sandbox.ID] = status
	}
	unhealthy := make([]Slot, 0)
	for _, slot := range candidates {
		status, ok := statuses[slot.SandboxID]
		if !ok {
			eligible, err := s.eligible(ctx, slot)
			if err != nil {
				return err
			}
			if !eligible {
				continue
			}
			return errors.New("incomplete runtime observation")
		}
		if status.Stale || status.Runtime != "running" || status.Sandbox.State != "running" {
			unhealthy = append(unhealthy, slot)
		}
	}
	if len(unhealthy) == 0 {
		return nil
	}
	return s.store.Write(ctx, func(tx WriteTx) error {
		for _, previous := range unhealthy {
			slot, err := tx.Slot(previous.Pool, previous.ID)
			if err != nil {
				return err
			}
			if slot.SandboxID != previous.SandboxID || slot.State != Ready && slot.State != Leased {
				continue
			}
			now := time.Now().UTC()
			if slot.State == Leased {
				lease, err := tx.Lease(slot.LeaseID)
				if err != nil {
					return err
				}
				lease.State = Lost
				lease.UpdatedAt = now
				tx.SaveLease(lease)
			}
			slot.State = Retiring
			slot.NextAttempt = time.Time{}
			slot.UpdatedAt = now
			tx.SaveSlot(slot)
		}
		return nil
	})
}

func (s *Service) eligible(ctx context.Context, slot Slot) (bool, error) {
	eligible := false
	err := s.store.Read(ctx, func(tx ReadTx) error {
		current, err := tx.Slot(slot.Pool, slot.ID)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		eligible = current.SandboxID == slot.SandboxID && (current.State == Ready || current.State == Leased)
		return nil
	})
	return eligible, err
}
