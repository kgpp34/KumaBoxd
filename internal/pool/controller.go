package pool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/kgpp34/kumaboxd/internal/runtimeclient"
)

type work struct {
	slot   Slot
	record Record
}
type outcome struct {
	work     work
	sandbox  runtimeclient.Sandbox
	removed  bool
	missing  bool
	err      error
	duration time.Duration
	retryAt  time.Time
}

// Run is the sole background scheduler. Events trigger immediate replenishment;
// periodic reconciliation handles expiry, failed work and missed notifications.
// All workers finish before Run returns, so the caller may then close the store.
func (s *Service) Run(ctx context.Context) error {
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("pool controller can only run once")
	}
	ticker := time.NewTicker(s.options.ReconcileInterval)
	defer ticker.Stop()
	for {
		if err := s.recover(ctx); err == nil {
			break
		} else {
			s.logger.ErrorContext(ctx, "recover pool inventory", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
	s.healthy.Store(true)
	s.running.Store(true)
	defer s.running.Store(false)
	completed := make(chan outcome, s.options.Workers+s.options.ReclaimWorkers)
	active := map[string]string{}
	var workers sync.WaitGroup
	defer workers.Wait()
	s.notify()
	round := 0
	nextGC := time.Now().Add(time.Minute)
	for {
		select {
		case <-ctx.Done():
			return nil
		case result := <-completed:
			delete(active, result.work.slot.ID)
		case <-s.wake:
		case <-ticker.C:
			healthErr := s.checkHealth(ctx)
			s.healthy.Store(healthErr == nil)
			if healthErr != nil && ctx.Err() == nil {
				s.logger.WarnContext(ctx, "observe pool VM health", "error", healthErr)
			}
		}
		if ctx.Err() != nil {
			return nil
		}
		gc := s.moreGC.Swap(false) || !time.Now().Before(nextGC)
		if gc {
			nextGC = time.Now().Add(time.Minute)
		}
		jobs, err := s.plan(ctx, active, round, gc)
		round++
		if err != nil {
			s.logger.ErrorContext(ctx, "reconcile pool inventory", "error", err)
			continue
		}
		for _, job := range jobs {
			active[job.slot.ID] = job.slot.State
			workers.Add(1)
			go func(job work) {
				defer workers.Done()
				result := s.execute(ctx, job)
				s.finish(ctx, result)
				completed <- result
			}(job)
		}
	}
}

func (s *Service) recover(ctx context.Context) error {
	// Quarantine spare inventory before any allocation can be accepted. Creating
	// intents are inspected by name, never blindly cloned again after a restart.
	var leased []work
	err := s.store.Write(ctx, func(tx WriteTx) error {
		records, err := tx.Pools()
		if err != nil {
			return err
		}
		for _, r := range records {
			if err := r.Spec.Validate(); err != nil {
				return err
			}
			if err := r.Template.Validate(); err != nil {
				return err
			}
			slots, err := tx.Slots(r.Spec.Name)
			if err != nil {
				return err
			}
			for _, slot := range slots {
				if !ValidID(slot.ID) || slot.Pool != r.Spec.Name || slot.Name != s.vmName(slot.ID) {
					return errors.New("invalid persisted pool ownership")
				}
				switch slot.State {
				case Creating:
					slot.State = Retiring
					slot.NextAttempt = time.Time{}
					tx.SaveSlot(slot)
				case Ready:
					slot.State = Checking
					slot.NextAttempt = time.Time{}
					tx.SaveSlot(slot)
				case Checking, Retiring:
				case Leased:
					lease, err := tx.Lease(slot.LeaseID)
					if err != nil {
						return err
					}
					if lease.Pool != slot.Pool || lease.SlotID != slot.ID || lease.SandboxID != slot.SandboxID || lease.State != Active {
						return errors.New("persisted lease and slot disagree")
					}
					leased = append(leased, work{slot, r})
				default:
					return errors.New("invalid persisted slot state")
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, job := range leased {
		checkCtx, cancel := context.WithTimeout(ctx, s.options.OperationTimeout)
		observed, err := s.runtime.InspectSandbox(checkCtx, job.slot.SandboxID)
		cancel()
		if err != nil && !notFound(err) {
			return err
		}
		if err == nil {
			if err := s.owns(job, observed); err != nil {
				return err
			}
			if observed.State == "running" {
				continue
			}
		}
		if err := s.store.Write(ctx, func(tx WriteTx) error {
			slot, err := tx.Slot(job.slot.Pool, job.slot.ID)
			if err != nil {
				return err
			}
			lease, err := tx.Lease(slot.LeaseID)
			if err != nil {
				return err
			}
			slot.State = Retiring
			slot.NextAttempt = time.Time{}
			lease.State = Lost
			lease.UpdatedAt = time.Now().UTC()
			tx.SaveSlot(slot)
			tx.SaveLease(lease)
			return nil
		}); err != nil {
			return err
		}
	}
	return s.checkHealth(ctx)
}

func (s *Service) execute(parent context.Context, job work) outcome {
	ctx, cancel := context.WithTimeout(parent, s.options.OperationTimeout)
	defer cancel()
	started := time.Now()
	result := outcome{work: job}
	switch job.slot.State {
	case Creating:
		result.sandbox, result.err = s.runtime.Clone(ctx, job.record.Template.SnapshotID, runtimeclient.CloneOptions{Name: job.slot.Name, WaitForNetwork: true})
		if result.err == nil {
			result.err = s.probe(ctx, job, result.sandbox)
		}
	case Checking:
		result.sandbox, result.err = s.runtime.InspectSandbox(ctx, reference(job.slot))
		if notFound(result.err) {
			result.err = nil
			result.missing = true
		} else if result.err == nil {
			result.err = s.probe(ctx, job, result.sandbox)
		}
	case Retiring:
		result.sandbox, result.err = s.runtime.InspectSandbox(ctx, reference(job.slot))
		switch {
		case notFound(result.err):
			result.err = nil
			// A name-only intent may belong to an RPC still finishing elsewhere. Retain
			// its reservation until the original operation deadline has elapsed.
			until := job.slot.CreatedAt.Add(s.options.OperationTimeout)
			if job.slot.SandboxID == "" && time.Now().Before(until) {
				result.retryAt = until
			} else {
				result.removed = true
			}
		case result.err == nil:
			if result.err = s.owns(job, result.sandbox); result.err != nil {
				break
			}
			_, result.err = s.runtime.Stop(ctx, result.sandbox.ID, true)
			if notFound(result.err) {
				result.err = nil
				result.removed = true
				break
			}
			if result.err == nil {
				_, result.err = s.runtime.Remove(ctx, result.sandbox.ID)
				if result.err == nil || notFound(result.err) {
					result.err = nil
					result.removed = true
				}
			}
		}
	}
	result.duration = time.Since(started)
	return result
}
func reference(slot Slot) string {
	if slot.SandboxID != "" {
		return slot.SandboxID
	}
	return slot.Name
}
func (s *Service) vmName(id string) string { return "kbd-" + s.store.NodeID()[:16] + "-" + id }
func (s *Service) owns(job work, vm runtimeclient.Sandbox) error {
	if vm.ID == "" || vm.Config.Name != job.slot.Name || job.slot.SandboxID != "" && vm.ID != job.slot.SandboxID || vm.ImageDigest != job.record.Template.ImageDigest || vm.VMM != job.record.Template.VMM {
		return errors.New("runtime sandbox does not match persisted pool ownership")
	}
	var shape runtimeclient.GuestConfig
	if err := json.Unmarshal(job.record.Template.GuestConfig, &shape); err != nil {
		return err
	}
	if vm.Config.CPUs != shape.CPUs || vm.Config.Memory != shape.Memory || vm.Config.Storage != shape.Storage || vm.Config.NICs != shape.NICs || shape.NetworkBackend != "" && vm.Config.NetworkBackend != shape.NetworkBackend {
		return errors.New("runtime sandbox shape does not match its pool template")
	}
	if len(shape.DataDisks) != len(vm.Config.DataDisks) {
		return errors.New("runtime data disks differ from template")
	}
	for i, disk := range shape.DataDisks {
		other := vm.Config.DataDisks[i]
		if disk.Size != other.Size || disk.Name != other.Name {
			return errors.New("runtime data disks differ from template")
		}
	}
	return nil
}
func (s *Service) probe(ctx context.Context, job work, vm runtimeclient.Sandbox) error {
	if err := s.owns(job, vm); err != nil {
		return err
	}
	if vm.State != "running" {
		return errors.New("sandbox is not running")
	}
	code, err := s.runtime.Exec(ctx, vm.ID, runtimeclient.Command{Args: []string{"/bin/true"}}, io.Discard, io.Discard)
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("guest readiness exited %d", code)
	}
	return nil
}
func notFound(err error) bool {
	var remote *runtimeclient.RemoteError
	return errors.As(err, &remote) && remote.Code == "NOT_FOUND"
}

func (s *Service) finish(parent context.Context, result outcome) {
	// Persist even if shutdown canceled the worker. An interrupted clone remains a
	// named intent and is inspected before any subsequent cleanup or allocation.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
	defer cancel()
	err := s.store.Write(ctx, func(tx WriteTx) error {
		slot, err := tx.Slot(result.work.slot.Pool, result.work.slot.ID)
		if err != nil {
			return err
		}
		if slot.State != result.work.slot.State {
			return nil
		}
		now := time.Now().UTC()
		if result.removed {
			if slot.LeaseID != "" {
				lease, err := tx.Lease(slot.LeaseID)
				if err != nil {
					return err
				}
				if lease.State != Lost {
					lease.State = Released
				}
				lease.UpdatedAt = now
				tx.SaveLease(lease)
			}
			tx.RemoveSlot(slot)
			return nil
		}
		if result.sandbox.ID != "" && s.owns(result.work, result.sandbox) == nil {
			slot.SandboxID = result.sandbox.ID
		}
		switch {
		case result.err != nil:
			slot.Attempts++
			slot.NextAttempt = now.Add(time.Second * time.Duration(1<<min(slot.Attempts, 5)))
			if slot.State == Creating {
				slot.State = Retiring
			}
			// Failed restart probes remain quarantined; they never re-enter Ready.
		case result.missing:
			slot.State = Retiring
			slot.NextAttempt = now
		case !result.retryAt.IsZero():
			slot.NextAttempt = result.retryAt
		case slot.State == Creating || slot.State == Checking:
			slot.State = Ready
			slot.NextAttempt = time.Time{}
			slot.Attempts = 0
		}
		slot.UpdatedAt = now
		tx.SaveSlot(slot)
		return nil
	})
	if err != nil {
		s.logger.Error("persist pool worker outcome", "error", err, "slot", result.work.slot.ID)
	}
	if result.err != nil {
		s.logger.Warn("pool runtime work failed", "pool", result.work.slot.Pool, "slot", result.work.slot.ID, "state", result.work.slot.State, "error", result.err)
	}
	if result.work.slot.State == Creating {
		s.metrics.update(result.work.slot.Pool, func(m *Metrics) {
			if result.err == nil && err == nil {
				observe(&m.ProvisionLatency, max(time.Duration(0), time.Since(result.work.slot.CreatedAt)))
				observe(&m.ProvisionRuntimeLatency, result.duration)
				m.Provisioned++
			} else {
				m.ProvisionFailures++
			}
		})
	}
}

func (s *Service) room(state string, active map[string]string, jobs []work) bool {
	cleanup := state == Retiring
	used := 0
	for _, current := range active {
		if (current == Retiring) == cleanup {
			used++
		}
	}
	for _, job := range jobs {
		if (job.slot.State == Retiring) == cleanup {
			used++
		}
	}
	limit := s.options.Workers
	if cleanup {
		limit = s.options.ReclaimWorkers
	}
	return used < limit
}
