package pool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/kgpp34/kumaboxd/internal/runtimeclient"
)

// Service owns policy execution and lease transitions. The store supplies atomic
// allocation; runtime calls never execute while a metadata transaction is open.
type Service struct {
	store   Store
	runtime Runtime
	catalog Catalog
	policy  Policy
	options Options
	logger  *slog.Logger
	wake    chan struct{}
	moreGC  atomic.Bool
	running atomic.Bool
	healthy atomic.Bool
	started atomic.Bool
	metrics measurements
}

// New keeps the policy replaceable without coupling algorithms to HTTP or VM work.
func New(store Store, runtime Runtime, catalog Catalog, policy Policy, options Options, logger *slog.Logger) (*Service, error) {
	if store == nil || runtime == nil || catalog == nil || policy == nil || logger == nil || !ValidID(store.NodeID()) {
		return nil, errors.New("pool store, runtime, catalog, policy, logger and node identity are required")
	}
	if err := options.Validate(); err != nil {
		return nil, err
	}
	return &Service{store: store, runtime: runtime, catalog: catalog, policy: policy, options: options, logger: logger, wake: make(chan struct{}, 1)}, nil
}
func (s *Service) notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Configure binds a pool to one immutable template; repeated calls can adjust its
// inventory target and limit. TargetReady=0 drains spare stock, never leased VMs.
func (s *Service) Configure(ctx context.Context, spec Spec) (Record, bool, error) {
	if err := spec.Validate(); err != nil {
		return Record{}, false, err
	}
	if spec.MaxTotal > s.options.MaxVMs {
		return Record{}, false, fmt.Errorf("%w: max_total exceeds the node VM budget", ErrInvalid)
	}
	captured, err := s.catalog.Get(ctx, spec.Template)
	if err != nil {
		return Record{}, false, err
	}
	if err := captured.Validate(); err != nil {
		return Record{}, false, err
	}
	var shape runtimeclient.GuestConfig
	if err := json.Unmarshal(captured.GuestConfig, &shape); err != nil {
		return Record{}, false, err
	}
	if !s.fits(reservation{}, shape) {
		return Record{}, false, fmt.Errorf("%w: template shape exceeds node reservation budgets", ErrInvalid)
	}
	var result Record
	created := false
	err = s.store.Write(ctx, func(tx WriteTx) error {
		existing, err := tx.Pool(spec.Name)
		switch {
		case err == nil:
			if existing.Spec.Template != spec.Template || existing.Template.SnapshotID != captured.SnapshotID {
				return ErrConflict
			}
			result = existing
			result.Spec = spec
		case errors.Is(err, ErrNotFound):
			all, err := tx.Pools()
			if err != nil {
				return err
			}
			if len(all) >= 128 {
				return fmt.Errorf("%w: node pool limit reached", ErrConflict)
			}
			result = Record{Spec: spec, Template: captured, CreatedAt: time.Now().UTC()}
			created = true
		default:
			return err
		}
		tx.SavePool(result)
		return nil
	})
	if err == nil {
		s.notify()
	}
	return result, created, err
}

// Status includes bounded cumulative observations for later policy evaluation.
type Status struct {
	ControllerReady bool       `json:"controller_ready"`
	Durability      Durability `json:"durability"`
	Record          Record     `json:"pool"`
	Counts          Counts     `json:"counts"`
	Metrics         Metrics    `json:"metrics"`
}

func (s *Service) Status(ctx context.Context, name string) (Status, error) {
	if err := validName(name); err != nil {
		return Status{}, err
	}
	out := Status{ControllerReady: s.Ready(), Durability: s.store.Durability()}
	err := s.store.Read(ctx, func(tx ReadTx) error {
		r, err := tx.Pool(name)
		if err != nil {
			return err
		}
		slots, err := tx.Slots(name)
		out.Record = r
		out.Counts = count(slots)
		return err
	})
	if err == nil {
		out.Metrics = s.metrics.snapshot(name)
	}
	return out, err
}
func (s *Service) List(ctx context.Context) ([]Status, error) {
	out := make([]Status, 0)
	err := s.store.Read(ctx, func(tx ReadTx) error {
		all, err := tx.Pools()
		if err != nil {
			return err
		}
		for _, r := range all {
			slots, err := tx.Slots(r.Spec.Name)
			if err != nil {
				return err
			}
			out = append(out, Status{ControllerReady: s.Ready(), Durability: s.store.Durability(), Record: r, Counts: count(slots)})
		}
		return nil
	})
	if err == nil {
		for i := range out {
			out[i].Metrics = s.metrics.snapshot(out[i].Record.Spec.Name)
		}
	}
	return out, err
}

// Claim returns only a fully initialized Ready VM. Empty inventory fails fast;
// it never silently falls back to clone or waits for a controller calculation.
// Repeating the same key and payload returns the original active lease.
func (s *Service) Claim(ctx context.Context, name string, request ClaimRequest) (Lease, bool, error) {
	if err := validName(name); err != nil {
		return Lease{}, false, err
	}
	if !text(request.Key, 128) || !text(request.Owner, 128) || request.TTLSeconds < 1 || request.TTLSeconds > int64(s.options.MaxTTL/time.Second) {
		return Lease{}, false, fmt.Errorf("%w: idempotency key, owner and bounded positive ttl_seconds are required", ErrInvalid)
	}
	if !s.Ready() {
		return Lease{}, false, ErrUnavailable
	}
	started := time.Now()
	created := false
	known := false
	var result Lease
	keyHash := digest([]byte(request.Key))
	payload, _ := json.Marshal(struct {
		Owner string
		TTL   int64
	}{request.Owner, request.TTLSeconds})
	requestHash := digest(payload)
	err := s.store.Write(ctx, func(tx WriteTx) error {
		r, err := tx.Pool(name)
		if err != nil {
			return err
		}
		known = true
		previous, err := tx.Request(name, keyHash)
		if err == nil {
			if previous.RequestHash != requestHash {
				return ErrConflict
			}
			if previous.State != Active || !time.Now().Before(previous.ExpiresAt) {
				return ErrExpired
			}
			result = previous
			return nil
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		if r.Spec.TargetReady == 0 {
			return ErrEmpty
		}
		slot, err := tx.Ready(name)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		result = Lease{Durability: s.store.Durability(), ID: newID(), Pool: name, SlotID: slot.ID, SandboxID: slot.SandboxID, Owner: request.Owner, State: Active, KeyHash: keyHash, RequestHash: requestHash, CreatedAt: now, UpdatedAt: now, ExpiresAt: now.Add(time.Duration(request.TTLSeconds) * time.Second)}
		slot.State = Leased
		slot.LeaseID = result.ID
		slot.UpdatedAt = now
		tx.SaveSlot(slot)
		tx.SaveLease(result)
		created = true
		return nil
	})
	if known {
		s.metrics.update(name, func(m *Metrics) {
			m.ClaimAttempts++
			observe(&m.ClaimLatency, time.Since(started))
			if err == nil {
				if created {
					m.Claimed++
				} else {
					m.Replays++
				}
			} else if errors.Is(err, ErrEmpty) {
				m.Misses++
			}
		})
	}
	if err == nil && created {
		s.notify()
	}
	return result, created, err
}

// GetLease returns the recorded receipt, including its original expiry deadline.
func (s *Service) GetLease(ctx context.Context, id string) (Lease, error) {
	if !ValidID(id) {
		return Lease{}, ErrInvalid
	}
	var result Lease
	err := s.store.Read(ctx, func(tx ReadTx) error { var err error; result, err = tx.Lease(id); return err })
	return result, err
}

// Renew accepts an absolute deadline and refuses resurrection after expiry.
func (s *Service) Renew(ctx context.Context, id string, expires time.Time) (Lease, error) {
	if !ValidID(id) || expires.IsZero() {
		return Lease{}, ErrInvalid
	}
	var result Lease
	err := s.store.Write(ctx, func(tx WriteTx) error {
		lease, err := tx.Lease(id)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		if lease.State != Active || !now.Before(lease.ExpiresAt) {
			return ErrExpired
		}
		if expires.Equal(lease.ExpiresAt) {
			result = lease
			return nil
		}
		if !expires.After(lease.ExpiresAt) || expires.After(now.Add(s.options.MaxTTL)) {
			return ErrInvalid
		}
		lease.Durability = s.store.Durability()
		lease.ExpiresAt = expires.UTC()
		lease.UpdatedAt = now
		tx.SaveLease(lease)
		result = lease
		return nil
	})
	if err == nil {
		s.notify()
	}
	return result, err
}

// Release records revoked allocation before asynchronous runtime reclamation.
// A receipt remains available through ReceiptRetention for retry and inspection.
func (s *Service) Release(ctx context.Context, id string) (Lease, error) {
	if !ValidID(id) {
		return Lease{}, ErrInvalid
	}
	var result Lease
	err := s.store.Write(ctx, func(tx WriteTx) error {
		lease, err := tx.Lease(id)
		if err != nil {
			return err
		}
		if lease.State == Active {
			slot, err := tx.Slot(lease.Pool, lease.SlotID)
			if err != nil {
				return err
			}
			lease.Durability = s.store.Durability()
			lease.State = Releasing
			lease.UpdatedAt = time.Now().UTC()
			slot.State = Retiring
			slot.UpdatedAt = lease.UpdatedAt
			slot.NextAttempt = time.Time{}
			tx.SaveSlot(slot)
			tx.SaveLease(lease)
		}
		result = lease
		return nil
	})
	if err == nil {
		s.notify()
	}
	return result, err
}

func text(value string, maxLen int) bool {
	if len(value) < 1 || len(value) > maxLen || strings.TrimSpace(value) == "" {
		return false
	}
	for _, r := range value {
		if r < 32 || r == 127 {
			return false
		}
	}
	return true
}
func digest(data []byte) string { v := sha256.Sum256(data); return hex.EncodeToString(v[:]) }
