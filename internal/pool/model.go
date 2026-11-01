// Package pool owns node-local warm inventory and persisted leases. VM, disk and
// network lifecycle operations remain behind the runtime RPC boundary.
//
// golden -> creating -> checking -> ready -> leased -> retiring -> removed
//
//	^                      |
//	+--- restart probe     +-- release / expiry
package pool

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/kgpp34/kumaboxd/internal/runtimeclient"
	"github.com/kgpp34/kumaboxd/internal/template"
)

var (
	ErrInvalid     = errors.New("invalid pool input")
	ErrNotFound    = errors.New("pool resource not found")
	ErrConflict    = errors.New("pool resource conflict")
	ErrEmpty       = errors.New("no ready sandbox available")
	ErrExpired     = errors.New("lease is no longer active")
	ErrUnavailable = errors.New("pool controller is not ready")
)

// Spec configures fixed spare inventory. MaxTotal includes leased, creating and
// retiring VMs: capacity is released only after runtime confirms removal.
type Spec struct {
	Name        string `json:"name"`
	Template    string `json:"template"`
	TargetReady int    `json:"target_ready"`
	MaxTotal    int    `json:"max_total"`
}

func (s Spec) Validate() error {
	if template.ValidateName(s.Name) != nil || template.ValidateName(s.Template) != nil || s.TargetReady < 0 || s.MaxTotal < 1 || s.TargetReady > s.MaxTotal {
		return fmt.Errorf("%w: names, 0 <= target_ready <= max_total and max_total >= 1 are required", ErrInvalid)
	}
	return nil
}

// Record pins an immutable template; policy settings may change independently.
type Record struct {
	Spec      Spec              `json:"spec"`
	Template  template.Template `json:"template"`
	CreatedAt time.Time         `json:"created_at"`
}

// Slot records a provisioning intent before any runtime mutation. Name is unique
// to this node database and survives lost clone responses and process restarts.
type Slot struct {
	ID          string    `json:"id"`
	Pool        string    `json:"pool"`
	Name        string    `json:"name"`
	SandboxID   string    `json:"sandbox_id,omitempty"`
	State       string    `json:"state"`
	LeaseID     string    `json:"lease_id,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	NextAttempt time.Time `json:"next_attempt,omitempty"`
	Attempts    int       `json:"attempts"`
}

const (
	Creating  = "creating"
	Checking  = "checking"
	Ready     = "ready"
	Leased    = "leased"
	Retiring  = "retiring"
	Active    = "active"
	Releasing = "releasing"
	Released  = "released"
	Lost      = "lost"
)

// Durability states whether an acknowledgement waits for stable storage.
type Durability string

const (
	// DurabilitySync waits for stable storage before acknowledging changes.
	DurabilitySync Durability = "sync"
	// DurabilityBuffered accepts an OS-buffered log append before checkpointing.
	DurabilityBuffered Durability = "buffered"
)

// Lease records allocation identity, expiry and acknowledgement durability.
// KeyHash retains no raw key; absolute renewal deadlines make retries stable.
type Lease struct {
	// Durability is empty only on legacy receipts created before this field existed.
	Durability  Durability `json:"durability,omitempty"`
	ID          string     `json:"id"`
	Pool        string     `json:"pool"`
	SlotID      string     `json:"slot_id"`
	SandboxID   string     `json:"sandbox_id"`
	Owner       string     `json:"owner"`
	State       string     `json:"state"`
	KeyHash     string     `json:"-"`
	RequestHash string     `json:"-"`
	CreatedAt   time.Time  `json:"created_at"`
	ExpiresAt   time.Time  `json:"expires_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

// ClaimRequest's key comes from Idempotency-Key; it is not a VM name or guest input.
type ClaimRequest struct {
	Key        string `json:"-"`
	Owner      string `json:"owner"`
	TTLSeconds int64  `json:"ttl_seconds"`
}

// ReadTx exposes bounded domain reads, not database handles or byte slices.
// Transactions must not escape their callback or make runtime calls.
type ReadTx interface {
	Pool(string) (Record, error)
	Pools() ([]Record, error)
	Slot(string, string) (Slot, error)
	Slots(string) ([]Slot, error)
	Ready(string) (Slot, error)
	Lease(string) (Lease, error)
	Request(string, string) (Lease, error)
	ReceiptsBefore(time.Time, int) ([]Lease, error)
}

// WriteTx stages domain changes. A failed callback publishes none of its writes.
type WriteTx interface {
	ReadTx
	SavePool(Record)
	SaveSlot(Slot)
	SaveLease(Lease)
	RemoveSlot(Slot)
	RemoveLease(Lease)
}

// Store serializes atomic transitions under an explicit acknowledgement policy.
type Store interface {
	NodeID() string
	Durability() Durability
	Read(context.Context, func(ReadTx) error) error
	Write(context.Context, func(WriteTx) error) error
}

// Provisioner prepares independent guest identity and reports command readiness.
type Provisioner interface {
	Clone(context.Context, string, runtimeclient.CloneOptions) (runtimeclient.Sandbox, error)
	Exec(context.Context, string, runtimeclient.Command, io.Writer, io.Writer) (int, error)
}

// Observer supplies actual VM identity and state after restarts or lost replies.
type Observer interface {
	InspectSandbox(context.Context, string) (runtimeclient.Sandbox, error)
	Status(context.Context, ...string) ([]runtimeclient.SandboxStatus, error)
}

// Reclaimer removes used or failed VMs instead of returning them to spare stock.
type Reclaimer interface {
	Stop(context.Context, string, bool) (runtimeclient.Sandbox, error)
	Remove(context.Context, string) (runtimeclient.Sandbox, error)
}

// Runtime composes only the capabilities consumed by the controller.
type Runtime interface {
	Provisioner
	Observer
	Reclaimer
}

// Catalog resolves an already registered immutable template.
type Catalog interface {
	Get(context.Context, string) (template.Template, error)
}

// Counts describe inventory, including reservations that still consume capacity.
type Counts struct {
	Ready    int `json:"ready"`
	Creating int `json:"creating"`
	Checking int `json:"checking"`
	Leased   int `json:"leased"`
	Retiring int `json:"retiring"`
	Total    int `json:"total"`
}

func count(slots []Slot) Counts {
	var c Counts
	for _, s := range slots {
		c.Total++
		switch s.State {
		case Ready:
			c.Ready++
		case Creating:
			c.Creating++
		case Checking:
			c.Checking++
		case Leased:
			c.Leased++
		case Retiring:
			c.Retiring++
		}
	}
	return c
}

// PoolObservation contains inventory and demand measurements for one template.
type PoolObservation struct {
	CPUs    uint32
	Memory  uint64
	Storage uint64
	Spec    Spec
	Counts  Counts
	Metrics Metrics
}

// Observation is copied before invoking policy code, outside database transactions.
// A joint policy can distribute spare capacity across all templates on this node.
type Observation struct {
	At              time.Time
	Pools           []PoolObservation
	ReservedVMs     int
	ReservedVCPUs   uint64
	ReservedMemory  uint64
	ReservedStorage uint64
	Limits          Options
}

// Decision controls spare inventory and background provisioning independently.
// The executor always rechecks resource budgets and clamps worker limits.
type Decision struct {
	TargetReady       int
	RefillConcurrency int
}

// Policy plans ahead without participating in allocation or persistence.
type Policy interface {
	Decide(context.Context, Observation) (map[string]Decision, error)
}

// FixedPolicy is the first-stage control law and fallback for future algorithms.
type FixedPolicy struct{}

func (FixedPolicy) Decide(_ context.Context, o Observation) (map[string]Decision, error) {
	out := make(map[string]Decision, len(o.Pools))
	for _, p := range o.Pools {
		out[p.Spec.Name] = Decision{TargetReady: p.Spec.TargetReady, RefillConcurrency: o.Limits.Workers}
	}
	return out, nil
}

// Options bound pool-owned reservations and background work, not claim requests.
// Budgets charge the configured shape conservatively, including in-flight work.
type Options struct {
	PolicyTimeout     time.Duration
	Workers           int
	ReclaimWorkers    int
	StorageBudget     uint64
	MaxVMs            int
	MaxVCPUs          uint64
	MemoryBudget      uint64
	OperationTimeout  time.Duration
	ReconcileInterval time.Duration
	MaxTTL            time.Duration
	ReceiptRetention  time.Duration
}

// DefaultOptions caps spare inventory conservatively until an operator supplies
// node-specific reservation budgets. Logical disk bytes are charged in full.
func DefaultOptions() Options {
	return Options{
		PolicyTimeout:     20 * time.Millisecond,
		Workers:           4,
		ReclaimWorkers:    2,
		MaxVMs:            64,
		MaxVCPUs:          64,
		MemoryBudget:      4 << 30,
		StorageBudget:     256 << 30,
		OperationTimeout:  2 * time.Minute,
		ReconcileInterval: time.Second,
		MaxTTL:            24 * time.Hour,
		ReceiptRetention:  24 * time.Hour,
	}
}

// Validate rejects invalid executor and admission limits before workers start.
func (o Options) Validate() error {
	switch {
	case o.PolicyTimeout <= 0 || o.PolicyTimeout > time.Second:
		return fmt.Errorf("%w: policy timeout must be within (0, 1s]", ErrInvalid)
	case o.Workers < 1 || o.Workers > 256 || o.ReclaimWorkers < 1 || o.ReclaimWorkers > 256:
		return fmt.Errorf("%w: provisioning and reclaim workers must be within 1..256", ErrInvalid)
	case o.MaxVMs < 1 || o.MaxVMs > 100000 || o.MaxVCPUs == 0 || o.MemoryBudget == 0 || o.StorageBudget == 0:
		return fmt.Errorf("%w: bounded positive VM, vCPU, memory and disk budgets are required", ErrInvalid)
	case o.OperationTimeout <= 0 || o.OperationTimeout > 24*time.Hour || o.ReconcileInterval <= 0:
		return fmt.Errorf("%w: operation timeout must be within (0, 24h] and reconciliation interval positive", ErrInvalid)
	case o.MaxTTL <= 0 || o.MaxTTL > 7*24*time.Hour || o.ReceiptRetention < time.Minute:
		return fmt.Errorf("%w: lease TTL must be within (0, 7d] and receipt retention at least 1m", ErrInvalid)
	}
	return nil
}

func newID() string {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		panic(fmt.Sprintf("generate cryptographic identity: %v", err))
	}
	return hex.EncodeToString(id[:])
}

// ValidID recognizes the canonical random node, slot and lease identifier.
func ValidID(id string) bool {
	b, e := hex.DecodeString(id)
	return e == nil && len(b) == 16 && len(id) == 32
}

func validName(value string) error {
	if template.ValidateName(value) != nil {
		return ErrInvalid
	}
	return nil
}

// Validate rejects records whose policy and pinned template disagree.
func (r Record) Validate() error {
	if err := r.Spec.Validate(); err != nil {
		return err
	}
	if err := r.Template.Validate(); err != nil {
		return err
	}
	if r.Spec.Template != r.Template.Name || r.CreatedAt.IsZero() {
		return errors.New("invalid pool template binding")
	}
	return nil
}

// Validate checks ownership and state before a record is eligible for allocation.
func (s Slot) Validate() error {
	if !ValidID(s.ID) || validName(s.Pool) != nil || !text(s.Name, 63) || s.CreatedAt.IsZero() || s.UpdatedAt.IsZero() || s.Attempts < 0 {
		return errors.New("invalid persisted slot identity")
	}
	switch s.State {
	case Creating, Checking, Retiring:
	case Ready:
		if s.SandboxID == "" || s.LeaseID != "" {
			return errors.New("invalid ready slot")
		}
	case Leased:
		if s.SandboxID == "" || !ValidID(s.LeaseID) {
			return errors.New("invalid leased slot")
		}
	default:
		return errors.New("invalid slot state")
	}
	return nil
}

// Validate preserves the idempotency receipt's link to one allocation.
func (l Lease) Validate() error {
	if l.Durability != "" && l.Durability != DurabilitySync && l.Durability != DurabilityBuffered {
		return errors.New("invalid lease durability")
	}
	if !ValidID(l.ID) || !ValidID(l.SlotID) || validName(l.Pool) != nil || l.SandboxID == "" || !text(l.Owner, 128) || l.CreatedAt.IsZero() || l.UpdatedAt.IsZero() || !l.ExpiresAt.After(l.CreatedAt) {
		return errors.New("invalid persisted lease")
	}
	for _, value := range []string{l.KeyHash, l.RequestHash} {
		b, err := hex.DecodeString(value)
		if err != nil || len(b) != 32 {
			return errors.New("invalid lease request identity")
		}
	}
	switch l.State {
	case Active, Releasing, Released, Lost:
		return nil
	default:
		return errors.New("invalid lease state")
	}
}
