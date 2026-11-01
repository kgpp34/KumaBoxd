package metadata

import (
	"errors"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/kgpp34/kumaboxd/internal/pool"
	"github.com/kgpp34/kumaboxd/internal/template"
)

// poolDelta is an owned change set shared by synchronous commits, journal
// records and checkpoints. Leases retain private idempotency fields on disk.
type poolDelta struct {
	Pools         map[string]pool.Record `json:"pools,omitempty"`
	Slots         map[string]pool.Slot   `json:"slots,omitempty"`
	Leases        map[string]leaseRow    `json:"leases,omitempty"`
	RemovedSlots  map[string]bool        `json:"removed_slots,omitempty"`
	RemovedLeases map[string]bool        `json:"removed_leases,omitempty"`
}

func newPoolDelta() poolDelta {
	return poolDelta{Pools: map[string]pool.Record{}, Slots: map[string]pool.Slot{}, Leases: map[string]leaseRow{}, RemovedSlots: map[string]bool{}, RemovedLeases: map[string]bool{}}
}
func (d poolDelta) empty() bool {
	return len(d.Pools)+len(d.Slots)+len(d.Leases)+len(d.RemovedSlots)+len(d.RemovedLeases) == 0
}
func cloneRecord(r pool.Record) pool.Record {
	r.Template.GuestConfig = slices.Clone(r.Template.GuestConfig)
	return r
}

// poolWriteTx stages a callback independently of the persistence policy. Reads
// see its own writes; a failed callback never publishes a partial change set.
type poolWriteTx struct {
	base    pool.ReadTx
	changes poolDelta
}

func newPoolWriteTx(base pool.ReadTx) *poolWriteTx {
	return &poolWriteTx{base: base, changes: newPoolDelta()}
}
func (t *poolWriteTx) Pool(name string) (pool.Record, error) {
	if v, ok := t.changes.Pools[name]; ok {
		return cloneRecord(v), nil
	}
	return t.base.Pool(name)
}
func (t *poolWriteTx) Pools() ([]pool.Record, error) {
	all, err := t.base.Pools()
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for i, v := range all {
		seen[v.Spec.Name] = true
		if next, ok := t.changes.Pools[v.Spec.Name]; ok {
			all[i] = cloneRecord(next)
		}
	}
	for name, v := range t.changes.Pools {
		if !seen[name] {
			all = append(all, cloneRecord(v))
		}
	}
	return all, nil
}
func (t *poolWriteTx) Slot(name, id string) (pool.Slot, error) {
	k := slotKey(name, id)
	if t.changes.RemovedSlots[k] {
		return pool.Slot{}, pool.ErrNotFound
	}
	if v, ok := t.changes.Slots[k]; ok {
		return v, nil
	}
	return t.base.Slot(name, id)
}
func (t *poolWriteTx) Slots(name string) ([]pool.Slot, error) {
	all, err := t.base.Slots(name)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	out := make([]pool.Slot, 0, len(all))
	for _, v := range all {
		k := slotKey(name, v.ID)
		seen[k] = true
		if t.changes.RemovedSlots[k] {
			continue
		}
		if next, ok := t.changes.Slots[k]; ok {
			v = next
		}
		out = append(out, v)
	}
	for k, v := range t.changes.Slots {
		if v.Pool == name && !seen[k] && !t.changes.RemovedSlots[k] {
			out = append(out, v)
		}
	}
	return out, nil
}
func (t *poolWriteTx) Ready(name string) (pool.Slot, error) {
	slot, err := t.base.Ready(name)
	if err == nil {
		key := slotKey(name, slot.ID)
		if !t.changes.RemovedSlots[key] {
			if current, ok := t.changes.Slots[key]; !ok {
				return slot, nil
			} else if current.State == pool.Ready {
				return current, nil
			}
		}
	} else if !errors.Is(err, pool.ErrEmpty) {
		return pool.Slot{}, err
	}

	// Only callbacks that changed the indexed candidate need a fallback scan.
	// The ordinary one-claim transaction always takes the indexed path above.
	if err == nil {
		slots, e := t.Slots(name)
		if e != nil {
			return pool.Slot{}, e
		}
		for _, v := range slots {
			if v.State == pool.Ready {
				return v, nil
			}
		}
	} else {
		for k, v := range t.changes.Slots {
			if v.Pool == name && v.State == pool.Ready && !t.changes.RemovedSlots[k] {
				return v, nil
			}
		}
	}
	return pool.Slot{}, pool.ErrEmpty
}
func (t *poolWriteTx) Lease(id string) (pool.Lease, error) {
	if t.changes.RemovedLeases[id] {
		return pool.Lease{}, pool.ErrNotFound
	}
	if v, ok := t.changes.Leases[id]; ok {
		return restoreLease(v), nil
	}
	return t.base.Lease(id)
}
func (t *poolWriteTx) Request(name, hash string) (pool.Lease, error) {
	for id, v := range t.changes.Leases {
		if v.Lease.Pool == name && v.KeyHash == hash && !t.changes.RemovedLeases[id] {
			return restoreLease(v), nil
		}
	}
	v, err := t.base.Request(name, hash)
	if err != nil {
		return pool.Lease{}, err
	}
	if t.changes.RemovedLeases[v.ID] {
		return pool.Lease{}, pool.ErrNotFound
	}
	if row, ok := t.changes.Leases[v.ID]; ok {
		return restoreLease(row), nil
	}
	return v, nil
}
func (t *poolWriteTx) ReceiptsBefore(before time.Time, limit int) ([]pool.Lease, error) {
	all, err := t.base.ReceiptsBefore(before, limit)
	if err != nil {
		return nil, err
	}
	out := make([]pool.Lease, 0, len(all))
	for _, v := range all {
		if !t.changes.RemovedLeases[v.ID] {
			out = append(out, v)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.Before(out[j].UpdatedAt) })
	return out, nil
}
func (t *poolWriteTx) SavePool(v pool.Record) { t.changes.Pools[v.Spec.Name] = cloneRecord(v) }
func (t *poolWriteTx) SaveSlot(v pool.Slot) {
	k := slotKey(v.Pool, v.ID)
	t.changes.Slots[k] = v
	delete(t.changes.RemovedSlots, k)
}
func (t *poolWriteTx) SaveLease(v pool.Lease) {
	t.changes.Leases[v.ID] = leaseRow{v, v.KeyHash, v.RequestHash}
	delete(t.changes.RemovedLeases, v.ID)
}
func (t *poolWriteTx) RemoveSlot(v pool.Slot) { t.changes.RemovedSlots[slotKey(v.Pool, v.ID)] = true }
func (t *poolWriteTx) RemoveLease(v pool.Lease) {
	t.changes.RemovedLeases[v.ID] = true
	t.changes.Leases[v.ID] = leaseRow{v, v.KeyHash, v.RequestHash}
}

func (d poolDelta) validate() error {
	for k, v := range d.Pools {
		if k != v.Spec.Name {
			return errors.New("pool key mismatch")
		}
		if err := v.Validate(); err != nil {
			return err
		}
	}
	for k, v := range d.Slots {
		if k != slotKey(v.Pool, v.ID) {
			return errors.New("slot key mismatch")
		}
		if !d.RemovedSlots[k] {
			if err := v.Validate(); err != nil {
				return err
			}
		}
	}
	for id, row := range d.Leases {
		v := restoreLease(row)
		if id != v.ID {
			return errors.New("lease key mismatch")
		}
		if err := v.Validate(); err != nil {
			return err
		}
	}
	for k := range d.RemovedSlots {
		if !validSlotKey(k) {
			return errors.New("invalid removed slot key")
		}
	}
	for id := range d.RemovedLeases {
		if _, ok := d.Leases[id]; !ok {
			return errors.New("missing removed lease identity")
		}
	}
	return nil
}
func (d *poolDelta) merge(next poolDelta) {
	for k, v := range next.Pools {
		d.Pools[k] = v
	}
	for k, v := range next.Slots {
		d.Slots[k] = v
		delete(d.RemovedSlots, k)
	}
	for k := range next.RemovedSlots {
		d.RemovedSlots[k] = true
	}
	for k, v := range next.Leases {
		d.Leases[k] = v
		delete(d.RemovedLeases, k)
	}
	for k := range next.RemovedLeases {
		d.RemovedLeases[k] = true
	}
}

func validSlotKey(k string) bool {
	name, id, ok := strings.Cut(k, "\x00")
	return ok && template.ValidateName(name) == nil && pool.ValidID(id)
}
