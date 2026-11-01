package metadata

import (
	"container/heap"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/kgpp34/kumaboxd/internal/pool"
	bolt "go.etcd.io/bbolt"
)

// poolCache indexes validated state. Its owner serializes writes and protects
// reads; returned records own their byte slices and cannot mutate the cache.
type poolCache struct {
	pools        map[string]pool.Record
	slots        map[string]map[string]pool.Slot
	ready        map[string]map[string]pool.Slot
	leases       map[string]pool.Lease
	requests     map[string]string
	receipts     receiptHeap
	receiptIndex map[string]*receiptEntry
}

func newPoolCache() *poolCache {
	return &poolCache{pools: map[string]pool.Record{}, slots: map[string]map[string]pool.Slot{}, ready: map[string]map[string]pool.Slot{}, leases: map[string]pool.Lease{}, requests: map[string]string{}, receiptIndex: map[string]*receiptEntry{}}
}
func loadPoolCache(tx *bolt.Tx) (*poolCache, error) {
	c := newPoolCache()
	reader := boltPoolReader{tx}
	records, err := reader.Pools()
	if err != nil {
		return nil, err
	}
	d := newPoolDelta()
	for _, r := range records {
		d.Pools[r.Spec.Name] = r
		slots, err := reader.Slots(r.Spec.Name)
		if err != nil {
			return nil, err
		}
		for _, slot := range slots {
			d.Slots[slotKey(slot.Pool, slot.ID)] = slot
		}
	}
	if err := reader.bucket(3).ForEach(func(k, v []byte) error {
		lease, err := reader.Lease(string(k))
		if err != nil {
			return err
		}
		d.Leases[lease.ID] = leaseRow{lease, lease.KeyHash, lease.RequestHash}
		return nil
	}); err != nil {
		return nil, err
	}
	c.apply(d)
	return c, nil
}
func (c *poolCache) Pool(name string) (pool.Record, error) {
	v, ok := c.pools[name]
	if !ok {
		return v, pool.ErrNotFound
	}
	return cloneRecord(v), nil
}
func (c *poolCache) Pools() ([]pool.Record, error) {
	all := make([]pool.Record, 0, len(c.pools))
	for _, v := range c.pools {
		all = append(all, cloneRecord(v))
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Spec.Name < all[j].Spec.Name })
	return all, nil
}
func (c *poolCache) Slot(name, id string) (pool.Slot, error) {
	v, ok := c.slots[name][id]
	if !ok {
		return v, pool.ErrNotFound
	}
	return v, nil
}
func (c *poolCache) Slots(name string) ([]pool.Slot, error) {
	all := make([]pool.Slot, 0, len(c.slots[name]))
	for _, v := range c.slots[name] {
		all = append(all, v)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	return all, nil
}
func (c *poolCache) Ready(name string) (pool.Slot, error) {
	for _, v := range c.ready[name] {
		return v, nil
	}
	return pool.Slot{}, pool.ErrEmpty
}
func (c *poolCache) Lease(id string) (pool.Lease, error) {
	v, ok := c.leases[id]
	if !ok {
		return v, pool.ErrNotFound
	}
	return v, nil
}
func (c *poolCache) Request(name, hash string) (pool.Lease, error) {
	id, ok := c.requests[slotKey(name, hash)]
	if !ok {
		return pool.Lease{}, pool.ErrNotFound
	}
	return c.Lease(id)
}

// ReceiptsBefore prunes future heap branches and visits at most twice the result
// limit. It need not scan every retained receipt while claim waits for the lock.
func (c *poolCache) ReceiptsBefore(before time.Time, limit int) ([]pool.Lease, error) {
	if limit < 1 || limit > 256 {
		return nil, pool.ErrInvalid
	}
	out := make([]pool.Lease, 0, limit)
	stack := []int{0}
	for len(stack) > 0 && len(out) < limit {
		i := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if i >= len(c.receipts) || !c.receipts[i].at.Before(before) {
			continue
		}
		entry := c.receipts[i]
		v, ok := c.leases[entry.id]
		if !ok {
			return nil, errors.New("receipt index references missing lease")
		}
		out = append(out, v)
		stack = append(stack, 2*i+1, 2*i+2)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.Before(out[j].UpdatedAt) })
	return out, nil
}
func (c *poolCache) apply(d poolDelta) {
	for name, r := range d.Pools {
		c.pools[name] = cloneRecord(r)
	}
	for k, v := range d.Slots {
		if d.RemovedSlots[k] {
			continue
		}
		if c.slots[v.Pool] == nil {
			c.slots[v.Pool] = map[string]pool.Slot{}
			c.ready[v.Pool] = map[string]pool.Slot{}
		}
		c.slots[v.Pool][v.ID] = v
		if v.State == pool.Ready {
			c.ready[v.Pool][v.ID] = v
		} else {
			delete(c.ready[v.Pool], v.ID)
		}
	}
	for k := range d.RemovedSlots {
		name, id, _ := strings.Cut(k, "\x00")
		delete(c.slots[name], id)
		delete(c.ready[name], id)
	}
	for id, row := range d.Leases {
		if item := c.receiptIndex[id]; item != nil {
			heap.Remove(&c.receipts, item.index)
			delete(c.receiptIndex, id)
		}
		if old, ok := c.leases[id]; ok {
			delete(c.requests, slotKey(old.Pool, old.KeyHash))
		}
		v := restoreLease(row)
		if d.RemovedLeases[id] {
			delete(c.leases, id)
			continue
		}
		c.leases[id] = v
		c.requests[slotKey(v.Pool, v.KeyHash)] = id
		if (v.State == pool.Released || v.State == pool.Lost) && c.slots[v.Pool][v.SlotID].ID == "" {
			item := &receiptEntry{id: id, at: v.UpdatedAt}
			heap.Push(&c.receipts, item)
			c.receiptIndex[id] = item
		}
	}
}

type receiptEntry struct {
	id    string
	at    time.Time
	index int
}
type receiptHeap []*receiptEntry

func (h receiptHeap) Len() int           { return len(h) }
func (h receiptHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h receiptHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *receiptHeap) Push(v any) {
	entry := v.(*receiptEntry)
	entry.index = len(*h)
	*h = append(*h, entry)
}
func (h *receiptHeap) Pop() any {
	last := len(*h) - 1
	v := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	return v
}
