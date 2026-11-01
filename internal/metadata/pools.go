package metadata

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/kgpp34/kumaboxd/internal/pool"
	bolt "go.etcd.io/bbolt"
)

var poolBuckets = [][]byte{[]byte("pool-config-v1"), []byte("pool-slots-v1"), []byte("pool-ready-v1"), []byte("pool-leases-v1"), []byte("pool-requests-v1"), []byte("pool-receipts-v1")}
var nodeIDKey = []byte("node-id")
var poolFormatKey = []byte("pool-format")

const poolFormat = "kumaboxd.pools.v1"

var _ pool.Store = (*Store)(nil)

// initializePools adds a versioned namespace without changing template records.
func initializePools(root *bolt.Bucket) error {
	marker := root.Get(poolFormatKey)
	if marker != nil {
		if string(marker) != poolFormat || !pool.ValidID(string(root.Get(nodeIDKey))) {
			return errors.New("unsupported pool metadata format or identity")
		}
		for _, key := range poolBuckets {
			if root.Bucket(key) == nil {
				return errors.New("incomplete pool metadata schema")
			}
		}
		return nil
	}
	if root.Get(nodeIDKey) != nil {
		return errors.New("pool identity exists without its schema marker")
	}
	for _, key := range poolBuckets {
		if root.Bucket(key) != nil {
			return errors.New("unversioned pool metadata namespace")
		}
		if _, err := root.CreateBucket(key); err != nil {
			return err
		}
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return err
	}
	if err := root.Put(nodeIDKey, []byte(hex.EncodeToString(id[:]))); err != nil {
		return err
	}
	return root.Put(poolFormatKey, []byte(poolFormat))
}

// NodeID fences runtime names to this private node database, not a hostname.
func (s *Store) NodeID() string { return s.nodeID }

// Read provides a consistent snapshot. Values are decoded copies owned by callers.
func (s *Store) Read(ctx context.Context, f func(pool.ReadTx) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return f(newPoolTx(tx))
	})
}

// Write stages a request's changes before applying them inside the shared durable
// transaction. A business error cannot leak partial writes into another request.
func (s *Store) Write(ctx context.Context, f func(pool.WriteTx) error) error {
	request := &writeRequest{ctx: ctx, reply: make(chan writeResult, 1), apply: func(tx *bolt.Tx) (writeResult, error) {
		staged := newPoolTx(tx)
		if err := f(staged); err != nil {
			return writeResult{err: err}, nil //nolint:nilerr // A business error aborts only this staged request, not the shared transaction.
		}
		changed := len(staged.pools)+len(staged.slots)+len(staged.leases)+len(staged.removedSlots) > 0
		return writeResult{changed: changed}, staged.flush()
	}}
	return s.writer.enqueue(request).err
}

type poolTx struct {
	tx            *bolt.Tx
	pools         map[string]pool.Record
	slots         map[string]pool.Slot
	leases        map[string]pool.Lease
	removedSlots  map[string]bool
	removedLeases map[string]bool
}

func newPoolTx(tx *bolt.Tx) *poolTx {
	return &poolTx{tx: tx, pools: map[string]pool.Record{}, slots: map[string]pool.Slot{}, leases: map[string]pool.Lease{}, removedSlots: map[string]bool{}, removedLeases: map[string]bool{}}
}
func (t *poolTx) bucket(n int) *bolt.Bucket { return t.tx.Bucket(metadataKey).Bucket(poolBuckets[n]) }
func slotKey(name, id string) string        { return name + "\x00" + id }
func readValue[T any](b *bolt.Bucket, key string) (out T, err error) {
	data := b.Get([]byte(key))
	if data == nil {
		return out, pool.ErrNotFound
	}
	err = json.Unmarshal(data, &out)
	if err != nil {
		err = fmt.Errorf("decode pool metadata: %w", err)
	}
	return
}
func writeValue[T any](b *bolt.Bucket, key string, value T) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return b.Put([]byte(key), data)
}
func scan[T any](b *bolt.Bucket, prefix string) ([]T, error) {
	out := make([]T, 0)
	cursor := b.Cursor()
	for k, v := cursor.Seek([]byte(prefix)); k != nil && strings.HasPrefix(string(k), prefix); k, v = cursor.Next() {
		var value T
		if err := json.Unmarshal(v, &value); err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, nil
}
func (t *poolTx) Pool(name string) (pool.Record, error) {
	if v, ok := t.pools[name]; ok {
		return v, nil
	}
	v, err := readValue[pool.Record](t.bucket(0), name)
	if err != nil {
		return v, err
	}
	if v.Spec.Name != name {
		return v, errors.New("pool key mismatch")
	}
	return v, v.Validate()
}
func (t *poolTx) Pools() ([]pool.Record, error) {
	list, err := scan[pool.Record](t.bucket(0), "")
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for i, v := range list {
		if err := v.Validate(); err != nil {
			return nil, err
		}
		seen[v.Spec.Name] = true
		if next, ok := t.pools[v.Spec.Name]; ok {
			list[i] = next
		}
	}
	for k, v := range t.pools {
		if !seen[k] {
			list = append(list, v)
		}
	}
	return list, nil
}
func (t *poolTx) Slot(name, id string) (pool.Slot, error) {
	key := slotKey(name, id)
	if t.removedSlots[key] {
		return pool.Slot{}, pool.ErrNotFound
	}
	if v, ok := t.slots[key]; ok {
		return v, nil
	}
	v, err := readValue[pool.Slot](t.bucket(1), key)
	if err != nil {
		return v, err
	}
	if v.ID != id || v.Pool != name {
		return v, errors.New("slot key mismatch")
	}
	return v, v.Validate()
}
func (t *poolTx) Slots(name string) ([]pool.Slot, error) {
	list, err := scan[pool.Slot](t.bucket(1), name+"\x00")
	if err != nil {
		return nil, err
	}
	out := make([]pool.Slot, 0, len(list))
	seen := map[string]bool{}
	for _, v := range list {
		if err := v.Validate(); err != nil {
			return nil, err
		}
		key := slotKey(v.Pool, v.ID)
		seen[key] = true
		if t.removedSlots[key] {
			continue
		}
		if next, ok := t.slots[key]; ok {
			v = next
		}
		out = append(out, v)
	}
	for k, v := range t.slots {
		if v.Pool == name && !seen[k] && !t.removedSlots[k] {
			out = append(out, v)
		}
	}
	return out, nil
}
func (t *poolTx) Ready(name string) (pool.Slot, error) {
	prefix := name + "\x00"
	cursor := t.bucket(2).Cursor()
	for key, _ := cursor.Seek([]byte(prefix)); key != nil && strings.HasPrefix(string(key), prefix); key, _ = cursor.Next() {
		id := string(key[len(prefix):])
		slot, err := t.Slot(name, id)
		if errors.Is(err, pool.ErrNotFound) {
			continue
		}
		if err != nil {
			return pool.Slot{}, err
		}
		if slot.State == pool.Ready {
			return slot, nil
		}
	}
	for k, v := range t.slots {
		if v.Pool == name && v.State == pool.Ready && !t.removedSlots[k] {
			return v, nil
		}
	}
	return pool.Slot{}, pool.ErrEmpty
}

// leaseRow keeps the idempotency fields private in the public API while retaining
// them on disk. They must survive restarts to prevent duplicate allocations.
type leaseRow struct {
	Lease       pool.Lease `json:"lease"`
	KeyHash     string     `json:"key_hash"`
	RequestHash string     `json:"request_hash"`
}

func restoreLease(v leaseRow) pool.Lease {
	v.Lease.KeyHash = v.KeyHash
	v.Lease.RequestHash = v.RequestHash
	return v.Lease
}
func (t *poolTx) Lease(id string) (pool.Lease, error) {
	if t.removedLeases[id] {
		return pool.Lease{}, pool.ErrNotFound
	}
	if v, ok := t.leases[id]; ok {
		return v, nil
	}
	v, err := readValue[leaseRow](t.bucket(3), id)
	if err != nil {
		return pool.Lease{}, err
	}
	result := restoreLease(v)
	if result.ID != id {
		return result, errors.New("lease key mismatch")
	}
	return result, result.Validate()
}
func (t *poolTx) Request(name, hash string) (pool.Lease, error) {
	for id, v := range t.leases {
		if v.Pool == name && v.KeyHash == hash && !t.removedLeases[id] {
			return v, nil
		}
	}
	id := t.bucket(4).Get([]byte(slotKey(name, hash)))
	if id == nil {
		return pool.Lease{}, pool.ErrNotFound
	}
	v, err := t.Lease(string(id))
	if err == nil && (v.Pool != name || v.KeyHash != hash) {
		return pool.Lease{}, errors.New("idempotency index mismatch")
	}
	return v, err
}

// ReceiptsBefore uses a terminal-time index; GC never scans every historical lease
// while holding the single writer. Each pass is bounded and yields to claims.
func (t *poolTx) ReceiptsBefore(before time.Time, limit int) ([]pool.Lease, error) {
	if limit < 1 || limit > 256 {
		return nil, pool.ErrInvalid
	}
	out := make([]pool.Lease, 0, limit)
	cursor := t.bucket(5).Cursor()
	for key, value := cursor.First(); key != nil && len(out) < limit; key, value = cursor.Next() {
		if len(key) != 40 {
			return nil, errors.New("invalid receipt index")
		}
		if binary.BigEndian.Uint64(key[:8]) >= uint64(before.UnixNano()) {
			break
		}
		lease, err := t.Lease(string(value))
		if err != nil {
			return nil, err
		}
		out = append(out, lease)
	}
	return out, nil
}
func receiptKey(lease pool.Lease) []byte {
	key := make([]byte, 8, 40)
	binary.BigEndian.PutUint64(key, uint64(lease.UpdatedAt.UnixNano()))
	return append(key, lease.ID...)
}

func (t *poolTx) SavePool(v pool.Record) { t.pools[v.Spec.Name] = v }
func (t *poolTx) SaveSlot(v pool.Slot) {
	k := slotKey(v.Pool, v.ID)
	t.slots[k] = v
	delete(t.removedSlots, k)
}
func (t *poolTx) SaveLease(v pool.Lease)   { t.leases[v.ID] = v; delete(t.removedLeases, v.ID) }
func (t *poolTx) RemoveSlot(v pool.Slot)   { t.removedSlots[slotKey(v.Pool, v.ID)] = true }
func (t *poolTx) RemoveLease(v pool.Lease) { t.removedLeases[v.ID] = true; t.leases[v.ID] = v }
func (t *poolTx) flush() error {
	for k, v := range t.pools {
		if err := v.Validate(); err != nil {
			return err
		}
		if err := writeValue(t.bucket(0), k, v); err != nil {
			return err
		}
	}
	for k, v := range t.slots {
		if t.removedSlots[k] {
			continue
		}
		if err := v.Validate(); err != nil {
			return err
		}
		if err := writeValue(t.bucket(1), k, v); err != nil {
			return err
		}
		var err error
		if v.State == pool.Ready {
			err = t.bucket(2).Put([]byte(k), []byte{1})
		} else {
			err = t.bucket(2).Delete([]byte(k))
		}
		if err != nil {
			return err
		}
	}
	for k := range t.removedSlots {
		if err := t.bucket(1).Delete([]byte(k)); err != nil {
			return err
		}
		if err := t.bucket(2).Delete([]byte(k)); err != nil {
			return err
		}
	}
	for id, v := range t.leases {
		previous, err := readValue[leaseRow](t.bucket(3), id)
		if err != nil && !errors.Is(err, pool.ErrNotFound) {
			return err
		}
		if err == nil {
			if err := t.bucket(5).Delete(receiptKey(previous.Lease)); err != nil {
				return err
			}
		}
		k := []byte(slotKey(v.Pool, v.KeyHash))
		if t.removedLeases[id] {
			if err := t.bucket(3).Delete([]byte(id)); err != nil {
				return err
			}
			if err := t.bucket(4).Delete(k); err != nil {
				return err
			}
			continue
		}
		if err := v.Validate(); err != nil {
			return err
		}
		if err := writeValue(t.bucket(3), id, leaseRow{v, v.KeyHash, v.RequestHash}); err != nil {
			return err
		}
		if err := t.bucket(4).Put(k, []byte(id)); err != nil {
			return err
		}
		if (v.State == pool.Released || v.State == pool.Lost) && t.bucket(1).Get([]byte(slotKey(v.Pool, v.SlotID))) == nil {
			if err := t.bucket(5).Put(receiptKey(v), []byte(id)); err != nil {
				return err
			}
		}
	}
	return nil
}
