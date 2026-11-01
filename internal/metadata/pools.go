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

// Durability exposes the actual acknowledgement policy to pool API receipts.
func (s *Store) Durability() pool.Durability {
	if s.journal != nil {
		return pool.DurabilityBuffered
	}
	return pool.DurabilitySync
}

// Read gives a consistent view independent of the selected persistence policy.
func (s *Store) Read(ctx context.Context, f func(pool.ReadTx) error) error {
	if s.journal != nil {
		return s.journal.read(ctx, f)
	}
	return s.db.View(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return f(boltPoolReader{tx})
	})
}

// Write validates and stages one request; failed callbacks publish no changes.
func (s *Store) Write(ctx context.Context, f func(pool.WriteTx) error) error {
	if s.journal != nil {
		return s.journal.write(ctx, f)
	}
	request := &writeRequest{ctx: ctx, reply: make(chan writeResult, 1), apply: func(tx *bolt.Tx) (writeResult, error) {
		staged := newPoolWriteTx(boltPoolReader{tx})
		if err := f(staged); err != nil {
			return writeResult{err: err}, nil //nolint:nilerr // A business error aborts this request, not the shared transaction.
		}
		return writeResult{changed: !staged.changes.empty()}, staged.changes.flush(tx)
	}}
	return s.writer.enqueue(request).err
}

// boltPoolReader decodes owned values; it never exposes mapped database pages.
type boltPoolReader struct{ tx *bolt.Tx }

func (t boltPoolReader) bucket(n int) *bolt.Bucket {
	return t.tx.Bucket(metadataKey).Bucket(poolBuckets[n])
}
func (t boltPoolReader) Pool(name string) (pool.Record, error) {
	v, err := readValue[pool.Record](t.bucket(0), name)
	if err != nil {
		return v, err
	}
	if v.Spec.Name != name {
		return v, errors.New("pool key mismatch")
	}
	return v, v.Validate()
}
func (t boltPoolReader) Pools() ([]pool.Record, error) {
	all, err := scan[pool.Record](t.bucket(0), "")
	if err != nil {
		return nil, err
	}
	for _, v := range all {
		if err := v.Validate(); err != nil {
			return nil, err
		}
	}
	return all, nil
}
func (t boltPoolReader) Slot(name, id string) (pool.Slot, error) {
	v, err := readValue[pool.Slot](t.bucket(1), slotKey(name, id))
	if err != nil {
		return v, err
	}
	if v.ID != id || v.Pool != name {
		return v, errors.New("slot key mismatch")
	}
	return v, v.Validate()
}
func (t boltPoolReader) Slots(name string) ([]pool.Slot, error) {
	all, err := scan[pool.Slot](t.bucket(1), name+"\x00")
	if err != nil {
		return nil, err
	}
	for _, v := range all {
		if err := v.Validate(); err != nil {
			return nil, err
		}
	}
	return all, nil
}
func (t boltPoolReader) Ready(name string) (pool.Slot, error) {
	prefix := name + "\x00"
	cursor := t.bucket(2).Cursor()
	for k, _ := cursor.Seek([]byte(prefix)); k != nil && strings.HasPrefix(string(k), prefix); k, _ = cursor.Next() {
		slot, err := t.Slot(name, string(k[len(prefix):]))
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
	return pool.Slot{}, pool.ErrEmpty
}
func (t boltPoolReader) Lease(id string) (pool.Lease, error) {
	row, err := readValue[leaseRow](t.bucket(3), id)
	if err != nil {
		return pool.Lease{}, err
	}
	v := restoreLease(row)
	if v.ID != id {
		return v, errors.New("lease key mismatch")
	}
	return v, v.Validate()
}
func (t boltPoolReader) Request(name, hash string) (pool.Lease, error) {
	id := t.bucket(4).Get([]byte(slotKey(name, hash)))
	if id == nil {
		return pool.Lease{}, pool.ErrNotFound
	}
	v, err := t.Lease(string(id))
	if err == nil && (v.Pool != name || v.KeyHash != hash) {
		return v, errors.New("idempotency index mismatch")
	}
	return v, err
}
func (t boltPoolReader) ReceiptsBefore(before time.Time, limit int) ([]pool.Lease, error) {
	if limit < 1 || limit > 256 {
		return nil, pool.ErrInvalid
	}
	out := make([]pool.Lease, 0, limit)
	cursor := t.bucket(5).Cursor()
	for k, v := cursor.First(); k != nil && len(out) < limit; k, v = cursor.Next() {
		if len(k) != 40 {
			return nil, errors.New("invalid receipt index")
		}
		if binary.BigEndian.Uint64(k[:8]) >= uint64(before.UnixNano()) {
			break
		}
		lease, err := t.Lease(string(v))
		if err != nil {
			return nil, err
		}
		out = append(out, lease)
	}
	return out, nil
}
func slotKey(name, id string) string { return name + "\x00" + id }
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
func receiptKey(lease pool.Lease) []byte {
	key := make([]byte, 8, 40)
	binary.BigEndian.PutUint64(key, uint64(lease.UpdatedAt.UnixNano()))
	return append(key, lease.ID...)
}

func (d poolDelta) flush(tx *bolt.Tx) error {
	if err := d.validate(); err != nil {
		return err
	}
	bucket := func(n int) *bolt.Bucket { return tx.Bucket(metadataKey).Bucket(poolBuckets[n]) }
	for k, v := range d.Pools {
		if err := writeValue(bucket(0), k, v); err != nil {
			return err
		}
	}
	for k, v := range d.Slots {
		if d.RemovedSlots[k] {
			continue
		}
		if err := writeValue(bucket(1), k, v); err != nil {
			return err
		}
		var err error
		if v.State == pool.Ready {
			err = bucket(2).Put([]byte(k), []byte{1})
		} else {
			err = bucket(2).Delete([]byte(k))
		}
		if err != nil {
			return err
		}
	}
	for k := range d.RemovedSlots {
		if err := bucket(1).Delete([]byte(k)); err != nil {
			return err
		}
		if err := bucket(2).Delete([]byte(k)); err != nil {
			return err
		}
	}
	for id, row := range d.Leases {
		v := restoreLease(row)
		previous, err := readValue[leaseRow](bucket(3), id)
		if err != nil && !errors.Is(err, pool.ErrNotFound) {
			return err
		}
		if err == nil {
			if err := bucket(5).Delete(receiptKey(previous.Lease)); err != nil {
				return err
			}
		}
		k := []byte(slotKey(v.Pool, v.KeyHash))
		if d.RemovedLeases[id] {
			if err := bucket(3).Delete([]byte(id)); err != nil {
				return err
			}
			if err := bucket(4).Delete(k); err != nil {
				return err
			}
			continue
		}
		if err := writeValue(bucket(3), id, leaseRow{v, v.KeyHash, v.RequestHash}); err != nil {
			return err
		}
		if err := bucket(4).Put(k, []byte(id)); err != nil {
			return err
		}
		if (v.State == pool.Released || v.State == pool.Lost) && bucket(1).Get([]byte(slotKey(v.Pool, v.SlotID))) == nil {
			if err := bucket(5).Put(receiptKey(v), []byte(id)); err != nil {
				return err
			}
		}
	}
	return nil
}
