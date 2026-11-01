package metadata

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/kgpp34/kumaboxd/internal/pool"
	bolt "go.etcd.io/bbolt"
	boltErrors "go.etcd.io/bbolt/errors"
)

const (
	journalFormat     = "kumaboxd.pool-journal.v1"
	maxJournalFrame   = 16 << 20
	maxPendingJournal = 64 << 20
	compactJournalAt  = 4 << 20
)

var poolCheckpointKey = []byte("pool-checkpoint-sequence")

// Durability selects acknowledgement semantics, never database NoSync.
// Buffered acknowledges an append to the OS page cache; recent acknowledgements
// can be lost on host failure. Sync retains synchronous bbolt commits.
type Durability = pool.Durability

const (
	Sync     Durability = pool.DurabilitySync
	Buffered Durability = pool.DurabilityBuffered
)

// Options fixes persistence policy for this state directory. CheckpointInterval
// is a flush cadence, not an upper bound on data loss or disk completion time.
type Options struct {
	PoolDurability     Durability
	CheckpointInterval time.Duration
}

func (o Options) normalized() (Options, error) {
	if o.PoolDurability == "" {
		o.PoolDurability = Sync
	}
	if o.CheckpointInterval == 0 {
		o.CheckpointInterval = time.Second
	}
	if o.PoolDurability != Sync && o.PoolDurability != Buffered {
		return o, errors.New("pool durability must be sync or buffered")
	}
	if o.CheckpointInterval < time.Millisecond || o.CheckpointInterval > time.Minute {
		return o, errors.New("pool checkpoint interval must be 1ms..1m")
	}
	return o, nil
}

type journalHeader struct {
	Format string `json:"format"`
	NodeID string `json:"node_id"`
	Base   uint64 `json:"base"`
}
type journalRecord struct {
	Sequence uint64    `json:"sequence"`
	Changes  poolDelta `json:"changes"`
}
type pendingRecord struct {
	record journalRecord
	frame  []byte
}

// poolJournal is a serialized, validated state machine with a redo log.
//
// request -> stage -> validate -> append frame -> publish indexes -> acknowledge
//
//	              |
//	background synchronous bbolt checkpoint
//
// No request waits for that checkpoint. bbolt always retains its normal crash
// consistency; only newer, buffered journal records have weaker durability.
type poolJournal struct {
	mu           sync.RWMutex
	store        *Store
	cache        *poolCache
	file         *os.File
	path         string
	sequence     uint64
	pending      []pendingRecord
	pendingBytes int
	size         int64
	fatal        error
	closed       bool
	interval     time.Duration
	stop         chan struct{}
	wake         chan struct{}
	done         chan struct{}
}

func openPoolJournal(store *Store, directory string, interval time.Duration) (*poolJournal, error) {
	j := &poolJournal{store: store, path: filepath.Join(directory, "pool.log"), interval: interval, stop: make(chan struct{}), wake: make(chan struct{}, 1), done: make(chan struct{})}
	var checkpoint uint64
	if err := store.db.View(func(tx *bolt.Tx) error {
		var err error
		j.cache, err = loadPoolCache(tx)
		if err != nil {
			return err
		}
		raw := tx.Bucket(metadataKey).Get(poolCheckpointKey)
		if len(raw) > 0 && len(raw) != 8 {
			return errors.New("invalid pool checkpoint sequence")
		}
		if len(raw) == 8 {
			checkpoint = binary.BigEndian.Uint64(raw)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	j.sequence = checkpoint
	if info, err := os.Lstat(j.path); err == nil && (!info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0) {
		return nil, errors.New("pool journal must be a private regular file")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	f, err := os.OpenFile(j.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	j.file = f
	if err := j.replay(checkpoint); err != nil {
		return nil, errors.Join(err, f.Close())
	}
	// Compact on startup to normalize a journal older than the durable checkpoint
	// and make its identity/header durable before accepting the first mutation.
	if err := j.rotate(checkpoint); err != nil {
		return nil, errors.Join(err, j.file.Close())
	}
	go j.run()
	return j, nil
}
func (j *poolJournal) read(ctx context.Context, f func(pool.ReadTx) error) error {
	j.mu.RLock()
	defer j.mu.RUnlock()
	if j.closed {
		return boltErrors.ErrDatabaseNotOpen
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return f(j.cache)
}
func (j *poolJournal) write(ctx context.Context, f func(pool.WriteTx) error) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return boltErrors.ErrDatabaseNotOpen
	}
	if j.fatal != nil {
		return errors.Join(pool.ErrUnavailable, fmt.Errorf("pool journal unavailable: %w", j.fatal))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	staged := newPoolWriteTx(j.cache)
	if err := f(staged); err != nil {
		return err
	}
	d := staged.changes
	if d.empty() {
		return nil
	}
	if err := d.validate(); err != nil {
		return err
	}
	if j.sequence == ^uint64(0) {
		return errors.New("pool journal sequence exhausted")
	}
	record := journalRecord{Sequence: j.sequence + 1, Changes: d}
	frame, err := encodeJournalFrame(record)
	if err != nil {
		return err
	}
	if j.pendingBytes+len(frame) > maxPendingJournal {
		return fmt.Errorf("%w: pool checkpoint backlog is full", pool.ErrUnavailable)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := writeJournalFrame(j.file, frame); err != nil {
		j.fatal = errors.Join(err, j.file.Truncate(j.size))
		return j.fatal
	}
	j.sequence = record.Sequence
	j.size += int64(len(frame))
	j.pendingBytes += len(frame)
	j.cache.apply(d)
	j.pending = append(j.pending, pendingRecord{record, frame})
	if j.pendingBytes >= compactJournalAt {
		select {
		case j.wake <- struct{}{}:
		default:
		}
	}
	return nil
}
func encodeJournalFrame(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	if len(raw) > maxJournalFrame {
		return nil, errors.New("pool journal frame exceeds 16MiB")
	}
	frame := make([]byte, 8+len(raw))
	binary.BigEndian.PutUint32(frame, uint32(len(raw)))
	binary.BigEndian.PutUint32(frame[4:], crc32.ChecksumIEEE(raw))
	copy(frame[8:], raw)
	return frame, nil
}
func writeJournalFrame(w io.Writer, frame []byte) error {
	n, err := w.Write(frame)
	if err == nil && n != len(frame) {
		err = io.ErrShortWrite
	}
	return err
}
func readJournalFrame(r io.Reader) ([]byte, int, error) {
	var header [8]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, 0, err
	}
	size := binary.BigEndian.Uint32(header[:])
	if size == 0 || size > maxJournalFrame {
		return nil, 0, errors.New("invalid pool journal frame size")
	}
	raw := make([]byte, int(size))
	if _, err := io.ReadFull(r, raw); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, 0, io.ErrUnexpectedEOF
		}
		return nil, 0, err
	}
	if crc32.ChecksumIEEE(raw) != binary.BigEndian.Uint32(header[4:]) {
		return nil, 0, errors.New("pool journal checksum mismatch")
	}
	return raw, len(raw) + 8, nil
}
func (j *poolJournal) replay(checkpoint uint64) error {
	if _, err := j.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	stat, err := j.file.Stat()
	if err != nil {
		return err
	}
	if stat.Size() == 0 {
		return nil
	}
	raw, n, err := readJournalFrame(j.file)
	if err != nil {
		return fmt.Errorf("read pool journal identity: %w", err)
	}
	var header journalHeader
	if err := json.Unmarshal(raw, &header); err != nil {
		return err
	}
	if header.Format != journalFormat || header.NodeID != j.store.nodeID || header.Base > checkpoint {
		return errors.New("pool journal identity or checkpoint mismatch")
	}
	last := header.Base
	j.size = int64(n)
	for {
		raw, n, err = readJournalFrame(j.file)
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			if err := j.file.Truncate(j.size); err != nil {
				return err
			}
			break
		}
		if err != nil {
			return err
		}
		var record journalRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			return err
		}
		if last == ^uint64(0) || record.Sequence != last+1 {
			return errors.New("non-contiguous pool journal sequence")
		}
		last = record.Sequence
		if err := record.Changes.validate(); err != nil {
			return err
		}
		if record.Sequence > checkpoint {
			frame, err := encodeJournalFrame(record)
			if err != nil {
				return err
			}
			j.pendingBytes += len(frame)
			if j.pendingBytes > maxPendingJournal {
				return errors.New("pool journal recovery exceeds backlog limit")
			}
			j.cache.apply(record.Changes)
			j.pending = append(j.pending, pendingRecord{record, frame})
			j.sequence = record.Sequence
		}
		j.size += int64(n)
	}
	return nil
}

// The selected policy is persisted so restarting with a default flag cannot read
// a stale checkpoint and silently ignore a buffered journal.
func setPoolDurability(root *bolt.Bucket, mode Durability) error {
	key := []byte("pool-durability")
	if previous := root.Get(key); previous != nil && string(previous) != string(mode) {
		return fmt.Errorf("pool durability is %s in this state directory; restart with the same mode", previous)
	}
	return root.Put(key, []byte(mode))
}
