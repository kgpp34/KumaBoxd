package metadata

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

func (j *poolJournal) run() {
	defer close(j.done)
	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()
	for {
		select {
		case <-j.stop:
			return
		case <-ticker.C:
		case <-j.wake:
		}
		if err := j.checkpoint(); err != nil {
			j.mu.Lock()
			if j.fatal == nil {
				j.fatal = err
			}
			j.mu.Unlock()
		}
	}
}

// checkpoint copies pending immutable records briefly under the cache lock.
// Serialization, database queuing and synchronous disk I/O happen after unlocking,
// so a slow fsync cannot stall claim behind the background writer.
func (j *poolJournal) checkpoint() error {
	j.mu.RLock()
	pending := append([]pendingRecord(nil), j.pending...)
	j.mu.RUnlock()
	if len(pending) == 0 {
		return nil
	}
	changes := newPoolDelta()
	for _, item := range pending {
		changes.merge(item.record.Changes)
	}
	sequence := pending[len(pending)-1].record.Sequence
	result := j.store.writer.enqueue(&writeRequest{ctx: context.Background(), reply: make(chan writeResult, 1), apply: func(tx *bolt.Tx) (writeResult, error) {
		if err := changes.flush(tx); err != nil {
			return writeResult{}, err
		}
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], sequence)
		return writeResult{changed: true}, tx.Bucket(metadataKey).Put(poolCheckpointKey, encoded[:])
	}})
	if result.err != nil {
		return result.err
	}
	j.mu.Lock()
	for _, item := range j.pending[:len(pending)] {
		j.pendingBytes -= len(item.frame)
	}
	clear(j.pending[:len(pending)])
	j.pending = j.pending[len(pending):]
	compact := j.size >= compactJournalAt
	j.mu.Unlock()
	if compact {
		return j.rotate(sequence)
	}
	return nil
}

// rotate first writes and syncs a new log outside the claim lock. A short final
// append captures requests arriving during the sync before the atomic rename.
// The old log remains usable until the replacement is complete.
func (j *poolJournal) rotate(base uint64) (returnErr error) {
	j.mu.RLock()
	captured := append([]pendingRecord(nil), j.pending...)
	j.mu.RUnlock()
	next, err := os.CreateTemp(filepath.Dir(j.path), "pool-log-*.tmp")
	if err != nil {
		return err
	}
	adopted := false
	defer func() {
		if !adopted {
			returnErr = errors.Join(returnErr, next.Close(), os.Remove(next.Name()))
		}
	}()
	header, err := encodeJournalFrame(journalHeader{Format: journalFormat, NodeID: j.store.nodeID, Base: base})
	if err != nil {
		return err
	}
	if err := writeJournalFrame(next, header); err != nil {
		return err
	}
	size := int64(len(header))
	for _, item := range captured {
		if err := writeJournalFrame(next, item.frame); err != nil {
			return err
		}
		size += int64(len(item.frame))
	}
	if err := next.Sync(); err != nil {
		return err
	}
	// CreateTemp does not use O_APPEND. Reopen with that flag before publishing;
	// replay reads and future appends must never share a mutable seek position.
	appendFile, err := os.OpenFile(next.Name(), os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		return err
	}
	if err := next.Close(); err != nil {
		_ = appendFile.Close()
		return err
	}
	next = appendFile
	j.mu.Lock()
	defer func() {
		j.mu.Unlock()
		if adopted {
			returnErr = errors.Join(returnErr, syncDirectory(filepath.Dir(j.path)))
		}
	}()
	for _, item := range j.pending[len(captured):] {
		if err := writeJournalFrame(next, item.frame); err != nil {
			return err
		}
		size += int64(len(item.frame))
	}
	if err := os.Rename(next.Name(), j.path); err != nil {
		return err
	}
	old := j.file
	j.file = next
	j.size = size
	adopted = true
	if err := old.Close(); err != nil {
		return err
	}
	// The durable database already contains base. Directory sync belongs outside
	// this lock as well; the deferred call runs after releasing it.
	return nil
}
func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
func (j *poolJournal) close() error {
	j.mu.Lock()
	j.closed = true
	close(j.stop)
	j.mu.Unlock()
	<-j.done
	// The owning Store still keeps the durable writer alive for this final flush.
	checkpointErr := j.checkpoint()
	j.mu.Lock()
	defer j.mu.Unlock()
	return errors.Join(j.fatal, checkpointErr, j.file.Close())
}
