package metadata

import (
	"context"
	"errors"
	"sync"

	bolt "go.etcd.io/bbolt"
	boltErrors "go.etcd.io/bbolt/errors"

	"github.com/kgpp34/kumaboxd/internal/template"
)

// These bound metadata buffering and transaction size, not VM concurrency.
const (
	writeQueueSize = 256
	writeBatchSize = 64
)

// writeQueue serializes commits and drains already queued work without a timer.
// Close stops admission, drains accepted requests, then lets Store close the DB.
type writeQueue struct {
	db        *bolt.DB
	requests  chan *writeRequest
	done      chan struct{}
	admission sync.RWMutex
	closed    bool
}

// A request carries only the lifetime of one Put, never the service lifetime.
type writeRequest struct {
	ctx    context.Context
	record template.Template
	reply  chan writeResult
}

type writeResult struct {
	record  template.Template
	created bool
	err     error
}

func newWriteQueue(db *bolt.DB) *writeQueue {
	writer := &writeQueue{
		db: db, requests: make(chan *writeRequest, writeQueueSize), done: make(chan struct{}),
	}
	go writer.run()
	return writer
}

func (w *writeQueue) submit(ctx context.Context, record template.Template) writeResult {
	request := &writeRequest{ctx: ctx, record: record, reply: make(chan writeResult, 1)}
	w.admission.RLock()
	if w.closed {
		w.admission.RUnlock()
		return writeResult{err: boltErrors.ErrDatabaseNotOpen}
	}
	select {
	case <-ctx.Done():
		w.admission.RUnlock()
		return writeResult{err: ctx.Err()}
	case w.requests <- request:
		w.admission.RUnlock()
	}
	// Returning early here would hide whether an in-flight commit succeeded.
	return <-request.reply
}

func (w *writeQueue) run() {
	defer close(w.done)
	batch := make([]*writeRequest, 0, writeBatchSize)
	for first := range w.requests {
		batch = append(batch, first)
	drain:
		for len(batch) < writeBatchSize {
			select {
			case request, ok := <-w.requests:
				if !ok {
					break drain
				}
				batch = append(batch, request)
			default:
				break drain
			}
		}
		w.commit(batch)
		clear(batch)
		batch = batch[:0]
	}
}

func (w *writeQueue) commit(batch []*writeRequest) {
	results := make([]writeResult, len(batch))
	err := w.db.Update(func(tx *bolt.Tx) error {
		for index, request := range batch {
			if err := request.ctx.Err(); err != nil {
				results[index].err = err
				continue
			}
			record, created, err := putTemplate(tx, request.record)
			results[index] = writeResult{record: record, created: created, err: err}
			// A name conflict is isolated to its request. Storage errors roll back
			// the whole transaction, so no earlier request receives false success.
			if err != nil && !errors.Is(err, template.ErrConflict) {
				return err
			}
		}
		return nil
	})
	for index, request := range batch {
		result := results[index]
		if err != nil {
			result = writeResult{err: err}
		}
		request.reply <- result
	}
}

func (w *writeQueue) close() {
	w.admission.Lock()
	w.closed = true
	close(w.requests)
	w.admission.Unlock()
	<-w.done
}
