package metadata

import (
	"context"
	"errors"
	"testing"

	"github.com/kgpp34/kumaboxd/internal/pool"
	"github.com/kgpp34/kumaboxd/internal/template"
)

func TestBatchKeepsConflictsLocalAndAcknowledgesCommittedRecords(t *testing.T) {
	store := openStore(t)
	first := batchRequest("shared")
	conflict := batchRequest("shared")
	conflict.record.SnapshotID = "22222222-2222-4222-8222-222222222222"
	other := batchRequest("other")
	store.writer.commit([]*writeRequest{first, conflict, other})
	if result := <-first.reply; result.err != nil || !result.created {
		t.Fatalf("first record was not committed: %+v", result)
	}
	if result := <-conflict.reply; !errors.Is(result.err, template.ErrConflict) || result.created {
		t.Fatalf("conflict was not isolated: %+v", result)
	}
	if result := <-other.reply; result.err != nil || !result.created {
		t.Fatalf("independent record was lost: %+v", result)
	}
	for _, name := range []string{"shared", "other"} {
		if _, err := store.Get(context.Background(), name); err != nil {
			t.Fatal(err)
		}
	}
}

func TestBatchRollsBackEarlierRequestsOnStorageError(t *testing.T) {
	store := openStore(t)
	first := batchRequest("first")
	invalid := batchRequest("invalid")
	// Bypass public validation to exercise a transaction-level encoding failure.
	invalid.record.GuestConfig = []byte("not-json")
	store.writer.commit([]*writeRequest{first, invalid})
	for _, request := range []*writeRequest{first, invalid} {
		if result := <-request.reply; result.err == nil || result.created {
			t.Fatalf("rolled-back write acknowledged success: %+v", result)
		}
	}
	if _, err := store.Get(context.Background(), "first"); !errors.Is(err, template.ErrNotFound) {
		t.Fatalf("partial transaction remained: %v", err)
	}
}

func TestCloseDrainsAcceptedRequestsAndReleasesDatabase(t *testing.T) {
	directory := privateDirectory(t)
	store, err := Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	request := batchRequest("accepted")
	store.writer.requests <- request
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if result := <-request.reply; result.err != nil || !result.created {
		t.Fatalf("accepted write lost on close: %+v", result)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	if _, err := reopened.Get(context.Background(), "accepted"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Put(context.Background(), storedTemplate("closed")); err == nil {
		t.Fatal("closed store accepted a new write")
	}
}

func TestCanceledQueuedRequestDoesNotPreventOtherCommits(t *testing.T) {
	store := openStore(t)
	canceled := batchRequest("canceled")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	canceled.ctx = ctx
	valid := batchRequest("valid")
	store.writer.commit([]*writeRequest{canceled, valid})
	if result := <-canceled.reply; !errors.Is(result.err, context.Canceled) || result.created {
		t.Fatalf("canceled request wrote state: %+v", result)
	}
	if result := <-valid.reply; result.err != nil || !result.created {
		t.Fatalf("unrelated request failed: %+v", result)
	}
}

func batchRequest(name string) *writeRequest {
	return &writeRequest{ctx: context.Background(), record: storedTemplate(name), reply: make(chan writeResult, 1)}
}

func TestReadOnlyPoolTransactionsDoNotCommitAnEmptyWrite(t *testing.T) {
	store := openStore(t)
	before := store.db.Stats().TxStats.Write
	for range 5 {
		if err := store.Write(context.Background(), func(pool.WriteTx) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	if after := store.db.Stats().TxStats.Write; after != before {
		t.Fatalf("read-only transitions wrote database pages: %d -> %d", before, after)
	}
}
