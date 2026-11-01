package metadata

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/kgpp34/kumaboxd/internal/pool"
)

func TestPoolBusinessErrorCannotPublishStagedWrites(t *testing.T) {
	store := openStore(t)
	ctx := context.Background()
	sentinel := errors.New("business failure")
	err := store.Write(ctx, func(tx pool.WriteTx) error {
		tx.SavePool(pool.Record{Spec: pool.Spec{Name: "discarded", Template: "test", TargetReady: 1, MaxTotal: 2}, Template: storedTemplate("test"), CreatedAt: time.Now().UTC()})
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if err := store.Read(ctx, func(tx pool.ReadTx) error { _, err := tx.Pool("discarded"); return err }); !errors.Is(err, pool.ErrNotFound) {
		t.Fatalf("failed callback published partial state: %v", err)
	}
}

func TestPoolNamespaceMigrationPreservesTemplatesAndIdentity(t *testing.T) {
	directory := privateDirectory(t)
	store, err := Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	nodeID := store.NodeID()
	if !pool.ValidID(nodeID) {
		t.Fatal("invalid node identity")
	}
	if _, _, err := store.Put(context.Background(), storedTemplate("original")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	if reopened.NodeID() != nodeID {
		t.Fatal("node identity changed on restart")
	}
	if _, err := reopened.Get(context.Background(), "original"); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyTemplateDatabaseGetsPoolNamespaceWithoutLosingTemplates(t *testing.T) {
	directory := privateDirectory(t)
	store, err := Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Put(context.Background(), storedTemplate("legacy")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := bolt.Open(filepath.Join(directory, "node.db"), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket(metadataKey)
		for _, key := range poolBuckets {
			if err := root.DeleteBucket(key); err != nil {
				return err
			}
		}
		if err := root.Delete(nodeIDKey); err != nil {
			return err
		}
		return root.Delete(poolFormatKey)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	if !pool.ValidID(reopened.NodeID()) {
		t.Fatal("legacy migration did not create identity")
	}
	if _, err := reopened.Get(context.Background(), "legacy"); err != nil {
		t.Fatal(err)
	}
}

func TestReceiptCleanupReadsBoundedOldTerminalRecords(t *testing.T) {
	store := openStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	err := store.Write(ctx, func(tx pool.WriteTx) error {
		for i := range 300 {
			lease := pool.Lease{ID: fmt.Sprintf("%032x", i+1), Pool: "test", SlotID: fmt.Sprintf("%032x", i+1000), SandboxID: fmt.Sprintf("vm-%d", i), Owner: "owner", State: pool.Released, KeyHash: fmt.Sprintf("%064x", i+1), RequestHash: fmt.Sprintf("%064x", i+500), CreatedAt: now.Add(-48 * time.Hour), UpdatedAt: now.Add(-47 * time.Hour), ExpiresAt: now.Add(-46 * time.Hour)}
			tx.SaveLease(lease)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var batch []pool.Lease
	err = store.Read(ctx, func(tx pool.ReadTx) error {
		var err error
		batch, err = tx.ReceiptsBefore(now.Add(-24*time.Hour), 256)
		return err
	})
	if err != nil || len(batch) != 256 {
		t.Fatalf("unbounded or missing receipt scan: %d %v", len(batch), err)
	}
	if err := store.Write(ctx, func(tx pool.WriteTx) error {
		for _, lease := range batch {
			tx.RemoveLease(lease)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	err = store.Read(ctx, func(tx pool.ReadTx) error {
		remaining, err := tx.ReceiptsBefore(now.Add(-24*time.Hour), 256)
		if len(remaining) != 44 {
			t.Errorf("remaining receipts=%d", len(remaining))
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}
