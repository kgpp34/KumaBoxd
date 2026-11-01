package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kgpp34/kumaboxd/internal/pool"
)

func bufferedStore(t *testing.T, interval time.Duration) *Store {
	t.Helper()
	store, err := OpenWithOptions(privateDirectory(t), Options{PoolDurability: Buffered, CheckpointInterval: interval})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}
func journalPool(name string) pool.Record {
	return pool.Record{Spec: pool.Spec{Name: name, Template: "test", TargetReady: 1, MaxTotal: 2}, Template: storedTemplate("test"), CreatedAt: time.Now().UTC()}
}
func putJournalPool(store *Store, name string) error {
	return store.Write(context.Background(), func(tx pool.WriteTx) error { tx.SavePool(journalPool(name)); return nil })
}

func TestBufferedWritesRollbackAndReturnOwnedValues(t *testing.T) {
	store := bufferedStore(t, time.Minute)
	before := store.journal.size
	sentinel := errors.New("reject")
	err := store.Write(context.Background(), func(tx pool.WriteTx) error { tx.SavePool(journalPool("discarded")); return sentinel })
	if !errors.Is(err, sentinel) || store.journal.size != before {
		t.Fatalf("failed callback published a journal frame: %v", err)
	}
	if err := putJournalPool(store, "test"); err != nil {
		t.Fatal(err)
	}
	if err := store.Read(context.Background(), func(tx pool.ReadTx) error {
		r, err := tx.Pool("test")
		if err == nil {
			r.Template.GuestConfig[0] = '!'
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Read(context.Background(), func(tx pool.ReadTx) error {
		r, err := tx.Pool("test")
		if err == nil {
			err = r.Validate()
		}
		return err
	}); err != nil {
		t.Fatalf("caller mutated shared cache: %v", err)
	}
	if store.db.NoSync {
		t.Fatal("buffered mode must not disable bbolt crash consistency")
	}
}
func TestBufferedClaimsDoNotWaitForCheckpointDiskTransaction(t *testing.T) {
	store := bufferedStore(t, time.Minute)
	if err := putJournalPool(store, "first"); err != nil {
		t.Fatal(err)
	}
	held, err := store.db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	release := sync.OnceFunc(func() {
		if err := held.Rollback(); err != nil {
			t.Error(err)
		}
	})
	defer release()
	checkpoint := make(chan error, 1)
	go func() { checkpoint <- store.journal.checkpoint() }()
	select {
	case err := <-checkpoint:
		t.Fatalf("checkpoint bypassed held database transaction: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	written := make(chan error, 1)
	go func() { written <- putJournalPool(store, "second") }()
	select {
	case err := <-written:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		release()
		t.Fatal("buffered write waited for checkpoint disk I/O")
	}
	release()
	if err := <-checkpoint; err != nil {
		t.Fatal(err)
	}
}
func TestBufferedModeCannotBeSilentlyReopenedAsSync(t *testing.T) {
	store := bufferedStore(t, time.Minute)
	directory := filepath.Dir(store.journal.path)
	if err := putJournalPool(store, "saved"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if other, err := Open(directory); err == nil {
		_ = other.Close()
		t.Fatal("sync mode ignored buffered state policy")
	}
	reopened, err := OpenWithOptions(directory, Options{PoolDurability: Buffered})
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Read(context.Background(), func(tx pool.ReadTx) error { _, err := tx.Pool("saved"); return err }); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

// The helper exits without Close or any checkpoint. The parent exercises the
// actual process-exit boundary, including kernel release of the database lock.
func TestJournalCrashHelper(t *testing.T) {
	directory := os.Getenv("KUMABOXD_JOURNAL_CRASH_PATH")
	if directory == "" {
		t.Skip("subprocess helper")
	}
	store, err := OpenWithOptions(directory, Options{PoolDurability: Buffered, CheckpointInterval: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	id := "11111111111111111111111111111111"
	hash := strings.Repeat("a", 64)
	err = store.Write(context.Background(), func(tx pool.WriteTx) error {
		tx.SavePool(journalPool("test"))
		tx.SaveSlot(pool.Slot{ID: id, Pool: "test", Name: "vm-test", SandboxID: "vm-test", State: pool.Leased, LeaseID: id, CreatedAt: now, UpdatedAt: now})
		tx.SaveLease(pool.Lease{ID: id, Pool: "test", SlotID: id, SandboxID: "vm-test", Owner: "owner", State: pool.Active, KeyHash: hash, RequestHash: hash, CreatedAt: now, UpdatedAt: now, ExpiresAt: now.Add(time.Hour)})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	os.Exit(0)
}
func uncleanJournal(t *testing.T) string {
	t.Helper()
	directory := privateDirectory(t)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "-test.run=^TestJournalCrashHelper$")
	command.Env = append(os.Environ(), "KUMABOXD_JOURNAL_CRASH_PATH="+directory)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("unclean child: %v %s", err, output)
	}
	return directory
}
func TestBufferedJournalReplaysLeaseAfterUncleanProcessExit(t *testing.T) {
	directory := uncleanJournal(t)
	store, err := OpenWithOptions(directory, Options{PoolDurability: Buffered})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	}()
	err = store.Read(context.Background(), func(tx pool.ReadTx) error {
		lease, err := tx.Request("test", strings.Repeat("a", 64))
		if err != nil {
			return err
		}
		if lease.Owner != "owner" || lease.RequestHash != strings.Repeat("a", 64) || lease.State != pool.Active {
			return errors.New("replay lost receipt identity")
		}
		_, err = tx.Ready("test")
		if !errors.Is(err, pool.ErrEmpty) {
			return errors.New("replayed leased VM became Ready")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
func TestBufferedJournalHandlesTornTailButRejectsCompleteCorruption(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprint(corrupt), func(t *testing.T) {
			directory := uncleanJournal(t)
			path := filepath.Join(directory, "pool.log")
			file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0)
			if err != nil {
				t.Fatal(err)
			}
			if corrupt {
				stat, err := file.Stat()
				if err != nil {
					t.Fatal(err)
				}
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
				file, err = os.OpenFile(path, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := file.WriteAt([]byte{'!'}, stat.Size()-1); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := file.Write([]byte{0, 0}); err != nil {
					t.Fatal(err)
				}
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			store, err := OpenWithOptions(directory, Options{PoolDurability: Buffered})
			if corrupt {
				if err == nil {
					_ = store.Close()
					t.Fatal("complete corrupted frame was accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Read(context.Background(), func(tx pool.ReadTx) error { _, err := tx.Request("test", strings.Repeat("a", 64)); return err }); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestBufferedJournalCompactsWhileWritesContinue(t *testing.T) {
	store := bufferedStore(t, time.Millisecond)
	record := journalPool("test")
	record.Template.GuestConfig = json.RawMessage(`{"CPUs":1,"Memory":1048576,"Storage":1048576,"NICs":0,"padding":"` + strings.Repeat("x", 96<<10) + `"}`)
	for i := range 64 {
		record.Spec.Name = fmt.Sprintf("pool-%03d", i)
		if err := store.Write(context.Background(), func(tx pool.WriteTx) error { tx.SavePool(record); return nil }); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenWithOptions(filepath.Dir(store.journal.path), Options{PoolDurability: Buffered})
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Read(context.Background(), func(tx pool.ReadTx) error {
		all, err := tx.Pools()
		if len(all) != 64 {
			t.Errorf("lost pools across checkpoint/rotation: %d", len(all))
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBufferedReceiptIndexCleanupAndCheckpoint(t *testing.T) {
	store := bufferedStore(t, time.Minute)
	ctx := context.Background()
	now := time.Now().UTC()
	if err := store.Write(ctx, func(tx pool.WriteTx) error {
		for i := range 300 {
			lease := pool.Lease{ID: fmt.Sprintf("%032x", i+1), Pool: "test", SlotID: fmt.Sprintf("%032x", i+1000), SandboxID: fmt.Sprint(i), Owner: "owner", State: pool.Released, KeyHash: fmt.Sprintf("%064x", i+1), RequestHash: fmt.Sprintf("%064x", i+500), CreatedAt: now.Add(-48 * time.Hour), UpdatedAt: now.Add(-47 * time.Hour), ExpiresAt: now.Add(-46 * time.Hour)}
			tx.SaveLease(lease)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Write(ctx, func(tx pool.WriteTx) error {
		batch, err := tx.ReceiptsBefore(now.Add(-24*time.Hour), 256)
		if err != nil {
			return err
		}
		if len(batch) != 256 {
			return fmt.Errorf("cleanup batch=%d", len(batch))
		}
		for _, v := range batch {
			tx.RemoveLease(v)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenWithOptions(filepath.Dir(store.journal.path), Options{PoolDurability: Buffered})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := reopened.Read(ctx, func(tx pool.ReadTx) error {
		batch, err := tx.ReceiptsBefore(now.Add(-24*time.Hour), 256)
		if len(batch) != 44 {
			return fmt.Errorf("checkpoint lost receipt removals: %d", len(batch))
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
func TestBufferedAppendFailureDoesNotPublishState(t *testing.T) {
	store, err := OpenWithOptions(privateDirectory(t), Options{PoolDurability: Buffered, CheckpointInterval: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	store.journal.mu.Lock()
	err = store.journal.file.Close()
	store.journal.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := putJournalPool(store, "rejected"); err == nil {
		t.Fatal("closed log acknowledged a mutation")
	}
	if err := store.Read(context.Background(), func(tx pool.ReadTx) error { _, err := tx.Pool("rejected"); return err }); !errors.Is(err, pool.ErrNotFound) {
		t.Fatalf("failed append published state: %v", err)
	}
	// The intentionally damaged log must remain a visible shutdown error.
	if err := store.Close(); err == nil {
		t.Fatal("close hid the append failure")
	}
}

func TestJournalCheckpointAndRotationPreserveUncheckpointedTail(t *testing.T) {
	directory := privateDirectory(t)
	store, err := OpenWithOptions(directory, Options{PoolDurability: Buffered, CheckpointInterval: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	// Stop only the periodic loop. Writes still append, letting this test put
	// a precisely known checkpoint and a newer tail on opposite sides of rotate.
	close(store.journal.stop)
	<-store.journal.done
	if err := putJournalPool(store, "checkpointed"); err != nil {
		t.Fatal(err)
	}
	if err := store.journal.checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := putJournalPool(store, "tail"); err != nil {
		t.Fatal(err)
	}
	if err := store.journal.rotate(1); err != nil {
		t.Fatal(err)
	}
	if err := putJournalPool(store, "after-rotation"); err != nil {
		t.Fatal(err)
	}
	// Abandon the process state without the normal final checkpoint. The earlier
	// subprocess test separately checks the OS process-exit boundary itself.
	store.closeOnce.Do(func() {
		store.writer.close()
		store.closeErr = errors.Join(store.journal.file.Close(), store.db.Close())
	})
	if store.closeErr != nil {
		t.Fatal(store.closeErr)
	}
	reopened, err := OpenWithOptions(directory, Options{PoolDurability: Buffered})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := reopened.Read(context.Background(), func(tx pool.ReadTx) error {
		for _, name := range []string{"checkpointed", "tail", "after-rotation"} {
			if _, err := tx.Pool(name); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
