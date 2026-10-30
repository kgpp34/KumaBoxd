package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/kgpp34/kumaboxd/internal/template"
)

func TestTemplateBindingSurvivesRestartAndPreservesUnknownFields(t *testing.T) {
	directory := privateDirectory(t)
	store, err := Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	record := storedTemplate("python")
	first, created, err := store.Put(context.Background(), record)
	if err != nil || !created {
		t.Fatalf("create = %+v, %v, %v", first, created, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	second, err := store.Get(context.Background(), record.Name)
	if err != nil || string(second.GuestConfig) != string(record.GuestConfig) || !second.CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("lost durable metadata: %+v, %v", second, err)
	}
	// JSON key order and insignificant whitespace do not change a binding.
	record.CreatedAt = record.CreatedAt.Add(time.Minute)
	record.GuestConfig = json.RawMessage(`{ "Future": { "enabled": true }, "NICs": 1, "Storage": 10737418240, "Memory":536870912, "CPUs":2 }`)
	again, created, err := store.Put(context.Background(), record)
	if err != nil || created || !again.CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("idempotent retry replaced metadata: %+v, %v, %v", again, created, err)
	}
	record.SnapshotID = "22222222-2222-4222-8222-222222222222"
	if _, _, err := store.Put(context.Background(), record); !errors.Is(err, template.ErrConflict) {
		t.Fatalf("changed binding accepted: %v", err)
	}
	if _, err := store.Get(context.Background(), "missing"); !errors.Is(err, template.ErrNotFound) {
		t.Fatalf("missing template = %v", err)
	}
}

func TestConcurrentRegistrationHasExactlyOneWinner(t *testing.T) {
	store := openStore(t)
	var winners atomic.Int32
	var workers sync.WaitGroup
	errorsFound := make(chan error, 32)
	for range 32 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, created, err := store.Put(context.Background(), storedTemplate("shared"))
			if err != nil {
				errorsFound <- err
			}
			if created {
				winners.Add(1)
			}
		}()
	}
	workers.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Error(err)
	}
	if winners.Load() != 1 {
		t.Fatalf("got %d successful creations, want 1", winners.Load())
	}
}

func TestListUsesBoundedExclusiveCursorAndHonorsCancellation(t *testing.T) {
	store := openStore(t)
	ctx := context.Background()
	items, next, err := store.List(ctx, "", 2)
	if err != nil || items == nil || len(items) != 0 || next != "" {
		t.Fatalf("empty list: %v %q %v", items, next, err)
	}
	for _, name := range []string{"gamma", "alpha", "beta"} {
		if _, _, err := store.Put(ctx, storedTemplate(name)); err != nil {
			t.Fatal(err)
		}
	}
	items, next, err = store.List(ctx, "", 2)
	if err != nil || len(items) != 2 || items[0].Name != "alpha" || items[1].Name != "beta" || next != "beta" {
		t.Fatalf("first page: %v %q %v", items, next, err)
	}
	items, next, err = store.List(ctx, next, 2)
	if err != nil || len(items) != 1 || items[0].Name != "gamma" || next != "" {
		t.Fatalf("last page: %v %q %v", items, next, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := store.Put(canceled, storedTemplate("canceled")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled write: %v", err)
	}
	if _, err := store.Get(ctx, "canceled"); !errors.Is(err, template.ErrNotFound) {
		t.Fatalf("canceled write changed state: %v", err)
	}
}

func TestWriterWaitIsCancelable(t *testing.T) {
	store := openStore(t)
	store.writer <- struct{}{}
	defer func() { <-store.writer }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, _, err := store.Put(ctx, storedTemplate("blocked")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("writer wait did not respect deadline: %v", err)
	}
}

func TestOpenRejectsSecondOwnerAndForeignDatabase(t *testing.T) {
	t.Run("second_owner", func(t *testing.T) {
		directory := privateDirectory(t)
		store, err := Open(directory)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = store.Close() }()
		other, err := Open(directory)
		if err == nil {
			_ = other.Close()
			t.Fatal("second process owner was accepted")
		}
	})
	t.Run("foreign_format", func(t *testing.T) {
		directory := privateDirectory(t)
		path := filepath.Join(directory, "node.db")
		foreign, err := bolt.Open(path, 0o600, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := foreign.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucket([]byte("foreign"))
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if err := foreign.Close(); err != nil {
			t.Fatal(err)
		}
		if store, err := Open(directory); err == nil {
			_ = store.Close()
			t.Fatal("populated foreign database was accepted")
		}
		foreign, err = bolt.Open(path, 0o600, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = foreign.Close() }()
		if err := foreign.View(func(tx *bolt.Tx) error {
			if tx.Bucket([]byte("foreign")) == nil || tx.Bucket(metadataKey) != nil {
				return errors.New("foreign database changed")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("symlink", func(t *testing.T) {
		directory := privateDirectory(t)
		if err := os.Symlink("/does-not-exist", filepath.Join(directory, "node.db")); err != nil {
			t.Fatal(err)
		}
		if store, err := Open(directory); err == nil {
			_ = store.Close()
			t.Fatal("database symlink was accepted")
		}
	})
}

func storedTemplate(name string) template.Template {
	return template.Template{
		Name: name, SnapshotID: "11111111-1111-4111-8111-111111111111",
		ImageDigest: "sha256:" + strings.Repeat("a", 64), VMM: "cloud-hypervisor",
		BootProfile: "overlay-v1", OS: "linux", Architecture: "amd64",
		GuestConfig: json.RawMessage(`{"CPUs":2,"Memory":536870912,"Storage":10737418240,"NICs":1,"Future":{"enabled":true}}`),
		CreatedAt:   time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC),
	}
}

func openStore(t testing.TB) *Store {
	t.Helper()
	store, err := Open(privateDirectory(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(fmt.Errorf("close test store: %w", err))
		}
	})
	return store
}

func privateDirectory(t testing.TB) string {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	return directory
}
