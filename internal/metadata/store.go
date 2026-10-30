// Package metadata owns KumaBoxd's database. It never opens runtime metadata.
package metadata

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/kgpp34/kumaboxd/internal/template"
)

const format = "kumaboxd.node.v1"

var (
	formatKey    = []byte("format")
	templatesKey = []byte("templates")
	metadataKey  = []byte("kumaboxd")
)

// Store holds one process-wide database handle with durable write transactions.
// Concurrent writes share durable transactions through one process-owned writer.
type Store struct {
	db        *bolt.DB
	writer    *writeQueue
	closeOnce sync.Once
	closeErr  error
}

var _ template.Store = (*Store)(nil)

// Open creates a private node.db with exclusive ownership and synchronous commits.
// An unknown file format is rejected; runtime database names are never used.
func Open(directory string) (*Store, error) {
	if !filepath.IsAbs(directory) {
		return nil, errors.New("metadata directory must be absolute")
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm()&0o022 != 0 {
		return nil, errors.New("metadata directory must be a directory without group or other write access")
	}
	path := filepath.Join(directory, "node.db")
	if info, err := os.Lstat(path); err == nil && (!info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0) {
		return nil, errors.New("node database must be a private regular file")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open node database: %w", err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket(metadataKey)
		if root != nil {
			if string(root.Get(formatKey)) != format || root.Bucket(templatesKey) == nil {
				return errors.New("unsupported node database format")
			}
			return nil
		}
		key, _ := tx.Cursor().First()
		if key != nil {
			return errors.New("refusing to initialize a populated foreign database")
		}
		root, err := tx.CreateBucket(metadataKey)
		if err != nil {
			return err
		}
		if err := root.Put(formatKey, []byte(format)); err != nil {
			return err
		}
		_, err = root.CreateBucket(templatesKey)
		return err
	})
	if err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return &Store{db: db, writer: newWriteQueue(db)}, nil
}

// Close releases node state after HTTP requests and background users have stopped.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.writer.close()
		s.closeErr = s.db.Close()
	})
	return s.closeErr
}

// Put binds a name once and acknowledges it only after a durable commit.
// Waiting to enqueue is cancelable. Accepted requests wait for a known result;
// cancellation is checked again before any transaction changes the record.
func (s *Store) Put(ctx context.Context, record template.Template) (template.Template, bool, error) {
	if err := record.Validate(); err != nil {
		return template.Template{}, false, err
	}
	result := s.writer.submit(ctx, record)
	return result.record, result.created, result.err
}

func putTemplate(tx *bolt.Tx, record template.Template) (template.Template, bool, error) {
	bucket := tx.Bucket(metadataKey).Bucket(templatesKey)
	key := []byte(record.Name)
	if data := bucket.Get(key); data != nil {
		var existing template.Template
		if err := decode(data, &existing); err != nil {
			return template.Template{}, false, err
		}
		if existing.SnapshotID != record.SnapshotID || existing.ImageDigest != record.ImageDigest ||
			existing.VMM != record.VMM || existing.BootProfile != record.BootProfile || existing.OS != record.OS ||
			existing.Architecture != record.Architecture || !sameJSON(existing.GuestConfig, record.GuestConfig) {
			return template.Template{}, false, template.ErrConflict
		}
		return existing, false, nil
	}
	data, err := json.Marshal(record)
	if err != nil {
		return template.Template{}, false, err
	}
	if err := bucket.Put(key, data); err != nil {
		return template.Template{}, false, err
	}
	return record, true, nil
}

// Get returns a validated copy that remains valid after its read transaction.
func (s *Store) Get(ctx context.Context, name string) (result template.Template, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		data := tx.Bucket(metadataKey).Bucket(templatesKey).Get([]byte(name))
		if data == nil {
			return template.ErrNotFound
		}
		return decode(data, &result)
	})
	return result, err
}

// List performs a bounded cursor scan, excluding the supplied previous name.
func (s *Store) List(ctx context.Context, after string, limit int) ([]template.Template, string, error) {
	if limit < 1 || limit > 200 {
		return nil, "", template.ErrInvalid
	}
	items := make([]template.Template, 0, limit)
	next := ""
	err := s.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(metadataKey).Bucket(templatesKey).Cursor()
		key, value := cursor.Seek([]byte(after))
		if key != nil && string(key) == after {
			key, value = cursor.Next()
		}
		for ; key != nil; key, value = cursor.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			if len(items) == limit {
				next = items[len(items)-1].Name
				break
			}
			var record template.Template
			if err := decode(value, &record); err != nil {
				return err
			}
			items = append(items, record)
		}
		return ctx.Err()
	})
	return items, next, err
}

func decode(data []byte, record *template.Template) error {
	if err := json.Unmarshal(data, record); err != nil {
		return fmt.Errorf("decode stored template: %w", err)
	}
	if err := record.Validate(); err != nil {
		return fmt.Errorf("invalid stored template: %w", err)
	}
	return nil
}

func sameJSON(left, right []byte) bool {
	canonical := func(data []byte) ([]byte, error) {
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		var value any
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}
	l, err := canonical(left)
	if err != nil {
		return false
	}
	r, err := canonical(right)
	return err == nil && bytes.Equal(l, r)
}
