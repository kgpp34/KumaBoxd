// Package template registers immutable golden references without owning VM state.
//
//	request -> resolve snapshot -> inspect exact image -> validate -> transaction
//
// Runtime calls complete before opening a metadata write transaction.
package template

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/kgpp34/kumaboxd/internal/runtimeclient"
)

var (
	// ErrInvalid identifies invalid registration or pagination input.
	ErrInvalid = errors.New("invalid template input")
	// ErrNotFound identifies an unknown registered template name.
	ErrNotFound = errors.New("template not found")
	// ErrConflict means a name already identifies another immutable template.
	ErrConflict = errors.New("template name already registered")
	validName   = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,62}$`)
)

// Runtime is the read-only runtime contract needed to register a template.
type Runtime interface {
	InspectSnapshot(context.Context, string) (runtimeclient.Snapshot, error)
	InspectImage(context.Context, string) (runtimeclient.Image, error)
}

// Store atomically binds template names and supports bounded ordered listing.
// Put returns the existing record for an identical binding, never overwriting it.
type Store interface {
	Put(context.Context, Template) (Template, bool, error)
	Get(context.Context, string) (Template, error)
	List(context.Context, string, int) ([]Template, string, error)
}

// Template pins the complete captured guest shape and content-addressed image.
// Native snapshot format compatibility is checked by the runtime on clone.
type Template struct {
	// Name is the unique, immutable node-local registration key.
	Name string `json:"name"`
	// SnapshotID is resolved once; mutable snapshot aliases are not persisted.
	SnapshotID string `json:"snapshot_id"`
	// ImageDigest pins guest userspace, agent and boot artifacts through its layers.
	ImageDigest string `json:"image_digest"`
	// VMM selects the snapshot's runtime backend.
	VMM string `json:"vmm"`
	// BootProfile names the image's declared host/guest startup protocol.
	BootProfile string `json:"boot_profile"`
	// OS and Architecture are taken from the exact captured image.
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
	// GuestConfig retains all runtime configuration fields without lossy projection.
	GuestConfig json.RawMessage `json:"guest_config"`
	// CreatedAt is the first durable registration time, preserved on retries.
	CreatedAt time.Time `json:"created_at"`
}

// RegisterRequest identifies an existing local snapshot by name or ID.
type RegisterRequest struct {
	Name     string `json:"name"`
	Snapshot string `json:"snapshot"`
}

// Page provides bounded template listing in lexical name order.
type Page struct {
	Items      []Template `json:"items"`
	NextCursor string     `json:"next_cursor,omitempty"`
}

// Service validates runtime facts before binding them to a durable template.
type Service struct {
	runtime Runtime
	store   Store
}

// New requires explicit runtime and persistence dependencies.
func New(runtime Runtime, store Store) (*Service, error) {
	if runtime == nil || store == nil {
		return nil, errors.New("template runtime and store are required")
	}
	return &Service{runtime: runtime, store: store}, nil
}

// Register is idempotent for the same name and snapshot ID, including after restart.
func (s *Service) Register(ctx context.Context, request RegisterRequest) (Template, bool, error) {
	if err := ValidateName(request.Name); err != nil {
		return Template{}, false, err
	}
	if request.Snapshot == "" || len(request.Snapshot) > 512 || strings.ContainsAny(request.Snapshot, "\x00\r\n") {
		return Template{}, false, fmt.Errorf("%w: snapshot reference is required", ErrInvalid)
	}
	capture, err := s.runtime.InspectSnapshot(ctx, request.Snapshot)
	if err != nil {
		return Template{}, false, fmt.Errorf("resolve template snapshot: %w", err)
	}
	image, err := s.runtime.InspectImage(ctx, capture.ImageDigest)
	if err != nil {
		return Template{}, false, fmt.Errorf("inspect template image: %w", err)
	}
	record := Template{
		Name: request.Name, SnapshotID: capture.ID, ImageDigest: capture.ImageDigest,
		VMM: capture.VMM, BootProfile: image.Boot.Profile, OS: image.Platform.OS,
		Architecture: image.Platform.Architecture, GuestConfig: capture.Config,
		CreatedAt: time.Now().UTC(),
	}
	if image.ManifestDigest != capture.ImageDigest {
		return Template{}, false, errors.New("runtime returned a different template image digest")
	}
	if err := record.Validate(); err != nil {
		return Template{}, false, fmt.Errorf("invalid runtime template metadata: %w", err)
	}
	return s.store.Put(ctx, record)
}

// Get reads a durable template without requiring a reachable runtime.
func (s *Service) Get(ctx context.Context, name string) (Template, error) {
	if err := ValidateName(name); err != nil {
		return Template{}, err
	}
	return s.store.Get(ctx, name)
}

// List uses an exclusive name cursor and caps each response at 200 entries.
func (s *Service) List(ctx context.Context, after string, limit int) (Page, error) {
	if after != "" {
		if err := ValidateName(after); err != nil {
			return Page{}, err
		}
	}
	if limit < 1 || limit > 200 {
		return Page{}, fmt.Errorf("%w: limit must be between 1 and 200", ErrInvalid)
	}
	items, next, err := s.store.List(ctx, after, limit)
	return Page{Items: items, NextCursor: next}, err
}

// ValidateName restricts names independently of filesystem path construction.
func ValidateName(name string) error {
	if !validName.MatchString(name) {
		return fmt.Errorf("%w: name must match %s", ErrInvalid, validName)
	}
	return nil
}

// Validate rejects incomplete persisted records before they can seed future pools.
func (t Template) Validate() error {
	if err := ValidateName(t.Name); err != nil {
		return err
	}
	if len(t.SnapshotID) != 36 || t.SnapshotID[8] != '-' || t.SnapshotID[13] != '-' ||
		t.SnapshotID[18] != '-' || t.SnapshotID[23] != '-' || t.SnapshotID[14] != '4' {
		return errors.New("snapshot ID must be a canonical UUIDv4")
	}
	compact := strings.ReplaceAll(t.SnapshotID, "-", "")
	id, err := hex.DecodeString(compact)
	if err != nil || len(id) != 16 || id[8]&0xc0 != 0x80 || strings.ToLower(t.SnapshotID) != t.SnapshotID {
		return errors.New("snapshot ID must be a canonical UUIDv4")
	}
	if len(t.ImageDigest) != 71 || !strings.HasPrefix(t.ImageDigest, "sha256:") || strings.ToLower(t.ImageDigest) != t.ImageDigest {
		return errors.New("image digest must be a lowercase sha256 digest")
	}
	if _, err := hex.DecodeString(t.ImageDigest[7:]); err != nil {
		return errors.New("image digest must be hexadecimal")
	}
	if t.VMM == "" || t.BootProfile == "" || t.OS != "linux" ||
		(t.Architecture != "amd64" && t.Architecture != "arm64") || t.CreatedAt.IsZero() {
		return errors.New("backend, boot profile, Linux platform and creation time are required")
	}
	var shape runtimeclient.GuestConfig
	if err := json.Unmarshal(t.GuestConfig, &shape); err != nil || shape.CPUs == 0 || shape.Memory <= 0 || shape.Storage <= 0 || shape.NICs < 0 {
		return errors.New("captured guest resources are invalid")
	}
	return nil
}
