package template

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/kgpp34/kumaboxd/internal/runtimeclient"
)

type observedRuntime struct {
	imageReference string
	badDigest      bool
	err            error
}

func (r *observedRuntime) InspectSnapshot(context.Context, string) (runtimeclient.Snapshot, error) {
	return runtimeclient.Snapshot{
		ID: "11111111-1111-4111-8111-111111111111", ImageDigest: "sha256:" + strings.Repeat("a", 64),
		VMM: "cloud-hypervisor", Config: json.RawMessage(`{"CPUs":2,"Memory":536870912,"Storage":10737418240,"NICs":1,"Future":true}`),
	}, r.err
}

func (r *observedRuntime) InspectImage(_ context.Context, reference string) (runtimeclient.Image, error) {
	r.imageReference = reference
	var image runtimeclient.Image
	image.ManifestDigest = reference
	if r.badDigest {
		image.ManifestDigest = "sha256:" + strings.Repeat("b", 64)
	}
	image.Platform.OS, image.Platform.Architecture = "linux", "amd64"
	image.Boot.Profile = "overlay-v1"
	return image, r.err
}

type observedStore struct {
	puts   int
	record Template
}

func (s *observedStore) Put(_ context.Context, record Template) (Template, bool, error) {
	s.puts++
	s.record = record
	return record, true, nil
}

func (s *observedStore) Get(context.Context, string) (Template, error) { return s.record, nil }

func (s *observedStore) List(context.Context, string, int) ([]Template, string, error) {
	return []Template{s.record}, "", nil
}

func TestRegistrationResolvesImmutableSnapshotAndExactImage(t *testing.T) {
	runtime, store := new(observedRuntime), new(observedStore)
	service, err := New(runtime, store)
	if err != nil {
		t.Fatal(err)
	}
	record, created, err := service.Register(context.Background(), RegisterRequest{Name: "python", Snapshot: "mutable-alias"})
	if err != nil || !created || store.puts != 1 || record.SnapshotID == "mutable-alias" ||
		runtime.imageReference != record.ImageDigest || !strings.Contains(string(record.GuestConfig), "Future") {
		t.Fatalf("incorrect binding: %+v, created %v error %v", record, created, err)
	}
}

func TestInvalidRuntimeMetadataNeverReachesStore(t *testing.T) {
	for _, runtime := range []*observedRuntime{
		{badDigest: true}, {err: errors.New("runtime unavailable")},
	} {
		store := new(observedStore)
		service, err := New(runtime, store)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := service.Register(context.Background(), RegisterRequest{Name: "python", Snapshot: "golden"}); err == nil || store.puts != 0 {
			t.Fatalf("bad runtime metadata reached store: %v, writes %d", err, store.puts)
		}
	}
}

func TestInputValidationPreventsRuntimeAndUnboundedListing(t *testing.T) {
	runtime, store := new(observedRuntime), new(observedStore)
	service, err := New(runtime, store)
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []RegisterRequest{{Name: "../bad", Snapshot: "x"}, {Name: "python"}} {
		if _, _, err := service.Register(context.Background(), request); !errors.Is(err, ErrInvalid) {
			t.Errorf("invalid request accepted: %+v, %v", request, err)
		}
	}
	for _, limit := range []int{0, -1, 201} {
		if _, err := service.List(context.Background(), "", limit); !errors.Is(err, ErrInvalid) {
			t.Errorf("invalid limit %d accepted: %v", limit, err)
		}
	}
	if runtime.imageReference != "" || store.puts != 0 {
		t.Fatal("invalid input caused dependency calls")
	}
}
