package pool_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/kgpp34/kumaboxd/internal/metadata"
	"github.com/kgpp34/kumaboxd/internal/pool"
	"github.com/kgpp34/kumaboxd/internal/runtimeclient"
	"github.com/kgpp34/kumaboxd/internal/template"
)

type fakeRuntime struct {
	loseReply bool
	mu        sync.Mutex
	vms       map[string]runtimeclient.Sandbox
	cloned    int
	removed   map[string]bool
	block     <-chan struct{}
}

func (f *fakeRuntime) Clone(ctx context.Context, _ string, o runtimeclient.CloneOptions) (runtimeclient.Sandbox, error) {
	f.mu.Lock()
	block := f.block
	f.mu.Unlock()
	if block != nil {
		select {
		case <-ctx.Done():
			return runtimeclient.Sandbox{}, ctx.Err()
		case <-block:
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cloned++
	vm := runtimeclient.Sandbox{ID: fmt.Sprintf("vm-%d", f.cloned), Config: runtimeclient.GuestConfig{Name: o.Name, CPUs: 1, Memory: 1 << 20, Storage: 1 << 20, NICs: 1, NetworkBackend: "ebpf"}, ImageDigest: fixture().ImageDigest, VMM: "cloud-hypervisor", State: "running"}
	f.vms[vm.ID] = vm
	if f.loseReply {
		f.loseReply = false
		return runtimeclient.Sandbox{}, &runtimeclient.OutcomeUnknownError{Operation: "snapshot.clone", Cause: io.EOF}
	}
	return vm, nil
}
func (f *fakeRuntime) InspectSandbox(_ context.Context, ref string) (runtimeclient.Sandbox, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, v := range f.vms {
		if v.ID == ref || v.Config.Name == ref {
			return v, nil
		}
	}
	return runtimeclient.Sandbox{}, &runtimeclient.RemoteError{Code: "NOT_FOUND"}
}
func (f *fakeRuntime) Exec(context.Context, string, runtimeclient.Command, io.Writer, io.Writer) (int, error) {
	return 0, nil
}
func (f *fakeRuntime) Stop(_ context.Context, id string, _ bool) (runtimeclient.Sandbox, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.vms[id]
	if !ok {
		return v, &runtimeclient.RemoteError{Code: "NOT_FOUND"}
	}
	v.State = "stopped"
	f.vms[id] = v
	return v, nil
}
func (f *fakeRuntime) Remove(_ context.Context, id string) (runtimeclient.Sandbox, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.vms[id]
	if !ok {
		return v, &runtimeclient.RemoteError{Code: "NOT_FOUND"}
	}
	delete(f.vms, id)
	f.removed[id] = true
	return v, nil
}

type catalog struct{}

func (catalog) Get(context.Context, string) (template.Template, error) { return fixture(), nil }
func fixture() template.Template {
	return template.Template{Name: "test", SnapshotID: "11111111-1111-4111-8111-111111111111", ImageDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", VMM: "cloud-hypervisor", BootProfile: "native-v1", OS: "linux", Architecture: "amd64", GuestConfig: json.RawMessage(`{"CPUs":1,"Memory":1048576,"Storage":1048576,"NICs":1,"NetworkBackend":"ebpf"}`), CreatedAt: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)}
}
func open(t *testing.T, path string) *metadata.Store {
	t.Helper()
	store, err := metadata.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	return store
}
func options() pool.Options {
	o := pool.DefaultOptions()
	o.OperationTimeout = time.Second
	o.ReconcileInterval = 10 * time.Millisecond
	o.Workers = 4
	o.MaxVMs = 32
	o.MemoryBudget = 32 << 20
	return o
}
func service(t *testing.T, store *metadata.Store, runtime *fakeRuntime, o pool.Options) *pool.Service {
	t.Helper()
	s, err := pool.New(store, runtime, catalog{}, pool.FixedPolicy{}, o, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func start(t *testing.T, s *pool.Service) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(10 * time.Second):
				t.Error("pool worker did not stop")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}
func until(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition did not converge")
}
func configured(t *testing.T, s *pool.Service, target, max int) {
	t.Helper()
	if _, _, err := s.Configure(context.Background(), pool.Spec{Name: "test", Template: "test", TargetReady: target, MaxTotal: max}); err != nil {
		t.Fatal(err)
	}
}
func ready(t *testing.T, s *pool.Service, n int) {
	t.Helper()
	until(t, func() bool {
		v, e := s.Status(context.Background(), "test")
		return e == nil && v.ControllerReady && v.Counts.Ready == n
	})
}
func runtimeFixture() *fakeRuntime {
	return &fakeRuntime{vms: map[string]runtimeclient.Sandbox{}, removed: map[string]bool{}}
}

func TestConcurrentClaimsDoNotDoubleAllocateAndRetryIsDurable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	store := open(t, path)
	runtime := runtimeFixture()
	s := service(t, store, runtime, options())
	stop := start(t, s)
	configured(t, s, 16, 16)
	ready(t, s, 16)
	var wg sync.WaitGroup
	receipts := make(chan pool.Lease, 16)
	failures := make(chan error, 16)
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, created, err := s.Claim(context.Background(), "test", pool.ClaimRequest{Key: fmt.Sprintf("request-%d", i), Owner: "owner", TTLSeconds: 300})
			if err != nil || !created {
				failures <- fmt.Errorf("claim: created=%v err=%w", created, err)
				return
			}
			receipts <- lease
		}()
	}
	wg.Wait()
	close(receipts)
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	seen := map[string]bool{}
	for v := range receipts {
		if seen[v.SandboxID] {
			t.Fatal("VM allocated twice")
		}
		seen[v.SandboxID] = true
	}
	if len(seen) != 16 {
		t.Fatalf("got %d allocations", len(seen))
	}
	if _, _, err := s.Claim(context.Background(), "test", pool.ClaimRequest{Key: "empty", Owner: "owner", TTLSeconds: 300}); !errors.Is(err, pool.ErrEmpty) {
		t.Fatalf("missing fail-fast exhaustion: %v", err)
	}
	original, created, err := s.Claim(context.Background(), "test", pool.ClaimRequest{Key: "request-0", Owner: "owner", TTLSeconds: 300})
	if err != nil || created {
		t.Fatalf("retry: %v %v", created, err)
	}
	if _, _, err := s.Claim(context.Background(), "test", pool.ClaimRequest{Key: "request-0", Owner: "other", TTLSeconds: 300}); !errors.Is(err, pool.ErrConflict) {
		t.Fatalf("payload conflict: %v", err)
	}
	stop()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := open(t, path)
	defer func() { _ = reopened.Close() }()
	next := service(t, reopened, runtime, options())
	stopNext := start(t, next)
	defer stopNext()
	until(t, func() bool {
		retry, created, err := next.Claim(context.Background(), "test", pool.ClaimRequest{Key: "request-0", Owner: "owner", TTLSeconds: 300})
		if errors.Is(err, pool.ErrUnavailable) {
			return false
		}
		if err != nil || created || retry.ID != original.ID {
			t.Fatalf("restart lost idempotency: %+v %v %v", retry, created, err)
		}
		return true
	})
}

func TestReleaseDestroysUsedVMAndRefillsFromFreshClone(t *testing.T) {
	store := open(t, filepath.Join(t.TempDir(), "state"))
	defer func() { _ = store.Close() }()
	runtime := runtimeFixture()
	s := service(t, store, runtime, options())
	stop := start(t, s)
	defer stop()
	configured(t, s, 1, 2)
	ready(t, s, 1)
	lease, _, err := s.Claim(context.Background(), "test", pool.ClaimRequest{Key: "one", Owner: "owner", TTLSeconds: 300})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := s.Release(context.Background(), lease.ID); err != nil {
			t.Fatal(err)
		}
	}
	until(t, func() bool {
		v, e := s.GetLease(context.Background(), lease.ID)
		return e == nil && v.State == pool.Released
	})
	ready(t, s, 1)
	next, _, err := s.Claim(context.Background(), "test", pool.ClaimRequest{Key: "two", Owner: "owner", TTLSeconds: 300})
	if err != nil {
		t.Fatal(err)
	}
	if next.SandboxID == lease.SandboxID {
		t.Fatal("used VM was handed out again")
	}
	runtime.mu.Lock()
	removed := runtime.removed[lease.SandboxID]
	runtime.mu.Unlock()
	if !removed {
		t.Fatal("released sandbox was not destroyed")
	}
	if _, _, err := s.Claim(context.Background(), "test", pool.ClaimRequest{Key: "one", Owner: "owner", TTLSeconds: 300}); !errors.Is(err, pool.ErrExpired) {
		t.Fatalf("released key allocated again: %v", err)
	}
}

func TestTTLAndAbsoluteRenewal(t *testing.T) {
	store := open(t, filepath.Join(t.TempDir(), "state"))
	defer func() { _ = store.Close() }()
	s := service(t, store, runtimeFixture(), options())
	stop := start(t, s)
	defer stop()
	configured(t, s, 1, 1)
	ready(t, s, 1)
	lease, _, err := s.Claim(context.Background(), "test", pool.ClaimRequest{Key: "ttl", Owner: "owner", TTLSeconds: 1})
	if err != nil {
		t.Fatal(err)
	}
	untilAt := lease.ExpiresAt.Add(100 * time.Millisecond)
	first, err := s.Renew(context.Background(), lease.ID, untilAt)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Renew(context.Background(), lease.ID, untilAt)
	if err != nil || !first.ExpiresAt.Equal(second.ExpiresAt) {
		t.Fatalf("renew retry extended deadline: %v", err)
	}
	until(t, func() bool {
		v, e := s.GetLease(context.Background(), lease.ID)
		return e == nil && v.State == pool.Released
	})
	if _, err := s.Renew(context.Background(), lease.ID, time.Now().Add(time.Hour)); !errors.Is(err, pool.ErrExpired) {
		t.Fatalf("expired lease resurrected: %v", err)
	}
}

func TestNodeBudgetIncludesCreatingAndLeasedVMs(t *testing.T) {
	store := open(t, filepath.Join(t.TempDir(), "state"))
	defer func() { _ = store.Close() }()
	o := options()
	o.MemoryBudget = 2 << 20
	s := service(t, store, runtimeFixture(), o)
	stop := start(t, s)
	defer stop()
	configured(t, s, 4, 4)
	ready(t, s, 2)
	lease, _, err := s.Claim(context.Background(), "test", pool.ClaimRequest{Key: "charge", Owner: "owner", TTLSeconds: 300})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Release(context.Background(), lease.ID); err != nil {
		t.Fatal(err)
	}
	ready(t, s, 2)
	status, err := s.Status(context.Background(), "test")
	if err != nil || status.Counts.Total != 2 {
		t.Fatalf("budget exceeded: %+v %v", status.Counts, err)
	}
	configured(t, s, 0, 4)
	until(t, func() bool { v, e := s.Status(context.Background(), "test"); return e == nil && v.Counts.Total == 0 })
}

func TestRestartQuarantinesReadyAndDoesNotRecloneIntent(t *testing.T) {
	store := open(t, filepath.Join(t.TempDir(), "state"))
	defer func() { _ = store.Close() }()
	runtime := runtimeFixture()
	s := service(t, store, runtime, options())
	stop := start(t, s)
	configured(t, s, 1, 2)
	ready(t, s, 1)
	stop()
	runtime.mu.Lock()
	for id := range runtime.vms {
		delete(runtime.vms, id)
	}
	runtime.mu.Unlock()
	next := service(t, store, runtime, options())
	stopNext := start(t, next)
	defer stopNext()
	ready(t, next, 1)
	lease, _, err := next.Claim(context.Background(), "test", pool.ClaimRequest{Key: "fresh", Owner: "owner", TTLSeconds: 300})
	if err != nil {
		t.Fatal(err)
	}
	if lease.SandboxID == "vm-1" {
		t.Fatal("stale Ready record was allocated")
	}
}

func TestReclamationDoesNotWaitForBlockedProvisioning(t *testing.T) {
	store := open(t, filepath.Join(t.TempDir(), "state"))
	defer func() { _ = store.Close() }()
	runtime := runtimeFixture()
	o := options()
	o.Workers = 1
	s := service(t, store, runtime, o)
	stop := start(t, s)
	defer stop()
	configured(t, s, 1, 2)
	ready(t, s, 1)
	runtime.mu.Lock()
	runtime.block = make(chan struct{})
	runtime.mu.Unlock()
	lease, _, err := s.Claim(context.Background(), "test", pool.ClaimRequest{Key: "active", Owner: "owner", TTLSeconds: 300})
	if err != nil {
		t.Fatal(err)
	}
	until(t, func() bool { v, e := s.Status(context.Background(), "test"); return e == nil && v.Counts.Creating == 1 })
	if _, err := s.Release(context.Background(), lease.ID); err != nil {
		t.Fatal(err)
	}
	until(t, func() bool {
		v, e := s.GetLease(context.Background(), lease.ID)
		return e == nil && v.State == pool.Released
	})
}

func TestLostCloneReplyCannotPublishOrReissueTheVM(t *testing.T) {
	store := open(t, filepath.Join(t.TempDir(), "state"))
	defer func() { _ = store.Close() }()
	runtime := runtimeFixture()
	runtime.loseReply = true
	s := service(t, store, runtime, options())
	stop := start(t, s)
	defer stop()
	configured(t, s, 1, 1)
	ready(t, s, 1)
	lease, _, err := s.Claim(context.Background(), "test", pool.ClaimRequest{Key: "known", Owner: "owner", TTLSeconds: 300})
	if err != nil {
		t.Fatal(err)
	}
	runtime.mu.Lock()
	removed := runtime.removed["vm-1"]
	count := runtime.cloned
	runtime.mu.Unlock()
	if !removed || count != 2 || lease.SandboxID != "vm-2" {
		t.Fatalf("ambiguous creation was reused or retried: removed=%v clones=%d lease=%+v", removed, count, lease)
	}
}

type gatedPolicy struct {
	mu      sync.Mutex
	enabled bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *gatedPolicy) Decide(ctx context.Context, o pool.Observation) (map[string]pool.Decision, error) {
	p.mu.Lock()
	enabled := p.enabled
	p.mu.Unlock()
	if enabled {
		p.once.Do(func() { close(p.entered) })
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.release:
		}
	}
	return (pool.FixedPolicy{}).Decide(ctx, o)
}
func TestPolicyComputationCannotHoldTheClaimTransaction(t *testing.T) {
	store := open(t, filepath.Join(t.TempDir(), "state"))
	defer func() { _ = store.Close() }()
	runtime := runtimeFixture()
	o := options()
	o.PolicyTimeout = time.Second
	policy := &gatedPolicy{entered: make(chan struct{}), release: make(chan struct{})}
	s, err := pool.New(store, runtime, catalog{}, policy, o, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	stop := start(t, s)
	defer stop()
	configured(t, s, 1, 1)
	ready(t, s, 1)
	policy.mu.Lock()
	policy.enabled = true
	policy.mu.Unlock()
	configured(t, s, 1, 1)
	select {
	case <-policy.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("policy was not invoked")
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := s.Claim(context.Background(), "test", pool.ClaimRequest{Key: "independent", Owner: "owner", TTLSeconds: 300})
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("claim waited for the blocked policy")
	}
	close(policy.release)
}

func TestConcurrentIdenticalKeysCreateOneDurableLease(t *testing.T) {
	store := open(t, filepath.Join(t.TempDir(), "state"))
	defer func() { _ = store.Close() }()
	s := service(t, store, runtimeFixture(), options())
	stop := start(t, s)
	defer stop()
	configured(t, s, 1, 1)
	ready(t, s, 1)
	var group sync.WaitGroup
	ids := make([]string, 32)
	created := make([]bool, 32)
	failures := make([]error, 32)
	for index := range 32 {
		group.Add(1)
		go func() {
			defer group.Done()
			var v pool.Lease
			v, created[index], failures[index] = s.Claim(context.Background(), "test", pool.ClaimRequest{Key: "same-key", Owner: "owner", TTLSeconds: 300})
			ids[index] = v.ID
		}()
	}
	group.Wait()
	fresh := 0
	for i, id := range ids {
		if failures[i] != nil || id == "" || id != ids[0] {
			t.Fatalf("non-idempotent parallel retry: ids=%v errors=%v", ids, failures)
		}
		if created[i] {
			fresh++
		}
	}
	if fresh != 1 {
		t.Fatalf("created %d leases for one request", fresh)
	}
}

func (f *fakeRuntime) Status(_ context.Context, references ...string) ([]runtimeclient.SandboxStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]runtimeclient.SandboxStatus, 0, len(f.vms))
	selected := map[string]bool{}
	for _, ref := range references {
		if _, ok := f.vms[ref]; !ok {
			return nil, &runtimeclient.RemoteError{Code: "NOT_FOUND"}
		}
		selected[ref] = true
	}
	for _, v := range f.vms {
		if len(references) > 0 && !selected[v.ID] {
			continue
		}
		state := "absent"
		if v.State == "running" {
			state = "running"
		}
		out = append(out, runtimeclient.SandboxStatus{Sandbox: v, Runtime: state})
	}
	return out, nil
}

func TestCrashedIdleVMIsRemovedAndReplaced(t *testing.T) {
	store := open(t, filepath.Join(t.TempDir(), "state"))
	defer func() { _ = store.Close() }()
	runtime := runtimeFixture()
	s := service(t, store, runtime, options())
	stop := start(t, s)
	defer stop()
	configured(t, s, 1, 1)
	ready(t, s, 1)
	runtime.mu.Lock()
	delete(runtime.vms, "vm-1")
	runtime.mu.Unlock()
	until(t, func() bool {
		runtime.mu.Lock()
		cloned := runtime.cloned
		runtime.mu.Unlock()
		status, err := s.Status(context.Background(), "test")
		return err == nil && status.Counts.Ready == 1 && cloned == 2
	})
	lease, _, err := s.Claim(context.Background(), "test", pool.ClaimRequest{Key: "after-crash", Owner: "owner", TTLSeconds: 300})
	if err != nil || lease.SandboxID != "vm-2" {
		t.Fatalf("stale VM delivered: %+v %v", lease, err)
	}
}

type catalogFunc func(context.Context, string) (template.Template, error)

func (f catalogFunc) Get(ctx context.Context, name string) (template.Template, error) {
	return f(ctx, name)
}
func TestAdmissionChargesInheritedDataDisks(t *testing.T) {
	store := open(t, filepath.Join(t.TempDir(), "state"))
	defer func() { _ = store.Close() }()
	o := options()
	o.StorageBudget = 2 << 20
	captured := fixture()
	captured.GuestConfig = json.RawMessage(`{"CPUs":1,"Memory":1048576,"Storage":1048576,"NICs":1,"NetworkBackend":"ebpf","DataDisks":[{"Name":"data","Size":3145728}]}`)
	s, err := pool.New(store, runtimeFixture(), catalogFunc(func(context.Context, string) (template.Template, error) { return captured, nil }), pool.FixedPolicy{}, o, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Configure(context.Background(), pool.Spec{Name: "test", Template: "test", TargetReady: 1, MaxTotal: 1}); !errors.Is(err, pool.ErrInvalid) {
		t.Fatalf("data disk reservation was ignored: %v", err)
	}
}
