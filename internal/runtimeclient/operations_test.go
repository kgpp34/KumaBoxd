package runtimeclient

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestTypedOperationsMatchRuntimeWireContract(t *testing.T) {
	var calls atomic.Int32
	socket := startRuntime(t, new(atomic.Int32), func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		params := readParams(t, r)
		result := `{"ID":"sandbox-id","State":"running","Config":{"Name":"clone-test","CPUs":2,"Memory":536870912,"NICs":1,"NetworkBackend":"ebpf"}}`
		switch strings.TrimPrefix(r.URL.Path, "/rpc/v1/") {
		case "snapshot.inspect":
			if !bytes.Contains(params, []byte(`"Reference":"golden"`)) {
				t.Errorf("snapshot parameters: %s", params)
			}
			result = `{"ID":"capture-id","Config":{"CPUs":2,"FutureField":true}}`
		case "image.inspect":
			result = `{"ManifestDigest":"digest","Boot":{"Profile":"overlay-v1"}}`
		case "snapshot.clone":
			var request target[CloneOptions]
			if err := json.Unmarshal(params, &request); err != nil {
				t.Error(err)
			}
			if request.Reference != "capture-id" || request.Options.Name != "clone-test" || !request.Options.WaitForNetwork {
				t.Errorf("clone parameters: %s", params)
			}
			if bytes.Contains(params, []byte("SkipReseed")) {
				t.Error("client must not expose a reseed bypass")
			}
		case "sandbox.list":
			if string(params) != "true" {
				t.Errorf("list parameters: %s", params)
			}
			result = "[" + result + "]"
		case "sandbox.status":
			if string(params) != "[]" {
				t.Errorf("status parameters: %s", params)
			}
			result = `[{"Sandbox":` + result + `,"Runtime":"absent","Stale":true}]`
		case "sandbox.stop":
			if !bytes.Contains(params, []byte(`"Force":true`)) {
				t.Errorf("stop parameters: %s", params)
			}
		case "sandbox.inspect", "sandbox.remove":
			if !bytes.Contains(params, []byte(`"Reference":"sandbox-id"`)) {
				t.Errorf("sandbox parameters: %s", params)
			}
		default:
			t.Errorf("unexpected operation %s", r.URL.Path)
		}
		_, _ = io.WriteString(w, `{"type":"status","data":"working"}`+"\n")
		_, _ = io.WriteString(w, `{"type":"result","data":`+result+"}\n")
	})
	client, err := New(socket, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	ctx := context.Background()
	capture, err := client.InspectSnapshot(ctx, "golden")
	if err != nil || !bytes.Contains(capture.Config, []byte("FutureField")) {
		t.Fatalf("capture fields lost: %+v, %v", capture, err)
	}
	image, err := client.InspectImage(ctx, "digest")
	if err != nil || image.Boot.Profile != "overlay-v1" {
		t.Fatalf("image = %+v, %v", image, err)
	}
	clone, err := client.Clone(ctx, capture.ID, CloneOptions{Name: "clone-test", WaitForNetwork: true})
	if err != nil || clone.Config.NetworkBackend != "ebpf" {
		t.Fatalf("clone = %+v, %v", clone, err)
	}
	if _, err := client.InspectSandbox(ctx, clone.ID); err != nil {
		t.Fatal(err)
	}
	listed, err := client.ListSandboxes(ctx, true)
	if err != nil || len(listed) != 1 {
		t.Fatalf("list = %+v, %v", listed, err)
	}
	observed, err := client.Status(ctx)
	if err != nil || len(observed) != 1 || !observed[0].Stale || observed[0].Runtime != "absent" {
		t.Fatalf("status observation lost: %+v %v", observed, err)
	}
	if _, err := client.Stop(ctx, clone.ID, true); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Remove(ctx, clone.ID); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 8 {
		t.Fatalf("got %d calls, want 8", calls.Load())
	}
}

func TestExecSeparatesBinaryStreamsAndExitStatus(t *testing.T) {
	output := []byte{0, 1, 255, '\n'}
	socket := startRuntime(t, new(atomic.Int32), func(w http.ResponseWriter, r *http.Request) {
		params := readParams(t, r)
		var request target[Command]
		if err := json.Unmarshal(params, &request); err != nil {
			t.Error(err)
		}
		if request.Options.Env["TEST"] != "ok" || request.Options.Cwd != "/tmp" || request.Options.User != "1000" {
			t.Errorf("command parameters: %s", params)
		}
		encoder := json.NewEncoder(w)
		_ = encoder.Encode(struct {
			Type  string `json:"type"`
			Bytes []byte `json:"bytes"`
		}{"stdout", output})
		_, _ = io.WriteString(w, "{\"type\":\"stderr\",\"bytes\":\"d2FybmluZw==\"}\n{\"type\":\"result\",\"data\":17}\n")
	})
	client, err := New(socket, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	var stdout, stderr bytes.Buffer
	code, err := client.Exec(context.Background(), "sandbox", Command{
		Args: []string{"/bin/false"}, Env: map[string]string{"TEST": "ok"}, Cwd: "/tmp", User: "1000",
	}, &stdout, &stderr)
	if err != nil || code != 17 || !bytes.Equal(stdout.Bytes(), output) || stderr.String() != "warning" {
		t.Fatalf("exec: code %d stdout %q stderr %q error %v", code, stdout.Bytes(), stderr.String(), err)
	}
}

func TestClonePreservesCommittedFailureAndDoesNotRetry(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(map[bool]string{false: "connection_lost", true: "committed_failure"}[terminal], func(t *testing.T) {
			var calls atomic.Int32
			socket := startRuntime(t, new(atomic.Int32), func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				if terminal {
					_, _ = io.WriteString(w, `{"type":"result","data":{"ID":"retained-id"},"failure":{"message":"cleanup failed","class":4,"code":"ARTIFACT_UNAVAILABLE","committed":true}}`+"\n")
				} else {
					_, _ = io.WriteString(w, `{"type":"sandbox_committed","data":{"ID":"retained-id"}}`+"\n")
				}
			})
			client, err := New(socket, 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = client.Close() }()
			result, err := client.Clone(context.Background(), "golden", CloneOptions{Name: "unique-name"})
			if calls.Load() != 1 {
				t.Fatalf("mutation retried %d times", calls.Load())
			}
			if terminal {
				var failure *RemoteError
				if !errors.As(err, &failure) || !failure.Committed || failure.Class != 4 ||
					failure.Code != "ARTIFACT_UNAVAILABLE" || result.ID != "retained-id" {
					t.Fatalf("lost partial result or commit boundary: %+v, %v", result, err)
				}
			} else {
				var unknown *OutcomeUnknownError
				if !errors.As(err, &unknown) {
					t.Fatalf("got %v, want unknown mutation outcome", err)
				}
			}
		})
	}
}

func readParams(t *testing.T, r *http.Request) json.RawMessage {
	t.Helper()
	var size [4]byte
	if _, err := io.ReadFull(r.Body, size[:]); err != nil {
		t.Error(err)
		return nil
	}
	length := binary.BigEndian.Uint32(size[:])
	if length == 0 || length > 1<<20 {
		t.Errorf("invalid request size %d", length)
		return nil
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r.Body, data); err != nil {
		t.Error(err)
		return nil
	}
	var envelope struct {
		Params json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		t.Error(err)
	}
	return envelope.Params
}
