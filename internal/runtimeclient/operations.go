package runtimeclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Snapshot contains immutable capture identity and source guest configuration.
// Config retains all wire fields so future guest-shape fields are not lost.
type Snapshot struct {
	ID          string          `json:"ID"`
	Name        string          `json:"Name"`
	ImageDigest string          `json:"ImageDigest"`
	VMM         string          `json:"VMM"`
	Config      json.RawMessage `json:"Config"`
}

// Image contains the content-addressed platform and boot contract.
type Image struct {
	ManifestDigest string `json:"ManifestDigest"`
	Platform       struct {
		OS           string `json:"OS"`
		Architecture string `json:"Architecture"`
	} `json:"Platform"`
	Boot struct {
		Profile     string `json:"Profile"`
		KernelFile  string `json:"KernelFile"`
		InitrdFile  string `json:"InitrdFile"`
		KernelLayer string `json:"KernelLayer"`
		InitrdLayer string `json:"InitrdLayer"`
	} `json:"Boot"`
}

// GuestConfig is the subset needed for admission and network readiness checks.
// The complete captured configuration is retained separately in Snapshot.Config.
type GuestConfig struct {
	Name           string `json:"Name"`
	CPUs           uint32 `json:"CPUs"`
	Memory         int64  `json:"Memory"`
	Storage        int64  `json:"Storage"`
	NICs           int    `json:"NICs"`
	NetworkBackend string `json:"NetworkBackend"`
	NetworkName    string `json:"NetworkName"`
}

// Sandbox is an observation; KumaBoxd does not become the VM's source of truth.
type Sandbox struct {
	ID          string      `json:"ID"`
	Config      GuestConfig `json:"Config"`
	ImageDigest string      `json:"ImageDigest"`
	VMM         string      `json:"VMM"`
	State       string      `json:"State"`
	Generation  uint64      `json:"Generation"`
}

// CloneOptions selects identity and restore policy; shape is inherited.
// Reseeding is enforced by the runtime; this client exposes no skip option.
type CloneOptions struct {
	Name           string `json:"Name"`
	WaitForNetwork bool   `json:"WaitForNetwork"`
	MemoryMode     string `json:"MemoryMode,omitempty"`
}

// Command is passed directly to the guest without inserting a shell.
type Command struct {
	Args []string          `json:"Args"`
	Env  map[string]string `json:"Env,omitempty"`
	Cwd  string            `json:"Cwd,omitempty"`
	User string            `json:"User,omitempty"`
}

// InspectSnapshot resolves a local name or immutable capture ID.
func (c *Client) InspectSnapshot(ctx context.Context, reference string) (Snapshot, error) {
	var result Snapshot
	err := c.referenced(ctx, "snapshot.inspect", reference, &result, false)
	return result, err
}

// InspectImage resolves the exact digest used by a snapshot.
func (c *Client) InspectImage(ctx context.Context, reference string) (Image, error) {
	var result Image
	err := c.referenced(ctx, "image.inspect", reference, &result, false)
	return result, err
}

// Clone restores a new VM. Errors may accompany an already committed sandbox.
func (c *Client) Clone(ctx context.Context, snapshot string, options CloneOptions) (Sandbox, error) {
	var result Sandbox
	if err := validReference(snapshot); err != nil {
		return result, err
	}
	if err := validReference(options.Name); err != nil {
		return result, fmt.Errorf("clone name: %w", err)
	}
	err := c.call(ctx, "snapshot.clone", target[CloneOptions]{snapshot, options}, &result, nil, nil, true)
	return result, err
}

// InspectSandbox resolves a sandbox after success or an ambiguous mutation.
func (c *Client) InspectSandbox(ctx context.Context, reference string) (Sandbox, error) {
	var result Sandbox
	err := c.referenced(ctx, "sandbox.inspect", reference, &result, false)
	return result, err
}

// ListSandboxes includes stopped and error records when all is true.
func (c *Client) ListSandboxes(ctx context.Context, all bool) ([]Sandbox, error) {
	result := make([]Sandbox, 0)
	err := c.call(ctx, "sandbox.list", all, &result, nil, nil, false)
	return result, err
}

// Exec streams stdout/stderr and returns the guest's numeric exit status.
// Stdin is closed at EOF in this initial adapter. Callers own output writers.
func (c *Client) Exec(ctx context.Context, reference string, command Command, stdout, stderr io.Writer) (int, error) {
	if err := validReference(reference); err != nil {
		return 0, err
	}
	if len(command.Args) == 0 || command.Args[0] == "" {
		return 0, errors.New("command must not be empty")
	}
	var exitCode int
	err := c.call(ctx, "sandbox.exec", target[Command]{reference, command}, &exitCode, stdout, stderr, true)
	return exitCode, err
}

// Stop uses normal runtime shutdown policy unless force is true.
func (c *Client) Stop(ctx context.Context, reference string, force bool) (Sandbox, error) {
	var result Sandbox
	if err := validReference(reference); err != nil {
		return result, err
	}
	type stopOptions struct {
		Force bool `json:"Force"`
	}
	err := c.call(ctx, "sandbox.stop", target[stopOptions]{reference, stopOptions{Force: force}}, &result, nil, nil, true)
	return result, err
}

// Remove deletes a stopped sandbox and its runtime-owned resources.
func (c *Client) Remove(ctx context.Context, reference string) (Sandbox, error) {
	var result Sandbox
	err := c.referenced(ctx, "sandbox.remove", reference, &result, true)
	return result, err
}

// target is only a transport envelope, not an exported generic RPC API.
type target[T any] struct {
	Reference string `json:"Reference"`
	Options   T      `json:"Options"`
}

func (c *Client) referenced(ctx context.Context, method, reference string, result any, mutation bool) error {
	if err := validReference(reference); err != nil {
		return err
	}
	return c.call(ctx, method, struct {
		Reference string `json:"Reference"`
	}{reference}, result, nil, nil, mutation)
}

func validReference(value string) error {
	if strings.TrimSpace(value) == "" || len(value) > 512 || strings.ContainsAny(value, "\x00\r\n") {
		return errors.New("resource reference must contain 1..512 bytes without control characters")
	}
	return nil
}
