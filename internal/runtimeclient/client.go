// Package runtimeclient connects to the KumaBox local RPC v1 endpoint.
// It owns transport connections only; VM state remains owned by the runtime.
package runtimeclient

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"time"
)

const maxProbeResponse = 64 << 10

// Client reuses HTTP/2 connections to one local runtime daemon.
type Client struct {
	http      *http.Client
	transport *http.Transport
}

// New validates the endpoint without opening runtime files or connecting yet.
func New(socket string) (*Client, error) {
	if !filepath.IsAbs(socket) {
		return nil, errors.New("runtime socket must be an absolute path")
	}
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	transport := &http.Transport{
		Protocols:       protocols,
		IdleConnTimeout: 90 * time.Second,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "unix", socket)
		},
	}
	return &Client{
		http:      &http.Client{Transport: transport, Timeout: 5 * time.Second},
		transport: transport,
	}, nil
}

// Check verifies a terminal node.info response, not just socket connectivity.
// This read-only adapter deliberately does not expose a generic mutation API.
func (c *Client) Check(ctx context.Context) error {
	deadline, _ := ctx.Deadline()
	metadata, err := json.Marshal(struct {
		Params   struct{}  `json:"params"`
		Deadline time.Time `json:"deadline,omitzero"`
	}{Deadline: deadline})
	if err != nil {
		return fmt.Errorf("encode node probe: %w", err)
	}
	// RPC v1 starts with a four-byte, big-endian metadata length.
	// The envelope has fixed fields and is always far below the 1 MiB limit.
	body := make([]byte, 4+len(metadata))
	binary.BigEndian.PutUint32(body, uint32(len(metadata)))
	copy(body[4:], metadata)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://kumabox/rpc/v1/node.info", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create node probe: %w", err)
	}
	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("contact KumaBox runtime: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("runtime node.info: HTTP %d", response.StatusCode)
	}
	if response.ProtoMajor != 2 {
		return errors.New("runtime node.info requires HTTP/2")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxProbeResponse+1))
	if err != nil {
		return fmt.Errorf("read node probe: %w", err)
	}
	if len(data) > maxProbeResponse {
		return errors.New("runtime node.info response exceeds 64 KiB")
	}
	return validateProbe(data)
}

func validateProbe(data []byte) error {
	var frame struct {
		Type string `json:"type"`
		Data struct {
			Version string `json:"version"`
		} `json:"data"`
		Failure *struct {
			Message string `json:"message"`
		} `json:"failure"`
	}
	if err := json.Unmarshal(data, &frame); err != nil {
		return fmt.Errorf("decode node probe: %w", err)
	}
	if frame.Type != "result" {
		return errors.New("runtime node.info did not return a terminal result")
	}
	if frame.Failure != nil {
		return fmt.Errorf("runtime node.info failed: %s", frame.Failure.Message)
	}
	if strings.TrimSpace(frame.Data.Version) == "" {
		return errors.New("runtime node.info returned no build version")
	}
	return nil
}

// Close releases idle connections. It never stops the runtime or its VMs.
func (c *Client) Close() error {
	c.transport.CloseIdleConnections()
	return nil
}
