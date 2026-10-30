// Package runtimeclient implements typed KumaBox RPC operations over a reused
// HTTP/2 Unix connection. It never opens runtime databases or retries mutations.
package runtimeclient

import (
	"bufio"
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

const maxFrame = 8 << 20

// Client owns reusable connections. Callers provide per-operation contexts;
// timeout is an additional upper bound, including streamed command output.
type Client struct {
	http      *http.Client
	transport *http.Transport
}

// New validates transport settings without connecting or opening runtime state.
func New(socket string, timeout time.Duration) (*Client, error) {
	if !filepath.IsAbs(socket) || timeout <= 0 {
		return nil, errors.New("absolute runtime socket and positive request timeout are required")
	}
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	transport := &http.Transport{
		Protocols: protocols, IdleConnTimeout: 90 * time.Second,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "unix", socket)
		},
	}
	return &Client{http: &http.Client{Transport: transport, Timeout: timeout}, transport: transport}, nil
}

// Check verifies a terminal node.info response, rather than socket connectivity.
func (c *Client) Check(ctx context.Context) error {
	var result struct {
		Version string `json:"Version"`
	}
	if err := c.call(ctx, "node.info", struct{}{}, &result, nil, nil, false); err != nil {
		return err
	}
	if strings.TrimSpace(result.Version) == "" {
		return errors.New("runtime node.info returned no build version")
	}
	return nil
}

// Close releases idle connections without stopping the runtime or its VMs.
func (c *Client) Close() error {
	c.transport.CloseIdleConnections()
	return nil
}

func (c *Client) call(ctx context.Context, method string, params, result any, stdout, stderr io.Writer, mutation bool) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	deadline, _ := ctx.Deadline()
	metadata, err := json.Marshal(struct {
		Params   any       `json:"params"`
		Deadline time.Time `json:"deadline,omitzero"`
	}{Params: params, Deadline: deadline})
	if err != nil {
		return fmt.Errorf("encode %s: %w", method, err)
	}
	if len(metadata) > 1<<20 {
		return errors.New("runtime request metadata exceeds 1 MiB")
	}
	body := make([]byte, 4+len(metadata))
	binary.BigEndian.PutUint32(body, uint32(len(metadata))) // len is bounded above.
	copy(body[4:], metadata)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://kumabox/rpc/v1/"+method, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/octet-stream")
	response, err := c.http.Do(request)
	if err != nil {
		return interrupted(method, err, mutation)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		err := &HTTPError{Status: response.StatusCode, Operation: method}
		return interrupted(method, err, mutation && response.StatusCode >= 500)
	}
	if response.ProtoMajor != 2 {
		return interrupted(method, errors.New("runtime RPC requires HTTP/2"), mutation)
	}
	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(make([]byte, 64<<10), maxFrame)
	for scanner.Scan() {
		var frame struct {
			Type    string          `json:"type"`
			Data    json.RawMessage `json:"data"`
			Bytes   []byte          `json:"bytes"`
			Failure *RemoteError    `json:"failure"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &frame); err != nil {
			return interrupted(method, fmt.Errorf("decode response: %w", err), mutation)
		}
		switch frame.Type {
		case "stdout", "stderr":
			writer := stdout
			if frame.Type == "stderr" {
				writer = stderr
			}
			if writer != nil {
				if n, err := writer.Write(frame.Bytes); err != nil || n != len(frame.Bytes) {
					if err == nil {
						err = io.ErrShortWrite
					}
					return interrupted(method, err, mutation)
				}
			}
		case "result":
			// Partial results remain useful when the runtime reports a committed error.
			if result != nil && len(frame.Data) != 0 && string(frame.Data) != "null" {
				if err := json.Unmarshal(frame.Data, result); err != nil {
					return interrupted(method, err, mutation)
				}
			}
			if frame.Failure != nil {
				return frame.Failure
			}
			if len(frame.Data) == 0 || string(frame.Data) == "null" {
				return interrupted(method, errors.New("runtime returned an empty result"), mutation)
			}
			return nil
		default:
			// Progress and commit notifications are not terminal acknowledgments.
		}
	}
	err = scanner.Err()
	if err == nil {
		err = io.ErrUnexpectedEOF
	}
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	return interrupted(method, err, mutation)
}
