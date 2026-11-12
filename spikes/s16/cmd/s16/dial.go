package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

// The tunnel dial, copied from spikes/s14/cmd/s14/dial.go; it follows spike S1c's s1cbench
// (spikes/s1c/cmd/s1cbench/dial.go), with the target chosen per request: the
// request's host is the Instance's UID, which the client sends frps in its
// CONNECT, as the front's transport does (HBEP-6, The front's half).

// errRefused is a definite "no" from the path: frps answered the CONNECT with
// a non-200 status (no route to that Instance).
type errRefused struct{ status string }

func (e *errRefused) Error() string { return "refused: " + e.status }

// dialTunnel opens a TCP connection to frps's tcpmux port and returns once
// frps answered 200 to "CONNECT <uid>:80".
func dialTunnel(ctx context.Context, mux, uid string) (net.Conn, error) {
	var d net.Dialer
	c, err := d.DialContext(ctx, "tcp4", mux)
	if err != nil {
		return nil, err
	}
	_ = c.(*net.TCPConn).SetNoDelay(true)
	if dl, ok := ctx.Deadline(); ok {
		_ = c.SetDeadline(dl)
	}
	if _, err := fmt.Fprintf(c, "CONNECT %s:80 HTTP/1.1\r\nHost: %s:80\r\n\r\n", uid, uid); err != nil {
		c.Close()
		return nil, err
	}
	status, err := readHeaderBlock(c)
	_ = c.SetDeadline(time.Time{})
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("CONNECT: %w", err)
	}
	if !strings.HasPrefix(status, "HTTP/1.1 200") && !strings.HasPrefix(status, "HTTP/1.0 200") {
		c.Close()
		return nil, &errRefused{status: status}
	}
	return c, nil
}

// readHeaderBlock reads one byte at a time up to the blank line that ends an
// HTTP header block, so nothing after it is consumed, and returns the status
// line.
func readHeaderBlock(r io.Reader) (string, error) {
	var buf []byte
	one := make([]byte, 1)
	for len(buf) < 8192 {
		if _, err := io.ReadFull(r, one); err != nil {
			return "", err
		}
		buf = append(buf, one[0])
		if len(buf) >= 4 && string(buf[len(buf)-4:]) == "\r\n\r\n" {
			line, _, _ := strings.Cut(string(buf), "\r\n")
			return line, nil
		}
	}
	return "", errors.New("header block over 8 KiB")
}
