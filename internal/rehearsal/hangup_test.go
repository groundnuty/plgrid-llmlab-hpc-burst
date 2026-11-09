package rehearsal

// New. watchHangup (hangup.go) on a plain unix socket pair: the part of the
// stream server's hang-up detection that needs no ssh. The end-to-end claim,
// that a step ends when the client dies while its input is backed up, is
// TestSrunStepEndsWhenTheSSHClientIsKilledUnderInputBackpressure. Every wait
// is on a channel with a generous deadline.

import (
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// socketPair returns the two ends of a unix stream connection: the server's
// (accepted) and the client's (dialled).
func socketPair(t *testing.T) (server, client *net.UnixConn) {
	t.Helper()
	dir, err := os.MkdirTemp("", "hu")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) }) // this test's own directory
	ln, err := net.Listen("unix", filepath.Join(dir, "s"))
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- c
	}()
	cc, err := net.Dial("unix", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	sc, ok := <-accepted
	if !ok {
		t.Fatal("the listener accepted nothing")
	}
	t.Cleanup(func() { _ = sc.Close(); _ = cc.Close() })
	return sc.(*net.UnixConn), cc.(*net.UnixConn)
}

// A hang-up is seen while data the peer sent waits unread, which is the state
// of a frame reader that cannot queue the next frame: the end of file sits
// behind the data, and a reader of the connection would never reach it. Both
// ways of hanging up count: closing the connection, and shutting down its
// writing side.
func TestWatchHangupSeesAHangUpBehindUnreadData(t *testing.T) {
	for _, tc := range []struct {
		name string
		hang func(client *net.UnixConn)
	}{
		{"close", func(c *net.UnixConn) { _ = c.Close() }},
		{"shutdown of writing", func(c *net.UnixConn) { _ = c.CloseWrite() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, client := socketPair(t)
			fired := make(chan struct{})
			stop, err := watchHangup(server, func() { close(fired) })
			if err != nil {
				t.Fatal(err)
			}
			defer stop()
			if _, err := client.Write(make([]byte, 4096)); err != nil { // unread on the server's side
				t.Fatal(err)
			}
			tc.hang(client)
			await(t, "the hang-up to be seen", fired)
			// The watch read nothing: the data is all still there.
			buf := make([]byte, 4096)
			if _, err := io.ReadFull(server, buf); err != nil {
				t.Errorf("reading the data the peer sent before it hung up: %v", err)
			}
		})
	}
}

// Closing the connection while it is watched does not wait for the watch (a
// watch that held the descriptor would make Close wait for it, and the relay
// server's close holds its lock across the Close of every connection), and the
// socket stays open until stop: the peer sees the end of file only then.
func TestWatchHangupDoesNotBlockCloseAndStopFreesTheSocket(t *testing.T) {
	server, client := socketPair(t)
	stop, err := watchHangup(server, func() { t.Error("a hang-up was reported, but the peer is still there") })
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	go func() { _ = server.Close(); close(closed) }()
	await(t, "Close of the watched connection to return", closed)
	stop()
	stop() // twice is allowed
	eof := make(chan error, 1)
	go func() { _, err := client.Read(make([]byte, 1)); eof <- err }()
	select {
	case err := <-eof:
		if err != io.EOF {
			t.Errorf("the peer's read after stop: %v, want io.EOF", err)
		}
	case <-time.After(waitDeadline):
		t.Fatalf("the peer saw no end of file within %s after stop", waitDeadline)
	}
}
