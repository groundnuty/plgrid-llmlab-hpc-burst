package main

// The tests of slurmwire_test.go's transport, over real loopback sockets and
// the in-process fake Slurm. Expected values are what the test hands the fake
// (the fault it injects, the environment it logs in with, the bytes it
// writes), never what the transport computes.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

// allWireKinds are the errors the wire is meant to keep apart.
var allWireKinds = []error{slurm.ErrNotStarted, slurm.ErrTimeout, slurm.ErrTransport, context.Canceled, context.DeadlineExceeded}

// matches names which of allWireKinds err matches.
func matches(err error) []string {
	var out []string
	for _, k := range allWireKinds {
		if errors.Is(err, k) {
			out = append(out, k.Error())
		}
	}
	return out
}

// notify sends one value on ch and never blocks, so a server's handler is never held up by a
// test that has stopped listening.
func notify(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// The command's time-out test runs a call with a short time-out against a server that holds the
// command. The time-out covers the dial and the send as well as the wait, so under load it can
// pass before the server has the command; then the call ends (ErrNotStarted when the dial was
// cut, ErrTimeout otherwise) with nothing held, and the premise of the test, a command running
// past its time-out, did not happen. The test says so and tries again with a longer time-out:
// it does not count that as a pass, since no command ran past its time-out, and it does not wait
// for an arrival that will not come. The receive of the arrival is bounded by heldGrace,
// far above what a loopback server needs to see a request that was sent.
var (
	timeoutAttempts = []time.Duration{300 * time.Millisecond, 2 * time.Second, 10 * time.Second}
	heldGrace       = 10 * time.Second
)

// runPastItsTimeout calls Run with each time-out in turn until the server reports the command's
// arrival on held within grace of the call's return. It returns that call's error and true, or
// the last attempt's error and false when no attempt was delivered.
func runPastItsTimeout(r *remoteRunner, held <-chan struct{}, attempts []time.Duration, grace time.Duration) (error, bool) {
	var err error
	for _, timeout := range attempts {
		_, err = r.Run(context.Background(), slurm.Command{Line: "printenv HOME", Timeout: timeout})
		select {
		case <-held:
			return err, true
		case <-time.After(grace):
		}
	}
	return err, false
}

func TestWireErrorsKeepTheirIdentity(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want []error // the kinds the original matches, stated here
	}{
		{"not started", slurm.ErrNotStarted, []error{slurm.ErrNotStarted}},
		{"timeout", slurm.ErrTimeout, []error{slurm.ErrTimeout}},
		{"transport", slurm.ErrTransport, []error{slurm.ErrTransport}},
		{"canceled", context.Canceled, []error{context.Canceled}},
		{"deadline", context.DeadlineExceeded, []error{context.DeadlineExceeded}},
		{"wrapped not started", fmt.Errorf("sbatch: %w: no route", slurm.ErrNotStarted), []error{slurm.ErrNotStarted}},
		{"wrapped timeout", fmt.Errorf("sbatch: %w after 1m0s", slurm.ErrTimeout), []error{slurm.ErrTimeout}},
		{"wrapped transport", fmt.Errorf("sbatch: %w", slurm.ErrTransport), []error{slurm.ErrTransport}},
		{"wrapped canceled", fmt.Errorf("run: %w", context.Canceled), []error{context.Canceled}},
		{"wrapped deadline", fmt.Errorf("run: %w", context.DeadlineExceeded), []error{context.DeadlineExceeded}},
		{"two kinds", errors.Join(slurm.ErrTransport, context.Canceled), []error{slurm.ErrTransport, context.Canceled}},
		{"no kind", errors.New("scancel: exit 1"), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The expectation is the table's, so check the original first.
			for _, k := range allWireKinds {
				want := false
				for _, w := range tc.want {
					want = want || w == k
				}
				if errors.Is(tc.err, k) != want {
					t.Fatalf("the case is wrong: errors.Is(original, %v) = %t, want %t", k, !want, want)
				}
			}
			kinds, msg := encodeError(tc.err)
			got := decodeError(kinds, msg)
			for _, k := range allWireKinds {
				want := false
				for _, w := range tc.want {
					want = want || w == k
				}
				if errors.Is(got, k) != want {
					t.Errorf("after the wire errors.Is(decoded, %q) = %t, want %t (codes %v)", k, !want, want, kinds)
				}
			}
			if got.Error() != tc.err.Error() {
				t.Errorf("decoded text %q, want %q", got.Error(), tc.err.Error())
			}
		})
	}
	t.Run("an unknown code is named, not dropped", func(t *testing.T) {
		got := decodeError([]string{"from-the-future"}, "boom")
		if !strings.Contains(got.Error(), `"from-the-future"`) || len(matches(got)) != 0 {
			t.Errorf("decoded %q matching %v, want the code named in the text and no kind", got, matches(got))
		}
	})
}

// A server over a fake Slurm session, and a client of it.
func TestRemoteRunnerOverTheFakeSlurm(t *testing.T) {
	sl, session := newFakeSlurm()
	srv := serveRunner(t, session, runnerHooks{})
	r := &remoteRunner{addr: srv.Addr()}
	ctx := context.Background()

	t.Run("a command's output and exit status", func(t *testing.T) {
		res, err := r.Run(ctx, slurm.Command{Line: "printenv HOME"})
		if err != nil || res.ExitCode != 0 || string(res.Stdout) != "/home/example-user\n" {
			t.Errorf("printenv HOME = %q, exit %d, %v; want the login's HOME, exit 0", res.Stdout, res.ExitCode, err)
		}
		res, err = r.Run(ctx, slurm.Command{Line: "printenv NOT_SET_ON_THE_LOGIN"})
		if err != nil || res.ExitCode != 1 {
			t.Errorf("printenv of an unset variable: exit %d, %v; want exit 1 and no error", res.ExitCode, err)
		}
	})

	t.Run("standard input arrives intact", func(t *testing.T) {
		content := []byte("example credential\x00with a NUL and a newline\n")
		dir := "/scratch/example-user/wire"
		if res, err := r.Run(ctx, slurm.Command{Line: "umask 077 && mkdir -p " + dir + " && cat > " + dir + "/f", Stdin: content}); err != nil || res.ExitCode != 0 {
			t.Fatalf("write: exit %d, %v", res.ExitCode, err)
		}
		sum := sha256.Sum256(content)
		res, err := r.Run(ctx, slurm.Command{Line: "sha256sum " + dir + "/f"})
		if err != nil || !strings.HasPrefix(string(res.Stdout), hex.EncodeToString(sum[:])) {
			t.Errorf("sha256sum = %q, %v; want the digest of the bytes written", res.Stdout, err)
		}
	})

	t.Run("a fault's error keeps its kind", func(t *testing.T) {
		for _, k := range []error{slurm.ErrNotStarted, slurm.ErrTimeout, slurm.ErrTransport} {
			sl.FailNext(1, k)
			_, err := r.Run(ctx, slurm.Command{Line: "printenv HOME"})
			if got := matches(err); len(got) != 1 || !errors.Is(err, k) {
				t.Errorf("a fault of %v came back as %v (matching %v), want it alone", k, err, got)
			}
		}
	})

	t.Run("the generation follows the session", func(t *testing.T) {
		want := session.Generation()
		if want == 0 {
			t.Fatal("the fake's generation is 0")
		}
		if got := r.Generation(); got != want {
			t.Errorf("Generation = %d, want %d", got, want)
		}
		session.Reconnect()
		if got := r.Generation(); got != want+1 {
			t.Errorf("Generation after a reconnect = %d, want %d", got, want+1)
		}
	})
}

// The client's own errors: what a dropped connection and a connection never
// made are, by runner.go's contract.
func TestRemoteRunnerTransportErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("no connection: never started", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := ln.Addr().String()
		ln.Close() // nothing listens there now
		_, err = (&remoteRunner{addr: addr}).Run(ctx, slurm.Command{Line: "printenv HOME"})
		if !errors.Is(err, slurm.ErrNotStarted) || errors.Is(err, slurm.ErrTransport) {
			t.Errorf("Run against a closed port = %v, want ErrNotStarted alone", err)
		}
	})

	t.Run("a connection that ends before a reply: it may have run", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()
		go func() {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			buf := make([]byte, 1)
			_, _ = conn.Read(buf) // the request has begun to arrive
			conn.Close()
		}()
		_, err = (&remoteRunner{addr: ln.Addr().String()}).Run(ctx, slurm.Command{Line: "printenv HOME"})
		if !errors.Is(err, slurm.ErrTransport) || errors.Is(err, slurm.ErrNotStarted) {
			t.Errorf("Run on a dropped connection = %v, want ErrTransport and not ErrNotStarted", err)
		}
	})

	// The server holds every command and never replies; the only ways out are
	// the caller's context and the command's own time-out.
	_, session := newFakeSlurm()
	held := make(chan struct{}, 4)
	srv := serveRunner(t, session, runnerHooks{before: func(slurm.Command) bool { notify(held); return true }})
	r := &remoteRunner{addr: srv.Addr()}

	t.Run("the caller's context ends while the command runs", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		defer cancel()
		go func() {
			select {
			case <-held: // the server has the command
				cancel()
			case <-cctx.Done(): // the test is over: do not wait for a command that never came
			}
		}()
		_, err := r.Run(cctx, slurm.Command{Line: "printenv HOME"})
		if !errors.Is(err, slurm.ErrTransport) || errors.Is(err, slurm.ErrNotStarted) {
			t.Errorf("Run with its context cancelled = %v, want ErrTransport and not ErrNotStarted", err)
		}
	})

	t.Run("the command's time-out", func(t *testing.T) {
		err, delivered := runPastItsTimeout(r, held, timeoutAttempts, heldGrace)
		if !delivered {
			t.Fatalf("the command never reached the server, in attempts of %v each followed by %s of waiting (the last call ended %v)", timeoutAttempts, heldGrace, err)
		}
		if !errors.Is(err, slurm.ErrTimeout) || errors.Is(err, slurm.ErrTransport) {
			t.Errorf("Run past its time-out = %v, want ErrTimeout alone", err)
		}
	})

	t.Run("a call whose deadline passed before the command arrived is tried again, not waited on", func(t *testing.T) {
		// The server reports no arrival for the first command it gets (as if the
		// deadline had cut the call before the command got there) and reports the
		// next. The first command to arrive is the first attempt's, or the second's
		// when the first attempt was cut in the dial, so there are three attempts and
		// the second or the third is the one delivered.
		_, session := newFakeSlurm()
		arrived := make(chan struct{}, 4)
		var mu sync.Mutex
		seen := 0
		srv := serveRunner(t, session, runnerHooks{before: func(slurm.Command) bool {
			mu.Lock()
			seen++
			first := seen == 1
			mu.Unlock()
			if !first {
				notify(arrived)
			}
			return true
		}})
		started := time.Now()
		err, delivered := runPastItsTimeout(&remoteRunner{addr: srv.Addr()}, arrived, []time.Duration{100 * time.Millisecond, 400 * time.Millisecond, 2 * time.Second}, 200*time.Millisecond)
		if !delivered || !errors.Is(err, slurm.ErrTimeout) || errors.Is(err, slurm.ErrTransport) {
			t.Errorf("after a first attempt with no arrival: delivered %t, %v; want a later attempt delivered and ErrTimeout alone", delivered, err)
		}
		if took := time.Since(started); took > 30*time.Second {
			t.Errorf("took %s: the receive of the arrival was not bounded by the grace", took)
		}
		// With no arrival at all it ends, bounded, saying so.
		_, session = newFakeSlurm()
		silent := serveRunner(t, session, runnerHooks{before: func(slurm.Command) bool { return true }})
		if _, delivered := runPastItsTimeout(&remoteRunner{addr: silent.Addr()}, make(chan struct{}), []time.Duration{50 * time.Millisecond, 100 * time.Millisecond}, 100*time.Millisecond); delivered {
			t.Error("a server that reports no arrival was reported as delivered")
		}
	})

	t.Run("the server ends the withheld commands when their clients are gone", func(t *testing.T) {
		done := make(chan struct{})
		go func() { srv.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("Close did not return: a handler is still withholding a reply for a client that left")
		}
	})
}
