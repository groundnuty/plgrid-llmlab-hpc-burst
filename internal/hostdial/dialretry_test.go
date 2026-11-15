package hostdial_test

// A test's dial of a job's channel listener must tolerate the relay being replaced (issue #590). A dial made while a relay has a new ssh and no
// session yet, or while the session it replaces ends, gets a connection closed with no greeting: the job side closes a connection whose session
// ended or was replaced under it, or one that found no session within its wait. TestANewGenRestartsTheLiveChannelAndKeepsASeededFinalOneDown
// dialled once after a new generation's relay had a PID, and failed once on CI with "no greeting: EOF" (its job side held the dial's stream until
// the 10 s keepalive and 10 s write timeout ended the session, 20.2 s). The channel's real clients dial again (doc.go), so the helper does,
// until the greeting comes or a bound passes, and then fails naming what it saw.

import (
	"io"
	"math"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// greetAfter is a loopback listener standing for a job's control listener: it closes its first closed connections with no greeting, as a job
// side does to a dial that lands on a session being replaced, and greets every one after them. It returns the ports a job would hold and the
// number of connections accepted.
func greetAfter(t *testing.T, closed int, greeting string) (jobPorts, *atomic.Int64) {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	var accepted atomic.Int64
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			if accepted.Add(1) <= int64(closed) {
				_ = c.Close()
				continue
			}
			_, _ = io.WriteString(c, greeting+"\n")
			go func() { _, _ = io.Copy(io.Discard, c); _ = c.Close() }()
		}
	}()
	return jobPorts{Control: ln.Addr().(*net.TCPAddr).Port}, &accepted
}

// The greeting that comes after connections closed with none is taken, and each dial is counted.
func TestDialUntilGreetingTakesTheGreetingThatComesAfterClosedConnections(t *testing.T) {
	ports, accepted := greetAfter(t, 2, "control")
	var attempts atomic.Int64
	c, err := dialUntilGreeting(job{id: "1", ports: ports}, control, time.Minute, &attempts)
	if err != nil {
		t.Fatalf("two closed connections, then a greeting: %v", err)
	}
	defer c.close()
	if c.greet != "control" || attempts.Load() != 3 || accepted.Load() != 3 {
		t.Errorf("greeting %q after %d dials (%d connections accepted), want %q after 3 and 3", c.greet, attempts.Load(), accepted.Load(), "control")
	}
}

// A listener that never greets fails the dial at its bound, with the number of dials made and the last one's error: a channel that never comes
// up is still a failure, and says why.
func TestDialUntilGreetingFailsNamingTheDialsAndTheLastErrorWhenNoGreetingComes(t *testing.T) {
	ports, _ := greetAfter(t, math.MaxInt, "control")
	var attempts atomic.Int64
	start := time.Now()
	c, err := dialUntilGreeting(job{id: "1", ports: ports}, control, time.Second, &attempts)
	if err == nil {
		c.close()
		t.Fatal("a listener that closes every connection gave a greeting")
	}
	if took := time.Since(start); took < time.Second {
		t.Errorf("it gave up after %s, before its bound of 1 s", took)
	}
	n := attempts.Load()
	if n < 2 {
		t.Errorf("it made %d dials, want it to dial again after the first closed", n)
	}
	for _, want := range []string{"no greeting within 1s over ", " dials, the last: no greeting: EOF"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error %q does not say %q", err, want)
		}
	}
}

// The failing run's dial, made deterministic: a new generation's relay may not start (Target.BeforeStart), and the first generation's has ended,
// so the job has no relay up, as it has between a relay's stop and its replacement's session. A single dial gets what CI's run got, a connection
// closed with no greeting; the job side's wait is 2 s here, so that it does within seconds (when the old session was still held it ends with that
// session instead). A dial that dials again takes the greeting of the generation 2 relay once the test lets it start, whatever number of dials that
// took, and the channel carries bytes.
func TestAControlDialAcrossAGenRestartIsRetriedUntilTheNewRelayGreets(t *testing.T) {
	w := newWorld(t)
	j := w.submit("hb-relay-drt-0123456789ab", 2*time.Second)
	own, _, _ := w.owner(hostdial.OwnerOptions{})
	tg := w.target(j)
	own.Want(tg)
	waitFor(t, time.Minute, "the control relay up", func() bool { return relayOf(own, j.tag, control).PID != 0 })
	first := relayOf(own, j.tag, control)
	roundTrip(t, j, control, "control", payload(1000))
	gate := make(chan struct{})
	tg.Gen = "2"
	tg.BeforeStart = func(agentapi.RelayChannel, int64, int64) bool {
		select {
		case <-gate:
			return true
		default:
			return false
		}
	}
	own.Want(tg)
	waitFor(t, time.Minute, "the first generation's relay gone", func() bool { return !alive(first.PID) })
	if c, err := dial(j, control, 30*time.Second); err == nil {
		c.close()
		t.Fatalf("a dial with no relay of generation 2 up got the greeting %q", c.greet)
	} else if !strings.Contains(err.Error(), "no greeting: EOF") {
		t.Fatalf("a single dial with no relay up ended with %v, want the failing run's \"no greeting: EOF\"", err)
	}
	var attempts atomic.Int64
	type result struct {
		c   *conn
		err error
	}
	done := make(chan result, 1)
	go func() {
		c, err := dialUntilGreeting(j, control, time.Minute, &attempts)
		done <- result{c, err}
	}()
	waitFor(t, time.Minute, "a dial to end with no greeting and the next to begin", func() bool { return attempts.Load() >= 2 })
	close(gate)
	r := <-done
	if r.err != nil {
		t.Fatalf("a dial that dials again, with the relay of generation 2 let go: %v", r.err)
	}
	defer r.c.close()
	if r.c.greet != "control" {
		t.Errorf("the greeting is %q, want %q", r.c.greet, "control")
	}
	if err := r.c.echo(payload(1000), 30*time.Second); err != nil {
		t.Errorf("the channel does not carry bytes after the dials: %v", err)
	}
	if got := relayOf(own, j.tag, control); got.Gen != "2" || got.PID == 0 || got.PID == first.PID {
		t.Errorf("the control relay is %+v, want a new process on generation 2", got)
	}
}
