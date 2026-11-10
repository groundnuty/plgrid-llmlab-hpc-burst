package relaykeeper

// New for issue #343 (PR 3): the Runner's tests' clients and probes
// (realworld_test.go): a job listener's client and its round trip, the
// Runner's relay processes as /proc shows them, and a log recorder.

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// rkWaitFor polls cond every 25 ms until it holds or the deadline passes.
func rkWaitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	end := time.Now().Add(d)
	for !cond() {
		if time.Now().After(end) {
			t.Fatalf("%s: not within %s", what, d)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// rkConn is a client of a job's loopback listener, as frpc or the control
// client is.
type rkConn struct {
	c     net.Conn
	r     *bufio.Reader
	greet string
}

// rkDial connects to a job's channel listener and reads the target's
// greeting, within d; an error means no byte came back (no session, or a
// cut).
func rkDial(j rkJob, ch agentapi.RelayChannel, d time.Duration) (*rkConn, error) {
	port := j.ports.Control
	if ch == agentapi.RelayServing {
		port = j.ports.Serving
	}
	c, err := net.DialTimeout("tcp4", "127.0.0.1:"+strconv.Itoa(port), d)
	if err != nil {
		return nil, err
	}
	_ = c.SetDeadline(time.Now().Add(d))
	r := bufio.NewReader(c)
	g, err := r.ReadString('\n')
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("no greeting: %w", err)
	}
	_ = c.SetDeadline(time.Time{})
	return &rkConn{c: c, r: r, greet: strings.TrimSuffix(g, "\n")}, nil
}

// echo writes p and reads it back, within d.
func (c *rkConn) echo(p []byte, d time.Duration) error {
	_ = c.c.SetDeadline(time.Now().Add(d))
	defer c.c.SetDeadline(time.Time{})
	if _, err := c.c.Write(p); err != nil {
		return err
	}
	got := make([]byte, len(p))
	if _, err := io.ReadFull(c.r, got); err != nil {
		return err
	}
	if string(got) != string(p) {
		return errors.New("the echo differs from what was sent")
	}
	return nil
}

func (c *rkConn) close() { _ = c.c.Close() }

// rkRoundTrip dials a job's channel, checks that the greeting names the
// channel's target, and echoes n bytes of every value.
func rkRoundTrip(t *testing.T, j rkJob, ch agentapi.RelayChannel, n int) {
	t.Helper()
	c, err := rkDial(j, ch, 30*time.Second)
	if err != nil {
		t.Fatalf("job %s's %s listener: %v", j.id, ch, err)
	}
	defer c.close()
	if c.greet != string(ch) {
		t.Fatalf("job %s's %s listener reached the %q target, want %q", j.id, ch, c.greet, ch)
	}
	if err := c.echo(rkPayload(n), 30*time.Second); err != nil {
		t.Fatalf("job %s's %s channel: the echo of %d bytes: %v", j.id, ch, n, err)
	}
}

// rkPayload is n bytes of every value, so a byte that is changed, dropped or
// added in transit shows.
func rkPayload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i * 7)
	}
	return b
}

// rkAlive reports whether pid names a live process (not a zombie).
func rkAlive(pid int) bool {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return false
	}
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	return i > 0 && i+2 < len(s) && s[i+2] != 'Z'
}

// relaySSHs are the live relay ssh clients this test process started for
// the jobs: its children whose command line carries an srun line for one of
// them. It is the Runner's process count, read from /proc, not from the
// Runner.
func relaySSHs(t *testing.T, jobIDs ...string) []int {
	t.Helper()
	ents, err := os.ReadDir("/proc")
	if err != nil {
		t.Fatal(err)
	}
	self := os.Getpid()
	var out []int
	for _, e := range ents {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		b, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		if err != nil {
			continue
		}
		s := string(b)
		i := strings.LastIndexByte(s, ')')
		if i < 0 {
			continue
		}
		f := strings.Fields(s[i+1:])
		if len(f) < 2 || f[0] == "Z" || f[1] != strconv.Itoa(self) {
			continue
		}
		args, err := os.ReadFile("/proc/" + e.Name() + "/cmdline")
		if err != nil {
			continue
		}
		cmd := string(args)
		for _, id := range jobIDs {
			if strings.Contains(cmd, "srun -u --overlap --jobid="+id+" ") {
				out = append(out, pid)
				break
			}
		}
	}
	return out
}

// logRecorder keeps the Runner's log records, for a test to read what the
// relays did (a start's PID and N, an end's exit code), and echoes them to
// the test's log.
type logRecorder struct {
	t  *testing.T
	mu sync.Mutex
	rs []map[string]string
}

func (l *logRecorder) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (l *logRecorder) Handle(_ context.Context, r slog.Record) error {
	m := map[string]string{"msg": r.Message, "level": r.Level.String()}
	r.Attrs(func(a slog.Attr) bool { m[a.Key] = a.Value.String(); return true })
	l.mu.Lock()
	l.rs = append(l.rs, m)
	l.mu.Unlock()
	return nil
}

func (l *logRecorder) WithAttrs(as []slog.Attr) slog.Handler { return &attrHandler{l: l, as: as} }
func (l *logRecorder) WithGroup(string) slog.Handler         { return l }

// records are the records with msg, each with its attributes.
func (l *logRecorder) records(msg string) []map[string]string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []map[string]string
	for _, r := range l.rs {
		if r["msg"] == msg {
			out = append(out, r)
		}
	}
	return out
}

// dump logs every record to the test's log.
func (l *logRecorder) dump() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, r := range l.rs {
		l.t.Logf("%v", r)
	}
}

// attrHandler is the recorder with a logger's attributes added to each
// record.
type attrHandler struct {
	l  *logRecorder
	as []slog.Attr
}

func (h *attrHandler) Enabled(ctx context.Context, lv slog.Level) bool { return true }
func (h *attrHandler) Handle(ctx context.Context, r slog.Record) error {
	r = r.Clone()
	r.AddAttrs(h.as...)
	return h.l.Handle(ctx, r)
}
func (h *attrHandler) WithAttrs(as []slog.Attr) slog.Handler {
	return &attrHandler{l: h.l, as: append(append([]slog.Attr{}, h.as...), as...)}
}
func (h *attrHandler) WithGroup(string) slog.Handler { return h }
