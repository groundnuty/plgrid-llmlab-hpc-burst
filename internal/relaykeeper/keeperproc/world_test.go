package keeperproc

// The job side and the control-plane ends, copied from
// internal/hostdial/harness_test.go: one fake cluster behind a rehearsal
// login node (a real sshd), whose srun -u --overlap step runs this binary as
// the agent's "relay <channel>" in a running job whose batch process is this
// binary as the agent's job side; and the targets, the control-plane host's
// loopback ports of both channels (the agent endpoint's and the dial-home
// listener's places), which echo every byte. The keeper's relays dial the
// targets. Every byte a test checks crosses the job's loopback listener, the
// session over the srun step's standard streams, the keeper's ssh and the
// keeper's own end of the session, into a target, and back: four processes
// at least sit between the two ends.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/sshrun"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

const (
	control = agentapi.RelayControl
	serving = agentapi.RelayServing
)

// target is one channel's loopback port on the control-plane host, a test
// listener: it greets each connection with its name and a newline, then
// echoes every byte, and keeps what each connection sent.
type target struct {
	name string
	ln   net.Listener

	mu   sync.Mutex
	recv [][]byte
}

func startTarget(t *testing.T, name string) *target {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tg := &target{name: name, ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go tg.serve(c)
		}
	}()
	return tg
}

func (tg *target) serve(c net.Conn) {
	defer c.Close()
	if _, err := io.WriteString(c, tg.name+"\n"); err != nil {
		return
	}
	tg.mu.Lock()
	i := len(tg.recv)
	tg.recv = append(tg.recv, nil)
	tg.mu.Unlock()
	b := make([]byte, 32<<10)
	for {
		n, err := c.Read(b)
		if n > 0 {
			tg.mu.Lock()
			tg.recv[i] = append(tg.recv[i], b[:n]...)
			tg.mu.Unlock()
			if _, werr := c.Write(b[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (tg *target) port() int { return tg.ln.Addr().(*net.TCPAddr).Port }

// receivedWith is the bytes of the connection whose first bytes are prefix,
// and whether there is one.
func (tg *target) receivedWith(prefix []byte) ([]byte, bool) {
	tg.mu.Lock()
	defer tg.mu.Unlock()
	for _, r := range tg.recv {
		if bytes.HasPrefix(r, prefix) {
			return append([]byte(nil), r...), true
		}
	}
	return nil, false
}

// world is one fake cluster behind a login node, the targets of both
// channels, and, for an alpha relay owner in the test process, the
// controller's transport and relay maker.
type world struct {
	t       *testing.T
	base    string
	plat    *fakeslurm.Platform
	node    *rehearsal.LoginNode
	creds   slurm.SSHConfig
	agent   string // the "agent binary" on the cluster: this test binary, linked into the fake's file system
	staging string // its staging directory, which holds the Allocations' directories
	jobs    *slurm.Batch
	relay   *sshrun.Relay
	targets map[agentapi.RelayChannel]*target
	n       atomic.Int64
}

const clusterUser = "example-user"

// worldSeq numbers this process's worlds, so their job IDs differ.
var worldSeq atomic.Int64

func newWorld(t *testing.T) *world {
	t.Helper()
	rehearsal.RequireTools(t)
	base, err := os.MkdirTemp("", "kp")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	w := &world{t: t, base: base}
	root := filepath.Join(base, "r")
	home := filepath.Join(root, "home", clusterUser)
	for _, d := range []string{home, filepath.Join(base, "c"), filepath.Join(base, "n"), filepath.Join(base, "s")} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	// Job IDs of this world's own: the relay steps are found by SLURM_JOB_ID.
	firstID := 30_000_000 + (os.Getpid()%10_000)*1_000 + int(worldSeq.Add(1))*20
	w.plat = fakeslurm.New(fake.NewClock(time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)), fakeslurm.Config{
		FirstJobID: firstID,
		Run:        &fakeslurm.RunMode{Root: root, JobDir: filepath.Join(base, "j"), KillWait: 2 * time.Second},
	})
	t.Cleanup(func() {
		if err := w.plat.Close(); err != nil {
			t.Errorf("Platform.Close: %v", err)
		}
	})
	w.node, err = rehearsal.StartLoginNode(rehearsal.LoginConfig{
		Dir: filepath.Join(base, "n"), Platform: w.plat,
		// The job sides' sockets are under each job's TMPDIR, in the world's
		// directory; their fallback, for a TMPDIR too long for a socket, is a
		// directory of the world's too, never the host's /dev/shm.
		Login: fakeslurm.Login{User: clusterUser, Env: map[string]string{"HOME": home, "HB_TEST_SHM": filepath.Join(base, "s")}},
	})
	if err != nil {
		t.Fatalf("StartLoginNode: %v", err)
	}
	t.Cleanup(func() {
		if err := w.node.Close(); err != nil {
			t.Errorf("LoginNode.Close: %v", err)
		}
	})
	credsPath := filepath.Join(base, "creds")
	if err := w.node.WriteCredentials(credsPath); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(credsPath)
	if err != nil {
		t.Fatal(err)
	}
	if w.creds, err = slurm.ParseSSHConfig(b, ""); err != nil {
		t.Fatal(err)
	}
	// The relay step's program and the job's batch process are this binary,
	// linked into the cluster's file system as a staged agent is,
	// <stagingDir>/agent/<digest>/hb-agent.
	w.staging = filepath.Join(root, "stage")
	w.agent = filepath.Join(w.staging, agentapi.StagedAgentPath(strings.Repeat("e", 64)))
	if err := os.MkdirAll(filepath.Dir(w.agent), 0o700); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Link(exe, w.agent); err != nil {
		t.Fatalf("link the test binary as the staged agent: %v", err)
	}
	tr, err := sshrun.New(sshrun.Options{Name: "keeperproc", Config: w.creds, ControlDir: filepath.Join(base, "c")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	w.jobs = slurm.NewBatch(tr, provider.Profile{Name: "example", Spec: v1alpha1.PlatformProfileSpec{JSONDataParser: "v0.0.42", StagingDir: root}},
		provider.Settings{Provider: "example"}, slurm.Options{PollInterval: 200 * time.Millisecond, BusyInterval: 200 * time.Millisecond})
	if w.relay, err = sshrun.NewRelay(sshrun.RelayOptions{Name: "keeperproc", Config: w.creds, ControlDir: filepath.Join(base, "c")}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.relay.Close() })
	w.targets = map[agentapi.RelayChannel]*target{control: startTarget(t, "control"), serving: startTarget(t, "serving")}
	t.Cleanup(func() {
		if t.Failed() {
			if b, err := os.ReadFile(filepath.Join(base, "n", "sshd.log")); err == nil {
				t.Logf("sshd's log:\n%s", tail(string(b), 100))
			}
		}
	})
	return w
}

// ports are the channels' target ports.
func (w *world) ports() map[agentapi.RelayChannel]int {
	return map[agentapi.RelayChannel]int{control: w.targets[control].port(), serving: w.targets[serving].port()}
}

// ssh is the Provider's ssh settings as a set carries them: the login node's
// credentials file, every field of it.
func (w *world) ssh() wire.SSH {
	return wire.SSH{HostName: w.creds.HostName, User: w.creds.User, Port: w.creds.Port, IdentityFile: w.creds.IdentityFile, KnownHostsFile: w.creds.UserKnownHostsFile}
}

// job is one running job whose batch process is the agent's job side.
type job struct {
	tag, id  string
	allocDir string // the Allocation's directory, which holds relay-socket
	uid      string // the Allocation's UID
	ports    jobPorts
}

// submit starts a job tagged tag whose job side waits wait for a session, and
// returns once its listeners are bound.
func (w *world) submit(tag string, wait time.Duration) job {
	w.t.Helper()
	portsFile := filepath.Join(w.base, "ports-"+strconv.FormatInt(w.n.Add(1), 10))
	j := job{tag: tag, allocDir: filepath.Join(w.staging, agentapi.AllocationDir(tag)), uid: "0a1b2c3d-0000-4000-8000-" + fmt.Sprintf("%012d", w.n.Add(1))}
	if err := os.MkdirAll(j.allocDir, 0o700); err != nil {
		w.t.Fatal(err)
	}
	line, err := slurm.Line("sbatch", "--parsable", "--job-name="+tag, "--wrap=exec "+w.agent+" "+jobRole+" "+portsFile+" "+wait.String()+" "+j.allocDir+" "+j.uid)
	if err != nil {
		w.t.Fatal(err)
	}
	res, err := w.node.Session().Run(w.t.Context(), slurm.Command{Line: line})
	if err != nil || res.ExitCode != 0 {
		w.t.Fatalf("sbatch: %v, %+v", err, res)
	}
	j.id = strings.TrimSpace(string(res.Stdout))
	waitFor(w.t, time.Minute, "job "+j.id+"'s listeners", func() bool {
		b, err := os.ReadFile(portsFile)
		return err == nil && json.Unmarshal(b, &j.ports) == nil && j.ports.Status != 0
	})
	return j
}

// end ends a running job, as its time limit or a scancel does: the fake kills
// its processes, the relay steps included.
func (w *world) end(j job) {
	w.t.Helper()
	if err := w.plat.End(atoi(w.t, j.id), "CANCELLED"); err != nil {
		w.t.Fatal(err)
	}
}

// status is what the job side shows of each channel.
func (j job) status(t *testing.T) map[agentapi.RelayChannel]jobChannel {
	t.Helper()
	c, err := net.DialTimeout("tcp4", "127.0.0.1:"+strconv.Itoa(j.ports.Status), 10*time.Second)
	if err != nil {
		t.Fatalf("job %s's status listener: %v", j.id, err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	var out map[agentapi.RelayChannel]jobChannel
	if err := json.NewDecoder(c).Decode(&out); err != nil {
		t.Fatalf("job %s's status: %v", j.id, err)
	}
	return out
}

// port is the job's loopback listener of a channel.
func (j job) port(ch agentapi.RelayChannel) int {
	if ch == serving {
		return j.ports.Serving
	}
	return j.ports.Control
}

// target is the relay target of a job for an alpha Owner in the test process.
func (w *world) target(j job) hostdial.Target {
	return hostdial.Target{Tag: j.tag, JobID: j.id, AgentPath: w.agent, AllocDir: j.allocDir, UID: j.uid, Login: w.relay.Login(),
		Jobs: w.jobs, Command: w.relay.Command, Stderr: sshrun.Stderr, Describe: sshrun.Classify, Gen: "1"}
}

// sruns are the srun lines the fake ran for a job ID: the fake Platform lives
// in the test process, so this count is independent of the keeper.
func (w *world) sruns(jobID string) []string {
	var out []string
	for _, c := range w.plat.Calls() {
		if len(c.Words) > 0 && c.Words[0] == "srun" {
			for _, word := range c.Words {
				if word == "--jobid="+jobID {
					out = append(out, strings.Join(c.Words, " "))
				}
			}
		}
	}
	return out
}

// srunsOn counts the srun lines the fake ran for a job's channel.
func (w *world) srunsOn(jobID string, ch agentapi.RelayChannel) int {
	n := 0
	for _, l := range w.sruns(jobID) {
		if strings.Contains(l, " "+agentapi.RelaySubcommand+" "+string(ch)+" ") {
			n++
		}
	}
	return n
}

// prov is a Provider as its sets name it.
type prov struct {
	name, uid string
	created   time.Time
}

// The Providers of the tests: example-0 is the oldest, example-b the newest.
var (
	provA   = prov{"example-a", "0a1b2c3d-0000-4000-a000-00000000000a", time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)}
	provB   = prov{"example-b", "0a1b2c3d-0000-4000-a000-00000000000b", time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)}
	provOld = prov{"example-0", "0a1b2c3d-0000-4000-a000-000000000000", time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)}
)

// The credentials generations of the tests.
const (
	gen1 = "1a2b3c4d5e6f7a8b"
	gen2 = "2b3c4d5e6f7a8b9c"
)

// validFor is a fresh set's validity: under the keeper's 2 min bound, and
// longer than any test runs between two sets of one leader.
const validFor = 110_000

// set is p's set at Lease epoch epoch, fresh (a listing of age 0, valid for
// validFor), on the world's login node, with entries.
func (w *world) set(p prov, epoch int64, complete bool, entries ...wire.Entry) wire.DesiredSet {
	return wire.DesiredSet{Provider: p.name, ProviderUID: p.uid, ProviderCreated: p.created, LeaseEpoch: epoch, Complete: complete,
		ListedAt: time.Now().UTC(), ListingAgeMS: 0, ValidForMS: validFor, SSH: w.ssh(), Allocations: entries}
}

// aged is s resting on a listing 5 ms short of its validity: accepted, it
// expires 5 ms after the keeper receives it. So a leader's last set before an
// outage is expired well before any relay's next start, which hostdial makes
// at least half its minimum backoff (here 100 ms) after a relay ends; the
// expiry is then read from the keeper's refusal, never from a wait.
func aged(s wire.DesiredSet) wire.DesiredSet {
	s.ListingAgeMS = s.ValidForMS - 5
	return s
}

// expired is s resting on a listing as old as its validity: expired on
// arrival, it moves the fence and changes nothing else.
func expired(s wire.DesiredSet) wire.DesiredSet {
	s.ListingAgeMS = s.ValidForMS
	return s
}

// entry is j's Allocation in a set, of credentials generation gen.
func (w *world) entry(j job, gen string) wire.Entry {
	return wire.Entry{UID: j.uid, Tag: j.tag, JobID: j.id, AllocDir: j.allocDir, AgentPath: w.agent, Gen: gen}
}

// endedForGood is e with RelayEndedForGood.
func endedForGood(e wire.Entry) wire.Entry {
	e.EndedForGood = true
	return e
}

// streamSeq numbers this process's streams, so each one's marker differs.
var streamSeq atomic.Int64

// stream is one long-lived connection to a job's channel listener, as frpc's
// or the control client's is, through the relay into the channel's target.
// Every chunk it sends comes back as the target's echo, and the target keeps
// what it got: whole is the check that the target got exactly the bytes sent,
// in order. Its first bytes are a marker line, by which the target's record
// of it is found.
type stream struct {
	t      *testing.T
	j      job
	ch     agentapi.RelayChannel
	tg     *target
	c      net.Conn
	r      *bufio.Reader
	marker []byte
	sent   []byte
	// echoed is how many of the sent bytes came back.
	echoed int
}

// connect opens a connection to the job's channel listener and sends the
// marker; nothing is read yet. A connection accepted while the channel has no
// session waits the job's wait for one.
func (w *world) connect(j job, ch agentapi.RelayChannel) *stream {
	w.t.Helper()
	c, err := net.DialTimeout("tcp4", "127.0.0.1:"+strconv.Itoa(j.port(ch)), 10*time.Second)
	if err != nil {
		w.t.Fatalf("job %s's %s listener: %v", j.id, ch, err)
	}
	w.t.Cleanup(func() { _ = c.Close() })
	s := &stream{t: w.t, j: j, ch: ch, tg: w.targets[ch], c: c, r: bufio.NewReader(c),
		marker: []byte(fmt.Sprintf("stream-%d-%d\n", os.Getpid(), streamSeq.Add(1)))}
	if err := s.write(s.marker); err != nil {
		w.t.Fatalf("job %s's %s stream: the marker: %v", j.id, ch, err)
	}
	return s
}

// openStream is connect, then the greeting of the channel's target and the
// marker's echo: a stream that crosses the relay.
func (w *world) openStream(j job, ch agentapi.RelayChannel) *stream {
	w.t.Helper()
	s := w.connect(j, ch)
	if err := s.greet(45 * time.Second); err != nil {
		w.t.Fatalf("job %s's %s stream: %v", j.id, ch, err)
	}
	return s
}

func (s *stream) write(p []byte) error {
	_ = s.c.SetWriteDeadline(time.Now().Add(30 * time.Second))
	_, err := s.c.Write(p)
	s.sent = append(s.sent, p...)
	return err
}

// greet reads the target's greeting, which names the channel, and the echo
// of everything sent so far, within d.
func (s *stream) greet(d time.Duration) error {
	_ = s.c.SetReadDeadline(time.Now().Add(d))
	g, err := s.r.ReadString('\n')
	if err != nil {
		return fmt.Errorf("no greeting: %w", err)
	}
	if g != string(s.ch)+"\n" {
		return fmt.Errorf("the greeting %q is not the %s target's", g, s.ch)
	}
	return s.readEcho(d)
}

// readEcho reads back every sent byte not yet echoed, within d.
func (s *stream) readEcho(d time.Duration) error {
	_ = s.c.SetReadDeadline(time.Now().Add(d))
	want := s.sent[s.echoed:]
	got := make([]byte, len(want))
	if _, err := io.ReadFull(s.r, got); err != nil {
		return err
	}
	if !bytes.Equal(got, want) {
		return errors.New("the echo differs from the bytes sent")
	}
	s.echoed = len(s.sent)
	return nil
}

// chunk is n bytes that continue the stream's pattern, so a byte changed,
// dropped, added or moved anywhere in the stream shows.
func (s *stream) chunk(n int) []byte {
	b := make([]byte, n)
	off := len(s.sent)
	for i := range b {
		k := off + i
		b[i] = byte(k*7 + k/251)
	}
	return b
}

// echo sends n more bytes and reads them back, within d.
func (s *stream) echo(n int, d time.Duration) error {
	if err := s.write(s.chunk(n)); err != nil {
		return err
	}
	return s.readEcho(d)
}

// mustEcho is echo, failing the test.
func (s *stream) mustEcho(n int, what string) {
	s.t.Helper()
	if err := s.echo(n, 30*time.Second); err != nil {
		s.t.Fatalf("job %s's %s stream %s: %v", s.j.id, s.ch, what, err)
	}
}

// whole checks that the target got exactly the stream's bytes, whole and in
// order, and that all of them came back.
func (s *stream) whole() {
	s.t.Helper()
	got, ok := s.tg.receivedWith(s.marker)
	switch {
	case !ok:
		s.t.Errorf("job %s's %s stream never reached its target", s.j.id, s.ch)
	case s.echoed != len(s.sent):
		s.t.Errorf("job %s's %s stream: %d of its %d bytes came back", s.j.id, s.ch, s.echoed, len(s.sent))
	case !bytes.Equal(got, s.sent):
		s.t.Errorf("job %s's %s stream: the target got %d bytes, not the %d sent, whole and in order", s.j.id, s.ch, len(got), len(s.sent))
	}
}

// cut waits until an echo fails: the stream was cut. It reports whether it
// was, within about 50 echoes; then it checks that what the target got is
// the bytes sent before the cut, in order, every echoed byte among them.
func (s *stream) cut() bool {
	s.t.Helper()
	cut := false
	for range 50 {
		if err := s.echo(1024, 10*time.Second); err != nil {
			cut = true
			break
		}
	}
	got, _ := s.tg.receivedWith(s.marker)
	if len(got) < s.echoed || !bytes.HasPrefix(s.sent, got) {
		s.t.Errorf("job %s's %s stream: the target got %d bytes, not a prefix of those sent with the %d echoed", s.j.id, s.ch, len(got), s.echoed)
	}
	return cut
}

// noSession checks that a new connection to the job's channel gets no byte
// back: it is closed once the job's wait for a session passes.
func (w *world) noSession(j job, ch agentapi.RelayChannel, why string) {
	w.t.Helper()
	s := w.connect(j, ch)
	if err := s.greet(90 * time.Second); err == nil {
		w.t.Errorf("job %s's %s listener crossed to its target %s", j.id, ch, why)
	}
}

// roundTrip opens a stream on a job's channel, echoes n bytes and checks
// them whole.
func (w *world) roundTrip(j job, ch agentapi.RelayChannel, n int) {
	w.t.Helper()
	s := w.openStream(j, ch)
	s.mustEcho(n, "in a round trip")
	s.whole()
	_ = s.c.Close()
}

// waitFor polls cond every 50 ms until it holds or the deadline passes.
func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	end := time.Now().Add(d)
	for !cond() {
		if time.Now().After(end) {
			t.Fatalf("%s: not within %s", what, d)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// testWriter logs to the test.
type testWriter struct{ t *testing.T }

func (tw testWriter) Write(p []byte) (int, error) {
	tw.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// alphaOwner starts an alpha relay owner in the test process, at a fixed
// Lease epoch, as the batch Provider's controller runs one; it is stopped at
// the test's end.
func (w *world) alphaOwner(epoch int64) *hostdial.Owner {
	w.t.Helper()
	own, err := hostdial.NewOwner(hostdial.OwnerOptions{Ports: w.ports(), Slots: hostdial.NewSlots(0),
		Epoch:      func(context.Context) (int64, error) { return epoch, nil },
		MinBackoff: 200 * time.Millisecond, MaxBackoff: 2 * time.Second, StopGrace: 2 * time.Second,
		Logger: slog.New(slog.NewTextHandler(testWriter{w.t}, &slog.HandlerOptions{Level: slog.LevelInfo}))})
	if err != nil {
		w.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := own.Start(ctx); err != nil {
			w.t.Errorf("the alpha relay owner: %v", err)
		}
	}()
	w.t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			w.t.Error("the alpha relay owner did not stop within 30 s")
		}
	})
	return own
}

func atoi(t *testing.T, s string) int {
	t.Helper()
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatalf("%q is not a job ID", s)
	}
	return n
}

// tail is the last n lines of s.
func tail(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = append([]string{fmt.Sprintf("... %d lines before", len(lines)-n)}, lines[len(lines)-n:]...)
	}
	return strings.Join(lines, "\n")
}
