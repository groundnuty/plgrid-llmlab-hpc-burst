package conformance

// The reach-host-dialed rows' rules, each on its own: the verdict from what a run saw (judgeHostDialed), the probe's
// report (parseProbe and the script itself, against a responder on this machine), what the text to post may name of a
// path, and the bounds of the waits. The expected values are the issue's and HBEP-6's, written out here, never made by
// the code under test.

import (
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/sshrun"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// ascending is the 256 byte values in order, the node's half of the exchange, written out with a loop of its own.
func ascending() []byte {
	b := make([]byte, 0, 256)
	for i := 0; i <= 255; i++ {
		b = append(b, byte(i))
	}
	return b
}

const goodAgent = "/scratch/stage/agent/" + "0123abcd" + "/hb-agent"

func goodLine(ch string, epoch, seq int) string {
	return "srun -u --overlap --jobid=4242 --ntasks=1 " + goodAgent + " relay " + ch + " --dir /scratch/stage/alloc/hb-x --allocation u-1 --epoch " +
		strconv.Itoa(epoch) + " --seq " + strconv.Itoa(seq)
}

// goodObs is a run in which everything held.
func goodObs() hostDialedObs {
	row, _ := alpha.RowByName("reach-host-dialed-athena")
	return hostDialedObs{
		Row: row, Epoch: 7, JobID: "4242", UID: "u-1", AgentPath: goodAgent,
		Lines: []string{goodLine("control", 7, 1), goodLine("serving", 7, 2)},
		Relays: map[agentapi.RelayChannel]hostdial.RelayState{
			agentapi.RelayControl: {Channel: agentapi.RelayControl, PID: 100, Starts: 1, Sessions: 1, Streams: 3, Joined: 3},
			agentapi.RelayServing: {Channel: agentapi.RelayServing, PID: 101, Starts: 1, Sessions: 1, Streams: 1, Joined: 1},
		},
		Registered: 1,
		Probe: &probeReport{TmpDirSet: true, TmpDir: "/tmp/job", Socket: "/tmp/job/hb-relay-123/relay.sock", SocketDirMode: "700",
			Listening: 1, PortFound: true, Connected: true, GotBytes: 256, GotSHA256: "cd6816b77f68d70001fc3eaa4d42bdd67cb5973b3151cc5292ecc02a3daac6ab"},
		Exchanges: []servingConn{{Received: ascending(), InPlace: true, Wrote: 256, Closed: true}},
		Stale:     staleResult{Ran: true, Exit: 5, Classify: "hb-relay: superseded"},
		KeptPID:   100, KeptStarts: 1, Stopped: true,
	}
}

func failedOf(o hostDialedObs) []string {
	f := failedChecks(judgeHostDialed(o))
	slices.Sort(f)
	return f
}

func TestJudgeHostDialedPassesARunInWhichEverythingHeld(t *testing.T) {
	checks := judgeHostDialed(goodObs())
	if f := failedChecks(checks); len(f) != 0 {
		t.Fatalf("a good run failed %v", f)
	}
	// One line per check the issue names, in the post's order.
	var names []string
	for _, c := range checks {
		names = append(names, c.Name)
	}
	want := []string{checkControl, checkServing, checkSrunU, checkRendezvous, checkPreamble, checkEpoch, checkBytes}
	if !slices.Equal(names, want) {
		t.Errorf("the checks are %q, want %q", names, want)
	}
}

// Each rule on its own: a run that differs from a good one in one fact fails exactly the checks that fact is part of.
func TestJudgeHostDialedFailsEachCheckOnItsOwnFact(t *testing.T) {
	desc := strings.Repeat("0", 64)
	for _, c := range []struct {
		name string
		mod  func(o *hostDialedObs)
		want []string
	}{
		{"no registration", func(o *hostDialedObs) { o.Registered = 0 }, []string{checkControl}},
		{"no registration and the job ended", func(o *hostDialedObs) { o.Registered, o.JobEnded = 0, true }, []string{checkControl}},
		{"not stopped when told", func(o *hostDialedObs) { o.Stopped = false }, []string{checkControl}},
		{"the serving session never joined a stream", func(o *hostDialedObs) {
			s := o.Relays[agentapi.RelayServing]
			s.Joined = 0
			o.Relays[agentapi.RelayServing] = s
		}, []string{checkServing}},
		{"no serving session", func(o *hostDialedObs) {
			s := o.Relays[agentapi.RelayServing]
			s.Sessions = 0
			o.Relays[agentapi.RelayServing] = s
		}, []string{checkPreamble, checkServing}},
		{"no control session", func(o *hostDialedObs) {
			s := o.Relays[agentapi.RelayControl]
			s.Sessions = 0
			o.Relays[agentapi.RelayControl] = s
		}, []string{checkPreamble}},
		{"no exchange reached the serving port", func(o *hostDialedObs) { o.Exchanges = nil }, []string{checkBytes, checkServing}},
		{"a line without -u", func(o *hostDialedObs) { o.Lines[1] = strings.Replace(o.Lines[1], "srun -u ", "srun ", 1) }, []string{checkSrunU}},
		{"a line without --overlap", func(o *hostDialedObs) { o.Lines[0] = strings.Replace(o.Lines[0], " --overlap", "", 1) }, []string{checkSrunU}},
		{"a line on another job", func(o *hostDialedObs) { o.Lines[0] = strings.Replace(o.Lines[0], "--jobid=4242", "--jobid=4243", 1) }, []string{checkSrunU}},
		{"a line of another agent", func(o *hostDialedObs) { o.Lines[0] = strings.Replace(o.Lines[0], goodAgent, "/tmp/hb-agent", 1) }, []string{checkSrunU}},
		{"no serving line", func(o *hostDialedObs) { o.Lines = o.Lines[:1] }, []string{checkSrunU}},
		{"no line at all", func(o *hostDialedObs) { o.Lines = nil }, []string{checkSrunU}},
		{"the socket's directory is mode 755", func(o *hostDialedObs) { o.Probe.SocketDirMode = "755" }, []string{checkRendezvous}},
		{"the socket under the fallback", func(o *hostDialedObs) { o.Probe.Fallback = 1 }, []string{checkRendezvous}},
		{"no listening line", func(o *hostDialedObs) { o.Probe.Listening = 0 }, []string{checkRendezvous}},
		{"a socket in a predictable directory", func(o *hostDialedObs) { o.Probe.Socket = "/tmp/job/relay/relay.sock" }, []string{checkRendezvous}},
		{"a socket of another name", func(o *hostDialedObs) { o.Probe.Socket = "/tmp/job/hb-relay-123/other.sock" }, []string{checkRendezvous}},
		{"no socket", func(o *hostDialedObs) { o.Probe.Socket = "" }, []string{checkRendezvous}},
		{"no probe", func(o *hostDialedObs) { o.Probe = nil }, []string{checkBytes, checkRendezvous}},
		{"a relay of another epoch", func(o *hostDialedObs) { o.Lines[1] = goodLine("serving", 6, 2) }, []string{checkEpoch}},
		{"the stale relay not run", func(o *hostDialedObs) { o.Stale = staleResult{} }, []string{checkEpoch}},
		{"the stale relay accepted", func(o *hostDialedObs) { o.Stale.Exit = 0 }, []string{checkEpoch}},
		{"the stale relay not ready", func(o *hostDialedObs) { o.Stale.Exit = hostdial.ExitNotReady }, []string{checkEpoch}},
		{"the stale relay wrote its preamble", func(o *hostDialedObs) { o.Stale.Stdout = 40 }, []string{checkEpoch}},
		{"the kept relay was restarted", func(o *hostDialedObs) { o.KeptPID = 99 }, []string{checkEpoch}},
		{"the kept relay started again", func(o *hostDialedObs) { o.KeptStarts = 0 }, []string{checkEpoch}},
		{"no control relay runs", func(o *hostDialedObs) {
			s := o.Relays[agentapi.RelayControl]
			s.PID = 0
			o.Relays[agentapi.RelayControl] = s
			o.KeptPID = 0
		}, []string{checkEpoch}},
		{"a byte out of place to the host", func(o *hostDialedObs) { o.Exchanges[0].InPlace = false }, []string{checkBytes}},
		{"a byte changed to the host", func(o *hostDialedObs) { o.Exchanges[0].Received = append(ascending()[:255], 0) }, []string{checkBytes}},
		{"255 bytes to the host", func(o *hostDialedObs) { o.Exchanges[0].Received = ascending()[:255] }, []string{checkBytes}},
		{"another digest to the node", func(o *hostDialedObs) { o.Probe.GotSHA256 = desc }, []string{checkBytes}},
		{"the ascending digest to the node", func(o *hostDialedObs) { o.Probe.GotSHA256 = ascendingSHA256 }, []string{checkBytes}},
		{"255 bytes to the node", func(o *hostDialedObs) { o.Probe.GotBytes = 255 }, []string{checkBytes}},
		{"no connect on the node", func(o *hostDialedObs) { o.Probe.Connected = false }, []string{checkBytes}},
		{"the relays' lines unreadable", func(o *hostDialedObs) { o.LinesErr = "permission denied" }, []string{checkReadable}},
		{"the probe's report unreadable", func(o *hostDialedObs) { o.ProbeErr = "the probe's line 3 is not socket" }, []string{checkReadable}},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := goodObs()
			c.mod(&o)
			want := append([]string(nil), c.want...)
			slices.Sort(want)
			if got := failedOf(o); !slices.Equal(got, want) {
				t.Errorf("failed %q, want %q", got, want)
			}
		})
	}
}

// The text to post: the verdict, a line per check with ok or FAIL, frps and frpc said not run, the job's shape; no path.
func TestHostDialedPostsNameEveryCheckAndNoPath(t *testing.T) {
	o := goodObs()
	posts := hostDialedPosts(o, judgeHostDialed(o), "plgrid-now (the partition's own; none requested)", 1, 1, "1Gi")
	for _, want := range []string{
		"reach-host-dialed-athena: PASS (posts to #305 and #145)\n",
		"- the control session: ok: ", "- the serving session: ok: ", "- srun -u: ok: ", "- the rendezvous: ok: ", "- the preamble: ok: ",
		"- the epoch: ok: both relays carried epoch 7; a control relay of epoch 6 was refused as superseded",
		"- the 256-byte check (HBEP-4, open question 16): ok: all 256 byte values crossed the serving session intact both ways",
		"under the job's $TMPDIR (under /tmp)", "an --overlap step's $TMPDIR: set, the same",
		"- frps and frpc: not run: ", "time limit 5 min, with no requeue\n",
	} {
		if !strings.Contains(posts, want) {
			t.Errorf("the text to post lacks %q:\n%s", want, posts)
		}
	}
	for _, private := range []string{"/tmp/job", "hb-relay-123", "4242", "u-1", goodAgent} {
		if strings.Contains(posts, private) {
			t.Errorf("the text to post holds %q:\n%s", private, posts)
		}
	}
	o.Registered = 0
	if posts := hostDialedPosts(o, judgeHostDialed(o), "x", 1, 1, "1Gi"); !strings.HasPrefix(posts, "reach-host-dialed-athena: FAIL ") || !strings.Contains(posts, "- the control session: FAIL: ") {
		t.Errorf("a failed check does not fail the post:\n%s", posts)
	}
}

func TestTopDirNamesOnlyAGenericTop(t *testing.T) {
	for in, want := range map[string]string{
		"/tmp/slurm-4242":         "under /tmp",
		"/dev/shm":                "under /dev/shm",
		"/dev/shm/user/4242":      "under /dev/shm",
		"/scratch/local/4242":     "under /scratch",
		"/net/ascratch/user/4242": "under /net",
		"/Scratch/x":              "under a top directory this row does not print",
		"/node0042/tmp":           "under a top directory this row does not print",
		"/averyveryverylongname/": "under a top directory this row does not print",
	} {
		if got := topDir(in); got != want {
			t.Errorf("topDir(%q) = %q, want %q", in, got, want)
		}
	}
	p := &probeReport{TmpDirSet: true, TmpDir: "/tmp/job/"}
	if got := stepTmpDir(p, "/tmp/job"); got != "set, the same" {
		t.Errorf("the same TMPDIR reads %q", got)
	}
	if got := stepTmpDir(&probeReport{TmpDirSet: true, TmpDir: "/scratch/j"}, "/dev/shm/x"); got != "set, another (under /scratch)" {
		t.Errorf("another TMPDIR reads %q", got)
	}
	if got := stepTmpDir(&probeReport{}, "/tmp/job"); got != "unset" {
		t.Errorf("an unset TMPDIR reads %q", got)
	}
}

const goodReport = "probe=1\ntmpdir-step=set\ntmpdir=/tmp/job\nsocket=/tmp/job/hb-relay-1/relay.sock\nsocket-dir-mode=700\nlistening=1\nfallback=0\n" +
	"serving-port=found\nconnect=ok\ngot-bytes=256\ngot-sha256=cd6816b77f68d70001fc3eaa4d42bdd67cb5973b3151cc5292ecc02a3daac6ab\ndone=1\n"

func TestParseProbeReadsOnlyItsOwnForm(t *testing.T) {
	p, err := parseProbe([]byte(goodReport))
	if err != nil {
		t.Fatal(err)
	}
	if !p.TmpDirSet || p.TmpDir != "/tmp/job" || p.Socket != "/tmp/job/hb-relay-1/relay.sock" || p.SocketDirMode != "700" || p.Listening != 1 || p.Fallback != 0 ||
		!p.PortFound || !p.Connected || p.GotBytes != 256 || p.GotSHA256 != descendingSHA256 {
		t.Errorf("the report read as %+v", p)
	}
	if p, err := parseProbe([]byte("probe=1\ntmpdir-step=unset\nsocket=\nsocket-dir-mode=unknown\nlistening=0\nfallback=0\nserving-port=missing\ndone=1\n")); err != nil || p.PortFound || p.TmpDirSet {
		t.Errorf("a report with no port: %+v, %v", p, err)
	}
	for name, bad := range map[string]string{
		"no end":               strings.TrimSuffix(goodReport, "done=1\n"),
		"a line after its end": goodReport + "extra=1\n",
		"a count with a zero":  strings.Replace(goodReport, "listening=1", "listening=01", 1),
		"a negative count":     strings.Replace(goodReport, "fallback=0", "fallback=-1", 1),
		"an unknown answer":    strings.Replace(goodReport, "connect=ok", "connect=maybe", 1),
		"a digest that is not": strings.Replace(goodReport, "got-sha256=cd", "got-sha256=xx", 1),
		"a mode that is not":   strings.Replace(goodReport, "socket-dir-mode=700", "socket-dir-mode=rwx", 1),
		"a TMPDIR not given":   strings.Replace(goodReport, "tmpdir=/tmp/job\n", "", 1),
		"lines out of order":   strings.Replace(goodReport, "listening=1\nfallback=0", "fallback=0\nlistening=1", 1),
		"another probe":        strings.Replace(goodReport, "probe=1", "probe=2", 1),
		"nothing":              "",
	} {
		if _, err := parseProbe([]byte(bad)); err == nil {
			t.Errorf("%s: the report was read", name)
		}
	}
}

// The probe script itself, run by bash on this machine against a responder, with the agent's two files as the agent
// writes them: the 256 values go out each in its place and the descending ones come back, and the report says so.
func TestTheServingProbeRunsItsExchangeAndReportsIt(t *testing.T) {
	r := startServingResponder(t)
	dir := t.TempDir()
	sockDir := filepath.Join(dir, "hb-relay-42")
	if err := os.Mkdir(sockDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, agentapi.RelaySocketFileName), []byte(filepath.Join(sockDir, "relay.sock")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	jobErr := `time=2026-11-10T00:00:00Z level=INFO msg="host-dialed: listening for the relays" tag=hb-x job=1 agentInstanceID=a control=127.0.0.1:1 serving=` +
		strconv.Itoa(r.port()) + " socket=" + filepath.Join(sockDir, "relay.sock") + "\n"
	if err := os.WriteFile(filepath.Join(dir, agentapi.JobErrorName), []byte(jobErr), 0o600); err != nil {
		t.Fatal(err)
	}
	run := func(env ...string) probeReport {
		t.Helper()
		cmd := exec.Command("bash", "-s", "--", dir)
		cmd.Stdin = strings.NewReader(strings.ReplaceAll(servingProbeScript, "@READ_SECONDS@", "5"))
		cmd.Env = append(os.Environ(), env...)
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("the probe: %v", err)
		}
		p, err := parseProbe(out)
		if err != nil {
			t.Fatalf("the probe's report: %v\n%s", err, out)
		}
		return p
	}
	p := run("TMPDIR=" + dir)
	if !p.TmpDirSet || p.TmpDir != dir || p.Socket != filepath.Join(sockDir, "relay.sock") || p.SocketDirMode != "700" || p.Listening != 1 || p.Fallback != 0 {
		t.Errorf("the rendezvous read as %+v", p)
	}
	if !p.PortFound || !p.Connected || p.GotBytes != 256 || p.GotSHA256 != descendingSHA256 {
		t.Errorf("the exchange read as %+v", p)
	}
	waitFor(t, 10*time.Second, func() bool { return len(r.exchanges()) == 1 })
	x := r.exchanges()[0]
	if len(x.Received) != 256 || !x.InPlace || sha256Hex(x.Received) != ascendingSHA256 || x.Wrote != 256 || !x.Closed {
		t.Errorf("the responder saw %d byte(s), in place %v, wrote %d, closed %v", len(x.Received), x.InPlace, x.Wrote, x.Closed)
	}
	// With no TMPDIR, the probe says so; with no line naming the port, it does not connect.
	if err := os.WriteFile(filepath.Join(dir, agentapi.JobErrorName), []byte("level=ERROR msg=\"cannot start\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if p := run("TMPDIR="); p.TmpDirSet || p.PortFound || p.Listening != 0 {
		t.Errorf("with no TMPDIR and no listening line the report read as %+v", p)
	}
	if n := len(r.exchanges()); n != 1 {
		t.Errorf("the responder has %d exchange(s), want the first one only", n)
	}
}

// waitFor polls cond every 20 ms until it holds, and fails at the deadline.
func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(d); !cond(); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("not within %s", d)
		}
	}
}

// The responder answers a node that sends the 256 values out of order with nothing, and records that they were not in
// place: the host's check is its own, of each byte at its index.
func TestTheServingResponderChecksEachByteInItsPlace(t *testing.T) {
	r := startServingResponder(t)
	c, err := net.Dial("tcp4", "127.0.0.1:"+strconv.Itoa(r.port()))
	if err != nil {
		t.Fatal(err)
	}
	b := ascending()
	b[10], b[13] = 13, 10
	if _, err := c.Write(b); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	got := make([]byte, 256)
	n, _ := c.Read(got)
	c.Close()
	if n == 0 {
		t.Fatal("the responder sent nothing back")
	}
	waitFor(t, 10*time.Second, func() bool { return len(r.exchanges()) == 1 })
	if x := r.exchanges()[0]; x.InPlace || len(x.Received) != 256 {
		t.Errorf("bytes out of place read as in place %v (%d)", x.InPlace, len(x.Received))
	}
}

// The waits the body adds are each derived from what they wait for (issue #305; the reviews of PR 284): the session
// wait from the relay's own worst case, named from internal/hostdial, never copied, with the listing's poll before it
// and the agent's wait for a session after; the probe's from the agent's wait for a session and the probe's read; the
// stale relay's from its ssh connect and the agent's answer.
func TestHostDialedWaitsCoverWhatTheyWaitFor(t *testing.T) {
	const worldPoll = 10 * time.Second // buildWorld's poll interval, a live run's
	if need := worldPoll + hostdial.RelayStartWorstCase + hostdial.DefaultWait; hostDialedSessionWait < need {
		t.Errorf("hostDialedSessionWait %s is below the listing's poll, hostdial.RelayStartWorstCase and hostdial.DefaultWait: %s", hostDialedSessionWait, need)
	}
	if need := hostdial.DefaultWait + probeReadSeconds*time.Second; hostDialedProbeWait < need {
		t.Errorf("hostDialedProbeWait %s is below the agent's wait for a session and the probe's read: %s", hostDialedProbeWait, need)
	}
	if need := sshrun.RelayConnectTimeout + 10*time.Second; hostDialedFenceWait < need {
		t.Errorf("hostDialedFenceWait %s is below the relay's ssh connect and the exchange's line: %s", hostDialedFenceWait, need)
	}
	if lim, _ := time.ParseDuration(hostDialedTimeLimit); lim > 10*time.Minute {
		t.Errorf("the job's time limit %s is above the 10 minutes a short row holds the slot for (issue #305)", lim)
	}
	for _, r := range alpha.HostDialedRows() {
		if lim, _ := time.ParseDuration(hostDialedTimeLimit); r.Lease != lim || r.StartWait != 10*time.Minute {
			t.Errorf("%s: the row's lease %s and start wait %s are not the job's %s and 10 min", r.Name, r.Lease, r.StartWait, lim)
		}
	}
}
