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
		Row: row, Epoch: 7, JobID: "4242", UID: "u-1", Tag: "hb-x", AgentPath: goodAgent,
		Lines: []string{goodLine("control", 7, 1), goodLine("serving", 7, 2)},
		Relays: map[agentapi.RelayChannel]hostdial.RelayState{
			agentapi.RelayControl: {Channel: agentapi.RelayControl, PID: 100, Starts: 1, Epoch: 7, Seq: 1, Sessions: 1, Streams: 3, Joined: 3},
			agentapi.RelayServing: {Channel: agentapi.RelayServing, PID: 101, Starts: 1, Epoch: 7, Seq: 2, Sessions: 1, Streams: 1, Joined: 1},
		},
		Kept: map[agentapi.RelayChannel]hostdial.RelayState{
			agentapi.RelayControl: {Channel: agentapi.RelayControl, PID: 100, Starts: 1, Epoch: 7, Seq: 1, Sessions: 1},
			agentapi.RelayServing: {Channel: agentapi.RelayServing, PID: 101, Starts: 1, Epoch: 7, Seq: 2, Sessions: 1},
		},
		Stale: map[agentapi.RelayChannel]hostdial.RelayState{
			agentapi.RelayControl: {Channel: agentapi.RelayControl, Starts: 1, Epoch: 6, Seq: 3, Final: true, FinalCause: "superseded: hb-relay: superseded"},
			agentapi.RelayServing: {Channel: agentapi.RelayServing, Starts: 1, Epoch: 6, Seq: 4, Final: true, FinalCause: "superseded: hb-relay: superseded"},
		},
		Registered: 1,
		Probe: &probeReport{TmpDirSet: true, TmpDir: "/tmp/jx42", Socket: "/tmp/jx42/hb-relay-123/relay.sock", SocketDirMode: "700",
			AgentLogOK: true, AgentTmpDir: "/tmp/jx42", PortFound: true, Connected: true, GotBytes: 256, GotSHA256: "cd6816b77f68d70001fc3eaa4d42bdd67cb5973b3151cc5292ecc02a3daac6ab"},
		Exchanges:  []servingConn{{Received: ascending(), InPlace: true, Wrote: 256, Closed: true}},
		StopServed: true, StopAcked: true, EndState: "COMPLETED",
		AgentTail: "time=2026-11-10T00:00:00Z level=INFO msg=\"allocation agent exiting\"\n",
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
		{"no registration and the job ended", func(o *hostDialedObs) { o.Registered, o.JobEnded, o.EndState = 0, true, "FAILED" }, []string{checkControl}},
		{"the job ended before the stop", func(o *hostDialedObs) { o.JobEnded, o.EndState = true, "FAILED" }, []string{checkControl}},
		{"the stop never served", func(o *hostDialedObs) { o.StopServed, o.StopAcked = false, false }, []string{checkControl}},
		{"no report after the stop", func(o *hostDialedObs) { o.StopAcked = false }, []string{checkControl}},
		{"the job FAILED after the stop", func(o *hostDialedObs) { o.EndState = "FAILED" }, []string{checkControl}},
		{"the job CANCELLED after the stop", func(o *hostDialedObs) { o.EndState = "CANCELLED" }, []string{checkControl}},
		{"the job's end state unread", func(o *hostDialedObs) { o.EndState = "" }, []string{checkControl}},
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
		// Another option in -u's place: -l would prefix every line of the step's output with its task number.
		{"a line with -l for -u", func(o *hostDialedObs) { o.Lines[1] = strings.Replace(o.Lines[1], "srun -u ", "srun -l ", 1) }, []string{checkSrunU}},
		{"a line without --overlap", func(o *hostDialedObs) { o.Lines[0] = strings.Replace(o.Lines[0], " --overlap", "", 1) }, []string{checkSrunU}},
		{"a line on another job", func(o *hostDialedObs) { o.Lines[0] = strings.Replace(o.Lines[0], "--jobid=4242", "--jobid=4243", 1) }, []string{checkSrunU}},
		{"a line of another agent", func(o *hostDialedObs) { o.Lines[0] = strings.Replace(o.Lines[0], goodAgent, "/tmp/hb-agent", 1) }, []string{checkSrunU}},
		{"no serving line", func(o *hostDialedObs) { o.Lines = o.Lines[:1] }, []string{checkSrunU}},
		{"no line at all", func(o *hostDialedObs) { o.Lines = nil }, []string{checkSrunU}},
		{"the socket's directory is mode 755", func(o *hostDialedObs) { o.Probe.SocketDirMode = "755" }, []string{checkRendezvous}},
		{"the socket under /dev/shm, not the agent's TMPDIR", func(o *hostDialedObs) { o.Probe.Socket = "/dev/shm/hb-relay-9/relay.sock" }, []string{checkRendezvous}},
		{"the socket a level below the agent's TMPDIR", func(o *hostDialedObs) { o.Probe.Socket = "/tmp/jx42/x/hb-relay-9/relay.sock" }, []string{checkRendezvous}},
		{"another TMPDIR in the agent's line", func(o *hostDialedObs) { o.Probe.AgentTmpDir = "/scratch/job" }, []string{checkRendezvous}},
		{"no TMPDIR in the agent's line", func(o *hostDialedObs) { o.Probe.AgentTmpDir = "" }, []string{checkRendezvous}},
		{"a relative TMPDIR in the agent's line", func(o *hostDialedObs) { o.Probe.AgentTmpDir = "tmp/job" }, []string{checkRendezvous}},
		{"the agent's output unreadable", func(o *hostDialedObs) { o.Probe.AgentLogOK = false }, []string{checkRendezvous}},
		{"a socket in a predictable directory", func(o *hostDialedObs) { o.Probe.Socket = "/tmp/jx42/relay/relay.sock" }, []string{checkRendezvous}},
		{"a socket in an hb-relay- directory with no random part", func(o *hostDialedObs) { o.Probe.Socket = "/tmp/jx42/hb-relay-x/relay.sock" }, []string{checkRendezvous}},
		{"a socket of another name", func(o *hostDialedObs) { o.Probe.Socket = "/tmp/jx42/hb-relay-123/other.sock" }, []string{checkRendezvous}},
		{"no socket", func(o *hostDialedObs) { o.Probe.Socket = "" }, []string{checkRendezvous}},
		{"no probe", func(o *hostDialedObs) { o.Probe = nil }, []string{checkBytes, checkRendezvous}},
		{"a relay of another epoch", func(o *hostDialedObs) { o.Lines[1] = goodLine("serving", 6, 2) }, []string{checkEpoch}},
		{"the stale pair not run", func(o *hostDialedObs) { o.Stale = nil }, []string{checkEpoch}},
		{"the stale serving relay not run", func(o *hostDialedObs) { delete(o.Stale, agentapi.RelayServing) }, []string{checkEpoch}},
		{"the stale control relay accepted", func(o *hostDialedObs) {
			st := o.Stale[agentapi.RelayControl]
			st.Final, st.FinalCause, st.Sessions = false, "", 1
			o.Stale[agentapi.RelayControl] = st
		}, []string{checkEpoch}},
		{"the stale serving relay ended for another reason", func(o *hostDialedObs) {
			st := o.Stale[agentapi.RelayServing]
			st.FinalCause = "failed: srun refused the step"
			o.Stale[agentapi.RelayServing] = st
		}, []string{checkEpoch}},
		{"a stale relay that wrote its preamble", func(o *hostDialedObs) {
			st := o.Stale[agentapi.RelayControl]
			st.Sessions = 1
			o.Stale[agentapi.RelayControl] = st
		}, []string{checkEpoch}},
		{"a stale relay of the kept epoch", func(o *hostDialedObs) {
			st := o.Stale[agentapi.RelayControl]
			st.Epoch = 7
			o.Stale[agentapi.RelayControl] = st
		}, []string{checkEpoch}},
		// The regression the review named: a stale relay whose sequence is the kept one's or below would be refused by an
		// agent that compares sequences only, epoch or not.
		{"a stale sequence equal to the kept one's", func(o *hostDialedObs) {
			st := o.Stale[agentapi.RelayControl]
			st.Seq = o.Kept[agentapi.RelayControl].Seq
			o.Stale[agentapi.RelayControl] = st
		}, []string{checkEpoch}},
		{"a stale serving sequence below the kept one's", func(o *hostDialedObs) {
			st := o.Stale[agentapi.RelayServing]
			st.Seq = 1
			o.Stale[agentapi.RelayServing] = st
		}, []string{checkEpoch}},
		{"the kept relay was restarted", func(o *hostDialedObs) {
			s := o.Relays[agentapi.RelayControl]
			s.PID = 99
			o.Relays[agentapi.RelayControl] = s
		}, []string{checkEpoch}},
		{"the kept relay started again", func(o *hostDialedObs) {
			s := o.Relays[agentapi.RelayControl]
			s.Starts = 2
			o.Relays[agentapi.RelayControl] = s
		}, []string{checkEpoch}},
		{"no control relay runs", func(o *hostDialedObs) {
			s := o.Relays[agentapi.RelayControl]
			s.PID = 0
			o.Relays[agentapi.RelayControl] = s
			k := o.Kept[agentapi.RelayControl]
			k.PID = 0
			o.Kept[agentapi.RelayControl] = k
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
		"- the epoch: ok: both relays carried epoch 7; a relay pair of epoch 6, each with a higher sequence than the kept relay's, was refused as superseded on both channels",
		"- the control session: ok: up: the agent registered over the control relay (1 registration(s), the recorded job); the stop was served to its poll over the same session, it sent its last report, and the job ended COMPLETED",
		"- the 256-byte check (HBEP-4, open question 16): ok: all 256 byte values crossed the serving session intact both ways",
		"- the rendezvous: ok: socket under $TMPDIR: yes; fallback to /dev/shm: no; its path is 33 bytes (at most 107); its directory mode 700; an --overlap step's $TMPDIR: set, the same; both relays, --overlap steps, reached it\n",
		"- frps and frpc: not run: ", "time limit 5 min, with no requeue\n",
	} {
		if !strings.Contains(posts, want) {
			t.Errorf("the text to post lacks %q:\n%s", want, posts)
		}
	}
	for _, private := range []string{"/tmp/jx42", "hb-relay-123", "4242", "u-1", goodAgent} {
		if strings.Contains(posts, private) {
			t.Errorf("the text to post holds %q:\n%s", private, posts)
		}
	}
	o.Registered = 0
	if posts := hostDialedPosts(o, judgeHostDialed(o), "x", 1, 1, "1Gi"); !strings.HasPrefix(posts, "reach-host-dialed-athena: FAIL ") || !strings.Contains(posts, "- the control session: FAIL: ") {
		t.Errorf("a failed check does not fail the post:\n%s", posts)
	}
}

// What the relay owner, the relay or a tool says goes into a failed line, and can name the job, the tag, paths and hosts:
// none of it reaches the text to post, which says where each stood.
func TestAFailedPostNamesNoJobPathOrHostFromTheOwnersWords(t *testing.T) {
	o := goodObs()
	o.Registered = 0
	ctl := o.Relays[agentapi.RelayControl]
	ctl.Sessions, ctl.LastEnd = 0, "retried: job 4242 is PENDING at /scratch/user/x on login01.cluster.example.org, job 1234567 too"
	o.Relays[agentapi.RelayControl] = ctl
	srv := o.Relays[agentapi.RelayServing]
	srv.Sessions, srv.Joined, srv.LastEnd, srv.LastRefusal = 0, 0, "", "job 4242 is not listed under tag hb-x"
	o.Relays[agentapi.RelayServing] = srv
	st := o.Stale[agentapi.RelayControl]
	st.FinalCause = "failed: hb-relay: the agent of Allocation u-1 accepted it\x1b[0m"
	o.Stale[agentapi.RelayControl] = st
	o.Lines[0] = strings.Replace(o.Lines[0], "--epoch 7", "--epoch /etc/passwd", 1)
	o.Lines[1] = strings.Replace(o.Lines[1], "relay serving", "relay /tmp/x", 1)
	posts := hostDialedPosts(o, judgeHostDialed(o), "x", 1, 1, "1Gi")
	for _, private := range []string{"4242", "1234567", "/scratch", "login01", "example.org", "hb-x", "u-1", "/etc/passwd", "/tmp/x", goodAgent, "\x1b"} {
		if strings.Contains(posts, private) {
			t.Errorf("the text to post holds %q:\n%s", private, posts)
		}
	}
	for _, want := range []string{"retried: job <the job> is PENDING at <a path> on <a name>, job <a number> too", "not started: job <the job> is not listed under tag <the tag>", "--epoch <a path>", "the agent of Allocation <the UID> accepted it"} {
		if !strings.Contains(posts, want) {
			t.Errorf("the text to post lacks %q:\n%s", want, posts)
		}
	}
	if long := postSafe(strings.Repeat("x", 500), o); len(long) != 160 {
		t.Errorf("postSafe kept %d bytes of 500, want 160", len(long))
	}
}

// How an --overlap step's TMPDIR compares with the agent's, in words only.
func TestStepTmpDirSaysOnlyHowItCompares(t *testing.T) {
	if got := stepTmpDir(&probeReport{TmpDirSet: true, TmpDir: "/tmp/jx42/"}, "/tmp/jx42"); got != "set, the same" {
		t.Errorf("the same TMPDIR reads %q", got)
	}
	if got := stepTmpDir(&probeReport{TmpDirSet: true, TmpDir: "/scratch/j"}, "/tmp/jx42"); got != "set, another" {
		t.Errorf("another TMPDIR reads %q", got)
	}
	if got := stepTmpDir(&probeReport{}, "/tmp/jx42"); got != "unset" {
		t.Errorf("an unset TMPDIR reads %q", got)
	}
}

// The science agent's ruling on PR 306: on a cluster $TMPDIR and the socket's path hold the user's login and the job's
// ID, so the text to post holds no part of either, whatever the run came to. The paths are shaped like a cluster's: a
// login-like and a job-ID-like component, under a scratch tree, and an --overlap step's TMPDIR under another root. The
// rendezvous line says yes or no, and lengths. /dev/shm's two components are the one exception: the ruling's own words
// name the fallback.
func TestAHostDialedPostHoldsNoPartOfAClusterPath(t *testing.T) {
	const (
		tmp     = "/net/ascratch/people/exlogin42/job-4242424"
		step    = "/scratch/exlogin42/slurm-4242424-step"
		sock    = tmp + "/hb-relay-8123456/relay.sock"
		longTmp = tmp + "/a-node-local-directory-of-the-job-with-a-long-name"
		shmSock = "/dev/shm/hb-relay-8123456/relay.sock"
		login   = "exlogin42"
		host    = "login01.cluster.example"
	)
	cluster := func() hostDialedObs {
		o := goodObs()
		o.JobID = "4242424"
		for i := range o.Lines {
			o.Lines[i] = strings.Replace(o.Lines[i], "--jobid=4242 ", "--jobid=4242424 ", 1)
		}
		o.Probe.TmpDir, o.Probe.AgentTmpDir, o.Probe.Socket = step, tmp, sock
		o.Private = []string{login, host}
		return o
	}
	pathError := `time=2026-11-10T00:00:01Z level=ERROR msg="no apptainer on this node (HBEP-4, open question 15)" err="exec: \"apptainer\": not in PATH ` + tmp + `/bin:/usr/bin of ` + login + `@login01"`
	for _, c := range []struct {
		name, line string
		ok         bool
		mod        func(o *hostDialedObs)
		scrubbed   bool // a quoted text held private values, which the post shows were taken out
	}{
		{"a pass", "- the rendezvous: ok: socket under $TMPDIR: yes; fallback to /dev/shm: no; its path is 70 bytes (at most 107); its directory mode 700; an --overlap step's $TMPDIR: set, another", true, func(*hostDialedObs) {}, false},
		{"the fallback", "- the rendezvous: FAIL: socket under $TMPDIR: no; fallback to /dev/shm: yes, since a socket under $TMPDIR could be 124 bytes, over the 107 a socket's path may have; its path is 36 bytes (at most 107)", false,
			func(o *hostDialedObs) { o.Probe.AgentTmpDir, o.Probe.Socket = longTmp, shmSock }, false},
		{"a fallback for no reason shown", "- the rendezvous: FAIL: socket under $TMPDIR: no; fallback to /dev/shm: yes, for a reason not shown (a socket under $TMPDIR would be at most 73 bytes, within 107)", false,
			func(o *hostDialedObs) { o.Probe.Socket = shmSock }, false},
		{"a socket elsewhere", "- the rendezvous: FAIL: socket under $TMPDIR: no; fallback to /dev/shm: no (its directory is elsewhere)", false,
			func(o *hostDialedObs) { o.Probe.Socket = step + "/hb-relay-8123456/relay.sock" }, false},
		{"no agent line", "- the rendezvous: FAIL: socket under $TMPDIR: not shown (the agent's output does not name its $TMPDIR)", false,
			func(o *hostDialedObs) { o.Probe.AgentTmpDir = "" }, false},
		{"an agent that stops at its start", "- the control session: FAIL: did not come up: the job ended FAILED before the agent registered", false, func(o *hostDialedObs) {
			o.Registered, o.JobEnded, o.EndState, o.AgentTail = 0, true, "FAILED", pathError+"\n"
		}, true},
		{"the owner's words", "- the control session: FAIL: did not come up: no registration", false, func(o *hostDialedObs) {
			o.Registered = 0
			ctl := o.Relays[agentapi.RelayControl]
			ctl.LastEnd = "retried: ssh: " + login + "@login01: Connection closed by " + host + " port 22 in " + tmp
			o.Relays[agentapi.RelayControl] = ctl
		}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := cluster()
			c.mod(&o)
			posts := hostDialedPosts(o, judgeHostDialed(o), "plgrid-now (the partition's own; none requested)", 1, 1, "1Gi")
			if !strings.Contains(posts, c.line) {
				t.Errorf("the text to post lacks %q:\n%s", c.line, posts)
			}
			if pass := strings.HasPrefix(posts, "reach-host-dialed-athena: PASS "); pass != c.ok {
				t.Errorf("the post passes %v, want %v:\n%s", pass, c.ok, posts)
			}
			if c.scrubbed && !strings.Contains(posts, "<private>") && !strings.Contains(posts, "<a path>") {
				t.Errorf("the quoted text's private values were not taken out where they stood:\n%s", posts)
			}
			// Every component of every path the run read, and the login's user and host: the observation holds each
			// (so the search means something), and the post none.
			var parts []string
			for _, p := range []string{o.Probe.TmpDir, o.Probe.AgentTmpDir, o.Probe.Socket, tmp, step, sock} {
				for _, part := range strings.Split(p, "/") {
					if part != "" && !(strings.HasPrefix(p, "/dev/shm/") && (part == "dev" || part == "shm")) {
						parts = append(parts, part)
					}
				}
			}
			parts = append(parts, login, host, "login01", "4242424")
			if len(parts) < 12 {
				t.Fatalf("%d private values to look for; the search would mean nothing", len(parts))
			}
			for _, part := range parts {
				if strings.Contains(posts, part) {
					t.Errorf("the text to post holds %q, a part of a private path or name:\n%s", part, posts)
				}
			}
		})
	}
}

const goodReport = "probe=1\ntmpdir-step=set\ntmpdir=/tmp/jx42\nsocket=/tmp/jx42/hb-relay-1/relay.sock\nsocket-dir-mode=700\nagent-log=ok\nagent-tmpdir=/tmp/jx42\n" +
	"serving-port=found\nconnect=ok\ngot-bytes=256\ngot-sha256=cd6816b77f68d70001fc3eaa4d42bdd67cb5973b3151cc5292ecc02a3daac6ab\ndone=1\n"

func TestParseProbeReadsOnlyItsOwnForm(t *testing.T) {
	p, err := parseProbe([]byte(goodReport))
	if err != nil {
		t.Fatal(err)
	}
	if !p.TmpDirSet || p.TmpDir != "/tmp/jx42" || p.Socket != "/tmp/jx42/hb-relay-1/relay.sock" || p.SocketDirMode != "700" || !p.AgentLogOK || p.AgentTmpDir != "/tmp/jx42" ||
		!p.PortFound || !p.Connected || p.GotBytes != 256 || p.GotSHA256 != descendingSHA256 {
		t.Errorf("the report read as %+v", p)
	}
	if p, err := parseProbe([]byte("probe=1\ntmpdir-step=unset\nsocket=\nsocket-dir-mode=unknown\nagent-log=unreadable\nagent-tmpdir=\nserving-port=missing\ndone=1\n")); err != nil || p.PortFound || p.TmpDirSet || p.AgentLogOK || p.AgentTmpDir != "" {
		t.Errorf("a report with no port: %+v, %v", p, err)
	}
	for name, bad := range map[string]string{
		"no end":               strings.TrimSuffix(goodReport, "done=1\n"),
		"a line after its end": goodReport + "extra=1\n",
		"a count with a zero":  strings.Replace(goodReport, "got-bytes=256", "got-bytes=0256", 1),
		"a negative count":     strings.Replace(goodReport, "got-bytes=256", "got-bytes=-256", 1),
		"another agent-log":    strings.Replace(goodReport, "agent-log=ok", "agent-log=maybe", 1),
		"an unknown answer":    strings.Replace(goodReport, "connect=ok", "connect=maybe", 1),
		"a digest that is not": strings.Replace(goodReport, "got-sha256=cd", "got-sha256=xx", 1),
		"a mode that is not":   strings.Replace(goodReport, "socket-dir-mode=700", "socket-dir-mode=rwx", 1),
		"a TMPDIR not given":   strings.Replace(goodReport, "tmpdir=/tmp/jx42\n", "", 1),
		"lines out of order":   strings.Replace(goodReport, "agent-log=ok\nagent-tmpdir=/tmp/jx42", "agent-tmpdir=/tmp/jx42\nagent-log=ok", 1),
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
		strconv.Itoa(r.port()) + " socket=" + filepath.Join(sockDir, "relay.sock") + " tmpdir=" + dir + "\n"
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
	if !p.TmpDirSet || p.TmpDir != dir || p.Socket != filepath.Join(sockDir, "relay.sock") || p.SocketDirMode != "700" || !p.AgentLogOK || p.AgentTmpDir != dir {
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
	if p := run("TMPDIR="); p.TmpDirSet || p.PortFound || !p.AgentLogOK || p.AgentTmpDir != "" {
		t.Errorf("with no TMPDIR and no listening line the report read as %+v", p)
	}
	// An agent's output that cannot be read is said to be, and names no TMPDIR: never a zero count read as a pass.
	if err := os.Chmod(filepath.Join(dir, agentapi.JobErrorName), 0); err != nil {
		t.Fatal(err)
	}
	if p := run("TMPDIR=" + dir); p.AgentLogOK || p.AgentTmpDir != "" || p.PortFound {
		t.Errorf("with job.err unreadable the report read as %+v", p)
	}
	if n := len(r.exchanges()); n != 1 {
		t.Errorf("the responder has %d exchange(s), want the first one only", n)
	}
}

// The agent's last error, from the tail of its output: the msg and err of its last ERROR line.
func TestLastAgentErrorReadsTheAgentsLastErrorLine(t *testing.T) {
	tail := "time=2026-11-10T00:00:00Z level=INFO msg=\"allocation agent starting\" mode=host-dialed\n" +
		"time=2026-11-10T00:00:01Z level=ERROR msg=\"no apptainer on this node (HBEP-4, open question 15)\" tag=hb-x job=4242 err=\"exec: \\\"apptainer\\\": executable file not found in $PATH\"\n"
	if got, want := lastAgentError(tail), `no apptainer on this node (HBEP-4, open question 15): exec: "apptainer": executable file not found in $PATH`; got != want {
		t.Errorf("lastAgentError = %q, want %q", got, want)
	}
	if got := lastAgentError("time=x level=INFO msg=\"allocation agent exiting\"\n"); got != `time=x level=INFO msg="allocation agent exiting"` {
		t.Errorf("with no ERROR line lastAgentError = %q, want the last line", got)
	}
	if got := lastAgentError(""); got != "none read" {
		t.Errorf("with nothing read lastAgentError = %q", got)
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
	if need := worldPollInterval + hostdial.RelayStartWorstCase + hostdial.DefaultWait; hostDialedSessionWait < need {
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
