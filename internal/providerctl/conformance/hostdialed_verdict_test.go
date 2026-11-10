package conformance

// The reach-host-dialed rows' verdict (issue #305): the facts a run saw, one check per line of the text to post, and the
// text itself. The checks are the issue's: both sessions up, srun -u, the rendezvous, the preamble, the epoch, and HBEP-4
// open question 16's 256-byte check.

import (
	"fmt"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// staleResult is how the relay of the epoch below the Lease's ended (HBEP-6, Fencing: the agent refuses a lower (E, N)
// with the reason superseded).
type staleResult struct {
	Ran      bool
	Exit     int
	Stdout   int    // bytes on its standard output: none, since only an accepted relay writes its preamble
	Classify string // its end in sshrun.Classify's words: the relay's own line, or the kind of ssh failure
}

// hostDialedObs is what one run saw: the facts the verdict is made of, read from the relay owner, the backend, the
// responder, the wrappers' record and the probe. Paths and the job ID are private, kept for the record only.
type hostDialedObs struct {
	Row       alpha.Row
	Epoch     int64
	JobID     string
	UID       string
	AgentPath string
	// JobEnded is set when the job ended before the checks did: the agent exited, or the job was cancelled.
	JobEnded bool
	// Lines are the remote lines the relays' ssh got; LinesErr says why the record could not be read.
	Lines    []string
	LinesErr string
	// Relays are the owner's relays of the Allocation, by channel, after the checks.
	Relays map[agentapi.RelayChannel]hostdial.RelayState
	// Registered counts the agent's registrations that presented the recorded job.
	Registered int
	// Probe is the serving probe's report, nil when it did not run; ProbeErr says why it could not be read.
	Probe    *probeReport
	ProbeErr string
	// Exchanges are the serving responder's connections.
	Exchanges []servingConn
	// Stale is the stale relay's end; KeptPID and KeptStarts are the control relay's process and starts before it, to
	// compare with Relays after it.
	Stale      staleResult
	KeptPID    int
	KeptStarts int
	// Stopped is set when the job ended after the agent was told to stop.
	Stopped bool
}

// hostDialedCheck is one line of the text to post: its name, whether it held, and what was found.
type hostDialedCheck struct {
	Name string
	OK   bool
	Text string
}

// The checks' names, as the text to post and a failure name them.
const (
	checkControl    = "the control session"
	checkServing    = "the serving session"
	checkSrunU      = "srun -u"
	checkRendezvous = "the rendezvous"
	checkPreamble   = "the preamble"
	checkEpoch      = "the epoch"
	checkBytes      = "the 256-byte check (HBEP-4, open question 16)"
	checkReadable   = "the run's state"
)

// lineWords splits a relay line into the words srun got: srun -u --overlap --jobid=<job> --ntasks=1 <agent> relay
// <channel> --dir <dir> --allocation <UID> --epoch <E> --seq <N> (agentapi.RelayLine).
func lineWords(l string) []string { return strings.Fields(l) }

// lineArg is the value after flag among the words, or "".
func lineArg(words []string, flag string) string {
	for i := 0; i+1 < len(words); i++ {
		if words[i] == flag {
			return words[i+1]
		}
	}
	return ""
}

// channelOf is a relay line's channel, the word after "relay", or "".
func channelOf(words []string) string { return lineArg(words, agentapi.RelaySubcommand) }

// judgeHostDialed is the verdict of a run: one check per line of the text to post, in that order. A run passes only if
// every check holds; a check that failed says what was found, and an unreadable state is its own failed check.
func judgeHostDialed(o hostDialedObs) []hostDialedCheck {
	var out []hostDialedCheck
	add := func(name string, ok bool, format string, a ...any) {
		out = append(out, hostDialedCheck{Name: name, OK: ok, Text: fmt.Sprintf(format, a...)})
	}
	ctl, srv := o.Relays[agentapi.RelayControl], o.Relays[agentapi.RelayServing]
	byChannel := map[string]int{}
	for _, l := range o.Lines {
		byChannel[channelOf(lineWords(l))]++
	}

	// The state the rest is read from.
	switch {
	case o.LinesErr != "":
		add(checkReadable, false, "the relays' remote lines could not be read: %s", o.LinesErr)
	case o.ProbeErr != "":
		add(checkReadable, false, "the serving probe's report could not be read: %s", o.ProbeErr)
	}

	// The control session: the agent's registration and its stop crossed it.
	switch {
	case o.Registered < 1 && o.JobEnded:
		add(checkControl, false, "did not come up: the job ended before the agent registered, so the agent stopped at its start (a job with no TMPDIR, a node with no Apptainer: HBEP-4, open questions 15 and 16)")
	case o.Registered < 1:
		add(checkControl, false, "did not come up: no registration of the agent reached the agent API over it (%d relay start(s), the last end: %s)", ctl.Starts, endText(ctl))
	case !o.Stopped:
		add(checkControl, false, "came up and the agent registered, but the agent was not seen to stop when told to over it")
	default:
		add(checkControl, true, "up: the agent registered over the control relay (%d registration(s), the recorded job) and stopped when told to, over the same session", o.Registered)
	}

	// The serving session: the probe's exchange crossed it.
	switch {
	case srv.Sessions < 1 || srv.Joined < 1 || len(o.Exchanges) == 0:
		why := "no session came up"
		if srv.Sessions >= 1 {
			why = "a session came up but no stream from the job reached the serving port"
		}
		add(checkServing, false, "did not come up: %s (%d relay start(s), the last end: %s)", why, srv.Starts, endText(srv))
	default:
		add(checkServing, true, "up: a stream the job opened crossed it to the serving port (%d stream(s))", srv.Joined)
	}

	// srun -u: every relay line's first words are srun -u --overlap --jobid=<the job> --ntasks=1 <the staged agent>.
	var bad []string
	for _, ch := range agentapi.RelayChannels {
		if byChannel[string(ch)] == 0 {
			bad = append(bad, "no "+string(ch)+" relay was started")
		}
	}
	for _, l := range o.Lines {
		w := lineWords(l)
		if len(w) < 7 || w[0] != "srun" || w[1] != "-u" || w[2] != "--overlap" || w[3] != "--jobid="+o.JobID || w[4] != "--ntasks=1" || w[5] != o.AgentPath || w[6] != agentapi.RelaySubcommand {
			bad = append(bad, "a "+channelOf(w)+" relay ran a line that is not srun -u --overlap --jobid=<the job> --ntasks=1 <the agent> relay")
			break
		}
	}
	if len(bad) > 0 {
		add(checkSrunU, false, "%s", strings.Join(bad, "; "))
	} else {
		add(checkSrunU, true, "every relay ran srun -u --overlap --jobid=<the job> --ntasks=1 <the staged agent> relay <channel> (%d line(s)), so the step's output is not held for a newline, which the sessions' binary frames lack", len(o.Lines))
	}

	// The rendezvous: the agent's socket in a mode-700 hb-relay-<random> directory under the job's TMPDIR.
	switch p := o.Probe; {
	case p == nil:
		add(checkRendezvous, false, "not read: the serving probe did not run")
	default:
		sockDir := path.Dir(p.Socket)
		var why []string
		if !path.IsAbs(p.Socket) || path.Base(p.Socket) != "relay.sock" || !regexp.MustCompile(`^hb-relay-[0-9]+$`).MatchString(path.Base(sockDir)) {
			why = append(why, "the rendezvous file did not name a relay.sock in an hb-relay-<random> directory")
		}
		if p.SocketDirMode != "700" {
			why = append(why, "the socket's directory is mode "+p.SocketDirMode+", not 700")
		}
		if p.Fallback != 0 {
			why = append(why, "the agent put it under the /dev/shm fallback, not under TMPDIR")
		}
		if p.Listening < 1 {
			why = append(why, "the agent's output does not say it listens for the relays")
		}
		if len(why) > 0 {
			add(checkRendezvous, false, "%s", strings.Join(why, "; "))
			break
		}
		add(checkRendezvous, true, "the agent's socket was in a mode-700 hb-relay-<random> directory under the job's $TMPDIR (%s), not the /dev/shm fallback, and both relays, --overlap steps, reached it; an --overlap step's $TMPDIR: %s",
			topDir(path.Dir(sockDir)), stepTmpDir(p, path.Dir(sockDir)))
	}

	// The preamble: a session counts only once its relay's first line was hpcburst-relay/1 <UID> (hostdial.Serve).
	if ctl.Sessions < 1 || srv.Sessions < 1 {
		add(checkPreamble, false, "no relay's first line was its preamble on %s (the last ends: control %s; serving %s)", missingChannels(ctl, srv), endText(ctl), endText(srv))
	} else {
		add(checkPreamble, true, "each relay's first line was hpcburst-relay/1 <the Allocation's UID>, with nothing before it (control %d session(s) of %d start(s), serving %d of %d)", ctl.Sessions, ctl.Starts, srv.Sessions, srv.Starts)
	}

	// The epoch: every relay carried the Lease's E, and a relay of E-1 was refused as superseded with the kept one up.
	var ep []string
	for _, l := range o.Lines {
		if lineArg(lineWords(l), "--epoch") != strconv.FormatInt(o.Epoch, 10) {
			ep = append(ep, "a relay ran with --epoch "+lineArg(lineWords(l), "--epoch")+", not the Lease's "+strconv.FormatInt(o.Epoch, 10))
			break
		}
	}
	switch {
	case !o.Stale.Ran:
		ep = append(ep, "the relay of the lower epoch was not run")
	case o.Stale.Exit != hostdial.ExitSuperseded || o.Stale.Stdout != 0:
		ep = append(ep, fmt.Sprintf("the relay of epoch %d was not refused as superseded (exit %d, %d byte(s) on its output: %s)", o.Epoch-1, o.Stale.Exit, o.Stale.Stdout, o.Stale.Classify))
	case ctl.PID == 0 || ctl.PID != o.KeptPID || ctl.Starts != o.KeptStarts:
		ep = append(ep, "the kept control relay did not stay up across the refusal")
	}
	if len(ep) > 0 {
		add(checkEpoch, false, "%s", strings.Join(ep, "; "))
	} else {
		add(checkEpoch, true, "both relays carried epoch %d; a control relay of epoch %d was refused as superseded, wrote nothing, and the kept session stayed up", o.Epoch, o.Epoch-1)
	}

	// The 256 byte values, each way, over the serving session.
	var by []string
	toHost := false
	for _, x := range o.Exchanges {
		if len(x.Received) == 256 && x.InPlace && sha256Hex(x.Received) == ascendingSHA256 {
			toHost = true
		}
	}
	if !toHost {
		by = append(by, "node to host: the 256 values did not arrive each in its place")
	}
	switch p := o.Probe; {
	case p == nil:
		by = append(by, "host to node: the probe did not run")
	case !p.Connected || p.GotBytes != 256 || p.GotSHA256 != descendingSHA256:
		by = append(by, fmt.Sprintf("host to node: %d byte(s) arrived, not the 256 values in descending order", p.GotBytes))
	}
	if len(by) > 0 {
		add(checkBytes, false, "%s", strings.Join(by, "; "))
	} else {
		add(checkBytes, true, "all 256 byte values crossed the serving session intact both ways: node to host each in its place (sha256 %s...), host to node in descending order (sha256 %s...)", ascendingSHA256[:8], descendingSHA256[:8])
	}
	return out
}

// endText is how a relay last ended, in the owner's words, or "none".
func endText(r hostdial.RelayState) string {
	switch {
	case r.Final:
		return r.FinalCause
	case r.LastEnd != "":
		return r.LastEnd
	case r.LastRefusal != "":
		return "not started: " + r.LastRefusal
	}
	return "none"
}

func missingChannels(ctl, srv hostdial.RelayState) string {
	var m []string
	if ctl.Sessions < 1 {
		m = append(m, "control")
	}
	if srv.Sessions < 1 {
		m = append(m, "serving")
	}
	return strings.Join(m, " and ")
}

// topDir names where a path is, for the text to post, without the path: /dev/shm, or its first directory when that is a
// short word of lower-case letters (/tmp, /scratch); else a phrase. A job's TMPDIR can hold the job's ID and the user's
// name, which are never posted; the path itself is in the private record.
func topDir(p string) string {
	if p == "/dev/shm" || strings.HasPrefix(p, "/dev/shm/") {
		return "under /dev/shm"
	}
	first, _, _ := strings.Cut(strings.TrimPrefix(p, "/"), "/")
	if regexp.MustCompile(`^[a-z]{1,16}$`).MatchString(first) {
		return "under /" + first
	}
	return "under a top directory this row does not print"
}

// stepTmpDir says how an --overlap step's TMPDIR compares with the agent's (the socket directory's parent).
func stepTmpDir(p *probeReport, agentTmp string) string {
	switch {
	case !p.TmpDirSet:
		return "unset"
	case path.Clean(p.TmpDir) == agentTmp:
		return "set, the same"
	}
	return "set, another (" + topDir(path.Clean(p.TmpDir)) + ")"
}

// hostDialedPosts is the text to post: the verdict, a line per check, what was not run and the job's shape.
func hostDialedPosts(o hostDialedObs, checks []hostDialedCheck, qos string, gpus, cpus int32, memory string) string {
	verdict := "PASS"
	for _, c := range checks {
		if !c.OK {
			verdict = "FAIL"
		}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %s (posts to %s)\n", o.Row.Name, verdict, o.Row.PostsTo)
	for _, c := range checks {
		word := "ok"
		if !c.OK {
			word = "FAIL"
		}
		fmt.Fprintf(&b, "- %s: %s: %s\n", c.Name, word, c.Text)
	}
	fmt.Fprintf(&b, "- frps and frpc: not run: the agent starts frpc only for an Instance, which this job has none of (%s hosts none); the stack rows prove that path\n", o.Row.Partition)
	fmt.Fprintf(&b, "- from a compute node of partition %s, qos %s; one job of %d GPU, %d CPU and %s, time limit %s, with no requeue\n", o.Row.Partition, qos, gpus, cpus, memory, fmtLimit(hostDialedTimeLimit))
	return b.String()
}

// fmtLimit is a time limit as the text to post words it: "5 min".
func fmtLimit(limit string) string {
	d, err := time.ParseDuration(limit)
	if err != nil {
		return limit
	}
	return fmt.Sprintf("%d min", int(d/time.Minute))
}

// failedChecks names the checks that did not hold.
func failedChecks(checks []hostDialedCheck) []string {
	var f []string
	for _, c := range checks {
		if !c.OK {
			f = append(f, c.Name)
		}
	}
	return f
}
