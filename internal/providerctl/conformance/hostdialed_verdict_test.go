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

// hostDialedObs is what one run saw: the facts the verdict is made of, read from the relay owner, the backend, the
// responder, the wrappers' record and the probe. Paths and the job ID are private, kept for the record only.
type hostDialedObs struct {
	Row       alpha.Row
	Epoch     int64
	JobID     string
	UID       string
	Tag       string
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
	// Kept are the owner's relays just before the stale pair ran, and Stale the stale pair's relays as they ended, by
	// channel (staleRelays); nil when the pair did not run.
	Kept, Stale map[agentapi.RelayChannel]hostdial.RelayState
	// StopServed is set when the agent's own poll was served the stop, and StopAcked when the agent reported after it
	// (stopWatch); EndState is the job's end state as the scheduler records it ("" when it was not read).
	StopServed, StopAcked bool
	EndState              string
	// AgentTail is the last bytes of the agent's output, job.err (private: the post takes its last error through
	// postSafe).
	AgentTail string
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
		add(checkControl, false, "did not come up: the job ended %s before the agent registered, so the agent stopped at its start; the agent's last error: %s",
			endStateText(o.EndState), postSafe(lastAgentError(o.AgentTail), o))
	case o.Registered < 1:
		add(checkControl, false, "did not come up: no registration of the agent reached the agent API over it (%d relay start(s), the last end: %s)", ctl.Starts, postSafe(endText(ctl), o))
	case o.JobEnded:
		add(checkControl, false, "came up and the agent registered, but the job ended %s before it was told to stop", endStateText(o.EndState))
	case !o.StopServed:
		add(checkControl, false, "came up and the agent registered, but the stop was never served to the agent's poll over it")
	case !o.StopAcked:
		add(checkControl, false, "the stop was served to the agent's poll over it, but the agent sent no report after it (the job ended %s)", endStateText(o.EndState))
	case o.EndState != "COMPLETED":
		add(checkControl, false, "the agent took the stop over it and reported, but the job ended %s, not COMPLETED with exit 0", endStateText(o.EndState))
	default:
		add(checkControl, true, "up: the agent registered over the control relay (%d registration(s), the recorded job); the stop was served to its poll over the same session, it sent its last report, and the job ended COMPLETED", o.Registered)
	}

	// The serving session: the probe's exchange crossed it.
	switch {
	case srv.Sessions < 1 || srv.Joined < 1 || len(o.Exchanges) == 0:
		why := "no session came up"
		if srv.Sessions >= 1 {
			why = "a session came up but no stream from the job reached the serving port"
		}
		add(checkServing, false, "did not come up: %s (%d relay start(s), the last end: %s)", why, srv.Starts, postSafe(endText(srv), o))
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
			bad = append(bad, "a "+postSafe(channelOf(w), o)+" relay ran a line that is not srun -u --overlap --jobid=<the job> --ntasks=1 <the agent> relay")
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
		// Affirmative, from two records the agent made: the socket's path in the rendezvous file and the TMPDIR its own
		// line names. A record that is missing or unreadable is a failure, never a pass (the review of PR 306, B4).
		sockDir := path.Dir(p.Socket)
		agentTmp := p.AgentTmpDir
		var why []string
		switch {
		case !p.AgentLogOK:
			why = append(why, "the agent's output, job.err, could not be read")
		case agentTmp == "" || !path.IsAbs(agentTmp):
			why = append(why, "the agent's output does not name the TMPDIR it listens under")
		}
		if !path.IsAbs(p.Socket) || path.Base(p.Socket) != "relay.sock" || !regexp.MustCompile(`^hb-relay-[0-9]+$`).MatchString(path.Base(sockDir)) {
			why = append(why, "the rendezvous file did not name a relay.sock in an hb-relay-<random> directory")
		} else if len(why) == 0 && path.Dir(sockDir) != path.Clean(agentTmp) {
			why = append(why, "the socket's directory is "+topDir(path.Dir(sockDir))+", not right under the job's $TMPDIR ("+topDir(path.Clean(agentTmp))+")")
		}
		if p.SocketDirMode != "700" {
			why = append(why, "the socket's directory is mode "+p.SocketDirMode+", not 700")
		}
		if len(why) > 0 {
			add(checkRendezvous, false, "%s", strings.Join(why, "; "))
			break
		}
		add(checkRendezvous, true, "the agent's socket was in a mode-700 hb-relay-<random> directory right under the job's $TMPDIR (%s), as the rendezvous file and the agent's own line agree, and both relays, --overlap steps, reached it; an --overlap step's $TMPDIR: %s",
			topDir(path.Clean(agentTmp)), stepTmpDir(p, path.Clean(agentTmp)))
	}

	// The preamble: a session counts only once its relay's first line was hpcburst-relay/1 <UID> (hostdial.Serve).
	if ctl.Sessions < 1 || srv.Sessions < 1 {
		add(checkPreamble, false, "no relay's first line was its preamble on %s (the last ends: control %s; serving %s)", missingChannels(ctl, srv), postSafe(endText(ctl), o), postSafe(endText(srv), o))
	} else {
		add(checkPreamble, true, "each relay's first line was hpcburst-relay/1 <the Allocation's UID>, with nothing before it (control %d session(s) of %d start(s), serving %d of %d)", ctl.Sessions, ctl.Starts, srv.Sessions, srv.Starts)
	}

	// The epoch: every relay carried the Lease's E, and a relay of E-1 was refused as superseded with the kept one up.
	var ep []string
	for _, l := range o.Lines {
		if lineArg(lineWords(l), "--epoch") != strconv.FormatInt(o.Epoch, 10) {
			ep = append(ep, "a relay ran with --epoch "+postSafe(lineArg(lineWords(l), "--epoch"), o)+", not the Lease's "+strconv.FormatInt(o.Epoch, 10))
			break
		}
	}
	// The stale pair: epoch E-1, each sequence above its kept relay's, so that only the epoch can refuse it (B1).
	for _, ch := range agentapi.RelayChannels {
		st, ok := o.Stale[ch]
		kept := o.Kept[ch]
		switch {
		case !ok:
			ep = append(ep, "the "+string(ch)+" relay of epoch "+strconv.FormatInt(o.Epoch-1, 10)+" was not run")
		case st.Epoch != o.Epoch-1:
			ep = append(ep, fmt.Sprintf("the stale %s relay carried epoch %d, not %d", ch, st.Epoch, o.Epoch-1))
		case st.Seq <= kept.Seq:
			ep = append(ep, fmt.Sprintf("the stale %s relay's sequence was not above the kept one's, so the epoch was not all that could refuse it", ch))
		case !st.Final || !strings.HasPrefix(st.FinalCause, "superseded") || st.Sessions != 0:
			ep = append(ep, fmt.Sprintf("the %s relay of epoch %d, with the higher sequence, was not refused as superseded before its preamble (%d session(s), its end: %s)", ch, o.Epoch-1, st.Sessions, postSafe(endText(st), o)))
		}
	}
	if keptCtl := o.Kept[agentapi.RelayControl]; o.Stale != nil && (ctl.PID == 0 || ctl.PID != keptCtl.PID || ctl.Starts != keptCtl.Starts) {
		ep = append(ep, "the kept control relay did not stay up across the refusal")
	}
	if len(ep) > 0 {
		add(checkEpoch, false, "%s", strings.Join(ep, "; "))
	} else {
		add(checkEpoch, true, "both relays carried epoch %d; a relay pair of epoch %d, each with a higher sequence than the kept relay's, was refused as superseded on both channels before any preamble, and the kept sessions stayed up", o.Epoch, o.Epoch-1)
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

var (
	pathWord   = regexp.MustCompile(`/[^\s"'),;]+`)
	dottedName = regexp.MustCompile(`[A-Za-z0-9-]+(\.[A-Za-z0-9-]+){2,}`)
	longNumber = regexp.MustCompile(`[0-9]{5,}`)
)

// postSafe is a text of the relay owner's, the relay's or a tool's, made fit for the text to post, which never holds a job
// ID, a host, a path or a user's name (the runbook): the run's own job ID, Allocation UID and tag and the agent's path
// become placeholders, then any other path, dotted name or number of five digits or more does too; what is left is held to
// printable ASCII and 160 bytes. The text as it was goes to the private log, never here.
func postSafe(s string, o hostDialedObs) string {
	for _, r := range []struct{ v, with string }{{o.AgentPath, "<the agent>"}, {o.Tag, "<the tag>"}, {o.UID, "<the UID>"}, {o.JobID, "<the job>"}} {
		if r.v != "" {
			s = strings.ReplaceAll(s, r.v, r.with)
		}
	}
	s = pathWord.ReplaceAllString(s, "<a path>")
	s = dottedName.ReplaceAllString(s, "<a name>")
	s = longNumber.ReplaceAllString(s, "<a number>")
	b := make([]byte, 0, len(s))
	for i := 0; i < len(s) && len(b) < 160; i++ {
		if c := s[i]; c >= 0x20 && c <= 0x7e {
			b = append(b, c)
		}
	}
	return string(b)
}

// endStateText words the job's end state for a failed line: "FAILED", or "in a state that was not read".
func endStateText(state string) string {
	if !regexp.MustCompile(`^[A-Z_]{1,24}$`).MatchString(state) {
		return "in a state that was not read"
	}
	return state
}

// lastAgentError is the agent's last error in the tail of its output: the msg and the err of its last ERROR line
// (slog's text form), or the last line when it logged none, or "none read".
func lastAgentError(tail string) string {
	lines := strings.Split(strings.TrimRight(tail, "\n"), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		l := lines[i]
		if !strings.Contains(l, "level=ERROR") {
			continue
		}
		msg, err := slogField(l, "msg"), slogField(l, "err")
		if err != "" {
			return msg + ": " + err
		}
		return msg
	}
	if last := strings.TrimSpace(lines[len(lines)-1]); last != "" {
		return last
	}
	return "none read"
}

// slogField is the value of key in a line of slog's text form, unquoted, or "".
func slogField(line, key string) string {
	i := strings.Index(line, " "+key+"=")
	if i < 0 {
		return ""
	}
	v := line[i+len(key)+2:]
	if strings.HasPrefix(v, `"`) {
		if u, err := strconv.QuotedPrefix(v); err == nil {
			if s, err := strconv.Unquote(u); err == nil {
				return s
			}
		}
		return ""
	}
	if j := strings.IndexByte(v, ' '); j >= 0 {
		v = v[:j]
	}
	return v
}

// endText is how a relay last ended, in the owner's words, or "none". It can name the job and paths: the text to post
// takes it through postSafe.
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
