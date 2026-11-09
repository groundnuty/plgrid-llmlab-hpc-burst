package conformance

// The reach rows (issue #225): one minimal job on a compute node of each alpha platform tries a TCP connect to
// the gateway's forwarded port, and what came of it is posted. It runs through the Provider's controller like the
// accounting row (gpuacct_test.go): the job is the Allocation's agent stand-in, so it starts under the row's
// one-job slot and its start wait, NOT RUN when the slot's owner keeps it waiting, and cancelled by its tag.
//
// The host and port are private (HPCBURST_CONF_PROBE_HOST, _PORT): they are in the job's script on the cluster
// and nowhere else, never in the text to post, the test's log or the run's records. The label
// (HPCBURST_CONF_PROBE_LABEL) is the only name the text to post carries.
//
// The job writes one fixed line, <outcome> <answer> (testdata/reach-probe.sh), and the test reads it back by its
// sha256 against the lines a probe can write, as nodeGPUs does, so no byte the other side sent reaches the test.

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
)

//go:embed testdata/reach-probe.sh
var reachProbeScript string

// The probe's own limits, in seconds, rendered into the job: the connect, and each read of what answers. They are
// parameters of the script so that a test of it can shorten them; the live run uses these.
const (
	reachConnectSeconds = 5
	reachReadSeconds    = 3
)

// reachTimeLimit is the reach job's time limit. The probe ends in under half a minute (a resolver wait of 10 s, a
// connect of 5 s and two reads of 3 s), so a job that stalls holds the owner's one-job slot for at most two
// minutes, not the harness's five (issue #225; the review of PR 235). hack/live-checks.sh's shape carries it as
// 00:02:00 (TestConformanceRowsPassTheRequestCheck compares them) and the rows' Lease is the same.
const reachTimeLimit = "2m"

// reachEndWait is how long the job may take after it has started: its time limit and a minute for the scheduler to
// close it. hack/live-checks.sh's timeout for the rows covers the start wait and this (qos_test.go compares them).
const reachEndWait = 3 * time.Minute

// reachCleanupWait is what the script's timeout leaves after the job's end for the read-back and the end-of-run
// cleanup (cancel by tag, removal of the staging directory); qos_test.go compares it with the rows' timeouts.
const reachCleanupWait = 3 * time.Minute

// reachFile is the file in the Allocation's directory (agentapi.AllocationDir) that holds the result.
const reachFile = "reach"

// The setting names a live run reads, and the dry run's own (the dry wrapper of hack/livechecks renames them, so a
// dry binary that finds a live setting stops, as the accounting rows' does).
const (
	liveProbePrefix = "HPCBURST_CONF_PROBE_"
	dryProbePrefix  = "HPCBURST_ALPHA_PROBE_"
)

var (
	probeHostForm  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$`)
	probeLabelForm = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,39}$`)
)

// probeTarget is the gateway's forwarded port as the compute nodes see it.
type probeTarget struct {
	host  string
	port  int
	label string
}

// readProbeTarget reads the target from <prefix>HOST, <prefix>PORT and <prefix>LABEL. Every problem is reported
// together and names the setting, never its value. The host and the port go into a shell script, so they are held to
// the narrow form the script's own check holds them to; a label that holds either would post it.
func readProbeTarget(get func(string) string, prefix string) (probeTarget, error) {
	var p probeTarget
	var bad []string
	if v := get(prefix + "HOST"); v == "" {
		bad = append(bad, prefix+"HOST is missing")
	} else if !probeHostForm.MatchString(v) {
		bad = append(bad, prefix+"HOST must be a host name or an address (letters, digits, dots, hyphens, underscores)")
	} else {
		p.host = v
	}
	if v := get(prefix + "PORT"); v == "" {
		bad = append(bad, prefix+"PORT is missing")
	} else if n, err := strconv.Atoi(v); err != nil || n < 1 || n > 65535 || strconv.Itoa(n) != v {
		bad = append(bad, prefix+"PORT must be a port number from 1 to 65535")
	} else {
		p.port = n
	}
	if v := get(prefix + "LABEL"); v == "" {
		bad = append(bad, prefix+"LABEL is missing")
	} else if !probeLabelForm.MatchString(v) {
		bad = append(bad, prefix+"LABEL must be 1 to 40 letters, digits, dots, underscores or hyphens")
	} else {
		p.label = v
	}
	if p.host != "" && p.label != "" {
		if holds := labelHolds(p.label, p.host, p.port); holds != "" {
			bad = append(bad, prefix+"LABEL holds the "+holds+"; the label is what is posted")
		}
	}
	if len(bad) > 0 {
		return probeTarget{}, fmt.Errorf("%s", strings.Join(bad, "; "))
	}
	return p, nil
}

// labelHolds names what a public label would give away, "host" or "port" (the host first), or "" for neither. A name
// is the same name in any case, with or without the DNS root dot, and with its dots, hyphens and underscores run
// together, so the label holds the host in any of those spellings, among other characters or not
// (GATEWAY.PRIVATE.EXAMPLE for gateway.private.example); the port is its digits, also with those characters between.
// hack/live-checks.sh's check_reach_settings is the same rule.
func labelHolds(label, host string, port int) string {
	flat := strings.NewReplacer(".", "", "-", "", "_", "")
	l, h := strings.ToLower(label), strings.TrimSuffix(strings.ToLower(host), ".")
	if h != "" && (strings.Contains(l, h) || strings.Contains(flat.Replace(l), flat.Replace(h))) {
		return "host"
	}
	if p := strconv.Itoa(port); port != 0 && (strings.Contains(l, p) || strings.Contains(flat.Replace(l), p)) {
		return "port"
	}
	return ""
}

// reachClientHello is a TLS ClientHello that a TLS 1.2 or 1.3 server answers with a TLS record (a ServerHello or an
// alert): offered versions 1.3 and 1.2, the usual AEAD suites, x25519 and P-256, a key share of the curve's base
// point. It carries no server name. It is built from its fields, not typed as bytes, so that its lengths are right.
func reachClientHello() []byte {
	u16 := func(n int) []byte { return []byte{byte(n >> 8), byte(n)} }
	vec16 := func(p []byte) []byte { return append(u16(len(p)), p...) }
	vec8 := func(p []byte) []byte { return append([]byte{byte(len(p))}, p...) }
	ext := func(typ int, body []byte) []byte { return append(u16(typ), vec16(body)...) }
	key := make([]byte, 32)
	key[0] = 9
	exts := bytes.Join([][]byte{
		ext(0x000a, vec16(append(u16(0x001d), u16(0x0017)...))),                              // supported_groups: x25519, P-256
		ext(0x000b, vec8([]byte{0})),                                                         // ec_point_formats: uncompressed
		ext(0x000d, vec16(bytes.Join([][]byte{u16(0x0403), u16(0x0804), u16(0x0401)}, nil))), // signature_algorithms
		ext(0x002b, vec8(append(u16(0x0304), u16(0x0303)...))),                               // supported_versions: 1.3, 1.2
		ext(0x0033, vec16(append(u16(0x001d), vec16(key)...))),                               // key_share: x25519
	}, nil)
	suites := bytes.Join([][]byte{u16(0x1301), u16(0x1302), u16(0x1303), u16(0xc02b), u16(0xc02f), u16(0xcca9), u16(0xcca8)}, nil)
	body := bytes.Join([][]byte{u16(0x0303), bytes.Repeat([]byte{0x42}, 32), vec8(nil), vec16(suites), vec8([]byte{0}), vec16(exts)}, nil)
	hs := append([]byte{0x01, byte(len(body) >> 16), byte(len(body) >> 8), byte(len(body))}, body...)
	return append([]byte{0x16, 0x03, 0x01}, vec16(hs)...)
}

// printfEscapes writes bytes as \xHH for printf, so no literal percent sign, quote or backslash is left in the text.
func printfEscapes(b []byte) string {
	var s strings.Builder
	for _, c := range b {
		fmt.Fprintf(&s, `\x%02x`, c)
	}
	return s.String()
}

// agent is the probe script for the target, with the given limits, ready to be the Allocation's agent stand-in.
func (p probeTarget) agent(connect, read int) []byte {
	if !probeHostForm.MatchString(p.host) || p.port < 1 || p.port > 65535 {
		panic("the probe target was not read by readProbeTarget")
	}
	script := strings.NewReplacer("@HOST@", p.host, "@PORT@", strconv.Itoa(p.port), "@CONNECT_SECONDS@", strconv.Itoa(connect),
		"@READ_SECONDS@", strconv.Itoa(read), "@HELLO@", printfEscapes(reachClientHello())).Replace(reachProbeScript)
	if strings.Contains(script, "@HOST@") || regexp.MustCompile(`@[A-Z_]+@`).MatchString(script) {
		panic("the probe script has a placeholder the test does not render")
	}
	return []byte(script)
}

// The outcomes and answers the probe writes, as its header gives them.
var (
	reachOutcomes = []string{"refused", "timeout", "unresolved", "unreachable", "error"}
	reachAnswers  = []string{"ssh", "tls", "data", "silent", "unknown"}
)

// reachLine is the line the probe writes.
func reachLine(outcome, answer string) string {
	return fmt.Sprintf("outcome=%s answer=%s\n", outcome, answer)
}

// reachLines are all the lines the probe can write, by sha256, to the outcome and answer.
func reachLines() map[string][2]string {
	m := map[string][2]string{}
	add := func(outcome, answer string) {
		sum := sha256.Sum256([]byte(reachLine(outcome, answer)))
		m[hex.EncodeToString(sum[:])] = [2]string{outcome, answer}
	}
	for _, a := range reachAnswers {
		add("reachable", a)
	}
	for _, o := range reachOutcomes {
		add(o, "none")
	}
	return m
}

// reachResult reads the line the job left in the Allocation's directory. The file is read by its sha256 (the one
// file command both a login node and the fake have), so nothing in it but one of the fixed lines can reach the test.
func (w *world) reachResult(ctx context.Context, tag string) (outcome, answer string) {
	w.t.Helper()
	paths, err := w.batch.Paths(ctx)
	if err != nil {
		w.t.Fatalf("the staging directory could not be resolved: %v", err)
	}
	res, err := w.cap.Run(ctx, slurm.Command{Line: "sha256sum " + paths.Staging + "/alloc/" + tag + "/" + reachFile})
	if err != nil || res.ExitCode != 0 {
		w.t.Fatalf("the job did not leave its result (err %v, exit %d)", err, res.ExitCode)
	}
	got := strings.Fields(string(res.Stdout))
	if len(got) == 0 {
		w.t.Fatal("the result file's sha256 is empty")
	}
	line, ok := reachLines()[got[0]]
	if !ok {
		w.t.Fatal("the job's result file holds something that is not one of the probe's fixed lines")
	}
	return line[0], line[1]
}

// outcomeText is what each outcome means, in the words of the text to post. An answer proves only that something
// answers (lesson L-17.7), and the wording keeps to that.
var outcomeText = map[string]string{
	"refused":     "the connect was refused: the network from the compute node is open and nothing listens at the port",
	"timeout":     "the connect timed out: nothing came back within the limit, so the path is filtered or the packets are dropped",
	"unresolved":  "the name did not resolve on the compute node",
	"unreachable": "the network answered that there is no route from the compute node to it",
	"error":       "the connect failed in a way the probe does not name (or a tool it needs is missing)",
}

var answerText = map[string]string{
	"ssh":     "something answered with an SSH banner (that proves only that something answers there, not that it is the gateway: lesson L-17.7)",
	"tls":     "something answered a TLS ClientHello with a TLS record",
	"data":    "something answered, with data that is neither an SSH banner nor a TLS record",
	"silent":  "a bare TCP connect succeeded; nothing was sent, and nothing answered a TLS ClientHello",
	"unknown": "a bare TCP connect succeeded; the second connection that asks what answers failed",
}

// qosUsed names the QOS the row's jobs ran under for the post: the one the Provider requested, or, on a partition
// with a QOS of its own and none requested, that one.
func qosUsed(requested, partitionOwn string) string {
	switch {
	case requested != "" && partitionOwn != "" && requested != partitionOwn:
		return fmt.Sprintf("%s (requested; the partition's own is %s)", requested, partitionOwn)
	case requested != "":
		return requested + " (requested)"
	case partitionOwn != "":
		return partitionOwn + " (the partition's own; none requested)"
	}
	return "none requested"
}

// reachPosts is the text to post for a run: the label and not the host or the port, the partition and the QOS used,
// and what came of the connect.
func reachPosts(row alpha.Row, label, verdict, outcome, answer, qos string, gpus, cpus int32, memory string) string {
	what := outcomeText[outcome]
	if outcome == "reachable" {
		what = answerText[answer]
	}
	return fmt.Sprintf("%s: %s (posts to %s)\n- the gateway's forwarded port, as %s: outcome %s\n- %s\n- from a compute node of partition %s, qos %s; one job of %d GPU, %d CPU and %s with no requeue\n",
		row.Name, verdict, row.PostsTo, label, outcome, what, row.Partition, qos, gpus, cpus, memory)
}

// reachBody is the run, live or dry: one job through the Provider's controller, then the line it left.
func reachBody(t testing.TB, w *world, rowName string, target probeTarget) {
	t.Helper()
	ctx := context.Background()
	row, ok := alpha.RowByName(rowName)
	if !ok || !strings.HasPrefix(rowName, "reach-") {
		t.Fatalf("%s is not a reach row", rowName)
	}
	name := "reach"
	w.timeLimit = reachTimeLimit
	a := w.newAlloc(name)
	tag := v1alpha1.JobTag(a.Namespace, a.Name, a.UID)
	// The job waits in the queue (the first clock), runs, and ends by itself. The rows have a start-wait bound: past
	// it (or past the wait in the queue, for a run with none) the job is cancelled by its tag and the run is NOT RUN,
	// with no text to post (startwait_test.go).
	started := func(a *v1alpha1.Allocation) bool { return accountingStarted(t, a) }
	w.untilRuns("the job", name, "the job runs", w.at(), "", started)
	w.until(name, "the job ends", reachEndWait, func(a *v1alpha1.Allocation) bool {
		accountingNotFailed(t, a)
		return a.Status.AtProvider.Phase == v1alpha1.AllocationEnded
	})
	outcome, answer := w.reachResult(ctx, tag)

	verdict := "PASS"
	if outcome != "reachable" {
		verdict = "FAIL"
	}
	qos := qosUsed(w.s.qos, w.s.part.QOS)
	posts := reachPosts(row, target.label, verdict, outcome, answer, qos, w.s.gpus, w.s.cpus, w.s.memory.String())
	w.p.save(alpha.PostsFile, []byte(posts))
	w.p.save(alpha.RecordFile, []byte(fmt.Sprintf("{\"row\": %q, \"verdict\": %q, \"gateway\": %q, \"outcome\": %q, \"answer\": %q, \"partition\": %q, \"qos\": %q}\n",
		row.Name, verdict, target.label, outcome, answer, row.Partition, qos)))
	if verdict == "FAIL" {
		t.Fatalf("%s: the gateway's forwarded port (%s) was not reached: %s", row.Name, target.label, outcome)
	}
	t.Logf("%s passed; the text to post is in %s", row.Name, alpha.PostsFile)
}

// failingBash stands for bash in a connect that fails: the two lines bash prints, with the reason in FAKE_REASON and
// the target (H and P, as the probe passes them) in the second.
const failingBash = "#!/bin/sh\necho \"bash: connect: $FAKE_REASON\" >&2\necho \"bash: line 1: /dev/tcp/$H/$P: $FAKE_REASON\" >&2\nexit 1\n"

// connectFailureEnv is the job environment of a node whose connect fails the way a kernel makes it fail where no
// loopback socket can: bash is replaced, on the job's PATH, by a script that prints what bash prints for a failed
// /dev/tcp connect (the shell's own two lines, the second naming the target) with the given reason and exits 1.
// Nothing else of the node is faked: the probe, its interpreter and its other commands are the real ones, and so is
// the name's resolution before the connect. The dry faults no-route and connect-error use it.
func connectFailureEnv(t testing.TB, reason string) map[string]string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "bash"), []byte(failingBash), 0o700); err != nil {
		t.Fatal(err)
	}
	return map[string]string{"PATH": dir + ":" + os.Getenv("PATH"), "FAKE_REASON": reason}
}

// The dry faults of a reach row that are the node's, and the reason each gives the connect.
var dryReachNodeFaults = map[string]string{"no-route": "No route to host", "connect-error": "Permission denied"}

// loopbackOnly reports whether a probe target is an address of the loopback network: a dry run connects only to
// those (the stand-ins) or to a name under .invalid.
func loopbackOnly(host string) bool {
	if strings.HasSuffix(host, ".invalid") {
		return true
	}
	a, err := netip.ParseAddr(host)
	return err == nil && a.IsLoopback()
}

// dryReach is the dry run of TestReach: the world is a fake Slurm in run mode, and the job is the probe script, the
// same one a live run stages, with the target the caller's settings name. The target is the stand-in the caller
// started (a loopback address or a name under .invalid), so this run connects to nothing else: a dry run that
// found a real target would probe it from this machine, which proves nothing about a compute node.
func dryReach(t *testing.T) {
	t.Helper()
	row := os.Getenv("HPCBURST_ALPHA_ROW")
	fault := os.Getenv("HPCBURST_ALPHA_DRY_FAULT")
	r, ok := alpha.RowByName(row)
	if !ok || !strings.HasPrefix(row, "reach-") {
		t.Fatalf("HPCBURST_ALPHA_ROW is %q, not a reach row", row)
	}
	// The stand-ins are the caller's (it starts them); the faults of this row are the owner's job holding the slot
	// and a node whose connect fails as no loopback socket can make it.
	reason, nodeFault := dryReachNodeFaults[fault]
	if fault != "" && fault != "slot-held" && !nodeFault {
		t.Fatalf("the dry fault %q is not one of the row's", fault)
	}
	var jobEnv map[string]string
	if nodeFault {
		jobEnv = connectFailureEnv(t, reason)
	}
	target, err := readProbeTarget(os.Getenv, dryProbePrefix)
	if err != nil {
		t.Fatal(err)
	}
	if !loopbackOnly(target.host) {
		t.Fatal("a dry run connects only to a loopback address or a name under .invalid")
	}
	payload := target.agent(reachConnectSeconds, reachReadSeconds)
	w, plat := dryWorld(t, r, fault, os.Getenv("HPCBURST_ALPHA_OUT"), dryShape{cpus: "1", memory: "1Gi"}, payload, jobEnv)
	// The target is in the job's script and nowhere else: not in the job's output and error files, the staging, the
	// agent builds, the scheduler's copies, the records. The search runs when the test ends, before the harness's
	// own cleanup removes the staging (cleanups run last in, first out), and a failed run is searched too. It says
	// how many it looked at, so that a search of nothing shows.
	t.Cleanup(func() {
		leaks, scanned := w.targetLeaks(target, payload, plat)
		t.Logf("%s: %d file(s) and scheduler record(s) of the fake cluster, the agent builds and the records searched for the target; %d hold it outside the job's script", row, scanned, len(leaks))
		for _, l := range leaks {
			t.Errorf("the target is in %s", l)
		}
	})
	reachBody(t, w, row, target)
}

// targetLeaks searches what a dry run left for the target and returns where it is, never what is there: every file
// under the world's artefact directories and the record directory (the agent builds, the credentials, the fake
// cluster's file system with the staging and each job's output and error files, the records), and the scheduler's own
// copies of what was submitted (the job's name, script and output paths). The only place the target belongs is the
// job's script, the probe the run staged: a file whose bytes are exactly payload is that, and is skipped. The host is
// searched as it is; the port as a number of its own, and only from 1024 on, because a smaller one (the unresolved
// stand-in's 9) is a number that files hold for other reasons. It also returns how many it looked at.
func (w *world) targetLeaks(target probeTarget, payload []byte, plat *fakeslurm.Platform) (leaks []string, scanned int) {
	w.t.Helper()
	var port *regexp.Regexp
	if target.port >= 1024 {
		port = regexp.MustCompile(`\b` + strconv.Itoa(target.port) + `\b`)
	}
	holds := func(b []byte) bool {
		return bytes.Contains(b, []byte(target.host)) || (port != nil && port.Match(b))
	}
	for _, dir := range append(append([]string(nil), w.artefacts...), w.p.dir) {
		err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return err
			}
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			scanned++
			if !bytes.Equal(b, payload) && holds(b) {
				rel, _ := filepath.Rel(dir, p)
				leaks = append(leaks, filepath.Base(dir)+"/"+rel)
			}
			return nil
		})
		if err != nil {
			w.t.Fatalf("the files of the run could not be searched: %v", err)
		}
	}
	if plat != nil {
		for _, j := range plat.Jobs() {
			for field, text := range map[string]string{"name": j.Name, "script": j.Wrap, "output path": j.Output, "error path": j.Error} {
				scanned++
				if holds([]byte(text)) {
					leaks = append(leaks, fmt.Sprintf("the scheduler's record of job %d (%s)", j.ID, field))
				}
			}
		}
	}
	sort.Strings(leaks)
	return leaks, scanned
}
