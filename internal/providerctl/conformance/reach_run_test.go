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
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

//go:embed testdata/reach-probe.sh
var reachProbeScript string

// The probe's own limits, in seconds, rendered into the job: the connect, and each read of what answers. They are
// parameters of the script so that a test of it can shorten them; the live run uses these.
const (
	reachConnectSeconds = 5
	reachReadSeconds    = 3
)

// reachEndWait is how long the job may take after it has started: its time limit, the harness's 5 minutes. The
// probe itself ends in under half a minute. hack/live-checks.sh's timeout for the rows covers the start wait and
// this (qos_test.go compares them).
const reachEndWait = 5 * time.Minute

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
	if p.host != "" && p.label != "" && strings.Contains(p.label, p.host) {
		bad = append(bad, prefix+"LABEL holds the host; the label is what is posted")
	}
	if p.port != 0 && p.label != "" && strings.Contains(p.label, strconv.Itoa(p.port)) {
		bad = append(bad, prefix+"LABEL holds the port; the label is what is posted")
	}
	if len(bad) > 0 {
		return probeTarget{}, fmt.Errorf("%s", strings.Join(bad, "; "))
	}
	return p, nil
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
	a := w.newAlloc(name)
	tag := v1alpha1.JobTag(a.Namespace, a.Name, a.UID)
	// The job waits in the queue (the first clock), runs, and ends by itself. The rows have a start-wait bound: past
	// it the job is cancelled by its tag and the run is NOT RUN, with no text to post (startwait_test.go).
	started := func(a *v1alpha1.Allocation) bool { return accountingStarted(t, a) }
	if w.s.startWait > 0 {
		w.untilRuns("the job", name, "the job runs", w.at(), false, started)
	} else {
		w.until(name, "the job starts", w.s.pendingMax, started)
	}
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
	// The faults of this row are the stand-ins' (the caller starts them); the one the world has is the owner's job.
	if fault != "" && fault != "slot-held" {
		t.Fatalf("the dry fault %q is not one of the row's", fault)
	}
	target, err := readProbeTarget(os.Getenv, dryProbePrefix)
	if err != nil {
		t.Fatal(err)
	}
	if !loopbackOnly(target.host) {
		t.Fatal("a dry run connects only to a loopback address or a name under .invalid")
	}
	w, _ := dryWorld(t, r, fault, os.Getenv("HPCBURST_ALPHA_OUT"), dryShape{cpus: "1", memory: "1Gi"}, target.agent(reachConnectSeconds, reachReadSeconds), nil)
	reachBody(t, w, row, target)
}
