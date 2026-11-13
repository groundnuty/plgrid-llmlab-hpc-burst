package alpha

// beta-athena-now-router (issue #521; beta's tracker is #522): the Envoy router's first run on a compute node, on Athena's
// plgrid-now, under profiles/athena-now.yaml, as row A's world with `hpcburst up --instance-router envoy` (HBEP-5, Inside an
// Instance; HBEP-7, Several workers). One job of one GPU, one worker behind the router. What it asks of the node, and of the
// path, that no loopback run can say:
//   - Envoy runs bare in the job, from the build the Provider staged by its pinned digest: a process of the job whose command
//     is envoy and whose ancestors hold no Apptainer, and whose executable is the pinned digest's file, read on the node;
//   - the node's glibc, version only (HBEP-5:133 asks for it): read on the node by one srun step of getconf and, from the
//     other side, from the agent's own log, which says why a refused start was refused. The two are read so that the NOT RUN
//     below does not depend on which comes first, since the agent starts Envoy at the Instance's start instruction;
//   - the router in the path: spec.launch.router is envoy, status.healthyWorkers is 1 (the controller reads it from Envoy's own
//     /clusters, so Envoy is running and holds the worker healthy), before the engine is killed and again once it is back;
//   - S13's cases through the real path, as a RESPONSE-LEVEL reading on real vLLM and not as bytes (the science agent's ruling
//     on #521, answer 1): the engine's received bytes cannot be read from vLLM, so what is judged is that the router never makes
//     a failure of its own. Exact bytes are CI's (#470, internal/agent/router_bytes_test.go). The post says so;
//   - one restart at the worker rung, with the router in front: the gate of HBEP-7.
//
// NOT RUN, exit 3, never a pass and never a failure: a node whose glibc is under 2.30 (ErrGlibcTooOld), naming the version.
// Envoy cannot run bare there, which is itself the evidence HBEP-5:133 asks for; running Envoy from an image is the route for
// such a node, and a separate decision.

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// BetaTracker is the issue the beta rows post their results to (#521, comment 6857397593). It is open; a beta row never posts to a
// closed issue (TestNoRowPostsToAClosedIssue).
const BetaTracker = "#522"

// BetaRouterName is the router row's name in the script.
const BetaRouterName = "beta-athena-now-router"

// BetaRouterRow is the router row: row A's hold of plgrid-now's one-job slot (one job, one GPU, a start wait of 10 minutes, a lease of
// 48 and two minutes of cleanup, the owner's go of plgrid-now), under athena-now.yaml, posting to the beta tracker.
func BetaRouterRow() Row {
	r := NowServeRow()
	r.Name, r.PostsTo = BetaRouterName, BetaTracker
	return r
}

// GlibcVersion is a glibc version, major and minor.
type GlibcVersion struct{ Major, Minor int }

// MinEnvoyGlibc is the glibc Envoy 1.40.2's pinned release needs on both pinned architectures (HBEP-5:133; the symbol versions the
// agent reads, internal/agent/envoyglibc.go).
var MinEnvoyGlibc = GlibcVersion{2, 30}

func (v GlibcVersion) String() string { return fmt.Sprintf("%d.%d", v.Major, v.Minor) }

// Less is whether v is older than w.
func (v GlibcVersion) Less(w GlibcVersion) bool {
	return v.Major < w.Major || (v.Major == w.Major && v.Minor < w.Minor)
}

var (
	// getconfGlibc is getconf GNU_LIBC_VERSION's answer, "glibc 2.34".
	getconfGlibc = regexp.MustCompile(`^glibc (\d+)\.(\d+)\s*$`)
	// agentGlibcRefusal is the clause of the agent's refusal that names the node's glibc (internal/agent/envoyglibc.go).
	agentGlibcRefusal = regexp.MustCompile(`Envoy needs glibc \d+\.\d+ or later, and this node's glibc is (\d+)\.(\d+)`)
)

func versionOf(maj, minor string) (GlibcVersion, bool) {
	a, err1 := strconv.Atoi(maj)
	b, err2 := strconv.Atoi(minor)
	if err1 != nil || err2 != nil {
		return GlibcVersion{}, false
	}
	return GlibcVersion{a, b}, true
}

// ParseGetconfGlibc reads the version off getconf GNU_LIBC_VERSION's output, which is one line.
func ParseGetconfGlibc(out []byte) (GlibcVersion, bool) {
	m := getconfGlibc.FindStringSubmatch(strings.TrimSpace(string(out)))
	if m == nil {
		return GlibcVersion{}, false
	}
	return versionOf(m[1], m[2])
}

// ParseGlibcRefusal reads the node's glibc off the agent's own log, where a refused router start says it: the agent refused Envoy on
// this node because its glibc is that. A log with no such clause says nothing.
func ParseGlibcRefusal(log string) (GlibcVersion, bool) {
	m := agentGlibcRefusal.FindStringSubmatch(log)
	if m == nil {
		return GlibcVersion{}, false
	}
	return versionOf(m[1], m[2])
}

// ErrGlibcTooOld is what the scenario returns when the compute node's glibc is older than Envoy needs, after it cancelled the run's
// jobs by their tags. It ends the run NOT RUN (NotRun): Envoy cannot run bare on the node, so the router was never put to the test.
var ErrGlibcTooOld = errors.New("the compute node's glibc is older than the one Envoy needs, and the run's jobs were cancelled by their tags")

// GlibcTooOldError is ErrGlibcTooOld with the version and where it was read.
type GlibcTooOldError struct {
	Version GlibcVersion
	// FromAgent says the version came from the agent's log of a refused start, not from the probe.
	FromAgent bool
}

func (e *GlibcTooOldError) Error() string {
	return fmt.Sprintf("%v (glibc %s, read %s)", ErrGlibcTooOld, e.Version, e.source())
}

func (e *GlibcTooOldError) source() string {
	if e.FromAgent {
		return "from the agent's own log"
	}
	return "on the node"
}

// Is makes the error ErrGlibcTooOld.
func (e *GlibcTooOldError) Is(target error) bool { return target == ErrGlibcTooOld }

// glibcTooOldWords is the marker's fact for a node under the minimum, read by hack/live-checks.sh.
const glibcTooOldWords = "the compute node's glibc is older than the 2.30 Envoy needs"

// GlibcOf is the version the run read, from the probe or from the agent's log, or false when neither said. When they differ the older
// one counts: the probe reads the node's glibc, the agent's refusal says what the agent saw, and a refusal is a refusal.
func GlibcOf(r GlibcReads) (GlibcVersion, bool) {
	switch {
	case r.Probe.OK && r.Agent.OK:
		if r.Agent.Version.Less(r.Probe.Version) {
			return r.Agent.Version, true
		}
		return r.Probe.Version, true
	case r.Probe.OK:
		return r.Probe.Version, true
	case r.Agent.OK:
		return r.Agent.Version, true
	}
	return GlibcVersion{}, false
}

// GlibcRead is one reading of the glibc: whether it was made, and what it said.
type GlibcRead struct {
	OK      bool         `json:"ok"`
	Version GlibcVersion `json:"version"`
}

// GlibcReads are the run's two readings of the node's glibc.
type GlibcReads struct {
	Probe GlibcRead `json:"probe"`
	Agent GlibcRead `json:"agent"`
}

// TooOld returns the NOT RUN error when either reading is under the minimum, else nil.
func (r GlibcReads) TooOld() error {
	if r.Agent.OK && r.Agent.Version.Less(MinEnvoyGlibc) {
		return &GlibcTooOldError{Version: r.Agent.Version, FromAgent: true}
	}
	if r.Probe.OK && r.Probe.Version.Less(MinEnvoyGlibc) {
		return &GlibcTooOldError{Version: r.Probe.Version}
	}
	return nil
}

// EnvoyObs is what the run read of Envoy on the node.
type EnvoyObs struct {
	// Procs counts the processes of the job whose command is envoy; Contained is true when one of them has an Apptainer
	// process among its ancestors, which would not be bare.
	Procs     int  `json:"procs"`
	Contained bool `json:"contained"`
	// PinnedDigest is the SHA-256 the controller stages Envoy by for the node's cpuArch (the build's pin); RunningDigest the
	// SHA-256 of the running Envoy's executable, read on the node: both lowercase hex, RunningDigest empty when it was not read.
	PinnedDigest  string `json:"pinnedDigest"`
	RunningDigest string `json:"runningDigest"`
	// Serving says the agent's own log of the job has its "router is serving" line for the router; Refused that it has "router
	// did not start".
	Serving bool `json:"serving"`
	Refused bool `json:"refused"`
}

// BytesObs is the response-level reading of S13's cases through the real path, on real vLLM (the ruling on #521). It is never bytes.
// Each variant of each case went through the front once, with the model's name in place of S13's and a small max_tokens where the
// case set none, which is what makes this a reading of the exchange and not of the bytes.
type BytesObs struct {
	// Variants is how many were sent; Expected how many the case set holds.
	Variants int `json:"variants"`
	Expected int `json:"expected"`
	// Complete counts those that ended with a whole response: status and a body that finished (a JSON document, or a stream that
	// ended with [DONE]); Status200 and Status4xx how they answered.
	Complete  int `json:"complete"`
	Status200 int `json:"status200"`
	Status4xx int `json:"status4xx"`
	// RouterMade counts the variants whose response was a failure the router or the path made: a gateway status (502, 503, 504), a
	// response that was cut or never finished, or one that is not an engine's answer. Counts only, never a body.
	RouterMade int `json:"routerMade"`
	// StreamOK and CompletionsOK and ChatOK count the 200 answers by mode and by endpoint, so that a reading in which one of them
	// never answered 200 is not a reading of it.
	StreamOK      int `json:"streamOK"`
	NonStreamOK   int `json:"nonStreamOK"`
	ChatOK        int `json:"chatOK"`
	CompletionsOK int `json:"completionsOK"`
}

// RouterObs is a run of the router row.
type RouterObs struct {
	Tags       []string       `json:"tags"`
	Deployment DeploymentSpec `json:"deployment"`
	// ReadyAt is when the Instance became ready, DrainStart its drain start from its recorded lease end.
	ReadyAt    time.Time `json:"readyAt"`
	DrainStart time.Time `json:"drainStart"`
	// LaunchRouter is the Instance's spec.launch.router, LaunchWorkers its spec.launch.workers (0 when absent, the one worker).
	LaunchRouter  string `json:"launchRouter"`
	LaunchWorkers int32  `json:"launchWorkers"`
	// HealthyAtReady and HealthyAfterRestart are status.healthyWorkers when the Instance was first seen ready and once it served
	// again after the engine kill; -1 when the field was absent.
	HealthyAtReady      int        `json:"healthyAtReady"`
	HealthyAfterRestart int        `json:"healthyAfterRestart"`
	Envoy               EnvoyObs   `json:"envoy"`
	Glibc               GlibcReads `json:"glibc"`
	// Rate is the engine's measured rate, from the one request the run sent first, through the front.
	Rate  float64  `json:"rate"`
	Bytes BytesObs `json:"bytes"`
	// WorkerRestarts counts the restarts at the worker rung, WorkerRestart their times; Ladder how far the ladder climbed.
	WorkerRestarts int          `json:"workerRestarts"`
	WorkerRestart  RestartTimes `json:"workerRestart"`
	Ladder         LadderObs    `json:"ladder"`
	// Responses counts the responses the run received through the front, Attributed those with HBEP-5's attribution headers.
	Responses  int `json:"responses"`
	Attributed int `json:"attributed"`
}

// envoyChecks: Envoy runs bare, as the pinned digest's build, and the agent says it serves.
func envoyChecks(e EnvoyObs) []string {
	var f []string
	switch {
	case e.Refused:
		f = append(f, "the agent's log says the router did not start")
	case !e.Serving:
		f = append(f, "the agent's log does not say the router is serving")
	}
	switch {
	case e.Procs != 1:
		f = append(f, "the node does not show exactly one Envoy process in the job")
	case e.Contained:
		f = append(f, "Envoy does not run bare: an Apptainer process is among its ancestors")
	}
	switch {
	case e.PinnedDigest == "":
		f = append(f, "the digest the Provider stages Envoy by was not read")
	case e.RunningDigest == "":
		f = append(f, "the running Envoy's executable was not read on the node")
	case e.RunningDigest != e.PinnedDigest:
		f = append(f, "the running Envoy is not the build the Provider staged by its pinned digest")
	}
	return f
}

// bytesChecks: the response-level reading is made, covers every variant, and the router made no failure of its own.
func bytesChecks(b BytesObs) []string {
	var f []string
	switch {
	case b.Expected <= 0 || b.Variants != b.Expected:
		return []string{"the response-level reading did not send every variant of S13's cases"}
	case b.Complete != b.Variants:
		f = append(f, "a response of the response-level reading did not finish")
	}
	if b.RouterMade > 0 {
		f = append(f, "a response of the response-level reading was a failure the router or the path made")
	}
	// A reading in which a mode or an endpoint never answered 200 is no reading of it.
	switch {
	case b.StreamOK == 0:
		f = append(f, "no streamed variant answered 200")
	case b.NonStreamOK == 0:
		f = append(f, "no non-streamed variant answered 200")
	case b.ChatOK == 0:
		f = append(f, "no chat variant answered 200")
	case b.CompletionsOK == 0:
		f = append(f, "no completions variant answered 200")
	}
	if b.Status200+b.Status4xx != b.Complete {
		f = append(f, "the response-level reading's status counts do not add up")
	}
	return f
}

// healthyChecks: the router holds the one worker healthy, before the kill and after it.
func healthyChecks(o RouterObs) []string {
	var f []string
	if o.HealthyAtReady != 1 {
		f = append(f, "status.healthyWorkers was not 1 when the Instance was first ready")
	}
	if o.HealthyAfterRestart != 1 {
		f = append(f, "status.healthyWorkers was not 1 once the Instance served again after the worker restart")
	}
	return f
}

// VerifyBetaRouter judges the router row.
//
//	hbep/0005-llmlab-compatible-front/README.md:133: Envoy runs bare, from the binary staged by digest, on a compute node.
//	hbep/0007-lifecycle-controller/README.md (Several workers): status.healthyWorkers; a worker restart at the worker rung.
//	hbep/0005-llmlab-compatible-front/README.md (Test plan, Beta, the router, Bytes): S13's cases, read at the response here.
func VerifyBetaRouter(row Row, o RouterObs, jobs []provider.Job) []string {
	f := ladderChecks(o.Ladder)
	f = append(f, deploymentChecks(row, o.Deployment)...)
	lf, _ := ledgerChecks(row, o.Tags, jobs)
	f = append(f, lf...)
	f = append(f, tagCountChecks(row, o.Tags, "the row makes exactly one Allocation")...)
	if o.ReadyAt.IsZero() {
		f = append(f, "the Instance was not seen ready")
	}
	if o.LaunchRouter != "envoy" {
		f = append(f, "the Instance's spec.launch.router is not envoy")
	}
	if o.LaunchWorkers > 1 {
		f = append(f, "the Instance has several workers; the row's deployment has one")
	}
	f = append(f, healthyChecks(o)...)
	f = append(f, envoyChecks(o.Envoy)...)
	if _, ok := GlibcOf(o.Glibc); !ok {
		f = append(f, "the node's glibc was read neither on the node nor from the agent's log")
	}
	if o.Rate <= 0 {
		f = append(f, "no request through the front was measured")
	}
	f = append(f, bytesChecks(o.Bytes)...)
	if o.Responses <= 0 {
		f = append(f, "the run recorded no response, so attribution is unchecked")
	} else if o.Attributed != o.Responses {
		f = append(f, "a response lacked its attribution headers")
	}
	f = append(f, restartChecks(o.WorkerRestarts)...)
	return dedupe(f)
}

// betaRouterPost is the router row's text to post. It says in plain words that the Bytes are a response-level reading on vLLM, and not
// bytes, and names the glibc by its version alone.
func betaRouterPost(row Row, o RouterObs, failures []string) string {
	var b strings.Builder
	glibc := "not read"
	if v, ok := GlibcOf(o.Glibc); ok {
		glibc = v.String()
	}
	fmt.Fprintf(&b, "%s\n%s\n- the router: spec.launch.router %q, status.healthyWorkers %d when ready and %d after the worker restart; Envoy %s, run bare as a process of the job from the pinned digest's build: %s; the agent says it serves: %t\n",
		verdictLine(row, failures), deploymentLine(o.Deployment), o.LaunchRouter, o.HealthyAtReady, o.HealthyAfterRestart,
		envoyProcsWord(o.Envoy), yesNo(len(envoyChecks(o.Envoy)) == 0), o.Envoy.Serving)
	fmt.Fprintf(&b, "- the compute node's glibc: %s (Envoy needs %s); read on the node: %t, from the agent's log: %t\n", glibc, MinEnvoyGlibc, o.Glibc.Probe.OK, o.Glibc.Agent.OK)
	by := o.Bytes
	fmt.Fprintf(&b, "- S13's cases through the real path, as a RESPONSE-LEVEL reading on vLLM, not bytes (vLLM cannot show the bytes it received; exact bytes are CI's): %d of %d variants sent, %d answered completely (%d with 200, %d with 4xx), %d failures made by the router or the path; 200 answers by mode: %d streamed, %d not; by endpoint: %d chat, %d completions\n",
		by.Variants, by.Expected, by.Complete, by.Status200, by.Status4xx, by.RouterMade, by.StreamOK, by.NonStreamOK, by.ChatOK, by.CompletionsOK)
	fmt.Fprintf(&b, "- responses with attribution: %d of %d; restarts at the worker rung: %d (measured engine rate %.0f tokens/s)\n", o.Attributed, o.Responses, o.WorkerRestarts, o.Rate)
	b.WriteString(restartLine(o.WorkerRestart))
	return b.String()
}

func envoyProcsWord(e EnvoyObs) string {
	if e.Procs == 1 {
		return "one process"
	}
	return fmt.Sprintf("%d processes", e.Procs)
}
