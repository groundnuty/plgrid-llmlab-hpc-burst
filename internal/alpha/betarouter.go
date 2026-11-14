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
//     a failure of its own. Exact bytes are CI's (#470, internal/agent/router_bytes_test.go). The post says so. Each variant is
//     also sent straight to the worker's engine socket on the node, and the row fails only where the two answers differ: a refusal
//     the engine gives on both paths is the deployment's, posted with the engine's error (the science agent's ruling (a) on the
//     router row, #522, comment 6874666948; routercontrol.go);
//   - one restart at the worker rung, with the router in front: the gate of HBEP-7.
//
// NOT RUN, exit 3, never a pass and never a failure: a node whose glibc is under 2.30 (ErrGlibcTooOld), naming the version.
// Envoy cannot run bare there, which is itself the evidence HBEP-5:133 asks for; running Envoy from an image is the route for
// such a node, and a separate decision.

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
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
	// ended with [DONE]); Status200, Status4xx, Status5xx and StatusOther how they answered (any other status, a 201 or a 3xx, say).
	Complete    int `json:"complete"`
	Status200   int `json:"status200"`
	Status4xx   int `json:"status4xx"`
	Status5xx   int `json:"status5xx"`
	StatusOther int `json:"statusOther"`
	// InvalidSent counts the variants S13 says vLLM refuses (its Invalid cases), InvalidRefused those answered with a 4xx.
	InvalidSent    int `json:"invalidSent"`
	InvalidRefused int `json:"invalidRefused"`
	// Failed names, by S13's variant key and at most FailedKept of them, the variants that did not finish or that the router or the path
	// failed: for the private record, never the post.
	Failed []string `json:"failed,omitempty"`
	// RouterMade counts the variants whose response was a failure the router or the path made: a gateway status (502, 503, 504), a
	// response that was cut or never finished, or one that is not an engine's answer. Counts only, never a body.
	RouterMade int `json:"routerMade"`
	// PastBound counts the variants still awaited when BytesBound ran out: the harness's own bound, not a failure of the router.
	PastBound int `json:"pastBound"`
	// StreamOK and CompletionsOK and ChatOK count the 200 answers by mode and by endpoint, so that a reading in which one of them
	// never answered 200 is not a reading of it.
	StreamOK      int `json:"streamOK"`
	NonStreamOK   int `json:"nonStreamOK"`
	ChatOK        int `json:"chatOK"`
	CompletionsOK int `json:"completionsOK"`
}

// S13VLLMVersion is the vLLM S13's cases were enumerated and validated against (spikes/s13/README.md; fields/vllm-v0.32.0.tsv). The post
// names it beside the engine's own version, as information; the direct-to-engine control, not S13's own setup, says which refusals are the
// engine's.
const S13VLLMVersion = "v0.32.0"

// engineVersionRE is the shape of a version the post may carry: what vLLM reports, "0.32.0" or "0.32.1.dev3+gabc123".
var engineVersionRE = regexp.MustCompile(`^[0-9A-Za-z.+_-]{1,40}$`)

// EngineVersionOf reads the engine's version off an Instance's verification record's data (the agent's verification, which asks the
// engine): "" when the record has none, or one of a shape no post should carry.
func EngineVersionOf(raw []byte) string {
	var d struct {
		EngineVersion string `json:"engineVersion"`
	}
	if json.Unmarshal(raw, &d) != nil || !engineVersionRE.MatchString(d.EngineVersion) {
		return ""
	}
	return d.EngineVersion
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
	// Control is the direct-to-engine control: S13's variants straight to the worker's engine socket on the node, compared with the
	// front's answers (routercontrol.go).
	Control ControlObs `json:"control"`
	// EngineVersion is the engine's vLLM version from the Instance's verification record, "" when it was not read: posted beside
	// S13VLLMVersion, never judged.
	EngineVersion string `json:"engineVersion"`
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

// bytesChecks: the response-level reading is made, covers every variant, and the router made no failure of its own. endedAlike is how many
// of the front's answers that did not finish the control found ended alike by the engine's own error event on both paths
// (ControlObs.EndedAlike): the engine's ending, not the path's, so the completeness check discounts exactly those.
func bytesChecks(b BytesObs, endedAlike int) []string {
	var f []string
	switch {
	case b.Expected <= 0 || b.Variants != b.Expected:
		return []string{"the response-level reading did not send every variant of S13's cases"}
	case b.Complete+endedAlike != b.Variants:
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
	if b.Status200+b.Status4xx+b.Status5xx+b.StatusOther != b.Complete {
		f = append(f, "the response-level reading's status counts do not add up")
	}
	if b.InvalidSent <= 0 || b.InvalidRefused != b.InvalidSent {
		f = append(f, "a variant S13 says vLLM refuses was not refused with a 4xx")
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
	f = append(f, bytesChecks(o.Bytes, o.Control.EndedAlike)...)
	f = append(f, controlChecks(o.Bytes, o.Control)...)
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
	fmt.Fprintf(&b, "- S13's cases through the real path, as a RESPONSE-LEVEL reading on vLLM, not bytes (vLLM cannot show the bytes it received; exact bytes are CI's): %d of %d variants sent, %d answered completely (%d with 200, %d with 4xx, %d with 5xx, %d with another status), %d failures made by the router or the path, %d still awaited at the harness's own bound of %s; 200 answers by mode: %d streamed, %d not; by endpoint: %d chat, %d completions\n",
		by.Variants, by.Expected, by.Complete, by.Status200, by.Status4xx, by.Status5xx, by.StatusOther, by.RouterMade, by.PastBound, BytesBound, by.StreamOK, by.NonStreamOK, by.ChatOK, by.CompletionsOK)
	engine := o.EngineVersion
	if engine == "" {
		engine = "not read"
	}
	fmt.Fprintf(&b, "- the engine's vLLM version: %s; S13's cases were enumerated against vLLM %s\n", engine, S13VLLMVersion)
	b.WriteString(controlLines(o.Control))
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

// FailedKept is how many failed variants' keys the record keeps.
const FailedKept = 20

// Answer is one variant's response, as the live flow hands it to Add: chat says the variant went to the chat endpoint (else completions),
// stream that it was streamed, valid that S13 says vLLM accepts it, key names it,
// and status, body and cut are what the front answered (status 0 is no answer, cut that the body ended early). PastBound says the response
// was still awaited when the reading's own bound, BytesBound, ran out: the harness's bound, which is no failure the router made.
type Answer struct {
	Chat, Stream, Valid bool
	Key                 string
	Status              int
	Body                []byte
	Cut, PastBound      bool
}

// Add counts one variant's response into the reading. It is the one place the reading's counts are made, so that the live flow only
// carries a response to it.
func (b *BytesObs) Add(a Answer) {
	b.Variants++
	if !a.Valid {
		b.InvalidSent++
		if a.Status >= 400 && a.Status < 500 && !a.PastBound {
			b.InvalidRefused++
		}
	}
	if a.PastBound {
		b.PastBound++
		b.keepFailed(a.Key)
		return
	}
	cls := ClassifyReply(a.Status, a.Body, a.Stream, a.Cut)
	if cls.Complete {
		b.Complete++
		switch {
		case a.Status == 200:
			b.Status200++
			if a.Stream {
				b.StreamOK++
			} else {
				b.NonStreamOK++
			}
			if a.Chat {
				b.ChatOK++
			} else {
				b.CompletionsOK++
			}
		case a.Status >= 400 && a.Status < 500:
			b.Status4xx++
		case a.Status >= 500:
			b.Status5xx++
		default:
			b.StatusOther++
		}
	}
	if cls.RouterMade {
		b.RouterMade++
	}
	if !cls.Complete || cls.RouterMade {
		b.keepFailed(a.Key)
	}
}

// keepFailed keeps a failed variant's key for the record, up to FailedKept of them.
func (b *BytesObs) keepFailed(key string) {
	if len(b.Failed) < FailedKept {
		b.Failed = append(b.Failed, key)
	}
}

// The router row's waits, counted by the budget (budget.go): BytesBound bounds the whole response-level reading, and RouterHealthyWait the
// wait for status.healthyWorkers to be 1 again once the Instance serves after the worker restart.
const (
	BytesBound        = 10 * time.Minute
	RouterHealthyWait = time.Minute
)

// bytesMaxTokens is the generation bound the reading puts on a variant whose case sets none, so that every variant finishes in
// seconds on a real engine. It changes the bytes, which is why the reading is a response-level one.
const bytesMaxTokens = 8

// s13Model matches the model member S13's bodies carry, as written: compact, or with the spaces of the pretty-printed case. Its value is the
// one thing replaced.
var s13Model = regexp.MustCompile(`("model"\s*:\s*)"s13-model"`)

// AdaptBody is a variant's body as the reading sends it: S13's model name replaced by the deployment's, and, where the case sets no
// limit on generation (neither max_tokens nor max_completion_tokens, null or not), a bound after the opening brace. Everything else is
// S13's bytes. It never fails: a body without S13's model name is sent as it is.
func AdaptBody(body []byte, model string) []byte {
	s := string(body)
	if model != "" {
		loc := s13Model.FindStringSubmatchIndex(s)
		if loc != nil {
			s = s[:loc[3]] + strconv.Quote(model) + s[loc[1]:]
		}
	}
	if !strings.Contains(s, `"max_tokens"`) && !strings.Contains(s, `"max_completion_tokens"`) {
		if i := strings.IndexByte(s, '{'); i >= 0 {
			s = s[:i+1] + `"max_tokens":` + strconv.Itoa(bytesMaxTokens) + `,` + s[i+1:]
		}
	}
	return []byte(s)
}

// envoyFailureText is what Envoy's own error responses say (its local reply texts), which the front passes on; a 5xx that says one is
// the router's, and so are the gateway statuses.
var envoyFailureText = []string{"upstream connect error", "no healthy upstream", "upstream request timeout", "delayed connect error", "connection termination"}

// ReplyClass is how the reading judges one response.
type ReplyClass struct {
	// Complete says the response finished: a status, and a body that is a whole JSON document, or a stream that ended with [DONE].
	Complete bool
	// RouterMade says the response is a failure the router or the path made: a gateway status, Envoy's own error text, or no response.
	RouterMade bool
}

// ClassifyReply judges a response to a variant. status 0 is no response. A cut or unfinished body is incomplete, and, since no engine
// answers by being cut, router-made.
func ClassifyReply(status int, body []byte, stream bool, cut bool) ReplyClass {
	if status == 0 || cut {
		return ReplyClass{RouterMade: true}
	}
	c := ReplyClass{}
	switch status {
	case 502, 503, 504:
		c.RouterMade = true
	}
	text := string(body)
	for _, e := range envoyFailureText {
		if strings.Contains(text, e) {
			c.RouterMade = true
		}
	}
	switch {
	case status == 200 && stream:
		c.Complete = strings.Contains(text, "data: [DONE]")
	default:
		c.Complete = json.Valid(body)
	}
	return c
}

// GlibcScript is the srun step that reads the node's glibc: one line, "glibc 2.34" (ParseGetconfGlibc). getconf is glibc's own, and
// reports the system's, the one Envoy's loader loads (internal/agent/envoyglibc.go).
const GlibcScript = "getconf GNU_LIBC_VERSION\n"

// PickEnvoy reads the Envoy of the run's job off a listing (ListScript): the processes of the job, or descending from one, whose program is
// named envoy, and whether any of them has an Apptainer process among its ancestors, which would not be bare. The agent starts Envoy as
// its own child, from the staged file, under the name envoy (internal/agent/envoy.go, StartEnvoy).
func PickEnvoy(procs []NodeProc) (envoys []NodeProc, contained bool) {
	t := newProcTree(procs, nil, nil)
	for _, p := range procs {
		if len(p.Argv) == 0 || filepath.Base(p.Argv[0]) != "envoy" || !t.inJob(p) {
			continue
		}
		envoys = append(envoys, p)
		cur := p
		for range len(procs) + 1 {
			q, ok := t.parent(cur)
			if !ok {
				break
			}
			if isApptainer(q.Argv) {
				contained = true
			}
			cur = q
		}
	}
	return envoys, contained
}

var (
	digitsOnly = regexp.MustCompile(`^[0-9]+$`)
	base64Only = regexp.MustCompile(`^[A-Za-z0-9+/=]+$`)
	sha256Hex  = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// EnvoyDigestScript is the srun step that reads the SHA-256 of a running Envoy's executable: the process at the PID must still be the one
// listed (the same start time and command line, as the kill's helper checks), and then its /proc/<pid>/exe is read, which is the file the
// kernel runs, not a path the agent says. It prints "digest <hex>", or one word: gone, changed or unreadable. The listed process's fields
// are checked before they are written into the script.
func EnvoyDigestScript(p NodeProc) (string, error) {
	if p.PID <= 0 || !digitsOnly.MatchString(p.Start) || !base64Only.MatchString(p.cmdline) {
		return "", errors.New("the listed process's fields are not ones a script can carry")
	}
	return `set -u
pid=` + strconv.Itoa(p.PID) + `
start=` + p.Start + `
cmd=` + p.cmdline + `
st=$(cat /proc/$pid/stat 2>/dev/null) || { echo gone; exit 0; }
rest=${st##*) }
set -- $rest
[ "${20}" = "$start" ] || { echo changed; exit 0; }
now=$(base64 -w0 < /proc/$pid/cmdline 2>/dev/null) || { echo gone; exit 0; }
[ "$now" = "$cmd" ] || { echo changed; exit 0; }
sum=$(sha256sum < /proc/$pid/exe 2>/dev/null) || { echo unreadable; exit 0; }
echo "digest ${sum%% *}"
`, nil
}

// ParseEnvoyDigest reads the digest off EnvoyDigestScript's output; false for any other word.
func ParseEnvoyDigest(out []byte) (string, bool) {
	f := strings.Fields(string(out))
	if len(f) != 2 || f[0] != "digest" || !sha256Hex.MatchString(f[1]) {
		return "", false
	}
	return f[1], true
}
