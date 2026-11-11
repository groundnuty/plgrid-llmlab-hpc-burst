package alpha

// Row A of issue #376, alpha-athena-now-serve: one job of one GPU on Athena's
// plgrid-now, the owner's shared one-job slot, under the shipped test profile
// profiles/athena-now.yaml (the science agent's rulings Q1 to Q8 on #376). Its
// flow (now_flows_test.go): the Instance ready; HBEP-5's 7,000-token stream at
// the engine's own rate, its length fixed by min_tokens, under the profile's
// deadline, with attribution on every response; the Instance's deployment ID
// under its Allocation's tag (HBEP-3, ported from the rehearsal's part 3);
// HBEP-8's events and engine gauges, through internal/rehearsal/obscheck; one
// worker-rung restart (HBEP-7); and last, scontrol requeuehold of the run's own
// job by its ID (Q5).
//
// The requeue's order (the parent brief's "show how", the code agent's ruling):
// before the deployment's endTime the lifecycle controller would replace a
// Requeued failure with a new Allocation, a second job in a one-job row
// (internal/lifecycle/reconcile.go: capacity and replacements run only while
// the deployment is not shutting down). So the run sets the deployment's end to
// now, waits until the controller is seen draining the Instance with nothing
// planned beside it, checks the job is the run's own by its ID and tag, and
// only then requeues it. The failure still ends the Instance with reason
// Requeued (internal/lifecycle/instances.go: allocationFailed runs before the
// shutdown's drain), and the release of the failed Allocation cancels the held
// job (internal/providerctl/delete.go, toCancel), which frees the owner's slot.
// A stream held open across that window keeps the Instance draining until the
// requeue lands: with no request running, the drain would end the Instance as
// Drained on the agent's next report, and the requeue would then meet a job its
// release had already cancelled. That stream is the window's means, recorded and
// not judged; the requeue's kill cuts it.
//
// A requeue the scheduler refuses is NOT RUN, exit 3 (ErrRequeueRefused),
// never a pass, with the reason posted as a fixed label; the run's other checks
// are judged first, and a failure there wins.

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The row's figures.
const (
	// ServeStreamTokens is HBEP-5's long stream (hbep/0005-llmlab-compatible-front/README.md:442), run at the engine's own
	// rate with min_tokens so that its length is exact, under the deployment's deadline (the science agent's ruling Q4 on
	// #376, mirroring the tunnel hour's of 2 Nov 2026). The 1 token/s wall-clock run stays local (internal/front).
	ServeStreamTokens = 7000
	// The margins of profiles/athena-now.yaml (PR #385): drainMargin 2m, handoverMargin 1m, health.startTimeout 12m; with
	// the deployment's 5-minute routing.maxRequestDuration, a drain window of 7 minutes and a lead of 20.
	NowDrainMargin    = 2 * time.Minute
	NowHandoverMargin = time.Minute
	NowStartTimeout   = 12 * time.Minute
	// NowServeLease is row A's leaseLimit: the hour bounds the row's whole hold of the slot, start waits included (Q3),
	// so its start wait, its lease and two minutes of cleanup are at most 60 minutes: 10 + 48 + 2.
	NowServeLease = 48 * time.Minute
	// NowStartWait is the start wait of each job of a plgrid-now row (issue #376, the plan's section 3): the run's first
	// clock, past which the run cancels its jobs by their tags and is NOT RUN, so that a busy queue never holds the
	// owner's slot for long.
	NowStartWait = 10 * time.Minute
	// NowServeBudget is the row's run budget, the start wait included, and the script's timeout for it (the shape's
	// timeout column, as for the other rows with a start wait): above the row's worst case with its factor
	// (TestEachStackRowsWorstCaseIsItsWaitsAddedUp; hack/livechecks, TestEveryStackRowsTimeoutIsAboveItsWorstCase).
	NowServeBudget = 115 * time.Minute
	// RequeueTimeout bounds the scontrol call of the requeue, the ssh included.
	RequeueTimeout = time.Minute
	// RequeueResultWait bounds the wait, after an accepted requeue, for its result: the Instance ended, its Allocation
	// released and the held job ended in the cluster's record. Each is one pass of a controller, polling at most every
	// few seconds, and the cancel one scancel.
	RequeueResultWait = 5 * time.Minute
	// holdStreamMax is the longest the stream held across the requeue's window lasts at the measured rate: the window
	// itself (the end set, the drain seen, the job checked, the requeue sent) takes seconds.
	holdStreamMax = 2 * time.Minute
)

// HoldStreamFor is how long the stream held across the requeue's window lasts at the measured rate: two minutes, or two
// thirds of the deadline when that is shorter, so that the front's deadline never ends it first.
func HoldStreamFor(tm Timing) time.Duration { return min(holdStreamMax, tm.MaxRequestDuration*2/3) }

// AthenaNowTiming is the clock of the rows under profiles/athena-now.yaml: its margins and the 5-minute deadline.
func AthenaNowTiming() Timing {
	return Timing{MaxRequestDuration: MaxRequestDuration, DrainMargin: NowDrainMargin, HandoverMargin: NowHandoverMargin, StartTimeout: NowStartTimeout}
}

// NowServeRow is row A: one job, one GPU, no QOS (plgrid-now's own applies, as on the reach rows), the owner's go of
// plgrid-now (Q6), a slot hold of its start wait, its lease and two minutes (Q3).
func NowServeRow() Row {
	return Row{Name: "alpha-athena-now-serve", Cluster: "athena", Profile: "athena-now", Partition: "plgrid-now", GPUs: 1, Lease: NowServeLease,
		Budget: NowServeBudget, SlotHold: NowStartWait + NowServeLease + 2*time.Minute, StartWait: NowStartWait, MostJobs: 1,
		PostsTo: "#145 and #376", Timing: AthenaNowTiming(), OwnerGo: OwnerGoAthenaNow}
}

// The pre-staging row (issue #376; the science agent's ruling (a) and its rulings 1 to 3 of 11 Nov 2026): a job of its
// own on plgrid-now, run before row A and outside its hour, whose agent build is a test-only helper
// (internal/providerctl/conformance/stagehelper) that calls the real stager with the agent's own configuration, and so
// stages the sandbox image and the checkpoint where the agent stages, the Provider's staging directory as the profile
// names it. It starts no Instance and runs no stack. A cold build on A100 is unmeasured, so this row is the cold-start
// measurement: a staging past its lease is a FAIL that names it.
const (
	// NowStageLease is the job's time limit: the cold staging's bound.
	NowStageLease = 30 * time.Minute
	// NowStageBudget is the row's timeout, its start wait included (the conformance package's
	// TestEveryRowsTimeoutIsAboveTheWorstCaseItsSourceAddsUpTo adds it up from the test's source).
	NowStageBudget = 60 * time.Minute
)

// NowStageRow is the pre-staging row: plgrid-now under athena-now.yaml, a start wait of 10 minutes, a lease of 30 and two
// minutes for the end, a hold of 42 minutes in its own hour, and the owner's go of plgrid-now.
func NowStageRow() Row {
	return Row{Name: "alpha-athena-now-stage", Cluster: "athena", Profile: "athena-now", Partition: "plgrid-now", GPUs: 1, Lease: NowStageLease,
		Budget: NowStageBudget, SlotHold: NowStartWait + NowStageLease + 2*time.Minute, StartWait: NowStartWait, MostJobs: 1,
		PostsTo: "#145 and #376", Timing: AthenaNowTiming(), OwnerGo: OwnerGoAthenaNow}
}

// ErrRequeueRefused is what row A's scenario returns, beside what it saw, when the scheduler refused the requeue of the
// run's own job (Q5): the requeue was never put to the test. Run judges the rest of the run, and a failure there wins;
// with none, the run is NOT RUN (NotRun), its record and text to post written with the reason, never a pass.
var ErrRequeueRefused = errors.New("the scheduler refused the requeue of the run's own job")

// RequeueRefusedError is ErrRequeueRefused with the reason, a fixed label (RequeueReplyOf).
type RequeueRefusedError struct{ Reason string }

func (e *RequeueRefusedError) Error() string {
	return fmt.Sprintf("%v (%s)", ErrRequeueRefused, e.Reason)
}

// Is makes the error ErrRequeueRefused.
func (e *RequeueRefusedError) Is(target error) bool { return target == ErrRequeueRefused }

// refusalOf is the reason a refusal gives.
func refusalOf(err error) string {
	var r *RequeueRefusedError
	if errors.As(err, &r) {
		return r.Reason
	}
	return refusalOther
}

// RequeueHoldLine is the scontrol line that requeues the run's own job and holds it (Q5: requeuehold, not a plain
// requeue, which would leave the job pending without a hold, free to run a second time in the slot). jobID must be
// digits only: it is checked to be the run's by its ID and tag first (ourJob).
func RequeueHoldLine(jobID string) (string, error) {
	if _, err := strconv.ParseUint(jobID, 10, 64); err != nil || strings.TrimLeft(jobID, "0123456789") != "" {
		return "", errors.New("a job ID must be digits only")
	}
	return "scontrol requeuehold " + jobID, nil
}

// The reasons of a refused requeue, as posted: fixed labels, since the scheduler's own text can carry the job's ID.
const (
	refusalDisabled  = "the scheduler says the operation is disabled for this job"
	refusalDenied    = "the scheduler denied the operation to this user"
	refusalNotNow    = "the scheduler says the job is not one it can requeue now"
	refusalNoCommand = "the scheduler's scontrol does not take requeuehold"
	refusalOther     = "another refusal; the scheduler's text is in the private log"
)

// RequeueReply is what the scontrol call of the requeue said: accepted (exit 0), or refused, with a fixed label.
type RequeueReply struct {
	Accepted bool
	Refusal  string
}

// RequeueReplyOf reads the call's exit code and its output. A transport failure is not a reply at all (the caller's error,
// a failure of the run): only an answer of the scheduler's is a refusal.
func RequeueReplyOf(exit int, stdout, stderr []byte) RequeueReply {
	if exit == 0 {
		return RequeueReply{Accepted: true}
	}
	text := strings.ToLower(string(stdout) + " " + string(stderr))
	switch {
	case strings.Contains(text, "disabled"):
		return RequeueReply{Refusal: refusalDisabled}
	case strings.Contains(text, "permission denied") || strings.Contains(text, "access denied") || strings.Contains(text, "not authorized"):
		return RequeueReply{Refusal: refusalDenied}
	case strings.Contains(text, "already finished") || strings.Contains(text, "not pending") || strings.Contains(text, "invalid job id") ||
		strings.Contains(text, "is not running") || strings.Contains(text, "completing"):
		return RequeueReply{Refusal: refusalNotNow}
	case strings.Contains(text, "invalid keyword") || strings.Contains(text, "invalid command") || strings.Contains(text, "unrecognized") ||
		strings.Contains(text, "supports only") || strings.Contains(text, "usage"):
		return RequeueReply{Refusal: refusalNoCommand}
	}
	return RequeueReply{Refusal: refusalOther}
}

// HBEP3Tag is an Allocation's job tag computed from HBEP-3 (Allocation and job identity), here and not with the
// contracts' helper, as the rehearsal's part 3 does (internal/rehearsal/scenario, part3_requeue_test.go): hb-<n>-<h>,
// <n> the name cut to its first 40 characters, <h> the first 12 hex characters of the SHA-256 of
// "<namespace>/<name>/<uid>".
func HBEP3Tag(namespace, name, uid string) string {
	sum := sha256.Sum256([]byte(namespace + "/" + name + "/" + uid))
	n := name
	if len(n) > 40 {
		n = n[:40]
	}
	return "hb-" + n + "-" + hex.EncodeToString(sum[:])[:12]
}

// fullDeploymentID is HBEP-3's full deployment ID: 64 lowercase hex characters.
var fullDeploymentID = regexp.MustCompile(`^[0-9a-f]{64}$`)

// IdentityObs is the Instance's deployment ID under its Allocation's tag, as the run read it (HBEP-3; the rehearsal's
// part 3, checkIdentity): each field a read, never a judgement.
type IdentityObs struct {
	// Read says the run read it at all.
	Read bool `json:"read"`
	// Tag is HBEP3Tag of the Instance's Allocation; JobName the name the queue lists for the Allocation's job, by its ID.
	Tag     string `json:"tag"`
	JobName string `json:"jobName"`
	// ExternalName and ObservedTag are the Allocation's external-name annotation and its status's observed tag.
	ExternalName string `json:"externalName"`
	ObservedTag  string `json:"observedTag"`
	// DeploymentID is the Instance's spec.deploymentID, DisplayLabel its deployment-ID label and IdentityRecord the
	// record it names; RecordDisplayLabel and RecordKind are that record's deployment-ID and record labels ("" when it
	// could not be read).
	DeploymentID       string `json:"deploymentID"`
	DisplayLabel       string `json:"displayLabel"`
	IdentityRecord     string `json:"identityRecord"`
	RecordDisplayLabel string `json:"recordDisplayLabel"`
	RecordKind         string `json:"recordKind"`
}

// identityChecks: "each Instance carries its deployment ID under its Allocation's tag" (HBEP-3, Test plan; the
// rehearsal's part 3, item 3): the job runs under the tag HBEP-3 computes, which is also the Allocation's external name
// and observed tag; the Instance carries a full deployment ID, its display form in its label, and the identity record it
// names exists for that ID.
func identityChecks(o IdentityObs) []string {
	if !o.Read {
		return []string{"the Instance's deployment ID under its tag was not read"}
	}
	var f []string
	if o.Tag == "" || o.JobName != o.Tag {
		f = append(f, "the Instance's job does not run under the tag HBEP-3 computes for its Allocation")
	}
	if o.ExternalName != o.Tag || o.ObservedTag != o.Tag {
		f = append(f, "the Allocation's external name or observed tag is not the tag HBEP-3 computes")
	}
	if !fullDeploymentID.MatchString(o.DeploymentID) {
		f = append(f, "the Instance carries no full deployment ID")
	} else {
		display := o.DeploymentID[:12]
		if o.DisplayLabel != display || o.IdentityRecord != "id-"+display {
			f = append(f, "the Instance's deployment-ID label or identity record is not its ID's display form")
		}
		if o.RecordDisplayLabel != display || o.RecordKind != v1alpha1.RecordIdentity {
			f = append(f, "the identity record the Instance names is missing or not for its deployment ID")
		}
	}
	return f
}

// ObsCheck is the result of one of HBEP-8's checks made through internal/rehearsal/obscheck by the live steps (which
// alone may import it): whether it was made, and what it found missing or wrong, each a line for the record. The
// expected set is the steps' own, written from HBEP-8's Reasons table and the row's steps, never from what the run
// emitted.
type ObsCheck struct {
	Checked  bool     `json:"checked"`
	Missing  []string `json:"missing,omitempty"`
	Problems []string `json:"problems,omitempty"`
}

// RequeueObs is the requeue of the run's own job as the run saw it.
type RequeueObs struct {
	// Attempted says the run sent the requeue; Accepted that the scheduler took it, Refusal its reason when not.
	Attempted bool   `json:"attempted"`
	Accepted  bool   `json:"accepted"`
	Refusal   string `json:"refusal,omitempty"`
	// DrainingSeen says the controller was seen draining the Instance, at the deployment's end the run set, with no
	// other Instance or Allocation of the run beside it, before the requeue was sent.
	DrainingSeen bool `json:"drainingSeen"`
	// EndReason is the reason the Instance ended with after the requeue ("" when the run did not see it end).
	EndReason string `json:"endReason,omitempty"`
	// AllocationReleased says the watch saw the Instance's Allocation deleted after the requeue; JobEnded that the
	// cluster's record then showed the job ended, and JobRequeued requeued (its requeue flag, or a restart count),
	// both read before the run ended anything itself.
	AllocationReleased bool `json:"allocationReleased"`
	JobEnded           bool `json:"jobEnded"`
	JobRequeued        bool `json:"jobRequeued"`
	// NewAllocations and NewInstances count those of the run the watch saw after the requeue that it had not seen
	// before it: a replacement.
	NewAllocations int `json:"newAllocations"`
	NewInstances   int `json:"newInstances"`
	// HoldStream is the stream held open across the window, recorded and not judged: the requeue's kill cuts it.
	HoldStream Stream `json:"holdStream"`
}

// NowServeObs is a run of row A.
type NowServeObs struct {
	Tags       []string       `json:"tags"`
	Deployment DeploymentSpec `json:"deployment"`
	// ReadyAt is when the Instance became ready; DrainStart its drain start, from its recorded lease end.
	ReadyAt    time.Time `json:"readyAt"`
	DrainStart time.Time `json:"drainStart"`
	// Stream is the long stream, Rate the engine's measured rate in tokens a second that sized nothing: the stream's
	// length is the row's (Timing.ServeTokens).
	Stream Stream  `json:"stream"`
	Rate   float64 `json:"rate"`
	// Identity is the deployment ID under its tag.
	Identity IdentityObs `json:"identity"`
	// Events and Gauges are HBEP-8's checks, through obscheck.
	Events ObsCheck `json:"events"`
	Gauges ObsCheck `json:"gauges"`
	// WorkerRestarts counts the restarts at the worker rung the run made, WorkerRestart their times.
	WorkerRestarts int          `json:"workerRestarts"`
	WorkerRestart  RestartTimes `json:"workerRestart"`
	// Responses counts the responses the run received; Attributed those that carried HBEP-5's attribution headers.
	Responses  int `json:"responses"`
	Attributed int `json:"attributed"`
	// Requeue is the last step.
	Requeue RequeueObs `json:"requeue"`
	// Warm is the warm start as the agent's own log of the job showed it (the ruling 1(b) on #376).
	Warm WarmObs `json:"warm"`
}

// serveStreamChecks: HBEP-5's long stream, "a completed stream", exact in length and within the deadline.
func serveStreamChecks(row Row, s Stream) []string {
	tm := row.Clock()
	var f []string
	switch {
	case s.CutBy != "":
		f = append(f, "the long stream was cut")
	case !s.Complete:
		f = append(f, "the long stream did not finish by its completion rule")
	}
	if s.Tokens != tm.ServeTokens {
		f = append(f, "the long stream did not deliver exactly the row's tokens")
	}
	if !s.Ended.After(s.Opened) || s.Ended.Sub(s.Opened) > tm.MaxRequestDuration {
		f = append(f, "the long stream did not end within the deployment's deadline")
	}
	return f
}

// obsChecks judges one of HBEP-8's checks.
func obsChecks(what string, c ObsCheck) []string {
	var f []string
	if !c.Checked {
		return []string{fmt.Sprintf("HBEP-8's %s were not checked", what)}
	}
	if len(c.Missing) > 0 {
		f = append(f, fmt.Sprintf("an expected one of HBEP-8's %s was not there", what))
	}
	if len(c.Problems) > 0 {
		f = append(f, fmt.Sprintf("HBEP-8's %s broke a rule (the record names each)", what))
	}
	return f
}

// requeueChecks: "a requeue of the Allocation's job fails its Instance" (HBEP-3, open question 13 and Test plan; Q5), in
// a one-job row: no replacement, and the held job cancelled by the failed Allocation's release, so that the slot frees.
// A refusal leaves the requeue unjudged (NOT RUN), but the window before it is the run's: the end set and the drain seen.
func requeueChecks(o RequeueObs) []string {
	if !o.Attempted {
		return []string{"the requeue of the run's own job was not attempted"}
	}
	var f []string
	if !o.DrainingSeen {
		f = append(f, "the controller was not seen draining the Instance alone at the deployment's end before the requeue")
	}
	if !o.Accepted {
		return f
	}
	if o.EndReason != v1alpha1.ReasonRequeued {
		f = append(f, "the requeued Instance did not end with reason Requeued")
	}
	if !o.JobRequeued {
		f = append(f, "the cluster's record does not show the job requeued")
	}
	if o.NewAllocations > 0 || o.NewInstances > 0 {
		f = append(f, "the requeue was followed by a replacement")
	}
	if !o.AllocationReleased || !o.JobEnded {
		f = append(f, "the failed Allocation's release did not end the held job before the run ended anything itself")
	}
	return f
}

// VerifyNowServe judges alpha-athena-now-serve.
//
//	hbep/0005-llmlab-compatible-front/README.md:442 and :488: the long stream through the front, attributed (Q4).
//	hbep/0003-resource-model-and-naming/README.md:380, :332: the deployment ID under its tag; a requeue fails its Instance.
//	hbep/0008-observability-and-events/README.md:450-451: the events and the engine gauges.
//	hbep/0007-lifecycle-controller/README.md:663: one restart at the worker rung (Q8: plgrid-now counts for it).
func VerifyNowServe(row Row, o NowServeObs, jobs []provider.Job) []string {
	f := deploymentChecks(row, o.Deployment)
	lf, _ := ledgerChecks(row, o.Tags, jobs)
	f = append(f, lf...)
	f = append(f, tagCountChecks(row, o.Tags, "the row makes exactly one Allocation")...)
	if o.ReadyAt.IsZero() {
		f = append(f, "the Instance was not seen ready")
	}
	f = append(f, serveStreamChecks(row, o.Stream)...)
	// Attribution on every response (HBEP-5): no response recorded is no evidence of it.
	if o.Responses <= 0 {
		f = append(f, "the run recorded no response, so attribution is unchecked")
	} else if o.Attributed != o.Responses {
		f = append(f, "a response lacked its attribution headers")
	}
	f = append(f, identityChecks(o.Identity)...)
	f = append(f, obsChecks("events", o.Events)...)
	f = append(f, obsChecks("engine gauges", o.Gauges)...)
	f = append(f, restartChecks(o.WorkerRestarts)...)
	f = append(f, requeueChecks(o.Requeue)...)
	return dedupe(f)
}

// nowServePost is row A's text to post. A refused requeue with no failure is NOT RUN, and the line says so with the
// reason; with a failure the run is a FAIL, the refusal beside it.
func nowServePost(row Row, o NowServeObs, failures []string) string {
	var b strings.Builder
	head := verdictLine(row, failures)
	warm := warmStartShownByTheAgentsOwnLog(o.Warm)
	if len(failures) == 0 {
		var why []string
		if o.Requeue.Attempted && !o.Requeue.Accepted {
			why = append(why, fmt.Sprintf("%s (%s)", requeueRefusedWords, o.Requeue.Refusal))
		}
		if warm != nil {
			why = append(why, fmt.Sprintf("%s (%s)", warmUnshownWords, warm.counts()))
		}
		if len(why) > 0 {
			head = fmt.Sprintf("%s: NOT RUN (posts to %s): %s", row.Name, row.PostsTo, strings.Join(why, "; "))
		}
	}
	tm := row.Clock()
	fmt.Fprintf(&b, "%s\n%s\n- the long stream: %d tokens of the %d asked over %s at the engine's own rate (measured %.0f tokens/s), under the %s deadline; cut: %t\n- responses with attribution: %d of %d; restarts at the worker rung: %d\n",
		head, deploymentLine(o.Deployment), o.Stream.Tokens, tm.ServeTokens, o.Stream.Ended.Sub(o.Stream.Opened).Round(time.Second), o.Rate, tm.MaxRequestDuration,
		o.Stream.CutBy != "", o.Attributed, o.Responses, o.WorkerRestarts)
	b.WriteString(restartLine(o.WorkerRestart))
	fmt.Fprintf(&b, "- the deployment ID under its tag: %s\n- HBEP-8's events: %s; engine gauges: %s\n", passWord(len(identityChecks(o.Identity)) == 0),
		obsWord(o.Events), obsWord(o.Gauges))
	if warm == nil {
		fmt.Fprintf(&b, "- the warm start, from the agent's own log: the image staged already, its digest named (%s, or %s), and all %d checkpoint files %s\n", StagedAlreadyVerified, StagedByAnother, len(o.Warm.Files), StagedPresentMatching)
	} else {
		fmt.Fprintf(&b, "- the warm start, from the agent's own log: NOT shown (%s): NOT RUN, never a pass\n", warm.counts())
	}
	r := o.Requeue
	switch {
	case !r.Attempted:
		b.WriteString("- the requeue: not attempted\n")
	case !r.Accepted:
		fmt.Fprintf(&b, "- the requeue: refused by the scheduler (%s): NOT RUN, never a pass; the drain at the deployment's end was seen first: %t\n", r.Refusal, r.DrainingSeen)
	default:
		fmt.Fprintf(&b, "- the requeue, after the deployment's end and the drain were seen (%t): the Instance ended %s; the job requeued in the cluster's record: %t; the failed Allocation released: %t, and the held job ended by it: %t; replacements: %d Allocations, %d Instances\n",
			r.DrainingSeen, orNone(r.EndReason), r.JobRequeued, r.AllocationReleased, r.JobEnded, r.NewAllocations, r.NewInstances)
	}
	return b.String()
}

func passWord(ok bool) string {
	if ok {
		return "as HBEP-3 says"
	}
	return "NOT as HBEP-3 says"
}

func obsWord(c ObsCheck) string {
	switch {
	case !c.Checked:
		return "not checked"
	case len(c.Missing) > 0 || len(c.Problems) > 0:
		return fmt.Sprintf("%d missing, %d problems", len(c.Missing), len(c.Problems))
	}
	return "all expected, none wrong"
}

func orNone(s string) string {
	if s == "" {
		return "with no end seen"
	}
	return "with reason " + s
}

// requeueRefusedWords say what happened to a run whose requeue was refused, in the marker, which hack/live-checks.sh
// reads for them.
const requeueRefusedWords = "the scheduler refused the requeue of the run's own job"

// The reason words of the stager's log lines for an artifact found already staged (issue #392; the science agent's
// ruling of 11 Nov 2026 on #376, relayed by the code agent's builder k343): each line names its image digest or its
// checkpoint file, and carries no path. The stager writes them as "stage: image <digest>: <reason>" and "stage:
// checkpoint file <file>: <reason>" (internal/agent/stage, PR #393), and a verification after the start (RehashOnly)
// writes none. These constants are the fixtures' source, pinned to the stager's own lines and formats
// (warmpin_test.go), so that the two cannot drift.
const (
	// StagedAlreadyVerified is an image's first check finding it staged and its integrity record matching.
	StagedAlreadyVerified = "already staged and verified"
	// StagedByAnother is the re-check after the image's digest lock, finding another Allocation staged it meanwhile.
	StagedByAnother = "staged by another Allocation"
	// StagedPresentMatching is one checkpoint file found present with its listed SHA-256.
	StagedPresentMatching = "present and matching"
	// StagedFailedCheck is an image found staged whose check against its integrity record failed, staged again
	// ("image <digest>: failed its check (<error>): staging it again"): whatever line follows, the staged image was not
	// sound, so it is no warm start, and a following StagedByAnother would otherwise read as one.
	StagedFailedCheck = "failed its check"
)

// The agent's own messages the warm start's check reads, each the msg of one slog line of the agent (pinned to their
// call sites in warmpin_test.go): its start (cmd/hb-agent/main.go), an image build and a failed staging attempt
// (internal/agent/converge.go), and a worker's start (internal/agent/workers.go), which comes only once the
// Instance's staging is done.
const (
	AgentStartMsg      = "allocation agent starting"
	AgentBuildMsg      = "building image"
	AgentStageFailMsg  = "staging failed"
	AgentWorkerMsg     = "worker started"
	agentRestartKey    = "restartCount"
	agentPreStartLevel = "INFO|WARN"
)

// AgentPreStartMsgs are the agent's own lines that may come before its start line in the job's standard error, which is
// the file row A reads: the bootstrap writes nothing there on a run that reaches the agent (internal/slurm, bootstrap),
// so the file opens with the agent's own lines. Each is a step of cmd/hb-agent's main before its start line that does
// not stop the agent (an Error line there is followed by its exit), or a goroutine of the host-dialed listener it starts
// before it (internal/hostdial, job.go), whose relays may connect before the start line is written. Nothing else may
// come first: a log that does not open with the agent's first start is not the whole log of the first execution, and
// shows no warm start (the science agent's condition of 11 Nov 2026 on #376). warmpin_test.go pins each to its call site.
var AgentPreStartMsgs = []string{
	"cannot mark inherited file descriptors close-on-exec",
	"no GPU list: registering without one",
	"host-dialed: listening for the relays",
	"the relay socket's directory is under the fallback, not TMPDIR, and outlives a killed agent",
	"relay session up",
	"relay session ended",
	"a relay was refused as superseded",
	"a relay connection was refused",
	"a relay's hello was refused",
	"a connection found no relay session and is closed",
}

// ErrWarmUnshown is row A's NOT RUN when the agent's own log of its job does not show the warm start (the science
// agent's ruling 1(b) on #376): the image staged already and every checkpoint file present and matching, by the
// stager's own lines, in the job's first staging attempt. A warm claim without that evidence is NOT RUN, never a pass:
// the row starts warm only because of the pre-staging row (PR #385's profile, a 12-minute startTimeout), and a cold
// start that happened to fit would prove nothing about it.
var ErrWarmUnshown = errors.New("the agent's own log did not show the warm start")

// WarmUnshownError is ErrWarmUnshown with what was missing, in fixed words and counts: the post and the marker carry
// no name or path.
type WarmUnshownError struct {
	LogRead bool
	// NotWhole: the read does not open with the agent's first start (restartCount 0, after its pre-start lines only),
	// or does not reach the end of its first staging.
	NotWhole bool
	// Retried: the first staging was not the only attempt (an image build, a failed attempt, or a second image line).
	Retried             bool
	ImageShown          bool
	FilesShown, FilesOf int
}

func (e *WarmUnshownError) Error() string {
	return fmt.Sprintf("%v (%s)", ErrWarmUnshown, e.counts())
}

func (e *WarmUnshownError) counts() string {
	switch {
	case !e.LogRead:
		return "the agent's log of the job could not be read"
	case e.NotWhole:
		return "the log read is not the whole of the agent's first start to its first worker"
	case e.Retried:
		return "the first staging was built, failed a check or was tried again: not the first attempt's evidence"
	}
	return fmt.Sprintf("the image %s: %t; checkpoint files %s: %d of %d", StagedAlreadyVerified, e.ImageShown, StagedPresentMatching, e.FilesShown, e.FilesOf)
}

// Is makes the error ErrWarmUnshown.
func (e *WarmUnshownError) Is(target error) bool { return target == ErrWarmUnshown }

// The kinds of AgentLogLine.
const (
	LineStart       = "start"
	LinePreStart    = "pre-start"
	LineBuild       = "build"
	LineStageFailed = "staging-failed"
	LineWorker      = "worker-started"
	LineImage       = "image"
	LineFailedCheck = "image-failed-check"
	LineFile        = "file"
	LineOther       = "other"
)

// AgentLogLine is one line of the agent's log of the job, as the warm start's check reads it: its kind, a start line's
// restartCount (-1 when it has none), and a stager line's reason and tokens, among which it names its digest or file.
// The record keeps these, not the line: an Apptainer line can name a path.
type AgentLogLine struct {
	Kind         string   `json:"kind"`
	RestartCount int      `json:"restartCount,omitempty"`
	Reason       string   `json:"reason,omitempty"`
	Tokens       []string `json:"tokens,omitempty"`
}

var (
	slogMsg     = regexp.MustCompile(`(?:^|\s)msg=("(?:[^"\\]|\\.)*"|\S+)`)
	slogLevel   = regexp.MustCompile(`(?:^|\s)level=(INFO|WARN|ERROR|DEBUG)(?:\s|$)`)
	slogRestart = regexp.MustCompile(`(?:^|\s)` + agentRestartKey + `=(\d+)(?:\s|$)`)
)

// ParseAgentLog reads the agent's log of the job (the job's standard error), line by line, in order. A line is the
// agent's own when it is slog's text form with one of the messages above (its msg, unquoted); a stager line with one of
// the reason words is an image or a file line, its tokens the words of the line split on white space, with quotes and
// surrounding punctuation taken off and a key=value word taken as its value, so that a name is matched as a whole token,
// never inside another (model.safetensors is not in model.safetensors.index.json). Every other line is "other".
func ParseAgentLog(log []byte) []AgentLogLine {
	var out []AgentLogLine
	text := strings.TrimSuffix(string(log), "\n")
	if text == "" {
		return nil
	}
	for _, l := range strings.Split(text, "\n") {
		out = append(out, parseAgentLine(l))
	}
	return out
}

func parseAgentLine(l string) AgentLogLine {
	msg := ""
	if m := slogMsg.FindStringSubmatch(l); m != nil {
		msg = m[1]
		if u, err := strconv.Unquote(msg); err == nil {
			msg = u
		}
	}
	level := ""
	if m := slogLevel.FindStringSubmatch(l); m != nil {
		level = m[1]
	}
	switch {
	case msg == AgentStartMsg && level == "INFO":
		n := -1
		if m := slogRestart.FindStringSubmatch(l); m != nil {
			n, _ = strconv.Atoi(m[1])
		}
		return AgentLogLine{Kind: LineStart, RestartCount: n}
	case msg == AgentBuildMsg:
		return AgentLogLine{Kind: LineBuild}
	case msg == AgentStageFailMsg:
		return AgentLogLine{Kind: LineStageFailed}
	case msg == AgentWorkerMsg:
		return AgentLogLine{Kind: LineWorker}
	case msg != "" && strings.Contains(agentPreStartLevel, level) && level != "" && slicesContains(AgentPreStartMsgs, msg):
		return AgentLogLine{Kind: LinePreStart}
	}
	if strings.HasPrefix(strings.TrimSpace(l), "stage: image ") && strings.Contains(l, ": "+StagedFailedCheck+" ") {
		return AgentLogLine{Kind: LineFailedCheck}
	}
	kind, reason := "", ""
	for _, r := range []string{StagedAlreadyVerified, StagedByAnother, StagedPresentMatching} {
		if strings.Contains(l, r) {
			kind, reason = LineImage, r
			if r == StagedPresentMatching {
				kind = LineFile
			}
			break
		}
	}
	if kind == "" {
		return AgentLogLine{Kind: LineOther}
	}
	var toks []string
	for _, w := range strings.Fields(l) {
		if i := strings.IndexByte(w, '='); i >= 0 {
			w = w[i+1:]
		}
		w = strings.Trim(w, "\"'`,;:()[]{}")
		if w != "" {
			toks = append(toks, w)
		}
	}
	return AgentLogLine{Kind: kind, Reason: reason, Tokens: toks}
}

// WarmObs is what row A read for its warm start: whether the agent's log of the job was read, the image digest, the
// checkpoint files that must each show present and matching (ExpectedWarmFiles: from outside the log), the files the
// stager skips, and the log's lines (ParseAgentLog).
type WarmObs struct {
	LogRead     bool           `json:"logRead"`
	ImageDigest string         `json:"imageDigest"`
	Files       []string       `json:"files"`
	Skipped     []string       `json:"skipped,omitempty"`
	Log         []AgentLogLine `json:"log,omitempty"`
}

// SkipRule is the stage package's rule of the checkpoint files the engine does not load, which staging skips with no
// line (internal/agent/stage, skippedFiles, by the launch's --load-format): one result per file, in order. The steps
// pass the stage package's own (stage.SkippedFiles, PR #393's export), never a rule of this package's, and never one
// read off the log.
type SkipRule func(files []agentapi.FileDigest, loadFormat string) []bool

// ExpectedWarmFiles is the set of checkpoint files of which every one must show "present and matching", from OUTSIDE
// the log (the science agent's point of 11 Nov 2026 on #376): the checkpoint's own file list from the Instance's identity
// record, the list the agent's stager is given (internal/lifecycle, desired.go: Checks.Verify.Files), less the files the
// stager skips by the deployment's load format, by the stage package's own rule. A nil rule skips nothing: that can
// only make a warm start NOT RUN, never pass one.
func ExpectedWarmFiles(files []agentapi.FileDigest, loadFormat string, skip SkipRule) (expected, skipped []string) {
	var skips []bool
	if skip != nil {
		skips = skip(files, loadFormat)
	}
	for i, f := range files {
		if i < len(skips) && skips[i] {
			skipped = append(skipped, f.Path)
			continue
		}
		expected = append(expected, f.Path)
	}
	return expected, skipped
}

// warmStartShownByTheAgentsOwnLog is the warm-start check's one seam (the ruling 1(b) on #376 and its conditions), nil
// when the warm start is shown:
//   - the log is whole: it opens with the agent's start line, restartCount 0, after the agent's pre-start lines only
//     (AgentPreStartMsgs), and the read reaches the end of the first staging;
//   - the window is that first execution's first staging: from the start line to the first of a worker's start
//     (AgentWorkerMsg, which comes only once staging is done) and a second start line (a second execution);
//   - in it, the first attempt alone: no image build, no failed attempt, no failed image check (StagedFailedCheck), and
//     exactly one image line, since a silent cancel that got past the image check leaves a second one;
//   - that one image line names the Instance's image digest, and for each expected file (ExpectedWarmFiles) a file
//     line names it; a line naming another digest or file shows nothing.
func warmStartShownByTheAgentsOwnLog(w WarmObs) *WarmUnshownError {
	e := &WarmUnshownError{LogRead: w.LogRead, FilesOf: len(w.Files)}
	if !w.LogRead {
		return e
	}
	i := 0
	for i < len(w.Log) && w.Log[i].Kind == LinePreStart {
		i++
	}
	if i == len(w.Log) || w.Log[i].Kind != LineStart || w.Log[i].RestartCount != 0 {
		e.NotWhole = true
		return e
	}
	end := -1
	for j := i + 1; j < len(w.Log); j++ {
		if k := w.Log[j].Kind; k == LineWorker || k == LineStart {
			end = j
			break
		}
	}
	if end < 0 {
		e.NotWhole = true
		return e
	}
	images, imageNamed := 0, false
	named := map[string]bool{}
	for _, l := range w.Log[i+1 : end] {
		switch l.Kind {
		case LineBuild, LineStageFailed, LineFailedCheck:
			e.Retried = true
		case LineImage:
			images++
			imageNamed = imageNamed || slicesContains(l.Tokens, w.ImageDigest) || slicesContains(l.Tokens, strings.TrimPrefix(w.ImageDigest, "sha256:"))
		case LineFile:
			for _, t := range l.Tokens {
				named[t] = true
			}
		}
	}
	if images > 1 {
		e.Retried = true
	}
	e.ImageShown = w.ImageDigest != "" && images == 1 && imageNamed
	for _, f := range w.Files {
		if named[f] {
			e.FilesShown++
		}
	}
	if !e.Retried && e.ImageShown && e.FilesOf > 0 && e.FilesShown == e.FilesOf {
		return nil
	}
	return e
}

func slicesContains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// nowServeNotRun is what ends a run of row A NOT RUN when nothing failed: the scheduler's refusal of the requeue (the
// scenario's ErrRequeueRefused), and the warm start the agent's own log did not show; both when both hold.
func nowServeNotRun(o NowServeObs, scenarioErr error) error {
	var errs []error
	if errors.Is(scenarioErr, ErrRequeueRefused) {
		errs = append(errs, scenarioErr)
	}
	if w := warmStartShownByTheAgentsOwnLog(o.Warm); w != nil {
		errs = append(errs, w)
	}
	return errors.Join(errs...)
}

// warmUnshownWords say what happened to a run whose warm start the agent's log did not show, in the marker, which
// hack/live-checks.sh reads for them.
const warmUnshownWords = "the agent's own log did not show the warm start"
