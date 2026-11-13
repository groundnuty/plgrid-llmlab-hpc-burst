package alpha

// beta-athena-now-keeper (issue #521; beta's tracker is #522): a controller restart while a stream is being served, in host-dialed mode with
// the relay keeper, on Athena's plgrid-now under profiles/athena-now.yaml (HBEP-6, The relay keeper and Controller restart; the graduation
// criterion "in host-dialed mode, the relay keeper's tests pass, and a controller restart while serving cuts no serving stream", open question
// 13). One job of 1 GPU, one worker, row A's hold of the slot. What it asks of the real path, that the loopback rehearsal
// (internal/rehearsal/scenario, keeper_restart_test.go) cannot say:
//   - the relays of host-dialed mode are the keeper's children, one ssh through the login node for each of the two channels, at the keeper's E
//     and the sequence it reports, and the controller runs none of its own;
//   - the controller is killed (SIGKILL, through a pidfd pinned along its chain of parents: no signal ever goes to a numeric PID, the rule of
//     #308), the supervisor starts the next copy, which leads the Provider's Lease once the old holder's has expired, and the keeper accepts
//     its epoch: the Lease's transitions rise by one and the keeper's fence follows;
//   - the relays are the very processes they were (the same PIDs and start times, the same E and (E, N) for each channel), so no tunnel was
//     rebuilt through the login node, and the agent, never judged unreachable, reports to the new controller with the same registration and no
//     restart of its own;
//   - the stream in flight across it is a whole stream: complete, not cut, its tokens exactly the ones asked for, still flowing after the
//     outcome, and the front counted no cut at any hop. A stream the front's context cannot hold for the window is NOT RUN, never a weaker pass.
//
// The stream is sized by time, not by row A's 7,000 tokens: it must outlast the restart's outcome, which the loopback rehearsal measured at about
// 35 s after the SIGKILL (the Lease's expiry and the supervisor's restart), at the rate measured before it. The window below is that, with room.
//
// It runs on real vLLM, as row A does: this row tests the relays and a stream, not bytes (the science agent's ruling on #521).

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// BetaKeeperName is the keeper row's name in the script.
const BetaKeeperName = "beta-athena-now-keeper"

// The keeper row's window: the stream is opened, the controller is killed KeeperKillAfter after its first token, the outcome is waited for up to
// KeeperOutcomeBound (the loopback rehearsal measured it at about 35 s, the Lease's expiry and the next copy's start), and the stream is still
// flowing KeeperMargin after it. The stream is at least KeeperWindow long.
const (
	KeeperKillAfter    = 10 * time.Second
	KeeperOutcomeBound = 2 * time.Minute
	KeeperMargin       = 30 * time.Second
	KeeperWindow       = KeeperKillAfter + KeeperOutcomeBound + KeeperMargin
)

// BetaKeeperRow is the keeper row: row A's hold of plgrid-now's one-job slot (one job, one GPU, a start wait of 10 minutes, a lease of 48 and
// two minutes of cleanup, the owner's go of plgrid-now), under athena-now.yaml, posting to the beta tracker. Its clock is the profile's, as every
// row's under it is (TestLiveRowsKeepTheLiveTiming); the window is FitKeeper's floor.
func BetaKeeperRow() Row {
	r := NowServeRow()
	r.Name, r.PostsTo = BetaKeeperName, BetaTracker
	return r
}

// ErrKeeperStreamTooShort is what the keeper row's scenario returns when the stream that fits the deployment's max-model-len at the engine's
// measured rate is under the window: no stream can span a controller restart's outcome, so the restart was never put to the test. Nothing was sent
// but the rate stream, and the run is NOT RUN (NotRun), exit 3, never a weaker pass.
var ErrKeeperStreamTooShort = errors.New("the deployment's max-model-len does not hold a stream long enough to span a controller restart at the engine's measured rate")

// keeperStreamTooShortWords say what happened, in the marker, which hack/live-checks.sh reads. The marker adds the rate and the context in
// parentheses, numbers only.
const keeperStreamTooShortWords = "the deployment's max-model-len does not hold a stream long enough to span a controller restart at the engine's measured rate"

// KeeperStreamTooShortError is ErrKeeperStreamTooShort with the rate and the context it was found at.
type KeeperStreamTooShortError struct {
	Rate        float64
	MaxModelLen int
	Fit, Floor  time.Duration
}

func (e *KeeperStreamTooShortError) counts() string {
	return fmt.Sprintf("at %.0f tokens/s with max-model-len %d the stream that fits is %s, under the window of %s", e.Rate, e.MaxModelLen, e.Fit, e.Floor)
}

func (e *KeeperStreamTooShortError) Error() string {
	return fmt.Sprintf("%v (%s)", ErrKeeperStreamTooShort, e.counts())
}

// Is makes the error ErrKeeperStreamTooShort.
func (e *KeeperStreamTooShortError) Is(target error) bool { return target == ErrKeeperStreamTooShort }

// FitKeeper is the length of the stream the keeper row holds across the restart: as long as max-model-len less PromptReserve holds at rate tokens
// a second, rounded down to a whole second, capped at the row's LongCap, and at least the window. Under the window it is a
// *KeeperStreamTooShortError. A rate that is not a positive finite number is a plain error. It is FitLong's sizing, with the window as the floor.
func FitKeeper(tm Timing, maxModelLen int, rate float64) (time.Duration, error) {
	if !(rate > 0) || math.IsInf(rate, 0) {
		return 0, fmt.Errorf("a stream cannot be sized from a measured rate of %v tokens/s", rate)
	}
	tm.LongFloor = KeeperWindow
	fit, err := FitLong(tm, maxModelLen, rate)
	var lt *LongTooShortError
	if errors.As(err, &lt) {
		return 0, &KeeperStreamTooShortError{Rate: lt.Rate, MaxModelLen: lt.MaxModelLen, Fit: lt.Fit, Floor: KeeperWindow}
	}
	return fit, err
}

// RelayObs is one relay's ssh as the run read it: the process (its PID and its start time, which name one process and not a later one that
// reused the PID) and the fence it runs at.
type RelayObs struct {
	PID        int    `json:"pid"`
	StartTicks uint64 `json:"startTicks"`
	Epoch      int64  `json:"epoch"`
	Seq        int64  `json:"seq"`
}

// KeeperRelaysObs are the keeper's two relays of the run's job, as the keeper reported them and as /proc shows them, read once.
type KeeperRelaysObs struct {
	// Control and Serving are the relays by channel; Read says both were found, each the keeper's own ssh child at the keeper's E.
	Control RelayObs `json:"control"`
	Serving RelayObs `json:"serving"`
	Read    bool     `json:"read"`
	// KeeperEpoch is the keeper's E; ControllerOwn is how many relays the controller runs itself, which in keeper mode it does not.
	KeeperEpoch   int64 `json:"keeperEpoch"`
	ControllerOwn int   `json:"controllerOwn"`
}

// KeeperObs is a run of the keeper row.
type KeeperObs struct {
	Tags       []string       `json:"tags"`
	Deployment DeploymentSpec `json:"deployment"`
	// ReadyAt is when the Instance became ready, DrainStart its drain start from its recorded lease end.
	ReadyAt    time.Time `json:"readyAt"`
	DrainStart time.Time `json:"drainStart"`
	// Rate is the engine's measured rate, and Asked the tokens the stream across the restart asked for.
	Rate  float64 `json:"rate"`
	Asked int     `json:"asked"`
	// Before and After are the relays before the controller was killed and once the outcome was seen.
	Before KeeperRelaysObs `json:"before"`
	After  KeeperRelaysObs `json:"after"`
	// KilledAt is when the signal went through the controller's pin, OutcomeAt when the outcome was seen (zero when it was not within the bound).
	KilledAt  time.Time `json:"killedAt"`
	OutcomeAt time.Time `json:"outcomeAt"`
	// ControllerReplaced says the controller process that leads now is not the one that was killed.
	ControllerReplaced bool `json:"controllerReplaced"`
	// LeaseBefore and LeaseAfter are the Provider's Lease's transitions, and HolderChanged says the holder did; FenceAfter is the keeper's highest
	// accepted epoch for the Provider once the outcome was seen.
	LeaseBefore   int64 `json:"leaseBefore"`
	LeaseAfter    int64 `json:"leaseAfter"`
	HolderChanged bool  `json:"holderChanged"`
	FenceAfter    int64 `json:"fenceAfter"`
	// AgentReported says the new controller holds a report of the agent; AgentSame that the Instance's agent registration is the one it had;
	// Unreachable that the Instance was ever judged unreachable between the kill and the outcome; RestartsBefore and RestartsAfter count its restarts.
	AgentReported  bool `json:"agentReported"`
	AgentSame      bool `json:"agentSame"`
	Unreachable    bool `json:"unreachable"`
	RestartsBefore int  `json:"restartsBefore"`
	RestartsAfter  int  `json:"restartsAfter"`
	// Stream is the stream held across the restart.
	Stream Stream `json:"stream"`
	// FrontCuts is how many cuts the front counted, at every hop, from before the kill to the stream's end; -1 when its metrics could not be read.
	FrontCuts int `json:"frontCuts"`
	// Responses counts the responses the run received through the front, Attributed those with HBEP-5's attribution headers.
	Responses  int `json:"responses"`
	Attributed int `json:"attributed"`
}

// KeeperEpochBase is the lowest epoch the keeper's own E may have (relaykeeper.EpochBase, 2^31): below it the E would be a controller's Lease
// epoch, and the keeper's relays could be taken for the controller's. It is written out here, since this file imports no internal package that is
// only for tests, and a test holds it to relaykeeper's.
const KeeperEpochBase = int64(1) << 31

// relaysChecks: the keeper runs the job's two relays as its own children at its E, and the controller runs none.
func relaysChecks(when string, r KeeperRelaysObs) []string {
	if !r.Read {
		return []string{fmt.Sprintf("the keeper's relays were not read %s", when)}
	}
	var f []string
	if r.KeeperEpoch < KeeperEpochBase {
		f = append(f, fmt.Sprintf("the keeper's E %s is below 2^31", when))
	}
	if r.ControllerOwn != 0 {
		f = append(f, fmt.Sprintf("the controller runs relays of its own %s: in keeper mode they are the keeper's", when))
	}
	return f
}

// sameRelayChecks: after the restart the relays are the very processes they were, at the same E and (E, N), which no new ssh through the login
// node could be.
func sameRelayChecks(before, after KeeperRelaysObs) []string {
	if !before.Read || !after.Read {
		return nil // reported by relaysChecks
	}
	var f []string
	if before.KeeperEpoch != after.KeeperEpoch {
		f = append(f, "the keeper's E changed across the controller's restart")
	}
	for _, c := range []struct {
		ch   string
		b, a RelayObs
	}{
		{"control", before.Control, after.Control},
		{"serving", before.Serving, after.Serving},
	} {
		if c.b.PID != c.a.PID || c.b.StartTicks != c.a.StartTicks {
			f = append(f, fmt.Sprintf("the %s relay's ssh is not the process it was before the restart: a relay was replaced", c.ch))
		}
		if c.b.Epoch != c.a.Epoch || c.b.Seq != c.a.Seq {
			f = append(f, fmt.Sprintf("the %s relay's E or N changed across the restart", c.ch))
		}
	}
	return f
}

// controllerChecks: the controller that was killed is replaced by another, which leads the Lease and whose epoch the keeper accepted.
func controllerChecks(o KeeperObs) []string {
	var f []string
	switch {
	case o.KilledAt.IsZero():
		return []string{"the controller was not killed"}
	case o.OutcomeAt.IsZero():
		f = append(f, fmt.Sprintf("the outcome of the controller's restart was not seen within %s", KeeperOutcomeBound))
	case !o.OutcomeAt.After(o.KilledAt):
		f = append(f, "the outcome was seen before the kill")
	case o.OutcomeAt.Sub(o.KilledAt) > KeeperOutcomeBound:
		f = append(f, fmt.Sprintf("the outcome of the controller's restart was not seen within %s", KeeperOutcomeBound))
	}
	if !o.ControllerReplaced {
		f = append(f, "the controller that leads after the kill is the one that was killed")
	}
	if !o.HolderChanged {
		f = append(f, "the Provider's Lease has the holder it had before the kill")
	}
	if o.LeaseAfter != o.LeaseBefore+1 {
		f = append(f, "the Provider's Lease's transitions did not rise by one across the restart")
	}
	if o.FenceAfter != o.LeaseAfter {
		f = append(f, "the keeper's fence is not the new leader's epoch: the keeper did not accept it")
	}
	return f
}

// agentChecks: the agent keeps its registration, is never judged unreachable, restarts nothing, and reports to the new controller.
func agentChecks(o KeeperObs) []string {
	var f []string
	if !o.AgentReported {
		f = append(f, "the new controller holds no report of the agent")
	}
	if !o.AgentSame {
		f = append(f, "the agent's registration changed across the restart")
	}
	if o.Unreachable {
		f = append(f, "the Instance was judged unreachable across the restart")
	}
	if o.RestartsAfter != o.RestartsBefore {
		f = append(f, "the Instance restarted across the controller's restart")
	}
	return f
}

// keeperStreamChecks: the stream asked for exactly Asked tokens and got them whole, opened before the kill and still flowing KeeperMargin after
// the outcome, within the deployment's deadline, and the front counted no cut.
func keeperStreamChecks(row Row, o KeeperObs) []string {
	tm := row.Clock()
	s := o.Stream
	var f []string
	switch {
	case s.CutBy != "":
		f = append(f, "the stream across the restart was cut")
	case !s.Complete:
		f = append(f, "the stream across the restart did not finish by its completion rule")
	}
	if o.Asked <= 0 || s.Tokens != o.Asked {
		f = append(f, "the stream across the restart did not deliver exactly the tokens asked for")
	}
	if !s.Ended.After(s.Opened) || s.Ended.Sub(s.Opened) > tm.MaxRequestDuration {
		f = append(f, "the stream across the restart did not end within the deployment's deadline")
	}
	if !o.KilledAt.IsZero() && !s.Opened.Before(o.KilledAt) {
		f = append(f, "the stream was not open before the controller was killed")
	}
	if !o.OutcomeAt.IsZero() && s.Ended.Sub(o.OutcomeAt) < KeeperMargin {
		f = append(f, "the stream did not flow on for the margin after the outcome")
	}
	switch {
	case o.FrontCuts < 0:
		f = append(f, "the front's cuts could not be counted")
	case o.FrontCuts > 0:
		f = append(f, "the front counted a cut during the restart")
	}
	return f
}

// VerifyBetaKeeper judges the keeper row.
//
//	hbep/0006-instance-reachability/README.md (Test plan, The relay keeper; Graduation criteria, Beta): a controller restart while serving cuts
//	no serving stream in host-dialed mode, the relays and the agent unchanged.
func VerifyBetaKeeper(row Row, o KeeperObs, jobs []provider.Job) []string {
	f := deploymentChecks(row, o.Deployment)
	lf, _ := ledgerChecks(row, o.Tags, jobs)
	f = append(f, lf...)
	f = append(f, tagCountChecks(row, o.Tags, "the row makes exactly one Allocation")...)
	if o.ReadyAt.IsZero() {
		f = append(f, "the Instance was not seen ready")
	}
	f = append(f, relaysChecks("before the restart", o.Before)...)
	f = append(f, relaysChecks("after the restart", o.After)...)
	f = append(f, controllerChecks(o)...)
	f = append(f, sameRelayChecks(o.Before, o.After)...)
	f = append(f, agentChecks(o)...)
	f = append(f, keeperStreamChecks(row, o)...)
	if o.Responses <= 0 {
		f = append(f, "the run recorded no response, so attribution is unchecked")
	} else if o.Attributed != o.Responses {
		f = append(f, "a response lacked its attribution headers")
	}
	return dedupe(f)
}

// betaKeeperPost is the keeper row's text to post: counts and times, never a PID, a job ID, a host or a path.
func betaKeeperPost(row Row, o KeeperObs, failures []string) string {
	var b strings.Builder
	tm := row.Clock()
	took := "not seen"
	if !o.OutcomeAt.IsZero() && !o.KilledAt.IsZero() {
		took = o.OutcomeAt.Sub(o.KilledAt).Round(time.Second).String()
	}
	same := func(a, c RelayObs) string {
		if a.PID == c.PID && a.StartTicks == c.StartTicks && a.Epoch == c.Epoch && a.Seq == c.Seq {
			return "the same process at the same E and N"
		}
		return "NOT the same"
	}
	fmt.Fprintf(&b, "%s\n%s\n- a controller restart (SIGKILL through a pinned pidfd) while a stream of %d tokens ran at the engine's own rate (measured %.0f tokens/s), under the %s deadline: the stream delivered %d tokens over %s, cut: %t, the front counted %d cuts\n",
		verdictLine(row, failures), deploymentLine(o.Deployment), o.Asked, o.Rate, tm.MaxRequestDuration, o.Stream.Tokens,
		o.Stream.Ended.Sub(o.Stream.Opened).Round(time.Second), o.Stream.CutBy != "", o.FrontCuts)
	fmt.Fprintf(&b, "- the outcome: the next controller led and the keeper accepted its epoch %s after the kill; the Lease's transitions %d to %d; the keeper's E %d (above 2^31: %t)\n",
		took, o.LeaseBefore, o.LeaseAfter, o.Before.KeeperEpoch, o.Before.KeeperEpoch >= KeeperEpochBase)
	fmt.Fprintf(&b, "- the relays through the login node after the restart: control %s, serving %s; the controller ran %d relays of its own\n",
		same(o.Before.Control, o.After.Control), same(o.Before.Serving, o.After.Serving), o.After.ControllerOwn)
	fmt.Fprintf(&b, "- the agent: registration the same: %t, judged unreachable: %t, restarts before and after: %d and %d, a report held by the new controller: %t\n",
		o.AgentSame, o.Unreachable, o.RestartsBefore, o.RestartsAfter, o.AgentReported)
	fmt.Fprintf(&b, "- responses with attribution: %d of %d\n", o.Attributed, o.Responses)
	return b.String()
}
