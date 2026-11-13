package alpha

// The keeper row (issue #521; betakeeper.go) over the fake cluster: one job, run on the manual clock; what the stack would show is stood in for.
// Each fault departs from the healthy run in one way that must fail the row, or, for the window faults, end it NOT RUN
// (TestAKeeperStreamTheContextCannotHoldIsNotRun).

import (
	"context"
	"time"
)

// dryKeeperRelays is a healthy reading of the keeper's two relays: the keeper's own ssh at its E, one for each channel, the controller running none.
func dryKeeperRelays() KeeperRelaysObs {
	return KeeperRelaysObs{Read: true, KeeperEpoch: KeeperEpochBase + 1,
		Control: RelayObs{PID: 4100, StartTicks: 5000, Epoch: KeeperEpochBase + 1, Seq: 2},
		Serving: RelayObs{PID: 4101, StartTicks: 5001, Epoch: KeeperEpochBase + 1, Seq: 1}}
}

// BetaKeeper is the keeper row over the fake cluster.
func (d *drySteps) BetaKeeper(_ context.Context, p Plan) (KeeperObs, error) {
	if d.fault("pend-past-bound") {
		return KeeperObs{}, d.pendPastBound(p)
	}
	tm := p.Row.Clock()
	tag := d.tag(p, "keeper")
	n, id := d.submit(tag, d.partitionFor(true), d.cfg.Row.QOS)
	j := d.running(id)
	started := *j.StartTime
	d.extras(p, tag)
	d.tick(8 * time.Minute) // a warm start, inside the profile's 12 minutes
	ready := d.now()
	drain := started.Add(d.deployment().LeaseLimit - DrainWindow(tm.MaxRequestDuration, tm.DrainMargin, 0))
	rate := 40.0
	switch {
	case d.fault("window-too-short"):
		// A context that holds a stream of a minute: the window of 160 s does not fit.
		if _, err := FitKeeper(tm, 2400+PromptReserve, rate); err != nil {
			if cerr := d.cancelTag(tag); cerr != nil {
				return KeeperObs{}, cerr
			}
			return KeeperObs{}, err
		}
	case d.fault("window-at-the-floor"):
		// A context that holds a stream of exactly the window: it fits.
		if _, err := FitKeeper(tm, int(rate*KeeperWindow.Seconds())+PromptReserve, rate); err != nil {
			return KeeperObs{}, err
		}
	}
	asked := int(rate * (KeeperWindow + time.Minute).Seconds())
	opened := ready.Add(time.Minute)
	killed := opened.Add(KeeperKillAfter)
	outcome := killed.Add(35 * time.Second)
	stream := Stream{ID: "keeper", Opened: opened, Ended: outcome.Add(KeeperMargin + time.Minute), Complete: true, Tokens: asked}
	rel := dryKeeperRelays()
	obs := KeeperObs{
		Deployment: d.deployment(), ReadyAt: ready, DrainStart: drain, Rate: rate, Asked: asked, Before: rel, After: rel,
		KilledAt: killed, OutcomeAt: outcome, ControllerReplaced: true, LeaseBefore: 3, LeaseAfter: 4, HolderChanged: true, FenceAfter: 4,
		AgentReported: true, AgentSame: true, RestartsBefore: 0, RestartsAfter: 0, Stream: stream, FrontCuts: 0, Responses: 4, Attributed: 4,
	}
	switch {
	case d.fault("not-ready"):
		obs.ReadyAt = time.Time{}
	case d.fault("relays-unread-before"):
		obs.Before.Read = false
	case d.fault("relays-unread-after"):
		obs.After.Read = false
	case d.fault("keeper-e-low"):
		obs.Before.KeeperEpoch, obs.After.KeeperEpoch = KeeperEpochBase-1, KeeperEpochBase-1
	case d.fault("controller-relays"):
		obs.Before.ControllerOwn = 2
	case d.fault("controller-relays-after"):
		obs.After.ControllerOwn = 2
	case d.fault("relay-replaced-control"):
		obs.After.Control.PID++
	case d.fault("relay-replaced-serving"):
		obs.After.Serving.StartTicks++
	case d.fault("relay-seq-changed"):
		obs.After.Serving.Seq++
	case d.fault("keeper-e-changed"):
		obs.After.KeeperEpoch++
	case d.fault("not-killed"):
		obs.KilledAt = time.Time{}
	case d.fault("outcome-unseen"):
		obs.OutcomeAt = time.Time{}
	case d.fault("outcome-late"):
		obs.OutcomeAt = killed.Add(KeeperOutcomeBound + time.Second)
		obs.Stream.Ended = obs.OutcomeAt.Add(KeeperMargin + time.Minute)
	case d.fault("same-controller"):
		obs.ControllerReplaced = false
	case d.fault("same-holder"):
		obs.HolderChanged = false
	case d.fault("lease-not-risen"):
		obs.LeaseAfter = obs.LeaseBefore
	case d.fault("lease-jumped"):
		obs.LeaseAfter = obs.LeaseBefore + 2
	case d.fault("fence-not-followed"):
		obs.FenceAfter = obs.LeaseBefore
	case d.fault("agent-not-reported"):
		obs.AgentReported = false
	case d.fault("agent-registration-changed"):
		obs.AgentSame = false
	case d.fault("unreachable"):
		obs.Unreachable = true
	case d.fault("instance-restarted"):
		obs.RestartsAfter = 1
	case d.fault("cut"):
		obs.Stream.Complete, obs.Stream.CutBy = false, "tunnel"
	case d.fault("incomplete"):
		obs.Stream.Complete = false
	case d.fault("short-stream"):
		obs.Stream.Tokens = asked - 1
	case d.fault("past-deadline"):
		obs.Stream.Ended = obs.Stream.Opened.Add(tm.MaxRequestDuration + time.Second)
	case d.fault("opened-after-kill"):
		obs.Stream.Opened = killed.Add(time.Second)
	case d.fault("ends-at-the-outcome"):
		obs.Stream.Ended = outcome.Add(KeeperMargin - time.Second)
	case d.fault("front-cut"):
		obs.FrontCuts = 1
	case d.fault("front-cuts-unread"):
		obs.FrontCuts = -1
	case d.fault("unattributed"):
		obs.Attributed = 3
	case d.fault("no-responses"):
		obs.Responses, obs.Attributed = 0, 0
	}
	d.tick(20 * time.Minute) // the restart and the rest, before the drain start
	d.tick(time.Minute)
	d.end(n, id)
	obs.Tags = append([]string(nil), d.tags...)
	return obs, nil
}

// betaKeeperCases are the keeper row's dry cases. The window faults that end NOT RUN are not here: Run returns their error and writes no verdict
// (TestAKeeperStreamTheContextCannotHoldIsNotRun); a window at the floor is no failure.
var betaKeeperCases = []dryCase{
	{"", ""},
	{"window-at-the-floor", ""},
	{"not-ready", "the Instance was not seen ready"},
	{"relays-unread-before", "the keeper's relays were not read before the restart"},
	{"relays-unread-after", "the keeper's relays were not read after the restart"},
	{"keeper-e-low", "the keeper's E before the restart is below 2^31"},
	{"controller-relays", "the controller runs relays of its own before the restart"},
	{"controller-relays-after", "the controller runs relays of its own after the restart"},
	{"relay-replaced-control", "the control relay's ssh is not the process it was before the restart: a relay was replaced"},
	{"relay-replaced-serving", "the serving relay's ssh is not the process it was before the restart: a relay was replaced"},
	{"relay-seq-changed", "the serving relay's E or N changed across the restart"},
	{"keeper-e-changed", "the keeper's E changed across the controller's restart"},
	{"not-killed", "the controller was not killed"},
	{"outcome-unseen", "the outcome of the controller's restart was not seen within 2m0s"},
	{"outcome-late", "the outcome of the controller's restart was not seen within 2m0s"},
	{"same-controller", "the controller that leads after the kill is the one that was killed"},
	{"same-holder", "the Provider's Lease has the holder it had before the kill"},
	{"lease-not-risen", "the Provider's Lease's transitions did not rise by one across the restart"},
	{"lease-jumped", "the Provider's Lease's transitions did not rise by one across the restart"},
	{"fence-not-followed", "the keeper's fence is not the new leader's epoch: the keeper did not accept it"},
	{"agent-not-reported", "the new controller holds no report of the agent"},
	{"agent-registration-changed", "the agent's registration changed across the restart"},
	{"unreachable", "the Instance was judged unreachable across the restart"},
	{"instance-restarted", "the Instance restarted across the controller's restart"},
	{"cut", "the stream across the restart was cut"},
	{"incomplete", "the stream across the restart did not finish by its completion rule"},
	{"short-stream", "the stream across the restart did not deliver exactly the tokens asked for"},
	{"past-deadline", "the stream across the restart did not end within the deployment's deadline"},
	{"opened-after-kill", "the stream was not open before the controller was killed"},
	{"ends-at-the-outcome", "the stream did not flow on for the margin after the outcome"},
	{"front-cut", "the front counted a cut during the restart"},
	{"front-cuts-unread", "the front's cuts could not be counted"},
	{"unattributed", "a response lacked its attribution headers"},
	{"no-responses", "no response, so attribution is unchecked"},
	{"untagged", "a job tag was not written down before its submission"},
	{"wrong-partition", "a job ran on another partition than the row's"},
	{"default-lease", "the deployment's leaseLimit is not the row's shortest lease"},
	{"short-lease", "not longer than the lead an Instance needs (PartitionTooShort)"},
	{"duplicate-tag", "a tag names more than one job"},
	{"extra-allocation", "exactly one Allocation"},
}
