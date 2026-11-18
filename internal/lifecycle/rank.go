package lifecycle

import (
	"errors"
	"fmt"
	"slices"
	"sort"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The ranking point (beta; HBEP-7, Over-allocation and qualification (beta), The ranking point; #616, PR 3).
//
// An Instance that passed verification, qualification and warm-up reaches its ranking point where starting would
// make it ready, after the tunnel's and the router's gates. The selection (select.go) is made with it as passed. A
// chosen Instance becomes ready as it always did, so ranking never holds back one the deployment needs. A surplus
// one waits in verifying until every chosen primary on its Provider is ready, and then it becomes ready only by
// outranking a peer, or it ends RankedOut with its Allocation kept.

// outrankMarginNum and outrankMarginDen are the margin: a candidate outranks a peer only when its time per output
// token is lower by more than 10% of the peer's, that is, when cand × 10 < peer × 9, in integer nanoseconds. 10% is
// warm-up's settling tolerance (HBEP-7, Instance start, Warm-up), so a serving peer is kept against noise.
const (
	outrankMarginNum = 9
	outrankMarginDen = 10
)

// tpot is an Instance's time per output token in nanoseconds, as its warm-up measured it. ok is false when it has
// none, or one that does not parse or is not positive: such a measurement shows nothing about either side of the
// margin, so it never outranks and is never outranked.
func tpot(in *v1alpha1.Instance) (ns int64, ok bool) {
	t := in.Status.Throughput
	if t == nil {
		return 0, false
	}
	d, err := t.TimePerOutputToken.AsDuration()
	if err != nil || d <= 0 {
		return 0, false
	}
	return int64(d), true
}

// outranks reports whether the candidate's time per output token is lower than the peer's by more than 10%.
func outranks(cand, peer *v1alpha1.Instance) bool {
	c, okC := tpot(cand)
	q, okQ := tpot(peer)
	return okC && okQ && c*outrankMarginDen < q*outrankMarginNum
}

// worstPeer is the peer the candidate is compared with: the highest time per output token, then the nearest hard
// end, then the lowest name. A peer with no measurement counts as the best, so it is chosen only when none has one.
func worstPeer(peers []*v1alpha1.Instance) *v1alpha1.Instance {
	ranked := slices.Clone(peers)
	sort.SliceStable(ranked, func(i, j int) bool {
		a, _ := tpot(ranked[i])
		b, _ := tpot(ranked[j])
		switch {
		case a != b:
			return a > b
		case leaseBefore(ranked[i], ranked[j]):
			return true
		case leaseBefore(ranked[j], ranked[i]):
			return false
		}
		return ranked[i].Name < ranked[j].Name
	})
	return ranked[0]
}

// inSelection reports whether the Instance is one of the listed hedges' representatives, or a member of such a hedge.
func inSelection(reps []*v1alpha1.Instance, in *v1alpha1.Instance) bool {
	return slices.ContainsFunc(reps, func(r *v1alpha1.Instance) bool { return hedgeKey(r) == hedgeKey(in) })
}

// enterReady is where starting would make an Instance ready, once its checks and gates passed. ready makes it so.
//
// The selection is made with the Instance as passed. A chosen Instance, and a replacement, which is never ranked
// (it waits for no ranking: HBEP-7, How it counts), becomes ready. A surplus one waits, returning nil with no
// write, until every chosen primary on its Provider is ready; the next pass that sees one change brings it back. It
// is then ranked against the peers: the chosen primaries on its Provider that are ready and have no live
// replacement, which is a handover ranking leaves alone.
func (p *pass) enterReady(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, ready func() error) error {
	// A kept started loser never enters the general ranking below: its one peer is its hedge's winner, never the
	// deployment's other primaries on its Provider (HBEP-7, The ranking point, Its peers).
	if waitingCandidate(in) {
		return p.candidateStep(in, alloc, ready)
	}
	chosen, surplus := selectPrimaries(p.selection(in), p.desiredCount())
	if !inSelection(surplus, in) || inSelection(chosen, in) {
		return p.readyUnmarked(in, ready)
	}
	// A peer that carries the mark has already lost to the Instance the mark names: it is a candidate that restarted
	// inside the front's window and re-passed. It never outranks. Were it ranked, a better re-measurement could mark
	// the Instance that outranked it, and both would be ready and marked, both surplus, both drained, with no primary
	// left. It ends RankedOut as it was going to, its Allocation kept, which also stops a peer and a candidate from
	// outranking each other for ever.
	if name := in.Annotations[annotationRankedOut]; name != "" {
		return p.rankOut(in, alloc, p.instanceNamed(name))
	}
	provider := in.Labels[v1alpha1.LabelProvider]
	var peers []*v1alpha1.Instance
	for _, c := range chosen {
		if c.Labels[v1alpha1.LabelProvider] != provider {
			continue
		}
		if c.Status.Phase != v1alpha1.InstanceReady {
			return nil // ranked once every chosen primary on its Provider is ready or has ended
		}
		if c.DeletionTimestamp.IsZero() && !p.replaced(c) {
			peers = append(peers, c)
		}
	}
	if len(peers) == 0 {
		return p.rankOut(in, alloc, nil)
	}
	peer := worstPeer(peers)
	if !outranks(in, peer) {
		return p.rankOut(in, alloc, peer)
	}
	return p.outrank(in, peer, ready)
}

// outrank makes the candidate ready over its peer. The writes go in this order, so the intent is durable before either
// Instance changes: the peer's mark, the candidate's becoming ready, then, once the front's probe has had its
// interval since the candidate became healthy (frontAdmitsAt), the peer's drain (step 7, through scaleDownTargets),
// and its RankedOut event (rankedOutPeer).
func (p *pass) outrank(in, peer *v1alpha1.Instance, ready func() error) error {
	if peer.Annotations[annotationRankedOut] != in.Name {
		if err := p.setMeta(peer, func(n *v1alpha1.Instance) {
			if n.Annotations == nil {
				n.Annotations = map[string]string{}
			}
			n.Annotations[annotationRankedOut] = in.Name
		}); err != nil {
			return err
		}
	}
	if err := ready(); err != nil {
		return err
	}
	if at, ok := frontAdmitsAt(in); ok {
		p.at(at)
	}
	return nil
}

// readyUnmarked makes an Instance ready that the selection chose or that no ranking holds, and clears the candidate
// mark of one whose winner has gone: it is chosen like any Instance, with no peer (the winner it was kept beside
// failed, and its extra is chosen in its place: HBEP-7, How it counts).
func (p *pass) readyUnmarked(in *v1alpha1.Instance, ready func() error) error {
	if candidateOf(in) != "" {
		if err := p.setMeta(in, func(n *v1alpha1.Instance) { delete(n.Annotations, annotationCandidateOf) }); err != nil {
			return err
		}
	}
	return ready()
}

// candidateWinner is the winner a kept started loser waits on, or nil (candidateWinnerOf).
func (p *pass) candidateWinner(in *v1alpha1.Instance) *v1alpha1.Instance {
	return candidateWinnerOf(in, p.instanceNamed, p.stale)
}

// candidateStep is a kept started loser's step at its ranking point, where starting would make it ready (HBEP-7,
// Over-allocation and qualification (beta), Hedging's started losers; #616, PR 5; the science agent's ruling on #721).
// The selection treats it as any Instance, so:
//   - the selection chooses it (the deployment is short, or it is a replacement whose winner has gone): it becomes
//     ready with no ranking, since ranking never holds back an Instance the deployment needs. Its mark stays while its
//     winner is countable, and is cleared once the winner has gone (readyUnmarked);
//   - the selection leaves it surplus: it is ranked against its winner alone while that winner can be ranked against
//     (rankCandidate), and otherwise it ends HedgeLost, kept as warm capacity. A winner that has gone, is draining,
//     is being deleted, or is already outranked (it carries the ranked-out mark of another Instance, whose rewriting
//     would leave two ready for one slot) cannot be ranked against.
//
// It is never ranked against another primary of its Provider.
func (p *pass) candidateStep(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, ready func() error) error {
	chosen, surplus := selectPrimaries(p.selection(in), p.desiredCount())
	w := p.candidateWinner(in)
	if !inSelection(surplus, in) || inSelection(chosen, in) {
		if w != nil {
			// Its winner serves beside it: it keeps its mark, which is what makes it a hedge of its own (hedgeKey). Without
			// the mark it would rejoin its winner's hedge, and the next resolveHedges would drain it as that hedge's loser.
			return ready()
		}
		return p.readyUnmarked(in, ready)
	}
	switch {
	case w == nil:
		return p.endCandidate(in, alloc, nil, fmt.Sprintf("hedge lost to %s, on Provider %s: its winner has left and the deployment's need is met without Instance %s, so it passed its checks and is kept as warm capacity", candidateOf(in), in.Labels[v1alpha1.LabelProvider], in.Name))
	case w.Annotations[annotationRankedOut] != "":
		return p.endCandidate(in, alloc, w, fmt.Sprintf("hedge lost to %s, on Provider %s: its winner has been outranked by another Instance, whose mark is not rewritten, so Instance %s passed its checks and is kept as warm capacity", w.Name, w.Labels[v1alpha1.LabelProvider], in.Name))
	}
	return p.rankCandidate(in, alloc, w, ready)
}

// rankCandidate ranks a kept started loser against its hedge's winner alone, whatever their classes (HBEP-7, Hedging's
// started losers), the winner being countable and not outranked:
//   - the winner has a live replacement: it is in its handover, which ranking leaves alone, so the loser ends with
//     HedgeLost;
//   - the winner is not ready (restarting): the loser waits, returning nil with no write, as one waits for a chosen
//     primary that is not ready; the pass that sees the winner change brings it back (HBEP-7: ranked once every chosen
//     primary on its Provider is ready or has ended);
//   - faster than the winner by more than 10% (outranks): it becomes ready and serves in the winner's place, which
//     drains and ends RankedOut (outrank);
//   - otherwise it ends with HedgeLost, kept as a qualified warm spare.
func (p *pass) rankCandidate(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, w *v1alpha1.Instance, ready func() error) error {
	provider := in.Labels[v1alpha1.LabelProvider]
	switch {
	case p.replaced(w):
		return p.endCandidate(in, alloc, w, fmt.Sprintf("hedge lost to %s, on Provider %s: it has a live replacement, a handover that ranking leaves alone, so Instance %s passed its checks on Provider %s and is kept as warm capacity", w.Name, w.Labels[v1alpha1.LabelProvider], in.Name, provider))
	case w.Status.Phase != v1alpha1.InstanceReady:
		return nil
	case !outranks(in, w):
		return p.endCandidate(in, alloc, w, fmt.Sprintf("hedge lost to %s, on Provider %s: Instance %s passed its checks on Provider %s and is not faster than it by more than 10%% in time per output token, so it is kept as warm capacity", w.Name, w.Labels[v1alpha1.LabelProvider], in.Name, provider))
	}
	return p.outrank(in, w, ready)
}

// endCandidate ends a kept started loser at its ranking point with HedgeLost, its Allocation kept as warm capacity
// (end with release false, so anchorIfLast writes idle-since). It passed every check, so startOutcomeOf counts a
// success for it. One replaced HedgeLost event regards it, with its winner related when it still exists (w is nil
// once it has gone).
func (p *pass) endCandidate(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, w *v1alpha1.Instance, msg string) error {
	if p.mem.once("hedge-lost/" + string(in.UID)) {
		var related client.Object
		if w != nil {
			related = w
		}
		p.r.Recorder.Event(in, related, eventReplaced, v1alpha1.ReasonHedgeLost, msg)
	}
	return p.end(in, alloc, v1alpha1.ReasonHedgeLost, msg, false)
}

// rankOut ends a candidate that did not outrank its peer, or had none: RankedOut, never ready, its Allocation kept
// as warm capacity (end with release false, so anchorIfLast writes idle-since). One replaced RankedOut event regards
// it, with its peer related when it had one.
func (p *pass) rankOut(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, peer *v1alpha1.Instance) error {
	msg := fmt.Sprintf("Instance %s passed its checks on Provider %s and has no peer to outrank: it is kept as warm capacity", in.Name, in.Labels[v1alpha1.LabelProvider])
	var related client.Object
	if peer != nil {
		related = peer
		msg = fmt.Sprintf("Instance %s passed its checks on Provider %s and is not faster than Instance %s by more than 10%% in time per output token: it is kept as warm capacity", in.Name, in.Labels[v1alpha1.LabelProvider], peer.Name)
		if in.Annotations[annotationRankedOut] == peer.Name {
			msg = fmt.Sprintf("Instance %s passed its checks again on Provider %s after Instance %s outranked it: it is kept as warm capacity", in.Name, in.Labels[v1alpha1.LabelProvider], peer.Name)
		}
	}
	if p.mem.once("ranked-out/" + string(in.UID)) {
		p.r.Recorder.Event(in, related, eventReplaced, v1alpha1.ReasonRankedOut, msg)
	}
	return p.end(in, alloc, v1alpha1.ReasonRankedOut, msg, false)
}

// readyThisPass reports whether an Instance that has passed its checks becomes ready in this very pass: its tunnel is
// connected, the agent has reported its warm-up done, the router's gates (when it is routed) are open, and the
// selection chooses it or holds no primary slot for it, so enterReady makes it ready and never holds it. Such an
// Instance is no longer judged by health.startTimeout (startSignal): a passed Instance that waited at its ranking point,
// behind a winner that was not ready, and is chosen only after its own start deadline, becomes ready, and is not failed
// with StartTimeout and released (the Fable review of #721, point 1). An Instance whose tunnel is not connected, or whose
// router has not admitted it, is judged by the deadline as before: TunnelNotConnected for a chosen Instance
// (HBEP-6, Key restriction).
func (p *pass) readyThisPass(in *v1alpha1.Instance, av agentView) bool {
	if p.md == nil || in.Status.ReadyTime != nil || !passedChecks(in) || !isTrue(in.Status.Conditions, v1alpha1.ConditionTunnelConnected) {
		return false
	}
	if routed(in) && (!routerProbePasses(in, av) || !routerAdmitsReady(in, av)) {
		return false
	}
	if !warmUpReported(in, av) {
		return false
	}
	_, surplus := selectPrimaries(p.selection(in), p.desiredCount())
	return !inSelection(surplus, in)
}

// warmUpReported reports whether the agent has reported the Instance's warm-up done at its current verification
// generation: every worker's, for several (the gate before the ranking point in starting and startingWorkers).
func warmUpReported(in *v1alpha1.Instance, av agentView) bool {
	ir := av.report
	if ir == nil || av.unreachable {
		return false
	}
	if several(in) {
		return everyWorker(workerNames(in), func(w string) bool {
			c := workerResults(ir, in, w)
			return c != nil && c.WarmUp != nil && c.WarmUp.Done
		})
	}
	c := ir.Checks
	return c != nil && c.VerificationGeneration == verificationGeneration(in) && c.WarmUp != nil && c.WarmUp.Done
}

// waitingAtRankingPoint reports whether the Instance has passed its checks, has its tunnel connected and is surplus
// in the selection: it waits at its ranking point, and health.startTimeout no longer judges it. It is keyed on the
// selection, never on the conditions alone: a chosen Instance with every check passed and TunnelConnected False is
// held by the tunnel's gate, and still fails at its deadline with TunnelNotConnected (HBEP-6, Key restriction). A
// surplus Instance whose tunnel is not connected could never serve either, so it keeps today's rule; so does one
// whose checks have not finished, which must fail at its deadline rather than wait for ever.
func (p *pass) waitingAtRankingPoint(in *v1alpha1.Instance) bool {
	if p.md == nil || !passedChecks(in) || !isTrue(in.Status.Conditions, v1alpha1.ConditionTunnelConnected) {
		return false
	}
	_, surplus := selectPrimaries(p.selection(in), p.desiredCount())
	return inSelection(surplus, in)
}

// clearStaleMarks clears the ranked-out mark of a peer whose candidate is no longer going to be ready: gone, ended,
// draining, or no longer passed (restarted, or failed a check), so a peer never starts to drain without a ready
// successor (step 6). A peer that already drains keeps its mark: the drain cannot be undone, and it ends RankedOut as
// it began. A candidate that is ready, or has passed and waits, keeps its mark: its own pass through starting makes
// it ready (enterReady, idempotent, the mark already written), which resumes an outranking a pass cut short.
func (p *pass) clearStaleMarks() error {
	var errs []error
	for _, in := range p.insts {
		name := in.Annotations[annotationRankedOut]
		if name == "" || ended(in) || in.Status.Phase == v1alpha1.InstanceDraining {
			continue
		}
		if c := p.instanceNamed(name); c != nil && countable(c) && (c.Status.Phase == v1alpha1.InstanceReady || passedChecks(c)) {
			continue
		}
		errs = append(errs, p.setMeta(in, func(n *v1alpha1.Instance) { delete(n.Annotations, annotationRankedOut) }))
	}
	return errors.Join(errs...)
}
