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
	chosen, surplus := selectPrimaries(p.selection(in), p.desiredCount())
	if !inSelection(surplus, in) || inSelection(chosen, in) {
		return ready()
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
	// The writes go in this order, so the intent is durable before either Instance changes: the peer's mark, the
	// candidate's becoming ready, then, once the front's probe has had its interval since the candidate became
	// healthy (frontAdmitsAt), the peer's drain (step 7, through scaleDownTargets), and its RankedOut event
	// (rankedOutPeer).
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
