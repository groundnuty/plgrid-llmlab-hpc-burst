package providerctl

// New for #343 (PR 4 of 5): the Provider controller's side of HBEP-6's relay
// keeper (beta; Dial-home modes, The relay keeper). With
// RelaySettings.KeeperSocket set, the Provider's leader runs no relay itself:
// it sends the keeper, over its Unix socket (internal/relaykeeper/wire),
// fenced desired sets of its running Allocations, level-triggered, and
// acknowledges each final end the keeper reports once it has written the
// Allocation's RelayEndedForGood mark, or once the write was refused. The
// rules are the science agent's rulings on #343's questions (10 Nov 2026):
//   - every set and acknowledgement carries the sender's Lease epoch, the
//     leaseTransitions of its own acquisition (RelayFence.Epoch);
//   - a set is complete only when every Allocation of the Provider that is
//     Running with a job ID has had an observation under this lead that
//     reached a decision, keep or drop; unresolved paths, or a login-host
//     claim the keeper has not answered, mean incomplete (Q6);
//   - a set carries its listing's age at sending and its validity, the poll
//     interval in force plus the renew deadline; the keeper computes the
//     expiry on its own clock (Q5);
//   - a final end that needs the mark writes RelayEndedForGood (reason
//     RelayFailed) and raises RelayFailed once, a superseded end too (Q3),
//     and only when the mark is newly written: a failed write raises nothing
//     until a write succeeds (Q12);
//   - a refusal for a login host another Provider holds makes the Provider's
//     Ready False with LoginHostInUse (claim, read by the status loop);
//   - a copy that loses the lead stops sending sets and acknowledgements
//     before it releases the Lease (stopKeeper, the fence's release hook);
//   - while no exchange with the keeper has succeeded under the current lead,
//     or none has for longer than a set's validity, the Provider is Ready
//     False with RelayKeeperUnavailable and admits no new Allocation, and
//     running ones are left as they are; the next successful exchange makes
//     it Ready again (HBEP-6, The relay keeper, as #360 amends it).
// No message carries a secret: the set names the paths of the Provider's key
// and known-hosts file and of each Allocation's staged agent, never their
// content.
//
// Provenance: new, for #343. Its mark write follows persistRelayFailed
// (relayfailed.go, #304), with the outcome the acknowledgement needs.

import (
	"context"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// keeperSendInterval is how often the leader sends its set when nothing
// changed: well inside the shortest validity (the 5 s poll interval plus the
// 10 s renew deadline), so a leader's set is renewed soon after each listing.
const keeperSendInterval = 2 * time.Second

// supersededInBeta is a superseded end's cause in the mark and the event: in
// beta no newer owner of a channel exists (HBEP-6, The relay keeper).
const supersededInBeta = "superseded, which is unexpected in beta"

// keeperState is one Provider controller's side of the keeper: what its
// observations decided, and what the keeper last answered, under this lead.
type keeperState struct {
	socket   string
	interval time.Duration

	// sending is held for each exchange with the keeper, from the lead's
	// check to the reply, and by the lead's end (endLeadAfterSends): so no
	// exchange starts once the lead has ended, and none is in flight when
	// the end returns.
	sending sync.Mutex
	// ending is set before the lead's end takes sending, so an exchange
	// queued behind one in flight does not begin (the review of PR 360).
	ending atomic.Bool

	mu sync.Mutex
	// gen numbers this controller's leads (runKeeper): a decision counts for
	// a set's completeness only under the lead that made it.
	gen int
	// decisions are the observations' decisions, by tag: a kept Allocation's
	// entry, or nil for a drop.
	decisions map[string]keeperDecision
	// answered is set once the keeper accepted an unexpired set of this
	// lead: the login-host claim is answered (Q6).
	answered bool
	// login is the login host the keeper last named, and inUse its refusal
	// of the Provider's set for a host another Provider holds.
	login, inUse string
	// lastErr is the last failed exchange's text, so a keeper that stays
	// unreachable is logged once, not at every send.
	lastErr string
	// okGen is the lead under which an exchange last succeeded, at okAt by
	// now, the controller's clock; validity is the last set's (the poll
	// interval in force plus the renew deadline). seen is what the status
	// loop's latest pass read of the keeper (observe); status wakes it while
	// that differs from the keeper's state now.
	now      func() time.Time
	okGen    int
	okAt     time.Time
	validity time.Duration
	renew    time.Duration
	seen     string
	status   chan struct{}
	// deadline wakes the status loop at okAt plus the validity, when the
	// keeper turns unreachable with no exchange to show it; off once the
	// lead's goroutines have returned.
	deadline    *time.Timer
	deadlineOff bool
	// provUID and provCreated are the Provider's, as this controller first
	// read them; replaced says why it stops, once a read shows another UID
	// under the same name (keeperProvider).
	provUID     string
	provCreated time.Time
	replaced    string
	// built, when set (by a test), is told each time a set is built, before
	// it waits to be sent.
	built func()
	// ends are the final ends reported and not handled yet, by their
	// identity; acks wakes their worker.
	ends map[wire.FinalEnd]bool
	acks chan struct{}
	kick chan struct{}
	// done is closed once this lead's sender and acknowledger have
	// returned; nil while none runs. stopped is set by stopKeeper, after
	// which no lead of this controller sends again.
	done    chan struct{}
	stopped bool
}

// keeperDecision is one observation's decision.
type keeperDecision struct {
	want *wire.Entry
	gen  int
}

// newKeeperRelayState is newRelayState in keeper mode: no relay owner, no ssh
// client, and a login-host register of its own, so the process's shared one
// sees nothing of this Provider (the keeper owns the claims).
func newKeeperRelayState(o Options, s RelaySettings) (*relayState, error) {
	if !filepath.IsAbs(s.KeeperSocket) || len(s.KeeperSocket) > agentapi.MaxRelaySocketPath {
		return nil, fmt.Errorf("providerctl: the relay keeper's socket must be an absolute path of at most %d bytes", agentapi.MaxRelaySocketPath)
	}
	if s.KeeperSendInterval <= 0 {
		s.KeeperSendInterval = keeperSendInterval
	}
	if s.RenewDeadline <= 0 {
		s.RenewDeadline = defaultRenewDeadline
	}
	s.Hosts = hostdial.NewHosts()
	k := &keeperState{socket: s.KeeperSocket, interval: s.KeeperSendInterval, decisions: map[string]keeperDecision{},
		ends: map[wire.FinalEnd]bool{}, acks: make(chan struct{}, 1), kick: make(chan struct{}, 1),
		now: o.Clock.Now, validity: o.PollInterval + s.RenewDeadline, renew: s.RenewDeadline, status: make(chan struct{}, 1)}
	return &relayState{settings: s, provider: o.Provider, hosts: s.Hosts, keeper: k}, nil
}

// claim is claimLogin in keeper mode: the login host the keeper last named,
// and a refusal while the claim is not this Provider's: another Provider
// holds the host, or the keeper has not accepted a set of this lead yet,
// the pending case alpha's claimLogin also refuses, so Create submits nothing
// meanwhile (the review of PR 360).
func (k *keeperState) claim() (login, inUse string, err error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	switch {
	case k.replaced != "":
		return k.login, k.replaced, nil
	case k.inUse != "":
		return k.login, k.inUse, nil
	case !k.answered:
		return k.login, claimPending, nil
	}
	return k.login, "", nil
}

// claimPending is the refusal while the keeper has not accepted a set of this
// lead.
const claimPending = "the relay keeper has not accepted this Provider's login-host claim under this copy's lead yet: one host-dialed Provider per login host (HBEP-6, Capacity)"

// decide records an observation's decision for tag: want is the entry to
// keep, nil a drop. A change wakes the sender.
func (k *keeperState) decide(tag string, want *wire.Entry) {
	k.mu.Lock()
	old, had := k.decisions[tag]
	d := keeperDecision{want: want, gen: k.gen}
	k.decisions[tag] = d
	k.mu.Unlock()
	if !had || old.gen != d.gen || (old.want == nil) != (want == nil) || (want != nil && *old.want != *want) {
		wake(k.kick)
	}
}

func wake(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// syncKeeper is syncRelays in keeper mode: an Allocation whose recorded job
// runs is kept, with the entry the keeper needs; one that is being deleted,
// or whose job is not running, is dropped. An observation that cannot read
// the login node's paths decides nothing, so the set stays incomplete (Q6).
// The mark needs no fallback here: the keeper keeps every final end until
// the leader acknowledges it.
func (e *external) syncKeeper(ctx context.Context, a *v1alpha1.Allocation, tag string) {
	k := e.s.relays.keeper
	o := a.Status.AtProvider
	if meta.WasDeleted(a) || o.Phase != v1alpha1.AllocationRunning || o.JobID == "" || o.JobCompleting || a.Spec.ForProvider.Batch == nil {
		k.decide(tag, nil)
		return
	}
	paths, err := e.p.Paths(ctx)
	if err != nil {
		e.s.o.Log.Info("the relays wait for the login node's paths", "tag", tag, "error", err)
		return
	}
	k.decide(tag, &wire.Entry{
		UID: string(a.UID), Tag: tag, JobID: o.JobID,
		AllocDir:     path.Join(paths.Staging, agentapi.AllocationDir(tag)),
		AgentPath:    path.Join(paths.Staging, agentapi.StagedAgentPath(a.Spec.ForProvider.Batch.AgentDigest)),
		Gen:          credentialsGen(e.v.SSH),
		EndedForGood: marked(a),
	})
}

// runKeeper is RunRelays in keeper mode: for this lead it sends the
// Provider's sets and acknowledges the keeper's final ends, until ctx ends
// or the fence's release hook ends the lead (stopKeeper), then waits for
// both to return.
func (c *Controller) runKeeper(ctx context.Context) error {
	r := c.shared.relays
	k := r.keeper
	defer r.close()
	k.mu.Lock()
	if k.stopped {
		k.mu.Unlock()
		<-ctx.Done()
		return nil
	}
	r.startLead(ctx)
	lead := r.leadContext()
	k.ending.Store(false)
	k.deadlineOff = false
	k.gen++
	k.answered, k.login, k.inUse = false, "", ""
	done := make(chan struct{})
	k.done = done
	k.mu.Unlock()
	var wg sync.WaitGroup
	var replaced error
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := c.shared.sendSets(lead); err != nil {
			replaced = err
			k.endLeadAfterSends(r)
		}
	}()
	go func() { defer wg.Done(); c.shared.ackEnds(lead) }()
	go func() { wg.Wait(); close(done) }()
	select {
	case <-ctx.Done():
	case <-done:
	}
	k.endLeadAfterSends(r)
	<-done
	k.stopDeadline()
	if replaced != nil {
		// The Provider was deleted and created again under its name: this
		// controller stops, so its manager is started again, afresh, by the
		// supervisor (keeper mode only; the review of PR 360).
		return replaced
	}
	<-ctx.Done()
	return nil
}

// endLeadAfterSends ends the lead once no exchange with the keeper is in
// flight: one that began under the lead runs to its reply, bounded by
// wire.ExchangeTimeout, so what this copy sent has reached the keeper and been
// answered before the Lease is released; none begins after.
func (k *keeperState) endLeadAfterSends(r *relayState) {
	k.ending.Store(true)
	k.sending.Lock()
	defer k.sending.Unlock()
	r.endLead()
}

// exchange is one exchange with the keeper under lead: none begins once lead
// has ended, and one that began is not cut by its end (endLeadAfterSends
// waits for it).
func (k *keeperState) exchange(lead context.Context, kind wire.Kind, body, reply any) error {
	k.sending.Lock()
	defer k.sending.Unlock()
	if k.fenced(lead) {
		return errLeadEnded
	}
	return wire.Exchange(context.WithoutCancel(lead), k.socket, kind, body, reply)
}

// fenced reports whether no exchange may begin: the lead has ended or its
// end has begun, or the Provider was found replaced. The caller holds
// sending.
func (k *keeperState) fenced(lead context.Context) bool {
	if k.ending.Load() || lead.Err() != nil {
		return true
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.replaced != ""
}

// exchangeSet is exchange for a set built from the listing read at listed:
// its age is stamped once sending is held, right before it is sent, so a
// wait behind another exchange is counted in it and never extends the set's
// expiry (#343's Q5; the review of PR 360).
func (k *keeperState) exchangeSet(lead context.Context, set wire.DesiredSet, listed time.Time, rep *wire.Report) error {
	k.sending.Lock()
	defer k.sending.Unlock()
	if k.fenced(lead) {
		return errLeadEnded
	}
	// The exchange's own bound is added, so the time the set then takes to
	// validate, dial, cross and be read is counted too: the keeper's expiry
	// can only come earlier than the listing's (the delta review of PR 360).
	age := k.now().Sub(listed)
	if age < 0 {
		age = 0
	}
	set.ListingAgeMS = (age + wire.ExchangeTimeout).Milliseconds()
	if err := set.Validate(); err != nil {
		return fmt.Errorf("%w: %v", errMalformedSet, err)
	}
	return wire.Exchange(context.WithoutCancel(lead), k.socket, wire.KindSet, set, rep)
}

// errMalformedSet: a set this controller built fails the wire's own checks,
// a bug; it is not sent.
var errMalformedSet = errors.New("the desired set for the relay keeper is malformed")

// decided reports whether a set's exchange succeeded: the keeper answered
// with its report, or refused the set for its login host or its epoch, which
// it decided on. A transport failure, and a refusal for its own state file,
// the protocol's version or a malformed request, is no success. Only a set's
// exchange counts: its reply carries the claim's decision, so the keeper's
// reachability and the claim change together (received); an ack's carries
// none, so an ack that reaches a keeper after an outage reopens nothing (the
// delta review of PR 360).
func decided(err error) bool {
	var refused *wire.RefusedError
	return err == nil || (errors.As(err, &refused) && (refused.Refused.Reason == wire.RefusedLoginHost || refused.Refused.Reason == wire.RefusedEpoch))
}

// admission says why no new Allocation may be submitted now, or "": the
// keeper unreachable, or the claim not granted, read as one state (the delta
// review of PR 360).
func (k *keeperState) admission(now time.Time) string {
	k.mu.Lock()
	defer k.mu.Unlock()
	if why := k.unavailableLocked(now); why != "" {
		return why
	}
	switch {
	case k.inUse != "":
		return k.inUse
	case !k.answered:
		return claimPending
	}
	return ""
}

// keeperAdmission is admission outside keeper mode too: always "" there.
func (s *shared) keeperAdmission(now time.Time) string {
	if r := s.relays; r != nil && r.keeper != nil {
		return r.keeper.admission(now)
	}
	return ""
}

// armDeadlineLocked sets the timer for okAt plus the validity, by the
// controller's clock; the caller holds mu.
func (k *keeperState) armDeadlineLocked() {
	if k.deadline != nil {
		k.deadline.Stop()
	}
	if k.deadlineOff {
		return
	}
	d := k.okAt.Add(k.validity).Sub(k.now())
	if d < 0 {
		d = 0
	}
	k.deadline = time.AfterFunc(d+time.Millisecond, k.atDeadline)
}

// atDeadline wakes the status loop when the keeper has turned unreachable by
// the passing of time alone, and waits again while it has not, which only a
// clock that the timer does not follow (a test's) leaves.
func (k *keeperState) atDeadline() {
	k.noteStatus()
	k.mu.Lock()
	defer k.mu.Unlock()
	if !k.deadlineOff && k.unavailableLocked(k.now()) == "" {
		k.armDeadlineLocked()
	}
}

// stopDeadline turns the timer off once the lead's goroutines have returned.
func (k *keeperState) stopDeadline() {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.deadlineOff = true
	if k.deadline != nil {
		k.deadline.Stop()
	}
}

// unavailable says why the keeper counts as unreachable now, or "": no
// exchange has succeeded under this lead, or none for longer than a set's
// validity.
func (k *keeperState) unavailable(now time.Time) string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.unavailableLocked(now)
}

func (k *keeperState) unavailableLocked(now time.Time) string {
	switch {
	case k.replaced != "":
		return k.replaced
	case k.gen == 0 || k.okGen != k.gen:
		return "no exchange with the relay keeper has succeeded under this copy's lead of the Provider"
	case now.Sub(k.okAt) > k.validity:
		return fmt.Sprintf("no exchange with the relay keeper has succeeded since %s, longer than a desired set's validity (%s)", k.okAt.UTC().Format(time.RFC3339), k.validity)
	}
	return ""
}

// noteStatus wakes the status loop while what it reads of the keeper differs
// from what its latest pass read: whether the keeper is reachable and how the
// claim stands, so Ready follows at once, not at the next poll, on the first
// answer above all (the review of PR 360). It compares with what the pass
// read, not with what an earlier call computed: a pass that ran after the
// keeper turned unreachable by time alone has read that, even when no call
// computed it, so the keeper's return still wakes the loop.
func (k *keeperState) noteStatus() {
	k.mu.Lock()
	differs := k.keyLocked(k.now()) != k.seen
	k.mu.Unlock()
	if differs {
		wake(k.status)
	}
}

// keyLocked is what the status loop reads of the keeper at now; the caller
// holds mu.
func (k *keeperState) keyLocked(now time.Time) string {
	return fmt.Sprintf("%t|%t|%s|%s", k.unavailableLocked(now) == "", k.answered, k.inUse, k.replaced)
}

// observe records what a status pass is about to read of the keeper. A
// change after it makes noteStatus wake the loop again.
func (k *keeperState) observe(now time.Time) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.seen = k.keyLocked(now)
}

// keeperObserve is observe for the status loop's pass; nothing outside keeper
// mode.
func (s *shared) keeperObserve() {
	if r := s.relays; r != nil && r.keeper != nil {
		r.keeper.observe(s.o.Clock.Now())
	}
}

// keeperStatus is the status loop's wake-up in keeper mode; nil, which
// never fires, otherwise.
func (l *statusLoop) keeperStatus() <-chan struct{} {
	if r := l.s.relays; r != nil && r.keeper != nil {
		return r.keeper.status
	}
	return nil
}

// keeperUnavailable is why the Provider is Ready False with
// RelayKeeperUnavailable now, or "": always "" outside keeper mode.
func (s *shared) keeperUnavailable(now time.Time) string {
	if r := s.relays; r != nil && r.keeper != nil {
		return r.keeper.unavailable(now)
	}
	return ""
}

// errLeadEnded: this copy's lead has ended, so nothing is sent.
var errLeadEnded = errors.New("this copy no longer leads the Provider: nothing is sent to the relay keeper")

// stopKeeper is StopRelays in keeper mode, the fence's release hook: it ends
// this copy's lead once no exchange is in flight, and returns once the sender
// and the acknowledger have returned, so nothing is sent after it, before the
// Lease is released (HBEP-6: a copy that loses the lead stops sending sets
// and acknowledgements before it releases the Lease). No lead of this
// controller starts after it.
func (c *Controller) stopKeeper() {
	r := c.shared.relays
	k := r.keeper
	k.mu.Lock()
	k.stopped = true
	done := k.done
	k.mu.Unlock()
	k.endLeadAfterSends(r)
	if done != nil {
		<-done
	}
}

// sendSets sends the Provider's set at once, then on every change and every
// interval, while lead lasts. It returns errProviderReplaced once the
// Provider is found created again under its name.
func (s *shared) sendSets(lead context.Context) error {
	k := s.relays.keeper
	t := time.NewTicker(k.interval)
	defer t.Stop()
	for {
		if err := s.sendSet(lead); errors.Is(err, errProviderReplaced) {
			k.noteStatus()
			return err
		}
		k.noteStatus()
		select {
		case <-lead.Done():
			return nil
		case <-t.C:
		case <-k.kick:
		}
	}
}

// sendSet builds the Provider's set and sends it, while lead lasts, and
// keeps what the keeper answers.
func (s *shared) sendSet(lead context.Context) error {
	if lead.Err() != nil {
		return nil
	}
	k := s.relays.keeper
	k.mu.Lock()
	gen, built := k.gen, k.built
	k.mu.Unlock()
	set, listed, err := s.desiredSet(lead, gen)
	if errors.Is(err, errProviderReplaced) {
		s.o.Log.Info("the Provider was created again under its name: this controller stops, and sends nothing more", "error", err)
		return err
	}
	if err != nil {
		if lead.Err() == nil {
			s.o.Log.Debug("no desired set is sent to the relay keeper yet", "error", err)
		}
		return nil
	}
	if built != nil {
		built()
	}
	var rep wire.Report
	err = k.exchangeSet(lead, set, listed, &rep)
	switch {
	case errors.Is(err, errLeadEnded):
		return nil
	case errors.Is(err, errMalformedSet):
		// A set this controller built that the keeper would refuse as
		// malformed is a bug: it is loud here, never sent.
		s.o.Log.Info("the desired set for the relay keeper is not sent", "error", err)
		return nil
	}
	k.received(gen, rep, err, s)
	k.noteStatus()
	if err == nil {
		k.queueEnds(rep.Ends)
	}
	return nil
}

// received keeps the keeper's answer to a set of lead gen.
func (k *keeperState) received(gen int, rep wire.Report, err error, s *shared) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if gen != k.gen {
		return
	}
	var refused *wire.RefusedError
	text := ""
	if err != nil {
		text = err.Error()
	}
	repeated := text == k.lastErr
	k.lastErr = text
	if decided(err) {
		// Reachability and the claim's decision change in this one critical
		// section, and the status loop is woken only after (sendSet), so no
		// reader sees the keeper back with an old claim (the delta review of
		// PR 360).
		k.okGen, k.okAt = k.gen, k.now()
		k.armDeadlineLocked()
	}
	switch {
	case err == nil:
		k.login = rep.LoginHost
		// An expired set moved the fence and changed nothing else: the
		// keeper did not decide the claim, so it is pending again.
		k.answered = !rep.Expired
		if !rep.Expired {
			k.inUse = ""
		}
	case errors.As(err, &refused) && refused.Refused.Reason == wire.RefusedLoginHost:
		k.answered = false
		k.login, k.inUse = refused.Refused.LoginHost, loginHostInUse(refused.Refused.LoginHost, refused.Refused.Holder, false)
	case repeated:
	case errors.As(err, &refused) && refused.Refused.Reason == wire.RefusedEpoch:
		s.o.Log.Info("the relay keeper refused this copy's set: its Lease epoch is below the keeper's fence", "highestEpoch", refused.Refused.HighestEpoch)
	default:
		s.o.Log.Info("the desired set did not reach the relay keeper", "error", err)
	}
}

// desiredSet is the Provider's set for lead gen (HBEP-6, The interface; #343's
// Q4, Q5 and Q6).
func (s *shared) desiredSet(ctx context.Context, gen int) (wire.DesiredSet, time.Time, error) {
	k := s.relays.keeper
	if s.relays.settings.Epoch == nil {
		return wire.DesiredSet{}, time.Time{}, errors.New("no epoch reader is set")
	}
	epoch, err := s.relays.settings.Epoch(ctx)
	if err != nil {
		return wire.DesiredSet{}, time.Time{}, err
	}
	uid, created, err := s.keeperProvider(ctx)
	if err != nil {
		return wire.DesiredSet{}, time.Time{}, err
	}
	p, _ := s.h.current()
	cfg, ok := s.h.ssh()
	if p == nil || !ok {
		return wire.DesiredSet{}, time.Time{}, errors.New("the Provider's platform is not made yet")
	}
	// The listing's time is read before the entries are checked against
	// the listing, so the age sent is never below the listing's they rest on.
	listed := p.Health().LastListing
	if listed.IsZero() {
		return wire.DesiredSet{}, time.Time{}, errors.New("no queue listing has been read yet")
	}
	var list v1alpha1.AllocationList
	if err := s.reader.List(ctx, &list); err != nil {
		return wire.DesiredSet{}, time.Time{}, fmt.Errorf("list the Allocations: %w", err)
	}
	k.mu.Lock()
	decisions := make(map[string]keeperDecision, len(k.decisions))
	for tag, d := range k.decisions {
		decisions[tag] = d
	}
	complete := k.answered
	k.mu.Unlock()

	gen16 := credentialsGen(cfg)
	var entries []wire.Entry
	seen := map[string]bool{}
	for i := range list.Items {
		a := &list.Items[i]
		if a.Spec.ProviderConfigReference == nil || a.Spec.ProviderConfigReference.Name != s.o.Provider {
			continue
		}
		tag, o := meta.GetExternalName(a), a.Status.AtProvider
		seen[tag] = true
		running := o.Phase == v1alpha1.AllocationRunning && o.JobID != ""
		d, decided := decisions[tag]
		if running && (!decided || d.gen != gen) {
			complete = false // not observed under this lead (Q6)
		}
		if !running || !decided || d.want == nil {
			continue
		}
		w := *d.want
		switch {
		case w.UID != string(a.UID) || w.JobID != o.JobID || w.Gen != gen16:
			// Decided for another job or other credentials: it waits for
			// its next observation, and nothing is dropped meanwhile.
			complete = false
			continue
		case meta.WasDeleted(a) || o.JobCompleting:
			continue // dropped
		}
		if !jobRunsInListing(ctx, p, w) {
			// HBEP-4's absence grace: not shown running in the listing, so
			// not sent, and the set drops nothing.
			complete = false
			continue
		}
		w.EndedForGood = w.EndedForGood || marked(a)
		entries = append(entries, w)
	}
	k.prune(seen)
	sort.Slice(entries, func(i, j int) bool { return entries[i].Tag < entries[j].Tag })
	if len(entries) > wire.MaxAllocations {
		entries, complete = entries[:wire.MaxAllocations], false
	}
	// The age is stamped when the set is sent (exchangeSet).
	validity := listingMaxAge(p, s.o.PollInterval) + k.renew
	k.mu.Lock()
	k.validity = validity
	k.mu.Unlock()
	return wire.DesiredSet{
		Provider: s.o.Provider, ProviderUID: uid, ProviderCreated: created, LeaseEpoch: epoch,
		Complete: complete, ListedAt: listed.UTC(),
		ValidForMS: validity.Milliseconds(),
		SSH: wire.SSH{HostName: cfg.HostName, User: cfg.User, Port: cfg.Port,
			IdentityFile: cfg.IdentityFile, KnownHostsFile: cfg.UserKnownHostsFile},
		Allocations: entries,
	}, listed, nil
}

// prune forgets the decisions of Allocations that are gone.
func (k *keeperState) prune(seen map[string]bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	for tag := range k.decisions {
		if !seen[tag] {
			delete(k.decisions, tag)
		}
	}
}

// jobRunsInListing is the check before each start that alpha's relays make
// (hostdial's jobIsOurs), made before the set is sent: the Provider's latest
// queue listing shows w's job RUNNING under its tag.
func jobRunsInListing(ctx context.Context, p Platform, w wire.Entry) bool {
	jobs, err := p.ListByTag(ctx, w.Tag)
	if err != nil {
		return false
	}
	for _, j := range jobs {
		if j.ID == w.JobID && j.Name == w.Tag && !j.Completing && j.Phase == v1alpha1.AllocationRunning {
			return true
		}
	}
	return false
}

// listingMaxAge is the poll interval in force, from a Platform that tells it
// (slurm.Batch.ListingMaxAge), else poll.
func listingMaxAge(p Platform, poll time.Duration) time.Duration {
	if m, ok := p.(interface{ ListingMaxAge() time.Duration }); ok {
		if d := m.ListingMaxAge(); d > 0 {
			return d
		}
	}
	return poll
}

// keeperProvider is the Provider's UID and creationTimestamp, read from the
// API server for every set. The supervisor runs a Provider's manager by name,
// so a Provider deleted and created again between its polls keeps this
// controller: a UID or creationTimestamp other than the one it first read is
// errProviderReplaced, and this controller stops rather than send the old
// identity, or the new one under the old one's state (the review of PR 360;
// HBEP-6, The interface: a claim made under another UID belongs to another
// Provider). Alpha's controllers are unchanged.
func (s *shared) keeperProvider(ctx context.Context) (string, time.Time, error) {
	var prov v1alpha1.Provider
	if err := s.reader.Get(ctx, client.ObjectKey{Name: s.o.Provider}, &prov); err != nil {
		return "", time.Time{}, fmt.Errorf("the Provider's UID: %w", err)
	}
	uid, created := string(prov.UID), prov.CreationTimestamp.Time
	if uid == "" || created.IsZero() {
		return "", time.Time{}, errors.New("the Provider has no UID or creationTimestamp yet")
	}
	k := s.relays.keeper
	k.mu.Lock()
	defer k.mu.Unlock()
	switch {
	case k.replaced != "":
		return "", time.Time{}, errProviderReplaced
	case k.provUID == "":
		k.provUID, k.provCreated = uid, created
	case k.provUID != uid || !k.provCreated.Equal(created):
		// The fence first, in the same critical section as the detection:
		// no exchange begins from here (the delta review of PR 360).
		k.ending.Store(true)
		k.replaced = fmt.Sprintf("Provider %s was deleted and created again (UID %s, was %s): its controller stops, and the next one starts afresh", s.o.Provider, uid, k.provUID)
		return "", time.Time{}, errProviderReplaced
	}
	return uid, created, nil
}

// errProviderReplaced: the Provider of this controller's name has another UID
// now.
var errProviderReplaced = errors.New("the Provider was deleted and created again under its name")

// ssh is the Provider's current ssh settings, and whether any were read.
func (h *holder) ssh() (slurm.SSHConfig, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.v.SSH, h.platform != nil
}

// queueEnds hands the reported ends to the acknowledger.
func (k *keeperState) queueEnds(ends []wire.FinalEnd) {
	if len(ends) == 0 {
		return
	}
	k.mu.Lock()
	for _, e := range ends {
		k.ends[e] = true
	}
	k.mu.Unlock()
	wake(k.acks)
}

// nextEnd takes one queued end.
func (k *keeperState) nextEnd() (wire.FinalEnd, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	for e := range k.ends {
		delete(k.ends, e)
		return e, true
	}
	return wire.FinalEnd{}, false
}

// ackEnds decides the reported final ends, one at a time, while lead lasts,
// apart from the sender, so a slow mark write never holds back a set. An end
// that is not decided (an API failure, the lead lost) gets no ack: the keeper
// reports it again in the next set's reply.
func (s *shared) ackEnds(lead context.Context) {
	k := s.relays.keeper
	for {
		select {
		case <-lead.Done():
			return
		case <-k.acks:
		}
		for {
			end, ok := k.nextEnd()
			if !ok || lead.Err() != nil {
				break
			}
			s.ackEnd(lead, end)
		}
	}
}

// keeperEndFailure is the failure an end records in the mark and the event.
func keeperEndFailure(end wire.FinalEnd) relayFailure {
	cause := end.Cause
	if end.Kind == wire.EndSuperseded {
		cause = supersededInBeta
	}
	return relayFailure{uid: end.UID, jobID: end.JobID, msg: relayFailedMessage(end.Channel, cause), at: end.At}
}

// ackEnd decides one final end: it writes the mark on the end's Allocation,
// raises RelayFailed when the mark is newly written, and acknowledges the end
// as marked; or it acknowledges the refusal with its reason. A failed write
// raises and acknowledges nothing (#343's Q12).
func (s *shared) ackEnd(lead context.Context, end wire.FinalEnd) {
	ack := wire.Ack{Provider: s.o.Provider, UID: end.UID, JobID: end.JobID, Channel: end.Channel, Epoch: end.Epoch, Seq: end.Seq}
	key, found, err := s.allocationByUID(lead, end.UID)
	if err != nil {
		s.o.Log.Info("a relay's final end waits: the Allocations cannot be read", "error", err)
		return
	}
	f := keeperEndFailure(end)
	if !found {
		ack.Outcome, ack.Reason = wire.OutcomeRefused, wire.AckDeleted
	} else {
		wrote, reason, cur, err := s.persistKeeperMark(lead, key, f)
		if err != nil {
			s.o.Log.Info("the RelayEndedForGood mark waits for the keeper's next report", "allocation", key.Name, "error", err)
			return
		}
		if reason != "" {
			ack.Outcome, ack.Reason = wire.OutcomeRefused, reason
		} else {
			if wrote {
				if lead.Err() != nil {
					return
				}
				s.o.Recorder.Event(cur, event.Warning(v1alpha1.EventReasonRelayFailed, errors.New(f.msg)))
			}
			ack.Outcome = wire.OutcomeMarked
		}
	}
	epoch, err := s.relays.settings.Epoch(lead)
	if err != nil || lead.Err() != nil {
		return
	}
	ack.LeaseEpoch = epoch
	if err := ack.Validate(); err != nil {
		s.o.Log.Info("the acknowledgement for the relay keeper is malformed; it is not sent", "error", err)
		return
	}
	var acked wire.Acked
	if err := s.relays.keeper.exchange(lead, wire.KindAck, ack, &acked); err != nil && !errors.Is(err, errLeadEnded) {
		s.o.Log.Info("the acknowledgement did not reach the relay keeper", "error", err)
	}
}

// allocationByUID finds the Allocation of uid among the Provider's, from the
// API server: none is found once it is gone, or re-created under another UID.
func (s *shared) allocationByUID(ctx context.Context, uid string) (client.ObjectKey, bool, error) {
	var list v1alpha1.AllocationList
	if err := s.reader.List(ctx, &list); err != nil {
		return client.ObjectKey{}, false, err
	}
	for i := range list.Items {
		a := &list.Items[i]
		if string(a.UID) == uid && a.Spec.ProviderConfigReference != nil && a.Spec.ProviderConfigReference.Name == s.o.Provider {
			return client.ObjectKeyFromObject(a), true, nil
		}
	}
	return client.ObjectKey{}, false, nil
}

// persistKeeperMark writes f's mark under the rules of persistRelayFailed:
// on the Allocation of f's UID and job, Running, not completing and not being
// deleted, while lead lasts, checked again just before the patch, under the
// read's resourceVersion, a conflict read again. It reports whether this
// write made the mark (wrote), or the acknowledgement's reason when the mark
// is refused; a mark that is there already is neither, and is acknowledged as
// marked. err is a write that did not happen: an API failure, or the lead
// lost.
func (s *shared) persistKeeperMark(lead context.Context, key client.ObjectKey, f relayFailure) (wrote bool, reason string, cur *v1alpha1.Allocation, err error) {
	ctx, cancel := context.WithTimeout(lead, markWriteTimeout)
	defer cancel()
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		wrote, reason = false, ""
		c := &v1alpha1.Allocation{}
		if err := s.reader.Get(ctx, key, c); err != nil {
			if kerrors.IsNotFound(err) {
				reason = wire.AckDeleted
				return nil
			}
			return err
		}
		cur = c
		if checkMark(c, f) != nil {
			reason = wire.AckAnotherJob
			if string(c.UID) != f.uid {
				reason = wire.AckAnotherUID
			}
			return nil
		}
		if marked(c) {
			return nil
		}
		if checkRunning(c) != nil {
			reason = wire.AckNotRunning
			if meta.WasDeleted(c) {
				reason = wire.AckDeleted
			}
			return nil
		}
		orig := c.DeepCopy()
		setRelayEndedForGood(c, f)
		if lead.Err() != nil {
			return fmt.Errorf("%w: this copy of the controller no longer leads", errMarkRefused)
		}
		if err := s.kube.Status().Patch(ctx, c, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}
		wrote = true
		return nil
	})
	return wrote, reason, cur, err
}
