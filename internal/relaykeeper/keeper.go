// Package relaykeeper is HBEP-6's relay keeper (beta; Dial-home modes, The
// relay keeper; issue #343): one process per control-plane host holds every
// host-dialed relay, so a controller restart or a hand-over cuts no serving
// stream. This package is the keeper's decisions: its state file, its relay
// epoch E, the fencing of the controllers' desired sets and acknowledgements
// by their Lease epochs, the sets' completeness and expiry, the final ends it
// keeps until the leader decides them, and the login-host claims. The relays
// themselves run in a Runner: in the keeper's process, one hostdial.Owner per
// Provider.
//
// The rules are HBEP-6's, with the science agent's rulings on #343's
// questions of 10 Nov 2026:
//   - E is 2³¹ plus the keeper's start count, on disk before any relay of the
//     run starts (OpenState).
//   - A set or an acknowledgement whose Lease epoch is lower than the
//     Provider's highest accepted is refused; a higher one is written and
//     synced before it takes effect.
//   - A set expires one poll interval plus the renew deadline after the
//     listing it rests on, by the keeper's own monotonic clock (Q5). Under no
//     unexpired set the Provider's relays are kept, and none starts or
//     restarts, after a keeper restart too, since sets are not persisted. A
//     set that has expired on arrival moves the fence and nothing else.
//   - Only a complete set (Q6) stops a relay, by leaving its Allocation out.
//   - A final end that needs the mark (failed, or superseded) is written and
//     synced, and reported in each of the Provider's reports until the leader
//     acknowledges it. A decided end stays recorded until its Allocation
//     leaves a complete set (Q1), and its channel is never restarted. An
//     Allocation that carries RelayEndedForGood gets no new relay.
//   - One Provider per login host (Q2): see claims.go.
//
// Provenance: new package, for #343.
package relaykeeper

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Options configure a Keeper.
type Options struct {
	// State is the state file's path.
	State string
	// Runner makes the Runner for the keeper's epoch E, once E is on disk.
	Runner func(epoch int64) (Runner, error)
	// Now is the clock; nil means time.Now. The expiry is computed from it,
	// so a real clock's monotonic reading makes it immune to steps of the
	// wall clock.
	Now func() time.Time
	// ClaimLapse is how long a claim on a login host lasts with no relay held
	// there and no unexpired set from its holder; zero means
	// DefaultClaimLapse.
	ClaimLapse time.Duration
	// Logger reports refusals and the state file's failures; nil means none.
	Logger *slog.Logger
}

// Keeper is the relay keeper's decisions. Its Handle answers the socket's
// requests (wire.Serve).
type Keeper struct {
	o       Options
	runner  Runner
	epoch   int64
	started time.Time

	mu       sync.Mutex
	st       *State
	sets     map[string]*heldSet              // Provider → its latest accepted set; not persisted
	wishes   map[string]wish                  // Provider → its latest unexpired set past the fence, accepted or not
	wanted   map[string]map[string]wire.Entry // Provider → tag → the Allocation kept
	starts   map[string]map[string]int        // Provider → UID/channel → ssh starts
	restarts map[string]map[agentapi.RelayChannel]uint64
	refused  map[string]map[string]uint64
}

// heldSet is a Provider's latest accepted set, with its expiry and login host.
type heldSet struct {
	set     wire.DesiredSet
	expires time.Time
	host    string
}

// wish is what a Provider's latest set past the fence asked of a login host,
// accepted or refused: the claims' "oldest Provider" is chosen among them.
type wish struct {
	host    string
	created time.Time
	expires time.Time
	entries int
}

// Open is the keeper's start: it counts the start on disk (OpenState), so E
// is fixed, then makes the Runner for E.
func Open(o Options) (*Keeper, error) {
	if o.State == "" || o.Runner == nil {
		return nil, fmt.Errorf("relaykeeper: Options.State and Options.Runner are required")
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.ClaimLapse <= 0 {
		o.ClaimLapse = DefaultClaimLapse
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	st, err := OpenState(o.State)
	if err != nil {
		return nil, err
	}
	r, err := o.Runner(st.Epoch())
	if err != nil {
		return nil, err
	}
	return &Keeper{o: o, runner: r, epoch: st.Epoch(), started: o.Now(), st: st,
		sets: map[string]*heldSet{}, wishes: map[string]wish{}, wanted: map[string]map[string]wire.Entry{}, starts: map[string]map[string]int{},
		restarts: map[string]map[agentapi.RelayChannel]uint64{}, refused: map[string]map[string]uint64{}}, nil
}

// Epoch is this run's relay epoch E.
func (k *Keeper) Epoch() int64 { return k.epoch }

// Handle answers one request: it is the keeper's wire.Handler.
func (k *Keeper) Handle(_ context.Context, req wire.Envelope) (wire.Kind, any) {
	switch req.Kind {
	case wire.KindPing:
		return wire.KindPong, wire.Pong{KeeperEpoch: k.epoch}
	case wire.KindStatus:
		return wire.KindStatusReport, k.status()
	case wire.KindSet:
		var s wire.DesiredSet
		if err := req.Decode(&s); err != nil {
			return refuse(wire.RefusedMalformed, err.Error())
		}
		if err := s.Validate(); err != nil {
			return refuse(wire.RefusedMalformed, err.Error())
		}
		return k.handleSet(s)
	case wire.KindAck:
		var a wire.Ack
		if err := req.Decode(&a); err != nil {
			return refuse(wire.RefusedMalformed, err.Error())
		}
		if err := a.Validate(); err != nil {
			return refuse(wire.RefusedMalformed, err.Error())
		}
		return k.handleAck(a)
	}
	return refuse(wire.RefusedMalformed, fmt.Sprintf("%s is not a request", req.Kind))
}

func refuse(reason, msg string) (wire.Kind, any) {
	return wire.KindRefused, wire.Refused{Reason: reason, Message: msg}
}

// fence checks a Lease epoch against the Provider's highest accepted, on
// next, and says whether it raises it. A lower one is refused and counted.
func (k *Keeper) fence(next *State, provider string, epoch int64) (raised bool, refused *wire.Refused) {
	f, seen := next.Providers[provider]
	if seen && epoch < f.HighestEpoch {
		k.count(provider, wire.RefusedEpoch)
		k.o.Logger.Info("a set or an acknowledgement from a lower Lease epoch is refused", "provider", provider, "epoch", epoch, "highest", f.HighestEpoch)
		return false, &wire.Refused{Reason: wire.RefusedEpoch, HighestEpoch: f.HighestEpoch,
			Message: fmt.Sprintf("Lease epoch %d is lower than Provider %s's highest accepted, %d", epoch, provider, f.HighestEpoch)}
	}
	if seen && epoch == f.HighestEpoch {
		return false, nil
	}
	if next.Providers == nil {
		next.Providers = map[string]ProviderFence{}
	}
	next.Providers[provider] = ProviderFence{HighestEpoch: epoch}
	return true, nil
}

func (k *Keeper) count(provider, reason string) {
	if k.refused[provider] == nil {
		k.refused[provider] = map[string]uint64{}
	}
	k.refused[provider][reason]++
}

// commit writes next and makes it the state; a write that fails leaves the
// state as it was, and the request is refused (wire.RefusedState).
func (k *Keeper) commit(next *State) error {
	if err := SaveState(k.o.State, next); err != nil {
		k.o.Logger.Error("the state file could not be written: the request took no effect", "err", err)
		return err
	}
	k.st = next
	return nil
}

func (k *Keeper) handleSet(s wire.DesiredSet) (wire.Kind, any) {
	k.mu.Lock()
	defer k.mu.Unlock()
	now := k.o.Now()
	p := s.Provider
	next := k.st.clone()
	raised, refused := k.fence(next, p, s.LeaseEpoch)
	if refused != nil {
		return wire.KindRefused, *refused
	}
	expires := now.Add(time.Duration(s.ValidForMS-s.ListingAgeMS) * time.Millisecond)
	host := k.runner.LoginHost(p, s.SSH)
	if !now.Before(expires) {
		// Expired on arrival: it moves the fence, and changes nothing else.
		if raised {
			if err := k.commit(next); err != nil {
				return refuse(wire.RefusedState, err.Error())
			}
		}
		return wire.KindReport, k.report(p, host, true)
	}
	k.wishes[p] = wish{host: host, created: s.ProviderCreated, expires: expires, entries: len(s.Allocations)}
	holder, claimsChanged := k.claim(next, s, host, now)
	if holder != "" {
		if claimsChanged {
			// A release of this Provider's claim on another host stands: its
			// set names this host now. A refused set moves no fence.
			n := k.st.clone()
			n.Claims = next.Claims
			if err := k.commit(n); err != nil {
				return refuse(wire.RefusedState, err.Error())
			}
		}
		k.count(p, wire.RefusedLoginHost)
		k.o.Logger.Info("a set for a login host another Provider holds is refused", "provider", p, "holder", holder)
		return wire.KindRefused, wire.Refused{Reason: wire.RefusedLoginHost, LoginHost: host, Holder: holder,
			Message: fmt.Sprintf("login host %s is in use by host-dialed Provider %s: one host-dialed Provider per login host (HBEP-6, Capacity)", host, holder)}
	}
	inSet := map[string]bool{}
	uids := map[string]bool{}
	for _, e := range s.Allocations {
		inSet[e.Tag], uids[e.UID] = true, true
	}
	endsChanged := false
	if s.Complete {
		// An Allocation that leaves a complete set leaves its ends with it.
		kept := next.Ends[:0:0]
		for _, r := range next.Ends {
			if r.Provider == p && !uids[r.End.UID] {
				endsChanged = true
				continue
			}
			kept = append(kept, r)
		}
		next.Ends = kept
	}
	if raised || claimsChanged || endsChanged {
		if err := k.commit(next); err != nil {
			return refuse(wire.RefusedState, err.Error())
		}
	}
	k.sets[p] = &heldSet{set: s, expires: expires, host: host}
	if k.wanted[p] == nil {
		k.wanted[p] = map[string]wire.Entry{}
	}
	for _, e := range s.Allocations {
		if e.EndedForGood {
			// It is being released: no relay of it starts, and the ones that
			// run are kept as they are.
			continue
		}
		k.wanted[p][e.Tag] = e
		k.runner.Want(p, s.SSH, Want{Entry: e, Final: k.finals(p, e), Jobs: jobs{k: k, provider: p},
			OnFinal: k.onFinal(p, e), OnStart: k.onStart(p, e)})
	}
	if s.Complete {
		for _, tag := range sortedKeys(k.wanted[p]) {
			if !inSet[tag] {
				delete(k.wanted[p], tag)
				k.runner.Drop(p, tag)
			}
		}
	}
	return wire.KindReport, k.report(p, host, false)
}

// report is the Provider's report; the caller holds the lock.
func (k *Keeper) report(p, host string, expired bool) wire.Report {
	r := wire.Report{Provider: p, KeeperEpoch: k.epoch, LoginHost: host, Expired: expired, Relays: k.runner.Relays(p)}
	for _, e := range k.st.Ends {
		if e.Provider == p && !e.Decided {
			r.Ends = append(r.Ends, e.End)
		}
	}
	return r
}

// finals are the channels of e's Allocation whose final ends the keeper
// holds, decided or not; the caller holds the lock.
func (k *Keeper) finals(p string, e wire.Entry) map[agentapi.RelayChannel]string {
	var out map[agentapi.RelayChannel]string
	for _, r := range k.st.Ends {
		if r.Provider == p && r.End.UID == e.UID && r.End.JobID == e.JobID {
			if out == nil {
				out = map[agentapi.RelayChannel]string{}
			}
			out[r.End.Channel] = r.End.Kind + ": " + r.End.Cause
		}
	}
	return out
}

// onFinal records a final end that needs the mark, written and synced before
// it is reported. A job's end needs none and is not recorded; an end the
// keeper holds already is not recorded again.
func (k *Keeper) onFinal(p string, e wire.Entry) func(agentapi.RelayChannel, hostdial.EndKind, string, int64, int64) {
	return func(ch agentapi.RelayChannel, kind hostdial.EndKind, cause string, epoch, seq int64) {
		var wk string
		switch kind {
		case hostdial.EndFailed:
			wk = wire.EndFailed
		case hostdial.EndSuperseded:
			wk = wire.EndSuperseded
		default:
			return
		}
		k.mu.Lock()
		defer k.mu.Unlock()
		for _, r := range k.st.Ends {
			if r.Provider == p && r.End.UID == e.UID && r.End.JobID == e.JobID && r.End.Channel == ch {
				return
			}
		}
		next := k.st.clone()
		next.Ends = append(next.Ends, EndRecord{Provider: p, End: wire.FinalEnd{UID: e.UID, Tag: e.Tag, JobID: e.JobID, Channel: ch,
			Kind: wk, Cause: cause, At: k.o.Now().UTC(), Epoch: epoch, Seq: seq}})
		if err := k.commit(next); err != nil {
			// The relay is final in its Runner all the same; the record is
			// kept in memory and written with the next change.
			k.st = next
		}
	}
}

// onStart counts a relay's ssh starts: each after the first of a channel of
// an Allocation is a restart (HBEP-8, hpcburst_relay_restarts_total).
func (k *Keeper) onStart(p string, e wire.Entry) func(agentapi.RelayChannel) {
	return func(ch agentapi.RelayChannel) {
		k.mu.Lock()
		defer k.mu.Unlock()
		if k.starts[p] == nil {
			k.starts[p] = map[string]int{}
		}
		key := e.UID + "/" + string(ch)
		k.starts[p][key]++
		if k.starts[p][key] > 1 {
			if k.restarts[p] == nil {
				k.restarts[p] = map[agentapi.RelayChannel]uint64{}
			}
			k.restarts[p][ch]++
		}
	}
}

func (k *Keeper) handleAck(a wire.Ack) (wire.Kind, any) {
	k.mu.Lock()
	defer k.mu.Unlock()
	next := k.st.clone()
	raised, refused := k.fence(next, a.Provider, a.LeaseEpoch)
	if refused != nil {
		return wire.KindRefused, *refused
	}
	decided, changed := false, raised
	for i, r := range next.Ends {
		e := r.End
		if r.Provider != a.Provider || e.UID != a.UID || e.JobID != a.JobID || e.Channel != a.Channel || e.Epoch != a.Epoch || e.Seq != a.Seq {
			continue
		}
		decided = true
		if !r.Decided {
			next.Ends[i].Decided, next.Ends[i].Outcome, next.Ends[i].Reason = true, a.Outcome, a.Reason
			changed = true
		}
	}
	if changed {
		if err := k.commit(next); err != nil {
			return refuse(wire.RefusedState, err.Error())
		}
	}
	return wire.KindAcked, wire.Acked{Provider: a.Provider, Decided: decided}
}

func (k *Keeper) status() wire.StatusReport {
	k.mu.Lock()
	defer k.mu.Unlock()
	names := map[string]bool{}
	for _, m := range []map[string]bool{keysOf(k.sets), keysOf(k.wanted), keysOf(k.restarts), keysOf(k.refused)} {
		for n := range m {
			names[n] = true
		}
	}
	r := wire.StatusReport{KeeperEpoch: k.epoch, Started: k.started.UTC()}
	for _, p := range sortedKeys(names) {
		ps := wire.ProviderStatus{Provider: p, Relays: k.runner.Relays(p), Restarts: map[agentapi.RelayChannel]uint64{}, Refused: map[string]uint64{}}
		for ch, n := range k.restarts[p] {
			ps.Restarts[ch] = n
		}
		for why, n := range k.refused[p] {
			ps.Refused[why] = n
		}
		r.Providers = append(r.Providers, ps)
	}
	return r
}

// jobs is a Provider's latest set as hostdial's listing (hostdial.JobReader):
// while the set is unexpired, it shows each of its Allocations' jobs RUNNING
// under its tag, except an Allocation that carries RelayEndedForGood, which
// gets no relay; once it has expired, or before any set since the keeper
// started, it is an error, so hostdial starts and restarts nothing (HBEP-6:
// "the keeper starts or restarts a relay only under an unexpired set that
// shows its job RUNNING under its tag, and a retry counts as a start").
type jobs struct {
	k        *Keeper
	provider string
}

func (j jobs) ListByTag(_ context.Context, tag string) ([]provider.Job, error) {
	k := j.k
	k.mu.Lock()
	defer k.mu.Unlock()
	hs := k.sets[j.provider]
	switch {
	case hs == nil:
		return nil, fmt.Errorf("no desired set from Provider %s's leader since the keeper started", j.provider)
	case !k.o.Now().Before(hs.expires):
		return nil, fmt.Errorf("Provider %s's desired set has expired: no leader's listing is fresh", j.provider)
	}
	for _, e := range hs.set.Allocations {
		if e.Tag == tag && !e.EndedForGood {
			return []provider.Job{{ID: e.JobID, Name: tag, Phase: v1alpha1.AllocationRunning}}, nil
		}
	}
	return nil, nil
}

func keysOf[V any](m map[string]V) map[string]bool {
	out := map[string]bool{}
	for k := range m {
		out[k] = true
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}
