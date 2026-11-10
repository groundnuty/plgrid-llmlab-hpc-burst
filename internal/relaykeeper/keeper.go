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
//     synced, and only then reported, in each of the Provider's reports until
//     the leader acknowledges it. One whose write fails is pending: never
//     reported, its Allocation started and restarted no more, and written
//     before anything else takes effect (Keeper.settle). A decided end stays recorded until its Allocation
//     leaves a complete set (Q1), and its channel is never restarted. An
//     Allocation that carries RelayEndedForGood gets neither of its relays
//     started or restarted, on a new generation too (#347).
//   - One Provider per login host (Q2): see claims.go.
//
// HBEP-6's stated limits (#347): the restart cap's hour (hostdial's) is kept
// in memory, so it starts again at a keeper restart. A Lease deleted by hand,
// or a store rebuilt from scratch with this state file kept, lowers
// leaseTransitions below the fence: the refusal names the Provider and its
// highest epoch, and an operator stops the keeper, which cuts every
// host-dialed stream, clears that Provider's fence, and starts it again. No
// tool besides the launcher's restore (ClearLeaseEpochs) does this.
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
	// Now is the clock; nil means time.Now. Every expiry and lapse is
	// computed from its readings as they are, never through UTC or Round, so
	// a real clock's monotonic reading makes them immune to steps of the wall
	// clock (#343's Q5).
	Now func() time.Time
	// ClaimLapse is how long a claim on a login host lasts with no relay
	// running there and no unexpired set from its holder; zero means
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

	mu sync.Mutex
	// st is the state as the file holds it: memory is never below the file,
	// so a later write cannot lower a fence (Keeper.commit).
	st *State
	// synced is false while the file's last write may not have reached the
	// disk (its directory's sync failed after the rename): then nothing takes
	// effect, and nothing is reported, until a write syncs (settle).
	synced bool
	// pending are final ends whose write failed: they are not in st, never
	// reported, and written by settle before anything else takes effect.
	pending  []EndRecord
	sets     map[string]*heldSet          // Provider's name → its latest accepted set; not persisted
	wishes   map[provKey]wish             // Provider → its latest unexpired set past the fence, accepted or not
	wanted   map[string]map[string]wanted // Provider's name → tag → the Allocation kept
	tagUID   map[string]map[string]string // Provider's name → tag → the UID of the Provider that wanted it, kept after a Drop
	starts   map[string]map[string]int    // Provider's name → UID/channel → ssh starts
	restarts map[string]map[agentapi.RelayChannel]uint64
	refused  map[string]map[string]uint64
}

// provKey is a Provider: its name and its UID. A Provider re-created under
// the same name is another Provider (#347).
type provKey struct{ name, uid string }

// heldSet is a Provider's latest accepted set, with its expiry and login host.
type heldSet struct {
	set     wire.DesiredSet
	expires time.Time
	host    string
}

// wish is what a Provider's latest set past the fence asked of a login host,
// accepted or refused: the claims' "oldest Provider" is chosen among them,
// and a claim's lapse counts from its holder's.
type wish struct {
	host    string
	created time.Time
	expires time.Time
}

// wanted is an Allocation the keeper keeps, with the UID of the Provider
// whose set asked for it.
type wanted struct {
	entry wire.Entry
	uid   string
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
	return &Keeper{o: o, runner: r, epoch: st.Epoch(), started: o.Now(), st: st, synced: true,
		sets: map[string]*heldSet{}, wishes: map[provKey]wish{}, wanted: map[string]map[string]wanted{},
		tagUID: map[string]map[string]string{}, starts: map[string]map[string]int{},
		restarts: map[string]map[agentapi.RelayChannel]uint64{}, refused: map[string]map[string]uint64{}}, nil
}

// Epoch is this run's relay epoch E.
func (k *Keeper) Epoch() int64 { return k.epoch }

// Handle answers one request: it is the keeper's wire.Handler.
func (k *Keeper) Handle(_ context.Context, req wire.Envelope) (wire.Kind, any) {
	switch req.Kind {
	case wire.KindPing, wire.KindStatus:
		var empty struct{}
		if err := req.Decode(&empty); err != nil {
			return refuse(wire.RefusedMalformed, err.Error())
		}
		if req.Kind == wire.KindPing {
			return wire.KindPong, wire.Pong{KeeperEpoch: k.epoch}
		}
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

// commit writes next. Once the rename is made, next is the state, even when
// the directory's sync then fails: memory then matches the file, so a later
// write cannot lower a fence below it (the review of PR 354). Until a write
// syncs, nothing takes effect (settle). A write that fails before the rename
// leaves the state as it was. Either failure refuses the request
// (wire.RefusedState).
func (k *Keeper) commit(next *State) (renamed bool, err error) {
	renamed, err = saveState(k.o.State, next)
	if renamed {
		k.st = next
	}
	if err != nil {
		k.synced = false
		k.o.Logger.Error("the state file could not be written and synced: the request takes no effect until a write syncs", "renamed", renamed, "err", err)
		return renamed, err
	}
	k.synced = true
	return renamed, nil
}

// settle writes the pending ends, and a state whose last write did not sync,
// before anything else takes effect: every change is written and synced
// before it takes effect or is reported (HBEP-6). The caller holds the lock.
func (k *Keeper) settle() error {
	if k.synced && len(k.pending) == 0 {
		return nil
	}
	next := k.st.clone()
	next.Ends = append(next.Ends, k.pending...)
	renamed, err := k.commit(next)
	if renamed {
		k.pending = nil
	}
	return err
}

// Settle is settle for the keeper process's timer, so a pending end reaches
// the disk with no request too.
func (k *Keeper) Settle() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.settle()
}

func (k *Keeper) handleSet(s wire.DesiredSet) (wire.Kind, any) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if err := k.settle(); err != nil {
		return refuse(wire.RefusedState, err.Error())
	}
	now := k.o.Now() // as it is: its monotonic reading is the expiry's clock
	p := s.Provider
	next := k.st.clone()
	raised, refused := k.fence(next, p, s.LeaseEpoch)
	if refused != nil {
		return wire.KindRefused, *refused
	}
	host := k.runner.LoginHost(p, s.SSH)
	// The age is compared with the validity before anything is converted,
	// so no arithmetic overflows (the review of PR 354); both are bounded
	// (wire.MaxListingAge, wire.MaxValidFor).
	if s.ListingAgeMS >= s.ValidForMS {
		// Expired on arrival: it moves the fence, and changes nothing else.
		if raised {
			if _, err := k.commit(next); err != nil {
				return refuse(wire.RefusedState, err.Error())
			}
		}
		return wire.KindReport, k.report(p, host, true)
	}
	expires := now.Add(time.Duration(s.ValidForMS-s.ListingAgeMS) * time.Millisecond)
	k.wishes[provKey{p, s.ProviderUID}] = wish{host: host, created: s.ProviderCreated, expires: expires}
	holder, claimsChanged := k.claim(next, s, host, now)
	if holder != "" {
		if claimsChanged {
			// A lapsed claim it found is dropped; a refused set moves no fence.
			n := k.st.clone()
			n.Claims = next.Claims
			if _, err := k.commit(n); err != nil {
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
		if _, err := k.commit(next); err != nil {
			return refuse(wire.RefusedState, err.Error())
		}
	}
	k.sets[p] = &heldSet{set: s, expires: expires, host: host}
	if k.wanted[p] == nil {
		k.wanted[p] = map[string]wanted{}
	}
	if k.tagUID[p] == nil {
		k.tagUID[p] = map[string]string{}
	}
	for _, e := range s.Allocations {
		if e.EndedForGood {
			// It is being released: no relay of it starts, and the ones that
			// run are kept as they are.
			continue
		}
		k.wanted[p][e.Tag] = wanted{entry: e, uid: s.ProviderUID}
		k.tagUID[p][e.Tag] = s.ProviderUID
		k.runner.Want(p, s.SSH, Want{Entry: e, Finals: k.finalsOf(p, e), Jobs: jobs{k: k, provider: p},
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

// report is the Provider's report: only the ends on disk, synced, that are
// not decided yet. The caller holds the lock, and has settled.
func (k *Keeper) report(p, host string, expired bool) wire.Report {
	r := wire.Report{Provider: p, KeeperEpoch: k.epoch, LoginHost: host, Expired: expired, Relays: k.runner.Relays(p)}
	for _, e := range k.st.Ends {
		if e.Provider == p && !e.Decided {
			r.Ends = append(r.Ends, e.End)
		}
	}
	return r
}

// endsOf are the ends the keeper holds for e's Allocation, on disk or
// pending; the caller holds the lock.
func (k *Keeper) endsOf(p string, e wire.Entry) []EndRecord {
	var out []EndRecord
	for _, recs := range [][]EndRecord{k.st.Ends, k.pending} {
		for _, r := range recs {
			if r.Provider == p && r.End.UID == e.UID && r.End.JobID == e.JobID {
				out = append(out, r)
			}
		}
	}
	return out
}

// finalsOf is the Want's Finals: the channels of e's Allocation whose final
// ends the keeper holds, decided, undecided or pending, read when the Runner
// starts the relays, not when the Want was made, so an end that comes in
// between keeps its channel down (the review of PR 354).
func (k *Keeper) finalsOf(p string, e wire.Entry) func() map[agentapi.RelayChannel]string {
	return func() map[agentapi.RelayChannel]string {
		k.mu.Lock()
		defer k.mu.Unlock()
		var out map[agentapi.RelayChannel]string
		for _, r := range k.endsOf(p, e) {
			if out == nil {
				out = map[agentapi.RelayChannel]string{}
			}
			out[r.End.Channel] = r.End.Kind + ": " + r.End.Cause
		}
		return out
	}
}

// onFinal records a final end that needs the mark, written and synced before
// it is reported. A job's end needs none and is not recorded; an end the
// keeper holds already is not recorded again. A superseded end of a
// generation the keeper has replaced since is that replacement's doing, not
// a failure, and is not recorded either. When the write fails, the end is
// pending (settle).
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
		if cur, ok := k.wanted[p][e.Tag]; ok && kind == hostdial.EndSuperseded && cur.entry != e {
			k.o.Logger.Info("a superseded end of a replaced generation is not a failure", "provider", p, "tag", e.Tag, "channel", ch)
			return
		}
		for _, r := range k.endsOf(p, e) {
			if r.End.Channel == ch {
				return
			}
		}
		rec := EndRecord{Provider: p, End: wire.FinalEnd{UID: e.UID, Tag: e.Tag, JobID: e.JobID, Channel: ch,
			Kind: wk, Cause: cause, At: k.o.Now().UTC(), Epoch: epoch, Seq: seq}}
		next := k.st.clone()
		next.Ends = append(append(next.Ends, k.pending...), rec)
		if renamed, err := k.commit(next); renamed {
			k.pending = nil
		} else if err != nil {
			k.pending = append(k.pending, rec)
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
	if err := k.settle(); err != nil {
		return refuse(wire.RefusedState, err.Error())
	}
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
		if _, err := k.commit(next); err != nil {
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
// under its tag, except an Allocation that carries RelayEndedForGood or whose
// final end the keeper holds (on disk or pending), which gets no relay
// started or restarted. It is read at each start, so an end or a mark that
// comes after a Want was made still holds that Want's starts (the review of
// PR 354). hostdial's listing is by tag, not channel, so once an end that
// needs the mark is held, neither of its Allocation's relays starts again,
// as for the mark that follows. Once the set has expired, or before any set
// since the keeper started, it is an error, so hostdial starts and restarts
// nothing (HBEP-6: "the keeper starts or restarts a relay only under an
// unexpired set that shows its job RUNNING under its tag, and a retry counts
// as a start").
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
		if e.Tag == tag && !e.EndedForGood && len(k.endsOf(j.provider, e)) == 0 {
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
