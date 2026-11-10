package relaykeeper

import (
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Runner runs the relays the keeper decides on: in the keeper process one
// hostdial.Owner per Provider, all at the keeper's epoch E, with their relays
// signalled through pidfds (OwnerOptions.Pidfd) and one hostdial.Slots
// shared by all, which is HBEP-4's 4 relays per login host. In tests it is a
// fake.
//
// The keeper calls the Runner with its lock held, so no method may wait for a
// relay to start or stop: Drop stops the relays in the background, and the
// Runner applies Want and Drop of one Provider in the order of the calls. The
// callbacks a Want carries are called from the Runner's own goroutines, at
// any time, and take the keeper's lock; so do the Want's Finals and its Jobs.
//
// HBEP-4's 4 relays per login host are the Runner's to keep, with one
// hostdial.Slots for every Provider (PR 3); this package does not count them.
type Runner interface {
	// LoginHost is the Provider's login host for its ssh settings: as ssh -G
	// resolves its HostName through the keeper's own -F file for them, in
	// lower case (HBEP-6, Capacity; the science agent's ruling on #343's Q4).
	LoginHost(provider string, ssh wire.SSH) string
	// Want keeps both relays of w's Allocation, on ssh. A Want of the same
	// identity (UID, job, channel, Gen, and the rest of the entry) changes
	// nothing, so a new leader adopts the relays and replaces none.
	Want(provider string, ssh wire.SSH, w Want)
	// Drop stops the relays of the Allocation tagged tag, in the background.
	Drop(provider, tag string)
	// Relays lists the Provider's relays, each with its login host and the
	// Provider UID of the Want it started under. A relay is up only while a
	// process of it runs: one that waits, retries or ended is not, for the
	// claims count only relays that run (HBEP-6's third lapse rule; the
	// review of PR 354). A relay whose process is still stopping, after a
	// Drop or a replacement by another identity, is listed as up, with its
	// own host and UID, until the process is gone: a host is not free while
	// an old process still runs there (the delta review of PR 354).
	Relays(provider string) []wire.RelayState
}

// Want is one Allocation's relays as the keeper wants them.
type Want struct {
	Entry wire.Entry
	// ProviderUID is the UID of the Provider whose set made the Want: the
	// relays started for it carry it (RelayState.ProviderUID).
	ProviderUID string
	// Finals names the channels that already ended for good, each with its
	// cause as hostdial shows it ("<kind>: <cause>"), whose relays are never
	// started (hostdial.Target.Final). The Runner calls it when it actually
	// starts the Allocation's relays, on its own goroutine, never inside Want,
	// so an end that comes between the Want and the start keeps its channel
	// down (the review of PR 354).
	Finals func() map[agentapi.RelayChannel]string
	// Jobs is this Want's start authorization, as a listing (hostdial.Target.
	// Jobs): a relay of it starts or restarts only when it shows the job
	// RUNNING under its tag, which it does only while the Provider's set is
	// unexpired, names this Want's host and Provider, the claim on that host
	// is this Provider's, and the Want is still the Allocation's. So a
	// replaced or moved relay's outstanding starts are fenced as soon as the
	// keeper accepts the set that replaces or moves it.
	//
	// A listing that shows the job is one start's authorization, of one
	// channel's relay, and it holds the Want's login host for its Provider
	// until it resolves: no other Provider can claim the host, and no lapse
	// takes it, while the start is in flight (the last delta review of PR
	// 354). The Runner reads Jobs once per channel per attempt, right before
	// the process starts, and resolves each authorization with exactly one
	// call: OnStart once the process has started, or StartFailed when it does
	// not start. The hold is fail-closed by design, with no deadline: an
	// authorization never resolved holds its host until the keeper stops,
	// past the authorizing set's expiry, the claim's lapse, a Drop, a move
	// or a replacement. A deadline would be unsafe, since once a hold expired
	// another Provider could claim the host while the authorized start,
	// suspended, could still launch there
	// (TestAnUnresolvedStartHoldsItsHostPastEveryLapse pins this). The
	// Runner's four duties (PR 3):
	//  1. It calls StartFailed on every path between the listing and the
	//     process start where the process does not start: a relay line, an
	//     ssh command or a process start that fails, and a stop or Drop that
	//     comes first. hostdial calls no hook on those paths, so PR 3 adds
	//     one to hostdial.Target, add-only, and calls StartFailed from it.
	//  2. It shows a started process as up in Relays BEFORE it calls
	//     OnStart, so the host is never unheld between the hold's release and
	//     the process's counting (hostdial sets the relay's PID before it
	//     calls OnStart).
	//  3. It PREVENTS the launch of an in-flight start of a REPLACED
	//     generation, and resolves it with StartFailed. Cancelling the
	//     relay's context is not enough with today's hostdial start path
	//     (owner.go, relay.once): once its listing passes, it goes on to the
	//     relay line, the ssh command and the process start without checking
	//     the context, and its command and start failures return with no
	//     callback. So PR 3's hook checks for a replacement right before the
	//     process start, and resolves there. Otherwise a late process of the
	//     old generation gets a higher N than the current relay's and
	//     supersedes it, and the keeper, whose record holds the current
	//     generation's (E, N) only (OnStart), then marks the Allocation: that
	//     fails closed, but releases a healthy Allocation.
	//  4. It resolves each authorization exactly once, per channel. The holds
	//     are counted per relay of an Allocation on a host, not per channel,
	//     since a listing names no channel, so the keeper cannot tell one
	//     channel's resolution from the other's: a duplicate resolution of
	//     the control channel's start while the serving channel's is still in
	//     flight would consume serving's hold, and let another Provider take
	//     the host early. A resolution with none in flight is logged and
	//     changes nothing.
	Jobs hostdial.JobReader
	// OnFinal is told of each final end, with the ended process's epoch and
	// sequence (hostdial.Target.OnFinal).
	OnFinal func(ch agentapi.RelayChannel, kind hostdial.EndKind, cause string, epoch, seq int64)
	// OnStart is told of each ssh start (hostdial.Target.OnStart), with the
	// (E, N) the relay started with: the keeper's record of what it issued.
	// It resolves that start's authorization (Jobs): the started process
	// holds the host from then on, as a relay up in Relays, which the Runner
	// shows before it calls OnStart (duty 2), once per authorization (duty 4).
	OnStart func(ch agentapi.RelayChannel, epoch, seq int64)
	// BeforeStart is asked right before each ssh start, with the (E, N) the
	// start would carry (hostdial.Target.BeforeStart): the keeper decides
	// there, under its lock, whether the start may go ahead.
	BeforeStart func(ch agentapi.RelayChannel, epoch, seq int64) bool
	// StartFailed resolves an authorization (Jobs) whose process did not
	// start, on every such path, a replaced generation's prevented start
	// included, once per authorization (duties 1, 3 and 4); it releases the
	// host the start held, through this Want's own Provider UID and host,
	// whatever the keeper's current set says.
	StartFailed func(ch agentapi.RelayChannel)
}
