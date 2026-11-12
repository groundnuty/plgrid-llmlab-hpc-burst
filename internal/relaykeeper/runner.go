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
	// An error means the host could not be read (ssh -G timed out, issue
	// 403): the keeper then claims nothing for that set and refuses it, since
	// a host it guessed could be another Provider's (`@hpc-burst-science-agent`,
	// ruling (b) on PR 409, 11 Nov 2026).
	LoginHost(provider string, ssh wire.SSH) (string, error)
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
	// Incarnation is the Want's identity: the keeper issues a new one, never
	// reused, whenever the entry, the Provider UID or the host changes, or the
	// tag is dropped and wanted again, and keeps it while the same Want is
	// re-sent. The Runner replaces an Allocation's relays exactly when it
	// changes, and its starts pass the gate only while it is the current
	// one's (the review of PR 356).
	Incarnation uint64
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
	// The listing takes no hold; the gate does (BeforeStart, the science
	// agent's ruling B for PR 3, which moved the hold there from PR 2's
	// listing). A start the gate allows holds the Want's login host for its
	// Provider, per channel, until it resolves: no other Provider can claim
	// the host, and no lapse takes it, while the start is in flight. The
	// hold is fail-closed by design, with no deadline: a start allowed and
	// never resolved holds its host until the keeper stops, past the set's
	// expiry, the claim's lapse, a Drop, a move or a replacement. A deadline
	// would be unsafe, since once a hold expired another Provider could claim
	// the host while the allowed start, suspended, could still launch there
	// (TestAnUnresolvedStartHoldsItsHostPastEveryLapse pins this). The
	// Runner's four duties (PR 3):
	//  1. It asks BeforeStart right before each ssh start, after the start's
	//     N is taken, and starts nothing on a false answer (hostdial's
	//     Target.BeforeStart). A refused start is no start: no process, no
	//     hold, nothing to resolve.
	//  2. It resolves each start BeforeStart allowed exactly once: OnStart
	//     once the process runs, StartFailed when it does not start
	//     (hostdial's Target.OnStartFailed). A second resolution of a
	//     channel's start would release that channel's next start's hold;
	//     the holds are per channel, so it cannot touch the other channel's.
	//  3. It shows a started process as up in Relays BEFORE it calls
	//     OnStart, so the host is never unheld between the hold's release and
	//     the process's counting (hostdial sets the relay's PID before it
	//     calls OnStart).
	//  4. It lists a replaced or dropped relay's process as up until it is
	//     gone (hostdial's Target.OnExit), and keeps listing a replaced
	//     relay while a start of its is in flight.
	Jobs hostdial.JobReader
	// OnFinal is told of each final end, with the ended process's epoch and
	// sequence (hostdial.Target.OnFinal).
	OnFinal func(ch agentapi.RelayChannel, kind hostdial.EndKind, cause string, epoch, seq int64)
	// OnStart is told of each ssh start (hostdial.Target.OnStart), with the
	// (E, N) the relay started with: the keeper's record of what it issued.
	// It resolves that start's hold (BeforeStart): the started process holds
	// the host from then on, as a relay up in Relays, which the Runner shows
	// before it calls OnStart (duty 3), once per allowed start (duty 2).
	OnStart func(ch agentapi.RelayChannel, epoch, seq int64)
	// BeforeStart is the gate, asked right before each ssh start with the
	// (E, N) the start would carry (hostdial.Target.BeforeStart, duty 1): the
	// keeper re-checks the start's authorization under its lock and, only
	// when it holds, takes the start's hold on its host.
	BeforeStart func(ch agentapi.RelayChannel, epoch, seq int64) bool
	// StartFailed resolves a start BeforeStart allowed whose process did not
	// start (hostdial's Target.OnStartFailed), once (duty 2); it releases the
	// host the start held, through this Want's own Provider UID and host,
	// whatever the keeper's current set says.
	StartFailed func(ch agentapi.RelayChannel)
}
