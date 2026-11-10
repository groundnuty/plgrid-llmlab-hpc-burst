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
	// call, on every path: OnStart once the process has started, or
	// StartFailed when it does not start, whatever the reason (a relay line,
	// an ssh command or a process start that fails, a stop or Drop that comes
	// first). An authorization never resolved holds its host until the keeper
	// stops. The holds are counted per relay of an Allocation on a host, not
	// per channel, since a listing names no channel: a second resolution of
	// one start is taken for the other channel's, so the Runner resolves each
	// exactly once. hostdial calls no hook on its paths between the listing and the
	// process start, so PR 3's Runner adds one to hostdial.Target (add-only)
	// and calls StartFailed from it.
	Jobs hostdial.JobReader
	// OnFinal is told of each final end, with the ended process's epoch and
	// sequence (hostdial.Target.OnFinal).
	OnFinal func(ch agentapi.RelayChannel, kind hostdial.EndKind, cause string, epoch, seq int64)
	// OnStart is told of each ssh start (hostdial.Target.OnStart), with the
	// (E, N) the relay started with: the keeper's record of what it issued.
	// It resolves that start's authorization (Jobs): the started process
	// holds the host from then on, as a relay up in Relays.
	OnStart func(ch agentapi.RelayChannel, epoch, seq int64)
	// StartFailed resolves an authorization (Jobs) whose process did not
	// start, on every such path; it releases the host the start held.
	StartFailed func(ch agentapi.RelayChannel)
}
