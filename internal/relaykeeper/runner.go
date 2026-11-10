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
// any time, and take the keeper's lock.
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
	// Relays lists the Provider's relays.
	Relays(provider string) []wire.RelayState
}

// Want is one Allocation's relays as the keeper wants them.
type Want struct {
	Entry wire.Entry
	// Final names the channels that already ended for good, each with its
	// cause as hostdial shows it ("<kind>: <cause>"): their relays are never
	// started (hostdial.Target.Final).
	Final map[agentapi.RelayChannel]string
	// Jobs is the Provider's latest set as a listing, while it is unexpired:
	// a relay starts or restarts only when it shows the job RUNNING under its
	// tag (hostdial.Target.Jobs).
	Jobs hostdial.JobReader
	// OnFinal is told of each final end, with the ended process's epoch and
	// sequence (hostdial.Target.OnFinal).
	OnFinal func(ch agentapi.RelayChannel, kind hostdial.EndKind, cause string, epoch, seq int64)
	// OnStart is told of each ssh start (hostdial.Target.OnStart).
	OnStart func(ch agentapi.RelayChannel)
}
