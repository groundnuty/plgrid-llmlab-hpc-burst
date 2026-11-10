// Package wire is the relay keeper's protocol (HBEP-6, Dial-home modes, The
// relay keeper; beta, issue #343): the messages that the leading controllers,
// the serving controller's metrics reader and the launcher exchange with the
// keeper over its Unix socket, their checks, and their framing.
//
// The transport is a Unix stream socket of this user's, mode 0600, at a path
// the launcher chooses (at most agentapi.MaxRelaySocketPath bytes). The keeper
// listens, and it refuses a peer of another user (SO_PEERCRED). One
// connection carries one exchange: the client writes one request and reads
// one reply, and the connection closes. The keeper never writes first, so
// level-triggering and the replay of final ends follow from the exchange: each
// set's reply is the Provider's whole report. A keeper restart costs a client
// one failed dial.
//
// Framing: one JSON object per line, at most MaxMessage bytes before its
// newline. encoding/json escapes every newline inside a string, so a line is
// always one message. Every message is an Envelope whose V is Version. A
// request of another version is refused (RefusedVersion, naming the versions
// served), and unknown fields are refused within a version, so a new field
// needs a new version. The controller and the keeper are one binary (the
// science agent's ruling on #343's Q9, 10 Nov 2026), so they agree in one
// release, and the launcher's upgrade stops them together.
//
// No message carries a secret: a set carries the paths of the Provider's key
// and known-hosts file, never their content, and every path is checked
// against HBEP-4's safe pattern, which a key's text cannot pass.
//
// Provenance: new package, for #343. Its rules are the science agent's rulings
// on #343's questions (10 Nov 2026), as HBEP-6's amendment of that day records
// them.
package wire

import (
	"encoding/json"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Version is the protocol's version.
const Version = 1

// MaxMessage bounds one message's line, its newline excluded.
const MaxMessage = 256 << 10

// MaxAllocations bounds a set's Allocations: the 2 a host-dialed Provider
// may have (agentapi.HostDialedMaxAllocations), with room for an unfenced
// hand-over's surplus.
const MaxAllocations = 8

// MaxValidFor bounds a set's validity: the poll interval (30 s, or 5 s) plus
// the renew deadline (10 s) fits well within it (#343's Q5).
const MaxValidFor = 2 * time.Minute

// MaxListingAge bounds a set's listing age. A listing older than its
// validity only makes the set expired; the bound keeps the arithmetic far
// from any overflow (the review of PR 354).
const MaxListingAge = 24 * time.Hour

// Kind names a message.
type Kind string

// The kinds. Each request kind has one reply kind; any request can instead
// be answered KindRefused.
const (
	KindPing         Kind = "ping"
	KindPong         Kind = "pong"
	KindSet          Kind = "set"
	KindReport       Kind = "report"
	KindAck          Kind = "ack"
	KindAcked        Kind = "acked"
	KindStatus       Kind = "status"
	KindStatusReport Kind = "status-report"
	KindRefused      Kind = "refused"
)

// ReplyKind is the reply kind of a request kind, and whether kind is a
// request at all.
func ReplyKind(kind Kind) (Kind, bool) {
	switch kind {
	case KindPing:
		return KindPong, true
	case KindSet:
		return KindReport, true
	case KindAck:
		return KindAcked, true
	case KindStatus:
		return KindStatusReport, true
	}
	return "", false
}

// Envelope is every message on the wire.
type Envelope struct {
	V    int             `json:"v"`
	Kind Kind            `json:"kind"`
	Body json.RawMessage `json:"body,omitempty"`
}

// DesiredSet is a Provider's desired relays: the leading controller of the
// Provider sends it, level-triggered, and it replaces the Provider's previous
// set. It is fenced by LeaseEpoch.
type DesiredSet struct {
	// Provider is the Provider's name.
	Provider string `json:"provider"`
	// ProviderUID and ProviderCreated are the Provider's UID and
	// creationTimestamp: the login-host claims' "oldest Provider" (#343's Q2).
	ProviderUID     string    `json:"providerUID"`
	ProviderCreated time.Time `json:"providerCreated"`
	// LeaseEpoch is the sender's provider-<name> Lease epoch: leaseTransitions
	// at its own acquisition write (providerctl.RelayFence.Epoch).
	LeaseEpoch int64 `json:"leaseEpoch"`
	// Complete is set once every running Allocation has been observed under
	// this lead (#343's Q6). Only a complete set stops a relay, by leaving its
	// Allocation out.
	Complete bool `json:"complete"`
	// ListedAt is the wall time of the Provider's queue listing the set rests
	// on, for logs only.
	ListedAt time.Time `json:"listedAt"`
	// ListingAgeMS is that listing's age when the set was sent, and ValidForMS
	// the set's validity from the listing: the poll interval in force plus the
	// renew deadline. The keeper computes the expiry on its own monotonic
	// clock at receipt (#343's Q5).
	ListingAgeMS int64 `json:"listingAgeMs"`
	ValidForMS   int64 `json:"validForMs"`
	// SSH is the Provider's ssh client settings, from which the keeper writes
	// its own -F file.
	SSH SSH `json:"ssh"`
	// Allocations are the Provider's running Allocations, each with its job
	// RUNNING under its tag in the listing.
	Allocations []Entry `json:"allocations"`
}

// SSH is the Provider's credentials file as slurm.SSHConfig parses it. It
// holds paths, never a key.
type SSH struct {
	HostName string `json:"hostName"`
	User     string `json:"user"`
	// Port is 0 for ssh's default.
	Port         int    `json:"port"`
	IdentityFile string `json:"identityFile"`
	// KnownHostsFile is "" for ssh's default.
	KnownHostsFile string `json:"knownHostsFile"`
}

// Entry is one running Allocation of a set.
type Entry struct {
	UID       string `json:"uid"`
	Tag       string `json:"tag"`
	JobID     string `json:"job"`
	AllocDir  string `json:"allocDir"`
	AgentPath string `json:"agentPath"`
	// Gen is the credentials generation (providerctl.credentialsGen): a relay's
	// identity is its UID, job, channel and Gen, never a file path.
	Gen string `json:"gen"`
	// EndedForGood is set when the Allocation carries RelayEndedForGood True.
	// The keeper starts no relay for it (#343's Q1).
	EndedForGood bool `json:"endedForGood"`
}

// Report is the keeper's reply to an accepted set: the Provider's whole state
// after the set was applied.
type Report struct {
	Provider string `json:"provider"`
	// KeeperEpoch is E, this keeper run's relay epoch.
	KeeperEpoch int64 `json:"keeperEpoch"`
	// LoginHost is the login host as the keeper resolved it (ssh -G), lower
	// case.
	LoginHost string `json:"loginHost"`
	// Expired is set when the set had expired on arrival: it moved the fence
	// and changed nothing else.
	Expired bool         `json:"expired"`
	Relays  []RelayState `json:"relays"`
	// Ends are the Provider's final ends that need the mark and are not
	// decided yet.
	Ends []FinalEnd `json:"ends"`
}

// The relay states of HBEP-8's hpcburst_relays. A relay waiting for its pair
// of slots is retrying (#343's Q8).
const (
	StateUp       = "up"
	StateRetrying = "retrying"
	StateFinal    = "final"
)

// RelayState is one relay as the keeper keeps it.
type RelayState struct {
	UID     string                `json:"uid"`
	Tag     string                `json:"tag"`
	JobID   string                `json:"job"`
	Channel agentapi.RelayChannel `json:"channel"`
	// State is up only while a process of the relay runs.
	State string `json:"state"`
	// PID is the relay's ssh process, 0 while none runs.
	PID int `json:"pid"`
	// Epoch and Seq are the current or last process's fence.
	Epoch  int64     `json:"epoch"`
	Seq    int64     `json:"seq"`
	Starts int       `json:"starts"`
	Since  time.Time `json:"since"`
	// LastEnd is how the last process ended, in hostdial's own fixed words,
	// never srun's or ssh's output.
	LastEnd string `json:"lastEnd"`
	// LoginHost is the login host the relay runs through, as the Runner
	// resolved it, and ProviderUID the UID of the Provider whose set the
	// relay was started under: the claims ask which relays of a Provider run
	// on a host, each relay counted for the Provider it started under.
	LoginHost   string `json:"loginHost"`
	ProviderUID string `json:"providerUID"`
}

// The final ends that need the RelayEndedForGood mark.
const (
	EndFailed     = "failed"
	EndSuperseded = "superseded"
)

// FinalEnd is a relay's final end that needs the mark: a failure, or in beta
// a superseded end (HBEP-6, The relay keeper).
type FinalEnd struct {
	UID     string                `json:"uid"`
	Tag     string                `json:"tag"`
	JobID   string                `json:"job"`
	Channel agentapi.RelayChannel `json:"channel"`
	Kind    string                `json:"kind"`
	// Cause is hostdial's description of the end.
	Cause string    `json:"cause"`
	At    time.Time `json:"at"`
	// Epoch and Seq are the relay process that ended: an Ack names exactly
	// this end.
	Epoch int64 `json:"epoch"`
	Seq   int64 `json:"seq"`
}

// The outcomes of an Ack.
const (
	OutcomeMarked  = "marked"
	OutcomeRefused = "refused"
)

// The reasons of a refused Ack: the mark's refusals as providerctl's
// checkMark and checkRunning make them.
const (
	AckAnotherUID = "another_uid"
	AckAnotherJob = "another_job"
	AckNotRunning = "not_running"
	AckDeleted    = "deleted"
)

// Ack is the leading controller's acknowledgement of a final end once it is
// decided: the mark written, or its write refused with its reason. It is
// fenced by LeaseEpoch.
type Ack struct {
	Provider   string                `json:"provider"`
	LeaseEpoch int64                 `json:"leaseEpoch"`
	UID        string                `json:"uid"`
	JobID      string                `json:"job"`
	Channel    agentapi.RelayChannel `json:"channel"`
	Epoch      int64                 `json:"epoch"`
	Seq        int64                 `json:"seq"`
	Outcome    string                `json:"outcome"`
	// Reason is set exactly when Outcome is OutcomeRefused.
	Reason string `json:"reason"`
}

// Acked is the keeper's reply to an accepted Ack. Decided is false when the
// keeper holds no such end: it was dropped already, or never recorded.
type Acked struct {
	Provider string `json:"provider"`
	Decided  bool   `json:"decided"`
}

// The refusals' reasons. Only RefusedEpoch and RefusedLoginHost are counted,
// in HBEP-8's hpcburst_relay_sets_refused_total.
const (
	RefusedEpoch     = "epoch"
	RefusedLoginHost = "login_host"
	RefusedVersion   = "version"
	RefusedMalformed = "malformed"
	// RefusedState is a keeper that could not write and sync its state file,
	// so the request took no effect; the sender tries again on its next round.
	RefusedState = "state"
)

// Refused is the keeper's answer to a request it refuses.
type Refused struct {
	Reason string `json:"reason"`
	// HighestEpoch is the Provider's highest accepted Lease epoch, for
	// RefusedEpoch.
	HighestEpoch int64 `json:"highestEpoch,omitempty"`
	// LoginHost and Holder name the login host and the Provider holding it,
	// for RefusedLoginHost.
	LoginHost string `json:"loginHost,omitempty"`
	Holder    string `json:"holder,omitempty"`
	// Supported are the versions served, for RefusedVersion.
	Supported []int  `json:"supported,omitempty"`
	Message   string `json:"message"`
}

// Pong is the reply to a ping: the launcher's socket half of the keeper's
// health (#343's Q10).
type Pong struct {
	KeeperEpoch int64 `json:"keeperEpoch"`
}

// StatusReport is the reply to a status request: read-only and unfenced, for
// the serving lifecycle copy's collector (HBEP-8), which need not lead the
// Provider.
type StatusReport struct {
	KeeperEpoch int64            `json:"keeperEpoch"`
	Started     time.Time        `json:"started"`
	Providers   []ProviderStatus `json:"providers"`
}

// ProviderStatus is one Provider's relays and counters since the keeper
// started.
type ProviderStatus struct {
	Provider string       `json:"provider"`
	Relays   []RelayState `json:"relays"`
	// Restarts counts each channel's relay starts after the first, over every
	// Allocation (HBEP-8's hpcburst_relay_restarts_total).
	Restarts map[agentapi.RelayChannel]uint64 `json:"restarts"`
	// Refused counts the refused sets and acknowledgements by reason
	// (hpcburst_relay_sets_refused_total).
	Refused map[string]uint64 `json:"refused"`
}
