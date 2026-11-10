package wire_test

import (
	"fmt"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Every value here is an example: no real user, host or account name.

// ts is a UTC time with a fraction, so a round trip through JSON loses
// nothing and reflect.DeepEqual holds.
var ts = time.Date(2026, 11, 10, 12, 0, 0, 123456789, time.UTC)

// validEntry is a valid Allocation; n makes its UID, tag and job unique.
func validEntry(n int) wire.Entry {
	return wire.Entry{
		UID:       fmt.Sprintf("0a1b2c3d-0000-4000-8000-%012d", n),
		Tag:       fmt.Sprintf("hb-job-%d-%012x", n, 0xabcdef00+n),
		JobID:     fmt.Sprintf("%d", 4000+n),
		AllocDir:  fmt.Sprintf("/scratch/example/hb/alloc-%d", n),
		AgentPath: "/scratch/example/hb/agent",
		Gen:       "0123456789abcdef",
	}
}

// validSet is a valid DesiredSet of two Allocations. Each call returns a set
// of its own, so a test may change it.
func validSet() wire.DesiredSet {
	return wire.DesiredSet{
		Provider:        "provider-a",
		ProviderUID:     "6f1d2c3b-aaaa-4bbb-8ccc-000000000001",
		ProviderCreated: ts,
		LeaseEpoch:      3,
		Complete:        true,
		ListedAt:        ts,
		ListingAgeMS:    1200,
		ValidForMS:      40000,
		SSH: wire.SSH{
			HostName:       "login.example",
			User:           "example-user",
			Port:           22,
			IdentityFile:   "/home/example-user/.ssh/id-ed25519",
			KnownHostsFile: "/home/example-user/.ssh/known-hosts",
		},
		Allocations: []wire.Entry{validEntry(1), validEntry(2)},
	}
}

// validAck is a valid Ack of a mark that was written.
func validAck() wire.Ack {
	return wire.Ack{
		Provider:   "provider-a",
		LeaseEpoch: 3,
		UID:        "u-1",
		JobID:      "4001",
		Channel:    agentapi.RelayControl,
		Epoch:      5,
		Seq:        2,
		Outcome:    wire.OutcomeMarked,
	}
}

func sampleRelay() wire.RelayState {
	return wire.RelayState{
		UID: "u-1", Tag: validEntry(1).Tag, JobID: "4001",
		Channel: agentapi.RelayControl, State: wire.StateUp, PID: 4242,
		Epoch: 5, Seq: 2, Starts: 2, Since: ts, LastEnd: "exited",
	}
}

func sampleReport() wire.Report {
	return wire.Report{
		Provider:    "provider-a",
		KeeperEpoch: 5,
		LoginHost:   "login.example",
		Relays:      []wire.RelayState{sampleRelay()},
		Ends: []wire.FinalEnd{{
			UID: "u-1", Tag: validEntry(1).Tag, JobID: "4001",
			Channel: agentapi.RelayServing, Kind: wire.EndFailed,
			Cause: "the relay failed", At: ts, Epoch: 5, Seq: 3,
		}},
	}
}

func sampleStatus() wire.StatusReport {
	return wire.StatusReport{
		KeeperEpoch: 5,
		Started:     ts,
		Providers: []wire.ProviderStatus{{
			Provider: "provider-a",
			Relays:   []wire.RelayState{sampleRelay()},
			Restarts: map[agentapi.RelayChannel]uint64{agentapi.RelayControl: 1, agentapi.RelayServing: 0},
			Refused:  map[string]uint64{wire.RefusedEpoch: 2},
		}},
	}
}

// message is one kind with a body and a pointer to a fresh value of the
// body's type.
type message struct {
	kind  wire.Kind
	body  any
	fresh func() any
}

// everyMessage has every kind of the protocol, request and reply.
func everyMessage() []message {
	return []message{
		{wire.KindPing, struct{}{}, func() any { return new(struct{}) }},
		{wire.KindPong, wire.Pong{KeeperEpoch: 9}, func() any { return new(wire.Pong) }},
		{wire.KindSet, validSet(), func() any { return new(wire.DesiredSet) }},
		{wire.KindReport, sampleReport(), func() any { return new(wire.Report) }},
		{wire.KindAck, validAck(), func() any { return new(wire.Ack) }},
		{wire.KindAcked, wire.Acked{Provider: "provider-a", Decided: true}, func() any { return new(wire.Acked) }},
		{wire.KindStatus, struct{}{}, func() any { return new(struct{}) }},
		{wire.KindStatusReport, sampleStatus(), func() any { return new(wire.StatusReport) }},
		{wire.KindRefused, wire.Refused{
			Reason: wire.RefusedLoginHost, LoginHost: "login.example", Holder: "provider-b",
			Supported: []int{1}, HighestEpoch: 7, Message: "the login host is held",
		}, func() any { return new(wire.Refused) }},
	}
}
