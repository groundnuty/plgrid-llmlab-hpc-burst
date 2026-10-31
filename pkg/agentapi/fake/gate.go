package fake

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"sync"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// GateAllocation is what the agent API server knows of one Allocation, found by
// its job tag: the Allocation's identity, its credential, and the job ID its
// provider recorded.
type GateAllocation struct {
	Namespace string
	Name      string
	UID       string
	// Credential is the Allocation's credential. It must not be empty.
	Credential string
	// JobID is the job ID the provider recorded, empty until it records one.
	JobID string
}

// Refusal is one call a Gate refused.
type Refusal struct {
	// Method is MethodRegister, MethodDesired or MethodReport.
	Method string
	Tag    string
	Code   agentapi.Code
}

// Gate stands in for the agent API server's authentication step (agentapi's
// documentation, How reports reach the controller): it finds the Allocation by
// the tag the agent presents, compares the credential in constant time, then
// compares the job ID with the recorded one, and only then calls the inner
// Backend, with the Caller filled from its table. A refused call never reaches
// the inner Backend. It is safe for concurrent use.
type Gate struct {
	inner agentapi.Backend

	mu       sync.Mutex
	table    map[string]GateAllocation
	refusals []Refusal
}

// NewGate returns a Gate in front of inner, with an empty table: it refuses
// every call until Add is called.
func NewGate(inner agentapi.Backend) *Gate {
	return &Gate{inner: inner, table: map[string]GateAllocation{}}
}

// Add puts the Allocation in the table under its tag, replacing one with that
// tag. It panics when the tag, the UID or the credential is empty, because an
// empty credential would let an empty presented one in.
func (g *Gate) Add(tag string, a GateAllocation) {
	if tag == "" || a.UID == "" || a.Credential == "" {
		panic("fake: Gate.Add needs a tag, an Allocation UID and a credential")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.table[tag] = a
}

// SetJobID records the job ID of the Allocation with the tag, as its provider
// does once it has submitted the job. It panics when the tag is not in the
// table, because a test that sets nothing would pass for the wrong reason.
func (g *Gate) SetJobID(tag, jobID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	a, ok := g.table[tag]
	if !ok {
		panic(fmt.Sprintf("fake: Gate.SetJobID: no Allocation with tag %q", tag))
	}
	a.JobID = jobID
	g.table[tag] = a
}

// Refusals returns every call the Gate has refused, oldest first.
func (g *Gate) Refusals() []Refusal {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]Refusal, len(g.refusals))
	copy(out, g.refusals)
	return out
}

// As returns the Backend an agent presenting this credential, tag and job ID
// talks to. Each call is authenticated afresh, against the table as it is
// then, as the server holds no session. The Caller the agent passes supplies
// only its AgentInstanceID; the rest comes from the table.
func (g *Gate) As(credential, tag, jobID string) agentapi.Backend {
	return presented{g: g, credential: credential, tag: tag, jobID: jobID}
}

// presented is an agent's view of a Gate: the identity it presents on every call.
type presented struct {
	g          *Gate
	credential string
	tag        string
	jobID      string
}

func (p presented) Register(ctx context.Context, c agentapi.Caller, r agentapi.RegisterRequest) (agentapi.RegisterResponse, error) {
	c, err := p.g.authenticate(MethodRegister, p, c)
	if err != nil {
		return agentapi.RegisterResponse{}, err
	}
	return p.g.inner.Register(ctx, c, r)
}

func (p presented) Desired(ctx context.Context, c agentapi.Caller) (agentapi.DesiredState, error) {
	c, err := p.g.authenticate(MethodDesired, p, c)
	if err != nil {
		return agentapi.DesiredState{}, err
	}
	return p.g.inner.Desired(ctx, c)
}

func (p presented) Report(ctx context.Context, c agentapi.Caller, r agentapi.Report) error {
	c, err := p.g.authenticate(MethodReport, p, c)
	if err != nil {
		return err
	}
	return p.g.inner.Report(ctx, c, r)
}

// authenticate applies the server's rule and returns the Caller to pass on. A
// wrong or unknown credential is CodeUnauthenticated, with one message for
// both; the right credential with another job ID, or before one is recorded, is
// CodeJobMismatch.
func (g *Gate) authenticate(method string, p presented, from agentapi.Caller) (agentapi.Caller, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	a, known := g.table[p.tag]
	credentialOK := sameSecret(p.credential, a.Credential) // also for an unknown tag
	if !known || !credentialOK {
		return agentapi.Caller{}, g.refuse(method, p.tag, agentapi.Errorf(agentapi.CodeUnauthenticated, "unknown tag or wrong credential"))
	}
	if a.JobID == "" || p.jobID != a.JobID {
		return agentapi.Caller{}, g.refuse(method, p.tag, agentapi.Errorf(agentapi.CodeJobMismatch, "the job ID is not the one recorded for tag %q", p.tag))
	}
	return agentapi.Caller{
		Namespace:       a.Namespace,
		Allocation:      a.Name,
		AllocationUID:   a.UID,
		Tag:             p.tag,
		JobID:           p.jobID,
		AgentInstanceID: from.AgentInstanceID,
	}, nil
}

// refuse records a refusal and returns its error. The caller holds g.mu.
func (g *Gate) refuse(method, tag string, err *agentapi.Error) error {
	g.refusals = append(g.refusals, Refusal{Method: method, Tag: tag, Code: err.Code})
	return err
}

// sameSecret compares two secrets in constant time, whatever their lengths, and
// is false when the stored one is empty.
func sameSecret(presented, stored string) bool {
	if stored == "" {
		return false
	}
	a, b := sha256.Sum256([]byte(presented)), sha256.Sum256([]byte(stored))
	return subtle.ConstantTimeCompare(a[:], b[:]) == 1
}
