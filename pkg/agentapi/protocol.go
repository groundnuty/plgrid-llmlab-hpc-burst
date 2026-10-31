package agentapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// The protocol's version and HTTP framing (HBEP-7, The allocation agent: the
// endpoint).
const (
	// ProtocolVersion is the version in the path.
	ProtocolVersion = "v1"

	// PathPrefix is what the front's direct-mode dial-home listener passes
	// through to the endpoint.
	PathPrefix = "/agent/" + ProtocolVersion + "/"

	// PathRegister takes POST with a RegisterRequest.
	PathRegister = PathPrefix + "register"
	// PathDesired takes GET, a long poll from QuerySince.
	PathDesired = PathPrefix + "desired"
	// PathReport takes POST with a Report, and answers 204.
	PathReport = PathPrefix + "report"

	// QuerySince is PathDesired's query parameter: the Version of the last
	// desired state the agent saw, empty on its first poll.
	QuerySince = "since"

	// ContentType of every body.
	ContentType = "application/json"

	// HeaderTag, HeaderJobID and HeaderAgentInstanceID identify the agent on
	// every call, beside "Authorization: Bearer <credential>". The agent
	// instance ID is random per agent process (doc.go), never the kernel's
	// boot ID.
	HeaderTag             = "Hpcburst-Tag"
	HeaderJobID           = "Hpcburst-Job-Id"
	HeaderAgentInstanceID = "Hpcburst-Agent-Instance-Id"

	// MaxBodyBytes is the largest request or response body either side
	// accepts.
	MaxBodyBytes = 4 << 20
)

// Timing of the protocol.
const (
	// ReportInterval: the agent reports at least this often, and at every
	// change (HBEP-7, Reports).
	ReportInterval = 15 * time.Second

	// LongPollTimeout: the longest a PathDesired call waits for a change.
	// The agent's HTTP client allows at least this plus ten seconds.
	LongPollTimeout = 30 * time.Second

	// MaxWarmUpRounds: a warm-up that has not settled after this many rounds
	// fails qualification (HBEP-7, Instance start).
	MaxWarmUpRounds = 5
)

// Caller is an agent the server has authenticated: the credential is the
// Allocation's and the job ID is the one its provider recorded. The server
// fills it from the request's headers and the Allocation it found.
type Caller struct {
	// Namespace and Allocation name the Allocation; AllocationUID is its UID.
	Namespace     string
	Allocation    string
	AllocationUID string
	// Tag is the Allocation's job tag.
	Tag string
	// JobID is the job ID the agent presented, equal to the recorded one.
	JobID string
	// AgentInstanceID is the agent process's random ID, from
	// HeaderAgentInstanceID.
	AgentInstanceID string
}

// Backend is the lifecycle controller's side of the agent API: the server
// (issue #60) authenticates each call and then calls one method; the
// lifecycle controller (issue #61) implements it. Methods are safe for
// concurrent use and do not block: the server long-polls by calling Desired
// again, at most once a second, until the Version changes or LongPollTimeout
// passes. An error that is an *Error goes to the agent with its code; any
// other error is CodeInternal.
type Backend interface {
	// Register records the caller's registration: it binds the agent
	// instance ID to the Allocation, and the controller copies it, with the
	// restart count, to the Instance it starts. Registering again with the
	// same agent instance ID, as after the controller's restart, is accepted
	// and changes nothing.
	Register(ctx context.Context, c Caller, r RegisterRequest) (RegisterResponse, error)

	// Desired returns what the caller's Allocation should run now, with its
	// Version set (DesiredState.WithVersion). It is computed from API objects
	// alone. A caller whose agent instance ID has no registration gets
	// CodeNotRegistered.
	Desired(ctx context.Context, c Caller) (DesiredState, error)

	// Report takes the caller's latest full snapshot. A caller whose agent
	// instance ID has no registration gets CodeNotRegistered.
	Report(ctx context.Context, c Caller, r Report) error
}

// Code is an error's code on the wire.
type Code string

// Error codes and their HTTP status (Error.HTTPStatus).
const (
	// CodeBadRequest (400): a malformed body, header or query.
	CodeBadRequest Code = "BadRequest"
	// CodeUnauthenticated (401): no credential, an unknown tag, or a
	// credential that is not the Allocation's. The agent retries with backoff
	// and stops when its job ends.
	CodeUnauthenticated Code = "Unauthenticated"
	// CodeJobMismatch (403): the Allocation's credential with a job ID that is
	// not the recorded one, or before one is recorded. The agent retries with
	// backoff; the controller fails the Instance after
	// health.registrationTimeout.
	CodeJobMismatch Code = "JobMismatch"
	// CodeNotRegistered (409): Desired or Report from an agent instance ID the
	// controller does not know, as after its own restart. The agent registers
	// again at once, with the same agent instance ID, and retries.
	CodeNotRegistered Code = "NotRegistered"
	// CodeUnavailable (503): the controller is not ready (its caches are not
	// synced, or it is not the leader). The agent retries with backoff.
	CodeUnavailable Code = "Unavailable"
	// CodeInternal (500): anything else.
	CodeInternal Code = "Internal"
)

// Error is the body of every error response.
type Error struct {
	Code    Code   `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return string(e.Code) + ": " + e.Message }

// HTTPStatus returns the status an error is sent with.
func (e *Error) HTTPStatus() int {
	switch e.Code {
	case CodeBadRequest:
		return http.StatusBadRequest
	case CodeUnauthenticated:
		return http.StatusUnauthorized
	case CodeJobMismatch:
		return http.StatusForbidden
	case CodeNotRegistered:
		return http.StatusConflict
	case CodeUnavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

// Errorf returns an *Error with the code and a formatted message.
func Errorf(code Code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// WithVersion returns d with its Version set from its other fields: the first
// 16 hex characters of the SHA-256 of its JSON with an empty Version. Equal
// states get equal versions, and a change to any field changes it.
func (d DesiredState) WithVersion() DesiredState {
	d.Version = ""
	b, err := json.Marshal(d)
	if err != nil {
		// Every field is a plain value; Marshal cannot fail.
		panic(fmt.Sprintf("agentapi: marshal desired state: %v", err))
	}
	sum := sha256.Sum256(b)
	d.Version = hex.EncodeToString(sum[:])[:16]
	return d
}
