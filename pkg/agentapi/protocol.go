package agentapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
	"unicode/utf8"
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
	// boot ID, and has the form CheckAgentInstanceID accepts (the agent draws
	// it with NewAgentInstanceID): the server answers another form with
	// CodeBadRequest before it calls the Backend.
	HeaderTag             = "Hpcburst-Tag"
	HeaderJobID           = "Hpcburst-Job-Id"
	HeaderAgentInstanceID = "Hpcburst-Agent-Instance-Id"

	// MaxBodyBytes is the largest request or response body either side
	// accepts. Both sides apply it through EncodeBody and DecodeBody (see
	// there), and the bounds below keep a report at the admission bounds under
	// it: a Report is a full snapshot, so its size is what the bounds allow.
	MaxBodyBytes = 4 << 20

	// MaxTargetSessions is the most concurrent requests a warm-up round sends:
	// the CRD's maximum for Workload.TargetSessions, so WarmUpRequest cannot
	// ask for more. A round holds at most 4 + 1 + MaxTargetSessions timings (the
	// four fixed shapes, the tool call, the concurrent requests).
	MaxTargetSessions = 1024

	// MaxTimingErrorBytes is the most bytes of RequestTiming.Error that a
	// report carries: the agent cuts the text to it (TruncateUTF8). Every
	// warm-up request of a round can carry one, so it is short.
	MaxTimingErrorBytes = 64

	// MaxMessageBytes is the most bytes of every other free-text field of a
	// report: VerifyResult.Error, CheckOutcome.Message and Staging.Message. The
	// agent cuts each to it (TruncateUTF8).
	MaxMessageBytes = 256

	// WarmUpBudgetBytes is the most the warm-up results of one report take, at
	// the admission bounds: MaxWarmUpRounds rounds of 4 + 1 + MaxTargetSessions
	// timings, each as long as JSON can make it. The rest of a report fits the
	// remaining MiB of MaxBodyBytes.
	WarmUpBudgetBytes = 3 << 20
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
	// HeaderAgentInstanceID, which the server has checked with
	// CheckAgentInstanceID.
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
	// and the controller's key files alone. A caller whose agent instance ID has no registration gets
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

// EncodeBody returns v as a body: its JSON, which must not exceed
// MaxBodyBytes. It refuses a larger result with an *Error of CodeBadRequest, so
// a sender learns that its own body is too large rather than sending it to be
// refused. The agent encodes every report with it, and the server every
// response.
func EncodeBody(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("encode body: %w", err)
	}
	if len(b) > MaxBodyBytes {
		return nil, Errorf(CodeBadRequest, "body is %d bytes, above the %d-byte limit", len(b), MaxBodyBytes)
	}
	return b, nil
}

// DecodeBody reads one body from r into v. It reads at most MaxBodyBytes+1
// bytes, so a body above MaxBodyBytes is refused after that many and never read
// to its end, and unmarshals the rest with encoding/json. Unknown fields are
// allowed, as everywhere in the protocol. Every failure, a body that is too
// large, unreadable or not JSON for v, is an *Error of CodeBadRequest.
//
// The server (issue #60) decodes every request body with it. A server that
// refuses a body it has not read to its end should close the connection after
// it answers, or read and discard the rest, so the agent sees the answer and
// not a reset connection.
func DecodeBody(r io.Reader, v any) error {
	b, err := io.ReadAll(io.LimitReader(r, MaxBodyBytes+1))
	if err != nil {
		return Errorf(CodeBadRequest, "reading the body: %v", err)
	}
	if len(b) > MaxBodyBytes {
		return Errorf(CodeBadRequest, "body is above the %d-byte limit", MaxBodyBytes)
	}
	if err := json.Unmarshal(b, v); err != nil {
		return Errorf(CodeBadRequest, "malformed body: %v", err)
	}
	return nil
}

// TruncateUTF8 returns s cut to at most n bytes at a rune boundary: it never
// ends in the middle of a multi-byte character, so the result of a valid string
// is valid. A string of n bytes or fewer is returned as it is, and a negative n
// is 0. The agent cuts the free text of a report with it (MaxTimingErrorBytes,
// MaxMessageBytes).
func TruncateUTF8(s string, n int) string {
	if n < 0 {
		n = 0
	}
	if len(s) <= n {
		return s
	}
	cut := n
	// s[cut] continues a character that starts before it: back up to the start,
	// at most the three bytes a character can have before its last.
	for back := 0; back < utf8.UTFMax-1 && cut > 0 && !utf8.RuneStart(s[cut]); back++ {
		cut--
	}
	return s[:cut]
}
