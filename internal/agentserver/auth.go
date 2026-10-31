package agentserver

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"io/fs"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// What the agent is told when it is refused. One message covers an unknown tag,
// a missing header, a wrong credential and a missing credential file, so the
// answer says nothing about which of them it was; none echoes a header value.
const (
	msgUnauthenticated = "unknown tag or wrong credential"
	msgJobMismatch     = "the job ID is not the one recorded for this Allocation"
	msgUnavailable     = "cannot look up the Allocation now"
)

// maxLogged cuts a header value in a log line: before authentication it is
// whatever the caller sent.
const maxLogged = 128

// authenticate applies the rule of pkg/agentapi/fake.Gate to every call, before
// the call reads its body or reaches the Backend:
//
//  1. The Allocation is found by the tag, and its credential read from the file
//     named by its UID. The presented credential is compared with it in
//     constant time, and the comparison is made for an unknown tag too. An
//     unknown or ambiguous tag, a missing Authorization header or tag, a wrong
//     credential and a missing credential file are all CodeUnauthenticated.
//  2. Then the job ID must be the recorded one, and a recorded one must exist:
//     an empty recorded job ID never matches, even an empty presented one. That
//     is CodeJobMismatch. A missing job ID header is an empty presented job ID.
//  3. Then the agent instance ID must have its pinned form (CodeBadRequest).
//
// On success the Caller comes from the Allocation and the headers.
func (s *Server) authenticate(ctx context.Context, r *http.Request, id *callIdentity) (agentapi.Caller, result) {
	token, hasToken := bearerToken(r.Header.Get("Authorization"))
	tag := r.Header.Get(agentapi.HeaderTag)
	jobID := r.Header.Get(agentapi.HeaderJobID)
	instanceID := r.Header.Get(agentapi.HeaderAgentInstanceID)
	*id = callIdentity{
		tag:             agentapi.TruncateUTF8(tag, maxLogged),
		jobID:           agentapi.TruncateUTF8(jobID, maxLogged),
		agentInstanceID: agentapi.TruncateUTF8(instanceID, maxLogged),
	}

	var (
		alloc  Allocation
		known  bool
		stored string
		why    error // for the log, when the answer is Unauthenticated
	)
	if hasToken && tag != "" {
		a, err := s.lookup.ByTag(ctx, tag)
		switch {
		case err == nil:
			alloc, known = a, true
		case errors.Is(err, ErrNotFound):
		case errors.Is(err, ErrAmbiguous):
			why = err
		default:
			return agentapi.Caller{}, result{err: agentapi.Errorf(agentapi.CodeUnavailable, msgUnavailable), cause: err}
		}
	}
	if known {
		key, err := s.readCredential(alloc.UID)
		switch {
		case err == nil:
			stored = key
		case errors.Is(err, fs.ErrNotExist):
			why = err
		default:
			return agentapi.Caller{}, result{err: agentapi.Errorf(agentapi.CodeInternal, "internal error"), cause: err}
		}
	}
	if !sameSecret(token, stored) { // also for an unknown tag: stored is then empty
		return agentapi.Caller{}, result{err: agentapi.Errorf(agentapi.CodeUnauthenticated, msgUnauthenticated), cause: why}
	}
	if alloc.JobID == "" || jobID != alloc.JobID {
		return agentapi.Caller{}, result{err: agentapi.Errorf(agentapi.CodeJobMismatch, msgJobMismatch)}
	}
	if err := agentapi.CheckAgentInstanceID(instanceID); err != nil {
		return agentapi.Caller{}, result{err: agentapi.Errorf(agentapi.CodeBadRequest, "%s: %v", agentapi.HeaderAgentInstanceID, err)}
	}
	return agentapi.Caller{
		Namespace:       alloc.Namespace,
		Allocation:      alloc.Name,
		AllocationUID:   alloc.UID,
		Tag:             tag,
		JobID:           jobID,
		AgentInstanceID: instanceID,
	}, result{}
}

// readCredential reads the Allocation's credential file. An error that wraps
// fs.ErrNotExist means no file; any other is a file the controller wrote wrong.
// The UID comes from a lookup that may be a stub, so it must be one path
// element.
func (s *Server) readCredential(uid string) (string, error) {
	if uid == "" || uid == "." || uid == ".." || uid != filepath.Base(uid) {
		return "", errors.New("the Allocation's UID is not usable as a file name")
	}
	return agentapi.ReadKeyFile(agentapi.AllocationCredentialPath(s.credDir, uid))
}

// bearerToken returns the token of an "Authorization: Bearer <token>" value,
// and false when the value is another scheme or has no token.
func bearerToken(h string) (string, bool) {
	const scheme = "Bearer "
	if len(h) <= len(scheme) || !strings.EqualFold(h[:len(scheme)], scheme) {
		return "", false
	}
	return h[len(scheme):], true
}

// sameSecret compares the presented secret with the stored one in constant
// time over their SHA-256, whatever their lengths, and is false when the stored
// one is empty. The comparison is made even then.
func sameSecret(presented, stored string) bool {
	a, b := sha256.Sum256([]byte(presented)), sha256.Sum256([]byte(stored))
	equal := subtle.ConstantTimeCompare(a[:], b[:]) == 1
	return equal && stored != ""
}
