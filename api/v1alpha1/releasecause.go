package v1alpha1

import (
	"regexp"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

// The release cause of an Allocation that an Operation released
// (AnnotationReleaseCause; HBEP-11, The Allocation's record; HBEP-10, Audit):
// the whole cause is "operation <namespace>/<name> <uid>", single-spaced, with
// nothing before or after it. The lifecycle controller's executor writes it
// with ReleaseCauseOperation, and the Provider's controller reads it with
// ParseReleaseCauseOperation to name the Operation in its JobEnded event's
// related object (HBEP-8, Events, Allocation events). One pair, so the writer
// and the reader cannot drift (#589, #625).
//
// The annotation holds at most 256 bytes, and its writer cuts a longer cause
// and ends it with "…" (internal/lifecycle, releaseCause). With a namespace of
// 63 characters and a 36-character UID, an Operation name of up to 145
// characters fits; a longer one is cut, and the parser refuses the cut cause,
// so the event then names no Operation (events are best-effort).

// releaseCauseOperationPattern is the whole cause: a namespace (a DNS label),
// an Operation name (a DNS subdomain) and a UID in the API server's lowercase
// UUID form. Each part is checked further after the match.
var releaseCauseOperationPattern = regexp.MustCompile(`^operation ([a-z0-9-]+)/(\S+) ([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$`)

// ReleaseCauseOperation returns the release cause of an Allocation that the
// Operation namespace/name, with that UID, released.
func ReleaseCauseOperation(namespace, name string, uid types.UID) string {
	return "operation " + namespace + "/" + name + " " + string(uid)
}

// ParseReleaseCauseOperation reads an Operation's release cause. ok is false,
// with every other result empty, for any other cause, for a cause with
// anything before or after the form, and for a cut one: a cause the writer
// shortened to the annotation's bound never yields a partial name or UID. The
// namespace must be a DNS label and the name a DNS subdomain, as the API
// server requires of an Operation.
func ParseReleaseCauseOperation(cause string) (namespace, name string, uid types.UID, ok bool) {
	m := releaseCauseOperationPattern.FindStringSubmatch(cause)
	if m == nil {
		return "", "", "", false
	}
	if len(validation.IsDNS1123Label(m[1])) != 0 || len(validation.IsDNS1123Subdomain(m[2])) != 0 {
		return "", "", "", false
	}
	return m[1], m[2], types.UID(m[3]), true
}
