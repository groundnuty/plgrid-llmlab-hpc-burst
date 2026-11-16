package front

import (
	"unicode/utf8"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/naming"
)

// The client's own text in a usage record is bounded (issue #645). A request's model name and path are the client's: the record used to copy
// the model whole, before the name was checked against the grammar, and the path whole, so a client with a valid key could make the front
// write a usage line as long as the 64 MiB body limit, and one with no key a path as long as the header limit (1 MiB), into the files that
// are the accounting record, and every reader of them pays for it (#644 met a line over 1 MiB that stopped a front from starting).
//
// The bound is the longest name a request can be SERVED under, so that no request that is served is recorded cut, and no response changes:
// the front keeps the whole name and path for the grammar, the routing and the answer, and only the record's copy is cut. The grammar itself
// has no length limit (a name with no "@" is a valid model-level name whatever its length), so the bound comes from what can be routed to.
// A longer name names nothing, and its answer, a 400 or a 503, is the same as it was. HBEP-5 (Usage records) words it: requestedModel as sent,
// cut at 1,291 bytes with a marker at the cut, the longest name the front can serve, and the decoded path at "/v1/models/" and that name, 1,302.
const (
	// apiModelChars is the most characters spec.model of a ModelDeployment holds (api/v1alpha1, ModelDeploymentSpec.Model, MaxLength). Every
	// name a request can be routed by has a model part from some ModelDeployment's spec.model, served or managed (internal/front/inventory).
	// TestTheNameBoundCoversTheLongestRoutableName reads the generated CRD to keep this equal to the API's limit.
	apiModelChars = 256
	// apiProviderNameBytes is the most bytes of a Provider's name, a Kubernetes object name.
	apiProviderNameBytes = 253
	// maxRoutableNameBytes is the longest name a request can be routed by, in bytes: the model, "@", the provider, "/" and the 12-character
	// deployment ID (naming.DisplayIDLength), the model's characters taking up to 4 bytes each in UTF-8. It is the number of bytes of a model name
	// the record keeps, 1,291.
	maxRoutableNameBytes = apiModelChars*utf8.UTFMax + 1 + apiProviderNameBytes + 1 + naming.DisplayIDLength
	// modelsPathPrefix and the bound of the path: the longest path the front answers by a name is GET /v1/models/<name>. The front takes the name
	// from the path decoded once, and the record holds the path decoded too (r.URL.Path), so a client that percent-encodes every byte of a name, 3
	// bytes each, does not lengthen what is recorded. A longer path can still be forwarded to an engine, and is recorded cut.
	modelsPathPrefix     = "/v1/models/"
	maxRecordedPathBytes = len(modelsPathPrefix) + maxRoutableNameBytes
	// cutMarker follows the bytes a recorded text keeps when it was cut, so that a reader sees it was.
	cutMarker = "…[cut]"
)

// boundedText returns s when it is at most max bytes. A longer s is cut after its first max bytes, and cutMarker follows them: the record keeps
// max bytes of the text and marks the cut. The cut is at a character boundary, so that the record holds valid UTF-8 (a record is JSON, and a split
// character would read back as U+FFFD): when byte max is inside a multi-byte character, the character is left out, and the kept part is up to 3
// bytes short of max. The boundary is looked for 3 bytes back at most, so that text that is not UTF-8 at all costs a few bytes, not the whole prefix.
func boundedText(s string, max int) string {
	if len(s) <= max {
		return s
	}
	keep := max
	for i := 0; i < utf8.UTFMax-1 && keep > 0 && !utf8.RuneStart(s[keep]); i++ {
		keep--
	}
	return s[:keep] + cutMarker
}
