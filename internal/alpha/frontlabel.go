package alpha

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// The label of where the front ran (issue #224). The host that runs the front decides the path to the login
// nodes, and so the timings, so every live row's text to post names the host role and its network position.
// hack/live-checks.sh takes it from the setting LIVE_FRONT_HOST_LABEL and passes it in EnvFrontLabel. It is a
// description, never a host name or an address, and it has no default: a run without one is refused, so a
// post can never carry a placeholder in its place.

// EnvFrontLabel is the variable that carries the label to a row's test. It is not a live-only setting: a dry
// run takes it too, and the accounting rows (internal/providerctl/conformance) read it by the same name.
const EnvFrontLabel = "HPCBURST_FRONT_HOST_LABEL"

// MaxFrontLabel is the longest label, in bytes.
const MaxFrontLabel = 120

var (
	// frontLabelRE is the narrow form: letters, digits, spaces and a few marks of prose. No dot, colon, slash or at
	// sign, which a host name, an address or a URL needs, so none of them can be a label.
	frontLabelRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9 ,;()'-]*$`)
	// positionRE is the network position the label must state, as a word between non-letters: the same form as
	// hack/live-checks.sh's, which hack/livechecks runs on the same labels.
	positionRE = regexp.MustCompile(`(?i)(^|[^a-z])(inside|outside)([^a-z]|$)`)
)

// ValidFrontLabel checks a front-host label on the properties this repository owns. Its messages never repeat
// the label.
func ValidFrontLabel(label string) error {
	switch {
	case label == "":
		return errors.New("the front host label is not set")
	case len(label) > MaxFrontLabel:
		return fmt.Errorf("the front host label is longer than %d bytes", MaxFrontLabel)
	case !frontLabelRE.MatchString(label):
		return errors.New("the front host label may hold only letters, digits, spaces and , ; ( ) ' - (no dot, colon, slash or @: it is a description, not a host name or an address)")
	case !positionRE.MatchString(label):
		return errors.New("the front host label must say whether the front ran inside or outside Cyfronet's network")
	}
	return nil
}

// FrontLine is the line of the text to post that names where the front ran.
func FrontLine(label string) string { return "- front ran on: " + label }

// InsertFrontLine puts the front line right after the first line of a text to post (the verdict line), so that
// every row's text names it in the same place.
func InsertFrontLine(text, label string) string {
	first, rest, found := strings.Cut(text, "\n")
	if !found {
		return first + "\n" + FrontLine(label) + "\n"
	}
	return first + "\n" + FrontLine(label) + "\n" + rest
}
