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

// FrontRoles are the roles a label may name, the first of its three parts. The list is this code's: a role is added by
// a change here (and in hack/live-checks.sh's FRONT ROLES table, which hack/livechecks compares with this list), never
// by the setting, so a host's name cannot stand in a role's place.
var FrontRoles = []string{"the development host", "a Cyfronet-network host", "a cloud host"}

// The label is ROLE ", " POSITION [ " (" NOTE ")" ] (PR 244, B1; the science agent's ruling 3 of 2 Nov 2026):
//   - ROLE is one of FrontRoles.
//   - POSITION is exactly "inside" or "outside", then " Cyfronet" or " Cyfronet's network". It is required.
//   - NOTE is optional, in parentheses: words of letters (an inner apostrophe or hyphen is a part of the word), and
//     numbers with a unit (3.4 ms, 40 MB/s), each after a space, a comma or a semicolon. A token that mixes letters
//     and digits (gw-01, node7), a dotted token that is not a decimal number (gw.example, 10.0.0.1) and a colon are
//     refused, so a short host name or an address cannot pass for a note. The 120-byte limit holds for the whole.
const (
	noteWord     = `[A-Za-z]+(?:['-][A-Za-z]+)*`
	noteQuantity = `[0-9]+(?:\.[0-9]+)? ?(?:ms|s|MB/s|Mb/s|%)`
	noteItem     = `(?:` + noteQuantity + `|` + noteWord + `)`
)

var (
	// positionRE takes what follows the role: the position, then the parenthesised note if there is one (group 1 is the
	// note with its parentheses, group 2 the note).
	positionRE = regexp.MustCompile(`^(?:inside|outside) Cyfronet(?:'s network)?( \((.*)\))?$`)
	noteRE     = regexp.MustCompile(`^` + noteItem + `(?:[,;]? ` + noteItem + `)*$`)
)

// ValidFrontLabel checks a front-host label on the properties this repository owns. Its messages never repeat
// the label. hack/live-checks.sh holds the same rule (check_front_label), and hack/livechecks runs both on the same
// labels.
func ValidFrontLabel(label string) error {
	if label == "" {
		return errors.New("the front host label is not set")
	}
	if len(label) > MaxFrontLabel {
		return fmt.Errorf("the front host label is longer than %d bytes", MaxFrontLabel)
	}
	rest, ok := cutRole(label)
	if !ok {
		return fmt.Errorf("the front host label must begin with a role, then a comma and a space: one of %s", strings.Join(FrontRoles, "; "))
	}
	m := positionRE.FindStringSubmatch(rest)
	if m == nil {
		return errors.New("the front host label must say inside or outside Cyfronet (or Cyfronet's network) after its role, then at most a note in parentheses")
	}
	if m[1] != "" && !noteRE.MatchString(m[2]) {
		return errors.New("the note of the front host label may hold only words of letters, numbers with a unit (3.4 ms, 40 MB/s) and , ; ' - (no word with a digit in it, no dotted name or address, no colon)")
	}
	return nil
}

// cutRole takes the role and its comma and space off the front of a label.
func cutRole(label string) (rest string, ok bool) {
	for _, r := range FrontRoles {
		if rest, ok := strings.CutPrefix(label, r+", "); ok {
			return rest, true
		}
	}
	return "", false
}

// FrontLine is the line of the text to post that names where the front ran.
func FrontLine(label string) string { return "- front ran on: " + label }

// InsertFrontLine puts the front line right after the first line of a text to post (the verdict line), so that
// every row's text names it in the same place.
func InsertFrontLine(text, label string) string {
	first, rest, _ := strings.Cut(text, "\n")
	return first + "\n" + FrontLine(label) + "\n" + rest
}
