// Package naming parses the model names clients send, by HBEP-3's grammar
// (Naming hierarchy, Grammar; HBEP-5, Request path step 2).
package naming

import (
	"errors"
	"strings"
)

// Level is the naming level a request asked for. The values are the usage
// record's level field (HBEP-5, Usage records).
type Level string

// Levels.
const (
	LevelModel    Level = "model"
	LevelProvider Level = "provider"
	LevelPinned   Level = "pinned"
)

// Name is a parsed model name.
type Name struct {
	Model    string
	Provider string // "" at LevelModel
	// DeploymentID is the display form, 12 lowercase hex characters; "" below
	// LevelPinned.
	DeploymentID string
	Level        Level
	// Workload is the name after the "#" suffix (HBEP-3, Grammar step 0): a
	// DNS-1123 label, "" for a name with no suffix. It does not change Level.
	Workload string
	// HeaderWorkload is the Workload the request's workloadHeader names, "" when
	// it names none. Parse never sets it: the front copies the header here, so a
	// request's whole naming travels in one value to every place that chooses a
	// target, including a retry's (HBEP-5, Workloads, Naming it).
	HeaderWorkload string
	// KeyFromHeader says the request's conversation key is its session header's, not an opening hash or a fresh key. Parse never sets it: the front does,
	// with the Workload header, since a Workload with sessionAffinity Route refuses a request without it (HBEP-5, Workloads, Sticky routes).
	KeyFromHeader bool
}

// ErrGrammar is a name that breaks the grammar: an empty part, a malformed
// deployment ID, a trailing "/", or a "#" suffix that is not one Workload name.
// It never falls back to a plain model.
var ErrGrammar = errors.New("naming: the model name breaks the grammar")

// DisplayIDLength is the length of a deployment ID's display form.
const DisplayIDLength = 12

// MaxWorkloadNameLength is the longest a Workload's name is: a DNS-1123 label.
const MaxWorkloadNameLength = 63

// Parse first splits off a "#<workload>" suffix, at the name's only "#" (HBEP-3,
// Grammar step 0): two or more "#", an empty part on either side, or a suffix
// that is not a DNS-1123 label of at most 63 characters is ErrGrammar. It then
// splits the rest at its last "@". The part before that is the model name and
// may contain "/" and "@"; the part after it is "<provider>" or
// "<provider>/<deployment-id>", split at its first "/". Whether the provider
// and the Workload exist is the caller's check (unknown_provider,
// unknown_workload).
func Parse(s string) (Name, error) {
	var workload string
	switch strings.Count(s, "#") {
	case 0:
	case 1:
		hash := strings.IndexByte(s, '#')
		s, workload = s[:hash], s[hash+1:]
		if s == "" || !IsWorkloadName(workload) {
			return Name{}, ErrGrammar
		}
	default:
		return Name{}, ErrGrammar
	}
	n, err := parseLevel(s)
	if err != nil {
		return Name{}, err
	}
	n.Workload = workload
	return n, nil
}

// parseLevel is Parse's split at the last "@", on a name with no suffix.
func parseLevel(s string) (Name, error) {
	at := strings.LastIndexByte(s, '@')
	if at < 0 {
		if s == "" {
			return Name{}, ErrGrammar
		}
		return Name{Model: s, Level: LevelModel}, nil
	}
	model, rest := s[:at], s[at+1:]
	if model == "" || rest == "" {
		return Name{}, ErrGrammar
	}
	provider, id, pinned := strings.Cut(rest, "/")
	if provider == "" {
		return Name{}, ErrGrammar
	}
	if !pinned {
		return Name{Model: model, Provider: provider, Level: LevelProvider}, nil
	}
	if !isDisplayID(id) {
		return Name{}, ErrGrammar
	}
	return Name{Model: model, Provider: provider, DeploymentID: id, Level: LevelPinned}, nil
}

// IsWorkloadName reports whether s is a Workload's name: a DNS-1123 label, which
// is lowercase letters, digits and "-", starting and ending with a letter or a
// digit, of at most 63 characters.
func IsWorkloadName(s string) bool {
	if s == "" || len(s) > MaxWorkloadNameLength {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		alnum := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z')
		if !alnum && (c != '-' || i == 0 || i == len(s)-1) {
			return false
		}
	}
	return true
}

func isDisplayID(s string) bool {
	if len(s) != DisplayIDLength {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
