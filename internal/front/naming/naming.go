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
}

// ErrGrammar is a name that breaks the grammar: an empty part, a malformed
// deployment ID or a trailing "/". It never falls back to a plain model.
var ErrGrammar = errors.New("naming: the model name breaks the grammar")

// DisplayIDLength is the length of a deployment ID's display form.
const DisplayIDLength = 12

// Parse splits a name at its last "@". The part before it is the model name
// and may contain "/" and "@"; the part after it is "<provider>" or
// "<provider>/<deployment-id>", split at its first "/". Whether the provider
// exists is the caller's check (unknown_provider).
func Parse(s string) (Name, error) {
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
