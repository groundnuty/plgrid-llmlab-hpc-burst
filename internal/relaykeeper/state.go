package relaykeeper

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
)

// StateSchema is the state file's schema.
const StateSchema = 1

// EpochBase is 2³¹: a keeper run's relay epoch E is EpochBase plus the
// keeper's start count. A Lease's leaseTransitions is an int32 and the job
// side's epoch an int64, so every keeper relay outranks every alpha relay, and
// a restarted keeper's relays outrank its former run's (HBEP-6, The relay
// keeper, Fencing).
const EpochBase = int64(1) << 31

// State is the keeper's state file (HBEP-6, The relay keeper): mode 600, in
// the launcher's state directory, kept by down. Every change is written and
// synced before it takes effect.
type State struct {
	Schema int `json:"schema"`
	// Starts is the keeper's start count, the epoch base's offset.
	Starts int64 `json:"starts"`
	// Providers holds each Provider's highest accepted Lease epoch. The
	// launcher's restore clears it (ClearLeaseEpochs), since a restored store
	// rolls leaseTransitions back.
	Providers map[string]ProviderFence `json:"providers,omitempty"`
	// Ends are the final ends that need the mark. A decided end stays here
	// until its Allocation leaves a complete set, so its channel is never
	// restarted, a keeper restart included (the science agent's ruling on
	// #343's Q1).
	Ends []EndRecord `json:"ends,omitempty"`
	// Claims are the login-host claims, by login host (#343's Q2).
	Claims map[string]Claim `json:"claims,omitempty"`
}

// ProviderFence is a Provider's fence.
type ProviderFence struct {
	HighestEpoch int64 `json:"highestEpoch"`
}

// EndRecord is a final end the keeper holds.
type EndRecord struct {
	Provider string        `json:"provider"`
	End      wire.FinalEnd `json:"end"`
	// Decided is set once the leader has acknowledged the end, with its
	// Outcome and, when refused, the Reason.
	Decided bool   `json:"decided,omitempty"`
	Outcome string `json:"outcome,omitempty"`
	Reason  string `json:"reason,omitempty"`
}

// Claim is a Provider's claim on a login host.
type Claim struct {
	Provider    string    `json:"provider"`
	ProviderUID string    `json:"providerUID"`
	Created     time.Time `json:"created"`
	// Since is when the claim was made.
	Since time.Time `json:"since"`
}

// Epoch is the relay epoch E of the keeper run that made Starts what it is.
func (s *State) Epoch() int64 { return EpochBase + s.Starts }

// clone is a deep copy of s.
func (s *State) clone() *State {
	c := *s
	c.Providers = maps.Clone(s.Providers)
	c.Ends = slices.Clone(s.Ends)
	c.Claims = maps.Clone(s.Claims)
	return &c
}

// LoadState reads the state file at path. A missing file is a first start: a
// fresh state. A file that cannot be read or parsed, or of another schema, is
// an error, never a fresh state: a fresh state would lower E below the
// relays of the run before (HBEP-6, Operational readiness 7: "its state file
// must be kept, or a new keeper's epochs could fall below its old relays'").
func LoadState(path string) (*State, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return &State{Schema: StateSchema}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("relaykeeper: read the state file: %w", err)
	}
	var s State
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(&s); err != nil {
		return nil, fmt.Errorf("relaykeeper: the state file cannot be parsed, and is not replaced: %w", err)
	}
	if s.Schema != StateSchema {
		return nil, fmt.Errorf("relaykeeper: the state file has schema %d, want %d, and is not replaced", s.Schema, StateSchema)
	}
	if s.Starts < 0 {
		return nil, errors.New("relaykeeper: the state file has a negative start count, and is not replaced")
	}
	return &s, nil
}

// SaveState writes s to path, mode 600: a temporary file in the same
// directory, written and synced, renamed over path, and the directory synced,
// so a crash leaves the old file or the new one, never a part of either.
func SaveState(path string, s *State) error {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".state-*")
	if err != nil {
		return fmt.Errorf("relaykeeper: write the state file: %w", err)
	}
	tmp := f.Name()
	werr := f.Chmod(0o600)
	if werr == nil {
		_, werr = f.Write(append(b, '\n'))
	}
	if werr == nil {
		werr = f.Sync()
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr == nil {
		werr = os.Rename(tmp, path)
	}
	if werr != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("relaykeeper: write the state file: %w", werr)
	}
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("relaykeeper: sync the state file's directory: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("relaykeeper: sync the state file's directory: %w", err)
	}
	return nil
}

// OpenState is a keeper's start: it makes the state file's directory (mode
// 700) when it is missing, reads the state, counts this start, and writes
// and syncs that before it returns. So E, the returned state's Epoch, is on
// disk before any relay of this run starts (HBEP-6: "written and synced before
// any relay of that run starts").
func OpenState(path string) (*State, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("relaykeeper: the state file's directory: %w", err)
	}
	s, err := LoadState(path)
	if err != nil {
		return nil, err
	}
	s.Starts++
	if err := SaveState(path, s); err != nil {
		return nil, err
	}
	return s, nil
}

// ClearLeaseEpochs drops every Provider's highest accepted Lease epoch from
// the state file at path and keeps the rest: the start count, the ends and
// the claims. It is the launcher's restore's (HBEP-6, The relay keeper: "The
// launcher's restore clears the accepted Lease epochs, since a restored store
// rolls leaseTransitions back"), run while no keeper runs. A missing file
// has nothing to clear.
func ClearLeaseEpochs(path string) error {
	if _, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	s, err := LoadState(path)
	if err != nil {
		return err
	}
	s.Providers = nil
	return SaveState(path, s)
}
