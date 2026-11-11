package relaykeeper

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
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
// fresh state. Anything else that is not a whole, valid state file is an
// error, never a fresh state, and the file is left as it is: a fresh state
// would lower E below the relays of the run before (HBEP-6, Operational
// readiness 7: "its state file must be kept, or a new keeper's epochs could
// fall below its old relays'"). Valid means: the codec's strict decoding
// (one JSON value and nothing after it, every key spelled exactly, none
// twice; wire.DecodeStrict), the schema and the start count present, and
// every field of the right form (State.validate). HBEP-6's recovery from a
// lowered Lease is an operator's edit of this file, so it is read as
// strictly as a message.
func LoadState(path string) (*State, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return &State{Schema: StateSchema}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("relaykeeper: read the state file: %w", err)
	}
	var s State
	if err := wire.DecodeStrict(b, &s); err != nil {
		return nil, fmt.Errorf("relaykeeper: the state file cannot be parsed, and is not replaced: %w", err)
	}
	var present struct {
		Schema *int   `json:"schema"`
		Starts *int64 `json:"starts"`
	}
	if err := json.Unmarshal(b, &present); err != nil || present.Schema == nil || present.Starts == nil {
		return nil, errors.New("relaykeeper: the state file has no schema or no start count, and is not replaced")
	}
	if err := s.validate(); err != nil {
		return nil, fmt.Errorf("relaykeeper: the state file is not valid, and is not replaced: %w", err)
	}
	return &s, nil
}

// validate checks every field of s, as LoadState requires before anything
// rewrites the file.
func (s *State) validate() error {
	var errs []error
	if s.Schema != StateSchema {
		errs = append(errs, fmt.Errorf("schema %d, want %d", s.Schema, StateSchema))
	}
	if s.Starts < 0 || s.Starts > MaxStarts {
		errs = append(errs, fmt.Errorf("a start count outside 0 to %d", MaxStarts))
	}
	for name, f := range s.Providers {
		if !providerName.MatchString(name) || f.HighestEpoch < 0 || f.HighestEpoch > math.MaxInt32 {
			errs = append(errs, errors.New("a Provider's fence: want a Provider's name and a Lease epoch of 0 to 2^31-1"))
		}
	}
	for _, r := range s.Ends {
		e := r.End
		_, cherr := agentapi.ParseRelayChannel(string(e.Channel))
		okKind := e.Kind == wire.EndFailed || e.Kind == wire.EndSuperseded
		okOutcome := !r.Decided && r.Outcome == "" && r.Reason == "" ||
			r.Decided && (r.Outcome == wire.OutcomeMarked && r.Reason == "" || r.Outcome == wire.OutcomeRefused && r.Reason != "")
		if !providerName.MatchString(r.Provider) || e.UID == "" || e.JobID == "" || cherr != nil || !okKind || e.Seq < 1 || e.Epoch < 0 || !okOutcome {
			errs = append(errs, errors.New("an end: want a Provider, an Allocation's UID and job, a channel, a kind of failed or superseded, a relay's epoch and sequence, and a decision of marked, or refused with a reason"))
		}
	}
	for host, c := range s.Claims {
		if host == "" || !providerName.MatchString(c.Provider) || c.ProviderUID == "" || c.Created.IsZero() {
			errs = append(errs, errors.New("a claim: want a login host, a Provider's name and UID, and its creation time"))
		}
	}
	return errors.Join(errs...)
}

// providerName is a Provider's name, as wire checks it.
var providerName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?$`)

// fsops are the state file's syncs; a test replaces them to fail one, or to
// count them.
var fsops = struct {
	syncFile func(*os.File) error
	syncDir  func(*os.File) error
}{syncFile: (*os.File).Sync, syncDir: (*os.File).Sync}

// SaveState writes s to path, mode 600: a temporary file in the same
// directory, written and synced, renamed over path, and the directory synced,
// so a crash leaves the old file or the new one, never a part of either.
func SaveState(path string, s *State) error {
	_, err := saveState(path, s)
	return err
}

// saveState is SaveState, and reports whether the rename was made, after
// which the file is the new state even when the directory's sync then
// failed: the keeper's memory is then the new state too, never below the
// file (Keeper.commit).
func saveState(path string, s *State) (renamed bool, err error) {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return false, err
	}
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".state-*")
	if err != nil {
		return false, fmt.Errorf("relaykeeper: write the state file: %w", err)
	}
	tmp := f.Name()
	werr := f.Chmod(0o600)
	if werr == nil {
		_, werr = f.Write(append(b, '\n'))
	}
	if werr == nil {
		werr = fsops.syncFile(f)
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr == nil {
		werr = os.Rename(tmp, path)
	}
	if werr != nil {
		_ = os.Remove(tmp)
		return false, fmt.Errorf("relaykeeper: write the state file: %w", werr)
	}
	d, err := os.Open(dir)
	if err != nil {
		return true, fmt.Errorf("relaykeeper: sync the state file's directory: %w", err)
	}
	defer d.Close()
	if err := fsops.syncDir(d); err != nil {
		return true, fmt.Errorf("relaykeeper: sync the state file's directory: %w", err)
	}
	return true, nil
}

// MaxStarts is the most starts a state file may count: E, EpochBase plus the
// count, must stay an int64.
const MaxStarts = math.MaxInt64 - EpochBase

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
	if s.Starts >= MaxStarts {
		// Refused before the file changes: one more start would overflow E.
		return nil, fmt.Errorf("relaykeeper: the state file's start count is %d, the most there can be: E would overflow; the file is not changed", s.Starts)
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
// rolls leaseTransitions back"), run while no keeper runs: it takes the
// keeper's process lock beside the file first, so a keeper that runs (one
// started by hand, say) refuses it, naming the lock, and nothing changes. A
// missing file has nothing to clear.
func ClearLeaseEpochs(path string) error {
	if _, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	lock, err := lockFile(filepath.Join(filepath.Dir(path), "lock"))
	if err != nil {
		return err
	}
	defer lock.Close()
	return ClearLeaseEpochsHeld(path)
}

// LockState takes the keeper's process lock on the launcher's state directory, as a keeper does
// for its life, so no keeper runs while the caller holds it; release gives it back. A caller that
// must check that no keeper runs before it changes anything takes it first (the launcher's
// restore), then clears with ClearLeaseEpochsHeld.
func LockState(stateDir string) (release func(), err error) {
	f, err := lockProcess(stateDir)
	if err != nil {
		return nil, err
	}
	return func() { _ = f.Close() }, nil
}

// ClearLeaseEpochsHeld is ClearLeaseEpochs for a caller that holds the keeper's lock (LockState).
func ClearLeaseEpochsHeld(path string) error {
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
