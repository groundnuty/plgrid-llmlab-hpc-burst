package relaykeeper

// The core's test harness: a fake Runner that records what the keeper asked
// of it, a fake clock, and helpers that build valid messages and send them
// through Handle. What a test checks of the state file, it reads from disk
// with LoadState, not from the keeper's memory.
//
// These are tests of the keeper's decisions alone. The Test plan's keeper
// items run in PR 3, with the keeper, the controllers and the job side as
// processes of their own, since HBEP-6 rules out "a keeper and a controller
// in one process" as proof.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// fakeRunner keeps the Wants it is given and records every call. Its relays
// are as a real Runner's would be, not "every wanted channel up": a relay
// shows up, with a process, only once the test starts it (start), which reads
// the Want's Finals as a real start does; until then it retries, with no
// process; a seeded channel is final; and a Drop forgets it.
type fakeRunner struct {
	epoch int64

	mu      sync.Mutex
	wants   map[string]map[string]Want // Provider → tag → the latest Want
	hosts   map[string]map[string]string
	running map[string]map[string]map[agentapi.RelayChannel]string // Provider → tag → channel → "" up, or a seed's cause
	calls   []string
	// onWant, when set, is called at each Want, before it is recorded.
	onWant func(provider string, w Want)
	// linger, when set, makes a Drop's relays stop in the background, as a
	// real Runner's do: they stay listed, and up if they ran, until reap.
	linger bool
}

func (f *fakeRunner) LoginHost(_ string, s wire.SSH) string { return strings.ToLower(s.HostName) }

func (f *fakeRunner) Want(p string, s wire.SSH, w Want) {
	if f.onWant != nil {
		f.onWant(p, w)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.wants[p] == nil {
		f.wants[p], f.hosts[p] = map[string]Want{}, map[string]string{}
	}
	if old, ok := f.wants[p][w.Entry.Tag]; !ok || old.Entry != w.Entry || f.hosts[p][w.Entry.Tag] != strings.ToLower(s.HostName) {
		delete(f.running[p], w.Entry.Tag) // another identity: replaced, not started yet
	}
	f.wants[p][w.Entry.Tag] = w
	f.hosts[p][w.Entry.Tag] = strings.ToLower(s.HostName)
	f.calls = append(f.calls, "want "+p+" "+w.Entry.Tag)
}

func (f *fakeRunner) Drop(p, tag string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "drop "+p+" "+tag)
	if f.linger {
		return
	}
	delete(f.wants[p], tag)
	delete(f.running[p], tag)
}

// reap ends a lingering Drop: the relays are gone.
func (f *fakeRunner) reap(p, tag string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.wants[p], tag)
	delete(f.running[p], tag)
}

// start starts the relays of the Allocation tagged tag, as a Runner's start
// does: it reads the Want's Finals then, and each channel not seeded final
// runs. It is called with none of the keeper's locks held.
func (f *fakeRunner) start(p, tag string) {
	f.mu.Lock()
	w, ok := f.wants[p][tag]
	f.mu.Unlock()
	if !ok {
		panic("start of a relay that is not wanted: " + tag)
	}
	seeds := w.Finals()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.running[p] == nil {
		f.running[p] = map[string]map[agentapi.RelayChannel]string{}
	}
	f.running[p][tag] = map[agentapi.RelayChannel]string{}
	for _, ch := range agentapi.RelayChannels {
		f.running[p][tag][ch] = seeds[ch]
	}
}

// stop ends the processes of the Allocation tagged tag, as a job's end or a
// lost leader's relay that retries: the relays stay wanted, with none up.
func (f *fakeRunner) stop(p, tag string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.running[p], tag)
}

func (f *fakeRunner) Relays(p string) []wire.RelayState {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []wire.RelayState
	for _, tag := range sortedKeys(f.wants[p]) {
		w := f.wants[p][tag]
		run, started := f.running[p][tag]
		for i, ch := range agentapi.RelayChannels {
			r := wire.RelayState{UID: w.Entry.UID, Tag: tag, JobID: w.Entry.JobID, Channel: ch, State: wire.StateRetrying,
				Epoch: f.epoch, LoginHost: f.hosts[p][tag]}
			switch {
			case started && run[ch] != "":
				r.State, r.LastEnd = wire.StateFinal, run[ch]
			case started:
				r.State, r.PID = wire.StateUp, 10_000+i
			}
			out = append(out, r)
		}
	}
	return out
}

func (f *fakeRunner) want(p, tag string) (Want, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w, ok := f.wants[p][tag]
	return w, ok
}

// log returns the calls so far and forgets them.
func (f *fakeRunner) log() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.calls
	f.calls = nil
	return out
}

// clock is a fake clock that moves only when told.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// rig is one keeper's state directory and clock, across the keeper's
// restarts.
type rig struct {
	t     *testing.T
	path  string
	clock *clock
}

func newRig(t *testing.T) *rig {
	t.Helper()
	return &rig{t: t, path: filepath.Join(t.TempDir(), "relaykeeper", "state.json"), clock: newClock()}
}

// open starts a keeper on the rig's state file, as a keeper process's start
// does, and returns it with its fake Runner.
func (r *rig) open(mods ...func(*Options)) (*Keeper, *fakeRunner) {
	r.t.Helper()
	var fr *fakeRunner
	o := Options{State: r.path, Now: r.clock.Now, Runner: func(e int64) (Runner, error) {
		fr = &fakeRunner{epoch: e, wants: map[string]map[string]Want{}, hosts: map[string]map[string]string{},
			running: map[string]map[string]map[agentapi.RelayChannel]string{}}
		return fr, nil
	}}
	for _, m := range mods {
		m(&o)
	}
	k, err := Open(o)
	if err != nil {
		r.t.Fatal(err)
	}
	return k, fr
}

// disk is the state file as it is on disk.
func (r *rig) disk() *State {
	r.t.Helper()
	s, err := LoadState(r.path)
	if err != nil {
		r.t.Fatal(err)
	}
	return s
}

// The Providers of the tests: example-a is the oldest.
var created = map[string]time.Time{
	"example-a": time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
	"example-b": time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC),
	"example-c": time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC),
}

// set is a valid set of provider at epoch, valid for 40 s from a listing of
// now, on login host host, with entries.
func set(provider string, epoch int64, complete bool, host string, entries ...wire.Entry) wire.DesiredSet {
	return wire.DesiredSet{Provider: provider, ProviderUID: "0a1b2c3d-0000-4000-8000-" + fmt.Sprintf("%012d", len(provider)),
		ProviderCreated: created[provider], LeaseEpoch: epoch, Complete: complete,
		ListedAt: time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC), ListingAgeMS: 0, ValidForMS: 40_000,
		SSH:         wire.SSH{HostName: host, User: "example-user", IdentityFile: "/home/example-user/.ssh/id_example"},
		Allocations: entries}
}

// entry is the n-th test Allocation.
func entry(n int) wire.Entry {
	tag := "hb-alloc-" + strconv.Itoa(n) + "-0123456789ab"
	return wire.Entry{UID: fmt.Sprintf("0a1b2c3d-0000-4000-9000-%012d", n), Tag: tag, JobID: strconv.Itoa(50_000 + n),
		AllocDir: "/stage/alloc/" + tag, AgentPath: "/stage/agent/e/hb-agent", Gen: "1a2b3c4d5e6f7a8b"}
}

// send hands one request to the keeper as the socket would, the body through
// JSON, and returns the reply.
func send(t *testing.T, k *Keeper, kind wire.Kind, body any) (wire.Kind, any) {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	return k.Handle(context.Background(), wire.Envelope{V: wire.Version, Kind: kind, Body: b})
}

// accepted sends s and returns its report, failing on a refusal.
func accepted(t *testing.T, k *Keeper, s wire.DesiredSet) wire.Report {
	t.Helper()
	kind, body := send(t, k, wire.KindSet, s)
	if kind != wire.KindReport {
		t.Fatalf("the set of %s at epoch %d was answered %s %+v, want a report", s.Provider, s.LeaseEpoch, kind, body)
	}
	return body.(wire.Report)
}

// refused sends a request and returns its refusal, failing on anything else.
func refused(t *testing.T, k *Keeper, kind wire.Kind, body any) wire.Refused {
	t.Helper()
	got, reply := send(t, k, kind, body)
	if got != wire.KindRefused {
		t.Fatalf("the %s was answered %s %+v, want a refusal", kind, got, reply)
	}
	return reply.(wire.Refused)
}

// tagsOf lists entries' tags.
func tagsOf(es ...wire.Entry) []string {
	var out []string
	for _, e := range es {
		out = append(out, e.Tag)
	}
	slices.Sort(out)
	return out
}

// mustMode checks a file's mode.
func mustMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != want {
		t.Errorf("%s has mode %v, want %v", filepath.Base(path), fi.Mode().Perm(), want)
	}
}
