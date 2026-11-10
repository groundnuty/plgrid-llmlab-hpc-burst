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
// are as a real Runner's would be (the delta review of PR 354):
//   - a relay shows up, with a process, only once the test starts it
//     (start), and a start consults the Want's Jobs first, as hostdial does
//     before each start, then reads its Finals; a channel seeded final
//     never runs;
//   - a started instance that a Want of another identity replaces, or that a
//     Drop stops, keeps running, with its own host, UID and (E, N), until the
//     test lands its stop (land): a stop runs in the background;
//   - each start numbers its relays, as hostdial numbers N, and tells
//     OnStart.
type fakeRunner struct {
	epoch int64

	mu    sync.Mutex
	cur   map[string]map[string]*instance   // Provider → tag → the current Want's relays
	old   map[string]map[string][]*instance // Provider → tag → replaced or dropped relays still stopping
	calls []string
	seq   int64
	// onWant, when set, is called at each Want, before it is recorded.
	onWant func(provider string, w Want)
}

// instance is the relays of one Want of an Allocation.
type instance struct {
	w       Want
	host    string
	started bool
	seeds   map[agentapi.RelayChannel]string
	seqs    map[agentapi.RelayChannel]int64
	up      map[agentapi.RelayChannel]bool
}

func newFakeRunner(e int64) *fakeRunner {
	return &fakeRunner{epoch: e, cur: map[string]map[string]*instance{}, old: map[string]map[string][]*instance{}}
}

func (f *fakeRunner) LoginHost(_ string, s wire.SSH) string { return strings.ToLower(s.HostName) }

func (f *fakeRunner) Want(p string, s wire.SSH, w Want) {
	if f.onWant != nil {
		f.onWant(p, w)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cur[p] == nil {
		f.cur[p], f.old[p] = map[string]*instance{}, map[string][]*instance{}
	}
	host := strings.ToLower(s.HostName)
	tag := w.Entry.Tag
	if in, ok := f.cur[p][tag]; ok && in.w.Entry == w.Entry && in.host == host && in.w.ProviderUID == w.ProviderUID {
		in.w = w // the same identity: nothing is replaced
	} else {
		if ok {
			// Kept, as a stopping instance, with any start still in flight.
			f.old[p][tag] = append(f.old[p][tag], in)
		}
		f.cur[p][tag] = &instance{w: w, host: host}
	}
	f.calls = append(f.calls, "want "+p+" "+tag)
}

func (f *fakeRunner) Drop(p, tag string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "drop "+p+" "+tag)
	if in, ok := f.cur[p][tag]; ok {
		f.old[p][tag] = append(f.old[p][tag], in)
		delete(f.cur[p], tag)
	}
}

// start starts the relays of the Allocation tagged tag, as hostdial does:
// it reads the Want's Finals, then, for each channel not seeded final, reads
// the Want's Jobs (the listing, which holds nothing), takes a new N, asks the
// gate (Want.BeforeStart, which takes the hold) and, when it allows,
// launches: the channel runs, told to OnStart, which resolves the hold. It
// reports whether any channel started. It is called with none of the
// keeper's locks held.
func (f *fakeRunner) start(p, tag string) bool {
	in := f.current(p, tag)
	seeds := in.w.Finals()
	f.mu.Lock()
	in.started, in.seeds = true, seeds
	f.mu.Unlock()
	started := false
	for _, ch := range agentapi.RelayChannels {
		if _, final := seeds[ch]; final {
			continue
		}
		if f.lists(in) && f.gate(in, ch) {
			f.launch(in, ch)
			started = true
		}
	}
	return started
}

// current is the Allocation's current instance.
func (f *fakeRunner) current(p, tag string) *instance {
	f.mu.Lock()
	defer f.mu.Unlock()
	in, ok := f.cur[p][tag]
	if !ok {
		panic("start of a relay that is not wanted: " + tag)
	}
	return in
}

// lists reads in's Jobs, as hostdial does before a start.
func (f *fakeRunner) lists(in *instance) bool {
	jobs, err := in.w.Jobs.ListByTag(context.Background(), in.w.Entry.Tag)
	return err == nil && len(jobs) > 0
}

// gate takes in's channel's next N and asks the gate, as hostdial does right
// before a start.
func (f *fakeRunner) gate(in *instance, ch agentapi.RelayChannel) bool {
	f.mu.Lock()
	if in.seqs == nil {
		in.seqs, in.up = map[agentapi.RelayChannel]int64{}, map[agentapi.RelayChannel]bool{}
	}
	f.seq++
	seq := f.seq
	in.seqs[ch] = seq
	f.mu.Unlock()
	return in.w.BeforeStart(ch, f.epoch, seq)
}

// authorize is the first half of a start of one channel with a scripted
// gap: the listing and the gate, which takes the hold. It returns the
// instance, or nil when either refuses. The test then resolves it with
// launch or launchFails.
func (f *fakeRunner) authorize(p, tag string, ch agentapi.RelayChannel) *instance {
	in := f.current(p, tag)
	if !f.lists(in) || !f.gate(in, ch) {
		return nil
	}
	return in
}

// listedOnly is a start that never reaches the gate: the listing passed, and
// the attempt ended before the gate (a relay line or an ssh command that
// failed). It holds nothing.
func (f *fakeRunner) listedOnly(p, tag string) bool {
	return f.lists(f.current(p, tag))
}

// launch starts in's channel, which the gate allowed: its process runs, with
// the N the gate saw, told to OnStart. The instance may have been replaced or
// dropped meanwhile: its process then runs as a stopping one, until land.
func (f *fakeRunner) launch(in *instance, ch agentapi.RelayChannel) {
	f.mu.Lock()
	in.started = true
	seq := in.seqs[ch]
	in.up[ch] = true
	f.mu.Unlock()
	in.w.OnStart(ch, f.epoch, seq)
}

// launchFails resolves in's channel's hold with a process that did not
// start.
func (f *fakeRunner) launchFails(in *instance, ch agentapi.RelayChannel) {
	in.w.StartFailed(ch)
}

// mustStart is start, failing the test when the Want's Jobs refuse it.
func (f *fakeRunner) mustStart(t *testing.T, p, tag string) {
	t.Helper()
	if !f.start(p, tag) {
		t.Fatalf("the relays of %s were refused their start", tag)
	}
}

// stop ends the processes of the Allocation's current relays, as a job's
// end or a lost leader's relay that retries: they stay wanted, with none up.
func (f *fakeRunner) stop(p, tag string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if in, ok := f.cur[p][tag]; ok {
		in.up = map[agentapi.RelayChannel]bool{}
	}
}

// land ends the stops in the background of the Allocation's replaced or
// dropped relays: their processes are gone.
func (f *fakeRunner) land(p, tag string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.old[p], tag)
}

// seqOf is the N the current relays' start gave a channel.
func (f *fakeRunner) seqOf(p, tag string, ch agentapi.RelayChannel) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if in, ok := f.cur[p][tag]; ok {
		return in.seqs[ch]
	}
	return 0
}

func (f *fakeRunner) Relays(p string) []wire.RelayState {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []wire.RelayState
	add := func(tag string, in *instance, current bool) {
		for i, ch := range agentapi.RelayChannels {
			r := wire.RelayState{UID: in.w.Entry.UID, Tag: tag, JobID: in.w.Entry.JobID, Channel: ch, State: wire.StateRetrying,
				Epoch: f.epoch, Seq: in.seqs[ch], LoginHost: in.host, ProviderUID: in.w.ProviderUID}
			switch {
			case in.up[ch]:
				r.State, r.PID = wire.StateUp, 10_000+i
			case current && in.started && in.seeds[ch] != "":
				r.State, r.LastEnd = wire.StateFinal, in.seeds[ch]
			case !current:
				continue // a stopping instance shows only what still runs
			}
			out = append(out, r)
		}
	}
	tags := map[string]bool{}
	for tag := range f.cur[p] {
		tags[tag] = true
	}
	for tag := range f.old[p] {
		tags[tag] = true
	}
	for _, tag := range sortedKeys(tags) {
		for _, in := range f.old[p][tag] {
			add(tag, in, false)
		}
		if in, ok := f.cur[p][tag]; ok {
			add(tag, in, true)
		}
	}
	return out
}

func (f *fakeRunner) want(p, tag string) (Want, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	in, ok := f.cur[p][tag]
	if !ok {
		return Want{}, false
	}
	return in.w, true
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
		fr = newFakeRunner(e)
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
