package lifecycle

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// registration is one agent process's registration: soft state, kept in
// memory, keyed by Allocation UID and agent instance ID (agentapi's doc,
// How reports reach the controller).
type registration struct {
	agentInstanceID string
	restartCount    int32
	jobID           string
	node            string
	cpuArch         string
	gpus            []agentapi.GPU
	at              time.Time
	// seq is the Seq of the last report kept from this agent instance, 0 while
	// none was numbered: a numbered report is kept only if it is newer
	// (agentapi.KeepsReport). It lives and dies with the registration, in memory.
	seq uint64
}

// agentState is what the Backend holds of one Allocation's agent.
type agentState struct {
	namespace, name string
	// md is the ModelDeployment the Allocation was assigned to when last seen.
	md         string
	regs       map[string]*registration
	report     *agentapi.Report
	reportAt   time.Time
	reportFrom string
	health     map[string]*tracker
}

// tracker follows one engine process's health over successive samples, for
// the signals that need more than one sample (HBEP-7, Health and restarts).
type tracker struct {
	// tokens is the last known generated-token count, nil while unknown, and
	// tokensSince when it last moved (or the engine last ran no request).
	tokens      *int64
	tokensSince time.Time
	// lastRead is the MetricsReadAt of the last sample taken in.
	lastRead *time.Time
	// skewSince is when this process's KV skew began, zero while it is not skewed, and skewSeen the
	// receipt time of the last report it was judged on, so each report is judged once (kvskew.go).
	skewSince, skewSeen time.Time
	// samples hold only reads with a known ResponsesByStatus.
	samples []healthSample
}

type healthSample struct {
	at        time.Time
	responses int64
	errors    int64
}

// maxSamples bounds a tracker's history; at one sample per report interval it
// covers far more than any errorOnlyWindow a profile sets.
const maxSamples = 512

// Backend is the lifecycle controller's side of the agent API
// (agentapi.Backend). The agent API server calls it after authenticating each
// call. It keeps registrations and the latest report per Allocation in memory,
// enqueues the Allocation's ModelDeployment on every call, and computes the
// desired state from API objects alone (Desired).
type Backend struct {
	// Reader reads the Allocations, Instances and ModelDeployments; the
	// manager's cached client in production.
	Reader client.Reader
	Clock  Clock
	// CredentialsDir holds the Instance key files (agentapi.InstanceKeyPath).
	CredentialsDir string
	// Enqueue asks for a pass over a ModelDeployment.
	Enqueue func(types.NamespacedName)
	// Ready reports whether the controller serves agents now: its caches are
	// synced and it leads. While it is false every call is CodeUnavailable.
	Ready func() bool

	mu     sync.Mutex
	agents map[string]*agentState
}

var _ agentapi.Backend = (*Backend)(nil)

func (b *Backend) state(c agentapi.Caller) *agentState {
	if b.agents == nil {
		b.agents = map[string]*agentState{}
	}
	s, ok := b.agents[c.AllocationUID]
	if !ok {
		s = &agentState{namespace: c.Namespace, name: c.Allocation, regs: map[string]*registration{}, health: map[string]*tracker{}}
		b.agents[c.AllocationUID] = s
	}
	return s
}

func (b *Backend) serving() error {
	if b.Ready != nil && !b.Ready() {
		return agentapi.Errorf(agentapi.CodeUnavailable, "the lifecycle controller is not ready")
	}
	return nil
}

// Register records the caller's registration. Registering again with the same
// agent instance ID changes nothing.
func (b *Backend) Register(ctx context.Context, c agentapi.Caller, r agentapi.RegisterRequest) (agentapi.RegisterResponse, error) {
	if err := b.serving(); err != nil {
		return agentapi.RegisterResponse{}, err
	}
	if c.AllocationUID == "" {
		return agentapi.RegisterResponse{}, agentapi.Errorf(agentapi.CodeBadRequest, "a registration needs the Allocation")
	}
	if err := agentapi.CheckAgentInstanceID(c.AgentInstanceID); err != nil {
		return agentapi.RegisterResponse{}, agentapi.Errorf(agentapi.CodeBadRequest, "agent instance ID: %v", err)
	}
	b.mu.Lock()
	s := b.state(c)
	if _, ok := s.regs[c.AgentInstanceID]; !ok {
		s.regs[c.AgentInstanceID] = &registration{
			agentInstanceID: c.AgentInstanceID, restartCount: r.RestartCount, jobID: c.JobID,
			node: r.Node, cpuArch: r.CPUArch, gpus: append([]agentapi.GPU(nil), r.GPUs...), at: b.Clock.Now(),
		}
	}
	b.mu.Unlock()
	b.enqueueFor(ctx, c)
	return agentapi.RegisterResponse{ProtocolVersion: agentapi.ProtocolVersion}, nil
}

func (b *Backend) registered(c agentapi.Caller) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.agents[c.AllocationUID]
	if !ok {
		return false
	}
	_, ok = s.regs[c.AgentInstanceID]
	return ok
}

// Report takes the caller's latest full snapshot. The Instance's key, sent
// until the controller says it is stored, is written to its mode-600 file.
//
// For a registered agent (CodeNotRegistered is answered first) the Backend keeps
// a report only from the agent instance currently registered for the
// Allocation, and only if its Seq is newer than the last kept from that
// instance (admit; HBEP-7, Report order). Any other report is answered as
// accepted. One dropped by the first decision, before the key's write, has no
// effect at all: no key written, no sample taken, no receipt time refreshed, no
// pass enqueued. One that loses to a newer report, or to another instance's
// registration, while its key is being written is dropped at the store, and its
// key may already be on disk: it is the Instance's key, the same in every
// report, so that is harmless. The count held for the instance rises only when
// a report is kept, so one that was refused is kept when the agent sends it
// again.
func (b *Backend) Report(ctx context.Context, c agentapi.Caller, r agentapi.Report) error {
	if err := b.serving(); err != nil {
		return err
	}
	// Early, so that a report that is not kept writes no key. It is decided
	// again where the report is stored: the key's write, a file write, runs
	// outside the lock, and another report of the agent can be kept meanwhile.
	b.mu.Lock()
	v := b.admit(c, r.Seq)
	b.mu.Unlock()
	if err := v.refusal(c); err != nil || v != admitKeep {
		return err
	}
	// Before anything is stored: a refused report has no effect, and the
	// earlier one stays the latest.
	if r.Instance != nil {
		if err := checkWorkerNames(r.Instance.Workers); err != nil {
			return err
		}
	}
	if r.Instance != nil && r.Instance.Key != "" {
		if err := b.storeKey(ctx, c, r.Instance.UID, r.Instance.Key); err != nil {
			return err
		}
		r.Instance.Key = "" // never kept beyond its file
	}
	// The decision and the store are one step under the lock.
	b.mu.Lock()
	if v := b.admit(c, r.Seq); v != admitKeep {
		b.mu.Unlock()
		return v.refusal(c)
	}
	// The receipt time is read here, with the report kept: read before the key's
	// write, it could be older than the time of a report kept meanwhile.
	now := b.Clock.Now()
	s := b.state(c)
	if r.Seq != 0 {
		s.regs[c.AgentInstanceID].seq = r.Seq
	}
	rc := r
	s.report, s.reportAt, s.reportFrom = &rc, now, c.AgentInstanceID
	if r.Instance != nil {
		for _, w := range r.Instance.Workers {
			if w.Health != nil {
				s.track(r.Instance.UID, w, now)
			}
		}
	}
	b.mu.Unlock()
	b.enqueueFor(ctx, c)
	return nil
}

// admission is what Report does with a report.
type admission int

const (
	admitKeep          admission = iota // keep it
	admitDrop                           // answer it as accepted and drop it
	admitNotRegistered                  // the agent instance is not registered
)

// refusal is the error Report answers with: CodeNotRegistered for an agent
// instance that is not registered, nil for a report kept or dropped.
func (v admission) refusal(c agentapi.Caller) error {
	if v == admitNotRegistered {
		return agentapi.Errorf(agentapi.CodeNotRegistered, "agent instance %s is not registered", c.AgentInstanceID)
	}
	return nil
}

// admit decides what Report does with a report from c counted seq. The caller
// holds b.mu. An agent instance that is not registered is not registered, first.
// Another instance than the one currently registered for the Allocation, the
// newest registration (regLess), is dropped: a first execution's late report
// must neither replace a second one's nor be answered not registered, which
// would make that agent register again and look like a third execution. From
// the current instance a report is kept if agentapi.KeepsReport says so.
func (b *Backend) admit(c agentapi.Caller, seq uint64) admission {
	s, ok := b.agents[c.AllocationUID]
	if !ok {
		return admitNotRegistered
	}
	reg, ok := s.regs[c.AgentInstanceID]
	if !ok {
		return admitNotRegistered
	}
	for _, other := range s.regs {
		if regLess(*reg, *other) {
			return admitDrop
		}
	}
	if !agentapi.KeepsReport(seq, reg.seq) {
		return admitDrop
	}
	return admitKeep
}

// checkWorkerNames refuses a report in which two workers share a name. A
// worker is named WorkerName(i) for its index (agentapi.DesiredWorker), so a
// repeated name is a malformed report; the metrics key a worker's series by
// its name, and two with one name would make duplicate series and fail the
// whole scrape.
func checkWorkerNames(ws []agentapi.WorkerReport) error {
	seen := make(map[string]bool, len(ws))
	for _, w := range ws {
		if seen[w.Name] {
			return agentapi.Errorf(agentapi.CodeBadRequest, "the report lists the worker %q twice; worker names are unique", w.Name)
		}
		seen[w.Name] = true
	}
	return nil
}

func trackerKey(instUID string, w agentapi.WorkerReport) string {
	st := ""
	if w.StartTime != nil {
		st = w.StartTime.UTC().Format(time.RFC3339Nano)
	}
	return fmt.Sprintf("%s/%s/%d/%s", instUID, w.Name, w.PID, st)
}

// track adds a health sample to the process's tracker. The caller holds b.mu.
func (s *agentState) track(instUID string, w agentapi.WorkerReport, now time.Time) {
	h := w.Health
	// Counters are compared only when the engine's metrics were read again:
	// a sample whose MetricsReadAt did not advance repeats the last values
	// (agentapi.Health).
	if h.MetricsReadAt == nil {
		return
	}
	k := trackerKey(instUID, w)
	t, ok := s.health[k]
	if ok && t.lastRead != nil && !h.MetricsReadAt.After(*t.lastRead) {
		return
	}
	if !ok {
		t = &tracker{tokensSince: now}
		s.health[k] = t
	}
	read := *h.MetricsReadAt
	t.lastRead = &read
	// An unknown counter is never compared; the first known one is the
	// baseline, and an engine that runs no request is never stalled.
	if g := h.GeneratedTokens; g != nil && (t.tokens == nil || *g != *t.tokens || (h.RunningRequests != nil && *h.RunningRequests == 0)) {
		v := *g
		t.tokens, t.tokensSince = &v, now
	}
	// A nil ResponsesByStatus is unknown: it is no sample of the answers, and
	// never a count of zero.
	if h.ResponsesByStatus == nil {
		return
	}
	var total int64
	for _, n := range h.ResponsesByStatus {
		total += n
	}
	// vLLM v0.32.0 labels statuses by class ("5xx"): server errors are read
	// only through agentapi.ServerErrors.
	errs := agentapi.ServerErrors(h.ResponsesByStatus)
	t.samples = append(t.samples, healthSample{at: now, responses: total, errors: errs})
	if len(t.samples) > maxSamples {
		t.samples = t.samples[len(t.samples)-maxSamples:]
	}
}

// skewClock advances worker w's KV-skew clock by the report received at reportAt, once per report, and
// returns since when the skew has held: zero while it does not, or when the process has no tracker (no
// metrics read yet), which never starts a clock (kvskew.go). The skew holds while the report's gauges
// show it (skewed) and the worker is not draining: its generated-token counter known and unchanged since
// the clock started (HBEP-7: a loaded worker still generates tokens; the science agent's correction on
// #518). Gauges that don't show it stop the clock; a counter that moved starts it again at this report.
func (b *Backend) skewClock(allocUID types.UID, instUID string, w agentapi.WorkerReport, reportAt time.Time, skewed bool) time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.agents[string(allocUID)]
	if !ok {
		return time.Time{}
	}
	t, ok := s.health[trackerKey(instUID, w)]
	if !ok {
		return time.Time{}
	}
	if !reportAt.Equal(t.skewSeen) {
		t.skewSeen = reportAt
		switch {
		case !skewed || t.tokens == nil:
			t.skewSince = time.Time{}
		case t.skewSince.IsZero() || t.tokensSince.After(t.skewSince):
			t.skewSince = reportAt
		}
	}
	return t.skewSince
}

// storeKey writes an Instance's key to its mode-600 file, once. The Instance
// must run on the caller's Allocation.
func (b *Backend) storeKey(ctx context.Context, c agentapi.Caller, instUID, key string) error {
	if err := agentapi.CheckKey([]byte(key)); err != nil {
		return agentapi.Errorf(agentapi.CodeBadRequest, "the Instance key: %v", err)
	}
	inst, err := b.instanceByUID(ctx, c, instUID)
	if err != nil {
		return err
	}
	if inst == nil {
		return agentapi.Errorf(agentapi.CodeBadRequest, "no Instance %s runs on Allocation %s", instUID, c.Allocation)
	}
	path := agentapi.InstanceKeyPath(b.CredentialsDir, instUID)
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	if err := agentapi.WriteKeyFile(path, key); err != nil {
		return fmt.Errorf("store the key of Instance %s: %w", inst.Name, err)
	}
	return nil
}

func (b *Backend) instanceByUID(ctx context.Context, c agentapi.Caller, uid string) (*v1alpha1.Instance, error) {
	var list v1alpha1.InstanceList
	if err := b.Reader.List(ctx, &list, client.InNamespace(c.Namespace)); err != nil {
		return nil, fmt.Errorf("list Instances: %w", err)
	}
	for i := range list.Items {
		in := &list.Items[i]
		if string(in.UID) == uid && in.Spec.AllocationRef.Name == c.Allocation {
			return in, nil
		}
	}
	return nil, nil
}

// enqueueFor asks for a pass over the caller's Allocation's ModelDeployment.
func (b *Backend) enqueueFor(ctx context.Context, c agentapi.Caller) {
	if b.Enqueue == nil {
		return
	}
	var a v1alpha1.Allocation
	if err := b.Reader.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: c.Allocation}, &a); err != nil {
		return
	}
	if a.Spec.ModelDeploymentRef != nil {
		b.mu.Lock()
		if s, ok := b.agents[c.AllocationUID]; ok {
			s.md = a.Spec.ModelDeploymentRef.Name
		}
		b.mu.Unlock()
		b.Enqueue(types.NamespacedName{Namespace: a.Namespace, Name: a.Spec.ModelDeploymentRef.Name})
	}
}

// prune drops what the Backend holds of the ModelDeployment's Allocations that
// no longer exist; present names those that do.
func (b *Backend) prune(namespace, md string, present map[string]bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for uid, s := range b.agents {
		if s.namespace == namespace && s.md == md && !present[s.name] {
			delete(b.agents, uid)
		}
	}
}

// registrations returns the registrations of an Allocation, oldest first.
func (b *Backend) registrations(allocUID types.UID) []registration {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.agents[string(allocUID)]
	if !ok {
		return nil
	}
	out := make([]registration, 0, len(s.regs))
	for _, r := range s.regs {
		out = append(out, *r)
	}
	sortRegistrations(out)
	return out
}

func sortRegistrations(rs []registration) {
	for i := 1; i < len(rs); i++ {
		for j := i; j > 0 && regLess(rs[j], rs[j-1]); j-- {
			rs[j], rs[j-1] = rs[j-1], rs[j]
		}
	}
}

func regLess(a, b registration) bool {
	if a.restartCount != b.restartCount {
		return a.restartCount < b.restartCount
	}
	if !a.at.Equal(b.at) {
		return a.at.Before(b.at)
	}
	return a.agentInstanceID < b.agentInstanceID
}

// latest returns an Allocation's latest report, when it came and from which
// agent instance ID.
func (b *Backend) latest(allocUID types.UID) (*agentapi.Report, time.Time, string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.agents[string(allocUID)]
	if !ok || s.report == nil {
		return nil, time.Time{}, ""
	}
	r := *s.report
	return &r, s.reportAt, s.reportFrom
}

// healthOf returns a copy of a process's tracker.
func (b *Backend) healthOf(allocUID types.UID, instUID string, w agentapi.WorkerReport) (tracker, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.agents[string(allocUID)]
	if !ok {
		return tracker{}, false
	}
	t, ok := s.health[trackerKey(instUID, w)]
	if !ok {
		return tracker{}, false
	}
	c := *t
	c.samples = append([]healthSample(nil), t.samples...)
	return c, true
}

// errNotFoundIsNil turns a NotFound into (false, nil).
func errNotFoundIsNil(err error) (bool, error) {
	if err == nil {
		return true, nil
	}
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	return false, err
}
