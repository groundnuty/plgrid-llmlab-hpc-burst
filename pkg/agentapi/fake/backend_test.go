package fake_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

func wantCode(t *testing.T, what string, err error, code agentapi.Code) {
	t.Helper()
	var e *agentapi.Error
	if !errors.As(err, &e) {
		t.Fatalf("%s: err = %v, want an *agentapi.Error with code %s", what, err, code)
	}
	if e.Code != code {
		t.Errorf("%s: code = %s, want %s", what, e.Code, code)
	}
}

func TestBackendAnswersNotRegisteredBeforeRegister(t *testing.T) {
	b := fake.NewBackend()
	_, err := b.Desired(bgndCtx, caller(agent1))
	wantCode(t, "Desired", err, agentapi.CodeNotRegistered)
	err = b.Report(bgndCtx, caller(agent1), agentapi.Report{Time: t0})
	wantCode(t, "Report", err, agentapi.CodeNotRegistered)
	if got := len(b.Reports(allocUID)); got != 0 {
		t.Errorf("a refused Report was recorded: %d reports", got)
	}

	// A agent instance ID that registered does not register another one.
	if _, err := b.Register(bgndCtx, caller(agent1), agentapi.RegisterRequest{Node: "node-a"}); err != nil {
		t.Fatal(err)
	}
	_, err = b.Desired(bgndCtx, caller(agent2))
	wantCode(t, "Desired from another agent instance ID", err, agentapi.CodeNotRegistered)
}

func TestBackendRegisterRecordsTheCallerAndAnswersWithTheProtocolVersion(t *testing.T) {
	b := fake.NewBackend()
	req := agentapi.RegisterRequest{Node: "node-a", CPUArch: "x86_64", GPUs: []agentapi.GPU{{Index: 0, Name: "gpu-a"}}}
	resp, err := b.Register(bgndCtx, caller(agent1), req)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, "ProtocolVersion", resp.ProtocolVersion, "v1")
	eq(t, "Registrations", b.Registrations(allocUID), []fake.Registration{{Caller: caller(agent1), Request: req}})

	// Registering again with the same agent instance ID is accepted and changes nothing.
	again := agentapi.RegisterRequest{Node: "node-b"}
	if _, err := b.Register(bgndCtx, caller(agent1), again); err != nil {
		t.Fatalf("second Register with the same agent instance ID: %v", err)
	}
	eq(t, "Registrations after a repeat", b.Registrations(allocUID), []fake.Registration{{Caller: caller(agent1), Request: req}})

	// Another agent instance ID of the Allocation is another registration, in order.
	if _, err := b.Register(bgndCtx, caller(agent2), again); err != nil {
		t.Fatal(err)
	}
	regs := b.Registrations(allocUID)
	if len(regs) != 2 || regs[0].Caller.AgentInstanceID != agent1 || regs[1].Caller.AgentInstanceID != agent2 {
		t.Errorf("Registrations = %+v, want agent1 then agent2", regs)
	}

	// Registrations are per Allocation.
	other := caller(agent1)
	other.AllocationUID = "alloc-uid-2"
	if got := b.Registrations(other.AllocationUID); len(got) != 0 {
		t.Errorf("another Allocation has registrations: %+v", got)
	}
	_, err = b.Desired(bgndCtx, other)
	wantCode(t, "Desired from another Allocation", err, agentapi.CodeNotRegistered)
}

func TestBackendForgetDropsEveryRegistration(t *testing.T) {
	b := fake.NewBackend()
	for _, boot := range []string{agent1, agent2} {
		if _, err := b.Register(bgndCtx, caller(boot), agentapi.RegisterRequest{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Report(bgndCtx, caller(agent1), agentapi.Report{Time: t0}); err != nil {
		t.Fatal(err)
	}

	b.Forget()

	for _, boot := range []string{agent1, agent2} {
		_, err := b.Desired(bgndCtx, caller(boot))
		wantCode(t, "Desired from "+boot, err, agentapi.CodeNotRegistered)
		err = b.Report(bgndCtx, caller(boot), agentapi.Report{Time: t0})
		wantCode(t, "Report from "+boot, err, agentapi.CodeNotRegistered)
	}
	if got := b.Registrations(allocUID); len(got) != 0 {
		t.Errorf("Registrations after Forget = %+v, want none", got)
	}
	if got := len(b.Reports(allocUID)); got != 1 {
		t.Errorf("Forget dropped recorded reports: %d left, want 1", got)
	}

	// The same agent instance ID registers again.
	if _, err := b.Register(bgndCtx, caller(agent1), agentapi.RegisterRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Desired(bgndCtx, caller(agent1)); err != nil {
		t.Errorf("Desired after registering again: %v", err)
	}
}

func TestBackendFailNextFailsTheNextCallOfThatMethodOnce(t *testing.T) {
	b := fake.NewBackend()
	if _, err := b.Register(bgndCtx, caller(agent1), agentapi.RegisterRequest{}); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("boom")
	b.FailNext(fake.MethodReport, boom)

	// Other methods are not affected.
	if _, err := b.Desired(bgndCtx, caller(agent1)); err != nil {
		t.Errorf("Desired failed after FailNext(Report): %v", err)
	}
	if err := b.Report(bgndCtx, caller(agent1), agentapi.Report{Time: t0}); !errors.Is(err, boom) {
		t.Fatalf("first Report: err = %v, want boom", err)
	}
	if got := len(b.Reports(allocUID)); got != 0 {
		t.Errorf("the failed Report was recorded: %d reports", got)
	}
	if err := b.Report(bgndCtx, caller(agent1), agentapi.Report{Time: t0}); err != nil {
		t.Errorf("second Report: %v, want success", err)
	}
	if got := len(b.Reports(allocUID)); got != 1 {
		t.Errorf("reports = %d, want 1", got)
	}
	eq(t, "Calls(Report), failed ones included", b.Calls(fake.MethodReport), 2)

	// Two queued failures fail the next two calls, in order.
	e1, e2 := errors.New("first"), errors.New("second")
	b.FailNext(fake.MethodRegister, e1)
	b.FailNext(fake.MethodRegister, e2)
	_, err1 := b.Register(bgndCtx, caller(agent2), agentapi.RegisterRequest{})
	_, err2 := b.Register(bgndCtx, caller(agent2), agentapi.RegisterRequest{})
	_, err3 := b.Register(bgndCtx, caller(agent2), agentapi.RegisterRequest{})
	if !errors.Is(err1, e1) || !errors.Is(err2, e2) || err3 != nil {
		t.Errorf("three Registers gave %v, %v, %v; want first, second, nil", err1, err2, err3)
	}
}

func TestBackendFailNextRefusesMisuse(t *testing.T) {
	b := fake.NewBackend()
	for name, f := range map[string]func(){
		"unknown method": func() { b.FailNext("Register ", errors.New("x")) },
		"nil error":      func() { b.FailNext(fake.MethodDesired, nil) },
		"unknown Calls":  func() { b.Calls("Nope") },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: no panic", name)
				}
			}()
			f()
		}()
	}
}

func TestBackendRefusesACallerWithoutAllocationOrAgentInstanceID(t *testing.T) {
	b := fake.NewBackend()
	_, err := b.Register(bgndCtx, caller(""), agentapi.RegisterRequest{})
	wantCode(t, "Register without a agent instance ID", err, agentapi.CodeBadRequest)
	noAlloc := caller(agent1)
	noAlloc.AllocationUID = ""
	_, err = b.Desired(bgndCtx, noAlloc)
	wantCode(t, "Desired without an Allocation UID", err, agentapi.CodeBadRequest)
}

func TestBackendServesIdleWithAVersionWhenNothingIsSet(t *testing.T) {
	b := fake.NewBackend()
	if _, err := b.Register(bgndCtx, caller(agent1), agentapi.RegisterRequest{}); err != nil {
		t.Fatal(err)
	}
	d, err := b.Desired(bgndCtx, caller(agent1))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, "Action", d.Action, agentapi.ActionIdle)
	if d.Instance != nil {
		t.Errorf("Instance = %+v, want nil", d.Instance)
	}
	eq(t, "Version length", len(d.Version), 16)
}

func desiredOf(t *testing.T, b *fake.Backend) agentapi.DesiredState {
	t.Helper()
	d, err := b.Desired(bgndCtx, caller(agent1))
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestBackendVersionsFollowTheState(t *testing.T) {
	b := fake.NewBackend()
	if _, err := b.Register(bgndCtx, caller(agent1), agentapi.RegisterRequest{}); err != nil {
		t.Fatal(err)
	}

	// Each state differs from the base by one field.
	base := func() agentapi.DesiredState { return run(instance()) }
	changes := map[string]func(*agentapi.DesiredState){
		"instance generation": func(d *agentapi.DesiredState) { d.Instance.Generation++ },
		"worker generation":   func(d *agentapi.DesiredState) { d.Instance.Workers[0].Generation++ },
		"key stored":          func(d *agentapi.DesiredState) { d.Instance.KeyStored = true },
		"checks":              func(d *agentapi.DesiredState) { d.Instance.Checks = &agentapi.Checks{VerificationGeneration: 1} },
		"action":              func(d *agentapi.DesiredState) { d.Action = agentapi.ActionStop; d.Instance = nil },
		"settings":            func(d *agentapi.DesiredState) { d.Settings.StopGrace = agentapi.Duration(30 * time.Second) },
	}

	b.SetDesired(allocUID, base())
	baseVersion := desiredOf(t, b).Version
	if baseVersion == "" {
		t.Fatal("the base state has no Version")
	}
	seen := map[string]string{baseVersion: "base"}
	for name, change := range changes {
		d := base()
		change(&d)
		b.SetDesired(allocUID, d)
		v := desiredOf(t, b).Version
		if v == baseVersion {
			t.Errorf("changing the %s left the Version at %s", name, v)
		}
		if prev, dup := seen[v]; dup {
			t.Errorf("changing the %s gave the Version of the %s", name, prev)
		}
		seen[v] = name
	}

	// A state rebuilt from scratch with the base's fields has the base's Version.
	b.SetDesired(allocUID, base())
	eq(t, "Version of an equal state", desiredOf(t, b).Version, baseVersion)

	// The Version is the spec's: 16 hex characters of the SHA-256 of the
	// state's JSON with an empty Version, worked out here without the code
	// under test.
	got := desiredOf(t, b)
	want := got
	want.Version = ""
	raw, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	eq(t, "Version", got.Version, hex.EncodeToString(sum[:])[:16])
	// And the state served is the state set.
	eq(t, "the state served, without its Version", want, base())
}

func TestBackendKeepsDesiredStatePerAllocation(t *testing.T) {
	b := fake.NewBackend()
	b.SetDesired("alloc-uid-2", run(instance()))
	if _, err := b.Register(bgndCtx, caller(agent1), agentapi.RegisterRequest{}); err != nil {
		t.Fatal(err)
	}
	eq(t, "Action of an Allocation nothing was set for", desiredOf(t, b).Action, agentapi.ActionIdle)
}

func richDesired() agentapi.DesiredState {
	tmpl := "template-example"
	i := instance(withChecks(agentapi.Checks{
		VerificationGeneration: 2,
		Verify:                 &agentapi.VerifyRequest{Files: []agentapi.FileDigest{{Path: "a", SHA256: "aa"}}},
		Qualify:                &agentapi.QualifyRequest{ToolCalls: true},
		WarmUp:                 &agentapi.WarmUpRequest{TargetSessions: 2},
	}))
	i.Launch.Env = []agentapi.EnvVar{{Name: "K", Value: "V"}}
	i.Launch.ChatTemplate = &tmpl
	return run(i)
}

func scrambleDesired(d *agentapi.DesiredState) {
	i := d.Instance
	i.Launch.Command[0] = "x"
	i.Launch.Env[0].Value = "x"
	*i.Launch.ChatTemplate = "x"
	i.Workers[0].Name = "x"
	i.Checks.Verify.Files[0].Path = "x"
	i.Checks.Qualify.ToolCalls = false
	i.Checks.WarmUp.TargetSessions = 99
	i.Generation = 99
}

func TestBackendOwnsTheDesiredStateItStoresAndServes(t *testing.T) {
	b := fake.NewBackend()
	if _, err := b.Register(bgndCtx, caller(agent1), agentapi.RegisterRequest{}); err != nil {
		t.Fatal(err)
	}
	set := richDesired()
	b.SetDesired(allocUID, set)
	scrambleDesired(&set) // the caller changes its value after SetDesired
	got := desiredOf(t, b)
	wantState := richDesired()
	wantState.Version = got.Version
	eq(t, "served after the caller changed its value", got, wantState)

	scrambleDesired(&got) // the agent changes what it was served
	again := desiredOf(t, b)
	eq(t, "served after the agent changed its copy", again.Instance, wantState.Instance)
}

func richReport() agentapi.Report {
	start := t0.Add(time.Second)
	read := t0.Add(2 * time.Second)
	return agentapi.Report{Time: t0, Instance: &agentapi.InstanceReport{
		UID:     "inst-uid-1",
		Staging: &agentapi.Staging{Phase: agentapi.StagingDone},
		Workers: []agentapi.WorkerReport{{
			Name: "worker-0", Phase: agentapi.PartExited, StartTime: &start, ExitCode: intp(3),
			Health: &agentapi.Health{
				ProbeOK: true, MetricsReadAt: &read, RunningRequests: i64p(4), WaitingRequests: i64p(1),
				GeneratedTokens: i64p(70), KVCacheUsage: f64p(0.25), ResponsesByStatus: map[string]int64{"200": 5},
			},
		}},
		Checks: &agentapi.CheckResults{
			VerificationGeneration: 1,
			Verify: &agentapi.VerifyResult{
				Files:    []agentapi.FileResult{{Path: "a", Outcome: agentapi.FileMatch}},
				Settings: map[string]string{"max-model-len": "8"},
			},
			Qualify: &agentapi.QualifyResult{
				Checks:           []agentapi.CheckOutcome{{Name: agentapi.CheckChat, Passed: true}},
				ServedModelNames: []string{"m"},
			},
			WarmUp: &agentapi.WarmUpResult{Rounds: []agentapi.WarmUpRound{{
				Round: 1, Requests: []agentapi.RequestTiming{{Shape: agentapi.ShapeShort, OutputTokens: 4}},
			}}},
		},
	}}
}

func scrambleReport(r *agentapi.Report) {
	i := r.Instance
	i.UID = "x"
	i.Staging.Phase = agentapi.StagingFailed
	w := &i.Workers[0]
	w.Name = "x"
	*w.StartTime = t0
	*w.ExitCode = 99
	w.Health.ProbeOK = false
	w.Health.ResponsesByStatus["200"] = 99
	*w.Health.MetricsReadAt = t0
	*w.Health.RunningRequests = 99
	*w.Health.WaitingRequests = 99
	*w.Health.GeneratedTokens = 99
	*w.Health.KVCacheUsage = 0.99
	i.Checks.Verify.Files[0].Path = "x"
	i.Checks.Verify.Settings["max-model-len"] = "x"
	i.Checks.Qualify.Checks[0].Passed = false
	i.Checks.Qualify.ServedModelNames[0] = "x"
	i.Checks.WarmUp.Rounds[0].Requests[0].OutputTokens = 99
	i.Checks.WarmUp.Rounds[0].Round = 99
}

func TestBackendOwnsTheReportsItRecords(t *testing.T) {
	b := fake.NewBackend()
	if _, err := b.Register(bgndCtx, caller(agent1), agentapi.RegisterRequest{}); err != nil {
		t.Fatal(err)
	}
	sent := richReport()
	if err := b.Report(bgndCtx, caller(agent1), sent); err != nil {
		t.Fatal(err)
	}
	scrambleReport(&sent) // the agent changes its value after Report
	eq(t, "recorded after the agent changed its value", b.Reports(allocUID), []agentapi.Report{richReport()})

	got := b.Reports(allocUID)
	scrambleReport(&got[0]) // the test changes what it was handed
	eq(t, "recorded after the test changed its copy", b.Reports(allocUID), []agentapi.Report{richReport()})
	last, _ := b.LastReport(allocUID)
	scrambleReport(&last)
	eq(t, "LastReport after the test changed its copy", func() agentapi.Report { r, _ := b.LastReport(allocUID); return r }(), richReport())
}

func TestBackendReportsInOrderPerAllocation(t *testing.T) {
	b := fake.NewBackend()
	if _, ok := b.LastReport(allocUID); ok {
		t.Error("LastReport on a fresh Backend reports one")
	}
	if _, err := b.Register(bgndCtx, caller(agent1), agentapi.RegisterRequest{}); err != nil {
		t.Fatal(err)
	}
	for n := range 3 {
		if err := b.Report(bgndCtx, caller(agent1), agentapi.Report{Time: t0.Add(time.Duration(n) * time.Second)}); err != nil {
			t.Fatal(err)
		}
	}
	reports := b.Reports(allocUID)
	if len(reports) != 3 || !reports[0].Time.Equal(t0) || !reports[2].Time.Equal(t0.Add(2*time.Second)) {
		t.Errorf("Reports = %+v, want three, oldest first", reports)
	}
	last, ok := b.LastReport(allocUID)
	if !ok || !last.Time.Equal(t0.Add(2*time.Second)) {
		t.Errorf("LastReport = %+v, %v", last, ok)
	}
	if got := b.Reports("alloc-uid-2"); len(got) != 0 {
		t.Errorf("another Allocation has reports: %+v", got)
	}
}

// The Backend is safe for concurrent use. Run under -race to make this check
// something.
func TestBackendIsSafeForConcurrentUse(t *testing.T) {
	b := fake.NewBackend()
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := caller(fmt.Sprintf("%032x", g+1))
			for range 50 {
				_, _ = b.Register(bgndCtx, c, agentapi.RegisterRequest{Node: "n"})
				_, _ = b.Desired(bgndCtx, c)
				_ = b.Report(bgndCtx, c, richReport())
				b.SetDesired(allocUID, richDesired())
				b.FailNext(fake.MethodReport, errors.New("x"))
				_ = b.Reports(allocUID)
				_ = b.Registrations(allocUID)
				_ = b.Calls(fake.MethodDesired)
				if g == 0 {
					b.Forget()
				}
			}
		}()
	}
	wg.Wait()
}
