package agent_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// The agent registers with its agent instance ID and the job's restart count,
// starts the worker with Apptainer from the launch, sends the Instance's key
// until KeyStored, and passes that key to the engine only through the
// environment.
func TestRunStartsTheWorkerWithTheLaunch(t *testing.T) {
	h := newHarness(t, harnessOptions{restart: 3})
	h.set(h.runState("", 1, 0, nil))
	r := h.waitReport("worker-0 running", 20*time.Second, running)

	regs := h.backend.Registrations(hAllocUID)
	if len(regs) != 1 || regs[0].Caller.AgentInstanceID != h.aid || regs[0].Request.RestartCount != 3 {
		t.Fatalf("registrations: %+v", regs)
	}
	key := r.Instance.Key
	if err := agentapi.CheckKey([]byte(key)); err != nil {
		t.Fatalf("the report's key: %v", err)
	}
	if r.Instance.ServingEndpoint != "unix:"+h.runDir+"/serve.sock" {
		t.Errorf("serving endpoint %q", r.Instance.ServingEndpoint)
	}
	execs := h.execs()
	if len(execs) != 1 {
		t.Fatalf("%d exec invocations, want 1", len(execs))
	}
	argv := execs[0].Argv
	wantTail := launch("", h.engLog).Command
	if !reflect.DeepEqual(argv[len(argv)-len(wantTail):], wantTail) {
		t.Errorf("the exec does not end with the launch's Command:\n got %q\nwant suffix %q", argv, wantTail)
	}
	for _, a := range argv {
		if strings.Contains(a, key) {
			t.Fatalf("the key is on Apptainer's command line")
		}
	}
	// The fake Apptainer records the key variable's value as its SHA-256.
	sum := sha256.Sum256([]byte(key))
	if !slices.Contains(execs[0].Env, "VLLM_API_KEY="+hex.EncodeToString(sum[:])) {
		t.Errorf("the engine's environment does not carry the report's key in VLLM_API_KEY: %q", execs[0].Env)
	}

	// Once the controller has stored the key, reports stop carrying it.
	d := h.runState("", 1, 0, nil)
	d.Instance.KeyStored = true
	h.set(d)
	h.waitReport("no key", 10*time.Second, func(r agentapi.Report) bool { return r.Instance != nil && r.Instance.Key == "" })
}

// A restart runs the Instance's original launch, even when a later desired
// state carries another launch for the same Instance (HBEP-7, Risks: a
// restart runs a different spec; Test plan).
func TestRestartRunsTheOriginalLaunch(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.set(h.runState("", 1, 0, nil))
	first := h.waitReport("worker-0 running", 20*time.Second, running)
	pid := worker0(first).PID

	changed := h.runState("", 1, 1, nil) // the worker's generation rises
	changed.Instance.Launch.Command = append(changed.Instance.Launch.Command, "--enable-prefix-caching=false")
	changed.Instance.Launch.Env = append(changed.Instance.Launch.Env, agentapi.EnvVar{Name: "NEW", Value: "1"})
	h.set(changed)
	h.waitReport("worker-0 restarted at generation 1", 20*time.Second, func(r agentapi.Report) bool {
		w := worker0(r)
		return w != nil && w.Phase == agentapi.PartRunning && w.Generation == 1 && w.PID != pid
	})
	execs := h.execs()
	if len(execs) != 2 {
		t.Fatalf("%d exec invocations, want 2", len(execs))
	}
	if !reflect.DeepEqual(execs[0].Argv, execs[1].Argv) {
		t.Errorf("the restart ran another command line:\nfirst   %q\nrestart %q", execs[0].Argv, execs[1].Argv)
	}
	if !reflect.DeepEqual(execs[0].Env, execs[1].Env) {
		t.Errorf("the restart ran another environment:\nfirst   %q\nrestart %q", execs[0].Env, execs[1].Env)
	}
}

// A part that exits cleanly without being told to is reported exited, with
// exit code 0, and the agent does not restart it by itself: it stays exited
// until the controller raises its generation (HBEP-7, Health and restarts;
// L-2.9; who decides).
func TestCleanExitCountsAsFailedAndIsNotRestartedByTheAgent(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.set(h.runState("exit-after=700ms:0", 1, 0, nil))
	h.waitReport("worker-0 running", 20*time.Second, running)
	r := h.waitReport("worker-0 exited", 20*time.Second, func(r agentapi.Report) bool {
		w := worker0(r)
		return w != nil && w.Phase == agentapi.PartExited
	})
	if w := worker0(r); w.ExitCode == nil || *w.ExitCode != 0 {
		t.Fatalf("exit code %v, want 0", w.ExitCode)
	}
	// Many polls and reports later, still one start.
	time.Sleep(1500 * time.Millisecond)
	if n := len(h.execs()); n != 1 {
		t.Fatalf("the agent restarted the part by itself: %d starts", n)
	}
	if w := worker0(mustLast(t, h)); w.Phase != agentapi.PartExited {
		t.Fatalf("worker-0 is %s, want exited", w.Phase)
	}

	// The restart comes only from a raised generation.
	h.set(h.runState("", 1, 1, nil))
	h.waitReport("worker-0 running at generation 1", 20*time.Second, func(r agentapi.Report) bool {
		w := worker0(r)
		return w != nil && w.Phase == agentapi.PartRunning && w.Generation == 1
	})
	if n := len(h.execs()); n != 2 {
		t.Fatalf("%d starts after the generation rose, want 2", n)
	}
}

// Each restart follows the controller's raised generation and nothing else:
// the agent applies the generation in the desired state it was served, so a
// restart's process never starts before the agent has seen a desired state
// carrying that generation. The controller's half (the event is emitted before
// the generation rises) is the lifecycle controller's (issue 61).
func TestRestartStartsOnlyAfterTheRaisedGenerationIsServed(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.set(h.runState("", 1, 0, nil))
	h.waitReport("worker-0 running", 20*time.Second, running)
	h.set(h.runState("", 1, 1, nil))
	r := h.waitReport("worker-0 running at generation 1", 20*time.Second, func(r agentapi.Report) bool {
		w := worker0(r)
		return w != nil && w.Phase == agentapi.PartRunning && w.Generation == 1
	})
	served, at := h.api.servedStates()
	firstGen1 := -1
	for i, d := range served {
		if d.Instance != nil && d.Instance.Workers[0].Generation == 1 {
			firstGen1 = i
			break
		}
	}
	if firstGen1 < 0 {
		t.Fatal("no desired state with generation 1 was served, yet the worker restarted")
	}
	if start := *worker0(r).StartTime; start.Before(at[firstGen1]) {
		t.Fatalf("the generation-1 process started at %s, before the agent was served generation 1 at %s", start, at[firstGen1])
	}
	if n := len(h.execs()); n != 2 {
		t.Fatalf("%d starts, want 2", n)
	}
}

// A process that ignores TERM is killed after the stop grace, and not before:
// it is reported stopping until then, then stopped with exit code -1, and
// every process of its group is gone. The control engine honours TERM and is
// stopped well inside the grace.
func TestProcessIgnoringTermIsKilledAfterStopGrace(t *testing.T) {
	for _, tc := range []struct {
		name     string
		modes    string
		wantCode int
		killed   bool
	}{
		{"ignores TERM", "ignore-term", -1, true},
		{"honours TERM", "", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, harnessOptions{})
			d := h.runState(tc.modes, 1, 0, nil)
			const grace = 1500 * time.Millisecond
			d.Settings.StopGrace = agentapi.Duration(grace)
			h.set(d)
			r := h.waitReport("worker-0 running", 20*time.Second, running)
			pid := worker0(r).PID
			pids := enginePIDs(t, h)

			idle := agentapi.DesiredState{Action: agentapi.ActionIdle, Settings: d.Settings}
			asked := time.Now()
			h.set(idle)
			if tc.killed {
				h.waitReport("worker-0 stopping", 10*time.Second, func(r agentapi.Report) bool {
					w := worker0(r)
					return w != nil && w.Phase == agentapi.PartStopping
				})
			}
			r = h.waitReport("worker-0 stopped", 10*time.Second, func(r agentapi.Report) bool {
				w := worker0(r)
				return w != nil && w.Phase == agentapi.PartStopped
			})
			took := time.Since(asked)
			w := worker0(r)
			if w.ExitCode == nil || *w.ExitCode != tc.wantCode {
				t.Fatalf("exit code %v, want %d", w.ExitCode, tc.wantCode)
			}
			if tc.killed && took < grace {
				t.Fatalf("killed after %s, before the %s grace", took, grace)
			}
			if !tc.killed && took >= grace {
				t.Fatalf("a TERM-honouring engine took %s, the whole grace", took)
			}
			waitGone(t, append([]int{pid}, pids...), 5*time.Second)
		})
	}
}

// An Action this agent does not know changes nothing: the worker keeps
// running with the same process.
func TestUnknownActionChangesNothing(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.set(h.runState("", 1, 0, nil))
	pid := worker0(h.waitReport("worker-0 running", 20*time.Second, running)).PID
	h.set(agentapi.DesiredState{Action: "drain-later"})
	time.Sleep(1200 * time.Millisecond)
	r := mustLast(t, h)
	if w := worker0(r); w.Phase != agentapi.PartRunning || w.PID != pid {
		t.Fatalf("an unknown action changed the worker: %+v", w)
	}
}

// Told to stop, the agent stops every part, sends a last report and exits.
func TestStopEndsTheAgent(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.set(h.runState("", 1, 0, nil))
	h.waitReport("worker-0 running", 20*time.Second, running)
	h.set(agentapi.DesiredState{Action: agentapi.ActionStop, Settings: agentapi.AgentSettings{StopGrace: agentapi.Duration(time.Second)}})
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatalf("Run returned %v after a stop", err)
		}
		h.done <- nil // for the cleanup's wait
	case <-time.After(20 * time.Second):
		t.Fatal("the agent did not exit after a stop")
	}
	if w := worker0(mustLast(t, h)); w == nil || w.Phase != agentapi.PartStopped {
		t.Fatalf("the last report's worker: %+v", w)
	}
}

// A registration with a wrong credential, or with the right credential and
// another job ID, is refused, and the agent runs nothing: no image is staged
// and no worker starts.
func TestRefusedRegistrationRunsNothing(t *testing.T) {
	for _, tc := range []struct {
		name string
		o    harnessOptions
		code agentapi.Code
	}{
		{"wrong credential", harnessOptions{credential: strings.Repeat("0", 64)}, agentapi.CodeUnauthenticated},
		{"another job ID", harnessOptions{jobID: "9999"}, agentapi.CodeJobMismatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, tc.o)
			h.set(h.runState("", 1, 0, nil))
			time.Sleep(800 * time.Millisecond)
			refusals := h.gate.Refusals()
			if len(refusals) == 0 {
				t.Fatal("no call was refused")
			}
			for _, r := range refusals {
				if r.Code != tc.code || r.Method != fake.MethodRegister {
					t.Fatalf("refusal %+v, want %s on %s", r, tc.code, fake.MethodRegister)
				}
			}
			if len(refusals) < 2 {
				t.Errorf("the agent did not retry the registration: %d attempts", len(refusals))
			}
			if n := h.backend.Calls(fake.MethodRegister); n != 0 {
				t.Errorf("a refused registration reached the Backend %d times", n)
			}
			if cmds := h.appCommands(); len(cmds) != 0 {
				t.Errorf("the agent ran Apptainer while unregistered: %q", cmds)
			}
		})
	}
}

func mustLast(t *testing.T, h *harness) agentapi.Report {
	t.Helper()
	r, ok := h.backend.LastReport(hAllocUID)
	if !ok {
		t.Fatal("no report")
	}
	return r
}

// enginePIDs returns the PIDs the engine double wrote to its log: every
// process of its tree, with ignore-term the engine and its child.
func enginePIDs(t *testing.T, h *harness) []int {
	t.Helper()
	var pids []int
	for _, line := range readLines(t, h.engLog) {
		var rec struct {
			PIDs []int `json:"pids"`
			PID  int   `json:"pid"`
			Argv []string `json:"argv"`
		}
		if json.Unmarshal([]byte(line), &rec) != nil {
			continue
		}
		pids = append(pids, rec.PIDs...)
		if len(rec.Argv) > 0 && rec.PID > 0 {
			pids = append(pids, rec.PID)
		}
	}
	if len(pids) == 0 {
		t.Fatal("the engine double logged no PID")
	}
	return pids
}

// waitGone waits until none of the PIDs is a live process (a zombie counts as
// gone: it runs nothing).
func waitGone(t *testing.T, pids []int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		var left []int
		for _, p := range pids {
			if processLives(p) {
				left = append(left, p)
			}
		}
		if len(left) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("processes %v still run %s after the stop", left, timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func processLives(pid int) bool {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false
	}
	// The state follows the command name in parentheses.
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	return i < 0 || i+2 >= len(s) || s[i+2] != 'Z'
}
