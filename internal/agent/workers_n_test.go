package agent_test

// The agent runs an Instance of several workers (HBEP-7, Several workers): each worker on its own
// socket, with its own slice of the job's devices, whatever the launch sets. The real agent, the fake
// Apptainer that records each exec, and the engine double. A run where every worker sees every device
// proves nothing about the slices (HBEP-7, Test plan), so the test names each worker's one device, and
// that the two differ.

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// workersState is runState with n workers, the launch's own CUDA_VISIBLE_DEVICES set to launchCVD
// when it is not empty, and the checks on each worker, none on the Instance.
func (h *harness) workersState(n int, launchCVD string) agentapi.DesiredState {
	d := h.runState("", 0, 0, nil)
	d.Instance.Checks = nil
	if launchCVD != "" {
		d.Instance.Launch.Env = append(d.Instance.Launch.Env, agentapi.EnvVar{Name: "CUDA_VISIBLE_DEVICES", Value: launchCVD})
	}
	d.Instance.Workers = nil
	for i := 0; i < n; i++ {
		// With several workers the checks are per worker (Q6); the first one's carries the file list
		// staging needs, as the Instance's own does with one worker.
		d.Instance.Workers = append(d.Instance.Workers, agentapi.DesiredWorker{
			Name: agentapi.WorkerName(i), EngineSocket: agentapi.ContainerEngineSocket(agentapi.WorkerName(i)),
			Checks: verifyChecks(1, nil),
		})
	}
	return d
}

// containerCVD is the CUDA_VISIBLE_DEVICES the container of an exec sees, every one of them, and its
// --uds argument. The fake Apptainer records the container's environment as --cleanenv makes it, from the
// APPTAINERENV_ variables with the prefix stripped: what the engine sees.
func containerCVD(inv appInvocation) (cvd []string, uds string) {
	for _, kv := range inv.Env {
		if v, ok := strings.CutPrefix(kv, "CUDA_VISIBLE_DEVICES="); ok {
			cvd = append(cvd, v)
		}
	}
	for _, a := range inv.Argv {
		if v, ok := strings.CutPrefix(a, "--uds="); ok {
			uds = v
		}
	}
	return cvd, uds
}

// Each worker sees exactly its own device, read from the container's own environment, in the
// controller's own shape, a launch that sets no CUDA_VISIBLE_DEVICES, where the job's whole list must
// not be forwarded after the slice (#495's review), and with a launch that sets one, which the slice
// replaces.
func TestEachWorkerRunsOnItsOwnDeviceAndSocket(t *testing.T) {
	for _, c := range []struct{ name, launchCVD string }{
		{"the launch sets none", ""},
		{"the launch sets 7", "7"},
	} {
		t.Run(c.name, func(t *testing.T) {
			launchCVD := c.launchCVD
			h := newHarness(t, harnessOptions{baseEnv: []string{"CUDA_VISIBLE_DEVICES=0,1"}})
			h.set(h.workersState(2, launchCVD))
			r := h.waitReport("both workers running", 60*time.Second, func(r agentapi.Report) bool {
				if r.Instance == nil || len(r.Instance.Workers) != 2 {
					return false
				}
				for _, w := range r.Instance.Workers {
					if w.Phase != agentapi.PartRunning {
						return false
					}
				}
				return true
			})
			got := map[string][]string{}
			socks := map[string]bool{}
			for _, inv := range h.execs() {
				cvd, uds := containerCVD(inv)
				got[uds] = cvd
				socks[uds] = true
			}
			want := map[string][]string{
				agentapi.ContainerEngineSocket("worker-0"): {"0"},
				agentapi.ContainerEngineSocket("worker-1"): {"1"},
			}
			for sock, cvd := range want {
				if !slices.Equal(got[sock], cvd) {
					t.Errorf("the worker on %s sees CUDA_VISIBLE_DEVICES %q, want exactly %q", sock, got[sock], cvd)
				}
			}
			if len(socks) != 2 {
				t.Errorf("the workers ran on sockets %v, want two of their own", socks)
			}
			for _, w := range r.Instance.Workers {
				if !strings.HasSuffix(w.EngineSocket, "/"+w.Name+".sock") {
					t.Errorf("%s reports socket %s, not its own", w.Name, w.EngineSocket)
				}
			}
		})
	}
}

// A job with fewer devices than the workers need is refused, never shared: the workers do not start,
// and each one's log tail says why.
func TestTooFewDevicesForTheWorkersIsRefused(t *testing.T) {
	h := newHarness(t, harnessOptions{baseEnv: []string{"CUDA_VISIBLE_DEVICES=0"}})
	h.set(h.workersState(2, ""))
	r := h.waitReport("both workers refused", 60*time.Second, func(r agentapi.Report) bool {
		if r.Instance == nil || len(r.Instance.Workers) != 2 {
			return false
		}
		for _, w := range r.Instance.Workers {
			if w.Phase != agentapi.PartExited {
				return false
			}
		}
		return true
	})
	for _, w := range r.Instance.Workers {
		if !strings.Contains(w.LogTail, "never share a device") || !strings.Contains(w.LogTail, "need 2") {
			t.Errorf("%s's log tail %q, want the refusal naming the devices needed", w.Name, w.LogTail)
		}
	}
	if n := len(h.execs()); n != 0 {
		t.Errorf("%d workers were executed, want none", n)
	}
}

// Each worker's checks, asked in its DesiredWorker.Checks, run against the engine double on its own
// socket, and their results are in that worker's own report; the Instance's own checks are absent, as
// the desired state asks none (Q6).
func TestEachWorkersCheckResultsAreInItsReport(t *testing.T) {
	h := newHarness(t, harnessOptions{baseEnv: []string{"CUDA_VISIBLE_DEVICES=0,1"}})
	h.set(h.workersState(2, ""))
	r := h.waitReport("each worker's verification in its own report", 60*time.Second, func(r agentapi.Report) bool {
		if r.Instance == nil || len(r.Instance.Workers) != 2 {
			return false
		}
		for _, w := range r.Instance.Workers {
			if w.Checks == nil || w.Checks.Verify == nil || !w.Checks.Verify.Done {
				return false
			}
		}
		return true
	})
	for _, w := range r.Instance.Workers {
		if w.Checks.VerificationGeneration != 1 || w.Checks.Verify.Error != "" || w.Checks.Verify.Fingerprint == "" {
			t.Errorf("%s's verification: %+v, want generation 1, a fingerprint and no error", w.Name, w.Checks.Verify)
		}
	}
	if r.Instance.Checks != nil {
		t.Errorf("the Instance's own checks are %+v, want none: only the workers' were asked", r.Instance.Checks)
	}
}
