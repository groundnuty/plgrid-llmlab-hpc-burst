package agent

// Each worker of an Instance of several workers runs the Instance's launch on its own socket and its own
// slice of the job's devices (HBEP-7, Several workers; the science agent's ruling on #439, Q1). Expected
// values are written out. A test where every worker sees every device proves nothing about the slices
// (HBEP-7, Test plan), so each check here names the one device each worker gets, and that they differ.

import (
	"slices"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

func TestWorkerDevicesSliceTheJobsDevicesInOrder(t *testing.T) {
	for _, c := range []struct {
		name    string
		env     string
		n, k    int
		want    [][]string
		refused string
	}{
		{"two workers of one GPU", "CUDA_VISIBLE_DEVICES=0,1", 2, 1, [][]string{{"0"}, {"1"}}, ""},
		{"two workers of two GPUs", "CUDA_VISIBLE_DEVICES=4,5,6,7", 2, 2, [][]string{{"4", "5"}, {"6", "7"}}, ""},
		{"UUIDs kept as Slurm gives them", "CUDA_VISIBLE_DEVICES=GPU-aaaa,GPU-bbbb", 2, 1, [][]string{{"GPU-aaaa"}, {"GPU-bbbb"}}, ""},
		{"more devices than needed: the first n*k", "CUDA_VISIBLE_DEVICES=0,1,2", 2, 1, [][]string{{"0"}, {"1"}}, ""},
		{"too few devices", "CUDA_VISIBLE_DEVICES=0", 2, 1, nil, "names 1 devices (\"0\"), and 2 workers of 1 GPUs need 2"},
		{"no devices named", "PATH=/usr/bin", 2, 1, nil, "names 0 devices"},
	} {
		t.Run(c.name, func(t *testing.T) {
			for i := 0; i < c.n; i++ {
				got, err := workerDevices([]string{"PATH=/usr/bin", c.env}, i, c.n, c.k)
				if c.refused != "" {
					if err == nil || !strings.Contains(err.Error(), c.refused) || !strings.Contains(err.Error(), "never share a device") {
						t.Fatalf("worker %d: %v, %v; want a refusal naming %q", i, got, err, c.refused)
					}
					continue
				}
				if err != nil || !slices.Equal(got, c.want[i]) {
					t.Errorf("worker %d: %q, %v; want %q", i, got, err, c.want[i])
				}
			}
		})
	}
}

func TestWorkerLaunchNamesTheWorkersOwnSocket(t *testing.T) {
	l := testLaunch() // "--uds=/hpcburst/run/worker-0.sock"
	got, err := workerLaunch(l, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"vllm", "serve", "/hpcburst/model", "--uds=/hpcburst/run/worker-1.sock", "--max-num-seqs=256"}
	if !slices.Equal(got.Command, want) {
		t.Errorf("worker-1's command\n got %q\nwant %q", got.Command, want)
	}
	if l.Command[3] != "--uds=/hpcburst/run/worker-0.sock" {
		t.Errorf("the Instance's launch was changed: %q", l.Command[3])
	}
	// The controller's own form: --uds and the socket as two arguments.
	l.Command = []string{"vllm", "serve", "/hpcburst/model", "--uds", "/hpcburst/run/worker-0.sock", "--max-num-seqs=256"}
	if got, err := workerLaunch(l, "worker-2"); err != nil || got.Command[4] != "/hpcburst/run/worker-2.sock" {
		t.Errorf("two-argument form: %q, %v; want worker-2's socket", got.Command, err)
	}
	for _, bad := range [][]string{
		{"vllm", "serve", "/hpcburst/model"},
		{"vllm", "serve", "--uds=/hpcburst/run/worker-0.sock", "--other=/hpcburst/run/worker-0.sock"},
	} {
		l.Command = bad
		if _, err := workerLaunch(l, "worker-1"); err == nil {
			t.Errorf("command %q was accepted, want refused: the socket must be named in exactly one argument", bad)
		}
	}
}

func TestWorkerIndex(t *testing.T) {
	for name, want := range map[string]int{"worker-0": 0, "worker-7": 7} {
		if got, err := workerIndex(name); err != nil || got != want {
			t.Errorf("%s: %d, %v; want %d", name, got, err, want)
		}
	}
	for _, bad := range []string{"router", "worker-", "worker-x", "worker-01", "worker--1"} {
		if _, err := workerIndex(bad); err == nil {
			t.Errorf("%q was taken as a worker", bad)
		}
	}
}

// With a slice, the container's CUDA_VISIBLE_DEVICES is the slice alone, whatever the launch or the job
// set: the launch's value is replaced, and the job's whole list is not forwarded.
func TestWorkerExecSetsTheSliceReplacingTheLaunchsValue(t *testing.T) {
	l := testLaunch()
	l.Env = append(l.Env, agentapi.EnvVar{Name: "CUDA_VISIBLE_DEVICES", Value: "7"})
	w := WorkerExec{Launch: l, Key: testKey, BaseEnv: []string{"PATH=/usr/bin", "CUDA_VISIBLE_DEVICES=0,1"}, Devices: []string{"1"}}
	env, err := w.Env()
	if err != nil {
		t.Fatal(err)
	}
	var cvd []string
	for _, kv := range env {
		if strings.HasPrefix(kv, "APPTAINERENV_CUDA_VISIBLE_DEVICES=") {
			cvd = append(cvd, kv)
		}
	}
	if !slices.Equal(cvd, []string{"APPTAINERENV_CUDA_VISIBLE_DEVICES=1"}) {
		t.Errorf("the container's CUDA_VISIBLE_DEVICES: %q, want exactly the slice, 1", cvd)
	}
	// Without a slice, alpha's precedence: the launch's value wins.
	w.Devices = nil
	env, err = w.Env()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(env, "APPTAINERENV_CUDA_VISIBLE_DEVICES=7") || slices.Contains(env, "APPTAINERENV_CUDA_VISIBLE_DEVICES=0,1") {
		t.Errorf("one worker: %q, want the launch's 7, as alpha", env)
	}
}
