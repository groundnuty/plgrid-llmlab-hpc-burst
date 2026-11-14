package main

import (
	"slices"
	"strings"
	"testing"
)

// Expected values from HBEP-7 (Several workers: each worker sees exactly its own device; the engine's
// socket is /hpcburst/run/worker-<i>.sock), HBEP-5 (a worker behind a router gets vLLM's keep-alive at
// 120 s) and the agent's Apptainer invocation (exec --nv --cleanenv --no-home, three binds).

func spec(index int, cuda string) workerSpec {
	return workerSpec{
		Index: index, Workers: 4, Apptainer: "apptainer", Image: "/st/images/abc.sandbox", Checkpoint: "/st/checkpoints/m/r",
		RunDir: "/scratch/r", CacheDir: "/scratch/c", Served: "Qwen/Qwen2.5-1.5B-Instruct", MaxNumSeqs: 256,
		PlacementLog: "/hpcburst/run/placement-B-w2.tsv", Flags: []string{"--max-model-len=16384", "--enable-prefix-caching"},
		BaseEnv: []string{"PATH=/usr/bin", "CUDA_VISIBLE_DEVICES=" + cuda, "APPTAINERENV_STRAY=1"},
	}
}

func TestWorkerInvocation(t *testing.T) {
	w, err := workerExec(spec(2, "4,5,6,7"))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"exec", "--nv", "--cleanenv", "--no-home",
		"--bind", "/st/checkpoints/m/r:/hpcburst/model:ro", "--bind", "/scratch/r:/hpcburst/run", "--bind", "/scratch/c:/hpcburst/cache",
		"/st/images/abc.sandbox",
		"vllm", "serve", "/hpcburst/model", "--uds", "/hpcburst/run/worker-2.sock", "--served-model-name", "Qwen/Qwen2.5-1.5B-Instruct",
		"--max-model-len=16384", "--enable-prefix-caching", "--max-num-seqs=256"}
	if got := w.Args(); !slices.Equal(got, want) {
		t.Fatalf("args\n got %q\nwant %q", got, want)
	}
	env, err := w.Env()
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range []string{
		"APPTAINERENV_CUDA_VISIBLE_DEVICES=6", "APPTAINERENV_VLLM_HTTP_TIMEOUT_KEEP_ALIVE=120",
		"APPTAINERENV_VLLM_SERVER_DEV_MODE=1", "APPTAINERENV_S2B_WORKER=2",
		"APPTAINERENV_S2B_PLACEMENT_LOG=/hpcburst/run/placement-B-w2.tsv", "APPTAINERENV_PYTHONPATH=/hpcburst/run/py",
	} {
		if !slices.Contains(env, kv) {
			t.Errorf("env lacks %s", kv)
		}
	}
	for _, kv := range env {
		if strings.HasPrefix(kv, "APPTAINERENV_CUDA_VISIBLE_DEVICES=") && kv != "APPTAINERENV_CUDA_VISIBLE_DEVICES=6" {
			t.Errorf("the worker would see more than its device: %s", kv)
		}
		if strings.HasPrefix(kv, "APPTAINERENV_STRAY=") {
			t.Errorf("an inherited APPTAINERENV_ variable reaches the container: %s", kv)
		}
	}
}

func TestWorkerDevices(t *testing.T) {
	for i, want := range []string{"4", "5", "6", "7"} {
		got, err := workerDevice([]string{"CUDA_VISIBLE_DEVICES=4,5,6,7"}, i, 4)
		if err != nil || !slices.Equal(got, []string{want}) {
			t.Errorf("worker %d: %q %v, want [%s]", i, got, err, want)
		}
	}
	if _, err := workerDevice([]string{"CUDA_VISIBLE_DEVICES=0,1,2"}, 0, 4); err == nil {
		t.Error("3 devices for 4 workers accepted: workers never share a device")
	}
	if _, err := workerDevice([]string{"PATH=/bin"}, 0, 4); err == nil {
		t.Error("no CUDA_VISIBLE_DEVICES accepted")
	}
	if _, err := workerDevice([]string{"CUDA_VISIBLE_DEVICES=0,1,2,3"}, 4, 4); err == nil {
		t.Error("worker 4 of 4 accepted")
	}
}

func TestWorkerCommandSwapsOneSocket(t *testing.T) {
	s := spec(3, "0,1,2,3")
	s.Flags = append(s.Flags, "--uds-extra=/hpcburst/run/worker-0.sock") // a flag VALUE that names it is not the argument
	cmd, err := workerCommand(s)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(cmd, "/hpcburst/run/worker-3.sock") || slices.Contains(cmd, "/hpcburst/run/worker-0.sock") {
		t.Fatalf("command %q", cmd)
	}
	s.Flags = []string{"/hpcburst/run/worker-0.sock"}
	if _, err := workerCommand(s); err == nil {
		t.Error("a command naming worker-0's socket twice accepted")
	}
}
