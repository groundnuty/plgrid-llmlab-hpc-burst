package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The worker: one vLLM engine under Apptainer, invoked as the allocation agent invokes it for an
// Instance of several workers behind a router (HBEP-7, Several workers; HBEP-5, Inside an Instance).
// The argument vector and the environment are agent.WorkerExec's own (Args, Env): exec --nv --cleanenv
// --no-home, the checkpoint bound read-only at /hpcburst/model, the run directory at /hpcburst/run, the
// cache directory at /hpcburst/cache, the worker's one device as the container's CUDA_VISIBLE_DEVICES,
// and vLLM's keep-alive at 120 s. The command is what the lifecycle controller renders (internal/
// lifecycle, renderLaunch): vllm serve on the checkpoint, listening on worker-0's socket, the served
// name, the flags one argument each, then --max-num-seqs=<n>; and, as the agent's workerLaunch does,
// worker-0's socket swapped for this worker's. Those two steps are unexported, so they are restated
// here; workerCommand's test pins the result.

// workerSpec is what one worker's invocation is built from.
type workerSpec struct {
	Index, Workers int
	Apptainer      string
	Image          string
	Checkpoint     string
	RunDir         string
	CacheDir       string
	Served         string
	MaxNumSeqs     int
	PlacementLog   string   // the placement middleware's log, a path inside the container
	Flags          []string // vLLM flags, one argument each
	BaseEnv        []string // the job's environment
}

// workerCommand is the Instance's launch command for worker index: renderLaunch's vector with
// worker-0's socket, then that one argument swapped for the worker's own (workerLaunch).
func workerCommand(s workerSpec) ([]string, error) {
	sock0 := agentapi.ContainerEngineSocket(agentapi.WorkerName(0))
	cmd := []string{"vllm", "serve", agentapi.ContainerModelDir, "--uds", sock0, "--served-model-name", s.Served}
	cmd = append(cmd, s.Flags...)
	cmd = append(cmd, "--max-num-seqs="+strconv.Itoa(s.MaxNumSeqs))
	n := 0
	for i, a := range cmd {
		if a == sock0 {
			cmd[i] = agentapi.ContainerEngineSocket(agentapi.WorkerName(s.Index))
			n++
		}
	}
	if n != 1 {
		return nil, fmt.Errorf("the command names worker-0's socket in %d arguments, want exactly 1", n)
	}
	return cmd, nil
}

// workerDevice is the worker's slice of the job's CUDA_VISIBLE_DEVICES: entry index, one GPU per
// worker, in Slurm's order (the agent's workerDevices with k=1). A job with fewer entries than workers
// is refused: two workers never share a device.
func workerDevice(baseEnv []string, index, n int) ([]string, error) {
	list, found := "", false
	for _, kv := range baseEnv {
		if v, ok := strings.CutPrefix(kv, "CUDA_VISIBLE_DEVICES="); ok {
			list, found = v, true
		}
	}
	var devs []string
	for _, d := range strings.Split(list, ",") {
		if d = strings.TrimSpace(d); d != "" {
			devs = append(devs, d)
		}
	}
	if index < 0 || index >= n {
		return nil, fmt.Errorf("worker %d of %d: no such worker", index, n)
	}
	if !found || len(devs) < n {
		return nil, fmt.Errorf("the job's CUDA_VISIBLE_DEVICES names %d devices and %d workers need %d: workers never share a device", len(devs), n, n)
	}
	return []string{devs[index]}, nil
}

// workerExec builds the agent's WorkerExec for the spec.
func workerExec(s workerSpec) (agent.WorkerExec, error) {
	cmd, err := workerCommand(s)
	if err != nil {
		return agent.WorkerExec{}, err
	}
	dev, err := workerDevice(s.BaseEnv, s.Index, s.Workers)
	if err != nil {
		return agent.WorkerExec{}, err
	}
	env := []agentapi.EnvVar{
		{Name: "VLLM_SERVER_DEV_MODE", Value: "1"}, // POST /reset_prefix_cache between arms
		{Name: "PYTHONPATH", Value: agentapi.ContainerRunDir + "/py"},
		{Name: "PYTHONDONTWRITEBYTECODE", Value: "1"},
		{Name: "S2B_WORKER", Value: strconv.Itoa(s.Index)},
		{Name: "S2B_PLACEMENT_LOG", Value: s.PlacementLog},
		{Name: "VLLM_NO_USAGE_STATS", Value: "1"},
		{Name: "DO_NOT_TRACK", Value: "1"},
	}
	return agent.WorkerExec{
		Apptainer: s.Apptainer, Image: s.Image, CheckpointDir: s.Checkpoint, RunDir: s.RunDir, CacheDir: s.CacheDir,
		Launch:  agentapi.Launch{Command: cmd, Env: env},
		BaseEnv: s.BaseEnv, Devices: dev, RouterKeepAlive: true,
	}, nil
}

func workerMain(args []string) int {
	fs := flag.NewFlagSet("worker", flag.ContinueOnError)
	var s workerSpec
	var record string
	fs.IntVar(&s.Index, "index", -1, "worker index")
	fs.IntVar(&s.Workers, "workers", 0, "workers in the Instance")
	fs.StringVar(&s.Apptainer, "apptainer", "apptainer", "apptainer binary")
	fs.StringVar(&s.Image, "image", "", "the staged image (absolute path)")
	fs.StringVar(&s.Checkpoint, "checkpoint", "", "the staged checkpoint directory (absolute path)")
	fs.StringVar(&s.RunDir, "run-dir", "", "the run directory (absolute path)")
	fs.StringVar(&s.CacheDir, "cache-dir", "", "the cache directory (absolute path)")
	fs.StringVar(&s.Served, "served", "", "served model name")
	fs.IntVar(&s.MaxNumSeqs, "max-num-seqs", 256, "vLLM --max-num-seqs")
	fs.StringVar(&s.PlacementLog, "placement-log", "", "placement log, a path inside the container")
	fs.StringVar(&record, "record", "", "file to write the invocation to (JSON)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	s.Flags = fs.Args()
	for name, v := range map[string]string{"image": s.Image, "checkpoint": s.Checkpoint, "run-dir": s.RunDir, "cache-dir": s.CacheDir, "served": s.Served, "placement-log": s.PlacementLog, "record": record} {
		if v == "" {
			return fail("worker: -%s is required", name)
		}
	}
	for name, p := range map[string]string{"image": s.Image, "checkpoint": s.Checkpoint, "run-dir": s.RunDir, "cache-dir": s.CacheDir} {
		if !strings.HasPrefix(p, "/") {
			return fail("worker: -%s %q is not an absolute path", name, p)
		}
	}
	s.BaseEnv = os.Environ()
	w, err := workerExec(s)
	if err != nil {
		return fail("worker %d: %v", s.Index, err)
	}
	env, err := w.Env()
	if err != nil {
		return fail("worker %d: %v", s.Index, err)
	}
	argv := w.Args()
	bin, err := exec.LookPath(s.Apptainer)
	if err != nil {
		return fail("worker %d: %v", s.Index, err)
	}
	// The record holds only the variables the container gets (APPTAINERENV_*) and the arguments: no
	// secret is among them (the Instance has no key here), and the job's own environment stays out.
	var carried []string
	for _, kv := range env {
		if strings.HasPrefix(kv, "APPTAINERENV_") {
			carried = append(carried, kv)
		}
	}
	b, _ := json.MarshalIndent(map[string]any{"argv": append([]string{"apptainer"}, argv...), "container_env": carried}, "", "  ")
	if err := os.WriteFile(record, append(b, '\n'), 0o644); err != nil {
		return fail("worker %d: record: %v", s.Index, err)
	}
	// Replace this process with apptainer, so the PID the job recorded is the worker's own.
	err = syscall.Exec(bin, append([]string{"apptainer"}, argv...), env)
	return fail("worker %d: exec %s: %v", s.Index, bin, err)
}
