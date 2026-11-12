package agent

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// workerDevices is worker index's slice of the job's CUDA_VISIBLE_DEVICES, for an Instance of n workers
// of k GPUs each: devices index*k to (index+1)*k - 1, in Slurm's order (HBEP-7, Several workers). The
// tokens are kept as Slurm gives them, an index, a UUID or a MIG name. A job with fewer than n*k
// devices, or none named, is refused: two workers never share a device (the science agent's ruling on
// #439, Q1).
func workerDevices(baseEnv []string, index, n, k int) ([]string, error) {
	var list string
	found := false
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
	if k < 1 {
		return nil, fmt.Errorf("each worker uses %d GPUs: want 1 or more", k)
	}
	if index < 0 || index >= n {
		return nil, fmt.Errorf("worker %d of %d: no such worker", index, n)
	}
	if !found || len(devs) < n*k {
		return nil, fmt.Errorf("the job's CUDA_VISIBLE_DEVICES names %d devices (%q), and %d workers of %d GPUs need %d: workers never share a device", len(devs), list, n, k, n*k)
	}
	return append([]string(nil), devs[index*k:(index+1)*k]...), nil
}

// workerIndex is i of a worker named worker-<i> (agentapi.WorkerName).
func workerIndex(name string) (int, error) {
	s, ok := strings.CutPrefix(name, "worker-")
	if !ok {
		return 0, fmt.Errorf("worker name %q is not worker-<i>", name)
	}
	i, err := strconv.Atoi(s)
	if err != nil || i < 0 || agentapi.WorkerName(i) != name {
		return 0, fmt.Errorf("worker name %q is not worker-<i>", name)
	}
	return i, nil
}

// workerLaunch is the launch worker name runs: the Instance's launch, whose command names worker-0's
// socket in one argument, with that argument naming the worker's own socket instead. Nothing else differs
// between workers (HBEP-7, Several workers). A command that names worker-0's socket in other than exactly
// one argument is refused.
func workerLaunch(l agentapi.Launch, name string) (agentapi.Launch, error) {
	sock0 := agentapi.ContainerEngineSocket(agentapi.WorkerName(0))
	sock := agentapi.ContainerEngineSocket(name)
	at := -1
	for i, arg := range l.Command {
		if arg == sock0 || strings.HasSuffix(arg, "="+sock0) {
			if at >= 0 {
				return agentapi.Launch{}, errors.New("the launch's command names worker-0's socket in more than one argument")
			}
			at = i
		}
	}
	if at < 0 {
		return agentapi.Launch{}, fmt.Errorf("the launch's command names no argument of worker-0's socket %s", sock0)
	}
	out := l
	out.Command = append([]string(nil), l.Command...)
	out.Command[at] = strings.TrimSuffix(l.Command[at], sock0) + sock
	return out, nil
}
