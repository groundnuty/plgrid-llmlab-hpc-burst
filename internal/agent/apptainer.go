package agent

import (
	"fmt"
	"os"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// apptainerEnvPrefixes are the prefixes Apptainer (and its Singularity
// compatibility) reads to set a variable inside the container. The agent
// drops every such variable it inherited, so the engine's environment is
// exactly the launch's (HBEP-7: a clean environment plus the launch's
// variables).
var apptainerEnvPrefixes = []string{"APPTAINERENV_", "SINGULARITYENV_"}

// WorkerExec is one engine worker's Apptainer invocation.
type WorkerExec struct {
	// Apptainer is the apptainer binary.
	Apptainer string
	// Image is the staged image's absolute path (a .sif file or a .sandbox
	// directory, agentapi.StagedImagePath).
	Image string
	// CheckpointDir is bound read-only at agentapi.ContainerModelDir.
	CheckpointDir string
	// RunDir is the job's private run directory, bound at
	// agentapi.ContainerRunDir; the engine's socket is in it.
	RunDir string
	// CacheDir is the Allocation's cache directory, bound read-write at
	// agentapi.ContainerCacheDir; the engine's cache variables point there.
	CacheDir string
	// Launch is the Instance's launch; its Command runs as it is.
	Launch agentapi.Launch
	// Key is the Instance's key, passed in Launch.KeyEnv.
	Key string
	// BaseEnv is the agent's own environment, os.Environ() in production.
	BaseEnv []string
}

// Args returns the apptainer argument vector, without the program: exec with
// GPU support, a clean environment and no home directory, the checkpoint bound
// read-only, the run directory and the Allocation's cache directory bound
// read-write, then the image and the launch's Command (HBEP-7, What it runs:
// the worker). No secret is on it: the key reaches the engine only through the
// environment (Env).
//
// --no-home keeps Apptainer from binding the user's home directory, where
// FlashInfer, the CUDA compute cache and other JIT caches wrote in #94's run
// although the cache variables pointed elsewhere (science-agent ruling,
// 1 Nov 2026, issue #125). A cache that ignores its variable then cannot write
// and fails the start, which WorkerReport.LogTail shows.
func (w WorkerExec) Args() []string {
	args := []string{
		"exec",
		"--nv",
		"--cleanenv",
		"--no-home",
		"--bind", w.CheckpointDir + ":" + agentapi.ContainerModelDir + ":ro",
		"--bind", w.RunDir + ":" + agentapi.ContainerRunDir,
		"--bind", w.CacheDir + ":" + agentapi.ContainerCacheDir,
		w.Image,
	}
	return append(args, w.Launch.Command...)
}

// engineCacheVars are the cache variables the agent points into
// agentapi.ContainerCacheDir, each at its own subdirectory (science-agent
// rulings, 1 Nov 2026, issue #125). A launch that sets one of them keeps its
// own value: the launch's variables are a deployment-ID input (HBEP-3), so the
// spec, not the agent, has the last word on them.
var engineCacheVars = []struct{ name, sub string }{
	{"XDG_CACHE_HOME", "xdg"},
	{"VLLM_CACHE_ROOT", "vllm"},
	{"TORCHINDUCTOR_CACHE_DIR", "torchinductor"},
	{"TRITON_CACHE_DIR", "triton"},
	{"FLASHINFER_WORKSPACE_BASE", "flashinfer"},
	{"CUDA_CACHE_PATH", "cuda"},
}

// Env returns the environment apptainer runs with: the agent's own, without
// any variable Apptainer would carry into the container, plus
// APPTAINERENV_<name> for each of the launch's variables, for each cache
// variable the launch does not set (engineCacheVars) and for the key
// variable. With --cleanenv the container sees only those (and Apptainer's
// own defaults). It refuses a launch variable whose name could not be
// carried, and a key variable that the launch's Env also sets.
func (w WorkerExec) Env() ([]string, error) {
	env := make([]string, 0, len(w.BaseEnv)+len(w.Launch.Env)+1)
	for _, kv := range w.BaseEnv {
		if hasAnyPrefix(kv, apptainerEnvPrefixes) {
			continue
		}
		env = append(env, kv)
	}
	seen := map[string]bool{}
	cudaVisible := ""
	for _, kv := range w.BaseEnv {
		if v, ok := strings.CutPrefix(kv, "CUDA_VISIBLE_DEVICES="); ok {
			cudaVisible = v
		}
	}
	for _, v := range w.Launch.Env {
		if !validEnvName(v.Name) {
			return nil, fmt.Errorf("launch variable %q is not a variable name", v.Name)
		}
		seen[v.Name] = true
		env = append(env, apptainerEnvPrefixes[0]+v.Name+"="+v.Value)
	}
	// --cleanenv drops the job's CUDA_VISIBLE_DEVICES, which Slurm sets to
	// the GPUs it gave the job: forward it, unless the launch sets it, so the
	// engine sees exactly those GPUs (Apptainer's GPU documentation; L-3.4).
	// It is not a deployment-ID input (HBEP-3's table).
	if cudaVisible != "" && !seen["CUDA_VISIBLE_DEVICES"] {
		env = append(env, apptainerEnvPrefixes[0]+"CUDA_VISIBLE_DEVICES="+cudaVisible)
	}
	for _, c := range engineCacheVars {
		if !seen[c.name] {
			env = append(env, apptainerEnvPrefixes[0]+c.name+"="+agentapi.ContainerCacheDir+"/"+c.sub)
		}
	}
	if w.Launch.KeyEnv != "" {
		if !validEnvName(w.Launch.KeyEnv) {
			return nil, fmt.Errorf("key variable %q is not a variable name", w.Launch.KeyEnv)
		}
		if seen[w.Launch.KeyEnv] {
			return nil, fmt.Errorf("the launch's env sets %s, which carries the Instance's key", w.Launch.KeyEnv)
		}
		env = append(env, apptainerEnvPrefixes[0]+w.Launch.KeyEnv+"="+w.Key)
	}
	return env, nil
}

func hasAnyPrefix(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

// validEnvName accepts the names a launch can carry: a letter or underscore,
// then letters, digits and underscores.
func validEnvName(s string) bool {
	if s == "" {
		return false
	}
	for i, c := range s {
		switch {
		case c == '_', c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z':
		case c >= '0' && c <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}

// writeChatTemplate writes the launch's chat template into the run directory,
// where the engine finds it at agentapi.ContainerChatTemplate.
func writeChatTemplate(runDir string, l agentapi.Launch) error {
	if l.ChatTemplate == nil {
		return nil
	}
	name := strings.TrimPrefix(agentapi.ContainerChatTemplate, agentapi.ContainerRunDir+"/")
	return os.WriteFile(runDir+"/"+name, []byte(*l.ChatTemplate), 0o600)
}
