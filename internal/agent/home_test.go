package agent_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// A cache that ignores its variable writes under HOME, which in a container
// without the home directory it cannot do, so the engine's start fails; the
// report then carries the end of the engine's output (WorkerReport.LogTail),
// which says why (science-agent ruling, 1 Nov 2026, #127 review of issue
// #125). The host's HOME stays empty, and the key never appears in the tail.
func TestWorkerStartFailureCarriesTheEngineLog(t *testing.T) {
	home := t.TempDir()
	h := newHarness(t, harnessOptions{home: home})
	h.set(h.runState("write-home", 1, 1, nil))
	r := h.waitReport("worker exited", 20*time.Second, func(r agentapi.Report) bool {
		w := worker0(r)
		return w != nil && w.Phase == agentapi.PartExited
	})
	w := worker0(r)
	if !strings.Contains(w.LogTail, "enginedouble: write-caches: mkdir ") {
		t.Errorf("LogTail %q does not show the failed write under HOME", w.LogTail)
	}
	if len(w.LogTail) > agentapi.MaxLogTailBytes {
		t.Errorf("LogTail is %d bytes, above %d", len(w.LogTail), agentapi.MaxLogTailBytes)
	}
	if k := r.Instance.Key; k != "" && strings.Contains(w.LogTail, k) {
		t.Errorf("LogTail carries the Instance's key")
	}
	entries, err := os.ReadDir(home)
	if err != nil || len(entries) != 0 {
		t.Errorf("the agent's HOME holds %v (%v), want nothing", entries, err)
	}
}

// The worker's exec keeps the engine out of the home directory and puts its
// caches in the Allocation's own directory (science-agent rulings, 1 Nov 2026,
// issue #125; #94, finding 5: FlashInfer, the CUDA compute cache and other JIT
// caches wrote under the home directory). The engine double's write-caches
// mode writes into every cache a real engine fills at start, one of them,
// like the CUDA compute cache, always under HOME. The fake Apptainer binds the
// caller's home and sets HOME to it, as Apptainer does, unless --no-home is
// given, and it maps each bound container path in the arguments and the
// variables to its host path, so the files land where the binds put them.
//
// Not proof on its own: the argument list. The test also requires that the
// agent's HOME, an empty directory, is still empty once the engine has written
// its caches, and that the caches are in the Allocation's cache directory.
func TestWorkerExecLeavesHomeUntouched(t *testing.T) {
	home := t.TempDir()
	h := newHarness(t, harnessOptions{home: home})
	h.set(h.runState("write-caches", 1, 1, nil))
	r := h.waitReport("worker running", 20*time.Second, running)
	if lt := worker0(r).LogTail; lt != "" {
		t.Errorf("a running worker carries LogTail %q", lt)
	}

	execs := h.execs()
	if len(execs) != 1 {
		t.Fatalf("%d execs, want 1", len(execs))
	}
	ex := execs[0]
	if !slices.Contains(ex.Argv, "--no-home") {
		t.Errorf("the exec has no --no-home: %q", ex.Argv)
	}
	bind := h.cacheDir + ":" + agentapi.ContainerCacheDir
	if i := slices.Index(ex.Argv, bind); i < 1 || ex.Argv[i-1] != "--bind" {
		t.Errorf("the exec does not bind the cache directory as --bind %s: %q", bind, ex.Argv)
	}
	for _, a := range ex.Argv {
		if a == home || filepath.Dir(a) == home {
			t.Errorf("the exec names the home directory: %q", a)
		}
	}
	for _, want := range []string{
		"XDG_CACHE_HOME=/hpcburst/cache/xdg",
		"VLLM_CACHE_ROOT=/hpcburst/cache/vllm",
		"TORCHINDUCTOR_CACHE_DIR=/hpcburst/cache/torchinductor",
		"TRITON_CACHE_DIR=/hpcburst/cache/triton",
		"FLASHINFER_WORKSPACE_BASE=/hpcburst/cache/flashinfer",
		"CUDA_CACHE_PATH=/hpcburst/cache/cuda",
	} {
		if !slices.Contains(ex.Env, want) {
			t.Errorf("the engine's environment lacks %s: %q", want, ex.Env)
		}
	}

	var inHome []string
	filepath.WalkDir(home, func(p string, d fs.DirEntry, err error) error {
		if err == nil && p != home {
			inHome = append(inHome, p)
		}
		return nil
	})
	if len(inHome) != 0 {
		t.Errorf("the engine wrote under the agent's HOME: %q", inHome)
	}
	for _, rel := range []string{
		"xdg/hb-double-jit/entry",
		"vllm/torch_compile_cache/entry",
		"torchinductor/entry",
		"triton/entry",
		"flashinfer/.cache/flashinfer/entry",
		"cuda/entry",
	} {
		if _, err := os.Stat(filepath.Join(h.cacheDir, rel)); err != nil {
			t.Errorf("cache entry %s is not in the Allocation's cache directory: %v", rel, err)
		}
	}
}
