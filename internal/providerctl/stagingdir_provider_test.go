package providerctl

// A Provider's own stagingDir at the controller (HBEP-4, Provider settings; #757, #735): the Provider's value, else the
// profile's, is THE staging directory everywhere HBEP-4 says stagingDir, so each caller that reads it gets one row. A row sets
// the Provider's value against a different profile value and reads the literal absolute path off the fake Slurm: where each
// file landed, what agent.json holds, which paths the sbatch words name, which tree the deletion removed, and what the
// keeper's set and the relays' ssh command line carry. The paths are invented and worked out by hand; none is read back from
// the code under test.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

const (
	rowProvDir   = "/invented/stage/path"  // the Provider's stagingDir
	rowProfRoot  = "/scratch/user-a/stage" // what the profile's "$SCRATCH/stage" resolves to for user-a on the fake
	rowProfLit   = "/profile/literal/path" // a profile's literal path, for the rows on the fake Platform
	rowAllocTag  = "alloc-a"
	rowAgentFile = "hb-agent"
)

// withProviderStagingDir gives the Provider's settings a stagingDir, against the profile's "$SCRATCH/stage".
func withProviderStagingDir(dir string) realOption {
	return func(e *realEnv) { e.profiles.v.Settings.StagingDir = dir }
}

func under(files map[string]fakeslurm.File, root string) []string {
	var got []string
	for p := range files {
		if p == root || strings.HasPrefix(p, root+"/") {
			got = append(got, p)
		}
	}
	return got
}

// One reconcile run to Running, then one named row per caller of the staging directory, then the deletion.
func TestTheProvidersStagingDirIsTheDirectoryEverywhere(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", withProviderStagingDir(rowProvDir))
	a := e.newAlloc(rowAllocTag, 1)
	e.run(rowAllocTag, 3)
	tag := v1alpha1.JobTag(a.Namespace, a.Name, a.UID)
	files := sl.Files()
	allocDir := rowProvDir + "/alloc/" + tag

	t.Run("EnsureFile stages the agent binary and frpc", func(t *testing.T) {
		for _, p := range []string{
			rowProvDir + "/agent/" + e.digest + "/" + rowAgentFile,
			rowProvDir + "/frpc/" + e.opts.Front.FrpcDigests[testArch] + "/frpc",
		} {
			if _, ok := files[p]; !ok {
				t.Errorf("%s was not staged", p)
			}
		}
	})
	t.Run("EnsureFile stages the Allocation's credential and agent.json", func(t *testing.T) {
		for _, p := range []string{allocDir + "/credential", allocDir + "/agent.json"} {
			if _, ok := files[p]; !ok {
				t.Errorf("%s was not staged", p)
			}
		}
	})
	t.Run("agent.json holds the Provider's directory", func(t *testing.T) {
		js, ok := files[allocDir+"/agent.json"]
		if !ok {
			t.Fatalf("no agent.json at %s/agent.json", allocDir)
		}
		cfg, err := agentapi.DecodeAgentConfig(js.Content)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.StagingDir != rowProvDir || cfg.DialHomeDir != "/home/user-a/.hpcburst/dial-home" {
			t.Errorf("agent.json stagingDir %q and dialHomeDir %q, want %q and the home's dial-home", cfg.StagingDir, cfg.DialHomeDir, rowProvDir)
		}
	})
	t.Run("the submission names the Provider's directory", func(t *testing.T) {
		var sb []string
		for _, c := range sl.Calls() {
			if c.Words[0] == "sbatch" {
				sb = c.Words
			}
		}
		if sb == nil {
			t.Fatal("no sbatch call")
		}
		line := strings.Join(sb, " ")
		for _, want := range []string{
			"--output=" + allocDir + "/job.out", "--error=" + allocDir + "/job.err",
			"exec " + rowProvDir + "/agent/" + e.digest + "/" + rowAgentFile + " " + allocDir,
		} {
			if !strings.Contains(line, want) {
				t.Errorf("the sbatch words do not hold %q: %s", want, line)
			}
		}
	})
	t.Run("nothing lands under the profile's directory", func(t *testing.T) {
		if got := under(files, rowProfRoot); len(got) != 0 {
			t.Errorf("files under the profile's resolved directory %s: %v", rowProfRoot, got)
		}
	})

	t.Run("the deletion removes the Allocation's directory under the Provider's", func(t *testing.T) {
		// A file under the profile's directory for the same tag must stay: it is not the staging directory.
		decoy := rowProfRoot + "/alloc/" + tag + "/decoy"
		if err := sl.WriteFile(decoy, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if got := under(sl.Files(), allocDir); len(got) == 0 {
			t.Fatalf("nothing under %s before the deletion: the row would prove nothing", allocDir)
		}
		if err := e.c.Delete(e.ctx, e.get(rowAllocTag)); err != nil {
			t.Fatal(err)
		}
		e.run(rowAllocTag, 4)
		after := sl.Files()
		if got := under(after, allocDir); len(got) != 0 {
			t.Errorf("the Allocation's directory under the Provider's is still there: %v", got)
		}
		if _, ok := after[decoy]; !ok {
			t.Errorf("the profile's directory was cleaned instead: %s is gone", decoy)
		}
	})
}

// ---- the hostdial callers, on the fake Platform: the keeper's set and the relays' command line ----

// withResolvedStaging makes the fake Platform's login-node paths give staging, as the real Batch gives the Provider's value,
// and the profile's stagingDir a different literal: a caller that reads the profile's field directly, and not what Paths gives,
// carries the profile's.
func withResolvedStaging(staging string) envOption {
	return func(e *env) {
		e.platform.paths.Staging = staging
		e.profiles.v.Profile.Spec.StagingDir = rowProfLit
		e.profiles.v.Settings.StagingDir = staging
	}
}

// The relay keeper's desired set carries each Allocation's directory and agent under the staging directory.
func TestTheKeepersSetUsesTheResolvedStagingDir(t *testing.T) {
	k := startFakeKeeper(t)
	e := newEnv(t, keeperMode(k, 5), withResolvedStaging(rowProvDir))
	keeperUp(e)
	a := e.running("alloc-a")
	tag := tagOf(a)
	s := k.waitSet(t, "a set with alloc-a decided", 0, func(s wire.DesiredSet) bool { _, ok := entryOf(s, tag); return ok })
	en, _ := entryOf(s, tag)
	if want := rowProvDir + "/alloc/" + tag; en.AllocDir != want {
		t.Errorf("the keeper's entry names the directory %q, want %q", en.AllocDir, want)
	}
	if want := rowProvDir + "/agent/" + e.digest + "/" + rowAgentFile; en.AgentPath != want {
		t.Errorf("the keeper's entry names the agent %q, want %q", en.AgentPath, want)
	}
}

// The host-dialed relays' ssh command line carries the Allocation's directory (--chdir and --dir) and the staged agent under the
// staging directory. The ssh binary of the row is a script that records its arguments and ends at once, as /bin/false does.
func TestTheRelaysUseTheResolvedStagingDir(t *testing.T) {
	rec := filepath.Join(t.TempDir(), "ssh-args")
	script := filepath.Join(t.TempDir(), "ssh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '%s\\n' \"$*\" >> "+rec+"\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	e := newEnv(t, hostDialed, withResolvedStaging(rowProvDir), func(e *env) { e.opts.Relays.SSHBinary = script })
	runRelays(e)
	e.newAlloc("alloc-a")
	e.run("alloc-a", 4)
	a := e.get("alloc-a")
	tag := tagOf(a)
	wantDir := "--chdir=" + rowProvDir + "/alloc/" + tag + " " + rowProvDir + "/agent/" + e.digest + "/" + rowAgentFile
	waitRelays(t, "an ssh start was recorded", func() bool {
		b, _ := os.ReadFile(rec)
		return strings.Contains(string(b), wantDir)
	})
	b, _ := os.ReadFile(rec)
	if !strings.Contains(string(b), "--dir "+rowProvDir+"/alloc/"+tag+" ") && !strings.Contains(string(b), "--dir "+rowProvDir+"/alloc/"+tag+"\n") {
		t.Errorf("no recorded command line holds --dir %s/alloc/%s:\n%s", rowProvDir, tag, b)
	}
	if strings.Contains(string(b), rowProfLit) {
		t.Errorf("a recorded command line holds the profile's directory %s:\n%s", rowProfLit, b)
	}
}
