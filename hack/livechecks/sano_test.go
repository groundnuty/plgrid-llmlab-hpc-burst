package livechecks

// The Sano rows through the script (issue #735): their cluster shapes are the package's rows and the profile's per-GPU sizes, the queue
// estimate asks for what Sano's job_submit plugin requires (a QOS, a memory and a job name that is not generic), the stage row is built and run
// with the 7B's example, a SIF and the Provider's staging root, and the staging directory is a setting that is held to a form and never printed.
// The stubs cannot tell a real plugin's answer, so these read the script's arguments and environment, against values taken from the shipped
// profile and the package's rows, never from the script.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
)

// sanoProfile is the part of profiles/sano.yaml the shapes are held to.
type sanoProfile struct {
	Spec struct {
		Partitions []struct {
			Name       string `json:"name"`
			CPUsPerGPU int    `json:"cpusPerGpu"`
			MemPerGPU  string `json:"memPerGpu"`
			QOSByGPUs  []struct {
				GPUs int    `json:"gpus"`
				QOS  string `json:"qos"`
			} `json:"qosByGpus"`
		} `json:"partitions"`
	} `json:"spec"`
}

func readSanoProfile(t *testing.T) (cpus int, mem, qos string) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), "profiles", "sano.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var p sanoProfile
	if err := yaml.Unmarshal(b, &p); err != nil {
		t.Fatal(err)
	}
	for _, part := range p.Spec.Partitions {
		if part.Name != "gpu" {
			continue
		}
		for _, e := range part.QOSByGPUs {
			if e.GPUs >= 1 {
				return part.CPUsPerGPU, part.MemPerGPU, e.QOS
			}
		}
	}
	t.Fatal("profiles/sano.yaml lists no partition gpu with a qosByGpus entry")
	return
}

// The three cluster shapes are the package's rows, with the profile's per-GPU CPUs and memory, the QOS the profile chooses for one GPU in the
// extra column (the estimate sends it, since the row itself names none), a time limit that is the row's lease, and no slot, no start wait.
func TestTheScriptsSanoShapesArePackagesRows(t *testing.T) {
	b, err := os.ReadFile(scriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	cpus, mem, qos := readSanoProfile(t)
	for _, r := range []alpha.Row{alpha.SanoStageRow(), alpha.SanoServeRow(), alpha.SanoHandoverRow()} {
		var f []string
		for _, l := range block(t, string(b), "CLUSTER SHAPES") {
			if g := strings.Split(l, "|"); g[0] == r.Name {
				f = g
			}
		}
		if len(f) != 13 {
			t.Fatalf("the script has no shape of 13 fields (no start wait) for %s: %q", r.Name, f)
		}
		// name | host | profile | partition | qos | slot | account key | GPUs | CPUs | memory | time limit | timeout | extra
		clock := fmt.Sprintf("%02d:%02d:00", int(r.Lease/time.Hour), int(r.Lease%time.Hour/time.Minute))
		want := []string{r.Name, r.Cluster, r.ProfileName(), r.Partition, "-", "-", "ACCOUNT_SANO_GPU", "1", fmt.Sprint(cpus), mem, clock,
			fmt.Sprint(int(r.Budget / time.Minute)), "--qos=" + qos + " -J hb-estimate"}
		if strings.Join(f, "|") != strings.Join(want, "|") {
			t.Errorf("the script's shape is %q, the package's row and the profile give %q", f, want)
		}
		if r.QOS != "" || r.SlotHold != 0 || r.StartWait != 0 || r.OwnerGo != "" || !r.ProviderStaging {
			t.Errorf("the package's %s is %+v: Sano has no slot, no start wait, no owner's go and no QOS in the row, and its Provider gives its own staging directory", r.Name, r)
		}
	}
}

// The estimate is the one sbatch call before --yes, and it asks what Sano's plugin requires on the gpu partition: a QOS, a memory (in MiB, which the
// shape's "65536Mi" becomes), CPUs, and a job name that is not the generic default of --wrap. A rejected estimate stops the run, so a stub that
// accepts anything would hide a missing one.
func TestTheSanoEstimateAsksForWhatTheSubmitPluginRequires(t *testing.T) {
	cpus, mem, qos := readSanoProfile(t)
	for _, row := range []string{"alpha-sano-serve", "alpha-sano-handover", "alpha-sano-stage"} {
		t.Run(row, func(t *testing.T) {
			w := newAlphaWorld(t)
			res := w.run(nil, "--run", row)
			if res.code != 4 {
				t.Fatalf("exit %d, want 4:\n%s", res.code, res.out)
			}
			argv := lines(w.stateFile("sbatch-argv"))
			if len(argv) != 1 {
				t.Fatalf("want exactly one sbatch call (the estimate), got %v", argv)
			}
			for _, f := range []string{"--test-only", "-p gpu", "--qos=" + qos, "-J hb-estimate", "-N 1", fmt.Sprintf("-c %d", cpus), "--mem=" + strings.TrimSuffix(mem, "Mi") + "M",
				"--gres=gpu:1", "--no-requeue", "-A SEEDACCT-sano", "--wrap true"} {
				if !strings.Contains(argv[0], f) {
					t.Errorf("the estimate lacks %q: %s", f, argv[0])
				}
			}
			if strings.Contains(argv[0], " -q ") {
				t.Errorf("the estimate sends a -q beside --qos: %s", argv[0])
			}
		})
	}
}

// The stage row builds its helper before its run from the 7B's example and a SIF (the partition's format), and its test gets the Provider's staging
// root and the run's directory below it, the profile and the partition, and no QOS, start wait or dial-home setting. The staging directory is a
// leak seed: it must reach the test's environment and no output.
func TestTheSanoStageRowBuildsItsHelperFromTheSevenBAndASIFAndGivesTheTestItsRoot(t *testing.T) {
	w := newAlphaWorld(t)
	res := w.run(nil, "--run", "alpha-sano-stage", "--yes")
	if res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	ev := w.stateFile("events")
	if !strings.Contains(ev, "build-stage-helper ") || !strings.Contains(ev, " example="+alpha.SanoDeployment+" format=sif") {
		t.Errorf("the helper was not built from %s as a SIF:\n%s", alpha.SanoDeployment, ev)
	}
	if i, j := strings.Index(ev, "build-stage-helper "), strings.Index(ev, "testbin "); i < 0 || j < 0 || i > j {
		t.Errorf("the helper was not built before the test ran:\n%s", ev)
	}
	env := w.stateFile("test-env-TestSanoStage")
	cpus, mem, _ := readSanoProfile(t) // the profile's per-GPU sizes, never the script's
	for _, want := range []string{"HPCBURST_CONFORMANCE=1", "HPCBURST_CONF_PROFILE=sano", "HPCBURST_CONF_PARTITION=gpu", "HPCBURST_CONF_GPUS=1", fmt.Sprintf("HPCBURST_CONF_CPUS=%d", cpus),
		"HPCBURST_CONF_MEMORY=" + mem, "HPCBURST_CONF_STAGING_ROOT=/SEEDSTAGE/hb-stage", "HPCBURST_CONF_STAGING=/SEEDSTAGE/hb-stage/hpc-burst-conformance",
		"HPCBURST_CONF_STAGE_HELPER=", "HPCBURST_CONF_PENDING_MAX_SECONDS="} {
		if !strings.Contains(env, want) {
			t.Errorf("the test's environment lacks %s:\n%s", want, env)
		}
	}
	for _, not := range []string{"HPCBURST_ALPHA_RUN_ID", "HPCBURST_ALPHA_DIALHOME_MODE", "HPCBURST_CONF_QOS", "HPCBURST_CONF_START_WAIT"} {
		if strings.Contains(env, not) {
			t.Errorf("the test's environment has %s:\n%s", not, env)
		}
	}
	// The Athena stage row is built as before: a sandbox, the default example, and no staging root.
	w2 := newAlphaWorld(t)
	if res := w2.run(nil, "--run", "alpha-athena-now-stage", "--yes"); res.code != 0 {
		t.Fatalf("the Athena stage row: exit %d:\n%s", res.code, res.out)
	}
	if ev := w2.stateFile("events"); !strings.Contains(ev, " example=modeldeployment-qwen25-instruct.yaml format=sandbox") {
		t.Errorf("the Athena helper was not built from the default example as a sandbox:\n%s", ev)
	}
	if env := w2.stateFile("test-env-TestNowStage"); strings.Contains(env, "HPCBURST_CONF_STAGING_ROOT") {
		t.Errorf("the Athena stage row's test got a staging root:\n%s", env)
	}
}

// The staging directory is a setting of the Sano rows: missing, malformed, or beside ssh mode (the compute nodes have no home directory for its
// key; direct mode is taken, below, with its address and its listen address both set), the run is refused (exit 3) before anything is built or
// called, naming the setting and never its value.
func TestTheSanoStagingDirectoryAndModeAreRefusedBeforeAnythingRuns(t *testing.T) {
	// ssh mode as the settings write it: a mode-600 key (the fixture's) and an address, so that the mode is the one refusal.
	sshMode := func(m map[string]string) {
		m["LIVE_DIALHOME_MODE"], m["LIVE_DIALHOME_ADDR"] = "ssh", "gateway-seed.example:29999"
		m["LIVE_DIALHOME_KEY"] = filepath.Join(filepath.Dir(m["LIVE_BIN_DIR"]), "dialhome-key")
	}
	// direct mode as the first study sets it, less the one setting named: either alone is refused at the settings (PR 792's review).
	directWith := func(drop string) func(m map[string]string) {
		return func(m map[string]string) {
			m["LIVE_DIALHOME_MODE"], m["LIVE_DIALHOME_ADDR"], m["LIVE_DIALHOME_LISTEN"] = "direct", "gateway-seed.example:29707", "127.0.0.1:29707"
			delete(m, drop)
		}
	}
	for _, c := range []struct {
		name, row string
		mutate    func(m map[string]string)
		want      string
	}{
		{"a missing staging directory", "alpha-sano-serve", func(m map[string]string) { delete(m, "LIVE_SANO_STAGING_DIR") }, "setting LIVE_SANO_STAGING_DIR is missing"},
		{"a missing staging directory on the stage row", "alpha-sano-stage", func(m map[string]string) { delete(m, "LIVE_SANO_STAGING_DIR") }, "setting LIVE_SANO_STAGING_DIR is missing"},
		{"a relative staging directory", "alpha-sano-handover", func(m map[string]string) { m["LIVE_SANO_STAGING_DIR"] = "SEEDSTAGE/rel" }, "LIVE_SANO_STAGING_DIR is not an absolute path"},
		{"a staging directory with .. in it", "alpha-sano-serve", func(m map[string]string) { m["LIVE_SANO_STAGING_DIR"] = "/SEEDSTAGE/../x" }, "LIVE_SANO_STAGING_DIR is not an absolute path"},
		{"a staging directory with a space", "alpha-sano-stage", func(m map[string]string) { m["LIVE_SANO_STAGING_DIR"] = "/SEEDSTAGE/a b" }, "LIVE_SANO_STAGING_DIR is not an absolute path"},
		{"a staging directory with a colon", "alpha-sano-serve", func(m map[string]string) { m["LIVE_SANO_STAGING_DIR"] = "/SEEDSTAGE/a:b" }, "LIVE_SANO_STAGING_DIR has a ':', '=' or ',' in it"},
		{"a staging directory with an equals sign", "alpha-sano-handover", func(m map[string]string) { m["LIVE_SANO_STAGING_DIR"] = "/SEEDSTAGE/a=b" }, "LIVE_SANO_STAGING_DIR has a ':', '=' or ',' in it"},
		{"a staging directory with a comma", "alpha-sano-stage", func(m map[string]string) { m["LIVE_SANO_STAGING_DIR"] = "/SEEDSTAGE/a,b" }, "LIVE_SANO_STAGING_DIR has a ':', '=' or ',' in it"},
		{"ssh mode", "alpha-sano-serve", sshMode, "does not run in ssh mode"},
		{"ssh mode on the handover row", "alpha-sano-handover", sshMode, "does not run in ssh mode"},
		{"ssh mode on the baseline row", "beta-sano-llmlab-baseline", sshMode, "does not run in ssh mode"},
		{"direct mode with an address and no listen address", "alpha-sano-serve", directWith("LIVE_DIALHOME_LISTEN"), "alpha-sano-serve in direct mode needs LIVE_DIALHOME_LISTEN"},
		{"direct mode with an address and no listen address on the handover row", "alpha-sano-handover", directWith("LIVE_DIALHOME_LISTEN"), "alpha-sano-handover in direct mode needs LIVE_DIALHOME_LISTEN"},
		{"direct mode with an address and no listen address on the baseline row", "beta-sano-llmlab-baseline", directWith("LIVE_DIALHOME_LISTEN"), "beta-sano-llmlab-baseline in direct mode needs LIVE_DIALHOME_LISTEN"},
		{"direct mode with a listen address and no address", "alpha-sano-serve", directWith("LIVE_DIALHOME_ADDR"), "setting LIVE_DIALHOME_ADDR is missing"},
		{"direct mode with a listen address and no address on the baseline row", "beta-sano-llmlab-baseline", directWith("LIVE_DIALHOME_ADDR"), "setting LIVE_DIALHOME_ADDR is missing"},
		{"the account the shape needs", "alpha-sano-handover", func(m map[string]string) { delete(m, "ACCOUNT_SANO_GPU") }, "setting ACCOUNT_SANO_GPU is missing"},
		{"the provider's credentials", "alpha-sano-stage", func(m map[string]string) { delete(m, "LIVE_CONF_CREDENTIALS_SANO") }, "setting LIVE_CONF_CREDENTIALS_SANO is missing"},
	} {
		t.Run(c.name, func(t *testing.T) {
			w := newAlphaWorld(t)
			w.exactMode = true // the mode as written: the harness gives no host-dialed mode here
			w.alphaSettings(func(m map[string]string) {
				m["LIVE_DIALHOME_MODE"] = "host-dialed"
				delete(m, "LIVE_DIALHOME_ADDR")
				c.mutate(m)
			})
			res := w.run(nil, "--run", c.row, "--yes")
			if res.code != 3 {
				t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
			}
			w.contains(res, c.want)
			if strings.Contains(res.out, "SEEDSTAGE") {
				t.Errorf("a refusal names the staging directory:\n%s", res.out)
			}
			if g := w.stateFile("go-argv"); g != "" || w.stateFile("sbatch-argv") != "" || w.stateFile("ssh-argv") != "" {
				t.Errorf("a refused run built or called the cluster: %s %s %s", g, w.stateFile("sbatch-argv"), w.stateFile("ssh-argv"))
			}
		})
	}
}

// A Sano row runs in direct mode (issue #735): the first study dials Sano that way, and only ssh mode is refused (the table above). The script takes
// the mode with its address and the front's listen address, and gives both to the row's test; a row that refused it, or that dropped the listen
// address, would stop the live check of direct mode before it began. It holds for every Sano row, the baseline row (Row 1) included. The row runs in
// the mode as written, not in the host-dialed mode the harness gives the Sano rows by default.
func TestASanoRowRunsInDirectModeAndGivesTheTestItsAddressAndListenAddress(t *testing.T) {
	for _, r := range []struct{ row, test string }{{"alpha-sano-serve", "TestAlphaSanoServe"}, {"alpha-sano-handover", "TestAlphaSanoHandover"},
		{"beta-sano-llmlab-baseline", "TestBetaSanoLLMLabBaseline"}} {
		t.Run(r.row, func(t *testing.T) {
			w := newAlphaWorld(t)
			w.exactMode = true
			w.alphaSettings(func(m map[string]string) {
				m["LIVE_DIALHOME_MODE"] = "direct"
				m["LIVE_DIALHOME_ADDR"] = "gateway-seed.example:29707"
				m["LIVE_DIALHOME_LISTEN"] = "127.0.0.1:29707"
			})
			res := w.run(nil, "--run", r.row, "--yes")
			if res.code != 0 {
				t.Fatalf("exit %d, want 0:\n%s", res.code, res.out)
			}
			w.contains(res, r.row+": PASS")
			env := w.stateFile("test-env-" + r.test)
			for _, want := range []string{"HPCBURST_ALPHA_DIALHOME_MODE=direct", "HPCBURST_ALPHA_DIALHOME_ADDR=gateway-seed.example:29707", "HPCBURST_ALPHA_DIALHOME_LISTEN=127.0.0.1:29707"} {
				if !strings.Contains(env, want) {
					t.Errorf("the test did not get %s:\n%s", want, env)
				}
			}
			if strings.Contains(env, "HPCBURST_ALPHA_DIALHOME_KEY") {
				t.Errorf("a key reached a direct-mode row's test:\n%s", env)
			}
		})
	}
}
