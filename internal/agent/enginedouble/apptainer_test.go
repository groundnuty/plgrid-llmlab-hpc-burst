package enginedouble

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"
)

var testDigest = strings.Repeat("ab", 32)

func TestPullAndBuildWriteFakeImages(t *testing.T) {
	dir := shortDir(t)
	ref := "docker://registry.example/org/img@sha256:" + testDigest
	want := "fake-sif registry.example/org/img@sha256:" + testDigest + "\n"

	sif := filepath.Join(dir, "x.sif")
	if code := ApptainerMain([]string{"pull", sif, ref}); code != 0 {
		t.Fatalf("pull: exit %d", code)
	}
	if b, err := os.ReadFile(sif); err != nil || string(b) != want {
		t.Errorf("the .sif holds %q (%v), want %q", b, err, want)
	}
	if st, _ := os.Stat(sif); !st.Mode().IsRegular() {
		t.Error("the .sif is not a regular file")
	}
	if code := ApptainerMain([]string{"pull", sif, ref}); code != 255 {
		t.Errorf("pull over an existing image: exit %d, want 255 as Apptainer refuses", code)
	}
	if code := ApptainerMain([]string{"pull", "--force", sif, ref}); code != 0 {
		t.Errorf("pull --force: exit %d, want 0", code)
	}

	sandbox := filepath.Join(dir, "x.sandbox")
	if code := ApptainerMain([]string{"build", "--sandbox", sandbox, ref}); code != 0 {
		t.Fatalf("build --sandbox: exit %d", code)
	}
	if st, err := os.Stat(sandbox); err != nil || !st.IsDir() {
		t.Fatalf("the sandbox is not a directory: %v", err)
	}
	if b, err := os.ReadFile(filepath.Join(sandbox, ".fake-sandbox")); err != nil || string(b) != want {
		t.Errorf(".fake-sandbox holds %q (%v), want %q", b, err, want)
	}
}

func TestPullRefusesWhatIsNotADigestReference(t *testing.T) {
	dir := shortDir(t)
	for name, args := range map[string][]string{
		"a tag":              {"pull", filepath.Join(dir, "a.sif"), "docker://registry.example/img:latest"},
		"a short digest":     {"pull", filepath.Join(dir, "b.sif"), "docker://registry.example/img@sha256:abc"},
		"upper-case hex":     {"pull", filepath.Join(dir, "c.sif"), "docker://registry.example/img@sha256:" + strings.ToUpper(testDigest)},
		"another scheme":     {"pull", filepath.Join(dir, "d.sif"), "library://registry.example/img@sha256:" + testDigest},
		"no destination":     {"pull", "docker://registry.example/img@sha256:" + testDigest},
		"build, no sandbox":  {"build", filepath.Join(dir, "e.sif"), "docker://registry.example/img@sha256:" + testDigest},
		"an unknown command": {"push", "x"},
		"no command":         {},
	} {
		if code := ApptainerMain(args); code != 255 {
			t.Errorf("%s: exit %d, want 255", name, code)
		}
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Errorf("a refused pull left files behind: %v", entries)
	}
}

func TestExecRefusals(t *testing.T) {
	dir := shortDir(t)
	good := filepath.Join(dir, "good.sif")
	must(t, os.WriteFile(good, []byte("fake-sif x\n"), 0o644))
	must(t, os.WriteFile(filepath.Join(dir, "plain.img"), []byte("x"), 0o644))
	must(t, os.Mkdir(filepath.Join(dir, "dir.sif"), 0o755))
	must(t, os.WriteFile(filepath.Join(dir, "file.sandbox"), []byte("x"), 0o644))
	for name, args := range map[string][]string{
		"a missing image":               {"exec", filepath.Join(dir, "none.sif"), "true"},
		"an image not named .sif":       {"exec", filepath.Join(dir, "plain.img"), "true"},
		"a directory named .sif":        {"exec", filepath.Join(dir, "dir.sif"), "true"},
		"a file named .sandbox":         {"exec", filepath.Join(dir, "file.sandbox"), "true"},
		"a bind source that is missing": {"exec", "--bind", filepath.Join(dir, "nowhere") + ":/hpcburst/run", good, "true"},
		"a malformed bind":              {"exec", "--bind", "justapath", good, "true"},
		"an unknown flag":               {"exec", "--fakeroot", good, "true"},
		"no command":                    {"exec", good},
		"a command that does not exist": {"exec", good, "no-such-program-hb"},
	} {
		if code := ApptainerMain(args); code != 255 {
			t.Errorf("%s: exit %d, want 255", name, code)
		}
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestExecExitCodes(t *testing.T) {
	good := filepath.Join(shortDir(t), "good.sif")
	must(t, os.WriteFile(good, []byte("fake-sif x\n"), 0o644))
	for _, tc := range []struct {
		script string
		want   int
	}{
		{"exit 0", 0}, {"exit 7", 7}, {"kill -9 $$", 128 + 9}, {"kill -15 $$", 128 + 15},
	} {
		if code := ApptainerMain([]string{"exec", good, "sh", "-c", tc.script}); code != tc.want {
			t.Errorf("sh -c %q: exit %d, want %d", tc.script, code, tc.want)
		}
	}
}

// run starts the fake apptainer through the Install symlink, as the agent
// does, with exactly env as its environment.
func runApptainer(t *testing.T, dir string, env []string, args ...string) *proc {
	t.Helper()
	cmd := exec.Command(filepath.Join(dir, "apptainer"), args...)
	cmd.Env = env
	return start(t, cmd)
}

func envLines(out string) []string {
	var lines []string
	for _, l := range strings.Split(strings.TrimSpace(out), "\n") {
		if l != "" {
			lines = append(lines, l)
		}
	}
	sort.Strings(lines)
	return lines
}

func TestExecEnvironment(t *testing.T) {
	dir := installed(t)
	img := filepath.Join(dir, "x.sif")
	must(t, os.WriteFile(img, []byte("fake-sif x\n"), 0o644))
	host := []string{"PATH=/usr/bin:/bin", "HOST_ONLY=leak", "A=host", "APPTAINERENV_A=launch", "APPTAINERENV_B=2", "SINGULARITYENV_B=old", "SINGULARITYENV_C=3"}

	p := runApptainer(t, dir, host, "exec", "--cleanenv", img, "env")
	if code := p.exitCode(t, 5*time.Second); code != 0 {
		t.Fatalf("exit %d: %s", code, p.out.String())
	}
	if got, want := envLines(p.out.String()), []string{"A=launch", "B=2", "C=3"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("with --cleanenv the container sees %v, want exactly %v", got, want)
	}

	q := runApptainer(t, dir, host, "exec", img, "env")
	q.exitCode(t, 5*time.Second)
	got := strings.Join(envLines(q.out.String()), ",")
	// the host's variables stay, the launch's win, the prefixed ones themselves are consumed
	if want := "A=launch,B=2,C=3,HOST_ONLY=leak,PATH=/usr/bin:/bin"; got != want {
		t.Errorf("without --cleanenv the container sees %s, want %s", got, want)
	}
}

func TestExecRecordsEachInvocationWithKeysHashed(t *testing.T) {
	dir := installed(t)
	img := filepath.Join(dir, "x.sif")
	must(t, os.WriteFile(img, []byte("fake-sif x\n"), 0o644))
	logPath := filepath.Join(dir, "alog")
	env := []string{"PATH=/usr/bin:/bin", EnvApptainerLog + "=" + logPath, "APPTAINERENV_VLLM_API_KEY=s3cret", "APPTAINERENV_HF_TOKEN=t0k", "APPTAINERENV_NAME=plain", "APPTAINERENV_KEYBOARD=visible"}
	args := []string{"exec", "--nv", "--cleanenv", img, "env"}
	p := runApptainer(t, dir, env, args...)
	p.exitCode(t, 5*time.Second)
	pull := runApptainer(t, dir, env, "pull", filepath.Join(dir, "y.sif"), "docker://r.example/i@sha256:"+testDigest)
	pull.exitCode(t, 5*time.Second)

	var recs []map[string]any
	b, err := os.ReadFile(logPath)
	must(t, err)
	if bytes.Contains(b, []byte("s3cret")) || bytes.Contains(b, []byte("t0k")) {
		t.Fatalf("a key is in the log in clear:\n%s", b)
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 2 {
		t.Fatalf("%d log lines, want one per invocation:\n%s", len(lines), b)
	}
	recs = decodeLines(t, lines)
	exec := recs[0]
	if got := toStrings(exec["argv"]); strings.Join(got, " ") != strings.Join(args, " ") {
		t.Errorf("argv = %v, want %v", got, args)
	}
	wantEnv := []string{"HF_TOKEN=" + sha256Ref("t0k"), "KEYBOARD=visible", "NAME=plain", "VLLM_API_KEY=" + sha256Ref("s3cret")}
	if got := toStrings(exec["env"]); strings.Join(got, " ") != strings.Join(wantEnv, " ") {
		t.Errorf("env = %v, want %v (sorted; KEYBOARD ends in neither KEY nor TOKEN)", got, wantEnv)
	}
	if pid, _ := exec["pid"].(float64); int(pid) <= 0 || int(pid) == p.pid() {
		t.Errorf("pid = %v, want the child's, not the fake's %d", exec["pid"], p.pid())
	}
	if got := toStrings(recs[1]["argv"]); got[0] != "pull" || recs[1]["env"] != nil {
		t.Errorf("pull record = %v", recs[1])
	}
}

// The engine through the fake apptainer, started the way the agent starts it:
// a clean environment, the key in APPTAINERENV_VLLM_API_KEY, the checkpoint and
// run directories bound, and the engine at a container socket path.
func TestEngineThroughFakeApptainer(t *testing.T) {
	dir := installed(t)
	ckpt, run := filepath.Join(dir, "ckpt"), filepath.Join(dir, "run")
	must(t, os.Mkdir(ckpt, 0o755))
	must(t, os.Mkdir(run, 0o755))
	img := filepath.Join(dir, "x.sif")
	must(t, os.WriteFile(img, []byte("fake-sif x\n"), 0o644))
	alog, elog := filepath.Join(dir, "alog"), filepath.Join(dir, "elog")
	socket := sockPath(t, run, "worker-0.sock")
	env := []string{
		"PATH=/usr/bin:/bin", "HOST_ONLY=leak",
		EnvModes + "=health-fail", // the host's mode must not reach a --cleanenv engine
		EnvApptainerLog + "=" + alog,
		"APPTAINERENV_VLLM_API_KEY=key-123",
		"APPTAINERENV_" + EnvModes + "=kv-low",
		"APPTAINERENV_" + EnvEngineLog + "=" + elog,
	}
	args := []string{"exec", "--nv", "--cleanenv",
		"--bind", ckpt + ":/hpcburst/model:ro", "--bind", run + ":/hpcburst/run",
		img, "vllm", "serve", "/hpcburst/model", "--uds=/hpcburst/run/worker-0.sock",
		"--served-model-name=served", "--max-model-len=4096", "--max-num-seqs=8"}
	p := runApptainer(t, dir, env, args...)
	waitSocket(t, socket)
	s := &testServer{t: t, hc: unixClient(socket)}

	if r := s.get("/v1/models"); r.status != 401 {
		t.Errorf("/v1/models without the key: HTTP %d, want 401", r.status)
	}
	var models struct {
		Data []struct {
			ID          string `json:"id"`
			Root        string `json:"root"`
			MaxModelLen int    `json:"max_model_len"`
		}
	}
	r := s.do("GET", "/v1/models", "key-123", nil)
	if r.status != 200 {
		t.Fatalf("/v1/models with the key: HTTP %d: %s", r.status, r.body)
	}
	r.json(t, &models)
	// the container path /hpcburst/model was rewritten to the host's checkpoint
	if d := models.Data[0]; d.ID != "served" || d.Root != ckpt || d.MaxModelLen != 4096 {
		t.Errorf("model card = %+v, want served, %s, 4096", d, ckpt)
	}
	if r := s.get("/health"); r.status != 200 {
		t.Errorf("/health: HTTP %d: the host's HB_ENGINE_DOUBLE=health-fail leaked through --cleanenv", r.status)
	}
	if got := lookup(labelsOf(t, grepLine(s.metrics(), "vllm:cache_config_info")), "kv_cache_size_tokens"); got != "1000" {
		t.Errorf("kv_cache_size_tokens = %s: APPTAINERENV_HB_ENGINE_DOUBLE=kv-low did not reach the engine", got)
	}

	start := readLog(t, elog)[0]
	wantArgv := []string{"serve", ckpt, "--uds=" + socket, "--served-model-name=served", "--max-model-len=4096", "--max-num-seqs=8"}
	if strings.Join(start.Argv, "\x00") != strings.Join(wantArgv, "\x00") {
		t.Errorf("the engine was started with %q, want the container paths rewritten: %q", start.Argv, wantArgv)
	}
	for _, kv := range start.Env {
		if strings.HasPrefix(kv, "HOST_ONLY=") || strings.HasPrefix(kv, "PATH=") {
			t.Errorf("--cleanenv leaked %s to the engine", kv)
		}
		if strings.Contains(kv, "key-123") {
			t.Errorf("the key is in the engine's log in clear: %s", kv)
		}
	}
	if !contains(start.Env, "VLLM_API_KEY="+sha256Ref("key-123")) || !contains(start.Env, "HB_TEST_ROLE=engine") {
		t.Errorf("engine env = %q, want VLLM_API_KEY hashed and HB_TEST_ROLE=engine", start.Env)
	}
	if rec := decodeLines(t, strings.Split(strings.TrimSpace(string(mustRead(t, alog))), "\n")); int(rec[0]["pid"].(float64)) != start.PID {
		t.Errorf("the apptainer record's pid %v is not the engine's %d", rec[0]["pid"], start.PID)
	}

	// TERM to the fake is forwarded to the engine; the fake lives until it is gone
	_ = syscall.Kill(p.pid(), syscall.SIGTERM)
	if code := p.exitCode(t, 3*time.Second); code != 0 {
		t.Errorf("exit code %d after TERM, want the engine's 0", code)
	}
	if _, err := os.Stat(socket); !os.IsNotExist(err) {
		t.Errorf("the socket remains after TERM: %v", err)
	}
}
