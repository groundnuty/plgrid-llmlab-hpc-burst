package stage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

const vllmImage = "docker.io/vllm/vllm-openai"

// imageEntries lists what is in images/ apart from the lock files, which stay.
func imageEntries(t *testing.T, s *Stager) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(s.c.StagingDir, "images"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".lock") {
			out = append(out, e.Name())
		}
	}
	return out
}

// apptainerCalls returns the first argument of every invocation the Go double
// recorded: "pull" or "build".
func apptainerCalls(t *testing.T, root string) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, "apptainer.jsonl"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var rec struct{ Argv []string }
		if err := json.Unmarshal([]byte(l), &rec); err != nil {
			t.Fatal(err)
		}
		out = append(out, rec.Argv[0])
	}
	return out
}

func stageImage(t *testing.T, s *Stager, format agentapi.ImageFormat) agent.ImageResult {
	t.Helper()
	res, err := s.Image(context.Background(), vllmImage, digest, format)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Verified {
		t.Fatalf("Image returned an unverified %+v", res)
	}
	return res
}

// An image that no longer matches its record is pulled again, once, and the
// copy moved aside is removed; an intact one is not pulled again.
func TestSIFThatFailsItsRecordIsPulledAgain(t *testing.T) {
	for _, tc := range []struct {
		name   string
		damage func(t *testing.T, sif string)
	}{
		{"truncated", func(t *testing.T, sif string) {
			if err := os.Truncate(sif, 5); err != nil {
				t.Fatal(err)
			}
		}},
		{"one byte changed", func(t *testing.T, sif string) {
			b, err := os.ReadFile(sif)
			if err != nil {
				t.Fatal(err)
			}
			b[len(b)/2] ^= 0xff
			if err := os.WriteFile(sif, b, 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{"sidecar removed, as older code left it", func(t *testing.T, sif string) {
			if err := os.Remove(sif + ".sha256"); err != nil {
				t.Fatal(err)
			}
		}},
		{"sidecar names another image", func(t *testing.T, sif string) {
			b, _ := os.ReadFile(sif + ".sha256")
			other := strings.Replace(string(b), digest, "sha256:"+strings.Repeat("3", 64), 1)
			if other == string(b) {
				t.Fatal("the sidecar does not hold the digest")
			}
			os.WriteFile(sif+".sha256", []byte(other), 0o640)
		}},
		{"sidecar garbled", func(t *testing.T, sif string) {
			os.WriteFile(sif+".sha256", []byte("not json"), 0o640)
		}},
		{"replaced by a directory", func(t *testing.T, sif string) {
			os.Remove(sif)
			os.Mkdir(sif, 0o755)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, root := newStager(t, nil)
			first := stageImage(t, s, agentapi.ImageFormatSIF)
			good, err := os.ReadFile(first.Path)
			if err != nil {
				t.Fatal(err)
			}
			if n := len(apptainerCalls(t, root)); n != 1 {
				t.Fatalf("%d pulls after the first staging", n)
			}
			tc.damage(t, first.Path)
			second := stageImage(t, s, agentapi.ImageFormatSIF)
			if n := len(apptainerCalls(t, root)); n != 2 {
				t.Fatalf("%d pulls after the damage, want 2", n)
			}
			if b, _ := os.ReadFile(second.Path); string(b) != string(good) {
				t.Errorf("the SIF holds %q after the repair", b)
			}
			stageImage(t, s, agentapi.ImageFormatSIF)
			if n := len(apptainerCalls(t, root)); n != 2 {
				t.Errorf("%d pulls: the repaired image was pulled again", n)
			}
			if left := imageEntries(t, s); len(left) != 2 {
				t.Errorf("images/ holds %v, want the SIF and its sidecar", left)
			}
		})
	}
}

// A sandbox with a file deleted, a file changed or a file added is built again.
func TestSandboxThatFailsItsManifestIsBuiltAgain(t *testing.T) {
	for _, tc := range []struct {
		name   string
		damage func(t *testing.T, sb string)
	}{
		{"file deleted", func(t *testing.T, sb string) {
			if err := os.Remove(filepath.Join(sb, ".fake-sandbox")); err != nil {
				t.Fatal(err)
			}
		}},
		{"file changed", func(t *testing.T, sb string) {
			p := filepath.Join(sb, ".fake-sandbox")
			b, _ := os.ReadFile(p)
			b[0] ^= 0xff
			if err := os.WriteFile(p, b, 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{"file added", func(t *testing.T, sb string) {
			if err := os.WriteFile(filepath.Join(sb, "extra"), []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{"manifest removed", func(t *testing.T, sb string) {
			if err := os.Remove(sb + ".manifest"); err != nil {
				t.Fatal(err)
			}
		}},
		{"manifest truncated", func(t *testing.T, sb string) {
			b, _ := os.ReadFile(sb + ".manifest")
			i := strings.LastIndex(strings.TrimSuffix(string(b), "\n"), "\n")
			os.WriteFile(sb+".manifest", b[:i+1], 0o640)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, root := newStager(t, nil)
			first := stageImage(t, s, agentapi.ImageFormatSandbox)
			tc.damage(t, first.Path)
			second := stageImage(t, s, agentapi.ImageFormatSandbox)
			if calls := apptainerCalls(t, root); len(calls) != 2 || calls[1] != "build" {
				t.Fatalf("invocations %q, want two builds", calls)
			}
			if b, err := os.ReadFile(filepath.Join(second.Path, ".fake-sandbox")); err != nil || !strings.HasPrefix(string(b), "fake-sif ") {
				t.Errorf("the rebuilt sandbox holds %q (%v)", b, err)
			}
			if _, err := os.Stat(filepath.Join(second.Path, "extra")); err == nil {
				t.Error("the added file is still in the sandbox")
			}
			stageImage(t, s, agentapi.ImageFormatSandbox)
			if n := len(apptainerCalls(t, root)); n != 2 {
				t.Errorf("%d builds: the rebuilt sandbox was built again", n)
			}
			if left := imageEntries(t, s); len(left) != 2 {
				t.Errorf("images/ holds %v, want the sandbox and its manifest", left)
			}
		})
	}
}

// A sandbox with nested directories, an executable, a symlink and a file the
// staging user cannot read: it is staged and then passes its check, so the
// manifest cannot make every real sandbox fail; retargeting the symlink, adding
// an empty directory and removing a directory each fail it.
func TestSandboxWithLinksDirectoriesAndAnUnreadableFile(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads every file")
	}
	build := `dest="$3"
mkdir -p "$dest/usr/bin" "$dest/etc/ssl" "$dest/empty"
printf 'binary' > "$dest/usr/bin/python" && chmod 755 "$dest/usr/bin/python"
ln -s python "$dest/usr/bin/python3"
printf 'secret' > "$dest/etc/shadow" && chmod 000 "$dest/etc/shadow"
echo build >> LOG
`
	for _, tc := range []struct {
		name   string
		damage func(t *testing.T, sb string)
	}{
		{"symlink retargeted", func(t *testing.T, sb string) {
			p := filepath.Join(sb, "usr/bin/python3")
			os.Remove(p)
			os.Symlink("evil", p)
		}},
		{"empty directory added", func(t *testing.T, sb string) { os.Mkdir(filepath.Join(sb, "usr/new"), 0o755) }},
		{"directory removed", func(t *testing.T, sb string) { os.Remove(filepath.Join(sb, "empty")) }},
		{"nested file changed", func(t *testing.T, sb string) {
			os.WriteFile(filepath.Join(sb, "usr/bin/python"), []byte("hacked"), 0o755)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, root := newStager(t, nil)
			scriptApptainer(t, root, strings.ReplaceAll(build, "LOG", filepath.Join(root, "builds")))
			builds := func() int {
				b, _ := os.ReadFile(filepath.Join(root, "builds"))
				return strings.Count(string(b), "build")
			}
			first := stageImage(t, s, agentapi.ImageFormatSandbox)
			stageImage(t, s, agentapi.ImageFormatSandbox)
			if builds() != 1 {
				t.Fatalf("%d builds: the intact sandbox, with its unreadable file, failed its check", builds())
			}
			tc.damage(t, first.Path)
			stageImage(t, s, agentapi.ImageFormatSandbox)
			if builds() != 2 {
				t.Fatalf("%d builds after the damage, want 2", builds())
			}
		})
	}
}

// Two Allocations staging one digest at once: one pulls, the other waits for
// its lock, finds the image intact and does not pull (the SIF of a second pull
// would differ from the first's, and the renames could pair one's SIF with the
// other's sidecar).
func TestConcurrentImageStagingPullsOnce(t *testing.T) {
	s1, _, root := newStager(t, nil)
	s2, err := New(s1.c)
	if err != nil {
		t.Fatal(err)
	}
	busy, once := make(chan struct{}), sync.Once{}
	for _, s := range []*Stager{s1, s2} {
		s.hooks.lockBusy = func() { once.Do(func() { close(busy) }) }
	}
	pulls, gate := filepath.Join(root, "pulls"), filepath.Join(root, "gate")
	scriptApptainer(t, root, fmt.Sprintf(`echo pull >> %s
while [ ! -e %s ]; do sleep 0.02; done
printf 'fake-sif\n' > "$2"
`, pulls, gate))

	var wg sync.WaitGroup
	results := make([]agent.ImageResult, 2)
	errs := make([]error, 2)
	for i, s := range []*Stager{s1, s2} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = s.Image(context.Background(), vllmImage, digest, agentapi.ImageFormatSIF)
		}()
	}
	select {
	case <-busy:
	case <-time.After(5 * time.Second):
		t.Error("neither stager ever waited for the other's lock")
	}
	os.WriteFile(gate, nil, 0o600)
	wg.Wait()
	for i := range results {
		if errs[i] != nil || !results[i].Verified || results[i].Path != results[0].Path {
			t.Errorf("stager %d: %+v, %v", i, results[i], errs[i])
		}
	}
	if b, _ := os.ReadFile(pulls); strings.Count(string(b), "pull") != 1 {
		t.Errorf("the pulls: %q, want exactly one", b)
	}
}

// Old temporary pull output of this digest, left by a run that died, is
// removed once it is an hour old; a younger one and another digest's are not.
func TestStaleTempsOfTheDigestAreRemoved(t *testing.T) {
	s, _, _ := newStager(t, nil)
	dir := filepath.Join(s.c.StagingDir, "images")
	os.MkdirAll(dir, 0o750)
	hex := strings.TrimPrefix(digest, "sha256:")
	other := strings.Repeat("3", 64)
	old := time.Now().Add(-2 * time.Hour)
	mk := func(name string, dirIn bool, mtime time.Time) string {
		p := filepath.Join(dir, name)
		if dirIn {
			os.MkdirAll(filepath.Join(p, "deep"), 0o755)
			os.WriteFile(filepath.Join(p, "deep", "f"), []byte("x"), 0o644)
		} else {
			os.WriteFile(p, []byte("x"), 0o644)
		}
		os.Chtimes(p, mtime, mtime)
		return p
	}
	oldSIF := mk(".tmp-"+hex+"-aaaa.sif", false, old)
	oldSandbox := mk(".tmp-"+hex+"-bbbb.sandbox", true, old)
	oldRecord := mk(".tmp-"+hex+"-bbbb.record", false, old)
	young := mk(".tmp-"+hex+"-cccc.sandbox", true, time.Now().Add(-10*time.Minute))
	otherDigest := mk(".tmp-"+other+"-dddd.sif", false, old)
	stageImage(t, s, agentapi.ImageFormatSIF)
	for _, p := range []string{oldSIF, oldSandbox, oldRecord} {
		if _, err := os.Lstat(p); !os.IsNotExist(err) {
			t.Errorf("%s was not removed (%v)", filepath.Base(p), err)
		}
	}
	for _, p := range []string{young, otherDigest} {
		if _, err := os.Lstat(p); err != nil {
			t.Errorf("%s was removed: %v", filepath.Base(p), err)
		}
	}
}

// A failed pull puts the damaged image back instead of leaving it moved aside
// for good, and leaves no temporary output.
func TestFailedRepairPutsTheDamagedImageBack(t *testing.T) {
	s, _, root := newStager(t, nil)
	first := stageImage(t, s, agentapi.ImageFormatSIF)
	os.Truncate(first.Path, 3)
	scriptApptainer(t, root, "echo down >&2\nexit 1\n")
	if _, err := s.Image(context.Background(), vllmImage, digest, agentapi.ImageFormatSIF); err == nil {
		t.Fatal("the pull failed but Image succeeded")
	}
	if fi, err := os.Stat(first.Path); err != nil || fi.Size() != 3 {
		t.Errorf("the damaged SIF was not put back: %v", err)
	}
	for _, e := range imageEntries(t, s) {
		if strings.Contains(e, ".tmp-") || strings.Contains(e, ".mismatch-") {
			t.Errorf("left behind: %s", e)
		}
	}
}
