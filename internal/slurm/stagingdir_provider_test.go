package slurm_test

// A Provider's own stagingDir at the Batch (HBEP-4, Provider settings; #757, #735): the Provider's value, else the profile's,
// is THE staging directory wherever HBEP-4 says stagingDir, one row per caller. Each row sets the Provider's value against a
// different profile value and reads the literal absolute path off the fake Slurm's call log and file system: where the file
// landed, which path the sbatch words name, which tree was removed. Every path below is invented and worked out by hand.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

const (
	provDir    = "/invented/stage/path"         // the Provider's stagingDir
	profileDir = "$SCRATCH/profile-stage"       // the profile's, a variable
	profileAbs = fakeScratch + "/profile-stage" // what the profile's resolves to on the fake
	stageTag   = "hb-example-0123456789ab"
)

// stagingRig is a Batch whose profile gives profile and whose Provider gives provDir ("" for none).
func stagingRig(t *testing.T, profile, providerDir string) (*slurm.Batch, *fakeslurm.Platform) {
	t.Helper()
	r := newFakeRig(t, profile)
	settings := provider.Settings{Provider: "example", Account: "example-account", Partition: fakePart, StagingDir: providerDir}
	r.batch.Configure(exampleProfile(profile), settings)
	return r.batch, r.plat
}

func fileAt(plat *fakeslurm.Platform, p string) bool { _, ok := plat.Files()[p]; return ok }

// Paths is the Provider's, resolved: a literal path as it is, and a variable on the login node, whichever the profile gives.
func TestPathsTakeTheProvidersStagingDir(t *testing.T) {
	for _, tc := range []struct {
		name, profile, provider, want string
	}{
		{"the Provider's path against the profile's variable", profileDir, provDir, provDir},
		{"the Provider's variable against the profile's variable", profileDir, "$SCRATCH/prov-stage", fakeScratch + "/prov-stage"},
		{"the Provider's path against a profile that gives none", "", provDir, provDir},
		{"no Provider value: the profile's", profileDir, "", profileAbs},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, _ := stagingRig(t, tc.profile, tc.provider)
			if got := pathsOf(t, b); got.Staging != tc.want || got.Home != fakeHome {
				t.Errorf("Paths = %+v, want staging %s and home %s", got, tc.want, fakeHome)
			}
		})
	}
}

// With neither, the Batch makes no call that needs the path and says why, without a value.
func TestPathsRefuseWhenNeitherGivesAStagingDir(t *testing.T) {
	b, _ := stagingRig(t, "", "")
	_, err := b.Paths(context.Background())
	if err == nil || !strings.Contains(err.Error(), "neither the Provider's spec.stagingDir nor the profile's sets one") {
		t.Fatalf("Paths = %v, want it to say neither field sets one", err)
	}
	if h := b.Health(); h.PathUnresolved == nil {
		t.Error("Health.PathUnresolved is nil: the Provider's PathUnresolved condition would not be set")
	}
}

// EnsureFile (provider.File under the staging directory): the file lands under the Provider's directory, and not under the
// profile's.
func TestEnsureFileUsesTheProvidersStagingDir(t *testing.T) {
	b, plat := stagingRig(t, profileDir, provDir)
	content := []byte("an example file\n")
	sum := sha256.Sum256(content)
	err := b.EnsureFile(context.Background(), provider.File{Path: "agent/example/hb-agent", SHA256: hex.EncodeToString(sum[:]), Mode: 0o700, Content: content})
	if err != nil {
		t.Fatal(err)
	}
	if !fileAt(plat, provDir+"/agent/example/hb-agent") {
		t.Errorf("the file is not at %s/agent/example/hb-agent; files: %v", provDir, plat.Files())
	}
	if fileAt(plat, profileAbs+"/agent/example/hb-agent") {
		t.Errorf("the file is under the profile's directory, %s", profileAbs)
	}
}

// RemoveAllocationFiles removes alloc/<tag>/ under the Provider's directory, and leaves the profile's alone.
func TestRemoveAllocationFilesUsesTheProvidersStagingDir(t *testing.T) {
	b, plat := stagingRig(t, profileDir, provDir)
	ours, theirs := provDir+"/alloc/"+stageTag+"/agent.json", profileAbs+"/alloc/"+stageTag+"/agent.json"
	for _, p := range []string{ours, theirs} {
		if err := plat.WriteFile(p, []byte("{}"), fs.FileMode(0o600)); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.RemoveAllocationFiles(context.Background(), stageTag); err != nil {
		t.Fatal(err)
	}
	if fileAt(plat, ours) {
		t.Errorf("%s is still there: the Provider's directory was not the one cleaned", ours)
	}
	if !fileAt(plat, theirs) {
		t.Errorf("%s is gone: the profile's directory was cleaned", theirs)
	}
}

// Submit names the Allocation's directory and the staged agent under the Provider's directory: the output and error files,
// and the bootstrap's agent path and directory.
func TestSubmitUsesTheProvidersStagingDir(t *testing.T) {
	b, plat := stagingRig(t, profileDir, provDir)
	if _, outcome, err := b.Submit(context.Background(), qosReq(stageTag, "")); outcome != provider.Accepted {
		t.Fatalf("Submit = %s, %v, want Accepted", outcome, err)
	}
	calls := sbatchWords(plat)
	if len(calls) != 1 {
		t.Fatalf("%d sbatch calls, want 1", len(calls))
	}
	words := strings.Join(calls[0], "\x00")
	for _, want := range []string{
		"--output=" + provDir + "/alloc/" + stageTag + "/job.out",
		"--error=" + provDir + "/alloc/" + stageTag + "/job.err",
	} {
		if !strings.Contains(words, "\x00"+want+"\x00") && !strings.HasSuffix(words, "\x00"+want) {
			t.Errorf("no sbatch word %q; words %q", want, calls[0])
		}
	}
	var wrap string
	for _, w := range calls[0] {
		if strings.HasPrefix(w, "--wrap=") {
			wrap = w
		}
	}
	for _, want := range []string{
		"sha256sum " + provDir + "/agent/" + qosDigest + "/hb-agent ",
		"exec " + provDir + "/agent/" + qosDigest + "/hb-agent " + provDir + "/alloc/" + stageTag,
	} {
		if !strings.Contains(wrap, want) {
			t.Errorf("the bootstrap does not hold %q: %s", want, wrap)
		}
	}
	if strings.Contains(strings.Join(calls[0], " "), profileAbs) {
		t.Errorf("an sbatch word holds the profile's directory %s: %q", profileAbs, calls[0])
	}
}

// Configure with a new Provider stagingDir makes the next Paths resolve it, though the connection's generation did not change:
// the cache alone would serve the old directory. Configure with the same one keeps the cache.
func TestConfigureResetsPathsOnANewProviderStagingDir(t *testing.T) {
	r := newFakeRig(t, profileDir)
	settings := func(dir string) provider.Settings {
		return provider.Settings{Provider: "example", Account: "example-account", Partition: fakePart, StagingDir: dir}
	}
	r.batch.Configure(exampleProfile(profileDir), settings("/invented/stage/a"))
	if got := pathsOf(t, r.batch); got.Staging != "/invented/stage/a" {
		t.Fatalf("Paths = %+v, want /invented/stage/a", got)
	}
	r.batch.Configure(exampleProfile(profileDir), settings("/invented/stage/a"))
	pathsOf(t, r.batch)
	if n := r.plat.Count("printenv"); n != 1 {
		t.Errorf("printenv ran %d times in all, want 1 (HOME only: the literal path needs no variable): the same dir keeps the cache", n)
	}
	r.batch.Configure(exampleProfile(profileDir), settings("/invented/stage/b"))
	if got := pathsOf(t, r.batch); got.Staging != "/invented/stage/b" {
		t.Errorf("Paths after a new Provider stagingDir = %+v, want /invented/stage/b", got)
	}
	// Dropping the Provider's value falls back to the profile's, which resolves again.
	r.batch.Configure(exampleProfile(profileDir), settings(""))
	if got := pathsOf(t, r.batch); got.Staging != profileAbs {
		t.Errorf("Paths after the Provider's value went = %+v, want the profile's, %s", got, profileAbs)
	}
}

// A Paths call that resolves the old Provider's stagingDir when Configure changes it must not put the old one back in the cache.
func TestConfigureDuringPathsDoesNotRecacheTheOldProviderDir(t *testing.T) {
	ctx := context.Background()
	r := newFakeRig(t, profileDir)
	settings := func(dir string) provider.Settings {
		return provider.Settings{Provider: "example", Account: "example-account", Partition: fakePart, StagingDir: dir}
	}
	r.batch.Configure(exampleProfile(profileDir), settings("$SCRATCH/stage-a"))
	gate := r.plat.Gate("printenv")
	done := make(chan error, 1)
	go func() {
		_, err := r.batch.Paths(ctx)
		done <- err
	}()
	gate.Wait()
	r.batch.Configure(exampleProfile(profileDir), settings("$SCRATCH/stage-b"))
	gate.Release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := pathsOf(t, r.batch); got.Staging != fakeScratch+"/stage-b" {
		t.Errorf("Paths = %+v, want staging %s/stage-b: the in-flight call re-cached the old directory", got, fakeScratch)
	}
}
