package alpha

// The Provider a live row creates (liveProvider, steps_live_test.go), read through the real ReadProfile on the row's shipped profile
// (issue #735). The dry runs and the stand-ins never build it, so without this a Sano row's Provider could fail its first read on the
// cluster: a qos beside qosByGpus, or no stagingDir where the profile has none.
//
// The expected values are literals of the shipped profile's own facts (sano.yaml: no stagingDir, qosByGpus gpu1 for one GPU,
// dialHomeModes host-dialed), never read back from the code that builds the Provider.

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// readLive reads the Provider liveProvider makes for cfg through ReadProfile, with the profile the row names read whole from profiles/.
func readLive(t *testing.T, cfg Config, mutate func(*v1alpha1.Provider)) (slurm.Validated, error) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "profiles", cfg.Row.ProfileName()+".yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var pp v1alpha1.PlatformProfile
	if err := yaml.UnmarshalStrict(b, &pp); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	key, creds := filepath.Join(dir, "key"), filepath.Join(dir, "creds")
	if err := os.WriteFile(key, []byte("example private key\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(creds, []byte("Host example-login\n  HostName login.example.org\n  User example-user\n  IdentityFile "+key+"\n  StrictHostKeyChecking yes\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg.Credentials, cfg.Account = creds, "example-account"
	prov := liveProvider(cfg, pp.Name)
	if mutate != nil {
		mutate(prov)
	}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects([]client.Object{prov, &pp}...).Build()
	return (&slurm.ProfileReader{Reader: c, Home: dir, DialHomeMode: agentapi.DialHomeHostDialed}).Read(context.Background(), prov.Name)
}

func sanoCfg(name string) Config {
	r, _ := RowByName(name)
	return Config{Row: r, DialHomeMode: "host-dialed", StagingDir: "/path/to/shared/example-stage"}
}

// Every Sano row's Provider is accepted: no qos of its own, the run's staging directory in its own settings, and the one partition. The
// plugin then chooses gpu1 for one GPU, from the profile.
func TestTheSanoRowsProvidersPassReadProfile(t *testing.T) {
	for _, name := range []string{SanoStageName, SanoServeName, SanoHandoverName} {
		t.Run(name, func(t *testing.T) {
			cfg := sanoCfg(name)
			v, err := readLive(t, cfg, nil)
			if err != nil {
				t.Fatalf("ReadProfile refused the row's Provider: %v", err)
			}
			if v.Settings.StagingDir != cfg.StagingDir || v.Settings.QOS != "" || v.Settings.Partition != "gpu" {
				t.Errorf("settings: staging dir kept %t, qos %q, partition %q; want the run's directory, no qos and gpu", v.Settings.StagingDir == cfg.StagingDir, v.Settings.QOS, v.Settings.Partition)
			}
			q, err := v1alpha1.QOSForGPUs(v.Profile.Partition, v.Settings.QOS, int32(cfg.Row.GPUs))
			if err != nil || q != "gpu1" {
				t.Errorf("the QOS chosen for %d GPU(s) is %q (%v), want gpu1", cfg.Row.GPUs, q, err)
			}
		})
	}
}

// The controls: the Provider's read fails without the pieces, so the pass above is not a read that accepts anything. Each is the Provider the
// row would have made with that piece wrong.
func TestTheSanoRowsProviderIsRefusedWithoutItsPieces(t *testing.T) {
	cfg := sanoCfg(SanoServeName)
	for _, c := range []struct {
		name   string
		mutate func(*v1alpha1.Provider)
		want   string
	}{
		{"no stagingDir of the Provider's own", func(p *v1alpha1.Provider) { p.Spec.StagingDir = "" }, "stagingDir"},
		{"a qos beside qosByGpus", func(p *v1alpha1.Provider) { p.Spec.QOS = "gpu1" }, "qos"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := readLive(t, cfg, c.mutate)
			var inv *provider.InvalidProfileError
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("ReadProfile = %v, want a refusal that names %s", err, c.want)
			}
			if !asInvalid(err, &inv) {
				t.Errorf("the refusal is %T, want an InvalidProfileError", err)
			}
		})
	}
}

func asInvalid(err error, target **provider.InvalidProfileError) bool {
	e, ok := err.(*provider.InvalidProfileError)
	if ok {
		*target = e
	}
	return ok
}

// A row's ProviderStaging says its profile has no stagingDir, and the Provider's own is then the only one: the two agree for every row, so a
// profile that gains a stagingDir, or a row added without one, fails here.
func TestEveryRowsProviderStagingFollowsItsProfile(t *testing.T) {
	for _, r := range slices.Concat(Rows(), StageRows()) {
		t.Run(r.Name, func(t *testing.T) {
			if got := shippedProfile(t, r.ProfileName()).StagingDir == ""; got != r.ProviderStaging {
				t.Errorf("the row's ProviderStaging is %t, and the profile's stagingDir is absent: %t", r.ProviderStaging, got)
			}
		})
	}
}

// A row's Provider on Sano is host-dialed alone, as its profile lists (the dialHomeModes of profiles/sano.yaml): ConfigFromEnv refuses another
// mode for a Sano row.
func TestASanoRowRefusesAnotherDialHomeMode(t *testing.T) {
	for _, mode := range []string{"direct", "ssh"} {
		m := liveSettings(SanoServeName)
		m[envMode], m[envAddr], m[envKey] = mode, "gateway.example:29227", "/example/key"
		if mode == "ssh" {
			m[envAddr] = "gateway.example:29227"
		}
		_, _, err := ConfigFromEnv(env(m), SanoServeName)
		if err == nil || !strings.Contains(err.Error(), "host-dialed") {
			t.Errorf("mode %s: err %v, want a refusal that names host-dialed", mode, err)
		}
	}
	m := liveSettings(SanoServeName)
	if _, _, err := ConfigFromEnv(env(m), SanoServeName); err != nil {
		t.Errorf("host-dialed: %v", err)
	}
}

// The staging directory is a setting of the Sano rows alone and is held to the CRD's form; no message names its value.
func TestTheStagingDirSettingIsRequiredCheckedAndNeverNamed(t *testing.T) {
	const secret = "/people/example-user/hb-secret-stage"
	m := liveSettings(SanoServeName)
	delete(m, envStagingDir)
	if _, _, err := ConfigFromEnv(env(m), SanoServeName); err == nil || !strings.Contains(err.Error(), envStagingDir+" is not set") {
		t.Errorf("a missing staging directory: %v", err)
	}
	for _, bad := range []string{"relative/dir", "/a/../b", "/has space", "/semi;colon", "$", "/" + strings.Repeat("a", 1100)} {
		m[envStagingDir] = bad
		if _, _, err := ConfigFromEnv(env(m), SanoServeName); err == nil || strings.Contains(err.Error(), bad) {
			t.Errorf("staging dir %.20q: err %v, want a refusal that does not quote the value", bad, err)
		}
	}
	m[envStagingDir] = secret
	cfg, _, err := ConfigFromEnv(env(m), SanoServeName)
	if err != nil || cfg.StagingDir != secret {
		t.Fatalf("a valid staging directory: %v", err)
	}
	// A row whose profile has a stagingDir takes none.
	a := liveSettings("alpha-athena-handover")
	a[envStagingDir] = secret
	if _, _, err := ConfigFromEnv(env(a), "alpha-athena-handover"); err == nil || strings.Contains(err.Error(), secret) {
		t.Errorf("a staging directory on the Athena row: %v, want a refusal that does not quote it", err)
	}
	// A dry run refuses it as a live setting.
	d := map[string]string{envRow: SanoServeName, envDry: "1", envOut: "/example/out", envStagingDir: secret, EnvFrontLabel: testFrontLabel}
	if _, _, err := ConfigFromEnv(env(d), SanoServeName); err == nil || strings.Contains(err.Error(), secret) {
		t.Errorf("a dry run with a staging directory: %v, want a refusal", err)
	}
}
