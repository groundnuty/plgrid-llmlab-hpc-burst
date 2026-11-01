package slurm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Expected values come from HBEP-4 (Core operations, ReadProfile; Provider
// settings) and HBEP-11 (credentials files are mode 600).

func writeFile(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

type profileRig struct {
	t      *testing.T
	dir    string
	creds  string
	key    string
	prov   *v1alpha1.Provider
	pp     *v1alpha1.PlatformProfile
	reader *ProfileReader
}

func newProfileRig(t *testing.T) *profileRig {
	t.Helper()
	dir := t.TempDir()
	r := &profileRig{t: t, dir: dir, creds: filepath.Join(dir, "creds"), key: filepath.Join(dir, "key")}
	writeFile(t, r.key, "example private key\n", 0o600)
	writeFile(t, r.creds, "# example\nHost example-login\n  HostName login.example.org\n  User example-user\n  IdentityFile "+r.key+"\n  StrictHostKeyChecking yes\n", 0o600)
	gpus := int32(8)
	maxTime := v1alpha1.Duration("48h")
	window := v1alpha1.Duration("5m")
	r.pp = &v1alpha1.PlatformProfile{ObjectMeta: metav1.ObjectMeta{Name: "example-profile"}, Spec: v1alpha1.PlatformProfileSpec{
		ProviderKind: v1alpha1.PlatformBatch, JSONDataParser: "v0.0.40", InflightWindow: &window, StagingDir: "$SCRATCH/stage",
		Partitions: []v1alpha1.PartitionProfile{
			{Name: "example-gpu", AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: "A100", CPUArch: "x86_64"}, GPUsPerNode: &gpus, MaxTime: &maxTime},
		},
	}}
	r.prov = &v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: "example-provider"}, Spec: v1alpha1.ProviderSpec{
		PlatformProfileRef: v1alpha1.ObjectReference{Name: "example-profile"}, Account: "example-account",
		AllowedPartitions: []string{"example-gpu"}, CredentialsRef: &v1alpha1.CredentialsReference{File: r.creds},
	}}
	return r
}

func (r *profileRig) client() client.Reader {
	r.t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		r.t.Fatal(err)
	}
	objs := []client.Object{r.prov}
	if r.pp != nil {
		objs = append(objs, r.pp)
	}
	return clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

// read reads the rig's Provider with a ProfileReader that leaves UID unset.
func (r *profileRig) read() (Validated, error) {
	r.t.Helper()
	return (&ProfileReader{Reader: r.client(), Home: r.dir}).Read(context.Background(), "example-provider")
}

func TestReadProfileValid(t *testing.T) {
	r := newProfileRig(t)
	v, err := r.read()
	if err != nil {
		t.Fatal(err)
	}
	if v.Settings.Partition != "example-gpu" || v.Settings.Account != "example-account" || v.Settings.CredentialsFile != r.creds {
		t.Errorf("settings %+v", v.Settings)
	}
	if v.Profile.Partition.Name != "example-gpu" || v.Profile.Partition.GPUModel != "A100" {
		t.Errorf("partition %+v", v.Profile.Partition)
	}
	if v.SSH != (SSHConfig{HostName: "login.example.org", User: "example-user", IdentityFile: r.key}) {
		t.Errorf("ssh %+v", v.SSH)
	}
}

// Every invalid field is named, not only the first.
func TestReadProfileNamesEveryInvalidField(t *testing.T) {
	r := newProfileRig(t)
	short := int32(40)
	low := v1alpha1.Duration("30s")
	r.pp.Spec.JobName = &v1alpha1.JobNameRules{MaxLength: &short, Pattern: "^[a-z]{1,10}$"}
	r.pp.Spec.JSONDataParser = "v0.0.41"
	r.pp.Spec.InflightWindow = &low
	r.pp.Spec.ProviderKind = v1alpha1.PlatformManaged
	r.prov.Spec.AllowedPartitions = []string{"example-gpu", "other"}
	_, err := r.read()
	var inv *provider.InvalidProfileError
	if !errors.As(err, &inv) {
		t.Fatalf("got %v, want an InvalidProfileError", err)
	}
	want := []string{"spec.providerKind", "provider.spec.allowedPartitions", "spec.jobName.maxLength", "spec.jobName.pattern",
		"spec.jsonDataParser", "spec.inflightWindow"}
	got := map[string]bool{}
	for _, f := range inv.Fields {
		got[f.Field] = true
	}
	for _, w := range want {
		if !got[w] {
			t.Errorf("field %s is not named; got %v", w, inv.Fields)
		}
	}
}

func TestReadProfileRefusals(t *testing.T) {
	cases := map[string]struct {
		mod   func(r *profileRig)
		field string
	}{
		"unlisted partition": {func(r *profileRig) { r.prov.Spec.AllowedPartitions = []string{"other"} }, "provider.spec.allowedPartitions"},
		"no inflightWindow":  {func(r *profileRig) { r.pp.Spec.InflightWindow = nil }, "spec.inflightWindow"},
		"window equal to the sbatch time-out": {func(r *profileRig) {
			d := v1alpha1.Duration("60s")
			r.pp.Spec.InflightWindow = &d
		}, "spec.inflightWindow"},
		"missing profile": {func(r *profileRig) { r.pp = nil }, "provider.spec.platformProfileRef.name"},
		"secret reference": {func(r *profileRig) {
			r.prov.Spec.CredentialsRef = &v1alpha1.CredentialsReference{SecretRef: &v1alpha1.SecretKeyReference{Name: "s", Namespace: "n", Key: "k"}}
		}, "provider.spec.credentialsRef.secretRef"},
		"no stagingDir":                {func(r *profileRig) { r.pp.Spec.StagingDir = "" }, "spec.stagingDir"},
		"tmpDir with a substitution":   {func(r *profileRig) { r.pp.Spec.Partitions[0].TmpDir = "$(id)" }, "spec.partitions[0].tmpDir"},
		"tmpDirFeature without tmpDir": {func(r *profileRig) { r.pp.Spec.Partitions[0].TmpDirFeature = "localfs" }, "spec.partitions[0].tmpDir"},
		"unsafe account":               {func(r *profileRig) { r.prov.Spec.Account = "acct;rm" }, "provider.spec.account"},
		"forwarding config": {func(r *profileRig) {
			writeFile(t, r.creds, "HostName h\nUser u\nIdentityFile "+r.key+"\nLocalForward 1 h:2\n", 0o600)
		}, "provider.spec.credentialsRef.file"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			r := newProfileRig(t)
			c.mod(r)
			_, err := r.read()
			var inv *provider.InvalidProfileError
			if !errors.As(err, &inv) {
				t.Fatalf("got %v, want an InvalidProfileError", err)
			}
			for _, f := range inv.Fields {
				if f.Field == c.field {
					return
				}
			}
			t.Errorf("field %s not named in %v", c.field, inv.Fields)
		})
	}
}

// The credentials file and the key it names must be the controller's, mode
// exactly 600 (HBEP-4, Core operations, ReadProfile; HBEP-11); anything else
// is ErrCredentials, and no secret is read into an error. Modes are set with
// an explicit chmod, so the umask plays no part.
func TestReadProfileCredentialModes(t *testing.T) {
	files := map[string]func(r *profileRig) string{
		"credentials": func(r *profileRig) string { return r.creds },
		"key":         func(r *profileRig) string { return r.key },
	}
	for file, path := range files {
		for _, mode := range []os.FileMode{0o400, 0o640, 0o604, 0o700} {
			t.Run(fmt.Sprintf("%s mode %o", file, mode), func(t *testing.T) {
				r := newProfileRig(t)
				if err := os.Chmod(path(r), mode); err != nil {
					t.Fatal(err)
				}
				_, err := r.read()
				if !errors.Is(err, provider.ErrCredentials) {
					t.Fatalf("got %v, want ErrCredentials", err)
				}
				if strings.Contains(err.Error(), "example private key") {
					t.Error("the key's content reached the error")
				}
			})
		}
		t.Run(file+" mode 600", func(t *testing.T) {
			r := newProfileRig(t)
			if err := os.Chmod(path(r), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := r.read(); err != nil {
				t.Errorf("mode 600 %s file: %v", file, err)
			}
		})
	}
	for name, mod := range map[string]func(r *profileRig){
		"missing key":         func(r *profileRig) { _ = os.Remove(r.key) },
		"missing credentials": func(r *profileRig) { _ = os.Remove(r.creds) },
	} {
		t.Run(name, func(t *testing.T) {
			r := newProfileRig(t)
			mod(r)
			if _, err := r.read(); !errors.Is(err, provider.ErrCredentials) {
				t.Fatalf("got %v, want ErrCredentials", err)
			}
		})
	}
}

// A ProfileReader that leaves UID unset means the controller's own user. The
// zero value of an integer would mean root, and every Provider of a caller
// that forgot the field would be CredentialsInvalid. The test names no UID, so
// it compiles whatever the field's type.
func TestZeroValueProfileReaderAcceptsFilesOfTheCurrentUser(t *testing.T) {
	r := newProfileRig(t)
	if _, err := (&ProfileReader{Reader: r.client(), Home: r.dir}).Read(context.Background(), "example-provider"); err != nil {
		t.Errorf("a zero-value ProfileReader refused files owned by the current user: %v", err)
	}
}

// A UID that is set must still own the files: a user other than the owner is
// refused.
func TestProfileReaderUIDMustOwnTheFiles(t *testing.T) {
	r := newProfileRig(t)
	other := os.Geteuid() + 1
	_, err := (&ProfileReader{Reader: r.client(), UID: &other, Home: r.dir}).Read(context.Background(), "example-provider")
	if !errors.Is(err, provider.ErrCredentials) {
		t.Errorf("got %v, want ErrCredentials for a UID that does not own the files", err)
	}
}

// A read error that is not the end of the file means the content is partial:
// no bytes come back, the error wraps ErrCredentials, and neither the partial
// content nor the read error's text (which may name a path) reaches it.
func TestReadBoundedReadError(t *testing.T) {
	failing := io.MultiReader(strings.NewReader("partial credentials"), iotest.ErrReader(errors.New("read /example/path: input/output error")))
	b, err := readBounded(failing)
	if b != nil || !errors.Is(err, provider.ErrCredentials) {
		t.Fatalf("readBounded = %q, %v; want no bytes and ErrCredentials", b, err)
	}
	for _, leak := range []string{"partial", "/example/path"} {
		if strings.Contains(err.Error(), leak) {
			t.Errorf("the error %q carries %q", err, leak)
		}
	}
}

// Content up to 1 MiB is read whole; one byte more is refused.
func TestReadBoundedSize(t *testing.T) {
	if b, err := readBounded(strings.NewReader(strings.Repeat("a", 1<<20))); err != nil || len(b) != 1<<20 {
		t.Errorf("1 MiB: %d bytes, %v; want all of it and no error", len(b), err)
	}
	if _, err := readBounded(strings.NewReader(strings.Repeat("a", 1<<20+1))); !errors.Is(err, provider.ErrCredentials) {
		t.Errorf("1 MiB and one byte: %v; want ErrCredentials", err)
	}
}

func TestParseSSHConfig(t *testing.T) {
	c, err := ParseSSHConfig([]byte("HostName=login.example.org\nUser example-user\nPort 2222\nIdentityFile ~/.ssh/example\nUserKnownHostsFile ~/.ssh/known\n"), "/home/example")
	if err != nil {
		t.Fatal(err)
	}
	want := SSHConfig{HostName: "login.example.org", User: "example-user", Port: 2222, IdentityFile: "/home/example/.ssh/example", UserKnownHostsFile: "/home/example/.ssh/known"}
	if c != want {
		t.Errorf("got %+v, want %+v", c, want)
	}
	for name, text := range map[string]string{
		"include":          "Include ~/.ssh/config\nHostName h\nUser u\nIdentityFile /k\n",
		"proxy command":    "ProxyCommand nc %h %p\nHostName h\nUser u\nIdentityFile /k\n",
		"control path":     "ControlPath /tmp/x\nHostName h\nUser u\nIdentityFile /k\n",
		"two hosts":        "Host a\nHost b\nHostName h\nUser u\nIdentityFile /k\n",
		"no key":           "HostName h\nUser u\n",
		"relative key":     "HostName h\nUser u\nIdentityFile key\n",
		"loose host check": "HostName h\nUser u\nIdentityFile /k\nStrictHostKeyChecking no\n",
		"bad host":         "HostName h;x\nUser u\nIdentityFile /k\n",
	} {
		if _, err := ParseSSHConfig([]byte(text), "/home/example"); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// Errors name keywords and lines, never values.
	_, err = ParseSSHConfig([]byte("HostName secret-host.example\nUser u\nProxyJump jump.example\n"), "")
	if err == nil || strings.Contains(err.Error(), "secret-host") || strings.Contains(err.Error(), "jump.example") {
		t.Errorf("error %v", err)
	}
}
