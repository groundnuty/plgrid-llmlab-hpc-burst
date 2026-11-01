package providerctl

// What Create stages beside the agent (HBEP-4, Submission: Files first): frpc
// for the partition's cpuArch and the launcher's dial-home certificate, every
// file before the claim. The fixtures here are written out in full, and their
// digests are the output of sha256sum(1) on exactly those bytes, so no expected
// value comes from the code under test or from a helper that made the file. An
// expected path is written by hand ("frpc/" + digest + "/frpc").

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl/ctltest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// The frpc fixtures, one per cpuArch, and the digest of each: printf of the
// text into sha256sum. A third file stands for a frpc that is not the pinned one.
const (
	frpcX86, frpcX86Digest         = "example frpc for x86_64\n", "0f3bd6fc6ae5bbd0ff0fb8b69219c5633b714f16a170ff1ffac74c645ce6563f"
	frpcAarch64, frpcAarch64Digest = "example frpc for aarch64\n", "7992e23ff91c0ea95a1df3e1e9d0e6dff013e437bbc54526e457442f3dbde281"
	frpcOther, frpcOtherDigest     = "another frpc build, not the pinned one\n", "6b267f488087d0b4dbc0a23d5ff07203b72db51eb7343f2abb14ae660db12575"
	agentAarch64                   = "#!/bin/sh\n# an example agent build for aarch64\n"
	agentAarch64Digest             = "cc0ef8b166476e588430d8131eadf06a9a728f3005ee652ed371ddbecc106ff5"
)

// stageBuilds writes the two frpc fixtures into the agent-builds directory and
// pins their digests in front, as frp-pins.txt does for the real ones.
func stageBuilds(t *testing.T, builds string, front *FrontSettings) {
	t.Helper()
	for arch, content := range map[string]string{"x86_64": frpcX86, "aarch64": frpcAarch64} {
		if err := os.WriteFile(filepath.Join(builds, "frpc-"+arch), []byte(content), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	front.FrpcDigests = map[string]string{"x86_64": frpcX86Digest, "aarch64": frpcAarch64Digest}
}

// pinOfPEM is the pin of the first certificate in b, computed here: the SHA-256
// of its DER SubjectPublicKeyInfo.
func pinOfPEM(t *testing.T, b []byte) string {
	t.Helper()
	blk, _ := pem.Decode(b)
	if blk == nil {
		t.Fatal("no PEM block in the staged certificate")
	}
	c, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(c.RawSubjectPublicKeyInfo)
	return hex.EncodeToString(sum[:])
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// The frpc staged for an Allocation is the one of the partition's cpuArch: on
// an x86_64 partition the x86_64 build, on a GH200 partition (cpuArch aarch64)
// the aarch64 build, whose digest is another. Each is at frpc/<its digest>/frpc,
// its bytes hash to that digest, and the other architecture's is not staged.
func TestGH200PartitionStagesTheAarch64Frpc(t *testing.T) {
	for _, tc := range []struct {
		name, arch                string
		content, digest, otherSum string
		gh200                     bool
	}{
		{name: "an x86_64 partition", arch: "x86_64", content: frpcX86, digest: frpcX86Digest, otherSum: frpcAarch64Digest},
		{name: "a GH200 partition", arch: "aarch64", content: frpcAarch64, digest: frpcAarch64Digest, otherSum: frpcX86Digest, gh200: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, func(e *env) {
				stageBuilds(t, e.buildDir, e.opts.Front)
				e.profiles.set(func(v *slurm.Validated) { v.Profile.Partition.AcceleratorClass.CPUArch = tc.arch })
				if tc.gh200 {
					e.profiles.set(func(v *slurm.Validated) { v.Profile.Partition.AcceleratorClass.GPUModel = "GH200" })
					if err := os.WriteFile(filepath.Join(e.buildDir, agentapi.AgentBuildName("aarch64")), []byte(agentAarch64), 0o700); err != nil {
						t.Fatal(err)
					}
					e.digest = agentAarch64Digest
				}
			})
			a := e.newAlloc("a")
			e.run("a", 3)
			if n := e.count("sbatch"); n != 1 {
				t.Fatalf("%d sbatch calls, want 1 (reason %q)", n, e.get("a").Status.AtProvider.Reason)
			}
			files := e.batch.Files()
			f, ok := files["frpc/"+tc.digest+"/frpc"]
			if !ok {
				t.Fatalf("no file at frpc/%s/frpc; staged: %v", tc.digest, stagedNames(files))
			}
			if got := sha256Hex(f.Content); got != tc.digest || string(f.Content) != tc.content {
				t.Errorf("the staged frpc hashes to %s (content %q), want %s (content %q)", got, f.Content, tc.digest, tc.content)
			}
			if f.Mode.Perm() != 0o700 {
				t.Errorf("frpc staged with mode %o, want 700: it is run", f.Mode.Perm())
			}
			if _, ok := files["frpc/"+tc.otherSum+"/frpc"]; ok {
				t.Errorf("the other architecture's frpc, %s, was staged too", tc.otherSum)
			}
			var frpcs []string
			for _, p := range stagedNames(files) {
				if strings.HasPrefix(p, "frpc/") {
					frpcs = append(frpcs, p)
				}
			}
			if len(frpcs) != 1 {
				t.Errorf("frpc files staged: %v, want only the partition's", frpcs)
			}
			cfg, err := agentapi.DecodeAgentConfig(files["alloc/"+tagOf(a)+"/agent.json"].Content)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.FrpcDigest != tc.digest {
				t.Errorf("agent.json names frpc %s, want %s", cfg.FrpcDigest, tc.digest)
			}
		})
	}
}

func stagedNames(files map[string]provider.File) []string {
	var out []string
	for p := range files {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// The certificate is per Allocation: each gets its own alloc/<tag>/dial-home.crt,
// at its own tag's path, holding the launcher certificate's PEM unchanged. The
// pin of what was staged is computed here from the staged bytes, and equals the
// CertSPKISHA256 that the same Allocation's agent.json carries. Mode 644: the
// certificate is public (HBEP-4), and it is the mode of the launcher's own file.
func TestDialHomeCertificateIsStagedPerAllocation(t *testing.T) {
	for _, m := range ptModes {
		t.Run(m.name, func(t *testing.T) {
			e := newEnv(t, m.mods...)
			launcherPEM := e.opts.Front.CertPEM
			a, b := e.newAlloc("a"), e.newAlloc("b")
			e.run("a", 3)
			e.run("b", 3)
			if n := e.count("sbatch"); n != 2 {
				t.Fatalf("%d sbatch calls, want 2", n)
			}
			files := e.batch.Files()
			var certs []string
			for _, p := range stagedNames(files) {
				if strings.HasSuffix(p, "/dial-home.crt") {
					certs = append(certs, p)
				}
			}
			tagA, tagB := tagOf(a), tagOf(b)
			if tagA == tagB {
				t.Fatalf("both Allocations have the tag %s", tagA)
			}
			if want := []string{"alloc/" + tagA + "/dial-home.crt", "alloc/" + tagB + "/dial-home.crt"}; !equalSets(certs, want) {
				t.Fatalf("certificate files %v, want exactly %v", certs, want)
			}
			for _, tag := range []string{tagA, tagB} {
				crt := files["alloc/"+tag+"/dial-home.crt"]
				if !bytes.Equal(crt.Content, launcherPEM) {
					t.Errorf("%s: the staged certificate is not the launcher's PEM", tag)
				}
				if crt.Mode.Perm() != 0o644 {
					t.Errorf("%s: certificate mode %o, want 644", tag, crt.Mode.Perm())
				}
				cfg, err := agentapi.DecodeAgentConfig(files["alloc/"+tag+"/agent.json"].Content)
				if err != nil {
					t.Fatal(err)
				}
				if pin := pinOfPEM(t, crt.Content); pin != cfg.CertSPKISHA256 {
					t.Errorf("%s: the staged certificate's pin %s, agent.json's %s", tag, pin, cfg.CertSPKISHA256)
				}
			}
			// Deleting one Allocation removes its directory only.
			if err := e.c.Delete(e.ctx, e.get("a")); err != nil {
				t.Fatal(err)
			}
			e.run("a", 4)
			left := e.batch.Files()
			if _, ok := left["alloc/"+tagA+"/dial-home.crt"]; ok {
				t.Errorf("a's certificate stayed after a was deleted")
			}
			if _, ok := left["alloc/"+tagB+"/dial-home.crt"]; !ok {
				t.Errorf("b's certificate went with a's Allocation")
			}
		})
	}
}

func equalSets(got, want []string) bool {
	sort.Strings(got)
	sort.Strings(want)
	return strings.Join(got, "\n") == strings.Join(want, "\n")
}

// Every file is ensured before the claim, and the claim before the sbatch, in
// HBEP-4's order: the agent's binary, frpc, the credential, the certificate,
// then agent.json. The claim is the first write of the submission record, which
// an interceptor on the client sees; the files it saw staged by then are
// compared with the order written out here. A fresh Allocation in each mode, so
// the shared agent and frpc files are written (a repeat is not logged).
func TestEveryFileIsEnsuredBeforeTheClaimAndTheSbatch(t *testing.T) {
	for _, m := range ptModes {
		t.Run(m.name, func(t *testing.T) {
			var mu sync.Mutex
			var atClaim []fake.Call
			claimed := false
			e := newEnv(t, append(append([]envOption{}, m.mods...), func(e *env) {
				stageBuilds(t, e.buildDir, e.opts.Front)
				e.c = interceptor.NewClient(e.c.(client.WithWatch), interceptor.Funcs{
					Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
						mu.Lock()
						if !claimed && obj.GetAnnotations()[v1alpha1.AnnotationSubmission] != "" {
							claimed, atClaim = true, e.batch.Calls()
						}
						mu.Unlock()
						return w.Update(ctx, obj, opts...)
					},
				})
			})...)
			a := e.newAlloc("a")
			e.run("a", 3)
			tag := tagOf(a)
			want := []string{
				"agent/" + e.digest + "/hb-agent",
				"frpc/" + frpcX86Digest + "/frpc",
				"alloc/" + tag + "/credential",
				"alloc/" + tag + "/dial-home.crt",
				"alloc/" + tag + "/agent.json",
			}
			mu.Lock()
			defer mu.Unlock()
			if !claimed {
				t.Fatal("the submission record was never claimed")
			}
			var got []string
			for _, c := range atClaim {
				switch c.Op {
				case "ensure-file":
					got = append(got, c.Args[0])
				case "sbatch":
					t.Errorf("an sbatch before the claim")
				}
			}
			if strings.Join(got, "\n") != strings.Join(want, "\n") {
				t.Errorf("files ensured before the claim:\n%s\nwant, in this order:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
			}
			if n := e.count("sbatch"); n != 1 {
				t.Fatalf("%d sbatch calls, want 1", n)
			}
			last := -1
			for i, c := range e.batch.Calls() {
				if c.Op == "ensure-file" {
					last = i
				}
				if c.Op == "sbatch" && i < last {
					t.Errorf("the sbatch (call %d) came before an ensure-file (call %d)", i, last)
				}
			}
		})
	}
}

// stageReal pins the two fixtures in a realEnv's front and writes them into its
// agent-builds directory.
func stageReal(t *testing.T) realOption {
	return func(e *realEnv) { stageBuilds(t, e.builds, e.opts.Front) }
}

const realStaging = "/scratch/user-a/stage/"

// refusedBeforeAnyCall judges a submission that must stop in the prerequisites:
// the fake login node's log holds only the reconcile's own job listing (no
// path resolution, no file, no sbatch), the Allocation has no claim record, and
// its reason names each of want.
func refusedBeforeAnyCall(t *testing.T, e *realEnv, sl *fakeslurm.Platform, name string, want ...string) {
	t.Helper()
	for _, c := range sl.Calls() {
		if w := c.Words[0]; w != "squeue" && w != "sacct" {
			t.Errorf("a platform call %v, want none beyond the job listing (squeue, sacct)", c.Words)
		}
	}
	if n := sl.Count("sbatch"); n != 0 {
		t.Errorf("%d sbatch calls, want 0", n)
	}
	for p := range sl.Files() {
		if strings.HasPrefix(p, realStaging) {
			t.Errorf("file %s staged", p)
		}
	}
	a := e.get(name)
	if got := a.Annotations[v1alpha1.AnnotationSubmission]; got != "" {
		t.Errorf("a submission record %q was written, want no claim", got)
	}
	reason := a.Status.AtProvider.Reason
	for _, w := range want {
		if !strings.Contains(reason, w) {
			t.Errorf("reason %q does not name %q", reason, w)
		}
	}
}

// A frpc that is missing, or whose content has another digest than the pinned
// one for the partition's cpuArch, is refused before any call: no claim, no
// file, no sbatch (HBEP-4, Submission: Files first). The reason names the
// cpuArch and both digests. Each case is its own control: with the file as
// pinned, the same Allocation submits once.
func TestFrpcWithTheWrongDigestIsRefusedBeforeAnyClaim(t *testing.T) {
	for _, tc := range []struct {
		name   string
		break_ func(builds string)
		want   []string
	}{
		{"a frpc with another digest", func(builds string) {
			if err := os.WriteFile(filepath.Join(builds, "frpc-x86_64"), []byte(frpcOther), 0o700); err != nil {
				t.Fatal(err)
			}
		}, []string{"x86_64", frpcOtherDigest, frpcX86Digest}},
		{"no frpc for the partition's cpuArch, the other architecture's present", func(builds string) {
			if err := os.Remove(filepath.Join(builds, "frpc-x86_64")); err != nil {
				t.Fatal(err)
			}
		}, []string{"no frpc build for cpuArch x86_64"}},
		{"the other architecture's frpc under this one's name", func(builds string) {
			if err := os.WriteFile(filepath.Join(builds, "frpc-x86_64"), []byte(frpcAarch64), 0o700); err != nil {
				t.Fatal(err)
			}
		}, []string{"x86_64", frpcAarch64Digest, frpcX86Digest}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := fake.NewClock(at(12, 0, 0))
			sl := newSlurm(clock, fakeslurm.Config{})
			e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", stageReal(t))
			tc.break_(e.builds)
			e.newAlloc("alloc-a", 1)
			e.run("alloc-a", 3)
			refusedBeforeAnyCall(t, e, sl, "alloc-a", tc.want...)

			stageBuilds(t, e.builds, e.opts.Front)
			e.run("alloc-a", 4)
			if n := sl.Count("sbatch"); n != 1 {
				t.Errorf("%d sbatch calls with the frpc as pinned, want 1", n)
			}
		})
	}
}

// A certificate that is not the front's is refused the same way: none at all, a
// pin that is not CertPin (another launcher's certificate), a file of two
// certificates (the agent refuses it too), and text that is no certificate.
func TestACertificateThatIsNotTheFrontsIsRefusedBeforeAnyClaim(t *testing.T) {
	otherPEM, otherPin := ctltest.Cert(t)
	for _, tc := range []struct {
		name string
		pem  func(good []byte) []byte
		want []string
	}{
		{"no certificate", func([]byte) []byte { return nil }, []string{"no dial-home certificate"}},
		{"a certificate with another pin", func([]byte) []byte { return otherPEM }, []string{"pin " + otherPin}},
		{"two certificates", func(good []byte) []byte { return append(append([]byte{}, good...), otherPEM...) }, []string{"exactly one PEM certificate"}},
		{"text that is no certificate", func([]byte) []byte { return []byte("not a certificate\n") }, []string{"not a PEM certificate"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := fake.NewClock(at(12, 0, 0))
			sl := newSlurm(clock, fakeslurm.Config{})
			e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
			good := e.opts.Front.CertPEM
			e.opts.Front.CertPEM = tc.pem(good)
			e.newAlloc("alloc-a", 1)
			e.run("alloc-a", 3)
			refusedBeforeAnyCall(t, e, sl, "alloc-a", tc.want...)

			e.opts.Front.CertPEM = good
			e.run("alloc-a", 4)
			if n := sl.Count("sbatch"); n != 1 {
				t.Errorf("%d sbatch calls with the front's certificate, want 1", n)
			}
		})
	}
}

// A file already on the cluster at the frpc path, with other content, is
// replaced before the claim, so the job never runs on it. This is EnsureFile's
// contract (HBEP-4, Core operations: it writes unless the file there already has
// the SHA-256), and Files first is where it is called. The real slurm.Batch is
// under test, over the fake Slurm: the planted file's digest is not the pinned
// one before the run, and at the moment of the claim, which an interceptor on
// the client sees, the file already has the pinned digest and no sbatch has run.
func TestAFrpcOnTheClusterWithOtherContentIsReplacedBeforeTheClaim(t *testing.T) {
	var mu sync.Mutex
	var sumAtClaim string
	var sbatchAtClaim = -1
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	path := realStaging + "frpc/" + frpcX86Digest + "/frpc"
	c := interceptor.NewClient(newFakeClient(t, "prov-a").(client.WithWatch), interceptor.Funcs{
		Update: func(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			mu.Lock()
			if sbatchAtClaim < 0 && obj.GetAnnotations()[v1alpha1.AnnotationSubmission] != "" {
				sumAtClaim, sbatchAtClaim = sha256Hex(sl.Files()[path].Content), sl.Count("sbatch")
			}
			mu.Unlock()
			return w.Update(ctx, obj, opts...)
		},
	})
	e := newRealEnv(t, clock, sl, c, "prov-a", "user-a", stageReal(t))
	if err := sl.WriteFile(path, []byte(frpcOther), 0o755); err != nil {
		t.Fatal(err)
	}
	if got := sha256Hex(sl.Files()[path].Content); got != frpcOtherDigest {
		t.Fatalf("precondition: the planted file hashes to %s, want %s", got, frpcOtherDigest)
	}
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 3)

	mu.Lock()
	defer mu.Unlock()
	if sbatchAtClaim < 0 {
		t.Fatalf("the submission was never claimed (reason %q)", e.get("alloc-a").Status.AtProvider.Reason)
	}
	if sumAtClaim != frpcX86Digest || sbatchAtClaim != 0 {
		t.Errorf("at the claim the file at the frpc path hashed to %q with %d sbatch calls run, want %s and 0", sumAtClaim, sbatchAtClaim, frpcX86Digest)
	}
	f := sl.Files()[path]
	if string(f.Content) != frpcX86 || f.Mode.Perm() != 0o700 {
		t.Errorf("the file at the frpc path is %q mode %o after the run, want the pinned frpc, mode 700", f.Content, f.Mode.Perm())
	}
	if n := sl.Count("sbatch"); n != 1 {
		t.Errorf("%d sbatch calls, want 1", n)
	}
}

// CheckCertificate, directly: what it accepts and each way it refuses.
func TestCheckCertificate(t *testing.T) {
	good, pin := ctltest.Cert(t)
	other, _ := ctltest.Cert(t)
	for _, tc := range []struct {
		name string
		f    FrontSettings
		want string // "" accepts
	}{
		{"one certificate with its pin", FrontSettings{CertPEM: good, CertPin: pin}, ""},
		{"surrounding white space", FrontSettings{CertPEM: append(append([]byte("\n"), good...), "\n\n"...), CertPin: pin}, ""},
		{"no certificate", FrontSettings{CertPin: pin}, "no dial-home certificate"},
		{"another pin", FrontSettings{CertPEM: good, CertPin: strings.Repeat("0", 64)}, "is not the front's certificate pin " + strings.Repeat("0", 64)},
		{"no pin", FrontSettings{CertPEM: good}, "is not the front's certificate pin"},
		{"a second certificate", FrontSettings{CertPEM: append(append([]byte{}, good...), other...), CertPin: pin}, "exactly one"},
		{"text after the certificate", FrontSettings{CertPEM: append(append([]byte{}, good...), "tail\n"...), CertPin: pin}, "exactly one"},
		{"a PEM header", FrontSettings{CertPEM: bytes.Replace(good, []byte("-----\n"), []byte("-----\nProc-Type: 4,ENCRYPTED\n"), 1), CertPin: pin}, "exactly one"},
		{"another PEM type", FrontSettings{CertPEM: bytes.ReplaceAll(good, []byte("CERTIFICATE"), []byte("PRIVATE KEY")), CertPin: pin}, "not a PEM certificate"},
		{"a block that is no certificate", FrontSettings{CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("junk")}), CertPin: pin}, "the dial-home certificate:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.f.CheckCertificate()
			switch {
			case tc.want == "" && err != nil:
				t.Errorf("refused: %v", err)
			case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
				t.Errorf("error %v, want one containing %q", err, tc.want)
			}
		})
	}
}

// find keeps a file's digest while its size and modification time stay the same,
// so a frpc rewritten in place with the same length and time passes it. What is
// staged is read again, with the digest, and a file with other bytes is refused
// there: the bytes that reach the cluster always have the pinned digest.
func TestAFrpcRewrittenBehindTheCacheIsRefusedWhenRead(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "frpc-x86_64")
	if err := os.WriteFile(p, []byte(frpcX86), 0o700); err != nil {
		t.Fatal(err)
	}
	var c buildCache
	if _, err := c.find(frpcBuild, dir, "x86_64", frpcX86Digest); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(strings.Repeat("x", len(frpcX86))), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(p, fi.ModTime(), fi.ModTime()); err != nil {
		t.Fatal(err)
	}
	if _, err := c.find(frpcBuild, dir, "x86_64", frpcX86Digest); err != nil {
		t.Fatalf("precondition: find's cache should not see a rewrite with the same size and time: %v", err)
	}
	if b, err := c.read(frpcBuild, dir, "x86_64", frpcX86Digest); err == nil || !strings.Contains(err.Error(), "has digest") {
		t.Errorf("read returned %q, %v; want a refusal that names the digests", b, err)
	}
}
