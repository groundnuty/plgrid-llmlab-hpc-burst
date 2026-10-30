package launcher

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// k3sBinDir is the directory of pinned k3s binaries ($HPCBURST_BIN_DIR, laid out by
// hack/fetch-k3s.sh). Without it the test skips, or fails under HPCBURST_REQUIRE_INTEGRATION=1.
func k3sBinDir(t *testing.T) string {
	t.Helper()
	dir := os.Getenv("HPCBURST_BIN_DIR")
	if dir == "" {
		skipOrFail(t, "HPCBURST_BIN_DIR is not set; this test starts a real k3s")
	}
	if _, err := os.Stat(filepath.Join(dir, "k3s-1.36.5", "k3s")); err != nil {
		skipOrFail(t, "no pinned k3s binary: "+err.Error())
	}
	return dir
}

// rootK3sListening lists which of the system k3s's ports (6443, 6444, 10250) listen, from ss(8).
func rootK3sListening(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("ss", "-ltnH").Output()
	if err != nil {
		t.Fatalf("ss: %v", err)
	}
	var have []string
	for _, p := range []string{"6443", "6444", "10250"} {
		for _, l := range strings.Split(string(out), "\n") {
			if f := strings.Fields(l); len(f) >= 4 && strings.HasSuffix(f[3], ":"+p) {
				have = append(have, p)
				break
			}
		}
	}
	return strings.Join(have, ",")
}

type testLog struct{ t *testing.T }

func (w testLog) Write(b []byte) (int, error) {
	w.t.Log(strings.TrimRight(string(b), "\n"))
	return len(b), nil
}

// cascades reports whether this host's garbage collector removes a dependent within d, checked
// with client-go alone, independently of the launcher's probe.
func cascades(t *testing.T, cs *kubernetes.Clientset, d time.Duration) bool {
	t.Helper()
	ctx := context.Background()
	cms := cs.CoreV1().ConfigMaps("default")
	owner, err := cms.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{GenerateName: "fixture-owner-"}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dep, err := cms.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{GenerateName: "fixture-dep-",
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "ConfigMap", Name: owner.Name, UID: owner.UID}}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	bg := metav1.DeletePropagationBackground
	if err := cms.Delete(ctx, owner.Name, metav1.DeleteOptions{PropagationPolicy: &bg}); err != nil {
		t.Fatal(err)
	}
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(200 * time.Millisecond) {
		if _, err := cms.Get(ctx, dep.Name, metav1.GetOptions{}); apierrors.IsNotFound(err) {
			return true
		}
	}
	_ = cms.Delete(ctx, dep.Name, metav1.DeleteOptions{})
	return false
}

// TestStatusGCProbeOnRealHosts starts the pinned k3s twice: as the launcher starts it, and with the
// controller manager's garbage collector switched off. HBEP-11 (Operational readiness 1) wants a
// host that serves the API but collects no garbage to be reported unhealthy. Each fixture is first
// shown, with client-go alone, to collect or not to collect; then status must say the same.
func TestStatusGCProbeOnRealHosts(t *testing.T) {
	bins := k3sBinDir(t)
	if gcProbeTimeout != time.Minute {
		t.Fatalf("the probe allows %s; HBEP-11 says within a minute", gcProbeTimeout)
	}
	for _, c := range []struct {
		name  string
		extra []string
		gc    bool
	}{
		{"as launched", nil, true},
		{"garbage collector off", []string{"--kube-controller-manager-arg=enable-garbage-collector=false"}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			before := rootK3sListening(t)
			if os.Getenv("HPCBURST_TEST_ROOT_K3S") == "1" && before != "6443,6444,10250" {
				t.Fatalf("HPCBURST_TEST_ROOT_K3S=1, but the root k3s listens only on %q", before)
			}
			t.Cleanup(func() {
				after := rootK3sListening(t)
				t.Logf("root k3s check after the test: listens on %s (before: %s)", after, before)
				if after != before {
					t.Errorf("the root k3s's listeners changed: %q -> %q", before, after)
				}
			})
			l, err := newLayout(filepath.Join(shortDir(t), "st"))
			if err != nil {
				t.Fatal(err)
			}
			bin, err := ResolveBinary(bins, DefaultVersion)
			if err != nil {
				t.Fatal(err)
			}
			defer bin.Close()
			ports, err := PortsFromBase(29220)
			if err != nil {
				t.Fatal(err)
			}
			if err := checkPortsFree(ports.All()); err != nil {
				t.Fatal(err)
			}
			spec := launchSpec{l: l, bin: bin, ports: ports, nice: 10, extraArgs: c.extra}
			st, _, _, err := startAndWait(context.Background(), spec, Front{}, 2*time.Minute, testLog{t})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if _, err := stopRecorded(context.Background(), st, 20*time.Second, testLog{t}); err != nil {
					t.Errorf("stop: %v", err)
				}
			})
			cfg, err := clientcmd.BuildConfigFromFlags("", l.kubeconfig())
			if err != nil {
				t.Fatal(err)
			}
			cs, err := kubernetes.NewForConfig(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if got := cascades(t, cs, 30*time.Second); got != c.gc {
				t.Fatalf("the fixture collects garbage: %v, want %v; the test would prove nothing", got, c.gc)
			}
			if !c.gc {
				gcProbeTimeout = 15 * time.Second // the fixture is known not to collect; no need to wait the full minute
				defer func() { gcProbeTimeout = time.Minute }()
			}
			rep, err := Status(context.Background(), StatusOptions{StateDir: l.dir})
			if err != nil {
				t.Fatal(err)
			}
			reasons := strings.Join(rep.Reasons, ";")
			t.Logf("status: %s, gc_ok=%v, executable %s, reasons %v", rep.Health, ptrStr(rep.GC), rep.Executable, rep.Reasons)
			if c.gc {
				if rep.Health != Healthy || rep.GC == nil || !*rep.GC {
					t.Fatalf("a collecting host: %s, gc_ok=%v, %s", rep.Health, ptrStr(rep.GC), reasons)
				}
			} else if rep.Health != Unhealthy || rep.GC == nil || *rep.GC || !strings.Contains(reasons, "garbage collector is not working") {
				t.Fatalf("a host that collects nothing: %s, gc_ok=%v, %s", rep.Health, ptrStr(rep.GC), reasons)
			}
			// The probe leaves nothing of its own behind but its namespace (the controller manager's
			// kube-root-ca.crt there is not the probe's).
			left, err := cs.CoreV1().ConfigMaps(gcProbeNamespace).List(context.Background(), metav1.ListOptions{LabelSelector: "app.kubernetes.io/managed-by=" + fieldManager})
			if err != nil || len(left.Items) != 0 {
				t.Errorf("the probe left %v behind (%v)", left, err)
			}
		})
	}
}

func ptrStr(b *bool) string {
	if b == nil {
		return "unset"
	}
	return fmt.Sprint(*b)
}
