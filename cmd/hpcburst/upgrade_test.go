package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

var widgetGVR = schema.GroupVersionResource{Group: "fixture.example.test", Version: "v1", Resource: "widgets"}

// created is an object the test made, with the identity the API server gave it in the create
// response. After the upgrade the test asks for each object again and compares against this
// record, not against anything the launcher computed.
type created struct {
	what, ns, name string
	uid            types.UID
	created        metav1.Time
}

// TestUpgrade starts k3s 1.35.9 with manifests and a front recorded, loads objects, upgrades to
// 1.36.5, and checks that every object is back with the same UID and creation time.
func TestUpgrade(t *testing.T) {
	const base = 29220
	h := newHost(t)
	keyFile := filepath.Join(filepath.Dir(h.dir), "restricted_key")
	if err := os.WriteFile(keyFile, []byte("a stand-in for the restricted key; never read"), 0o600); err != nil {
		t.Fatal(err)
	}

	r := cli(t, h.upArgs(base, "--version", "1.35.9", "--json",
		"--front-mode", "ssh", "--front-addr", "front.example.test", "--front-key", keyFile,
		"--manifests", "testdata/manifests")...)
	if r.code != exitOK {
		t.Fatalf("up exited %d", r.code)
	}
	if strings.Contains(r.err, "warning: no front") {
		t.Errorf("a front was given, yet up warns: %q", r.err)
	}
	st := readState(t, h.dir)
	if st.Version != "v1.35.9+k3s1" || st.Front.Mode != "ssh" || st.Front.Addr != "front.example.test:22" || st.Front.KeyPath != keyFile {
		t.Errorf("recorded: %+v", st)
	}
	oldPID := st.Processes[0].PID
	c := h.clients()
	ctx, cancel := testCtx()
	defer cancel()
	if v, err := c.cs.Discovery().ServerVersion(); err != nil || !strings.HasPrefix(v.GitVersion, "v1.35.9") {
		t.Fatalf("server version before: %v, %v", v, err)
	}

	// The manifests hook: the CRD is Established and the CR (which sorts before the CRD) exists.
	var objs []created
	remember := func(what, ns, name string, uid types.UID, ts metav1.Time) {
		objs = append(objs, created{what, ns, name, uid, ts})
	}
	w, err := c.dyn.Resource(widgetGVR).Namespace("default").Get(ctx, "first-widget", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("the CR from the manifests is missing: %v", err)
	}
	remember("widget", "default", "first-widget", w.GetUID(), w.GetCreationTimestamp())
	ns, err := c.cs.CoreV1().Namespaces().Get(ctx, "fixture-ns", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("the Namespace from the manifests is missing: %v", err)
	}
	remember("namespace", "", "fixture-ns", ns.UID, ns.CreationTimestamp)

	// The population: a namespace, 100 ConfigMaps, 3 more custom objects.
	pop, err := c.cs.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "pop"}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	remember("namespace", "", "pop", pop.UID, pop.CreationTimestamp)
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("cm-%03d", i)
		cm, err := c.cs.CoreV1().ConfigMaps("pop").Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name}, Data: map[string]string{"i": fmt.Sprint(i)}}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		remember("configmap", "pop", name, cm.UID, cm.CreationTimestamp)
	}
	for i := 1; i <= 3; i++ {
		o := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "fixture.example.test/v1", "kind": "Widget",
			"metadata": map[string]any{"name": fmt.Sprintf("w-%d", i), "namespace": "pop"},
			"spec":     map[string]any{"size": int64(i)},
		}}
		got, err := c.dyn.Resource(widgetGVR).Namespace("pop").Create(ctx, o, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		remember("widget", "pop", got.GetName(), got.GetUID(), got.GetCreationTimestamp())
	}
	crd, err := c.dyn.Resource(schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}).
		Get(ctx, "widgets.fixture.example.test", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	remember("crd", "", crd.GetName(), crd.GetUID(), crd.GetCreationTimestamp())
	t.Logf("created or recorded %d objects before the upgrade", len(objs))

	// ---- refusals leave the host alone ----
	for _, to := range []string{"1.35.9", "1.34.1", "1.99.0", "not-a-version"} {
		if r := cli(t, h.args("upgrade", "--bin-dir", h.binDir, "--to", to)...); r.code != exitFail {
			t.Errorf("upgrade --to %s: exit %d, want 1", to, r.code)
		}
	}
	if !running(oldPID) || readState(t, h.dir).Processes[0].PID != oldPID {
		t.Fatal("a refused upgrade changed the host")
	}

	// ---- the upgrade ----
	var up struct {
		From, To     string
		Backup       string `json:"backup"`
		Verification struct {
			Before  int      `json:"objects_before"`
			After   int      `json:"objects_after"`
			Kept    int      `json:"objects_kept"`
			Added   int      `json:"objects_added"`
			Missing []string `json:"missing"`
			Changed []string `json:"changed"`
		} `json:"verification"`
		StopMS      int64   `json:"stop_ms"`
		ReadyMS     int64   `json:"ready_ms"`
		StopToReady int64   `json:"stop_to_ready_ms"`
		RSSMiB      float64 `json:"rss_mib"`
	}
	r = cli(t, h.args("upgrade", "--bin-dir", h.binDir, "--to", "1.36.5", "--json")...)
	if r.code != exitOK {
		t.Fatalf("upgrade exited %d", r.code)
	}
	parseJSON(t, r.out, &up)
	v := up.Verification
	t.Logf("NUMBERS upgrade: stop %d ms, new host ready %d ms, stop to ready %d ms, RSS %.0f MiB; the launcher's own count: kept %d of %d (%d added)",
		up.StopMS, up.ReadyMS, up.StopToReady, up.RSSMiB, v.Kept, v.Before, v.Added)
	if v.Kept != v.Before || v.Before < len(objs) || len(v.Missing) != 0 || len(v.Changed) != 0 {
		t.Errorf("the launcher's verification: %+v", v)
	}

	// ---- judged from the test's own record ----
	if running(oldPID) {
		t.Errorf("the old k3s (pid %d) still runs", oldPID)
	}
	st2 := readState(t, h.dir)
	newPID := st2.Processes[0].PID
	if newPID == oldPID || !running(newPID) || st2.Version != "v1.36.5+k3s1" {
		t.Errorf("after the upgrade: %+v", st2)
	}
	if st2.Front != st.Front {
		t.Errorf("the front changed across the upgrade: %+v -> %+v", st.Front, st2.Front)
	}
	if got := listenersOf(t, newPID); len(got) != 4 {
		t.Errorf("the new k3s holds %v", got)
	}
	c2 := h.clients()
	if v, err := c2.cs.Discovery().ServerVersion(); err != nil || !strings.HasPrefix(v.GitVersion, "v1.36.5") {
		t.Fatalf("server version after: %v, %v", v, err)
	}
	kept := 0
	for _, o := range objs {
		var uid types.UID
		var ts metav1.Time
		switch o.what {
		case "configmap":
			got, err := c2.cs.CoreV1().ConfigMaps(o.ns).Get(ctx, o.name, metav1.GetOptions{})
			if err != nil {
				t.Errorf("%s %s/%s: %v", o.what, o.ns, o.name, err)
				continue
			}
			uid, ts = got.UID, got.CreationTimestamp
			if got.Data["i"] == "" {
				t.Errorf("configmap %s lost its data", o.name)
			}
		case "namespace":
			got, err := c2.cs.CoreV1().Namespaces().Get(ctx, o.name, metav1.GetOptions{})
			if err != nil {
				t.Errorf("%s %s: %v", o.what, o.name, err)
				continue
			}
			uid, ts = got.UID, got.CreationTimestamp
		case "widget":
			got, err := c2.dyn.Resource(widgetGVR).Namespace(o.ns).Get(ctx, o.name, metav1.GetOptions{})
			if err != nil {
				t.Errorf("%s %s/%s: %v", o.what, o.ns, o.name, err)
				continue
			}
			uid, ts = got.GetUID(), got.GetCreationTimestamp()
			if _, found, _ := unstructured.NestedInt64(got.Object, "spec", "size"); !found {
				t.Errorf("widget %s lost its spec", o.name)
			}
		case "crd":
			got, err := c2.dyn.Resource(schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}).Get(ctx, o.name, metav1.GetOptions{})
			if err != nil {
				t.Errorf("crd %s: %v", o.name, err)
				continue
			}
			uid, ts = got.GetUID(), got.GetCreationTimestamp()
		}
		if uid != o.uid || !ts.Equal(&o.created) {
			t.Errorf("%s %s/%s: uid %s -> %s, created %s -> %s", o.what, o.ns, o.name, o.uid, uid, o.created, ts)
			continue
		}
		kept++
	}
	t.Logf("kept %d of %d objects the test created, by UID and creation time (independent of the launcher's count)", kept, len(objs))
	if kept != len(objs) {
		t.Errorf("kept %d of %d", kept, len(objs))
	}

	// The backup holds the SQLite store as it was before.
	if fi, err := os.Stat(filepath.Join(up.Backup, "state.db")); err != nil || fi.Size() == 0 {
		t.Errorf("backup: %v, %v", fi, err)
	}
	if !strings.Contains(up.Backup, "v1.35.9+k3s1") {
		t.Errorf("the backup name does not say what it holds: %s", up.Backup)
	}
	// And the upgraded host is healthy by status.
	if r := cli(t, h.args("status")...); r.code != exitOK {
		t.Errorf("status after the upgrade: exit %d: %s", r.code, r.out)
	}
}
