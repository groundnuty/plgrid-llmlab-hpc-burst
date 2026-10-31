package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
// 1.36.5, and checks that every object is back with the same UID and creation time. Then it changes
// the store, stops the host, restores the upgrade's backup, and checks that the objects are back as
// they were before the upgrade and the later changes are gone.
func TestUpgrade(t *testing.T) {
	const base = 29220
	h := newHost(t)
	keyFile := filepath.Join(filepath.Dir(h.dir), "restricted_key")
	if err := os.WriteFile(keyFile, []byte("a stand-in for the restricted key; never read"), 0o600); err != nil {
		t.Fatal(err)
	}

	ctlBin, _ := fakeController(t)
	r := cli(t, h.upArgs(base, "--version", "1.35.9", "--json",
		"--front-mode", "ssh", "--front-addr", "front.example.test", "--front-key", keyFile,
		"--manifests", "testdata/manifests", "--controller-bin", ctlBin)...)
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
	// The controllers run beside the old host: remember them, to see them replaced.
	oldProcs := recordedProcs(t, h.dir)
	oldRec, ok := readSupervisorRecord(t, h.dir)
	if len(oldProcs) != 2 || oldProcs[1].Name != "supervisor" || !ok || oldRec.Entries[0].Proc == nil {
		t.Fatalf("before the upgrade: %+v, %+v", oldProcs, oldRec)
	}
	pairBefore := readEndpointPair(t, h.dir) // created by up, for the controllers
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
	// The upgrade stopped the supervisor with the old host and started it again with the new one:
	// the old supervisor and controller are gone, and new ones run.
	newProcs := recordedProcs(t, h.dir)
	if len(newProcs) != 2 || newProcs[1].Name != "supervisor" || newProcs[1].PID == oldProcs[1].PID || !running(newProcs[1].PID) {
		t.Errorf("after the upgrade the record is %+v: the supervisor was not started again", newProcs)
	} else {
		newRec, ok := readSupervisorRecord(t, h.dir)
		if !ok || newRec.Entries[0].Proc == nil || newRec.Entries[0].Proc.PID == oldRec.Entries[0].Proc.PID ||
			!running(newRec.Entries[0].Proc.PID) || ppidOfPID(t, newRec.Entries[0].Proc.PID) != newProcs[1].PID {
			t.Errorf("after the upgrade supervisor.json is %+v (before: %+v)", newRec, oldRec)
		}
	}
	if running(oldProcs[1].PID) || running(oldRec.Entries[0].Proc.PID) {
		t.Errorf("the old supervisor (pid %d) or controller (pid %d) still runs", oldProcs[1].PID, oldRec.Entries[0].Proc.PID)
	}
	// The agent endpoint's pair is kept: the agents' pin must not change under a running platform.
	if after := readEndpointPair(t, h.dir); after.pin != pairBefore.pin || !bytes.Equal(after.cert, pairBefore.cert) || !bytes.Equal(after.key, pairBefore.key) {
		t.Errorf("the agent endpoint's pair changed across the upgrade: pin %s -> %s", pairBefore.pin, after.pin)
	}
	c2 := h.clients()
	if v, err := c2.cs.Discovery().ServerVersion(); err != nil || !strings.HasPrefix(v.GitVersion, "v1.36.5") {
		t.Fatalf("server version after: %v, %v", v, err)
	}
	kept := checkObjects(ctx, t, c2, objs)
	t.Logf("kept %d of %d objects the test created, by UID and creation time (independent of the launcher's count)", kept, len(objs))
	if kept != len(objs) {
		t.Errorf("kept %d of %d", kept, len(objs))
	}

	// The backup holds the store and the server token as the old release left them, mode 600, and
	// backup.json names that release.
	for _, f := range []string{"db/state.db", "token"} {
		fi, err := os.Stat(filepath.Join(up.Backup, f))
		if err != nil || !fi.Mode().IsRegular() || fi.Size() == 0 || fi.Mode().Perm() != 0o600 {
			t.Errorf("backup %s: %v, %v (want a non-empty regular file, mode 600)", f, fi, err)
		}
	}
	var info struct {
		Version string   `json:"version"`
		TakenAt string   `json:"taken_at"`
		Files   []string `json:"files"`
	}
	if b, err := os.ReadFile(filepath.Join(up.Backup, "backup.json")); err != nil {
		t.Errorf("backup.json: %v", err)
	} else {
		parseJSON(t, string(b), &info)
		if info.Version != "v1.35.9+k3s1" || info.TakenAt == "" {
			t.Errorf("backup.json: %+v, want the release v1.35.9+k3s1 and a time", info)
		}
	}
	if !strings.Contains(up.Backup, "v1.35.9+k3s1") {
		t.Errorf("the backup name does not say what it holds: %s", up.Backup)
	}
	// And the upgraded host is healthy by status.
	if r := cli(t, h.args("status")...); r.code != exitOK {
		t.Errorf("status after the upgrade: exit %d: %s", r.code, r.out)
	}

	// ---- changes after the upgrade, then the way down: restore ----
	// The store goes back to the backup, so what the test does now must be gone afterwards: the
	// first checks below prove the changes landed, or "gone" would be true for nothing.
	ctx2, cancel2 := testCtx()
	defer cancel2()
	cms := c2.cs.CoreV1().ConfigMaps("pop")
	if _, err := cms.Create(ctx2, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "after-upgrade"}, Data: map[string]string{"i": "after"}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := cms.Delete(ctx2, "cm-000", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	cm1, err := cms.Get(ctx2, "cm-001", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cm1.Data = map[string]string{"i": "changed after the upgrade"}
	if _, err := cms.Update(ctx2, cm1, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := cms.Get(ctx2, "after-upgrade", metav1.GetOptions{}); err != nil {
		t.Fatalf("after-upgrade was not created: %v", err)
	}
	if _, err := cms.Get(ctx2, "cm-000", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("cm-000 was not deleted: %v", err)
	}
	if got, err := cms.Get(ctx2, "cm-001", metav1.GetOptions{}); err != nil || got.Data["i"] != "changed after the upgrade" {
		t.Fatalf("cm-001 was not updated: %v, %v", got, err)
	}

	// restore needs the host stopped.
	if r := cli(t, h.args("restore", "--backup", up.Backup)...); r.code != exitFail || !running(newPID) {
		t.Errorf("restore on a running host: exit %d, k3s running: %v", r.code, running(newPID))
	}
	if r := cli(t, h.args("down")...); r.code != exitOK {
		t.Fatalf("down exited %d", r.code)
	}
	assertNothingStarted(t, h)

	var restored struct {
		Version  string `json:"version"`
		Backup   string `json:"backup"`
		Replaced string `json:"replaced"`
	}
	r = cli(t, h.args("restore", "--backup", up.Backup, "--json")...)
	if r.code != exitOK {
		t.Fatalf("restore exited %d", r.code)
	}
	parseJSON(t, r.out, &restored)
	if restored.Version != "v1.35.9+k3s1" || filepath.Clean(restored.Backup) != filepath.Clean(up.Backup) {
		t.Errorf("restore reported %+v", restored)
	}
	// The store it replaced is moved aside, not deleted.
	if restored.Replaced == "" || filepath.Dir(restored.Replaced) != filepath.Join(h.dir, "data", "server") {
		t.Errorf("the replaced store is at %q, want a directory under %s", restored.Replaced, filepath.Join(h.dir, "data", "server"))
	} else if fi, err := os.Stat(filepath.Join(restored.Replaced, "db", "state.db")); err != nil || fi.Size() == 0 {
		t.Errorf("the replaced store was not kept: %v, %v", fi, err)
	}
	assertStoreVersion(t, filepath.Join(h.dir, "store-version.json"), "v1.35.9+k3s1")

	// The store is now 1.35.9's: 1.36.5 is only reached through upgrade again.
	r = cli(t, h.upArgs(base, "--version", "1.36.5")...)
	if r.code != exitFail || !strings.Contains(r.err, "hpcburst upgrade") {
		t.Errorf("up --version 1.36.5 after the restore: exit %d, stderr %q; want exit 1 naming hpcburst upgrade", r.code, r.err)
	}
	assertNothingStarted(t, h)

	// A plain up starts the release the store was restored to.
	if r := cli(t, h.upArgs(base)...); r.code != exitOK {
		t.Fatalf("up after the restore exited %d", r.code)
	}
	assertRunning(t, h, "v1.35.9+k3s1")

	// Judged from the test's own record: every object from before the upgrade is back with the
	// UID and creation time its create response gave, so cm-000 is the original, not a new one.
	c3 := h.clients()
	kept = checkObjects(ctx2, t, c3, objs)
	t.Logf("after the restore: %d of %d recorded objects are back with their UID and creation time", kept, len(objs))
	if kept != len(objs) {
		t.Errorf("after the restore only %d of %d recorded objects match", kept, len(objs))
	}
	cms = c3.cs.CoreV1().ConfigMaps("pop")
	if got, err := cms.Get(ctx2, "cm-001", metav1.GetOptions{}); err != nil || got.Data["i"] != "1" {
		t.Errorf("cm-001 after the restore: %v, %v; want its original data i=1", got, err)
	}
	if _, err := cms.Get(ctx2, "cm-000", metav1.GetOptions{}); err != nil {
		t.Errorf("cm-000 is not back after the restore: %v", err)
	}
	if _, err := cms.Get(ctx2, "after-upgrade", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("after-upgrade survived the restore (%v): the store was not put back", err)
	}
}

// checkObjects asks the API server for every recorded object and compares UID and creation time
// with what the create response said. It reports each difference and returns how many matched.
func checkObjects(ctx context.Context, t *testing.T, c clients, objs []created) int {
	t.Helper()
	kept := 0
	for _, o := range objs {
		var uid types.UID
		var ts metav1.Time
		switch o.what {
		case "configmap":
			got, err := c.cs.CoreV1().ConfigMaps(o.ns).Get(ctx, o.name, metav1.GetOptions{})
			if err != nil {
				t.Errorf("%s %s/%s: %v", o.what, o.ns, o.name, err)
				continue
			}
			uid, ts = got.UID, got.CreationTimestamp
			if got.Data["i"] == "" {
				t.Errorf("configmap %s lost its data", o.name)
			}
		case "namespace":
			got, err := c.cs.CoreV1().Namespaces().Get(ctx, o.name, metav1.GetOptions{})
			if err != nil {
				t.Errorf("%s %s: %v", o.what, o.name, err)
				continue
			}
			uid, ts = got.UID, got.CreationTimestamp
		case "widget":
			got, err := c.dyn.Resource(widgetGVR).Namespace(o.ns).Get(ctx, o.name, metav1.GetOptions{})
			if err != nil {
				t.Errorf("%s %s/%s: %v", o.what, o.ns, o.name, err)
				continue
			}
			uid, ts = got.GetUID(), got.GetCreationTimestamp()
			if _, found, _ := unstructured.NestedInt64(got.Object, "spec", "size"); !found {
				t.Errorf("widget %s lost its spec", o.name)
			}
		case "crd":
			got, err := c.dyn.Resource(schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}).Get(ctx, o.name, metav1.GetOptions{})
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
	return kept
}
