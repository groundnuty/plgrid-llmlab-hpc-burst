package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestUpStatusDown starts the default pinned k3s, checks what it is and how it is isolated,
// checks status, exercises the identity guard on the real process, and stops it.
func TestUpStatusDown(t *testing.T) {
	const base = 29220
	h := newHost(t)

	// ---- up ----
	var up struct {
		Version    string  `json:"version"`
		PID        int     `json:"pid"`
		Kubeconfig string  `json:"kubeconfig"`
		ReadyzMS   int64   `json:"readyz_ms"`
		ReadyMS    int64   `json:"ready_ms"`
		RSSMiB     float64 `json:"rss_mib"`
	}
	r := cli(t, h.upArgs(base, "--json")...)
	if r.code != exitOK {
		t.Fatalf("up exited %d", r.code)
	}
	parseJSON(t, r.out, &up)
	if !strings.Contains(r.err, "warning: no front recorded") || !strings.Contains(r.err, "no jobs will be submitted until a front is configured") {
		t.Errorf("up with no front must warn that no jobs will be submitted until a front is configured (@hpc-burst-science-agent, 30 Oct 2026); stderr: %q", r.err)
	}
	t.Logf("NUMBERS cold start: /readyz after %d ms, controller manager after %d ms, RSS %.0f MiB", up.ReadyzMS, up.ReadyMS, up.RSSMiB)
	if up.Version != "v1.36.5+k3s1" {
		t.Errorf("the default version is %s", up.Version)
	}
	st := readState(t, h.dir)
	pid := st.Processes[0].PID
	if pid != up.PID || !running(pid) || startTicks(t, pid) != st.Processes[0].StartTicks {
		t.Fatalf("the recorded process is not the running one: record %+v, up said %d", st.Processes[0], up.PID)
	}

	// The kubeconfig: mode 600, loopback, this launcher's port.
	kc := filepath.Join(h.dir, "kubeconfig")
	if fi, err := os.Stat(kc); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("kubeconfig: %v, %v", fi, err)
	}
	if !strings.Contains(fileOf(kc), "server: https://127.0.0.1:29221") {
		t.Errorf("the kubeconfig does not point at 127.0.0.1:29221")
	}
	for _, f := range []string{"state.json"} {
		if fi, err := os.Stat(filepath.Join(h.dir, f)); err != nil || fi.Mode().Perm() != 0o600 {
			t.Errorf("%s: %v, %v", f, fi, err)
		}
	}

	// Listeners, from ss: exactly the planned ports, all on loopback.
	got := listenersOf(t, pid)
	want := []string{"127.0.0.1:29220", "127.0.0.1:29221", "127.0.0.1:29222", "::1:29221"}
	sort.Strings(want)
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("k3s listens on %v, want %v", got, want)
	}
	if rows := listeningInRange(t, 29220, 29239); len(rows) != len(want) {
		t.Errorf("listeners in the block: %v", rows)
	}

	// ---- isolation ----
	env, err := os.ReadFile("/proc/" + itoa(pid) + "/environ")
	if err != nil {
		t.Fatal(err)
	}
	vars := map[string]string{}
	for _, kv := range strings.Split(strings.TrimRight(string(env), "\x00"), "\x00") {
		if kv == "" {
			continue
		}
		k, v, _ := strings.Cut(kv, "=")
		vars[k] = v
	}
	wantVars := map[string]string{
		"HOME": h.dir + "/home", "XDG_RUNTIME_DIR": h.dir + "/run",
		"KUBECONFIG": h.dir + "/home/kubeconfig", "K3S_CONFIG_FILE": h.dir + "/empty-config.yaml",
		"TMPDIR": h.dir + "/tmp",
	}
	for k, v := range wantVars {
		if vars[k] != v {
			t.Errorf("k3s environment %s=%q, want %q", k, vars[k], v)
		}
	}
	// k3s re-executes itself and adds PATH and RES_OPTIONS; nothing else may be there.
	for k := range vars {
		if _, ok := wantVars[k]; !ok && k != "PATH" && k != "RES_OPTIONS" {
			t.Errorf("k3s environment has an unexpected variable %s", k)
		}
	}
	if strings.Contains(string(env), "leak") {
		t.Errorf("the launcher's polluted environment reached k3s: %q", env)
	}
	for _, f := range []string{"empty-config.yaml", "empty-registries.yaml"} {
		if fi, err := os.Stat(filepath.Join(h.dir, f)); err != nil || fi.Size() != 0 {
			t.Errorf("%s must exist and be empty: %v, %v", f, fi, err)
		}
	}
	logText := fileOf(filepath.Join(h.dir, "logs", "k3s.log"))
	for _, p := range forbiddenSys {
		if strings.Contains(logText, p) {
			t.Errorf("the k3s log mentions the system path %s", p)
		}
	}
	// The registry flag took effect: k3s read our empty file, not /etc/rancher/k3s/registries.yaml.
	if !strings.Contains(logText, "Using private registry config file at "+h.dir+"/empty-registries.yaml") {
		t.Error("the k3s log does not show the empty private-registry file in use")
	}
	if !strings.Contains(logText, "Preparing data dir "+h.dir+"/data/") {
		t.Error("the k3s log does not show our data directory in use")
	}

	// ---- what runs: the components S3 disabled stay disabled; the garbage collector runs ----
	c := h.clients()
	ctx, cancel := testCtx()
	defer cancel()
	groups, err := c.cs.Discovery().ServerGroups()
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range groups.Groups {
		if g.Name == "metrics.k8s.io" || g.Name == "helm.cattle.io" {
			t.Errorf("API group %s is served: metrics-server or the helm controller is running", g.Name)
		}
	}
	leases, err := c.cs.CoordinationV1().Leases("kube-system").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var leaseNames []string
	for _, l := range leases.Items {
		leaseNames = append(leaseNames, l.Name)
	}
	t.Logf("leases in kube-system: %v", leaseNames)
	// (k3s runs its embedded controllers without leader election, so no lease proves they run;
	// the cascade below proves the garbage collector, and the exact listener set above proves that
	// the scheduler's and the cloud controller manager's ports are not open.)
	for _, n := range leaseNames {
		if strings.Contains(n, "scheduler") || strings.Contains(n, "cloud-controller") {
			t.Errorf("lease %s: the scheduler or the cloud controller manager is running", n)
		}
	}
	deps, _ := c.cs.AppsV1().Deployments("kube-system").List(ctx, metav1.ListOptions{})
	dss, _ := c.cs.AppsV1().DaemonSets("kube-system").List(ctx, metav1.ListOptions{})
	svcs, _ := c.cs.CoreV1().Services("kube-system").List(ctx, metav1.ListOptions{})
	if len(deps.Items)+len(dss.Items)+len(svcs.Items) != 0 {
		t.Errorf("packaged add-ons were deployed: %d deployments, %d daemonsets, %d services", len(deps.Items), len(dss.Items), len(svcs.Items))
	}
	nodes, _ := c.cs.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if len(nodes.Items) != 0 {
		t.Errorf("an agent is running: %d nodes", len(nodes.Items))
	}

	// The garbage collector works through this host: delete an owner, and its dependent goes.
	owner, err := c.cs.CoreV1().ConfigMaps("default").Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "gc-owner"}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.cs.CoreV1().ConfigMaps("default").Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "gc-dependent",
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "ConfigMap", Name: owner.Name, UID: owner.UID}}}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	bg := metav1.DeletePropagationBackground
	if err := c.cs.CoreV1().ConfigMaps("default").Delete(ctx, "gc-owner", metav1.DeleteOptions{PropagationPolicy: &bg}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 60*time.Second, "the dependent to be garbage collected", func() bool {
		_, err := c.cs.CoreV1().ConfigMaps("default").Get(ctx, "gc-dependent", metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	})

	// ---- status ----
	var rep struct {
		Health     string   `json:"health"`
		Reasons    []string `json:"reasons"`
		Listeners  []string `json:"listeners"`
		RSSMiB     float64  `json:"rss_mib"`
		Readyz     *bool    `json:"readyz_ok"`
		KCM        *bool    `json:"controller_manager_ok"`
		Writes     *bool    `json:"writes_ok"`
		GC         *bool    `json:"gc_ok"`
		Executable string   `json:"executable"`
		Processes  []struct {
			PID     int    `json:"pid"`
			Verdict string `json:"verdict"`
		} `json:"processes"`
	}
	probeRV := func() string {
		cm, err := c.cs.CoreV1().ConfigMaps("kube-system").Get(ctx, "hpcburst-launcher-probe", metav1.GetOptions{})
		if err != nil {
			return "absent"
		}
		return cm.ResourceVersion
	}
	rvBefore := probeRV()
	r = cli(t, h.args("status", "--json")...)
	parseJSON(t, r.out, &rep)
	if r.code != exitOK || rep.Health != "healthy" || rep.Processes[0].PID != pid || rep.Processes[0].Verdict != "ours" ||
		rep.Readyz == nil || !*rep.Readyz || rep.KCM == nil || !*rep.KCM || rep.Writes == nil || !*rep.Writes {
		t.Fatalf("status exit %d: %+v", r.code, rep)
	}
	t.Logf("NUMBERS status: RSS %.0f MiB, listeners %v", rep.RSSMiB, rep.Listeners)
	if rep.GC == nil || !*rep.GC {
		t.Errorf("status: the garbage-collector probe says %s, want true", ptrStr(rep.GC))
	}
	// k3s must run the binary it unpacked into this host's data directory, not another k3s's
	// (the system one's /var/lib/rancher/k3s/data). Checked in status's report, and again against
	// the kernel's own link for the process.
	realDir, err := filepath.EvalSymlinks(h.dir)
	if err != nil {
		t.Fatal(err)
	}
	assetDir := filepath.Join(realDir, "data", "data") + string(filepath.Separator)
	if !strings.HasPrefix(rep.Executable, assetDir) {
		t.Errorf("status: executable %q is not under %s", rep.Executable, assetDir)
	}
	if exe, err := os.Readlink("/proc/" + itoa(pid) + "/exe"); err != nil || !strings.HasPrefix(exe, assetDir) {
		t.Errorf("/proc/%d/exe is %q (%v), want a file under %s", pid, exe, err, assetDir)
	}
	if probeRV() == rvBefore {
		t.Error("status reports a successful write probe, but the probe object did not change")
	}
	if r := cli(t, h.args("status")...); r.code != exitOK || !strings.HasPrefix(r.out, "healthy") {
		t.Errorf("text status: exit %d: %q", r.code, r.out)
	}

	// A second `up` on a running host is refused as such, and changes nothing.
	r = cli(t, h.upArgs(base)...)
	if r.code != exitFail || !strings.Contains(r.err, "already running") || st.Processes[0].PID != readState(t, h.dir).Processes[0].PID {
		t.Errorf("second up: exit %d, stderr %q", r.code, r.err)
	}

	// ---- the audit log: our requests are in it, and no Secret content is ----
	auditPath := filepath.Join(h.dir, "logs", "audit.log")
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	secretValue := hex.EncodeToString(raw)
	const auditSecret, auditCM = "audit-test-secret", "audit-test-configmap"
	if _, err := c.cs.CoreV1().Secrets("default").Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: auditSecret},
		Data: map[string][]byte{"password": []byte(secretValue)}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.cs.CoreV1().ConfigMaps("default").Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: auditCM}, Data: map[string]string{"k": "v"}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	// The log is buffered by the API server: wait for our two requests to show up, reading complete
	// lines only (the last one may be half written while the server runs).
	waitFor(t, 10*time.Second, "the create of our ConfigMap and Secret in the audit log", func() bool {
		if _, err := os.Stat(auditPath); err != nil {
			return false
		}
		_, evs := readAudit(t, auditPath, true)
		return findAudit(evs, "create", "configmaps", auditCM) != nil && findAudit(evs, "create", "secrets", auditSecret) != nil
	})
	checkAudit := func(complete bool) {
		t.Helper()
		text, evs := readAudit(t, auditPath, complete)
		if e := findAudit(evs, "create", "configmaps", auditCM); e == nil {
			t.Errorf("no create event for ConfigMap %s among %d audit events", auditCM, len(evs))
		}
		if e := findAudit(evs, "create", "secrets", auditSecret); e == nil {
			t.Errorf("no create event for Secret %s among %d audit events", auditSecret, len(evs))
		} else if e.Level != "Metadata" {
			t.Errorf("the Secret's create is logged at level %q, want Metadata", e.Level)
		}
		for _, form := range []string{secretValue, base64.StdEncoding.EncodeToString([]byte(secretValue))} {
			if strings.Contains(text, form) {
				t.Errorf("the audit log contains the Secret's content (%d chars of it, as %q...)", len(form), form[:4])
			}
		}
		for i, e := range evs {
			if e.hasBody {
				t.Errorf("audit event %d (%s %s) carries a request or response body", i, e.Verb, e.ObjectRef.Resource)
				break
			}
		}
	}
	checkAudit(true)

	// ---- the identity guard, on the real process ----
	// Record the start time wrongly, as if the PID had been reused. status must call the host
	// unhealthy, and down must not signal the (live, perfectly healthy) k3s.
	good, err := os.ReadFile(filepath.Join(h.dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	bad := strings.Replace(string(good), `"start_ticks": `+itoa64(st.Processes[0].StartTicks), `"start_ticks": `+itoa64(st.Processes[0].StartTicks+1), 1)
	if bad == string(good) {
		t.Fatal("could not tamper with the record")
	}
	if err := os.WriteFile(filepath.Join(h.dir, "state.json"), []byte(bad), 0o600); err != nil {
		t.Fatal(err)
	}
	r = cli(t, h.args("status", "--json")...)
	parseJSON(t, r.out, &rep)
	if r.code != exitFail || rep.Health != "unhealthy" || rep.Processes[0].Verdict != "foreign" {
		t.Errorf("with a wrong start time status says exit %d %+v", r.code, rep)
	}
	r = cli(t, h.args("down", "--grace", "5s")...)
	if r.code != exitOK || !strings.Contains(r.out, "foreign") {
		t.Errorf("down with a foreign PID: exit %d, %q", r.code, r.out)
	}
	time.Sleep(time.Second)
	if !running(pid) || startTicks(t, pid) != st.Processes[0].StartTicks {
		t.Fatal("down signalled a process whose recorded start time did not match")
	}
	if got := listenersOf(t, pid); len(got) != len(want) {
		t.Errorf("k3s lost its listeners: %v", got)
	}
	// Put the true record back: now down stops it.
	if err := os.WriteFile(filepath.Join(h.dir, "state.json"), good, 0o600); err != nil {
		t.Fatal(err)
	}

	// ---- down ----
	t0 := time.Now()
	r = cli(t, h.args("down", "--json")...)
	if r.code != exitOK {
		t.Fatalf("down exited %d", r.code)
	}
	t.Logf("NUMBERS down: %s", time.Since(t0).Round(time.Millisecond))
	// Judged without the launcher: the process, then every listener on the machine in the block.
	if !waitDone(pid) {
		t.Fatalf("k3s (pid %d) is still running after down: state %c", pid, procState(pid))
	}
	if rows := listeningInRange(t, 29220, 29239); len(rows) != 0 {
		t.Errorf("listeners remain after down: %v", rows)
	}
	if pids := k3sProcessesUsing(h.dir); len(pids) != 0 {
		t.Errorf("processes remain: %v", pids)
	}
	for _, f := range []string{"state.json", "kubeconfig", "k3s-admin.yaml"} {
		if _, err := os.Stat(filepath.Join(h.dir, f)); !os.IsNotExist(err) {
			t.Errorf("%s remains after down (%v)", f, err)
		}
	}
	if fi, err := os.Stat(filepath.Join(h.dir, "data/server/db/state.db")); err != nil || fi.Size() == 0 {
		t.Errorf("down must keep the store: %v, %v", fi, err)
	}
	if r := cli(t, h.args("down")...); r.code != exitOK || !strings.Contains(r.out, "nothing to stop") {
		t.Errorf("second down: exit %d, %q", r.code, r.out)
	}
	if r := cli(t, h.args("status")...); r.code != exitStopped {
		t.Errorf("status after down: exit %d, %q", r.code, r.out)
	}
	// The server is gone, so the audit log is complete now: every line must parse, in full.
	checkAudit(false)
}

func waitDone(pid int) bool {
	end := time.Now().Add(15 * time.Second)
	for time.Now().Before(end) {
		if !running(pid) {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return !running(pid)
}
