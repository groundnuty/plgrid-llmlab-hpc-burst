package main

// The relay keeper through the CLI, on a real k3s (#343, PR 5; HBEP-11, Single-user host; HBEP-6,
// The relay keeper and its Test plan: "after down, up without --relay-keeper refuses while
// host-dialed Allocations run, and --relay-keeper without --front-mode host-dialed is refused").
// The keeper is the real one, the fake controller's relaykeeper role (relaykeeper.Run). No relay
// runs, so nothing here is a stream test: the keeper's second start (E+1) shows its record and
// health, never a stream's continuity.

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper"
)

// keeperEntry is the keeper's entry of supervisor.json, or nil.
func keeperEntry(t *testing.T, dir string) *stateProc {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "controller", "supervisor.json"))
	if err != nil {
		return nil
	}
	var r supervisorRecord
	if json.Unmarshal(b, &r) != nil {
		return nil
	}
	for _, e := range r.Entries {
		if e.Name == "relaykeeper" && e.State == "running" {
			return e.Proc
		}
	}
	return nil
}

// runningAllocation creates an Allocation in phase Running, as a Provider's controller leaves one
// whose job runs.
func (h *host) runningAllocation(name string) {
	h.t.Helper()
	a := &v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}}
	a.APIVersion, a.Kind = v1alpha1.GroupVersion.String(), "Allocation"
	a.Spec.ProviderConfigReference = &xpv2.ProviderConfigReference{Kind: v1alpha1.ProviderConfigKind, Name: "example-a"}
	a.Spec.ForProvider = v1alpha1.AllocationParameters{Batch: &v1alpha1.BatchParameters{
		Partition: "example-gpu", TimeLimit: "1h", GPUs: 1, CPUsPerTask: 16,
		Memory: resource.MustParse("120Gi"), AgentDigest: strings.Repeat("3", 64)}}
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(a)
	if err != nil {
		h.t.Fatal(err)
	}
	res := h.clients().dyn.Resource(v1alpha1.GroupVersion.WithResource("allocations")).Namespace("default")
	got, err := res.Create(h.t.Context(), &unstructured.Unstructured{Object: obj}, metav1.CreateOptions{})
	if err != nil {
		h.t.Fatalf("create the Allocation: %v", err)
	}
	// The status through the API type, so the phase lands where the CRD has it.
	a.Status.AtProvider.Phase = v1alpha1.AllocationRunning
	withStatus, err := runtime.DefaultUnstructuredConverter.ToUnstructured(a)
	if err != nil {
		h.t.Fatal(err)
	}
	got.Object["status"] = withStatus["status"]
	if _, err := res.UpdateStatus(h.t.Context(), got, metav1.UpdateOptions{}); err != nil {
		h.t.Fatalf("set the Allocation Running: %v", err)
	}
}

// setAllocationPhase sets an Allocation's observed phase, through the API type.
func (h *host) setAllocationPhase(name string, phase v1alpha1.AllocationPhase) {
	h.t.Helper()
	res := h.clients().dyn.Resource(v1alpha1.GroupVersion.WithResource("allocations")).Namespace("default")
	got, err := res.Get(h.t.Context(), name, metav1.GetOptions{})
	if err != nil {
		h.t.Fatal(err)
	}
	var a v1alpha1.Allocation
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(got.Object, &a); err != nil {
		h.t.Fatal(err)
	}
	a.Status.AtProvider.Phase = phase
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&a)
	if err != nil {
		h.t.Fatal(err)
	}
	if _, err := res.UpdateStatus(h.t.Context(), &unstructured.Unstructured{Object: obj}, metav1.UpdateOptions{}); err != nil {
		h.t.Fatalf("set the Allocation %s: %v", phase, err)
	}
}

// up --relay-keeper in host-dialed mode runs the keeper, which status shows with its E. An
// upgrade brings the host back with the keeper, at the next E, the controller given its socket
// (the review of PR 361: it had come back as alpha). down stops the keeper and keeps its state
// file. Then, with an Allocation Running, up without --relay-keeper refuses, naming it, and leaves
// no host running; up with it starts again, the keeper at the next E. Last, the keeper retired
// marker (HBEP-6, Compatibility and upgrades, as amended 10 Nov 2026): with the Allocation drained,
// up without the flag retires the keeper and writes the marker; with the marker, a later alpha up
// passes with an alpha Allocation running; and up --relay-keeper removes it, the keeper resuming
// at the next E.
func TestRelayKeeperUpStatusDownAndTheSwitchBack(t *testing.T) {
	const base = 28020
	h := newHost(t)
	bin, ctlDir := fakeController(t)
	up := func(extra ...string) cliResult {
		return cli(t, h.upArgs(base, append(append(frpsArgs(t), "--json", "--controller-bin", bin, "--manifests", "../../config/crd", "--front-mode", "host-dialed"), extra...)...)...)
	}
	r := up("--relay-keeper", "--version", "1.35.9")
	if r.code != exitOK {
		t.Fatalf("up --relay-keeper exited %d: %s", r.code, r.err)
	}
	var rep struct {
		RelayKeeperSocket string `json:"relay_keeper_socket"`
	}
	parseJSON(t, r.out, &rep)
	if rep.RelayKeeperSocket != filepath.Join(h.dir, "relaykeeper.sock") {
		t.Errorf("up reports the keeper's socket %q", rep.RelayKeeperSocket)
	}
	st := cli(t, "status", "--state-dir", h.dir)
	const firstE = int64(1)<<31 + 1
	if st.code != exitOK || !strings.Contains(st.out, "supervised relaykeeper: running=true") || !strings.Contains(st.out, "E "+strconv.FormatInt(firstE, 10)) {
		t.Fatalf("status exited %d:\n%s\n%s", st.code, st.out, st.err)
	}
	keeper := keeperEntry(t, h.dir)
	if keeper == nil || !running(keeper.PID) {
		t.Fatalf("the keeper's entry in supervisor.json: %+v", keeper)
	}

	// The upgrade keeps the keeper.
	if u := cli(t, h.args("upgrade", "--bin-dir", h.binDir, "--to", "1.36.5")...); u.code != exitOK {
		t.Fatalf("upgrade exited %d: %s", u.code, u.err)
	}
	if st := cli(t, "status", "--state-dir", h.dir); st.code != exitOK || !strings.Contains(st.out, "supervised relaykeeper: running=true") || !strings.Contains(st.out, "E "+strconv.FormatInt(firstE+1, 10)) {
		t.Fatalf("status after the upgrade exited %d; want the keeper at E %d:\n%s\n%s", st.code, firstE+1, st.out, st.err)
	}
	var rec struct {
		RelayKeeper bool `json:"relay_keeper"`
	}
	parseJSON(t, fileOf(filepath.Join(h.dir, "state.json")), &rec)
	if !rec.RelayKeeper {
		t.Error("state.json after the upgrade does not say relay_keeper")
	}
	r2, ok := readSupervisorRecordN(t, h.dir, 3) // the controller, frps and the keeper
	if !ok || r2.Entries[0].Proc == nil {
		t.Fatalf("supervisor.json after the upgrade: %+v", r2)
	}
	var argv []string
	parseJSON(t, fileOf(filepath.Join(ctlDir, "argv-"+strconv.Itoa(r2.Entries[0].Proc.PID)+".json")), &argv)
	if len(argv) < 2 || argv[len(argv)-2] != "--relay-keeper-socket" || argv[len(argv)-1] != filepath.Join(h.dir, "relaykeeper.sock") {
		t.Errorf("the upgraded controller's command line %q: want --relay-keeper-socket %s at its end", argv, filepath.Join(h.dir, "relaykeeper.sock"))
	}
	if keeper = keeperEntry(t, h.dir); keeper == nil || !running(keeper.PID) {
		t.Fatalf("the keeper's entry after the upgrade: %+v", keeper)
	}

	h.runningAllocation("example-alloc")
	if d := cli(t, "down", "--state-dir", h.dir); d.code != exitOK {
		t.Fatalf("down exited %d: %s", d.code, d.err)
	}
	if running(keeper.PID) && startTicks(t, keeper.PID) == keeper.StartTicks {
		t.Errorf("the keeper pid %d still runs after down", keeper.PID)
	}
	ks, err := relaykeeper.LoadState(relaykeeper.StatePath(h.dir))
	if err != nil || ks.Starts != 2 {
		t.Fatalf("the keeper's state file after down: %+v, %v; want it kept, with two starts", ks, err)
	}

	r = up()
	if r.code == exitOK || !strings.Contains(r.err, "relay keeper ran on this host") || !strings.Contains(r.err, "default/example-alloc") {
		t.Fatalf("up without --relay-keeper exited %d; want refused, naming default/example-alloc:\n%s", r.code, r.err)
	}
	if _, err := os.Stat(filepath.Join(h.dir, "state.json")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the launcher's record after the refused up: %v, want the host stopped again", err)
	}
	if ks, _ := relaykeeper.LoadState(relaykeeper.StatePath(h.dir)); ks == nil || ks.Starts != 2 {
		t.Errorf("the keeper started during the refused up: %+v", ks)
	}

	if r = up("--relay-keeper"); r.code != exitOK {
		t.Fatalf("up --relay-keeper again exited %d: %s", r.code, r.err)
	}
	if st = cli(t, "status", "--state-dir", h.dir); st.code != exitOK || !strings.Contains(st.out, "E "+strconv.FormatInt(firstE+2, 10)) {
		t.Errorf("status after the second up exited %d:\n%s", st.code, st.out)
	}

	// The keeper retired marker.
	marker := filepath.Join(h.dir, "relaykeeper", "retired")
	h.setAllocationPhase("example-alloc", v1alpha1.AllocationEnded)
	if d := cli(t, "down", "--state-dir", h.dir); d.code != exitOK {
		t.Fatalf("down exited %d: %s", d.code, d.err)
	}
	if r = up(); r.code != exitOK || !strings.Contains(r.err, "the relay keeper is retired") {
		t.Fatalf("up without --relay-keeper after the drain exited %d; want it to pass and retire the keeper:\n%s", r.code, r.err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("the retired marker after the drained switch back: %v", err)
	}
	if st = cli(t, "status", "--state-dir", h.dir); strings.Contains(st.out, "supervised relaykeeper") {
		t.Errorf("status of the alpha host shows a keeper:\n%s", st.out)
	}
	h.runningAllocation("alpha-alloc")
	if d := cli(t, "down", "--state-dir", h.dir); d.code != exitOK {
		t.Fatalf("down exited %d: %s", d.code, d.err)
	}
	if r = up(); r.code != exitOK || !strings.Contains(r.err, "the relay keeper was retired here") {
		t.Fatalf("an alpha up with the marker and an alpha Allocation running exited %d; want it to pass:\n%s", r.code, r.err)
	}
	if d := cli(t, "down", "--state-dir", h.dir); d.code != exitOK {
		t.Fatalf("down exited %d: %s", d.code, d.err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("the retired marker after down: %v, want it kept", err)
	}
	if r = up("--relay-keeper"); r.code != exitOK || !strings.Contains(r.err, "removed the relay keeper's retired marker") {
		t.Fatalf("up --relay-keeper with the marker exited %d; want it to remove the marker:\n%s", r.code, r.err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the retired marker after up --relay-keeper: %v, want it removed", err)
	}
	if st = cli(t, "status", "--state-dir", h.dir); st.code != exitOK || !strings.Contains(st.out, "E "+strconv.FormatInt(firstE+3, 10)) {
		t.Errorf("status after the keeper resumed exited %d; want E %d:\n%s", st.code, firstE+3, st.out)
	}
}

// up refuses --relay-keeper without --front-mode host-dialed, before anything starts.
func TestUpRefusesTheRelayKeeperOutsideHostDialed(t *testing.T) {
	bin, _ := fakeController(t)
	dir := filepath.Join(shortTmp(t, false), "st")
	r := cli(t, "up", "--state-dir", dir, "--port-base", strconv.Itoa(28020), "--controller-bin", bin,
		"--front-mode", "direct", "--front-addr", "front.example.test:8443", "--relay-keeper")
	if r.code == exitOK || !strings.Contains(r.err, "--relay-keeper is for --front-mode host-dialed only") {
		t.Fatalf("up --relay-keeper in direct mode exited %d: %s", r.code, r.err)
	}
	if _, err := os.Stat(filepath.Join(dir, "state.json")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a record after the refusal: %v", err)
	}
}
