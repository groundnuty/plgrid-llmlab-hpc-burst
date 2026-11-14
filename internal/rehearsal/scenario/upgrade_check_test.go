package scenario_test

// The upgrade test's checks (issue #552), each read from outside the launcher's report: which file a
// process runs (its /proc/<pid>/exe's inode, since two builds of one commit are byte-identical), the
// host's processes pinned by pidfd before the upgrade and found exited after it, every framework object's
// UID before and after, the lifecycle controller's Lease, and the store's backup on disk.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"

	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/keeperprobe"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// upgradeBins is the second build of the commit's launcher, controller and front (hack/rehearsal.sh,
// $out/second).
type upgradeBins struct{ HPCBurst, Controller, Front string }

// secondBuild finds the second build beside the first. A run without one (REHEARSAL_PREBUILT, or a
// release archive) fails here, naming what is missing: the upgrade test never runs without it.
func secondBuild(t *testing.T) upgradeBins {
	t.Helper()
	dir := filepath.Join(os.Getenv("HPCBURST_REHEARSAL_BIN"), "second")
	b := upgradeBins{HPCBurst: filepath.Join(dir, "hpcburst"), Controller: filepath.Join(dir, "hpcburst-controller"), Front: filepath.Join(dir, "hpcburst-front")}
	for _, p := range []string{b.HPCBurst, b.Controller, b.Front} {
		if fi, err := os.Stat(p); err != nil || fi.IsDir() || fi.Mode()&0o111 == 0 {
			t.Fatalf("the upgrade test needs a second build of the commit in %s (hack/rehearsal.sh builds it; REHEARSAL_PREBUILT and a release archive have none): %s: %v", dir, p, err)
		}
	}
	return b
}

// fileID is a file's identity: its device and inode.
type fileID struct{ Dev, Ino uint64 }

func (f fileID) String() string { return fmt.Sprintf("%d:%d", f.Dev, f.Ino) }

func idOf(fi os.FileInfo) (fileID, error) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return fileID{}, errors.New("no stat")
	}
	return fileID{Dev: uint64(st.Dev), Ino: st.Ino}, nil
}

// fileIDOf is the identity of the file at path.
func fileIDOf(path string) (fileID, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return fileID{}, err
	}
	return idOf(fi)
}

// exeIDOf is the identity of the file process pid runs: /proc/<pid>/exe resolves to the very file it
// was started from, even after that path was replaced and the old file unlinked.
func exeIDOf(pid int) (fileID, error) {
	fi, err := os.Stat("/proc/" + strconv.Itoa(pid) + "/exe")
	if err != nil {
		return fileID{}, err
	}
	return idOf(fi)
}

// copyExecutable copies the executable src to dst, mode 0755, synced.
func copyExecutable(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o755)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// replaceAtomically puts a copy of src at dst by a rename over it, as an operator installs a new build
// in place: a new file at dst's path, whose identity it returns. A process started from dst before keeps
// running the old file.
func replaceAtomically(src, dst string) (fileID, error) {
	tmp := dst + ".new"
	if err := copyExecutable(src, tmp); err != nil {
		return fileID{}, err
	}
	if err := os.Rename(tmp, dst); err != nil {
		return fileID{}, err
	}
	return fileIDOf(dst)
}

// ownCopies copies b's controller and front into dir and returns b naming the copies: up runs them from
// there, and the upgrade replaces them there, leaving the job's shared set untouched.
func ownCopies(t *testing.T, b stack.Binaries, dir string) stack.Binaries {
	t.Helper()
	for _, f := range []*string{&b.Controller, &b.Front} {
		dst := filepath.Join(dir, filepath.Base(*f))
		if err := copyExecutable(*f, dst); err != nil {
			t.Fatalf("copy %s to %s: %v", *f, dst, err)
		}
		*f = dst
	}
	return b
}

// pinRecorded pins the process state.json records under name (the launcher's own record: k3s, the
// supervisor), verified as recorded in this boot with its recorded start time.
func pinRecorded(t *testing.T, state, name string) (*stackPin, error) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(state, "state.json"))
	if err != nil {
		return nil, err
	}
	var st struct {
		Procs []launcherProc `json:"processes"`
	}
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, err
	}
	i := slices.IndexFunc(st.Procs, func(p launcherProc) bool { return p.Name == name })
	if i < 0 {
		return nil, fmt.Errorf("state.json records no %s", name)
	}
	rec := st.Procs[i]
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return nil, err
	}
	if rec.BootID != strings.TrimSpace(string(boot)) {
		return nil, fmt.Errorf("%s was recorded in boot %s, not this one", name, rec.BootID)
	}
	p, err := keeperprobe.OpenPin(t, "the "+name, rec.PID)
	if err != nil {
		return nil, err
	}
	if _, ticks, err := keeperprobe.StatRow(rec.PID); err != nil || ticks != rec.StartTicks {
		return p, fmt.Errorf("%s's pid %d started at tick %d, not the recorded %d (%v)", name, rec.PID, ticks, rec.StartTicks, err)
	}
	p.Ticks = rec.StartTicks
	return p, nil
}

// hostPins are the host's processes, each pinned by a pidfd: k3s, the supervisor and each supervised
// process named.
type hostPins map[string]*stackPin

// pinHost pins k3s, the supervisor and the supervised processes entries.
func pinHost(t *testing.T, state string, entries ...string) hostPins {
	t.Helper()
	pins := hostPins{}
	k3s, err := pinRecorded(t, state, "k3s")
	if err != nil {
		t.Fatalf("pin k3s: %v", err)
	}
	pins["k3s"] = k3s
	sup, err := pinSupervisor(t, state)
	if err != nil {
		t.Fatalf("pin the supervisor: %v", err)
	}
	pins["supervisor"] = sup
	for _, name := range entries {
		p, _, err := pinEntry(t, state, name, sup)
		if err != nil {
			t.Fatalf("pin the supervised %s: %v", name, err)
		}
		pins[name] = p
	}
	return pins
}

// restartProblems compares the processes pinned before (old, all expected exited) with those after
// (fresh): each of old's must have exited, and each fresh one be another process (another PID, or the
// same PID started later). It lists what fails, in name order.
func restartProblems(old, fresh hostPins) []string {
	var out []string
	names := make([]string, 0, len(old))
	for n := range old {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		o, f := old[n], fresh[n]
		switch {
		case !o.Exited():
			out = append(out, fmt.Sprintf("%s: the old pid %d is still running", n, o.PID))
		case f == nil:
			out = append(out, fmt.Sprintf("%s: no new process", n))
		case f.PID == o.PID && f.Ticks == o.Ticks:
			out = append(out, fmt.Sprintf("%s: the same process %d after the upgrade", n, f.PID))
		}
	}
	return out
}

// objKey names an object across the upgrade.
type objKey struct{ Kind, Namespace, Name string }

// frameworkUIDs reads every framework object, and the Leases in the controllers' namespace, through the
// API: the UIDs the upgrade must keep, read by the test itself, not taken from the launcher's comparison.
func frameworkUIDs(t *testing.T, c client.Client) map[objKey]types.UID {
	t.Helper()
	ctx, cancel := stack.CallCtx(t)
	defer cancel()
	out := map[objKey]types.UID{}
	add := func(kind string, objs []client.Object) {
		for _, o := range objs {
			out[objKey{kind, o.GetNamespace(), o.GetName()}] = o.GetUID()
		}
	}
	var mds v1alpha1.ModelDeploymentList
	var ins v1alpha1.InstanceList
	var als v1alpha1.AllocationList
	var prs v1alpha1.ProviderList
	var pps v1alpha1.PlatformProfileList
	var ls coordinationv1.LeaseList
	for _, l := range []struct {
		list client.ObjectList
		opts []client.ListOption
	}{{&mds, nil}, {&ins, nil}, {&als, nil}, {&prs, nil}, {&pps, nil}, {&ls, []client.ListOption{client.InNamespace(v1alpha1.ControllerNamespace)}}} {
		if err := c.List(ctx, l.list, l.opts...); err != nil {
			t.Fatalf("list %T: %v", l.list, err)
		}
	}
	for i := range mds.Items {
		add("ModelDeployment", []client.Object{&mds.Items[i]})
	}
	for i := range ins.Items {
		add("Instance", []client.Object{&ins.Items[i]})
	}
	for i := range als.Items {
		add("Allocation", []client.Object{&als.Items[i]})
	}
	for i := range prs.Items {
		add("Provider", []client.Object{&prs.Items[i]})
	}
	for i := range pps.Items {
		add("PlatformProfile", []client.Object{&pps.Items[i]})
	}
	for i := range ls.Items {
		add("Lease", []client.Object{&ls.Items[i]})
	}
	return out
}

// uidProblems lists each object of before that is missing after, or has another UID there.
func uidProblems(before, after map[objKey]types.UID) []string {
	var out []string
	for k, uid := range before {
		switch got, ok := after[k]; {
		case !ok:
			out = append(out, fmt.Sprintf("%s %s/%s is gone", k.Kind, k.Namespace, k.Name))
		case got != uid:
			out = append(out, fmt.Sprintf("%s %s/%s has UID %s, was %s", k.Kind, k.Namespace, k.Name, got, uid))
		}
	}
	sort.Strings(out)
	return out
}

// lifecycleLease is the lifecycle controller's Lease, its holder and transitions.
func lifecycleLease(t *testing.T, c client.Client) (leaseState, bool) {
	t.Helper()
	var l coordinationv1.Lease
	ctx, cancel := stack.CallCtx(t)
	defer cancel()
	if err := c.Get(ctx, types.NamespacedName{Namespace: v1alpha1.ControllerNamespace, Name: v1alpha1.LeaseLifecycle}, &l); err != nil {
		return leaseState{}, false
	}
	var s leaseState
	if l.Spec.HolderIdentity != nil {
		s.Holder = *l.Spec.HolderIdentity
	}
	if l.Spec.LeaseTransitions != nil {
		s.Transitions = int64(*l.Spec.LeaseTransitions)
	}
	return s, true
}

// backupNames lists the directories under <state>/backups.
func backupNames(state string) map[string]bool {
	out := map[string]bool{}
	ents, _ := os.ReadDir(filepath.Join(state, "backups"))
	for _, e := range ents {
		if e.IsDir() {
			out[e.Name()] = true
		}
	}
	return out
}

// newBackup is the one directory under <state>/backups that before did not list.
func newBackup(state string, before map[string]bool) (string, error) {
	var fresh []string
	for n := range backupNames(state) {
		if !before[n] {
			fresh = append(fresh, n)
		}
	}
	if len(fresh) != 1 {
		return "", fmt.Errorf("%d new directories under backups/ (%v), want the upgrade's one", len(fresh), fresh)
	}
	return filepath.Join(state, "backups", fresh[0]), nil
}

// backupProblems checks the backup dir as the launcher's upgrade must have made it before the new
// release started (HBEP-11, "backs up the store before every upgrade"): complete (its final name, no
// .partial), backup.json naming wantVersion as the release that wrote the store, every file it lists
// present with mode 0600, a db among them, and the store's copy holding each of holds (the UIDs of objects
// the store held before the upgrade).
func backupProblems(dir, wantVersion string, holds []string) []string {
	var out []string
	if strings.HasSuffix(dir, ".partial") {
		out = append(out, dir+" is an unfinished backup")
	}
	b, err := os.ReadFile(filepath.Join(dir, "backup.json"))
	if err != nil {
		return append(out, "backup.json: "+err.Error())
	}
	var info struct {
		Version string   `json:"version"`
		Files   []string `json:"files"`
	}
	if err := json.Unmarshal(b, &info); err != nil {
		return append(out, "backup.json: "+err.Error())
	}
	if info.Version != wantVersion {
		out = append(out, fmt.Sprintf("backup.json names %q as the store's writer, want %q, the release before the upgrade", info.Version, wantVersion))
	}
	var db []byte
	for _, f := range info.Files {
		fi, err := os.Stat(filepath.Join(dir, f))
		if err != nil {
			out = append(out, fmt.Sprintf("%s: %v", f, err))
			continue
		}
		if fi.Mode().Perm() != 0o600 {
			out = append(out, fmt.Sprintf("%s has mode %o, want 600", f, fi.Mode().Perm()))
		}
		if strings.HasPrefix(f, "db/") {
			c, _ := os.ReadFile(filepath.Join(dir, f))
			db = append(db, c...)
		}
	}
	if db == nil {
		out = append(out, "the backup holds no store file (db/)")
	}
	for _, h := range holds {
		if !bytes.Contains(db, []byte(h)) {
			out = append(out, fmt.Sprintf("the store's copy holds no %s", h))
		}
	}
	return out
}

// storeVersion is the release store-version.json says last wrote the store.
func storeVersion(t *testing.T, state string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(state, "store-version.json"))
	if err != nil {
		t.Fatalf("the launcher's store-version.json: %v", err)
	}
	var v struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(b, &v); err != nil || v.Version == "" {
		t.Fatalf("store-version.json names no version (%v): %s", err, b)
	}
	return v.Version
}
