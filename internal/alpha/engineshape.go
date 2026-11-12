package alpha

// The job's process tree as the worker fault sees it (issue #445), and its
// redacted shape. On Athena on 12 Nov 2026 the worker fault found no engine,
// and which of PickEngine's conditions failed was unknown, since the listing
// holds paths, logins and job IDs and is never logged. The shape tells the
// cases apart with none of them (the science agent's ruling on #445): per
// process of the job's tree, its depth, a kind for its argv[0], how it is of
// the job, its environment's and its cgroup's say, its command line's form
// and which of PickEngine's three conditions it meets. Only kinds, depths and
// counts are printed: no PID, path, login, host or job ID.

import (
	"fmt"
	"path/filepath"
	"slices"
	"strings"
)

// The kinds of a process's argv[0] in the shape.
const (
	kindApptainer     = "apptainer"
	kindStarter       = "starter"
	kindRuntimeParent = "runtime-parent"
	kindPython        = "python"
	kindVLLM          = "vllm"
	kindShell         = "shell"
	kindOther         = "other"
)

// shells are the base names of the shells kindOf knows.
var shells = map[string]bool{"sh": true, "bash": true, "dash": true, "zsh": true, "ksh": true, "mksh": true, "ash": true}

// kindOf is the kind of a process's argv[0]:
//   - runtime-parent: the title Apptainer's master takes ("Apptainer runtime
//     parent", or SingularityCE's), with anything after it, as an image's name;
//   - starter: the starter's binary (starter, starter-suid), or its shim,
//     which renames itself appinit (Apptainer 1.4.5,
//     internal/pkg/runtime/engine/apptainer/process_linux.go, StartProcess) or
//     sinit (SingularityCE);
//   - apptainer: a base name that names apptainer or singularity;
//   - python: a Python interpreter, python[3[.N]];
//   - vllm: a base name that names vllm, its titles (VLLM::EngineCore) too;
//   - shell: sh, bash, dash, zsh, ksh, mksh or ash, a login shell's "-" too;
//   - other: anything else, an empty command line too.
func kindOf(argv []string) string {
	if len(argv) == 0 {
		return kindOther
	}
	a0 := strings.ToLower(argv[0])
	if strings.HasPrefix(a0, "apptainer runtime parent") || strings.HasPrefix(a0, "singularity runtime parent") {
		return kindRuntimeParent
	}
	base := strings.ToLower(filepath.Base(argv[0]))
	switch {
	case strings.HasPrefix(base, "starter") || base == "appinit" || base == "sinit":
		return kindStarter
	case strings.Contains(base, "apptainer") || strings.Contains(base, "singularity"):
		return kindApptainer
	case pythonRE.MatchString(base):
		return kindPython
	case strings.Contains(base, "vllm"):
		return kindVLLM
	case shells[strings.TrimPrefix(base, "-")]:
		return kindShell
	}
	return kindOther
}

// argvForm is the form of a process's command line, with nothing of its
// words: "program" (not a Python interpreter); "python-script" (an
// interpreter, then an absolute path); "python-options-script" (with the
// interpreter's options before it, skipPyOptions); "python-module" (-m);
// "python-code" (-c); "python-other"; "python" (the interpreter alone).
func argvForm(argv []string) string {
	if len(argv) == 0 || !pythonRE.MatchString(filepath.Base(argv[0])) {
		return "program"
	}
	rest := skipPyOptions(argv[1:])
	switch {
	case len(rest) == 0:
		return "python"
	case rest[0] == "-m":
		return "python-module"
	case rest[0] == "-c":
		return "python-code"
	case !strings.HasPrefix(rest[0], "/"):
		return "python-other"
	case len(rest) < len(argv)-1:
		return "python-options-script"
	}
	return "python-script"
}

// procTree is a listing as the job's process tree: the processes by PID, and
// the launch's command with its match for the engine.
type procTree struct {
	procs   []NodeProc
	byPID   map[int]NodeProc
	command []string
	match   func(argv, command []string) bool
}

func newProcTree(procs []NodeProc, command []string, match func(argv, command []string) bool) procTree {
	byPID := make(map[int]NodeProc, len(procs))
	for _, p := range procs {
		byPID[p.PID] = p
	}
	return procTree{procs: procs, byPID: byPID, command: command, match: match}
}

// parent is p's parent, when the listing has it and it is not p.
func (t procTree) parent(p NodeProc) (NodeProc, bool) {
	q, ok := t.byPID[p.PPID]
	return q, ok && q.PID != p.PID
}

// descendsFrom reports whether p is the process pid or descends from it,
// through processes the listing has; pid itself need not be listed. Every
// walk up the tree is bounded by the listing's length, so a malformed
// listing with a cycle ends.
func (t procTree) descendsFrom(p NodeProc, pid int) bool {
	cur := p
	for range len(t.procs) + 1 {
		if cur.PID == pid || cur.PPID == pid {
			return true
		}
		q, ok := t.parent(cur)
		if !ok {
			return false
		}
		cur = q
	}
	return false
}

// inJob reports whether p is of the job: InJob itself, or a descendant of a
// process that is (PickEngine's condition 1).
func (t procTree) inJob(p NodeProc) bool {
	cur := p
	for range len(t.procs) + 1 {
		if cur.InJob {
			return true
		}
		q, ok := t.parent(cur)
		if !ok {
			return false
		}
		cur = q
	}
	return false
}

// parentHidden reports whether p's parent is hidden from this user: its PID is not in the listing and the listing could not read
// its /proc entry (issue #488). A parent the listing has, or one it could read, is not hidden.
func (t procTree) parentHidden(p NodeProc) bool {
	_, listed := t.parent(p)
	return !listed && p.ParentHidden
}

// underApptainer reports whether p's parent is Apptainer's, or a shell under
// one, and so on up, each of the job; or whether p's own parent is hidden from
// this user, as Apptainer's setuid starter is (PickEngine's condition 2). The
// hidden parent counts at p's own parent alone, never above a shell: it is the
// engine's parent that the listing cannot see.
func (t procTree) underApptainer(p NodeProc) bool {
	if t.parentHidden(p) {
		return true
	}
	cur := p
	for range len(t.procs) + 1 {
		q, ok := t.parent(cur)
		if !ok || !t.inJob(q) {
			return false
		}
		switch kindOf(q.Argv) {
		case kindApptainer, kindStarter, kindRuntimeParent:
			return true
		case kindShell:
			cur = q
		default:
			return false
		}
	}
	return false
}

// isEngine reports whether p meets PickEngine's three conditions.
func (t procTree) isEngine(p NodeProc) bool {
	return t.inJob(p) && t.underApptainer(p) && t.match(p.Argv, t.command)
}

// maxShapeLines bounds the shape: a job's tree past it is cut, with a count.
const maxShapeLines = 40

// Shape is the redacted shape of the job's process tree in a listing (issue
// #445): a line of counts, then one line per process of the job's tree, its
// descendants after it (siblings by kind and form, then PID), each with its
// depth (d0 for a process whose parent is not of the job), its kind (kindOf), how it is of the job (by its env, its
// cgroup, both, as the listing said, or by descent), what its environment said
// (job, no, unreadable), its cgroup's form (job_<n> or other: never the
// number), its command line's form (argvForm) and the conditions of
// PickEngine it meets (job, apptainer, command). A process at depth 0 says
// whether its parent was listed. It holds no PID, path, login, host or job
// ID: the only numbers in it are depths and counts.
func Shape(procs []NodeProc, command []string, match func(argv, command []string) bool) string {
	return newProcTree(procs, command, match).shape()
}

func (t procTree) shape() string {
	children := map[int][]NodeProc{}
	var roots []NodeProc
	inTree, engines := 0, 0
	for _, p := range t.procs {
		if !t.inJob(p) {
			continue
		}
		inTree++
		if t.isEngine(p) {
			engines++
		}
		if q, ok := t.parent(p); ok && t.inJob(q) {
			children[q.PID] = append(children[q.PID], p)
		} else {
			roots = append(roots, p)
		}
	}
	// Siblings in the order of their kind and form, then of their PIDs, so that the shape of one tree does not
	// change with the order in which the kernel gave out the PIDs.
	order := func(a, b NodeProc) int {
		if c := strings.Compare(kindOf(a.Argv)+" "+argvForm(a.Argv), kindOf(b.Argv)+" "+argvForm(b.Argv)); c != 0 {
			return c
		}
		return a.PID - b.PID
	}
	slices.SortFunc(roots, order)
	for _, c := range children {
		slices.SortFunc(c, order)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "the job's process tree on its node, redacted (kinds, depths and counts only): %d processes listed, %d in the job's tree, %d meeting all three conditions",
		len(t.procs), inTree, engines)
	lines := 0
	var walk func(p NodeProc, depth int)
	walk = func(p NodeProc, depth int) {
		if lines < maxShapeLines {
			b.WriteString("\n  " + t.line(p, depth))
		}
		lines++
		for _, c := range children[p.PID] {
			walk(c, depth+1)
		}
	}
	for _, r := range roots {
		walk(r, 0)
	}
	if lines > maxShapeLines {
		fmt.Fprintf(&b, "\n  and %d more", lines-maxShapeLines)
	}
	return b.String()
}

// line is one process of the shape.
func (t procTree) line(p NodeProc, depth int) string {
	how := "descent"
	switch {
	case p.Env == EnvJob && p.CgroupJob:
		how = "env+cgroup"
	case p.Env == EnvJob:
		how = "env"
	case p.CgroupJob:
		how = "cgroup"
	case p.InJob:
		how = "listing"
	}
	env := map[EnvState]string{EnvNotRead: "unknown", EnvJob: "job", EnvOther: "no", EnvUnreadable: "unreadable"}[p.Env]
	cgroup := "other"
	if p.CgroupJobForm {
		cgroup = "job_<n>"
	}
	meets := []string{"job"}
	if t.underApptainer(p) {
		meets = append(meets, "apptainer")
	}
	if t.match(p.Argv, t.command) {
		meets = append(meets, "command")
	}
	l := fmt.Sprintf("d%d %s in-job:%s env:%s cgroup:%s argv:%s meets:%s", depth, kindOf(p.Argv), how, env, cgroup, argvForm(p.Argv), strings.Join(meets, "+"))
	if depth == 0 {
		switch _, ok := t.parent(p); {
		case ok:
			l += " parent:outside-the-job"
		case p.ParentHidden:
			l += " parent:hidden"
		default:
			l += " parent:unlisted"
		}
	}
	return l
}
