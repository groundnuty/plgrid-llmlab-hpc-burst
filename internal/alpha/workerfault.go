package alpha

// The worker-rung fault of the stack rows (issue #245; the science agent's
// ruling of 2 Nov 2026): HBEP-7:637 asks for "one restart at the worker rung"
// on each alpha platform, so the run kills the engine in its own job and the
// stack must restart the worker. The run reaches the node the way an operator
// would, from the login node with srun --overlap into its own job's
// allocation, and:
//
//  1. the job is ours: its ID comes from our Allocation's status, and right
//     before each srun it is read again from the queue's --me listing under
//     our tag (the steps check it; a mismatch fails the row);
//  2. one srun step lists the processes this user owns on the node, and
//     PickEngine finds EXACTLY ONE engine: the process the agent launched
//     through Apptainer, that is a process of our job (or a descendant of
//     one) under an Apptainer process, with only Apptainer's and shells
//     between them, whose command is the launch's engine command, its
//     program included (MatchesCommand). Zero or several fail the row, with
//     no retry, and the failure carries a redacted shape of the job's
//     process tree (Shape, issue #445), never the listing;
//  3. a second step kills that process alone, with SIGKILL, through a pidfd
//     (pidfd_open(2), pidfd_send_signal(2)): the pidfd is opened first, the
//     process at the PID is then checked to be the one listed (its owner,
//     start time and command line) and the pidfd to name it still, and the
//     signal goes through the pidfd, which names that process and never a
//     later one that reused its PID (review of PR 255, B1). A node where no
//     pidfd can be opened kills nothing. Never by name, and nothing outside
//     our job's step.
//
// The listing and the choice are split so that the choice is a pure function
// of what the node showed, and the kill script re-checks the process it is
// given rather than searching again.

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

// SrunLine is the srun command line of a step in the run's own job: --overlap
// to run beside the job's batch step, one task, a one-minute limit, the
// script on standard input. jobID must be digits only.
func SrunLine(jobID string) (string, error) {
	if _, err := strconv.ParseUint(jobID, 10, 64); err != nil || strings.TrimLeft(jobID, "0123456789") != "" {
		return "", fmt.Errorf("a job ID must be digits only")
	}
	return "srun --overlap --jobid=" + jobID + " --ntasks=1 --time=1 bash -s", nil
}

// ListScript lists, on the job's node, every process this user owns: its PID,
// its parent's, its start time (field 22 of /proc/<pid>/stat), what its
// environment and its cgroup say of the job, and its command line, base64.
// Its first line names the listing's own shell ("self <pid>"), so that
// ParseListing leaves the listing's own processes out. Processes of other
// users are skipped unread.
//
// The environment field is 1 when it carries the job's SLURM_JOB_ID, 0 when it
// can be read and does not, and u when it cannot be read. A process that is
// not dumpable keeps its /proc/<pid> owned by its effective user, and so is
// listed, but its environ is readable by root alone (proc(5)); setuid
// Apptainer's master is such a process, since it changed its user IDs
// (Apptainer 1.4.5, cmd/starter/c/starter.c, set_master_privileges), and
// the engine runs --cleanenv, so neither carries SLURM_JOB_ID (issue #445).
// The cgroup fields: 1 when a path segment of /proc/<pid>/cgroup is the
// job's job_<id> (Slurm's cgroup plugins put every task of a job there, under
// v1 and v2), the segment last or followed by "/"; and the cgroup's form, j
// when any of its segments is job_<digits>, of any job, o otherwise. The form
// never carries the number.
func ListScript(jobID string) string { return listScript("/proc", jobID) }

// listScript is ListScript over a process tree at proc, so that a test can
// give it one of its own.
func listScript(proc, jobID string) string {
	// No set -f: the loop's glob needs globbing, and the stat fields after the
	// command name hold no glob character.
	// A redirection that fails is reported on the shell's standard error, so
	// 2>/dev/null comes before it: an unreadable environ says u, silently.
	return `set -u
job=` + jobID + `
proc='` + proc + `'
echo "self $$"
for d in "$proc"/[0-9]*; do
  [ -O "$d" ] || continue
  st=$(cat "$d/stat" 2>/dev/null) || continue
  rest=${st##*) }
  set -- $rest
  [ $# -ge 20 ] || continue
  ppid=$2 start=${20}
  if envs=$(tr '\0' '\n' 2>/dev/null < "$d/environ"); then
    env=0
    if printf '%s\n' "$envs" | grep -qx "SLURM_JOB_ID=$job"; then env=1; fi
  else
    env=u
  fi
  cg=0 form=o
  if cgs=$(cat 2>/dev/null < "$d/cgroup"); then
    if printf '%s\n' "$cgs" | grep -Eq "/job_$job(/|\$)"; then cg=1; fi
    if printf '%s\n' "$cgs" | grep -Eq '/job_[0-9]+(/|$)'; then form=j; fi
  fi
  cmd=$(base64 -w0 < "$d/cmdline" 2>/dev/null) || continue
  [ -n "$cmd" ] || continue
  echo "proc ${d#"$proc"/} $ppid $start $env $cg $form $cmd"
done
echo "listed"
`
}

// NodeProc is one process the listing showed.
type NodeProc struct {
	PID, PPID int
	// Start is the process's start time in clock ticks since boot: with the
	// PID, it names one process, not a later one that reused the PID.
	Start string
	// InJob is true when its environment carries the job's SLURM_JOB_ID or
	// its cgroup is the job's (Env, CgroupJob). A process with neither is
	// still of the job when it descends from one that has one (PickEngine).
	InJob bool
	// Env is what its environment said of the job.
	Env EnvState
	// CgroupJob is true when its cgroup holds the job's segment job_<id>;
	// CgroupJobForm when it holds a segment job_<digits> of any job.
	CgroupJob, CgroupJobForm bool
	Argv                     []string
	// cmdline is the raw command line, base64, as the kill script reads it again.
	cmdline string
}

// EnvState is what a process's environment said of the job. The zero value
// is a process the listing did not describe (one a test wrote by hand).
type EnvState uint8

const (
	EnvNotRead EnvState = iota
	// EnvJob: it carries the job's SLURM_JOB_ID.
	EnvJob
	// EnvOther: it can be read and does not.
	EnvOther
	// EnvUnreadable: it cannot be read (a process that is not dumpable).
	EnvUnreadable
)

// envStates are the listing's environment fields.
var envStates = map[string]EnvState{"1": EnvJob, "0": EnvOther, "u": EnvUnreadable}

// ErrListing is a listing that did not come back whole.
var ErrListing = errors.New("the node's process listing is incomplete or malformed")

// ParseListing reads ListScript's output. It fails unless the listing began
// with its own PID ("self <pid>"), ended (its last line is "listed") and
// every process line parses. The listing's own processes, its shell and what
// that shell started, are left out.
func ParseListing(out []byte) ([]NodeProc, error) {
	var procs []NodeProc
	self, ended := 0, false
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	sc.Buffer(make([]byte, 1<<20), 4<<20)
	for sc.Scan() {
		l := strings.TrimSpace(sc.Text())
		if l == "" {
			continue
		}
		if ended {
			return nil, ErrListing
		}
		f := strings.Fields(l)
		if self == 0 {
			n, err := strconv.Atoi(strings.TrimPrefix(l, "self "))
			if len(f) != 2 || f[0] != "self" || err != nil || n <= 0 {
				return nil, ErrListing
			}
			self = n
			continue
		}
		if l == "listed" {
			ended = true
			continue
		}
		p, err := parseProc(f)
		if err != nil {
			return nil, err
		}
		procs = append(procs, p)
	}
	if err := sc.Err(); err != nil || !ended {
		return nil, ErrListing
	}
	return withoutTreeOf(procs, self), nil
}

// parseProc reads one process line: proc, the PID, the parent's, the start
// time, the environment field, the cgroup's two and the command line.
func parseProc(f []string) (NodeProc, error) {
	if len(f) != 8 || f[0] != "proc" || (f[5] != "0" && f[5] != "1") || (f[6] != "j" && f[6] != "o") || (f[5] == "1" && f[6] != "j") {
		return NodeProc{}, ErrListing
	}
	env, ok := envStates[f[4]]
	pid, err1 := strconv.Atoi(f[1])
	ppid, err2 := strconv.Atoi(f[2])
	_, err3 := strconv.ParseUint(f[3], 10, 64)
	raw, err4 := base64.StdEncoding.DecodeString(f[7])
	if !ok || err1 != nil || err2 != nil || err3 != nil || err4 != nil || pid <= 0 {
		return NodeProc{}, ErrListing
	}
	cg := f[5] == "1"
	return NodeProc{PID: pid, PPID: ppid, Start: f[3], InJob: env == EnvJob || cg, Env: env, CgroupJob: cg, CgroupJobForm: f[6] == "j",
		Argv: splitArgv(raw), cmdline: f[7]}, nil
}

// withoutTreeOf is procs without the process self and its descendants.
func withoutTreeOf(procs []NodeProc, self int) []NodeProc {
	t := newProcTree(procs, nil, nil)
	out := make([]NodeProc, 0, len(procs))
	for _, p := range procs {
		if !t.descendsFrom(p, self) {
			out = append(out, p)
		}
	}
	return out
}

// splitArgv is a /proc cmdline's arguments: NUL-separated, with a final NUL.
func splitArgv(raw []byte) []string {
	s := strings.TrimSuffix(string(raw), "\x00")
	if s == "" {
		return nil
	}
	return strings.Split(s, "\x00")
}

// isApptainer reports whether a process is Apptainer's (kindOf): its
// program's base name names apptainer, singularity or the starter, the
// starter's shim has renamed itself appinit (sinit in SingularityCE), or the
// master has taken the runtime parent's title.
func isApptainer(argv []string) bool {
	switch kindOf(argv) {
	case kindApptainer, kindStarter, kindRuntimeParent:
		return true
	}
	return false
}

// MatchesCommand reports whether a process runs the launch's command (review
// of PR 255, B2): its program, then exactly the launch's arguments, each equal
// (inside the container a path is the launch's own). The program is the
// launch's first word: a bare name is any absolute path whose base name it is
// (the PATH lookup), or the name itself; an absolute path is that path. It
// runs directly, or in the one interpreter form the engine's console script
// gives, a Python interpreter (python, python3 or python3.N, by any path)
// with the interpreter's options before the program's path: those of no value
// (-b, -B, -d, -E, -I, -O, -P, -q, -R, -s, -S, -u, -v, any of them together),
// and -X and -W with their value, attached or as the next word (issue #445,
// candidate (c)): python3 -X faulthandler /usr/local/bin/vllm serve .... -c
// and -m are not the program's script, and end the form. Any other program, a
// model, a port or a socket of another value, and an argument more or less,
// is another process.
func MatchesCommand(argv, command []string) bool {
	if len(command) < 2 || len(argv) == 0 {
		return false
	}
	rest := argv
	if !isProgram(rest[0], command[0]) {
		if !pythonRE.MatchString(filepath.Base(rest[0])) {
			return false
		}
		rest = skipPyOptions(rest[1:])
		if len(rest) == 0 || !strings.HasPrefix(rest[0], "/") || !isProgram(rest[0], command[0]) {
			return false
		}
	}
	if len(rest) != len(command) {
		return false
	}
	for i := 1; i < len(command); i++ {
		if rest[i] != command[i] {
			return false
		}
	}
	return true
}

// pythonRE, pyFlagsRE and pyValueRE are the interpreter form MatchesCommand
// accepts: the interpreter, its options of no value, and -X or -W with the
// value attached.
var (
	pythonRE  = regexp.MustCompile(`^python(3(\.[0-9]+)?)?$`)
	pyFlagsRE = regexp.MustCompile(`^-[bBdEIOPqRsSuv]+$`)
	pyValueRE = regexp.MustCompile(`^-[XW].+$`)
)

// skipPyOptions is the words after a Python interpreter's options: those of
// no value, and -X and -W with their value. It stops at anything else, -c
// and -m included.
func skipPyOptions(rest []string) []string {
	for len(rest) > 0 {
		switch {
		case pyFlagsRE.MatchString(rest[0]), pyValueRE.MatchString(rest[0]):
			rest = rest[1:]
		case (rest[0] == "-X" || rest[0] == "-W") && len(rest) > 1:
			rest = rest[2:]
		default:
			return rest
		}
	}
	return rest
}

// isProgram reports whether a process's argument names the launch's program.
func isProgram(arg, program string) bool {
	if strings.Contains(program, "/") {
		return arg == program
	}
	return arg == program || (strings.HasPrefix(arg, "/") && filepath.Base(arg) == program)
}

// ErrEngineCount is a listing in which the run did not find exactly one
// engine. Shape is the job's process tree, redacted (Shape).
type ErrEngineCount struct {
	N     int
	Shape string
}

func (e ErrEngineCount) Error() string {
	msg := fmt.Sprintf("the node shows %d engine processes of the run's job, want exactly 1: nothing was killed", e.N)
	if e.Shape != "" {
		msg += "; " + e.Shape
	}
	return msg
}

// PickEngine finds the one engine of the run's job in a listing: the process
// the agent launched through Apptainer (issue #445). It is
//
//  1. of the job: InJob, or a descendant of a process that is, since a child
//     of a job's process is the job's, though the engine runs --cleanenv and
//     a cgroup may not name the job;
//  2. under Apptainer: its parent is Apptainer's (isApptainer), or a shell
//     whose parent is, and so on up, each of them of the job; any other
//     process between them ends the walk. So vLLM's own children (its
//     EngineCore or workers, even a forked copy with the same command line)
//     are never the engine: the process to kill is the one whose exit makes
//     apptainer exit, which the agent reports and the worker rung restarts;
//  3. running the launch's command (match: MatchesCommand on a cluster; a
//     stand-in gives its own, for the fake Apptainer's translated paths).
//
// Anything but exactly one is an error that carries the redacted shape of the
// job's process tree. Every listed process is this user's (ListScript), and
// the kill checks the process again (KillHelper).
func PickEngine(procs []NodeProc, command []string, match func(argv, command []string) bool) (NodeProc, error) {
	t := newProcTree(procs, command, match)
	var found []NodeProc
	for _, p := range procs {
		if t.isEngine(p) {
			found = append(found, p)
		}
	}
	if len(found) != 1 {
		return NodeProc{}, ErrEngineCount{N: len(found), Shape: t.shape()}
	}
	return found[0], nil
}

// FindEngine is the worker fault's first step (issue #445): one srun step in
// the run's job lists the node's processes (ListScript), and PickEngine picks
// the engine. It returns the engine and the job's process tree, redacted
// (Shape), for the row's log; an error never carries the listing, and a
// count that is not one carries the shape.
func FindEngine(ctx context.Context, run slurm.Runner, jobID string, command []string, match func(argv, command []string) bool, timeout time.Duration) (NodeProc, string, error) {
	line, err := SrunLine(jobID)
	if err != nil {
		return NodeProc{}, "", err
	}
	res, err := run.Run(ctx, slurm.Command{Line: line, Stdin: []byte(ListScript(jobID)), Timeout: timeout})
	if err != nil || res.ExitCode != 0 {
		return NodeProc{}, "", fmt.Errorf("the srun step that lists the job's processes failed (exit %d, transport error %v)", res.ExitCode, err != nil)
	}
	procs, err := ParseListing(res.Stdout)
	if err != nil {
		return NodeProc{}, "", err
	}
	shape := Shape(procs, command, match)
	eng, err := PickEngine(procs, command, match)
	return eng, shape, err
}

// KillEngine is the worker fault's second step: one srun step in the run's
// job kills the engine FindEngine picked (KillScript), through its pidfd. It
// fails unless the step says it sent the signal.
func KillEngine(ctx context.Context, run slurm.Runner, jobID string, eng NodeProc, timeout time.Duration) error {
	line, err := SrunLine(jobID)
	if err != nil {
		return err
	}
	res, err := run.Run(ctx, slurm.Command{Line: line, Stdin: []byte(KillScript(eng)), Timeout: timeout})
	if err != nil || res.ExitCode != 0 || !Killed(res.Stdout, eng) {
		return fmt.Errorf("the srun step that kills the engine did not (exit %d, transport error %v, said %q)", res.ExitCode, err != nil, res.Stdout)
	}
	return nil
}

// KillScript kills the process p with SIGKILL through a pidfd, if the
// process at its PID is still the one listed: owned by this user, with the
// same start time and the same command line (KillHelper). It prints
// "killed <pid>" when it sent the signal; "changed" or "gone" when the process
// is not the one listed or has ended; and "no-pidfd" when the node has no
// python3 or cannot open a pidfd, so that nothing can be killed safely: the
// row then fails, killing nothing.
func KillScript(p NodeProc) string {
	return `set -u
set -f
command -v python3 >/dev/null 2>&1 || { echo no-pidfd; exit 0; }
exec python3 -I - ` + strconv.Itoa(p.PID) + ` ` + p.Start + ` ` + p.cmdline + ` <<'HB_KILL_HELPER'
` + KillHelper + `HB_KILL_HELPER
`
}

// KillHelper is the kill's Python program: pidfd_open(2) on the PID, then the
// identity of the process at the PID in /proc (owner, start time, command
// line), then the pidfd's own PID in /proc/self/fdinfo (it is -1 once the
// process it names has been reaped, so a PID now used by another process does
// not pass), then pidfd_send_signal(2). The pidfd names the process that had
// the PID when it was opened: a process that passed the check and exited
// before the signal is not replaced by a later one with its PID, and the
// signal then fails with ESRCH. The syscall numbers, 434 and 424, are the same
// on x86_64 and aarch64 (the generic table, Linux 5.3 and later); Python 3.9
// and later have the calls themselves. It prints one word and exits 0.
// main's after_check is the seam a test uses to run code between the check
// and the signal.
const KillHelper = `import base64, ctypes, errno, os, signal, sys

PIDFD_OPEN, PIDFD_SEND_SIGNAL = 434, 424


def pidfd_open(pid):
    if hasattr(os, "pidfd_open"):
        return os.pidfd_open(pid)
    libc = ctypes.CDLL(None, use_errno=True)
    fd = libc.syscall(PIDFD_OPEN, ctypes.c_int(pid), ctypes.c_uint(0))
    if fd < 0:
        e = ctypes.get_errno()
        raise OSError(e, os.strerror(e))
    return fd


def pidfd_send_signal(fd, sig):
    if hasattr(signal, "pidfd_send_signal"):
        return signal.pidfd_send_signal(fd, sig)
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.syscall(PIDFD_SEND_SIGNAL, ctypes.c_int(fd), ctypes.c_int(sig), None, ctypes.c_uint(0)) != 0:
        e = ctypes.get_errno()
        raise OSError(e, os.strerror(e))


def same(pid, start, cmdline):
    d = "/proc/%d" % pid
    if os.stat(d).st_uid != os.getuid():
        return False
    with open(d + "/stat") as f:
        st = f.read()
    rest = st[st.rindex(")") + 2:].split()
    if len(rest) < 20 or rest[19] != start:
        return False
    with open(d + "/cmdline", "rb") as f:
        return base64.b64encode(f.read()).decode() == cmdline


def fd_pid(fd):
    with open("/proc/self/fdinfo/%d" % fd) as f:
        for line in f:
            if line.startswith("Pid:"):
                return int(line.split()[1])
    return None


def main(argv, after_check=None):
    pid, start, cmdline = int(argv[0]), argv[1], argv[2]
    try:
        fd = pidfd_open(pid)
    except OSError as e:
        return "gone" if e.errno == errno.ESRCH else "no-pidfd"
    try:
        if not same(pid, start, cmdline):
            return "changed"
    except (OSError, ValueError):
        return "gone"
    got = fd_pid(fd)
    if got is None:
        return "no-pidfd"
    if got != pid:
        return "gone"
    if after_check is not None:
        after_check()
    try:
        pidfd_send_signal(fd, signal.SIGKILL)
    except OSError as e:
        return "gone" if e.errno == errno.ESRCH else "no-pidfd"
    return "killed %d" % pid


if __name__ == "__main__":
    print(main(sys.argv[1:]))
`

// Killed reports whether KillScript's output says it sent the signal to p.
func Killed(out []byte, p NodeProc) bool {
	return strings.TrimSpace(string(out)) == "killed "+strconv.Itoa(p.PID)
}

// CommandDigest is a short digest of a command, for the log in place of the
// command itself.
func CommandDigest(argv []string) string {
	s := sha256.Sum256([]byte(strings.Join(argv, "\x00")))
	return fmt.Sprintf("%x", s[:6])
}

// RestartTimes are the times of the worker-rung fault, posted with the row
// (the science agent's condition 5): the kill; the controller first showing
// the exit the agent reported (Healthy False with PartExited); the restart it
// decided (status.restarts); and the Instance ready again.
type RestartTimes struct {
	Killed       time.Time `json:"killed"`
	AgentNoticed time.Time `json:"agentNoticed"`
	Restarted    time.Time `json:"restarted"`
	ReadyAgain   time.Time `json:"readyAgain"`
}

// Line is the times as one line to post: each step's delay from the kill.
func (r RestartTimes) Line() string {
	since := func(t time.Time) string {
		if t.IsZero() || r.Killed.IsZero() {
			return "not seen"
		}
		return t.Sub(r.Killed).Round(time.Second).String()
	}
	return fmt.Sprintf("worker-rung restart, from the engine's kill: the exit shown %s, the restart decided %s, ready again %s",
		since(r.AgentNoticed), since(r.Restarted), since(r.ReadyAgain))
}
