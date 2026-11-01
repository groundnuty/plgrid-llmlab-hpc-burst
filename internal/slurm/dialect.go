package slurm

import (
	"fmt"
	"path"
	"regexp"
	"strconv"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The command templates of HBEP-4's Slurm dialect. Each returns one command
// line; every variable value in it has passed CheckValue (or is a number this
// package formats), so the remote shell reads each word literally. The only
// shell syntax is && between simple commands and one "> file" redirection.

// jobIDForm is a Slurm job ID as this package accepts it: decimal digits.
var jobIDForm = regexp.MustCompile(`^[0-9]{1,20}$`)

func checkJobID(id string) error {
	if !jobIDForm.MatchString(id) {
		return fmt.Errorf("%w: job ID %q is not a decimal number", provider.ErrInvalidRequest, id)
	}
	return nil
}

// submission is one sbatch call, after the request has been checked.
type submission struct {
	tag, partition, account string
	hold                    bool
	minutes                 int64
	gpus, cpusPerTask       int32
	memMiB                  int64
	allocDir, agentPath     string // absolute
	agentDigest             string
	// tmpDir and tmpDirFeature are the partition's (checkTmpDir): the job's
	// temporary directory and the node feature it needs; "" when absent.
	tmpDir, tmpDirFeature string
}

// bootstrap is the batch script, the --wrap command (HBEP-4, Submission): it
// checks the staged agent binary's SHA-256 against the digest, then runs the
// binary with the Allocation's directory as its only argument. Nothing else
// runs in the batch script. sbatch writes it into a /bin/sh script, so it is
// plain POSIX shell, with no single quote, because the whole value travels as
// one single-quoted word through the login node's shell.
func bootstrap(agentPath, digest, allocDir string) string {
	return bootstrapIn(agentPath, digest, allocDir, "")
}

// bootstrapIn is bootstrap with the partition's tmpDir: it sets and exports
// TMPDIR first (setTmpDir), then checks the digest, then runs the agent, as
// HBEP-4's Submission orders them. With no tmpDir it is bootstrap's line, byte
// for byte, and the job keeps its own TMPDIR.
func bootstrapIn(agentPath, digest, allocDir, tmpDir string) string {
	check := "[ $(sha256sum " + agentPath + " | cut -c1-64) = " + digest + " ]"
	run := "exec " + agentPath + " " + allocDir
	if tmpDir == "" {
		return check + " && " + run
	}
	return setTmpDir(tmpDir) + " && " + check + " && " + run
}

// setTmpDir sets TMPDIR to the partition's tmpDir, expanded by the job's own
// shell: its variable is one the job sets, such as $MEMFS on Helios, unset on
// the login node (science-agent ruling, 1 Nov 2026, issue #125). The variable is
// written ${NAME:?}, so an unset or empty one stops the job before the agent
// starts, never falling back to the scheduler's TMPDIR. Then the value is held
// to the rule stagingDir's resolution applies (absSafe): an absolute path of
// letters, digits and ._:=,/- with no ".." segment. The directory is created.
func setTmpDir(dir string) string {
	v := dir
	if m := varPrefix.FindStringSubmatch(dir); m != nil {
		v = "${" + m[1] + ":?}" + dir[len(m[0]):]
	}
	return "TMPDIR=" + v +
		` && case "$TMPDIR" in ""|[!/]*|*[!A-Za-z0-9._:=,/-]*|*/../*|*/..) echo "hb: tmpDir resolves to $TMPDIR, not an absolute path of letters, digits and ._:=,/-" >&2; exit 1 ;; esac` +
		` && mkdir -p "$TMPDIR" && export TMPDIR`
}

// submitLine is HBEP-4's submission:
//
//	sbatch --parsable --job-name=<tag> --no-requeue [--hold] --partition=<p>
//	  --account=<a> --time=<minutes> --nodes=1 [--gres=gpu:<n>]
//	  --cpus-per-task=<c> --mem=<m>M [--constraint=<tmpDirFeature>]
//	  --output=<dir>/job.out --error=<dir>/job.err --wrap=<bootstrap>
//
// GPUs are requested untyped, and --gres is left out for a CPU job. The
// account is left out only when the Provider names none. --constraint comes
// after --mem, where HBEP-4 lists it. It is the partition's tmpDirFeature, the
// node feature its tmpDir needs, and is left out when the partition names none
// (science-agent ruling, 1 Nov 2026, issue #125).
func submitLine(s submission) (string, error) {
	w := []string{"sbatch", "--parsable", "--job-name=" + s.tag, "--no-requeue"}
	if s.hold {
		w = append(w, "--hold")
	}
	w = append(w, "--partition="+s.partition)
	if s.account != "" {
		w = append(w, "--account="+s.account)
	}
	w = append(w, "--time="+strconv.FormatInt(s.minutes, 10), "--nodes=1")
	if s.gpus > 0 {
		w = append(w, "--gres=gpu:"+strconv.Itoa(int(s.gpus)))
	}
	w = append(w,
		"--cpus-per-task="+strconv.Itoa(int(s.cpusPerTask)),
		"--mem="+strconv.FormatInt(s.memMiB, 10)+"M",
	)
	if s.tmpDirFeature != "" {
		w = append(w, "--constraint="+s.tmpDirFeature)
	}
	w = append(w,
		"--output="+path.Join(s.allocDir, agentapi.JobOutputName),
		"--error="+path.Join(s.allocDir, agentapi.JobErrorName),
		"--wrap="+bootstrapIn(s.agentPath, s.agentDigest, s.allocDir, s.tmpDir),
	)
	return Line(w...)
}

// queueLine lists every job of the calling user (the Provider's cached
// listing); queueJobLine lists one job.
func queueLine() string { return "squeue --me --json" }

func queueJobLine(id string) string { return "squeue --me --json --jobs=" + id }

// accountingTagLine reads accounting for the tag from seconds ago until now.
// -S is always given, as a relative time: without it sacct starts at midnight,
// and the relative form avoids the login session's time zone (S4). -D keeps
// earlier executions, which show a requeue.
func accountingTagLine(tag string, seconds int64) string {
	if seconds < 60 {
		seconds = 60
	}
	return "sacct --json -X -D --name=" + tag + " -S now-" + strconv.FormatInt(seconds, 10) + " -E now"
}

func accountingJobLine(id string) string { return "sacct --json -X -D -j " + id }

func cancelLine(id string) string { return "scancel " + id }

func releaseLine(id string) string { return "scontrol release " + id }

func printenvLine(name string) string { return "printenv " + name }

func sha256Line(abs string) string { return "sha256sum " + abs }

// writeTempLine creates dir (umask 077, so a new directory is mode 700 and a
// new file mode 600 from the start) and writes standard input to tmp.
func writeTempLine(dir, tmp string) string {
	return And("umask 077", "mkdir -p "+dir, "cat > "+tmp)
}

// installLine sets the mode of tmp and renames it into place. mv -T takes abs
// as the file's own path: without it, a directory at abs would take tmp inside
// it and mv would exit 0, and the file would never be at abs.
func installLine(mode, tmp, abs string) string {
	return And("chmod "+mode+" "+tmp, "mv -fT "+tmp+" "+abs)
}

func removeLine(abs string) string { return "rm -f " + abs }

func removeTreeLine(abs string) string { return "rm -rf " + abs }

func testFilesLine(paths ...string) string {
	lines := make([]string, len(paths))
	for i, p := range paths {
		lines[i] = "test -f " + p
	}
	return And(lines...)
}

// ---- reading replies ----

// transientReasons are sbatch failure texts after which slurmctld may still
// have registered the job: the reply was lost, or the client gave up (S4).
var transientReasons = []string{
	"Socket timed out", "temporarily unavailable", "Unable to contact slurm controller",
	"Communication connection failure", "Zero Bytes were transmitted", "Connection refused",
	"Connection reset", "Transport endpoint", "Slurm backup controller in standby mode",
}

const submissionFailed = "Batch job submission failed:"

// classifySubmit turns an sbatch call's result into HBEP-4's outcome.
//   - Accepted: exit 0 and a job ID on the first line (--parsable prints
//     <jobid>[;<cluster>]).
//   - Rejected: "Batch job submission failed:" with a reason that is not
//     transient, or a command that never started (no connection; sbatch not
//     found on the login node).
//   - Unknown: a time-out, a transport error, a signal, unparseable output, a
//     transient reason, or any other failure.
func classifySubmit(res Result, runErr error) (string, provider.SubmitOutcome, error) {
	if runErr != nil {
		if isNotStarted(runErr) {
			return "", provider.Rejected, fmt.Errorf("sbatch: %w", runErr)
		}
		return "", provider.Unknown, fmt.Errorf("sbatch: %w", runErr)
	}
	if res.ExitCode == 0 {
		first := strings.TrimSpace(strings.SplitN(strings.SplitN(string(res.Stdout), "\n", 2)[0], ";", 2)[0])
		if jobIDForm.MatchString(first) && first != "0" {
			return first, provider.Accepted, nil
		}
		return "", provider.Unknown, fmt.Errorf("sbatch: exit 0 without a job ID on its first line")
	}
	stderr := string(res.Stderr)
	if res.ExitCode == 126 || res.ExitCode == 127 {
		return "", provider.Rejected, fmt.Errorf("sbatch: the command could not run on the login node (exit %d)", res.ExitCode)
	}
	i := strings.Index(stderr, submissionFailed)
	if i < 0 {
		return "", provider.Unknown, fmt.Errorf("sbatch: exit %d: %s", res.ExitCode, slurmMessage(stderr))
	}
	reason := strings.TrimSpace(strings.SplitN(stderr[i+len(submissionFailed):], "\n", 2)[0])
	for _, t := range transientReasons {
		if strings.Contains(stderr, t) {
			return "", provider.Unknown, fmt.Errorf("sbatch: transient failure: %s", redactPaths(reason))
		}
	}
	return "", provider.Rejected, fmt.Errorf("%s %s", submissionFailed, redactPaths(reason))
}

// slurmMessage returns the first line of a Slurm command's standard error
// that carries Slurm's own error text, with absolute paths removed and its
// length capped, for an error message. Raw output never goes into an error:
// it may name a host or a path.
func slurmMessage(stderr string) string {
	for _, l := range strings.Split(stderr, "\n") {
		l = strings.TrimSpace(l)
		if strings.Contains(l, "error") || strings.Contains(l, "Error") {
			return redactPaths(l)
		}
	}
	return "no error text"
}

// What redactPaths removes from Slurm's own error text: an absolute path (a
// "/" at the start of a word, so "account/partition" and "send/recv" stay),
// Slurm's "host:<name>[:<port>]", an IPv4 address, and a dotted host name
// whose last label is letters (so "v0.0.42" and "23.11" stay).
var (
	absPath   = regexp.MustCompile(`(^|[\s'"(=:])/[^\s:'"]*`)
	hostPort  = regexp.MustCompile(`\bhost:[^\s:]+(:[0-9]+)?`)
	ipv4      = regexp.MustCompile(`\b[0-9]{1,3}(\.[0-9]{1,3}){3}\b`)
	dottedFQN = regexp.MustCompile(`\b[A-Za-z][A-Za-z0-9-]*(\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}\b`)
)

// redactPaths removes every absolute path and host name and caps the length.
func redactPaths(s string) string {
	s = absPath.ReplaceAllString(s, "${1}<path>")
	s = hostPort.ReplaceAllString(s, "host:<host>")
	s = ipv4.ReplaceAllString(s, "<address>")
	s = dottedFQN.ReplaceAllString(s, "<host>")
	if len(s) > 300 {
		s = s[:300] + "..."
	}
	return s
}
