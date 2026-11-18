package fakeslurm

// New. Tests of srun's --chdir and of RunMode.NoComputeHome (srun.go,
// run.go), added for HBEP-4's and HBEP-6's amendment of 18 Nov 2026 (#735):
// the relay's srun line names the Allocation directory with --chdir, because
// srun from the login node starts the step in the login home, which Sano's
// compute nodes lack ("couldn't chdir"). The expected values are the
// amendment's rules and the messages and paths written out here; none is read
// back from the fake.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

// chdirRig is a run rig with a running job and a directory on the node
// beside the home: the stage the step is told to start in.
func chdirRig(t *testing.T, mode RunMode) (r *runRig, id int, stage string) {
	t.Helper()
	r = newRunRig(t, mode)
	stage = filepath.Join(r.root, "scratch", "example-user", "stage")
	if err := os.MkdirAll(stage, 0o755); err != nil {
		t.Fatal(err)
	}
	return r, r.holdJob(r.s, r.root+"/release"), stage
}

// srunPwd runs "srun ..., pwd" with the given options, buffered (Run).
func srunPwd(r *runRig, id int, options string) (out, errOut string, code int) {
	return r.run("srun " + options + " --jobid=" + itoa(id) + " pwd")
}

// Without NoComputeHome, a step with --chdir starts in that directory, wherever
// the option stands among the others, and one without it starts in the home
// as before.
func TestSrunChdirSetsTheStepsDirectory(t *testing.T) {
	r, id, stage := chdirRig(t, RunMode{})
	for _, options := range []string{
		"-u --overlap --ntasks=1 --chdir=" + stage,
		"--chdir=" + stage + " -u --overlap --ntasks=1",
		"--overlap --chdir=" + stage,
	} {
		if out, errOut, code := srunPwd(r, id, options); out != stage+"\n" || errOut != "" || code != 0 {
			t.Errorf("srun %s: stdout %q, stderr %q, exit %d; want %q, none, 0", options, out, errOut, code, stage+"\n")
		}
	}
	if out, errOut, code := srunPwd(r, id, "--overlap"); out != r.home+"\n" || errOut != "" || code != 0 {
		t.Errorf("srun without --chdir: stdout %q, stderr %q, exit %d; want the home %q, none, 0", out, errOut, code, r.home+"\n")
	}
}

// With NoComputeHome, a step without --chdir fails as Sano's does, with the
// login home in the message, exit 1 and nothing started; one with --chdir to a
// directory that exists runs in it. Both srun paths agree: Run, and Stream, the
// path of HBEP-6's relay.
func TestNoComputeHomeFailsAStepWithoutChdir(t *testing.T) {
	r, id, stage := chdirRig(t, RunMode{NoComputeHome: true})
	marker := filepath.Join(r.root, "marker")
	want := "srun: error: couldn't chdir to '" + r.home + "': No such file or directory\n"
	touch := "sh -c " + quote("echo ran > "+marker)

	res, err := r.s.Run(context.Background(), slurm.Command{Line: "srun -u --overlap --jobid=" + itoa(id) + " --ntasks=1 " + touch})
	if err != nil || res.ExitCode != 1 || string(res.Stderr) != want || len(res.Stdout) != 0 {
		t.Errorf("Run without --chdir: %v, exit %d, stdout %q, stderr %q; want exit 1, nothing, %q", err, res.ExitCode, res.Stdout, res.Stderr, want)
	}
	var stdout, stderr strings.Builder
	code, err := r.s.Stream(context.Background(), streamLine(id, true, touch), strings.NewReader(""), &stdout, &stderr)
	if err != nil || code != 1 || stderr.String() != want || stdout.Len() != 0 {
		t.Errorf("Stream without --chdir: %v, exit %d, stdout %q, stderr %q; want exit 1, nothing, %q", err, code, stdout.String(), stderr.String(), want)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a step without --chdir ran in the no-home mode: marker stat = %v", err)
	}

	if out, errOut, code := srunPwd(r, id, "-u --overlap --ntasks=1 --chdir="+stage); out != stage+"\n" || errOut != "" || code != 0 {
		t.Errorf("Run with --chdir: stdout %q, stderr %q, exit %d; want %q, none, 0", out, errOut, code, stage+"\n")
	}
	stdout.Reset()
	stderr.Reset()
	code, err = r.s.Stream(context.Background(), "srun -u --overlap --jobid="+itoa(id)+" --ntasks=1 --chdir="+stage+" pwd", strings.NewReader(""), &stdout, &stderr)
	if err != nil || code != 0 || stdout.String() != stage+"\n" || stderr.Len() != 0 {
		t.Errorf("Stream with --chdir: %v, exit %d, stdout %q, stderr %q; want 0, %q, none", err, code, stdout.String(), stderr.String(), stage+"\n")
	}
}

// A --chdir to a directory that is not there fails the same way, in either
// mode, and starts nothing.
func TestSrunChdirToAMissingDirectoryFails(t *testing.T) {
	for _, noHome := range []bool{false, true} {
		r, id, _ := chdirRig(t, RunMode{NoComputeHome: noHome})
		missing := filepath.Join(r.root, "scratch", "example-user", "no-such-dir")
		marker := filepath.Join(r.root, "marker")
		line := "srun -u --overlap --jobid=" + itoa(id) + " --ntasks=1 --chdir=" + missing + " sh -c " + quote("echo ran > "+marker)
		want := "srun: error: couldn't chdir to '" + missing + "': No such file or directory\n"
		if out, errOut, code := r.run(line); out != "" || errOut != want || code != 1 {
			t.Errorf("NoComputeHome %v: stdout %q, stderr %q, exit %d; want nothing, %q, 1", noHome, out, errOut, code, want)
		}
		if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("NoComputeHome %v: the step ran: marker stat = %v", noHome, err)
		}
		// A file is not a directory.
		file := filepath.Join(r.root, "a-file")
		if err := os.WriteFile(file, nil, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, errOut, code := srunPwd(r, id, "--overlap --chdir="+file); code != 1 || !strings.Contains(errOut, "couldn't chdir to '"+file+"'") {
			t.Errorf("NoComputeHome %v: --chdir to a file: stderr %q, exit %d; want the chdir refusal, 1", noHome, errOut, code)
		}
	}
}

// parseSrun reads --chdir in its one form and refuses the others by name.
func TestParseSrunChdir(t *testing.T) {
	a, msg := parseSrun([]string{"--overlap", "--chdir=/s/alloc/hb-x", "--jobid=5", "prog", "--chdir=/other"})
	if msg != "" || a.chdir != "/s/alloc/hb-x" || a.program != "prog" || len(a.args) != 1 || a.args[0] != "--chdir=/other" {
		t.Errorf("parseSrun = %+v, %q; want --chdir /s/alloc/hb-x, program prog with its own --chdir as an argument", a, msg)
	}
	for _, c := range []struct{ name, arg, want string }{
		{"two words", "--chdir", `not "--chdir"`},
		{"relative", "--chdir=alloc/hb-x", "clean absolute directory"},
		{"empty", "--chdir=", "clean absolute directory"},
		{"unclean", "--chdir=/s/../alloc", "clean absolute directory"},
	} {
		if _, msg := parseSrun([]string{"--overlap", "--jobid=5", c.arg, "prog"}); !strings.Contains(msg, c.want) {
			t.Errorf("%s: parseSrun(%q) message %q; want it to hold %q", c.name, c.arg, msg, c.want)
		}
	}
}
