package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// buildHelper builds this command as hack/build-stage-helper.sh does, with in compiled in, into a directory of the
// test's own.
func buildHelper(t *testing.T, in inputs) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "stagehelper")
	flags := "-X main.image=" + in.image + " -X main.imageDigest=" + in.imageDigest + " -X main.checkpointRepo=" + in.checkpointRepo +
		" -X main.checkpointRev=" + in.checkpointRev + " -X main.loadFormat=" + in.loadFormat
	cmd := exec.Command("go", "build", "-ldflags", flags, "-o", out, ".")
	cmd.Dir = sourceDir(t)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build the stage helper: %v\n%s", err, b)
	}
	return out
}

// The time limit's signal, to the real helper process, built as the script builds it: Slurm sends TERM at a job's time
// limit, and a cold sandbox build on A100 may still be running then. Here the build blocks (a fake Apptainer that marks
// its call and becomes a sleep), the helper gets TERM through a pidfd of its own (SysProcAttr.PidFD, no PID), and it
// writes the signalled line, naming the image step, and exits 0 at once: never the image step's error line, which the
// row would post as a failed step instead of the cold staging past its lease.
func TestTheTimeLimitsSignalIsItsOwnOutcome(t *testing.T) {
	bin := buildHelper(t, goodInputs())
	fx := newFixture(t, stack.StartHub(t).URL)
	called := filepath.Join(fx.root, "apptainer-called")
	slow := filepath.Join(fx.bin, "slow-apptainer")
	if err := os.WriteFile(slow, []byte("#!/bin/sh\n: > "+called+"\nexec sleep 600\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	fx.env["HPCBURST_APPTAINER"] = slow
	env := os.Environ()
	for k, v := range fx.env {
		env = append(env, k+"="+v)
	}
	var errs syncBuf
	fd := -1
	cmd := exec.Command(bin, fx.alloc)
	cmd.Env, cmd.Stdout, cmd.Stderr = env, &errs, &errs
	cmd.SysProcAttr = &syscall.SysProcAttr{PidFD: &fd}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	// The KILL, through the same pidfd, if the test leaves before the helper has ended; then the reap.
	stop := func(why string) {
		_ = unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0)
		<-exited
		t.Fatalf("%s; the helper's stderr:\n%s", why, errs.String())
	}
	for deadline := time.Now().Add(2 * time.Minute); ; time.Sleep(50 * time.Millisecond) {
		if _, err := os.Stat(called); err == nil {
			break
		}
		select {
		case err := <-exited:
			t.Fatalf("the helper ended before its image build began: %v; stderr:\n%s", err, errs.String())
		default:
		}
		if time.Now().After(deadline) {
			stop("the helper's image build did not begin within 2 minutes")
		}
	}
	signalled := time.Now()
	if err := unix.PidfdSendSignal(fd, unix.SIGTERM, nil, 0); err != nil {
		stop("TERM could not be sent: " + err.Error())
	}
	var err error
	select {
	case err = <-exited:
	case <-time.After(time.Minute):
		stop("the helper did not end within a minute of TERM")
	}
	if took := time.Since(signalled); err != nil {
		var ee *exec.ExitError
		t.Fatalf("the helper exited %v (exit error %t) %s after TERM, want 0 once its line is written; stderr:\n%s", err, errors.As(err, &ee), took, errs.String())
	}
	if line := fx.result(t); line != "outcome=signalled step=image" {
		t.Errorf("the helper's line after TERM is %q, want the signalled line of its image step; stderr:\n%s", line, errs.String())
	}
	if _, err := os.Stat(filepath.Join(fx.alloc, resultName+".part")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the temporary result file is left behind: %v", err)
	}
}

// A step that fails once the staging's context is cancelled is signalled whatever the step, and one that fails with the
// context live is the step's error (TestFailureLineNamesTheStep), so the two lines are told apart by the signal alone.
func TestACancelledStagingIsSignalledNotAnError(t *testing.T) {
	fx := newFixture(t, stack.StartHub(t).URL)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var errs syncBuf
	if code := runInputs(ctx, goodInputs(), []string{fx.alloc}, fx.getenv, &errs); code != 0 {
		t.Fatalf("exit %d: %s", code, errs.String())
	}
	if line := fx.result(t); line != "outcome=signalled step=image" {
		t.Errorf("a staging cancelled before its image line is %q, want the signalled line of its image step", line)
	}
}
