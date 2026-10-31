package stage

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/procgroup"
)

// runApptainer runs the apptainer binary with args in a process group of its
// own and returns once every member of the group is gone (PR #87 review, Astra
// B3, Opus B4: exec.CommandContext killed Apptainer alone, and a helper it had
// started, the squashfs builder for one, went on writing into staging).
//
// On ctx cancel the group gets TERM, then KILL after StopGrace. A leader that
// exits and leaves a member behind gives that member StopGrace to finish too.
// The caller removes its temporary output only after this returns, so nothing
// still writes to it.
//
// Apptainer's layer cache is a directory of its own under TmpDir for this one
// run, removed with it: the OCI layers of a pull are not needed once the image
// is written, and kept beside the images they pile up (Opus nit).
func (s *Stager) runApptainer(ctx context.Context, args []string) error {
	cache, err := os.MkdirTemp(s.c.TmpDir, "hb-apptainer-cache-")
	if err != nil {
		return err
	}
	defer removeAll(cache)
	g, err := procgroup.Start(procgroup.Spec{
		Path: s.c.Apptainer,
		Args: args,
		Env: append(append([]string(nil), s.c.BaseEnv...),
			"APPTAINER_CACHEDIR="+cache,
			"APPTAINER_TMPDIR="+s.c.TmpDir,
		),
		Stdout: s.c.Log,
		Stderr: s.c.Log,
	}, time.Now())
	if err != nil {
		return err
	}
	stop := func() {
		g.Stop(s.c.StopGrace)
		<-g.Done()
	}
	select {
	case <-g.Exited():
		select {
		case <-g.Done():
		case <-ctx.Done():
			stop()
		case <-time.After(s.c.StopGrace):
			s.logf("apptainer %s: the process left members of its group running; stopping them", args[0])
			stop()
		}
	case <-ctx.Done():
		stop()
	}
	if code := g.ExitCode(); code != 0 {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("apptainer %s: %w", args[0], err)
		}
		if code == procgroup.KilledExitCode {
			return fmt.Errorf("apptainer %s: ended by a signal", args[0])
		}
		return fmt.Errorf("apptainer %s: exit code %d", args[0], code)
	}
	return nil
}

// logf writes one line to the log.
func (s *Stager) logf(format string, a ...any) {
	fmt.Fprintf(s.c.Log, "stage: "+format+"\n", a...)
}
