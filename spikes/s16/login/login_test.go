// Package login is spike S16's login node, built as a test binary
// (go test -c -o s16login.test ./login) because internal/rehearsal may be
// linked only by _test.go files.
//
// With S16_LOGIN=1 the binary runs internal/rehearsal's login node, unchanged:
// a user-level OpenSSH sshd in inetd mode behind a kernel-chosen 127.0.0.1
// port (the guarded sshd of #308's fix), whose client key forces this binary
// again as the relay client (rehearsal.Dispatch), in front of a run-mode
// fakeslurm Platform. It submits one job that holds until it is cancelled, so
// every relay's "srun -u --overlap --jobid=<id> --ntasks=1 <agent> relay ..."
// runs as a streamed step of it, and writes the ready file: the credentials in
// ssh_config form (for the Provider's -F file) and the job's ID. On SIGTERM it
// cancels the job and closes the login node and the Platform, which stop every
// process under them.
package login

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// clusterUser is the fake cluster's user name, an example of the shape.
const clusterUser = "example-user"

func TestMain(m *testing.M) {
	rehearsal.Dispatch() // the forced command: returns unless this process is the relay client
	if os.Getenv("S16_LOGIN") == "1" {
		if err := runLogin(os.Args[1:]); err != nil {
			fmt.Fprintln(os.Stderr, "s16login:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func runLogin(args []string) error {
	fs := flag.NewFlagSet("login", flag.ContinueOnError)
	dir := fs.String("dir", "", "a private (mode 700), short scratch directory")
	readyPath := fs.String("ready", "", "the ready file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" || *readyPath == "" {
		return errors.New("-dir and -ready are required")
	}
	root := filepath.Join(*dir, "r")
	home := filepath.Join(root, "home", clusterUser)
	nodeDir := filepath.Join(*dir, "n")
	for _, d := range []string{home, nodeDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return err
		}
	}
	plat := fakeslurm.New(fake.NewClock(time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)), fakeslurm.Config{Run: &fakeslurm.RunMode{Root: root}})
	node, err := rehearsal.StartLoginNode(rehearsal.LoginConfig{Dir: nodeDir, Platform: plat,
		Login: fakeslurm.Login{User: clusterUser, Env: map[string]string{"HOME": home}}})
	if err != nil {
		return errors.Join(err, plat.Close())
	}
	ctx := context.Background()
	res, err := node.Session().Run(ctx, slurm.Command{Line: holdJob(filepath.Join(root, "release"))})
	jobID := strings.TrimSpace(string(res.Stdout))
	if err == nil && res.ExitCode != 0 {
		err = fmt.Errorf("sbatch exited %d: %s", res.ExitCode, strings.TrimSpace(string(res.Stderr)))
	}
	id, cerr := strconv.Atoi(jobID)
	if err == nil && cerr != nil {
		err = fmt.Errorf("sbatch printed %q, not a job ID", jobID)
	}
	if err == nil {
		err = waitRunning(plat, id, 30*time.Second)
	}
	if err == nil {
		err = writeReady(*readyPath, node.SSHConfig(), jobID)
	}
	if err == nil {
		term := make(chan os.Signal, 1)
		signal.Notify(term, syscall.SIGTERM, syscall.SIGINT)
		<-term
	}
	if id > 0 {
		_, _ = node.Session().Run(ctx, slurm.Command{Line: "scancel " + jobID})
	}
	return errors.Join(err, node.Close(), plat.Close())
}

// holdJob is the sbatch line of a job whose shell waits until the release
// file exists or it is cancelled: the Allocation every relay's step runs in.
func holdJob(release string) string {
	return "sbatch --parsable '--wrap=while [ ! -e " + release + " ]; do sleep 1; done'"
}

func waitRunning(plat *fakeslurm.Platform, id int, within time.Duration) error {
	for end := time.Now().Add(within); ; time.Sleep(50 * time.Millisecond) {
		if j, ok := plat.Job(id); ok && j.PID > 0 {
			return nil
		}
		if time.Now().After(end) {
			return fmt.Errorf("job %d did not start within %s", id, within)
		}
	}
}

func writeReady(path, sshConfig, jobID string) error {
	body := fmt.Sprintf("{%q: %q, %q: %q}\n", "ssh_config", sshConfig, "job_id", jobID)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(body), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// The login node's own unit test; the node itself runs only under the driver.
func TestHoldJobWaitsOnTheReleaseFile(t *testing.T) {
	got := holdJob("/r/release")
	if got != "sbatch --parsable '--wrap=while [ ! -e /r/release ]; do sleep 1; done'" {
		t.Fatalf("holdJob = %q", got)
	}
}
