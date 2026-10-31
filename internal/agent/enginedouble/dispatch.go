package enginedouble

import (
	"fmt"
	"os"
	"path/filepath"
)

// Dispatch makes a test binary the fake programs. Call it first thing in
// TestMain, before the test flags are parsed. It reads the environment
// variable HB_TEST_ROLE and the base name of os.Args[0]:
//
//   - HB_TEST_ROLE=engine runs Main; HB_TEST_ROLE=engine-child runs ChildMain;
//   - a base name "apptainer" runs ApptainerMain, and "ssh" runs SSHMain.
//
// Each of them exits the process. Anything else returns, and the tests run.
func Dispatch() {
	args := os.Args[1:]
	switch os.Getenv(EnvRole) {
	case RoleEngine:
		os.Exit(Main(args))
	case RoleEngineChild:
		os.Exit(ChildMain())
	}
	switch filepath.Base(os.Args[0]) {
	case "apptainer":
		os.Exit(ApptainerMain(args))
	case "ssh":
		os.Exit(SSHMain(args))
	}
}

// Install creates dir/apptainer and dir/ssh as symlinks to the running binary,
// so that a directory on the PATH, or a path in the agent's settings, makes
// the agent start the fakes. An existing file of those names is replaced.
func Install(dir string) error {
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("the running binary: %w", err)
	}
	for _, name := range []string{"apptainer", "ssh"} {
		link := filepath.Join(dir, name)
		if err := os.Remove(link); err != nil && !os.IsNotExist(err) {
			return err
		}
		if err := os.Symlink(exe, link); err != nil {
			return err
		}
	}
	return nil
}
