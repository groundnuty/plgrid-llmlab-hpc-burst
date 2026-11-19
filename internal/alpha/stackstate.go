package alpha

// The run's own stack, named for the script's cleanup (the review of PR 809, point 1). The launcher detaches its supervisor (internal/launcher/controllers.go),
// so a test that is stopped by a timeout or a signal, which its Go cleanup never sees, leaves the run's controller RUNNING, and a controller that is running can
// submit another job, or reopen relays, after hack/live-checks.sh has cancelled the run's jobs. The test therefore writes the stack's state directory to a
// mode-600 file of the run's record directory BEFORE it starts the stack, and the script runs `hpcburst down` on exactly that directory, waits for it, and only
// then cancels the jobs by their tags.

import "path/filepath"

// StackStateFile is the file in the run's record directory that names the run's own stack: one line, "state=<the stack's state directory>", the directory
// `hpcburst up --state-dir` and `hpcburst down --state-dir` take. It is the stack of this run alone: the stack's directory is made by this run (stack.ShortDir) and no
// other run's, nor the owner's or the study's, is ever named by it.
const StackStateFile = "stack-state-dir"

// StackStateNote is the file's text for a stack whose scratch directory is dir: the state directory is the "st" below it (stack.StartHost).
func StackStateNote(dir string) string {
	return "state=" + filepath.Join(dir, "st") + "\n"
}
