package slurm

// Redaction takes a whole path (#762, Astra's review): a staging directory may hold a colon, an equals sign and a comma, which
// stagingDir's pattern allows, so a path that stops at the first colon leaves the rest in an Allocation's status. The directory
// is the Provider's account data: no status, event or log line holds it. The expected texts are written out by hand; every path
// is invented, and a colon-bearing one carries a unique tail that no other word holds.

import (
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

func TestRedactPathsTakesTheWholePath(t *testing.T) {
	for _, c := range []struct{ name, in, want string }{
		{"a colon inside a quoted path", "Unable to open file '/shared/team:alicexw8m4/run/alloc/hb-x/job.out'", "Unable to open file '<path>'"},
		{"a colon after an equals sign", "--output=/shared/team:alicexw8m4/run/alloc/hb-x/job.out failed", "--output=<path> failed"},
		{"an equals sign and a comma", "open /shared/a=b,c/run/alloc/hb-x/job.out failed", "open <path> failed"},
		{"several colons", "open /shared/a:b:alicexw8m4/run failed", "open <path> failed"},
		{"the colon that ends the path is Slurm's own", "cannot read /shared/team:alicexw8m4/run: Permission denied", "cannot read <path>: Permission denied"},
		{"a path at the end of the text", "no such file /shared/team:alicexw8m4/run", "no such file <path>"},
		{"a double-quoted path", `error: "/shared/team:alicexw8m4/run" is not writable`, `error: "<path>" is not writable`},
		{"two paths", "from /shared/team:alicexw8m4/a to /shared/b:cw9/c", "from <path> to <path>"},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := redactPaths(c.in); got != c.want {
				t.Errorf("redactPaths(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
	// What stays: words with a slash that are not a path, a host with a port, an address, a dotted host name and a version.
	for _, keep := range []string{
		"Invalid account or account/partition combination specified", "Socket timed out on send/recv operation",
		"v0.0.42", "plugin data_parser/v0.0.42",
	} {
		if got := redactPaths(keep); got != keep {
			t.Errorf("redactPaths(%q) = %q, want it kept", keep, got)
		}
	}
	if got := redactPaths("connect to host:node-7:6817 at 10.1.2.3 and login.example.org failed"); got != "connect to host:<host> at <address> and <host> failed" {
		t.Errorf("a host, an address and a dotted name: %q", got)
	}
}

// A submission Slurm refuses with a text that echoes the output path under a staging directory with a colon: the error holds no
// part of the path.
func TestARefusedSubmissionNamesNoPartOfAColonPath(t *testing.T) {
	const dir = "/shared/team:alicexw8m4/run"
	stderr := "sbatch: error: Batch job submission failed: Unable to open file '" + dir + "/alloc/hb-x/job.out'\n"
	_, outcome, err := classifySubmit(Result{ExitCode: 1, Stderr: []byte(stderr)}, nil)
	if outcome != provider.Rejected || err == nil {
		t.Fatalf("classifySubmit = %s, %v; want Rejected with an error", outcome, err)
	}
	if want := "Batch job submission failed: Unable to open file '<path>'"; err.Error() != want {
		t.Errorf("the error is %q, want %q", err.Error(), want)
	}
	for _, part := range []string{"alicexw8m4", "team", "/run", "shared"} {
		if strings.Contains(err.Error(), part) {
			t.Errorf("the error %q holds %q of the path", err, part)
		}
	}
	// The same through a transient reason and through slurmMessage.
	if _, _, err := classifySubmit(Result{ExitCode: 1, Stderr: []byte("sbatch: error: Batch job submission failed: Socket timed out on send/recv operation " + dir + "/alloc\n")}, nil); err == nil || strings.Contains(err.Error(), "alicexw8m4") {
		t.Errorf("a transient reason that names the path: %v", err)
	}
	if got := slurmMessage("sbatch: error: cannot open " + dir + "/alloc/hb-x/job.err\n"); strings.Contains(got, "alicexw8m4") || got != "sbatch: error: cannot open <path>" {
		t.Errorf("slurmMessage = %q", got)
	}
}
