package fakeslurm

// New. The call log: one entry per simple command that ran, never the content
// of standard input.

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

func TestCallLogRecordsEachSimpleCommand(t *testing.T) {
	r := newRig(t, Config{})
	r.clock.Advance(7 * time.Second)
	r.runAs(r.s, "umask 077 && mkdir -p /d && cat > /d/f", []byte("12345"))
	got := r.p.Calls()
	want := []Call{
		{User: "example-user", Words: []string{"umask", "077"}, Time: t0.Add(7 * time.Second)},
		{User: "example-user", Words: []string{"mkdir", "-p", "/d"}, Time: t0.Add(7 * time.Second)},
		{User: "example-user", Words: []string{"cat", ">", "/d/f"}, StdinLen: 5, Time: t0.Add(7 * time.Second)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Calls\n got  %+v\n want %+v", got, want)
	}
}

func TestCountAndCountFor(t *testing.T) {
	r := newRig(t, Config{})
	bob := loginAs(r.p, "bob")
	r.sbatch()
	r.sbatch()
	r.sbatchAs(bob)
	r.must("printenv HOME")
	for _, tc := range []struct {
		user, word string
		want       int
	}{
		{"", "sbatch", 3},
		{"example-user", "sbatch", 2},
		{"bob", "sbatch", 1},
		{"nobody", "sbatch", 0},
		{"", "printenv", 1},
		{"", "squeue", 0},
	} {
		if got := r.p.CountFor(tc.user, tc.word); got != tc.want {
			t.Errorf("CountFor(%q, %q) = %d, want %d", tc.user, tc.word, got, tc.want)
		}
	}
	if got := r.p.Count("sbatch"); got != 3 {
		t.Errorf("Count(sbatch) = %d, want 3", got)
	}
}

// The credential's bytes go through standard input and must be in no log, no
// reply and no error of the fake.
func TestStdinIsNeverStored(t *testing.T) {
	r := newRig(t, Config{})
	secret := "example-credential-9d41c7e2"
	r.p.FailNext(1, slurm.ErrTransport)
	if _, err := r.s.Run(context.Background(), slurm.Command{Line: "cat > /d/cred", Stdin: []byte(secret)}); err == nil {
		t.Fatal("the faulted command returned no error")
	}
	var seen []string
	for _, line := range []string{
		"cat > /missing/dir/cred",        // fails: no directory
		"mkdir -p /d && cat > /d/cred",   // works
		"sha256sum /d/cred",              // prints a digest, not the content
		"cat > /d/cred && test -f /nope", // the chain fails after the write
		"cat > /d/cred g",                // a syntax error
		"squeue --me --json && sacct --json && printenv HOME",
	} {
		out, errOut, _ := r.runAs(r.s, line, []byte(secret))
		seen = append(seen, out, errOut)
	}
	for _, c := range r.p.Calls() {
		seen = append(seen, c.User, strings.Join(c.Words, " "))
	}
	for _, f := range r.p.Faulted() {
		seen = append(seen, f.User, strings.Join(f.Words, " "), f.Err.Error())
	}
	for _, s := range seen {
		if strings.Contains(s, secret) {
			t.Errorf("the secret appears in %q", s)
		}
	}
	// Only the length is kept, and only for the cat that ran.
	for _, c := range r.p.Calls() {
		wantLen := 0
		if c.Words[0] == "cat" {
			wantLen = len(secret)
		}
		if c.StdinLen != wantLen {
			t.Errorf("call %v has StdinLen %d, want %d", c.Words, c.StdinLen, wantLen)
		}
	}
	if string(r.p.Files()["/d/cred"].Content) != secret {
		t.Errorf("the file system does not hold the content: it is the one place where it belongs")
	}
}

func TestCallsReturnsACopy(t *testing.T) {
	r := newRig(t, Config{})
	r.must("printenv HOME")
	calls := r.p.Calls()
	calls[0].Words[0] = "changed"
	calls[0].User = "changed"
	if got := r.p.Calls()[0]; got.Words[0] != "printenv" || got.User != "example-user" {
		t.Errorf("the log changed through the copy: %+v", got)
	}
}

func TestOnlyCommandsThatRanAreLogged(t *testing.T) {
	r := newRig(t, Config{})
	r.run("mkdir -p /x; mkdir -p /y") // syntax error
	if n := len(r.p.Calls()); n != 0 {
		t.Errorf("a line with a syntax error logged %d calls, want 0", n)
	}
	r.run("nosuchcommand")
	if n := r.p.Count("nosuchcommand"); n != 1 {
		t.Errorf("an unknown command began to run once; logged %d times", n)
	}
}
