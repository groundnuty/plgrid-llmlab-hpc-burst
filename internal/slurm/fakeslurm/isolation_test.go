package fakeslurm

// New. Two logins on one platform: jobs are per user, files and the call log
// are shared, and each Session has its own environment and generation.

import (
	"testing"
)

func TestPerUserViews(t *testing.T) {
	r := newRig(t, Config{})
	alice := r.s
	bob := loginAs(r.p, "bob")
	a1, a2 := r.sbatchAs(alice, "--job-name=alice1"), r.sbatchAs(alice, "--job-name=alice2")
	b1 := r.sbatchAs(bob, "--job-name=bob1")

	list := func(s *Session, line string) []int {
		out, errOut, code := r.runAs(s, line, nil)
		if code != 0 {
			t.Fatalf("%q: exit %d, %q", line, code, errOut)
		}
		return idsOf(t, decode(t, out))
	}
	for _, tc := range []struct {
		who  *Session
		line string
		want []int
	}{
		{alice, "squeue --me --json", []int{a1, a2}},
		{bob, "squeue --me --json", []int{b1}},
		{alice, "squeue --json", []int{a1, a2, b1}}, // without --me: everyone's
		{alice, "sacct --json -X -D", []int{a1, a2}},
		{bob, "sacct --json -X -D", []int{b1}},
		{alice, "sacct --json -X -D -j " + itoa(b1), nil}, // another user's job is not accounted to alice
		{alice, "sacct --json -X -D --name=bob1", nil},
	} {
		got := list(tc.who, tc.line)
		if len(got) != len(tc.want) || (len(got) > 0 && !equalInts(got, tc.want)) {
			t.Errorf("%s: %q lists %v, want %v", tc.who.User(), tc.line, got, tc.want)
		}
	}
	// The job records carry their owner.
	if j, _ := r.p.Job(b1); j.User != "bob" {
		t.Errorf("job %d owner %q, want bob", b1, j.User)
	}
}

func TestSharedFilesAndSeparateEnvironments(t *testing.T) {
	r := newRig(t, Config{})
	alice := r.s
	bob := p2(r)

	// Files are the platform's: what alice writes, bob reads.
	r.runAs(alice, "mkdir -p /shared && cat > /shared/f", []byte("abc"))
	out, _, code := r.runAs(bob, "sha256sum /shared/f", nil)
	if code != 0 || out != sha256ABC+"  /shared/f\n" {
		t.Errorf("bob's sha256sum of alice's file: exit %d, %q", code, out)
	}
	if owner := r.p.Files()["/shared/f"].Owner; owner != "example-user" {
		t.Errorf("file owner %q, want the writer", owner)
	}

	// The environment is the Session's: HOME and SCRATCH differ, and a variable
	// bob's login does not set is unset for him.
	for _, tc := range []struct {
		s    *Session
		line string
		out  string
		code int
	}{
		{alice, "printenv HOME", "/home/example-user\n", 0},
		{bob, "printenv HOME", "/home/bob\n", 0},
		{alice, "printenv SCRATCH", "/scratch/example-user\n", 0},
		{bob, "printenv SCRATCH", "", 1}, // bob's login has no SCRATCH
	} {
		out, _, code := r.runAs(tc.s, tc.line, nil)
		if out != tc.out || code != tc.code {
			t.Errorf("%s: %q = %q exit %d, want %q exit %d", tc.s.User(), tc.line, out, code, tc.out, tc.code)
		}
	}

	// A relative path is relative to the Session's HOME.
	r.runAs(alice, "mkdir -p work", nil)
	r.runAs(bob, "mkdir -p work", nil)
	for _, d := range []string{"/home/example-user/work", "/home/bob/work"} {
		if _, ok := r.p.Dirs()[d]; !ok {
			t.Errorf("directory %s was not created", d)
		}
	}

	// The call log is the platform's, with each call's user; Generation is the Session's.
	if r.p.CountFor("bob", "printenv") != 2 || r.p.CountFor("example-user", "printenv") != 2 {
		t.Errorf("printenv calls: bob %d, alice %d, want 2 and 2", r.p.CountFor("bob", "printenv"), r.p.CountFor("example-user", "printenv"))
	}
	alice.Reconnect()
	if alice.Generation() != 2 || bob.Generation() != 1 {
		t.Errorf("generations %d and %d, want 2 and 1", alice.Generation(), bob.Generation())
	}
}

// p2 is a second user whose login sets HOME only.
func p2(r *rig) *Session {
	return r.p.Login(Login{User: "bob", Env: map[string]string{"HOME": "/home/bob"}})
}

func TestLoginDefaults(t *testing.T) {
	r := newRig(t, Config{})
	s := r.p.Login(Login{})
	if s.User() != "example-user" {
		t.Errorf("default user = %q", s.User())
	}
	if _, _, code := r.runAs(s, "printenv HOME", nil); code != 1 {
		t.Errorf("printenv HOME with no environment: exit %d, want 1", code)
	}
	// The map handed to Login is copied.
	env := map[string]string{"HOME": "/home/x"}
	s = r.p.Login(Login{User: "x", Env: env})
	env["HOME"] = "/changed"
	if out, _, _ := r.runAs(s, "printenv HOME", nil); out != "/home/x\n" {
		t.Errorf("printenv HOME = %q after the caller changed its map", out)
	}
}

func TestNewPanicsOnABadConfig(t *testing.T) {
	for name, cfg := range map[string]Config{
		"data parser": {DataParser: "v9"},
		"mode":        {NotListed: "elsewhere"},
		"zone":        {TimeZone: "Nowhere/Land"},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("New with a bad %s did not panic", name)
				}
			}()
			newRig(t, cfg)
		}()
	}
}
