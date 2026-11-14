package fakeslurm

// tail -c <n> <path>: the last n bytes of a file, as they are, on both file systems (issue #305: the
// reach-host-dialed rows keep the tail of a job's job.err, which a real login node reads with tail).

import (
	"os"
	"strings"
	"testing"
)

func TestTailGivesTheLastBytesOfAFile(t *testing.T) {
	r := newRig(t, Config{})
	r.p.WriteFile("/d/f", []byte("0123\n\x00789"), 0o600)
	if out := r.must("tail -c 4 /d/f"); out != "\x00789" {
		t.Errorf("tail -c 4 = %q, want the last 4 bytes as they are", out)
	}
	if out := r.must("tail -c 65536 /d/f"); out != "0123\n\x00789" {
		t.Errorf("tail -c 65536 of 9 bytes = %q, want the whole file", out)
	}
	if out, errOut, code := r.run("tail -c 4 /d/missing"); code != 1 || out != "" || !strings.Contains(errOut, "No such file or directory") {
		t.Errorf("tail of a missing file: exit %d stdout %q stderr %q, want exit 1 and an error", code, out, errOut)
	}
	for _, line := range []string{"tail /d/f", "tail -n 4 /d/f", "tail -c 0 /d/f", "tail -c 65537 /d/f", "tail -c 04 /d/f", "tail -c x /d/f", "tail -c 4 /d/f /d/f"} {
		if out, _, code := r.run(line); code == 0 || out != "" {
			t.Errorf("%q: exit %d stdout %q, want a refusal and no output", line, code, out)
		}
	}
}

// On disk tail reads from the size less n on, never more than n bytes: a sparse file far over every read limit
// gives its last bytes at once, and the bytes cross unchanged.
func TestRunModeTailReadsOnlyTheLastBytes(t *testing.T) {
	r := newRunRig(t, RunMode{})
	big := strings.Repeat("0123456789abcdef", 3<<16) + "\x00the end\n"
	if err := os.WriteFile(r.home+"/big", []byte(big), 0o644); err != nil {
		t.Fatal(err)
	}
	if out := r.must("tail -c 9 " + r.home + "/big"); out != "\x00the end\n" {
		t.Errorf("tail -c 9 of 3 MiB = %q", out)
	}
	writeSized(t, r.home+"/sparse", 1<<36)
	if out := r.must("tail -c 5 " + r.home + "/sparse"); out != "\x00\x00\x00\x00\x00" {
		t.Errorf("tail -c 5 of a 64 GiB sparse file = %q, want its last 5 bytes", out)
	}
	if _, _, code := r.run("tail -c 5 " + r.home); code != 1 {
		t.Errorf("tail of a directory: exit %d, want 1", code)
	}
}

// head -c <n> <path>: the first n bytes of a file, on both file systems, as tail gives the last (the bulk row reads a sandbox manifest's first line,
// which names the image's digest; #579).
func TestHeadGivesTheFirstBytesOfAFile(t *testing.T) {
	r := newRig(t, Config{})
	r.p.WriteFile("/d/f", []byte("0123\n\x00789"), 0o600)
	if out := r.must("head -c 4 /d/f"); out != "0123" {
		t.Errorf("head -c 4 = %q, want the first 4 bytes", out)
	}
	if out := r.must("head -c 65536 /d/f"); out != "0123\n\x00789" {
		t.Errorf("head -c 65536 of 9 bytes = %q, want the whole file", out)
	}
	if out, errOut, code := r.run("head -c 4 /d/missing"); code != 1 || out != "" || !strings.Contains(errOut, "No such file or directory") {
		t.Errorf("head of a missing file: exit %d stdout %q stderr %q, want exit 1 and an error", code, out, errOut)
	}
	for _, line := range []string{"head /d/f", "head -n 4 /d/f", "head -c 0 /d/f", "head -c 65537 /d/f", "head -c 04 /d/f", "head -c 4 /d/f /d/f"} {
		if out, _, code := r.run(line); code == 0 || out != "" {
			t.Errorf("%q: exit %d stdout %q, want a refusal and no output", line, code, out)
		}
	}
	rr := newRunRig(t, RunMode{})
	if err := os.WriteFile(rr.home+"/m", []byte("hpcburst-sandbox-manifest/1 sha256:x\nD\t\".\"\nend\t1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if out := rr.must("head -c 37 " + rr.home + "/m"); out != "hpcburst-sandbox-manifest/1 sha256:x\n" {
		t.Errorf("head -c 37 on disk = %q", out)
	}
}
