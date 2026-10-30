package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestRelativeBinDir: --bin-dir may be a relative path, and means the same directory for the
// check of the binary and for running it. Both used to be resolved against different directories:
// the launcher hashed the binary relative to the caller's working directory, but started it
// relative to the state directory, so a file planted at <state>/bins/k3s-1.36.5/k3s was run
// although the file that passed the check was another one.
//
// The decoy writes a marker outside the state directory and exits 1. If it is ever run, the marker
// exists; up would fail too, but that alone could have other causes.
func TestRelativeBinDir(t *testing.T) {
	const base = 29220
	h := newHost(t)
	work := shortTmp(t, false) // the caller's working directory: bins -> the pinned binaries
	if err := os.Symlink(h.binDir, filepath.Join(work, "bins")); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(shortTmp(t, false), "decoy-ran")
	decoyDir := filepath.Join(h.dir, "bins", "k3s-1.36.5")
	if err := os.MkdirAll(decoyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	decoy := filepath.Join(decoyDir, "k3s")
	if err := os.WriteFile(decoy, []byte("#!/bin/sh\necho ran > "+marker+"\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	// The decoy can fire: run it by hand once (this also shows the directory allows execution).
	err := exec.Command(decoy).Run()
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 1 {
		t.Fatalf("the decoy did not run and exit 1: %v", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("the decoy ran but left no marker: %v", err)
	}
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}

	for _, c := range []struct {
		name string
		nice []string
	}{{"with --nice 10", []string{"--nice", "10"}}, {"without --nice", nil}} {
		args := append([]string{"up", "--state-dir", h.dir, "--bin-dir", "bins", "--port-base", itoa(base)}, c.nice...)
		r := cliIn(t, work, args...)
		// The marker first: if the decoy ran, that is the finding, whatever up exited with.
		if _, err := os.Stat(marker); !os.IsNotExist(err) {
			t.Fatalf("%s: the decoy under the state directory was executed instead of the binary that passed the check (%v); up exited %d", c.name, err, r.code)
		}
		if r.code != exitOK {
			t.Fatalf("%s: up exited %d: %s", c.name, r.code, r.err)
		}
		if r := cli(t, h.args("status")...); r.code != exitOK {
			t.Errorf("%s: status exit %d: %s", c.name, r.code, r.out)
		}
		if r := cli(t, h.args("down")...); r.code != exitOK {
			t.Fatalf("%s: down exited %d", c.name, r.code)
		}
		if _, err := os.Stat(marker); !os.IsNotExist(err) {
			t.Fatalf("%s: the decoy was executed (%v)", c.name, err)
		}
	}
}
