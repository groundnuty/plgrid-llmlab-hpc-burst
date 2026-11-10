package gotoolchain

// hack/fetch-go.sh installs the Go toolchain CI runs from a pinned tarball, and CI restores the
// tarball from a cache so that a GitHub or go.dev outage does not fail every job (issue #294). The
// script treats the cache as untrusted: the tarball is checked against its pin on every run, and the
// tree is extracted anew from it on every run. These tests run offline. The release is a file://
// mirror of a fake tarball (GO_RELEASE_BASE), a dead mirror is a file:// path that does not exist, and
// a pins file and a go.mod made for the fake release replace the real ones (GO_PINS_FILE,
// GO_MOD_FILE). The sums in the pins file come from crypto/sha256 over the bytes the test built, and
// the script computes its own with sha256sum, so neither checks itself.
//
// A run against a mirror that is up proves little: a script that skipped its checks would pass it. Each
// pass here that needs no download has a twin that differs by one changed byte and must fail.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A first run downloads the tarball, verifies it, extracts it and installs the tree. The environment
// says nothing about a download that was skipped: this run has no cache.
func TestFetchGoInstallsFromAVerifiedDownload(t *testing.T) {
	r := newRelease(t)
	d := newDirs(t)
	out, code := d.install(t, r.live())
	if code != 0 || !strings.Contains(out, "fetching go "+fakeVersion) || !strings.Contains(out, "verified "+r.tarball) || !strings.Contains(out, "installed go "+fakeVersion) {
		t.Fatalf("exit %d, want 0 after a fetch, a verification and an install:\n%s", code, out)
	}
	r.expectTree(t, d.inst)
	if got := names(t, d.dl); len(got) != 1 || got[0] != r.tarball {
		t.Errorf("the download directory holds %v, want only %s (the cache saves that directory)", got, r.tarball)
	}
	if got := names(t, d.inst); len(got) != 1 || got[0] != "go" {
		t.Errorf("the install directory holds %v, want only go (no stage left behind)", got)
	}
}

// A warm download directory, which is what a restored cache is, needs no download: the run passes with
// the release site dead. The twin shows that is the check at work: the same tarball changed, either
// way, is refused, and the run fails because the site is dead, so the changed tarball was not used.
func TestFetchGoWarmTarballNeedsNoDownloadAndAChangedOneIsRefused(t *testing.T) {
	r := newRelease(t)
	d := newDirs(t)
	if out, code := d.install(t, r.live()); code != 0 {
		t.Fatalf("filling the directories: exit %d:\n%s", code, out)
	}
	good := mustRead(t, filepath.Join(d.dl, r.tarball))

	// A fresh VM with the cache restored: the tarball is there, the install directory is empty.
	warm := func() dirs {
		fresh := newDirs(t)
		if err := os.MkdirAll(fresh.dl, 0o755); err != nil {
			t.Fatal(err)
		}
		write(t, filepath.Join(fresh.dl, r.tarball), good, 0o644)
		return fresh
	}
	fresh := warm()
	out, code := fresh.install(t, r.dead())
	if code != 0 || !strings.Contains(out, "ok       go "+fakeVersion+" tarball") || strings.Contains(out, "fetching") {
		t.Fatalf("warm tarball, dead site: exit %d, want 0 with an ok line and no fetch:\n%s", code, out)
	}
	r.expectTree(t, fresh.inst)

	for _, dmg := range damages {
		t.Run(dmg.name, func(t *testing.T) {
			fresh := warm()
			dmg.do(t, r, filepath.Join(fresh.dl, r.tarball))
			out, code := fresh.install(t, r.dead())
			if code != 1 || strings.Contains(out, "ok       go") || strings.Contains(out, "installed go") {
				t.Fatalf("a changed tarball was accepted from the warm directory: exit %d:\n%s", code, out)
			}
			if !strings.Contains(out, "download of ") || !strings.Contains(out, r.tarball) || !strings.Contains(out, "failed") {
				t.Errorf("the failure does not name the tarball it could not download:\n%s", out)
			}
			if got := names(t, fresh.inst); len(got) != 0 {
				t.Errorf("the install directory holds %v, want nothing installed from the changed tarball", got)
			}
			if got := names(t, fresh.dl); len(got) != 0 {
				t.Errorf("the download directory holds %v, want the changed tarball deleted", got)
			}
		})
	}
}

// A restored tarball that does not match its pin is a cache miss, never a failure and never a run on it:
// the tarball is deleted and downloaded again, and the tree comes from the verified one.
func TestFetchGoDownloadsAgainATarballThatDoesNotMatchItsPin(t *testing.T) {
	for _, dmg := range damages {
		t.Run(dmg.name, func(t *testing.T) {
			r := newRelease(t)
			d := newDirs(t)
			if out, code := d.install(t, r.live()); code != 0 {
				t.Fatalf("filling the directories: exit %d:\n%s", code, out)
			}
			dmg.do(t, r, filepath.Join(d.dl, r.tarball))
			out, code := d.install(t, r.live())
			if code != 0 || !strings.Contains(out, "refusing ") || !strings.Contains(out, "fetching go "+fakeVersion) || !strings.Contains(out, "verified "+r.tarball) {
				t.Fatalf("exit %d, want 0 after a refusal, a fetch and a verification:\n%s", code, out)
			}
			if got := sum(mustRead(t, filepath.Join(d.dl, r.tarball))); got != r.tarSum {
				t.Errorf("the tarball in the download directory has sha256 %s, want the pinned %s", got, r.tarSum)
			}
			r.expectTree(t, d.inst)
		})
	}
}

// With nothing in the download directory and the release site dead, the run fails, says which tarball
// it could not download, and leaves nothing behind that a cache could save or a PATH could find.
func TestFetchGoMissWithADeadSiteFailsNamingTheTarball(t *testing.T) {
	r := newRelease(t)
	d := newDirs(t)
	out, code := d.install(t, r.dead())
	if code != 1 || !strings.Contains(out, "download of ") || !strings.Contains(out, r.tarball) || !strings.Contains(out, "failed") {
		t.Fatalf("exit %d, want 1 naming %s:\n%s", code, r.tarball, out)
	}
	if got := names(t, d.dl); len(got) != 0 {
		t.Errorf("the download directory holds %v, want it empty", got)
	}
	if got := names(t, d.inst); len(got) != 0 {
		t.Errorf("the install directory holds %v, want it empty", got)
	}
}

// A download that is not the pinned tarball is deleted, nothing is extracted, and the run fails. The
// mirror here serves a valid tarball of the same name whose go/bin/go is the pinned one and whose
// compiler is not, so that only the tarball's digest can tell it from the real one.
func TestFetchGoDeletesADownloadThatDoesNotMatchItsPinAndExtractsNothing(t *testing.T) {
	r := newRelease(t)
	write(t, filepath.Join(r.mirror, r.tarball), r.tarballWith(t, func(f map[string][]byte) {
		f["go/pkg/tool/linux_amd64/compile"] = []byte("planted compile\n")
	}), 0o644)
	d := newDirs(t)
	out, code := d.install(t, r.live())
	if code != 1 || !strings.Contains(out, "checksum mismatch for "+r.tarball) || strings.Contains(out, "installed go") {
		t.Fatalf("exit %d, want 1 with a checksum mismatch:\n%s", code, out)
	}
	if got := names(t, d.dl); len(got) != 0 {
		t.Errorf("the download directory holds %v, want the bad download deleted", got)
	}
	if got := names(t, d.inst); len(got) != 0 {
		t.Errorf("the install directory holds %v, want nothing extracted", got)
	}
}

// The pin on go/bin/go is checked on the extracted file as well as the tarball's own. A pins file
// whose tarball sum is right and whose go sum is not (a pin copied wrong) installs nothing.
func TestFetchGoRefusesABinaryThatDoesNotMatchItsPin(t *testing.T) {
	r := newRelease(t)
	r.writePins(r.tarSum, sum([]byte("not the go binary")))
	d := newDirs(t)
	out, code := d.install(t, r.live())
	if code != 1 || !strings.Contains(out, "go/bin/go") || !strings.Contains(out, "does not match its pin") {
		t.Fatalf("exit %d, want 1 naming go/bin/go:\n%s", code, out)
	}
	if got := names(t, d.inst); len(got) != 0 {
		t.Errorf("the install directory holds %v, want nothing installed", got)
	}
}

// A verified tarball is also held to the release it is pinned for. One whose go/VERSION names another
// release, and one whose go reports another release, are both refused, each with its pins made from
// its own bytes so that the digests agree and only these two checks can refuse them.
func TestFetchGoRefusesAVerifiedTarballOfAnotherRelease(t *testing.T) {
	for _, tc := range []struct {
		name string
		mod  func(f map[string][]byte)
		want string
	}{
		{"go/VERSION", func(f map[string][]byte) { f["go/VERSION"] = []byte("go1.98.0\n") }, "go/VERSION"},
		{"go reports another release", func(f map[string][]byte) {
			f["go/bin/go"] = []byte("#!/bin/sh\necho 'go version go1.98.0 linux/amd64'\n")
		}, "reports"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newReleaseWith(t, tc.mod)
			d := newDirs(t)
			out, code := d.install(t, r.live())
			if code != 1 || !strings.Contains(out, tc.want) {
				t.Fatalf("exit %d, want 1 naming %q:\n%s", code, tc.want, out)
			}
			if strings.Contains(out, "installed go") {
				t.Errorf("the run says it installed a toolchain it refused:\n%s", out)
			}
		})
	}
}

// A tree that is already installed is never reused: a file of it that no pin covers, a source file and
// the pinned binary itself are all changed, and the next run, with the site dead, puts the tarball's
// bytes back. So everything that runs comes from the verified tarball of that run.
func TestFetchGoNeverReusesAnInstalledTree(t *testing.T) {
	r := newRelease(t)
	d := newDirs(t)
	if out, code := d.install(t, r.live()); code != 0 {
		t.Fatalf("filling the directories: exit %d:\n%s", code, out)
	}
	for _, name := range []string{"go/pkg/tool/linux_amd64/compile", "go/src/fmt/print.go", "go/bin/go"} {
		flip(t, filepath.Join(d.inst, name))
	}
	write(t, filepath.Join(d.inst, "go/extra"), []byte("planted"), 0o644)
	out, code := d.install(t, r.dead())
	if code != 0 || strings.Contains(out, "fetching") {
		t.Fatalf("exit %d, want 0 from the cached tarball alone:\n%s", code, out)
	}
	r.expectTree(t, d.inst)
	if _, err := os.Stat(filepath.Join(d.inst, "go/extra")); err == nil {
		t.Errorf("a file planted in the installed tree survived the next run")
	}
}

// --version reads go.mod's go line as hack/build-agents.sh does: after "go", up to a comment, with a
// tab as well as a space, and the toolchain line is no part of it (GOTOOLCHAIN=local).
func TestFetchGoVersionReadsGoModsGoLine(t *testing.T) {
	r := newRelease(t)
	for _, tc := range []struct {
		name, mod, want string
	}{
		{"plain", "module m\n\ngo 1.99.0\n", "1.99.0"},
		{"comment", "module m\n\ngo 1.99.0 // pinned\n", "1.99.0"},
		{"tab", "module m\n\ngo\t1.99.0\n", "1.99.0"},
		{"toolchain line ignored", "module m\n\ngo 1.99.0\n\ntoolchain go1.99.5\n", "1.99.0"},
		{"go line after a require block", "module m\n\nrequire (\n\tgo.uber.org/zap v1.0.0\n)\n\ngo 1.99.0\n", "1.99.0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			write(t, r.gomod, []byte(tc.mod), 0o644)
			out, code := run(t, r.live(), "--version")
			if code != 0 || out != tc.want+"\n" {
				t.Errorf("exit %d, output %q, want 0 and %q", code, out, tc.want+"\n")
			}
		})
	}
}

// A go.mod that is not an exact release, or names one with no pin, stops the run before the first
// download, so a bump without its pin fails loudly in every job instead of running an unpinned Go.
func TestFetchGoStopsBeforeAnyDownloadWithoutAnExactPinnedVersion(t *testing.T) {
	for _, tc := range []struct {
		name, mod, want string
	}{
		{"patchless", "module m\n\ngo 1.99\n", "exact version"},
		{"release candidate", "module m\n\ngo 1.99rc1\n", "exact version"},
		{"no go line", "module m\n", "exact version"},
		{"a release with no pin", "module m\n\ngo 1.99.1\n", "is not pinned"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRelease(t)
			write(t, r.gomod, []byte(tc.mod), 0o644)
			d := newDirs(t)
			out, code := d.install(t, r.live())
			if code != 1 || !strings.Contains(out, tc.want) || !strings.Contains(out, "nothing was fetched") {
				t.Fatalf("exit %d, want 1 saying %q and that nothing was fetched:\n%s", code, tc.want, out)
			}
			if got := names(t, d.dl); len(got) != 0 {
				t.Errorf("the download directory holds %v, want nothing fetched", got)
			}
		})
	}
}

// --check passes only when the go that PATH finds is the installed one with the pinned digest. Three
// ways to fail: another go first on PATH (the runner image has others), no go at all, and the pinned
// binary changed after the install.
func TestFetchGoCheckAcceptsOnlyThePinnedGoFirstOnPath(t *testing.T) {
	r := newRelease(t)
	d := newDirs(t)
	if out, code := d.install(t, r.live()); code != 0 {
		t.Fatalf("filling the directories: exit %d:\n%s", code, out)
	}
	bin := filepath.Join(d.inst, "go", "bin")
	system := os.Getenv("PATH")

	out, code := run(t, append(r.live(), "PATH="+bin+":"+system), "--check", d.inst)
	if code != 0 || !strings.Contains(out, "ok       go "+fakeVersion+" on PATH") {
		t.Fatalf("the pinned go first on PATH: exit %d, want 0:\n%s", code, out)
	}

	// Another go first on PATH, one that reports the right version.
	other := t.TempDir()
	write(t, filepath.Join(other, "go"), r.files["go/bin/go"], 0o755)
	out, code = run(t, append(r.live(), "PATH="+other+":"+bin+":"+system), "--check", d.inst)
	if code != 1 || !strings.Contains(out, "not the pinned") {
		t.Errorf("another go first on PATH: exit %d, want 1 saying it is not the pinned one:\n%s", code, out)
	}

	// No go on PATH at all: a directory of links to the tools the script uses, and nothing else.
	out, code = run(t, append(r.live(), "PATH="+toolsOnly(t, "awk", "uname", "readlink", "sha256sum", "env", "dirname")), "--check", d.inst)
	if code != 1 || !strings.Contains(out, "no go on PATH") {
		t.Errorf("no go on PATH: exit %d, want 1 saying there is no go:\n%s", code, out)
	}

	// The pinned binary changed after the install: still first on PATH, no longer the pinned bytes.
	flip(t, filepath.Join(bin, "go"))
	out, code = run(t, append(r.live(), "PATH="+bin+":"+system), "--check", d.inst)
	if code != 1 || !strings.Contains(out, "pinned sha256") {
		t.Errorf("a changed go/bin/go: exit %d, want 1 naming the pinned sha256:\n%s", code, out)
	}
}

// What the repository itself pins. The go.dev listing's sha256 for go1.26.0 linux-amd64 is written out
// here as read from https://go.dev/dl/?mode=json&include=all on 10 Nov 2026, not taken from the pins
// file, so a pin that drifted is caught here. A go.mod bump needs its new line in the pins file, and
// the script enforces that in every CI job; this test only holds the line that is there.
func TestRealPinsFileHoldsGoModsReleaseAsGoDevPublishesIt(t *testing.T) {
	gomod, err := os.ReadFile(filepath.Join("..", "..", "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	pins, err := os.ReadFile(filepath.Join("..", "go-pins.txt"))
	if err != nil {
		t.Fatal(err)
	}
	var gover string
	for _, line := range strings.Split(string(gomod), "\n") {
		if f := strings.Fields(line); len(f) >= 2 && f[0] == "go" {
			gover = f[1]
			break
		}
	}
	var rows [][]string
	for _, line := range strings.Split(string(pins), "\n") {
		if f := strings.Fields(line); len(f) > 0 && !strings.HasPrefix(f[0], "#") && f[0] == gover {
			rows = append(rows, f)
		}
	}
	if len(rows) != 1 || len(rows[0]) != 5 {
		t.Fatalf("hack/go-pins.txt has %d lines for go.mod's go %s, want one with five columns: %v", len(rows), gover, rows)
	}
	row := rows[0]
	if row[1] != "amd64" || row[2] != "go"+gover+".linux-amd64.tar.gz" {
		t.Errorf("the line is for %s %s, want amd64 and go%s.linux-amd64.tar.gz", row[1], row[2], gover)
	}
	// From go.dev's download listing, read 10 Nov 2026 (and equal to the sha256sum of a download).
	published := map[string]string{
		"1.26.0": "aac1b08a0fb0c4e0a7c1555beb7b59180b05dfc5a3d62e40e9de90cd42f88235",
	}
	want, ok := published[gover]
	if !ok {
		t.Fatalf("go.mod's go %s has no go.dev sha256 written into this test: read it from https://go.dev/dl/?mode=json&include=all, and add it to the pins file and here", gover)
	}
	if row[3] != want {
		t.Errorf("the pinned tarball sha256 of go %s is %s, go.dev publishes %s", gover, row[3], want)
	}
	if len(row[4]) != 64 {
		t.Errorf("the go binary's sha256 %q is not 64 hex characters", row[4])
	}
}
