package agent

import (
	"bytes"
	"context"
	"debug/elf"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Envoy's release binary is dynamically linked against glibc: its loader, named in its ELF header, and
// glibc's own libraries, with symbol versions up to GLIBC_2.30 for both pinned architectures (#454's
// measurement). It runs bare on a node whose glibc is that or later. On an older one its exec would
// fail with a loader's error and exit 127, so the agent checks first and refuses, naming the glibc it
// found and the one Envoy needs (the science agent's ruling on #454). Both are read, not assumed: the
// need from Envoy's own symbol versions, the node's glibc from getconf GNU_LIBC_VERSION.
//
// getconf, not the loader's --version: ld.so only gained --version (and --help) in glibc 2.33, so on
// an older node it takes --version for a program to run, and fails without naming a version. getconf
// is glibc's own (its base package on every distribution: libc-bin, glibc-common), has printed
// "glibc X.Y" since 2.3.2, and reports the system glibc, the one Envoy's loader loads (#477's review).
// Running libc.so.6 itself would print the version too, but its path differs between distributions.

// envoyLoader is the path the check looks for the loader Envoy names at. Tests replace it with a fake
// node's loader; production looks at the interpreter itself.
var envoyLoader = func(interp string) string { return interp }

// glibcProbe is the command that prints the node's glibc version. Tests replace it with a fake node's.
var glibcProbe = []string{"getconf", "GNU_LIBC_VERSION"}

// glibcVersion is the version in getconf GNU_LIBC_VERSION's answer: "glibc 2.34".
var glibcVersion = regexp.MustCompile(`^glibc (\d+)\.(\d+)`)

// checkEnvoyGlibc refuses an Envoy binary whose loader this node lacks, or whose glibc this node's is
// older than.
func checkEnvoyGlibc(bin *os.File) error {
	interp, need, err := envoyGlibcNeed(bin)
	if err != nil {
		return refuse("reading Envoy's ELF header: %v", err)
	}
	if interp == "" {
		return nil // statically linked: no loader and no glibc to check
	}
	if fi, err := os.Stat(envoyLoader(interp)); err != nil || !fi.Mode().IsRegular() || fi.Mode()&0o111 == 0 {
		return refuse("Envoy needs glibc %d.%d or later through its loader %s, which this node does not have (%v)", need[0], need[1], interp, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, glibcProbe[0], glibcProbe[1:]...)
	cmd.Env = []string{}
	out, err := cmd.Output()
	if err != nil {
		return refuse("Envoy needs glibc %d.%d or later, and reading this node's glibc (%s) failed: %v", need[0], need[1], strings.Join(glibcProbe, " "), err)
	}
	m := glibcVersion.FindSubmatch(bytes.TrimSpace(out))
	if m == nil {
		return refuse("Envoy needs glibc %d.%d or later, and %s names no glibc version: %.120q", need[0], need[1], strings.Join(glibcProbe, " "), bytes.TrimSpace(out))
	}
	maj, _ := strconv.Atoi(string(m[1]))
	minor, _ := strconv.Atoi(string(m[2]))
	if maj < need[0] || (maj == need[0] && minor < need[1]) {
		return refuse("Envoy needs glibc %d.%d or later, and this node's glibc is %d.%d: Envoy cannot run bare here", need[0], need[1], maj, minor)
	}
	return nil
}

// envoyGlibcNeed reads the loader Envoy names (PT_INTERP) and the highest GLIBC_ symbol version it
// imports; interp is "" for a statically linked binary.
func envoyGlibcNeed(bin *os.File) (interp string, need [2]int, err error) {
	f, err := elf.NewFile(bin)
	if err != nil {
		return "", need, err
	}
	for _, p := range f.Progs {
		if p.Type == elf.PT_INTERP {
			b := make([]byte, p.Filesz)
			if _, err := p.ReadAt(b, 0); err != nil {
				return "", need, err
			}
			interp = strings.TrimRight(string(b), "\x00")
		}
	}
	if interp == "" {
		return "", need, nil
	}
	syms, err := f.ImportedSymbols()
	if err != nil {
		return "", need, err
	}
	for _, s := range syms {
		v, ok := strings.CutPrefix(s.Version, "GLIBC_")
		if !ok {
			continue
		}
		parts := strings.Split(v, ".")
		if len(parts) < 2 {
			continue
		}
		maj, err1 := strconv.Atoi(parts[0])
		minor, err2 := strconv.Atoi(parts[1])
		if err1 != nil || err2 != nil {
			continue
		}
		if maj > need[0] || (maj == need[0] && minor > need[1]) {
			need = [2]int{maj, minor}
		}
	}
	if need == [2]int{} {
		return "", need, fmt.Errorf("it names the loader %s but imports no GLIBC_ symbol version", interp)
	}
	return interp, need, nil
}
