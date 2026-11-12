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
// need from Envoy's own symbol versions, the node's from its loader's --version.

// envoyLoader is the loader the check runs for the interpreter Envoy names. Tests replace it with a
// fake loader; production runs the interpreter itself.
var envoyLoader = func(interp string) string { return interp }

// loaderVersion is the version in a glibc loader's --version: "... release version 2.34."
var loaderVersion = regexp.MustCompile(`release version (\d+)\.(\d+)`)

// checkEnvoyGlibc refuses an Envoy binary whose glibc this node's loader is older than.
func checkEnvoyGlibc(bin *os.File) error {
	interp, need, err := envoyGlibcNeed(bin)
	if err != nil {
		return refuse("reading Envoy's ELF header: %v", err)
	}
	if interp == "" {
		return nil // statically linked: no loader and no glibc to check
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, envoyLoader(interp), "--version")
	cmd.Env = []string{}
	out, err := cmd.Output()
	if err != nil {
		return refuse("Envoy needs glibc %d.%d or later, through its loader %s, and running that loader on this node failed: %v", need[0], need[1], interp, err)
	}
	m := loaderVersion.FindSubmatch(out)
	if m == nil {
		return refuse("Envoy needs glibc %d.%d or later, and this node's loader %s names no glibc version: %.120q", need[0], need[1], interp, bytes.TrimSpace(out))
	}
	maj, _ := strconv.Atoi(string(m[1]))
	minor, _ := strconv.Atoi(string(m[2]))
	if maj < need[0] || (maj == need[0] && minor < need[1]) {
		return refuse("Envoy needs glibc %d.%d or later, and this node's glibc is %d.%d (its loader %s): Envoy cannot run bare here", need[0], need[1], maj, minor, interp)
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
