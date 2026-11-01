package launcher

import (
	"debug/elf"
	"sort"
)

// archInfo is what is known of one architecture that builds exist for.
type archInfo struct {
	GOARCH     string      // Go's name, which frp uses too (amd64, arm64)
	CPUArch    string      // the platform profiles' cpuArch: the kernel's machine name (x86_64, aarch64)
	ELFMachine elf.Machine // the machine type in the header of an ELF binary for it
}

// archTable is the one table of the architectures builds exist for. A new architecture is added
// here and nowhere else in Go: cpuArchOf, CPUArchs and the ELF check of an agent build read it.
// hack/cpu-arch.sh is its bash copy of the first two columns, and TestScriptsMapArchitectures...
// compares the two.
var archTable = []archInfo{
	{GOARCH: "amd64", CPUArch: "x86_64", ELFMachine: elf.EM_X86_64},
	{GOARCH: "arm64", CPUArch: "aarch64", ELFMachine: elf.EM_AARCH64},
}

// cpuArchOf maps the release's architecture names to the kernel's machine names, which platform
// profiles use for cpuArch (v1alpha1.AcceleratorClass) and agent.json's frpc digest is keyed by.
func cpuArchOf() map[string]string {
	out := make(map[string]string, len(archTable))
	for _, a := range archTable {
		out[a.GOARCH] = a.CPUArch
	}
	return out
}

// elfMachineFor is the ELF machine type of a binary for a cpuArch.
func elfMachineFor(cpuArch string) (elf.Machine, bool) {
	for _, a := range archTable {
		if a.CPUArch == cpuArch {
			return a.ELFMachine, true
		}
	}
	return elf.EM_NONE, false
}

// CPUArchs lists the cpuArch values builds exist for, sorted: those of archTable.
func CPUArchs() []string {
	out := make([]string, 0, len(archTable))
	for _, a := range archTable {
		out = append(out, a.CPUArch)
	}
	sort.Strings(out)
	return out
}
