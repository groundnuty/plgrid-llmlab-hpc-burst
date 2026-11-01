# Sourced by hack/fetch-frp.sh and hack/build-agents.sh; never run.
#
# The one bash map between the architecture names Go and frp use (GOARCH: amd64, arm64) and the
# platform profiles' cpuArch (the kernel's machine names: x86_64, aarch64). The launcher's
# cpuArchOf (internal/launcher/frps_pins.go) is the Go one, and TestScriptsMapArchitecturesAsCPUArchOfDoes
# compares the two. Both directions below are read from this one table, and a script that spells its
# own pair of names instead of sourcing this file fails that test.

# GOARCH and cpuArch, one pair per entry.
CPU_ARCH_PAIRS=("amd64 x86_64" "arm64 aarch64")

# cpu_arch GOARCH: the cpuArch of a Go or frp architecture name; fails for any other.
cpu_arch() {
  local pair
  for pair in "${CPU_ARCH_PAIRS[@]}"; do
    [ "${pair%% *}" = "$1" ] && { echo "${pair##* }"; return 0; }
  done
  return 1
}

# go_arch CPUARCH: the GOARCH of a cpuArch; fails for any other.
go_arch() {
  local pair
  for pair in "${CPU_ARCH_PAIRS[@]}"; do
    [ "${pair##* }" = "$1" ] && { echo "${pair%% *}"; return 0; }
  done
  return 1
}

# cpu_archs: every cpuArch, one per line.
cpu_archs() {
  local pair
  for pair in "${CPU_ARCH_PAIRS[@]}"; do
    echo "${pair##* }"
  done
}
