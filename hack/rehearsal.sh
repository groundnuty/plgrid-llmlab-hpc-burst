#!/usr/bin/env bash
# The one-host alpha rehearsal (issue #146): builds the four binaries and the
# rehearsal test, then runs the whole stack on this host, the real hpcburst up
# over a fake Slurm cluster in run mode reached over real ssh and sshd.
#
#   HPCBURST_BIN_DIR=<dir with k3s-1.36.5/k3s> HPCBURST_FRP_DIR=<dir with frps, frpc> hack/rehearsal.sh
#   REHEARSAL_ARCHIVE=<a release archive> hack/rehearsal.sh
#
# The second form runs a draft release's own bytes (HBEP-15, Releases): hack/release-unpack.sh unpacks
# the archive (hack/release-archive.sh) into REHEARSAL_OUT, under the names the prebuilt mode takes
# (hpcburst, hpcburst-controller, hpcburst-front, hb-agent), takes the k3s directory from the archive's
# bin/ (HPCBURST_BIN_DIR) and puts its frps and its x86_64 frpc in REHEARSAL_OUT/frp (HPCBURST_FRP_DIR).
# The archive gives all three, so neither variable may be set. rehearsal.test is built from the checkout,
# which is the tag's: the archive holds no test. A SHA256SUMS beside the archive is checked. The archive
# has the one pinned k3s, so '^TestRehearsalKeeper', which needs k3s-1.35.9 too, does not run from it.
#
# Optional:
#   REHEARSAL_UNPACK_ONLY=1 with REHEARSAL_ARCHIVE, unpack and stop: nothing is built or run
#   HPCBURST_SSHD           sshd for the fake login node and the gateway (default /usr/sbin/sshd)
#   REHEARSAL_RUN           a -test.run pattern (default '^TestRehearsal(Direct|SSH|HostDialed)$', issue #146's two
#                           and #298's host-dialed mode;
#                           '^TestRehearsalPart3$' runs issue #193's third part, about 8 minutes more;
#                           '^TestRehearsalKeeper' runs #343's relay keeper tests, about 7 minutes,
#                           which need HPCBURST_BIN_DIR to hold k3s-1.35.9/k3s too)
#   REHEARSAL_OUT           where the binaries go (default a new temporary directory)
#   REHEARSAL_PREBUILT=1    run the binaries already in REHEARSAL_OUT instead of building them,
#                           so the run can take a host's resource lock without a build inside it
#   REHEARSAL_MEMORY_MAX    the run's memory cap (default 4G); the stack peaks under 1 GiB
#   REHEARSAL_TIMEOUT       the test binary's -test.timeout (default 30m); its waits give up two
#                           minutes before it, so a slow run still cleans up
#   HPCBURST_TEST_ROOT_K3S=1  a system k3s runs here: the test fails unless it listens on 6443,
#                           6444 and 10250. Without it, the test records whichever of them listen
#                           and fails if that changes.
# It binds 127.0.0.1 only, on ports 29500-29539 (issue #146's tests), 29780-29799 (#298's host-dialed
# mode), 29680-29699 (issue #193's), 30020-30033 (#343's relay keeper tests)
# and kernel-chosen ones, and never touches a system k3s.
#
# The test starts k3s, the supervisor, the fake jobs and the sshds in sessions of their own, and
# only its clean-up stops them: go test's -test.timeout panic and a signal both skip it. So the
# test runs in a transient systemd user scope (systemd-run --user --scope), whose cgroup holds
# every process it starts, and the script stops the scope however it ends: on its exit, and on
# INT or TERM. A process still in the scope once a passing test has returned fails the run. This
# needs a systemd user manager; on a CI runner, enable lingering for the user first.
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -n "${REHEARSAL_ARCHIVE:-}" ]; then
	if [ -n "${HPCBURST_BIN_DIR:-}" ] || [ -n "${HPCBURST_FRP_DIR:-}" ]; then
		echo "rehearsal.sh: REHEARSAL_ARCHIVE gives the k3s and frp directories itself; unset HPCBURST_BIN_DIR and HPCBURST_FRP_DIR" >&2
		exit 2
	fi
	if [ "${REHEARSAL_PREBUILT:-0}" = 1 ]; then
		echo "rehearsal.sh: REHEARSAL_ARCHIVE and REHEARSAL_PREBUILT=1 are two ways to the binaries; use one" >&2
		exit 2
	fi
else
	: "${HPCBURST_BIN_DIR:?set HPCBURST_BIN_DIR to a directory with k3s-1.36.5/k3s (hack/fetch-k3s.sh DIR 1.36.5), or set REHEARSAL_ARCHIVE}"
	: "${HPCBURST_FRP_DIR:?set HPCBURST_FRP_DIR to a directory with the pinned frps and frpc (hack/fetch-frp.sh DIR), or set REHEARSAL_ARCHIVE}"
fi
out="${REHEARSAL_OUT:-$(mktemp -d)}"
mkdir -p "$out"
if [ -n "${REHEARSAL_ARCHIVE:-}" ]; then
	bash hack/release-unpack.sh "$REHEARSAL_ARCHIVE" "$out" "$out/frp"
	export HPCBURST_BIN_DIR="$out/archive/bin" HPCBURST_FRP_DIR="$out/frp"
	echo "HPCBURST_BIN_DIR=$HPCBURST_BIN_DIR"
	echo "HPCBURST_FRP_DIR=$HPCBURST_FRP_DIR"
	if [ "${REHEARSAL_UNPACK_ONLY:-0}" = 1 ]; then
		exit 0
	fi
	go test -c -o "$out/rehearsal.test" ./internal/rehearsal/scenario
	echo "running the binaries of $REHEARSAL_ARCHIVE in $out"
elif [ "${REHEARSAL_PREBUILT:-0}" = 1 ]; then
	for f in hpcburst hpcburst-controller hpcburst-front hb-agent rehearsal.test; do
		if [ ! -x "$out/$f" ]; then
			echo "rehearsal.sh: REHEARSAL_PREBUILT=1, but $out/$f is missing" >&2
			exit 2
		fi
	done
	echo "running the binaries in $out"
else
	echo "building into $out"
	go build -o "$out/" ./cmd/hpcburst ./cmd/hpcburst-controller ./cmd/hpcburst-front ./cmd/hb-agent
	go test -c -o "$out/rehearsal.test" ./internal/rehearsal/scenario
fi
if ! systemd-run --user --scope -q true; then
	echo "rehearsal.sh: systemd-run --user does not work here, and the rehearsal runs only in a scope it can stop" >&2
	echo "rehearsal.sh: start a user manager (loginctl enable-linger) and set XDG_RUNTIME_DIR" >&2
	exit 2
fi

unit="hpcburst-rehearsal-$$-$(date +%s)"
scope="$unit.scope"

# scope_procs lists the PIDs left in the scope's cgroup, sub-cgroups included; nothing when the
# scope is gone.
scope_procs() {
	local cg
	cg=$(systemctl --user show -p ControlGroup --value "$scope" 2> /dev/null || true)
	if [ -n "$cg" ] && [ -d "/sys/fs/cgroup$cg" ]; then
		find "/sys/fs/cgroup$cg" -name cgroup.procs -exec cat {} + 2> /dev/null || true
	fi
}

# stop_scope kills whatever is left in the scope and returns once its cgroup holds no process,
# killing again each second, for at most a minute: a SIGKILL is delivered at once, but a large
# process (k3s) takes a moment to exit and give back its ports. The test itself is killed too, in
# case a signal came before systemd-run had made the scope.
stop_scope() {
	local i left n=0
	if [ -n "${child:-}" ]; then
		kill -KILL "$child" 2> /dev/null || true
	fi
	for i in $(seq 1 60); do
		left=$(scope_procs)
		[ -n "$left" ] || break
		[ "$i" -gt 1 ] || n=$(echo "$left" | wc -l)
		systemctl --user kill --signal=SIGKILL "$scope" 2> /dev/null || true
		# shellcheck disable=SC2086 # one PID a word
		kill -KILL $left 2> /dev/null || true
		sleep 1
	done
	systemctl --user stop "$scope" 2> /dev/null || true
	if [ -n "$(scope_procs)" ]; then
		echo "rehearsal.sh: $scope still holds processes after a minute of SIGKILL: $(scope_procs | tr '\n' ' ')" >&2
		exit 1 # in the EXIT trap: the run fails
	fi
	if [ "$n" -gt 0 ]; then
		echo "rehearsal.sh: stopped $scope: killed $n processes, none left" >&2
	fi
}

trap stop_scope EXIT
trap 'echo "rehearsal.sh: interrupted; stopping $scope" >&2; exit 130' INT
trap 'echo "rehearsal.sh: terminated; stopping $scope" >&2; exit 143' TERM

echo "the test runs in $scope"
cd internal/rehearsal/scenario
# -test.timeout 30m by default: twice the target of 15 minutes (issue #146). The test's waits give
# up two minutes before it, so a slow run fails with its clean-up done; the scope is the backstop.
# In the background, so that a trapped signal ends the wait at once: bash runs a trap only after
# a foreground command returns.
HPCBURST_REHEARSAL=1 HPCBURST_REHEARSAL_BIN="$out" HPCBURST_REQUIRE_FRP=1 HPCBURST_REQUIRE_SSHD=1 \
	systemd-run --user --scope --collect -q --unit="$unit" \
	-p MemoryMax="${REHEARSAL_MEMORY_MAX:-4G}" -p MemorySwapMax=0 \
	"$out/rehearsal.test" -test.v -test.timeout "${REHEARSAL_TIMEOUT:-30m}" -test.run "${REHEARSAL_RUN:-^TestRehearsal(Direct|SSH|HostDialed)\$}" &
child=$!
rc=0
wait "$child" || rc=$?
child=
left=$(scope_procs)
if [ -n "$left" ]; then
	echo "rehearsal.sh: processes left in $scope after the test returned (exit $rc):" >&2
	# shellcheck disable=SC2086 # one PID a word
	ps -o pid=,args= -p "$(echo $left | tr ' ' ',')" >&2 || true
	if [ "$rc" -eq 0 ]; then
		rc=1
	fi
fi
exit "$rc"
