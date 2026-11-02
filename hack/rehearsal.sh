#!/usr/bin/env bash
# The one-host alpha rehearsal (issue #146): builds the four binaries and the
# rehearsal test, then runs the whole stack on this host, the real hpcburst up
# over a fake Slurm cluster in run mode reached over real ssh and sshd.
#
#   HPCBURST_BIN_DIR=<dir with k3s-1.36.5/k3s> HPCBURST_FRP_DIR=<dir with frps, frpc> hack/rehearsal.sh
#
# Optional:
#   HPCBURST_SSHD           sshd for the fake login node and the gateway (default /usr/sbin/sshd)
#   REHEARSAL_RUN           a -test.run pattern (default every rehearsal test)
#   REHEARSAL_OUT           where the binaries go (default a new temporary directory)
#   REHEARSAL_PREBUILT=1    run the binaries already in REHEARSAL_OUT instead of building them,
#                           so the run can take a host's resource lock without a build inside it
#   REHEARSAL_MEMORY_MAX    the run's memory cap (default 4G); the stack peaks under 1 GiB
#   HPCBURST_TEST_ROOT_K3S=1  a system k3s runs here: the test fails unless it listens on 6443,
#                           6444 and 10250. Without it, the test records whichever of them listen
#                           and fails if that changes.
# It binds 127.0.0.1 only, on ports 29500-29539 and kernel-chosen ones, and never touches a
# system k3s.
#
# The test starts k3s, the supervisor, the fake jobs and the sshds in sessions of their own, and
# only its clean-up stops them: go test's -test.timeout panic and a signal both skip it. So the
# test runs in a transient systemd user scope (systemd-run --user --scope), whose cgroup holds
# every process it starts, and the script stops the scope however it ends: on its exit, and on
# INT or TERM. A process still in the scope once a passing test has returned fails the run. This
# needs a systemd user manager; on a CI runner, enable lingering for the user first.
set -euo pipefail
cd "$(dirname "$0")/.."
: "${HPCBURST_BIN_DIR:?set HPCBURST_BIN_DIR to a directory with k3s-1.36.5/k3s (hack/fetch-k3s.sh DIR 1.36.5)}"
: "${HPCBURST_FRP_DIR:?set HPCBURST_FRP_DIR to a directory with the pinned frps and frpc (hack/fetch-frp.sh DIR)}"
out="${REHEARSAL_OUT:-$(mktemp -d)}"
mkdir -p "$out"
if [ "${REHEARSAL_PREBUILT:-0}" = 1 ]; then
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
# -test.timeout 30m: twice the target of 15 minutes (issue #146). The test's waits give up two
# minutes before it, so a slow run fails with its clean-up done; the scope is the backstop.
# In the background, so that a trapped signal ends the wait at once: bash runs a trap only after
# a foreground command returns.
HPCBURST_REHEARSAL=1 HPCBURST_REHEARSAL_BIN="$out" HPCBURST_REQUIRE_FRP=1 HPCBURST_REQUIRE_SSHD=1 \
	systemd-run --user --scope --collect -q --unit="$unit" \
	-p MemoryMax="${REHEARSAL_MEMORY_MAX:-4G}" -p MemorySwapMax=0 \
	"$out/rehearsal.test" -test.v -test.timeout 30m -test.run "${REHEARSAL_RUN:-^TestRehearsal}" &
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
