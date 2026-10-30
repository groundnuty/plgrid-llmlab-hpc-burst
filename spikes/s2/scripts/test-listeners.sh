#!/usr/bin/env bash
# Tests stack.sh check-listeners (the check every start runs): a process listening on any address but
# loopback, directly or through a child, must fail the check, and so must a process whose listeners
# cannot be seen. Uses throw-away python listeners on ports 19890-19893 of 127.0.0.1 / 0.0.0.0, in a
# temporary S2_HOME; stops them by PID.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
T=$(mktemp -d "${TMPDIR:-/tmp}/s2-test-listeners.XXXXXX")
pids=()
killtree() { local c; for c in $(ps -o pid= --ppid "$1" 2>/dev/null); do killtree "$c"; done; kill "$1" 2>/dev/null; return 0; }
cleanup() { for p in "${pids[@]}"; do killtree "$p"; done; rm -rf "$T"; }
trap cleanup EXIT
export S2_HOME="$T/home" LISTENER_WAIT_S=2
RUN="$S2_HOME/run/none"
fails=0
check() { local d=$1; shift; if "$@"; then echo "ok   $d"; else echo "FAIL $d"; fails=$((fails + 1)); fi; }

LISTEN='import socket,sys,time
s=socket.socket(socket.AF_INET6 if ":" in sys.argv[1] else socket.AF_INET)
s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
s.bind((sys.argv[1],int(sys.argv[2]))); s.listen(); time.sleep(120)'
rec() { # rec <name> <pid> : record "pid starttime" like stack.sh does
  mkdir -p "$RUN"; echo "$2 $(awk '{print $22}' "/proc/$2/stat")" > "$RUN/$1.pid"; pids+=("$2")
}
reset() { for p in "${pids[@]}"; do killtree "$p"; done; pids=(); rm -rf "$RUN"; sleep 0.3; }
spawn_listener() { python3 -I -c "$LISTEN" "$2" "$3" & rec "$1" $!; sleep 0.5; }
verdict() { "$HERE/stack.sh" check-listeners none > "$T/out" 2>&1; echo $?; }

for port in 19890 19891 19892 19893; do ss -H -ltn "sport = :$port" | grep -q . && { echo "port $port is in use; aborting"; exit 2; }; done

spawn_listener sim0 127.0.0.1 19890
check "a loopback listener passes" test "$(verdict)" = 0
spawn_listener sim1 127.0.0.1 19891
check "two loopback listeners pass" test "$(verdict)" = 0
reset

spawn_listener sim0 0.0.0.0 19890
rc=$(verdict)
check "a listener on 0.0.0.0 fails" test "$rc" = 1
check "the message names the address" grep -q 'listens on 0.0.0.0:19890, not loopback' "$T/out"
reset

spawn_listener sim0 127.0.0.1 19890; spawn_listener epp 0.0.0.0 19891
check "one loopback and one all-interface process fail" test "$(verdict)" = 1
reset

if python3 -I -c 'import socket; s=socket.socket(socket.AF_INET6); s.bind(("::",0))' 2>/dev/null; then
  spawn_listener sim0 :: 19892
  check "a listener on [::] fails" test "$(verdict)" = 1
  reset
else echo "skip a listener on [::] (no IPv6)"; fi

sleep 100 & rec sim0 $!
rc=$(verdict)
check "a process with no listener fails: unseen is not a pass" test "$rc" = 1
check "the message says it cannot verify" grep -q 'cannot verify' "$T/out"
reset

# a parent that listens on loopback and forks a child that listens on all interfaces
bash -c "python3 -I -c '$LISTEN' 127.0.0.1 19890 & python3 -I -c '$LISTEN' 0.0.0.0 19893 & wait" & rec sim0 $!; sleep 1
check "a child listening on 0.0.0.0 under a loopback parent fails" test "$(verdict)" = 1
reset

echo; if [[ $fails -eq 0 ]]; then echo "test-listeners: all passed"; else echo "test-listeners: $fails FAILED"; fi
exit $((fails > 0))
