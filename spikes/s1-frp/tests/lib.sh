#!/usr/bin/env bash
# lib.sh -- helpers for the S1 kit tests. Source it.
#
# S1_KIT_DIR (default: the directory above tests/) is the kit under test. The runner can point it at an
# older commit (to show a test fails before a fix) or at a mutated copy (to show a test notices a break).
# Every process a test starts is recorded with its start time and stopped by that identity; a job under test
# runs in its own session, and that session is stopped as a group at the end.

TESTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KIT="${S1_KIT_DIR:-$(cd "$TESTS_DIR/.." && pwd)}"
source "$TESTS_DIR/throwaway-sshd.sh"
S1_TEST_RESULTS="${S1_TEST_RESULTS:-}"

_start_time() { local s; s="$(cat "/proc/$1/stat" 2>/dev/null)" || return 1; s="${s##*) }"; set -- $s; echo "${20}"; }
BG_PIDS=() BG_STS=() BG_GROUPS=()

bg() {   # run a command in the background, recorded for cleanup: bg CMD ARGS... ; the pid is in $BG_LAST
  "$@" &
  BG_LAST=$!
  BG_PIDS+=("$BG_LAST"); BG_STS+=("$(_start_time "$BG_LAST")")
}
bg_session() {   # like bg, in a new session (its own process group), stopped as a group at the end
  setsid "$@" &
  BG_LAST=$!
  BG_PIDS+=("$BG_LAST"); BG_STS+=("$(_start_time "$BG_LAST")")
  BG_GROUPS+=("$BG_LAST")
}
bg_cleanup() {
  local i pid st g
  for g in "${BG_GROUPS[@]:-}"; do
    [ -n "$g" ] || continue
    kill -TERM -- "-$g" 2>/dev/null
  done
  sleep 0.5
  for g in "${BG_GROUPS[@]:-}"; do
    [ -n "$g" ] || continue
    kill -KILL -- "-$g" 2>/dev/null
  done
  for i in "${!BG_PIDS[@]}"; do
    pid="${BG_PIDS[$i]}"; st="${BG_STS[$i]}"
    if [ -n "$st" ] && [ "$(_start_time "$pid")" = "$st" ]; then kill -TERM "$pid" 2>/dev/null; fi
  done
  sleep 0.2
  for i in "${!BG_PIDS[@]}"; do
    pid="${BG_PIDS[$i]}"; st="${BG_STS[$i]}"
    if [ -n "$st" ] && [ "$(_start_time "$pid")" = "$st" ]; then kill -KILL "$pid" 2>/dev/null; fi
    { wait "$pid"; } 2>/dev/null
  done
  BG_PIDS=() BG_STS=() BG_GROUPS=()
}
pid_gone() { local pid="$1" st="$2"; [ -z "$st" ] || [ "$(_start_time "$pid")" != "$st" ] || [ "$(awk '{print $3}' "/proc/$pid/stat" 2>/dev/null | head -c1)" = Z ]; }

wait_for() {   # wait_for SECONDS COMMAND...: poll every 0.2 s until the command succeeds
  local n="$1" i; shift
  for i in $(seq 1 $(( n * 5 ))); do "$@" && return 0; sleep 0.2; done
  return 1
}
file_has() { grep -q -- "$2" "$1" 2>/dev/null; }

free_port() { python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'; }

host_platform() { case "$(uname -m)" in x86_64) echo helios-cpu ;; aarch64) echo helios-gh200 ;; *) echo unsupported ;; esac; }
host_frp_arch() { case "$(uname -m)" in x86_64) echo amd64 ;; aarch64) echo arm64 ;; esac; }

# render_job OUT: renders $KIT/job.sbatch.tmpl from the T_* variables the test has set
render_job() {
  python3 "$KIT/render_tmpl.py" "$KIT/job.sbatch.tmpl" PLATFORM="$(host_platform)" ACCOUNT=acct PARTITION=part \
    TIME=00:30:00 MEM=1G VM_ADDR="${T_VM_ADDR:-127.0.0.1}" FRPS_PORT="${T_FRPS_PORT:-7000}" VM_USER="${T_VM_USER:-nobody}" \
    SSH_KEY="${T_SSH_KEY:-/nonexistent}" SSH_KNOWN_HOSTS="${T_KNOWN_HOSTS:-/nonexistent}" LPORT="${T_LPORT:-1}" \
    PROBE_TLS_PORTS=none TOKEN_FILE="${T_TOKEN:-/nonexistent}" PROTOCOL=tcp INSTANCE_PREFIX=s1t \
    KIT_DIR="${T_KIT:-$KIT}" PYTHON=python3 > "$1"
}

# make_fake_kit DEST: a kit directory whose frpc is a script (for job-logic tests); the other files are links to $KIT
make_fake_kit() {
  local dest="$1" f arch
  arch="$(host_frp_arch)"
  mkdir -p "$dest/dl/linux_$arch/frp_fake_linux_$arch"
  for f in fake_streamer.py stream_watch.py render_tmpl.py frpc.toml.tmpl; do ln -s "$KIT/$f" "$dest/$f"; done
  cat > "$dest/dl/linux_$arch/frp_fake_linux_$arch/frpc" <<'FRPC'
#!/bin/bash
# a stand-in for frpc: logs "login to server success" after FAKE_FRPC_DELAY seconds unless FAKE_FRPC_LOGIN=never
case "${1:-}" in --version) echo "frpc fake"; exit 0 ;; verify) exit 0 ;; esac
cfg=""; while [ $# -gt 0 ]; do [ "$1" = -c ] && cfg="$2"; shift; done
log="$(sed -n 's/^log.to = "\(.*\)"$/\1/p' "$cfg")"
S=""
trap '[ -n "$S" ] && kill "$S" 2>/dev/null; exit 0' TERM INT
if [ "${FAKE_FRPC_LOGIN:-ok}" = ok ]; then
  ( sleep "${FAKE_FRPC_DELAY:-1}"; echo "2026/10/30 00:00:00 [I] [fake.go:1] [fake] login to server success, get run id [fake]" >> "$log" ) &
fi
sleep 3600 & S=$!
wait "$S"
FRPC
  chmod +x "$dest/dl/linux_$arch/frp_fake_linux_$arch/frpc"
}

# --- the result table --------------------------------------------------------------------------------
t_pass() { echo "PASS  $1"; [ -z "$S1_TEST_RESULTS" ] || echo "PASS	$1" >> "$S1_TEST_RESULTS"; }
t_fail() { echo "FAIL  $1  -- $2"; [ -z "$S1_TEST_RESULTS" ] || echo "FAIL	$1" >> "$S1_TEST_RESULTS"; }

# run_test NAME: runs the function test_NAME in a subshell with a private work directory; PASS when it returns 0
run_test() {
  local name="$1" out rc
  W="$(mktemp -d "${TMPDIR:-/tmp}/s1-t.XXXXXX")"
  out="$( ( set +e; trap 'bg_cleanup; tsshd_stop; rm -rf "$W"' EXIT; export W; "test_$name" ) 2>&1 )"; rc=$?
  rm -rf "$W"
  [ -z "${S1_TEST_VERBOSE:-}" ] || printf '%s\n' "$out"
  if [ "$rc" -eq 0 ]; then t_pass "$name"; else t_fail "$name" "$(printf '%s\n' "$out" | tail -n 6 | tr '\n' '|')"; fi
}
expect() {   # expect DESCRIPTION CONDITION-COMMAND...: prints and returns 1 when the condition fails
  local d="$1"; shift
  "$@" || { echo "expectation failed: $d"; return 1; }
}
