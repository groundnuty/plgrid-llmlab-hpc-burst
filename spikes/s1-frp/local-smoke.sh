#!/usr/bin/env bash
# local-smoke.sh -- a 120 s CONFIG CHECK of the S1 kit, entirely on this machine.
#
# It runs frps, the plugin stub, frpc, the fake streamer and the watcher on 127.0.0.1 and
# stops them all by PID. It checks that the configs load, that registration reaches the stub,
# that a stream passes through tcpmux, and that each injected fault is seen.
#
# THIS IS NOT SPIKE EVIDENCE. Same-host traffic proves nothing about compute nodes reaching
# out, and 120 s proves nothing about long streams. Issue #18 rules both out.
#
# Usage: local-smoke.sh [direct | ssh-forward | all]      (default: all, one after the other)
#   direct       frpc dials frps on loopback.
#   ssh-forward  frpc dials the local end of "ssh -N -L" to frps on 127.0.0.1:<S1_TEST_FORWARD_PORT>,
#                the same path as job.sbatch.tmpl with TRANSPORT=ssh-forward. It runs ONLY if
#                S1_TEST_KEY names a restricted key that is already authorised for this user on
#                this machine, with permitopen for 127.0.0.1:<S1_TEST_FORWARD_PORT> (default 7000).
#                If S1_TEST_KEY is unset the variant prints SKIPPED, tests nothing and exits 0.
#                S1_TEST_USER (default: the current user) names the account the key logs in as.
#                This script creates no key and never touches ~/.ssh or authorized_keys.
#
# Needs the pinned binaries: bash fetch-frp.sh. Writes only under a mktemp -d directory.
# Timeline (seconds after the watcher starts; the watcher runs 120 s). Stream 1 runs from about
# 1 to 41 and is left alone. Stream 2 starts at about 42 and holds a 3 s freeze of frpc at 45
# (calibration: the gap detector must see a stall it was told about). Faults: 55 (kill the
# engine), 72 (freeze frpc until frps drops it), 108 (kill frpc).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="${PYTHON:-python3}"
VARIANT="${1:-all}"
case "$VARIANT" in
  direct|ssh-forward) ;;
  all)   # each variant in its own process, so each has its own cleanup; the exit code is the first failure
    rc=0
    bash "${BASH_SOURCE[0]}" direct || rc=$?
    rc2=0; bash "${BASH_SOURCE[0]}" ssh-forward || rc2=$?
    [ "$rc" -ne 0 ] || rc=$rc2
    exit "$rc" ;;
  *) echo "usage: local-smoke.sh [direct | ssh-forward | all]" >&2; exit 2 ;;
esac
if [ "$VARIANT" = ssh-forward ] && [ -z "${S1_TEST_KEY:-}" ]; then
  echo "local-smoke: SKIPPED the ssh-forward variant: S1_TEST_KEY is not set. Nothing was tested; this is not a pass."
  echo "local-smoke:   It needs a restricted key already authorised on this machine (see the header of this script)."
  exit 0
fi
INSTANCE="smoke-instance"
DURATION=120

case "$(uname -m)" in
  x86_64) ARCH=amd64 ;;
  aarch64|arm64) ARCH=arm64 ;;
  *) echo "local-smoke: no frp build for $(uname -m)" >&2; exit 2 ;;
esac
BIN="$(ls -d "$HERE"/dl/linux_"$ARCH"/frp_*_linux_"$ARCH" 2>/dev/null | head -1 || true)"
[ -n "$BIN" ] && [ -x "$BIN/frps" ] && [ -x "$BIN/frpc" ] || { echo "local-smoke: run 'bash fetch-frp.sh' first" >&2; exit 2; }

WORK="$(mktemp -d "${TMPDIR:-/tmp}/s1-smoke.XXXXXX")"
chmod 700 "$WORK"
FRPS_PID="" FRPC_PID="" STUB_PID="" STREAMER_PID="" WATCH_PID="" CTRL_PID="" SSH_PID=""

all_pids() { echo "$WATCH_PID $FRPC_PID $FRPS_PID $STUB_PID $STREAMER_PID $CTRL_PID $SSH_PID"; }

cleanup() {
  local rc=$? pid
  trap - EXIT INT TERM
  for pid in $(all_pids); do
    if kill -0 "$pid" 2>/dev/null; then kill -CONT "$pid" 2>/dev/null || true; kill -TERM "$pid" 2>/dev/null || true; fi
  done
  for _ in $(seq 1 25); do
    local alive=0
    for pid in $(all_pids); do kill -0 "$pid" 2>/dev/null && alive=1; done
    [ "$alive" -eq 0 ] && break
    sleep 0.2
  done
  for pid in $(all_pids); do kill -0 "$pid" 2>/dev/null && kill -KILL "$pid" 2>/dev/null || true; done
  { wait; } 2>/dev/null || true
  if [ "$rc" -ne 0 ]; then echo "local-smoke: FAILED (rc=$rc); logs kept in $WORK" >&2; else rm -rf "$WORK"; fi
  exit "$rc"
}
trap cleanup EXIT INT TERM

step() { echo "[$(date -u +%H:%M:%S)] $*"; }
reap() { { wait "$1"; } 2>/dev/null || true; }

# --- free loopback ports, a throwaway token, rendered configs ---------------------------------
read -r BIND_PORT TCPMUX_PORT PLUGIN_PORT STREAM_PORT LPORT < <("$PYTHON" -c '
import socket
socks = [socket.socket() for _ in range(5)]
for s in socks:
    s.bind(("127.0.0.1", 0))
print(" ".join(str(s.getsockname()[1]) for s in socks))')
SERVER_PORT="$BIND_PORT"
if [ "$VARIANT" = ssh-forward ]; then
  # frps must listen on exactly the one port the test key may forward to; frpc dials the local end.
  BIND_PORT="${S1_TEST_FORWARD_PORT:-7000}"
  SERVER_PORT="$LPORT"
  "$PYTHON" -c 'import socket, sys; s = socket.socket(); s.bind(("127.0.0.1", int(sys.argv[1])))' "$BIND_PORT" 2>/dev/null \
    || { echo "local-smoke: 127.0.0.1:$BIND_PORT is in use; the ssh-forward variant needs it free (set S1_TEST_FORWARD_PORT to the port the key may forward to)" >&2; exit 2; }
  [ -f "$S1_TEST_KEY" ] && [ -r "$S1_TEST_KEY" ] || { echo "local-smoke: S1_TEST_KEY is not a readable file" >&2; exit 2; }
  case "$(stat -c %a "$S1_TEST_KEY")" in 600|400) ;; *) echo "local-smoke: S1_TEST_KEY must be mode 600 or 400" >&2; exit 2 ;; esac
  HOSTKEY_PUB=/etc/ssh/ssh_host_ed25519_key.pub
  [ -r "$HOSTKEY_PUB" ] || { echo "local-smoke: cannot read $HOSTKEY_PUB to pin this machine's host key" >&2; exit 2; }
fi
umask 077
head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$WORK/token"
chmod 600 "$WORK/token"
[ "$(stat -c %a "$WORK/token")" = 600 ] || { echo "token file is not mode 600" >&2; exit 1; }

render() { "$PYTHON" "$HERE/render_tmpl.py" "$@"; }
render "$HERE/frps.toml.tmpl" BIND_ADDR=127.0.0.1 BIND_PORT="$BIND_PORT" TCPMUX_PORT="$TCPMUX_PORT" \
  PROXY_BIND_ADDR=127.0.0.1 TOKEN_FILE="$WORK/token" PLUGIN_ADDR="127.0.0.1:$PLUGIN_PORT" \
  LOG_FILE="$WORK/frps.log" > "$WORK/frps.toml"
render "$HERE/frpc.toml.tmpl" SERVER_ADDR=127.0.0.1 SERVER_PORT="$SERVER_PORT" INSTANCE="$INSTANCE" \
  LOG_FILE="$WORK/frpc.log" TOKEN_FILE="$WORK/token" PROTOCOL=tcp LOCAL_PORT="$STREAM_PORT" > "$WORK/frpc.toml"
step "frps verify: $("$BIN/frps" verify -c "$WORK/frps.toml" 2>&1 | tail -1)"
step "frpc verify: $("$BIN/frpc" verify -c "$WORK/frpc.toml" 2>&1 | tail -1)"

# --- ssh-forward variant: the forward first. One ssh, no reconnect loop (the job has the loop). The
# options are those of build_ssh_args in job.sbatch.tmpl; keep the two in step. The host key is pinned
# from this machine's public host key file (a file read, not a scan of the network).
if [ "$VARIANT" = ssh-forward ]; then
  printf '127.0.0.1 %s\n' "$(cut -d' ' -f1,2 "$HOSTKEY_PUB")" > "$WORK/known_hosts"
  ssh -F /dev/null -p 22 -N -T -i "$S1_TEST_KEY" \
    -o BatchMode=yes -o IdentitiesOnly=yes -o ExitOnForwardFailure=yes \
    -o ServerAliveInterval=10 -o ServerAliveCountMax=3 \
    -o StrictHostKeyChecking=yes -o "UserKnownHostsFile=$WORK/known_hosts" -o GlobalKnownHostsFile=/dev/null \
    -o ControlMaster=no -o ControlPath=none \
    -o ConnectTimeout=8 -o PreferredAuthentications=publickey \
    -L "127.0.0.1:$LPORT:127.0.0.1:$BIND_PORT" "${S1_TEST_USER:-$(id -un)}@127.0.0.1" 2>"$WORK/ssh.err" </dev/null &
  SSH_PID=$!
  for _ in $(seq 1 60); do
    kill -0 "$SSH_PID" 2>/dev/null || break
    timeout 2 bash -c 'exec 3<>"/dev/tcp/127.0.0.1/$1"' _ "$LPORT" 2>/dev/null && break
    sleep 0.25
  done
  if ! kill -0 "$SSH_PID" 2>/dev/null || ! timeout 2 bash -c 'exec 3<>"/dev/tcp/127.0.0.1/$1"' _ "$LPORT" 2>/dev/null; then
    echo "local-smoke: S1_TEST_KEY did not open the forward within 15 s. ssh said: $(tail -n 1 "$WORK/ssh.err")" >&2; exit 2
  fi
  step "ssh forward is up (pid $SSH_PID): 127.0.0.1:$LPORT -> 127.0.0.1:$BIND_PORT through this machine's sshd"
fi

# --- start: engine, then frpc BEFORE frps (loginFailExit=false must keep it alive) -------------
start_streamer() {
  "$PYTHON" "$HERE/fake_streamer.py" --port "$STREAM_PORT" >/dev/null 2>>"$WORK/streamer.log" &
  STREAMER_PID=$!
}
start_streamer
for _ in $(seq 1 25); do curl -s -m 1 -o /dev/null "http://127.0.0.1:$STREAM_PORT/health" && break; sleep 0.2; done

# Negative control: the same config with the frp default loginFailExit = true must end by itself.
sed 's/^loginFailExit = false$/loginFailExit = true/' "$WORK/frpc.toml" | sed "s|^log.to = .*|log.to = \"$WORK/frpc-control.log\"|" > "$WORK/frpc-control.toml"
grep -q '^loginFailExit = true$' "$WORK/frpc-control.toml" || { echo "local-smoke: control config was not built" >&2; exit 1; }
"$BIN/frpc" -c "$WORK/frpc-control.toml" >"$WORK/frpc-control.out" 2>&1 &
CTRL_PID=$!
"$BIN/frpc" -c "$WORK/frpc.toml" >"$WORK/frpc.out" 2>&1 &
FRPC_PID=$!
step "frpc started before frps (pid $FRPC_PID), plus a control frpc with loginFailExit=true (pid $CTRL_PID); waiting 4 s"
sleep 4
if kill -0 "$FRPC_PID" 2>/dev/null; then LOGINFAIL_OK=1; step "  frpc is still running: loginFailExit=false works"
else LOGINFAIL_OK=0; step "  frpc EXITED: loginFailExit=false did not hold"; fi
if kill -0 "$CTRL_PID" 2>/dev/null; then CONTROL_OK=0; step "  control frpc is still running: the loginFailExit check would prove nothing"
else CONTROL_OK=1; reap "$CTRL_PID"; step "  control frpc (loginFailExit=true) ended by itself, as the frp default does"; fi

"$PYTHON" "$HERE/plugin_stub.py" --listen "127.0.0.1:$PLUGIN_PORT" --log "$WORK/stub.jsonl" >/dev/null 2>"$WORK/stub.err" &
STUB_PID=$!
"$BIN/frps" -c "$WORK/frps.toml" >"$WORK/frps.out" 2>&1 &
FRPS_PID=$!
step "stub pid $STUB_PID, frps pid $FRPS_PID; waiting for the proxy to register through the stub"
for _ in $(seq 1 60); do
  grep -q '"op": "NewProxy"' "$WORK/stub.jsonl" 2>/dev/null && break
  sleep 1
done
grep -q '"op": "NewProxy"' "$WORK/stub.jsonl" 2>/dev/null || { echo "local-smoke: no NewProxy reached the stub in 60 s" >&2; exit 1; }

# --- watcher and fault timeline ------------------------------------------------------------------
"$PYTHON" "$HERE/stream_watch.py" --tcpmux "127.0.0.1:$TCPMUX_PORT" --domain "$INSTANCE" \
  --duration "$DURATION" --stream-seconds 40 --fault-window 8 --direct-url "http://127.0.0.1:$STREAM_PORT" \
  --stub-log "$WORK/stub.jsonl" --out "$WORK/summary.json" >"$WORK/watch.out" 2>&1 &
WATCH_PID=$!
T0="$(date +%s.%N)"
step "watcher pid $WATCH_PID started; the run takes $DURATION s"
sleep_until() {  # sleep until T0 + $1 seconds
  local d
  d="$(awk -v t0="$T0" -v now="$(date +%s.%N)" -v at="$1" 'BEGIN { d = t0 + at - now; print (d > 0 ? d : 0) }')"
  sleep "$d"
}
mark() { kill -USR1 "$WATCH_PID"; }

sleep_until 45
step "CALIBRATION (inside stream 2): mark, SIGSTOP frpc for 3 s (the token gap detector must report it)"
mark; kill -STOP "$FRPC_PID"
sleep_until 48
kill -CONT "$FRPC_PID"
sleep_until 55
step "FAULT 1 (engine dies, frpc stays): mark, SIGKILL fake_streamer pid $STREAMER_PID"
mark; kill -KILL "$STREAMER_PID"; reap "$STREAMER_PID"
sleep_until 63
step "engine restarted"
start_streamer
sleep_until 72
step "FAULT 2 (silent loss, no FIN): mark, SIGSTOP frpc pid $FRPC_PID"
mark; kill -STOP "$FRPC_PID"
sleep_until 100
step "frpc continued (reconnect expected)"
kill -CONT "$FRPC_PID"
sleep_until 108
step "FAULT 3 (frpc killed): mark, SIGKILL frpc pid $FRPC_PID"
mark; kill -KILL "$FRPC_PID"; reap "$FRPC_PID"

WATCH_RC=0
wait "$WATCH_PID" || WATCH_RC=$?
step "watcher exited rc=$WATCH_RC"
cat "$WORK/watch.out"
[ -s "$WORK/summary.json" ] || { echo "local-smoke: watcher wrote no summary" >&2; exit 1; }

# --- judge the run: a config check, with the numbers shown -----------------------------------------
echo
echo "=== local smoke result [$VARIANT]: CONFIG CHECK ONLY, same host, ${DURATION} s. Not spike evidence. ==="
SSH_ALIVE=na
if [ -n "$SSH_PID" ]; then
  if kill -0 "$SSH_PID" 2>/dev/null; then SSH_ALIVE=1; else SSH_ALIVE=0; fi
fi
RC=0
"$PYTHON" -I - "$WORK" "$INSTANCE" "$LOGINFAIL_OK" "$CONTROL_OK" "$SSH_ALIVE" <<'PY' || RC=$?
import json, sys
work, instance, loginfail_ok, control_ok = sys.argv[1], sys.argv[2], sys.argv[3] == "1", sys.argv[4] == "1"
ssh_alive = sys.argv[5]
summary = json.load(open(work + "/summary.json"))
stub = [json.loads(l) for l in open(work + "/stub.jsonl") if l.strip()]
mine = [e for e in stub if e.get("user") == instance]
ops = {}
for e in mine:
    ops[e["op"]] = ops.get(e["op"], 0) + 1
results = []

def check(name, ok, detail):
    results.append(ok)
    print("%s  %s  (%s)" % ("PASS" if ok else "FAIL", name, detail))

print("stub log, calls for %s by op: %s" % (instance, json.dumps(ops, sort_keys=True)))
newproxy = next((e for e in mine if e["op"] == "NewProxy"), {})
check("frpc survived a failed first login (loginFailExit=false)", loginfail_ok, "started before frps")
check("control: the same config with loginFailExit=true ends frpc by itself", control_ok, "so the line above is not vacuous")
if ssh_alive != "na":
    check("the ssh forward process stayed up for the whole run (no unprompted exit)", ssh_alive == "1", "faults hit frpc and the engine only")
check("Login reached the stub", ops.get("Login", 0) >= 1, "%d Login" % ops.get("Login", 0))
check("NewProxy reached the stub as a tcpmux/httpconnect proxy named by the instance",
      newproxy.get("proxy_type") == "tcpmux" and newproxy.get("multiplexer") == "httpconnect"
      and instance in (newproxy.get("domains") or []), json.dumps({k: newproxy.get(k) for k in ("proxy", "proxy_type", "multiplexer", "domains")}, sort_keys=True))
check("Ping reaches the stub (application heartbeat is on)", ops.get("Ping", 0) >= 3, "%d Ping" % ops.get("Ping", 0))

s1 = summary["streams"][0] if summary["streams"] else {}
g = summary["gaps"]
check("first stream completed, no cut, no sequence break", bool(s1.get("completed")) and s1.get("seq_breaks") == 0,
      "%s tokens of %s in %s s, ttfb %s s" % (s1.get("tokens"), s1.get("requested_s"), s1.get("duration_s"), s1.get("ttfb_s")))
in_s1 = [o for o in g["over_threshold"] if o["stream"] == 1]
check("stream 1 has no token gap over 2 s (no fault-window exclusion applied)", not in_s1 and s1.get("tokens") == 40,
      "gaps over 2 s in stream 1: %d" % len(in_s1))
check("no token gap above 2 s anywhere outside fault windows", g["over_threshold_unfaulted"] == 0 and (g["stats_unfaulted_s"].get("max") or 0) <= 2.0,
      "max unfaulted gap %s s, p99 %s s, %d gaps over 2 s outside fault windows" % (g["stats_unfaulted_s"].get("max"), g["stats_unfaulted_s"].get("p99"), g["over_threshold_unfaulted"]))

marks = summary["marks"]
check("four marks were taken (calibration plus three faults)", len(marks) == 4, "%d" % len(marks))
stalls = [o for o in g["over_threshold"] if o["near_mark"] and 2.5 <= o["gap_s"] <= 5.0]
check("calibration: the gap detector reports the deliberate 3 s stall", len(stalls) >= 1,
      "gaps over 2 s near a mark: %s" % json.dumps([o["gap_s"] for o in g["over_threshold"] if o["near_mark"]]))
stall_stream = next((x for x in summary["streams"] if stalls and x["id"] == stalls[0]["stream"]), {})
check("calibration: the stall fell in stream 2 and tokens kept coming after it",
      bool(stalls) and stalls[0]["stream"] == 2 and stall_stream.get("tokens", 0) > stalls[0]["seq"] + 2,
      "stall at token %s of stream %s, which delivered %s tokens in all" % (stalls[0]["seq"] if stalls else None, stalls[0]["stream"] if stalls else None, stall_stream.get("tokens")))
if len(marks) == 4:
    _, m1, m2, m3 = marks
    check("fault 1, engine killed: /health declared down within 10 s, class no_response",
          m1.get("probe_declared_down_s", 99) <= 10 and m1.get("probe_class") == "no_response",
          "declared down after %s s, class %s" % (m1.get("probe_declared_down_s"), m1.get("probe_class")))
    check("fault 1: the path stays up (no CloseProxy, Pings keep arriving)",
          m1.get("stub_close_proxy_s") is None and (m1.get("stub_pings_in_30s_after") or 0) >= 1,
          "CloseProxy %s, %s Pings in the next 30 s" % (m1.get("stub_close_proxy_s"), m1.get("stub_pings_in_30s_after")))
    check("fault 1: /health recovered after the engine came back", m1.get("probe_recovered_s") is not None,
          "recovered after %s s" % m1.get("probe_recovered_s"))
    check("fault 2, frpc frozen: /health declared down within 10 s",
          m2.get("probe_declared_down_s", 99) <= 10, "declared down after %s s, class %s" % (m2.get("probe_declared_down_s"), m2.get("probe_class")))
    check("fault 2: CloseProxy at the stub within the 30 s silent-loss budget",
          m2.get("stub_close_proxy_s") is not None and m2["stub_close_proxy_s"] <= 30,
          "CloseProxy after %s s (yamux keepalive 10 s + 10 s, heartbeat backstop 25 s)" % m2.get("stub_close_proxy_s"))
    check("fault 2: frpc reconnected after SIGCONT (new Login), /health recovered",
          m2.get("stub_login_after_s") is not None and m2.get("probe_recovered_s") is not None,
          "new Login after %s s, recovered after %s s" % (m2.get("stub_login_after_s"), m2.get("probe_recovered_s")))
    check("fault 3, frpc killed: CloseProxy at the stub within 5 s",
          m3.get("stub_close_proxy_s") is not None and m3["stub_close_proxy_s"] <= 5, "CloseProxy after %s s" % m3.get("stub_close_proxy_s"))
    check("fault 3: /health declared down within 10 s", m3.get("probe_declared_down_s", 99) <= 10,
          "declared down after %s s, class %s" % (m3.get("probe_declared_down_s"), m3.get("probe_class")))
al = summary.get("added_latency_ms")
print("loopback latency (tunnel minus direct, ms; meaningless as a network figure): %s" % json.dumps(al, sort_keys=True))
print("%d of %d checks passed" % (sum(results), len(results)))
sys.exit(0 if all(results) else 1)
PY
echo
echo "stub log, one line per call (user, op, seconds since the first call):"
"$PYTHON" -I -c '
import json, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
t0 = rows[0]["epoch"] if rows else 0
for e in rows:
    if e["op"] != "Ping":
        print("  +%7.1f s  %-10s %s %s" % (e["epoch"] - t0, e["op"], e.get("user"), e.get("proxy") or ""))
print("  (%d Ping lines not shown)" % sum(1 for e in rows if e["op"] == "Ping"))' "$WORK/stub.jsonl"
exit "$RC"
