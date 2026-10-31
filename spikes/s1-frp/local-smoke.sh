#!/usr/bin/env bash
# local-smoke.sh -- a CONFIG CHECK of the S1 kit's alpha wiring, entirely on this machine.
#
# It starts the VM side on 127.0.0.1 (Envoy with a throwaway CA's certificate on the dial-home port, frps behind it,
# the plugin stub, the agent endpoint's stand-in and the path-rate sink), runs job.sbatch.tmpl itself as a plain
# script, and a 120 s watcher with the fault timeline. It checks registration, a stream through tcpmux with the
# Instance UID as the domain, the path-rate measurement, fault detection, and negative controls (a wrong CA, a wrong
# server name and a wrong domain must fail). It stops everything by PID, and the job by its session.
#
# THIS IS NOT SPIKE EVIDENCE. Same-host traffic proves nothing about compute nodes reaching out, and 120 s proves
# nothing about long streams. Issue #18 rules both out. The path rate is CAPPED (SMOKE_CAP_BPS, default 4,000,000
# B/s) so that the shared VM's load stays low; the check is that the measured rate matches the cap.
#
# Usage: local-smoke.sh [direct | ssh | all]      (default: all, one after the other)
#   direct  the job dials the VM side's two ports on 127.0.0.1 itself (TRANSPORT=direct).
#   ssh     the job's two separate "ssh -N -L" forwards go through a THROWAWAY sshd that this script starts as the
#           current user (not root) on a loopback port, with its own host key, its own client key, its own
#           AuthorizedKeysFile holding the README's key line (its two ports swapped for the smoke's), in a temporary
#           directory (tests/throwaway-sshd.sh). It never reads or writes ~/.ssh or /etc/ssh. If no sshd can run here
#           the variant prints SKIPPED, tests nothing and exits 0: not a pass.
#
# Every listener binds 127.0.0.1 on a port in 29400-29499. Needs the pinned binaries: bash fetch-frp.sh, and Envoy
# 1.33.2 at ENVOY_BIN (default dl/envoy/envoy-1.33.2; checked with fetch-envoy.sh verify). Waits while the 1-minute
# load is above 4.0 (the shared VM's rule), at most 10 min. Writes only under a mktemp -d directory.
# Timeline, in seconds after the watcher's run clock starts (the job first spends about 2 min on the path rate):
# stream 1 runs from about 1 to 41 and is left alone; stream 2 starts at about 42 and holds a 3 s freeze of frpc at
# 45 (calibration: the gap detector must see a stall it was told about). Faults: 55 USR2 (the job kills the engine,
# restarts it 8 s later), 72 HUP (the job freezes frpc for 28 s), 108 USR1 (the job kills frpc).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="${PYTHON:-python3}"
VARIANT="${1:-all}"
case "$VARIANT" in
  direct|ssh) ;;
  all)   # each variant in its own process, so each has its own cleanup; the exit code is the first failure
    rc=0
    bash "${BASH_SOURCE[0]}" direct || rc=$?
    rc2=0; bash "${BASH_SOURCE[0]}" ssh || rc2=$?
    [ "$rc" -ne 0 ] || rc=$rc2
    exit "$rc" ;;
  *) echo "usage: local-smoke.sh [direct | ssh | all]" >&2; exit 2 ;;
esac
# shellcheck source=tests/throwaway-sshd.sh
source "$HERE/tests/throwaway-sshd.sh"          # the port allocator (29400-29499), and the sshd of the ssh variant
if [ "$VARIANT" = ssh ]; then
  if [ ! -x "$TSSHD_SSHD" ] || ! command -v ssh-keygen >/dev/null 2>&1 || { [ ! -d /run/sshd ] && [ ! -d /var/empty ]; }; then
    echo "local-smoke: SKIPPED the ssh variant: no sshd can run here as a plain user. Nothing was tested; this is not a pass."
    exit 0
  fi
fi
INSTANCE="smoke-instance"
INSTANCE_UID="5e1f0000-0000-4000-8000-000000000001"
TLS_NAME="dial-home.hpcburst.invalid"
DURATION=120
CAP="${SMOKE_CAP_BPS:-4000000}"
PATHRATE_S=30
JOBID=4242

case "$(uname -m)" in
  x86_64) ARCH=amd64; PLATFORM=helios-cpu ;;
  aarch64|arm64) ARCH=arm64; PLATFORM=helios-gh200 ;;
  *) echo "local-smoke: no frp build for $(uname -m)" >&2; exit 2 ;;
esac
BIN="$(ls -d "$HERE"/dl/linux_"$ARCH"/frp_*_linux_"$ARCH" 2>/dev/null | head -1 || true)"
[ -n "$BIN" ] && [ -x "$BIN/frps" ] && [ -x "$BIN/frpc" ] || { echo "local-smoke: run 'bash fetch-frp.sh' first" >&2; exit 2; }
ENVOY_BIN="${ENVOY_BIN:-$HERE/dl/envoy/envoy-1.33.2}"
bash "$HERE/fetch-envoy.sh" verify "$ENVOY_BIN" >/dev/null || { echo "local-smoke: ENVOY_BIN is not the pinned Envoy 1.33.2 (bash fetch-envoy.sh)" >&2; exit 2; }

step() { echo "[$(date -u +%H:%M:%S)] $*"; }
for _ in $(seq 1 10); do
  load="$(cut -d' ' -f1 /proc/loadavg)"
  awk -v l="$load" 'BEGIN { exit !(l <= 4.0) }' && break
  step "load $load is above 4.0; waiting 60 s"
  sleep 60
done
awk -v l="$(cut -d' ' -f1 /proc/loadavg)" 'BEGIN { exit !(l <= 4.0) }' || { echo "local-smoke: the machine stayed busy (load above 4.0); not started" >&2; exit 2; }

WORK="$(mktemp -d "${TMPDIR:-/tmp}/s1-smoke.XXXXXX")"
chmod 700 "$WORK"
S1_PORT_STATE="$WORK/ports"; export S1_PORT_STATE
STUB_PID="" FRPS_PID="" SINK_PID="" AGENT_PID="" ENVOY_PID="" WATCH_PID="" JOB_PID="" LFE_PID="" CTRL_PID="" NEG_PIDS=""
SSHD_STARTED=0

all_pids() { echo "$WATCH_PID $LFE_PID $CTRL_PID $NEG_PIDS $ENVOY_PID $AGENT_PID $SINK_PID $FRPS_PID $STUB_PID"; }
cleanup() {
  local rc=$? pid
  trap - EXIT INT TERM
  if [ -n "$JOB_PID" ] && kill -0 "$JOB_PID" 2>/dev/null; then
    kill -TERM -- "-$JOB_PID" 2>/dev/null || true
    for _ in $(seq 1 50); do kill -0 "$JOB_PID" 2>/dev/null || break; sleep 0.2; done
    kill -KILL -- "-$JOB_PID" 2>/dev/null || true
  fi
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
  [ "$SSHD_STARTED" -eq 0 ] || tsshd_stop      # before the blanket wait below: the throwaway sshd is our child and would block it
  { wait; } 2>/dev/null || true
  if [ "$rc" -ne 0 ]; then echo "local-smoke: FAILED (rc=$rc); logs kept in $WORK" >&2; else rm -rf "$WORK"; fi
  exit "$rc"
}
trap cleanup EXIT INT TERM
reap() { { wait "$1"; } 2>/dev/null || true; }
port_up() { timeout 1 bash -c 'exec 3<>"/dev/tcp/127.0.0.1/$1"' _ "$1" 2>/dev/null; }
wait_port() { local i; for i in $(seq 1 50); do port_up "$1" && return 0; sleep 0.2; done; echo "local-smoke: nothing listens on 127.0.0.1:$1" >&2; return 1; }
render() { "$PYTHON" "$HERE/render_tmpl.py" "$@"; }

# --- ports (29400-29499), the test CA, a throwaway token ------------------------------------------------
AGENT_PORT="$(tsshd_free_port)"; DIALHOME_PORT="$(tsshd_free_port)"; FRPS_PORT="$(tsshd_free_port)"; TCPMUX_PORT="$(tsshd_free_port)"
PLUGIN_PORT="$(tsshd_free_port)"; SINK_PORT="$(tsshd_free_port)"; ENGINE_PORT="$(tsshd_free_port)"
LPORT_CTL="$(tsshd_free_port)"; LPORT_TUN="$(tsshd_free_port)"; CTL_ENGINE_PORT="$(tsshd_free_port)"
umask 077
head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$WORK/token"
chmod 600 "$WORK/token"
bash "$HERE/make-test-ca.sh" "$WORK/ca" "$TLS_NAME" >/dev/null
bash "$HERE/make-test-ca.sh" "$WORK/otherca" "$TLS_NAME" >/dev/null     # for the wrong-CA control
CA="$WORK/ca/ca.crt"

# --- the VM side: stub, sink, agent stand-in, Envoy now; frps after the loginFailExit check ---------------
render "$HERE/frps.toml.tmpl" BIND_ADDR=127.0.0.1 BIND_PORT="$FRPS_PORT" TCPMUX_PORT="$TCPMUX_PORT" \
  PROXY_BIND_ADDR=127.0.0.1 TOKEN_FILE="$WORK/token" PLUGIN_ADDR="127.0.0.1:$PLUGIN_PORT" LOG_FILE="$WORK/frps.log" > "$WORK/frps.toml"
render "$HERE/envoy.yaml.tmpl" LISTEN_PORT="$DIALHOME_PORT" CERT_FILE="$WORK/ca/server.crt" KEY_FILE="$WORK/ca/server.key" \
  FRPS_PORT="$FRPS_PORT" SINK_PORT="$SINK_PORT" > "$WORK/envoy.yaml"
step "frps verify: $("$BIN/frps" verify -c "$WORK/frps.toml" 2>&1 | tail -1)"
step "envoy validate: $(nice -n 10 "$ENVOY_BIN" --mode validate -c "$WORK/envoy.yaml" --disable-hot-restart 2>&1 | tail -1)"
"$PYTHON" "$HERE/plugin_stub.py" --listen "127.0.0.1:$PLUGIN_PORT" --log "$WORK/stub.jsonl" >/dev/null 2>"$WORK/stub.err" &
STUB_PID=$!
"$PYTHON" "$HERE/path_rate.py" serve --listen "127.0.0.1:$SINK_PORT" --max-seconds 1800 --log "$WORK/sink.jsonl" 2>"$WORK/sink.err" &
SINK_PID=$!
"$PYTHON" "$HERE/path_rate.py" serve --listen "127.0.0.1:$AGENT_PORT" --tls-cert "$WORK/ca/server.crt" --tls-key "$WORK/ca/server.key" \
  --max-seconds 1800 --log "$WORK/agent.jsonl" 2>"$WORK/agent.err" &
AGENT_PID=$!
nice -n 10 "$ENVOY_BIN" -c "$WORK/envoy.yaml" --disable-hot-restart --concurrency 2 --log-level warn >"$WORK/envoy.log" 2>&1 &
ENVOY_PID=$!
wait_port "$SINK_PORT"; wait_port "$AGENT_PORT"; wait_port "$DIALHOME_PORT"
for p in "$AGENT_PORT" "$DIALHOME_PORT"; do
  "$PYTHON" "$HERE/path_rate.py" probe --host 127.0.0.1 --port "$p" --tls --ca "$CA" --server-name "$TLS_NAME" > "$WORK/vmprobe-$p.json" \
    || { echo "local-smoke: the VM side does not answer the verified probe on $p: $(cat "$WORK/vmprobe-$p.json")" >&2; exit 1; }
done
step "VM side up: Envoy pid $ENVOY_PID on $DIALHOME_PORT (TLS), agent stand-in pid $AGENT_PID on $AGENT_PORT (TLS), sink $SINK_PORT, stub $PLUGIN_PORT; both answer the verified probe"

# frpc configs for the checks before the job: rendered from the kit's template, straight at Envoy
frpc_cfg() {   # NAME USER UID CA SERVER-NAME: renders $WORK/NAME.toml
  render "$HERE/frpc.toml.tmpl" SERVER_ADDR=127.0.0.1 SERVER_PORT="$DIALHOME_PORT" INSTANCE="$2" INSTANCE_UID="$3" \
    LOG_FILE="$WORK/$1.log" TOKEN_FILE="$WORK/token" CA_FILE="$4" TLS_SERVER_NAME="$5" LOCAL_PORT="$CTL_ENGINE_PORT" > "$WORK/$1.toml"
}
CTL_UID="5e1f0000-0000-4000-8000-0000000000c0"

# --- loginFailExit: frpc started while frps is down (Envoy answers 503) must keep retrying -------------------
frpc_cfg lfe smoke-lfe "$CTL_UID" "$CA" "$TLS_NAME"
sed 's/^loginFailExit = false$/loginFailExit = true/' "$WORK/lfe.toml" | sed "s|^log.to = .*|log.to = \"$WORK/lfe-control.log\"|" > "$WORK/lfe-control.toml"
grep -q '^loginFailExit = true$' "$WORK/lfe-control.toml" || { echo "local-smoke: control config was not built" >&2; exit 1; }
"$BIN/frpc" -c "$WORK/lfe-control.toml" >"$WORK/lfe-control.out" 2>&1 &
CTRL_PID=$!
"$BIN/frpc" -c "$WORK/lfe.toml" >"$WORK/lfe.out" 2>&1 &
LFE_PID=$!
step "frpc started before frps (pid $LFE_PID), plus a control frpc with loginFailExit=true (pid $CTRL_PID); waiting 6 s"
sleep 6
if kill -0 "$LFE_PID" 2>/dev/null; then LOGINFAIL_OK=1; else LOGINFAIL_OK=0; fi
if kill -0 "$CTRL_PID" 2>/dev/null; then CONTROL_OK=0; else CONTROL_OK=1; fi
kill -TERM "$LFE_PID" "$CTRL_PID" 2>/dev/null || true; reap "$LFE_PID"; reap "$CTRL_PID"; LFE_PID="" CTRL_PID=""

"$BIN/frps" -c "$WORK/frps.toml" >"$WORK/frps.out" 2>&1 &
FRPS_PID=$!
wait_port "$TCPMUX_PORT"
step "frps pid $FRPS_PID (bind $FRPS_PORT behind Envoy, tcpmux $TCPMUX_PORT)"

# --- negative controls for verified wss: a wrong server name and another CA must not log in; the right ones must
frpc_cfg neg-name smoke-neg-name "$CTL_UID" "$CA" wrong-name.invalid
frpc_cfg neg-ca smoke-neg-ca "$CTL_UID" "$WORK/otherca/ca.crt" "$TLS_NAME"
frpc_cfg pos smoke-pos "$CTL_UID" "$CA" "$TLS_NAME"
for n in neg-name neg-ca pos; do "$BIN/frpc" -c "$WORK/$n.toml" >"$WORK/$n.out" 2>&1 & NEG_PIDS="$NEG_PIDS $!"; done
sleep 8
NEG=""
for n in neg-name neg-ca pos; do
  if grep -q 'login to server success' "$WORK/$n.log" 2>/dev/null; then NEG="$NEG $n=login"; else NEG="$NEG $n=none"; fi
done
for pid in $NEG_PIDS; do kill -TERM "$pid" 2>/dev/null || true; reap "$pid"; done; NEG_PIDS=""
step "verified-wss controls after 8 s:$NEG"

# --- ssh variant: the throwaway sshd authorising the README's key line, ports swapped for the smoke's -----------
VM_USER=none SSH_KEY=none KNOWN_HOSTS=none LCTL=none LTUN=none SSH_PORT_ENV=""
if [ "$VARIANT" = ssh ]; then
  tsshd_start || { echo "local-smoke: the throwaway sshd did not start" >&2; exit 2; }
  SSHD_STARTED=1
  KEY_OPTS="$(sed -n 's/^ *\(restrict,port-forwarding,[^ ]*\) ssh-ed25519 .*/\1/p' "$HERE/README.md" | head -n 1 \
    | sed "s/127\.0\.0\.1:29223/127.0.0.1:$AGENT_PORT/; s/127\.0\.0\.1:29227/127.0.0.1:$DIALHOME_PORT/")"
  case "$KEY_OPTS" in *"permitopen=\"127.0.0.1:$AGENT_PORT\",permitopen=\"127.0.0.1:$DIALHOME_PORT\""*) ;;
    *) echo "local-smoke: the README's key line was not found, or its two ports are not 29223 and 29227: '$KEY_OPTS'" >&2; exit 2 ;; esac
  tsshd_authorize "$KEY_OPTS"
  step "throwaway sshd on 127.0.0.1:$TSSHD_PORT (pid $TSSHD_PID), key line from the README with its ports swapped"
  VM_USER="$TSSHD_USER" SSH_KEY="$TSSHD_KEY" KNOWN_HOSTS="$TSSHD_KNOWN_HOSTS" LCTL="$LPORT_CTL" LTUN="$LPORT_TUN" SSH_PORT_ENV="VM_SSH_PORT=$TSSHD_PORT"
fi

# --- the watcher first (as the run book says), then the job ------------------------------------------------------
render "$HERE/job.sbatch.tmpl" PLATFORM="$PLATFORM" ACCOUNT=acct PARTITION=part TIME=00:30:00 MEM=1G VM_ADDR=127.0.0.1 \
  AGENT_PORT="$AGENT_PORT" DIALHOME_PORT="$DIALHOME_PORT" VM_USER="$VM_USER" SSH_KEY="$SSH_KEY" SSH_KNOWN_HOSTS="$KNOWN_HOSTS" \
  LPORT_CTL="$LCTL" LPORT_TUN="$LTUN" CA_FILE="$CA" TLS_SERVER_NAME="$TLS_NAME" TOKEN_FILE="$WORK/token" \
  INSTANCE_PREFIX="$INSTANCE" INSTANCE_UID="$INSTANCE_UID" KIT_DIR="$HERE" PYTHON="$PYTHON" > "$WORK/job.sbatch"
"$PYTHON" "$HERE/stream_watch.py" --tcpmux "127.0.0.1:$TCPMUX_PORT" --domain "$INSTANCE_UID" --wait-up 600 \
  --duration "$DURATION" --stream-seconds 40 --fault-window 8 --direct-url "http://127.0.0.1:$ENGINE_PORT" \
  --stub-log "$WORK/stub.jsonl" --stub-user "$INSTANCE" --out "$WORK/summary.json" >"$WORK/watch.out" 2>&1 &
WATCH_PID=$!
mkdir -p "$WORK/scratch"
# shellcheck disable=SC2086
setsid env SCRATCH="$WORK/scratch" SLURM_JOB_ID="$JOBID" TRANSPORT="$VARIANT" $SSH_PORT_ENV PATHRATE_S="$PATHRATE_S" \
  PATHRATE_MAX_BPS="$CAP" ENGINE_PORT="$ENGINE_PORT" ENGINE_DOWN_S=8 FREEZE_SECONDS=28 WATCHER_WAIT_S=600 OUTAGE_DEADLINE_S=120 \
  nice -n 10 bash "$WORK/job.sbatch" >"$WORK/job.out" 2>&1 &
JOB_PID=$!
JD="$WORK/scratch/s1-frp/$JOBID-r0"
step "watcher pid $WATCH_PID, job pid $JOB_PID (TRANSPORT=$VARIANT); the job measures the path rate first (4 x ${PATHRATE_S} s, capped at $CAP B/s)"

for _ in $(seq 1 450); do grep -q 'run clock started' "$WORK/watch.out" 2>/dev/null && break; kill -0 "$JOB_PID" 2>/dev/null || break; sleep 1; done
grep -q 'run clock started' "$WORK/watch.out" || { echo "local-smoke: the run clock never started; job.log tail:" >&2; tail -n 8 "$JD/job.log" >&2; exit 1; }
T0="$(date +%s.%N)"
for _ in $(seq 1 30); do grep -q 'watcher attached' "$JD/job.log" 2>/dev/null && break; sleep 0.5; done
grep -q 'watcher attached' "$JD/job.log" || { echo "local-smoke: the job did not see the watcher" >&2; exit 1; }
FRPC_PID="$(sed -n 's/^frpc=\([0-9]*\) .*/\1/p' "$JD/pids")"
step "run clock started; the job is armed (frpc pid $FRPC_PID)"
sleep_until() {  # sleep until T0 + $1 seconds
  local d
  d="$(awk -v t0="$T0" -v now="$(date +%s.%N)" -v at="$1" 'BEGIN { d = t0 + at - now; print (d > 0 ? d : 0) }')"
  sleep "$d"
}
mark() { kill -USR1 "$WATCH_PID"; }

sleep_until 10
# the wiring while it runs: two separate ssh connections (ssh variant), and a CONNECT to another domain gets no route
if [ "$VARIANT" = ssh ]; then
  { awk -v r="$(printf '0100007F:%04X' "$TSSHD_PORT")" '$3 == r && $4 == "01"' /proc/net/tcp | wc -l
    for f in ssh-ctl.pid ssh-tun.pid; do p="$(cut -d' ' -f1 "$JD/$f")"; echo "$p $(cat "/proc/$p/comm" 2>/dev/null) $(tr '\0' '\n' < "/proc/$p/cmdline" 2>/dev/null | grep -cx -- -L)"; done
  } > "$WORK/ssh-check" 2>&1 || true
fi
"$PYTHON" -I - "$TCPMUX_PORT" "$INSTANCE_UID" "5e1f0000-0000-4000-8000-0000000000ff" > "$WORK/domain-check" <<'PY' || true
import socket, sys
def status(dom):
    s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), 5)
    s.settimeout(5)
    s.sendall(("CONNECT %s:80 HTTP/1.1\r\nHost: %s:80\r\n\r\n" % (dom, dom)).encode())
    data = b""
    try:
        while b"\r\n" not in data:
            chunk = s.recv(256)
            if not chunk:
                break
            data += chunk
    except OSError:
        pass
    s.close()
    first = data.split(b"\r\n")[0].decode("ascii", "replace").split()
    return first[1] if len(first) > 1 else "none"
print("%s %s" % (status(sys.argv[2]), status(sys.argv[3])))
PY
step "mid-run checks taken (domain CONNECT: $(cat "$WORK/domain-check"))"

sleep_until 45
step "CALIBRATION (inside stream 2): mark, SIGSTOP frpc for 3 s (the token gap detector must report it)"
mark; kill -STOP "$FRPC_PID"
sleep_until 48
kill -CONT "$FRPC_PID"
sleep_until 55
step "FAULT 1 (engine dies, frpc stays): mark, USR2 to the job (it kills the engine and restarts it after 8 s)"
mark; kill -USR2 "$JOB_PID"
sleep_until 72
step "FAULT 2 (silent loss, no FIN): mark, HUP to the job (it stops frpc for 28 s)"
mark; kill -HUP "$JOB_PID"
sleep_until 108
step "FAULT 3 (frpc killed): mark, USR1 to the job (it kills frpc)"
mark; kill -USR1 "$JOB_PID"

WATCH_RC=0
wait "$WATCH_PID" || WATCH_RC=$?
WATCH_PID=""
step "watcher exited rc=$WATCH_RC"
cat "$WORK/watch.out"
[ -s "$WORK/summary.json" ] || { echo "local-smoke: watcher wrote no summary" >&2; exit 1; }
kill -TERM "$JOB_PID"
JOB_RC=0
wait "$JOB_PID" || JOB_RC=$?
JOB_PID=""
step "job exited rc=$JOB_RC after TERM"
CHILDREN_LEFT=0
while IFS='=' read -r name rest; do
  pid="${rest%% *}"
  [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null && { echo "still running: $name pid $pid"; CHILDREN_LEFT=1; }
done < "$JD/pids"
ARGS=(--summary "$WORK/summary.json" --job-log "$JD/export/job.log" --probe "$JD/export/probe.jsonl")
for f in ctl tun; do [ -f "$JD/export/ssh-$f-counters.json" ] && ARGS+=(--ssh-counters "$JD/export/ssh-$f-counters.json"); done
"$PYTHON" "$HERE/verdict.py" "${ARGS[@]}" > "$WORK/verdict.out" || true

# --- judge the run: a config check, with the numbers shown ----------------------------------------------------
echo
echo "=== local smoke result [$VARIANT]: CONFIG CHECK ONLY, same host, ${DURATION} s. Not spike evidence. ==="
cat "$WORK/verdict.out"
echo
RC=0
"$PYTHON" -I - "$WORK" "$JD" "$VARIANT" "$INSTANCE" "$INSTANCE_UID" "$CAP" "$PATHRATE_S" "$LOGINFAIL_OK" "$CONTROL_OK" "$NEG" \
  "$JOB_RC" "$CHILDREN_LEFT" <<'PY' || RC=$?
import json, os, sys
work, jd, variant, instance, uid, cap, pr_s, loginfail_ok, control_ok, neg, job_rc, children_left = sys.argv[1:13]
cap, pr_s = int(cap), int(pr_s)
summary = json.load(open(work + "/summary.json"))
stub = [json.loads(l) for l in open(work + "/stub.jsonl") if l.strip()]
probe = [json.loads(l) for l in open(jd + "/export/probe.jsonl") if l.strip()]
verdict = open(work + "/verdict.out").read()
mine = [e for e in stub if e.get("user") == instance]
ops = {}
for e in mine:
    ops[e["op"]] = ops.get(e["op"], 0) + 1
results = []

def check(name, ok, detail):
    results.append(bool(ok))
    print("%s  %s  (%s)" % ("PASS" if ok else "FAIL", name, detail))

def line(name):
    return next((p for p in probe if p.get("probe") == name), {})

negs = dict(x.split("=") for x in neg.split())
print("stub log, calls for %s by op: %s" % (instance, json.dumps(ops, sort_keys=True)))
check("frpc survived a failed first login (loginFailExit=false; Envoy up, frps down)", loginfail_ok == "1", "started before frps")
check("control: the same config with loginFailExit=true ends frpc by itself", control_ok == "1", "so the line above is not vacuous")
check("verified wss: a wrong server name does not log in", negs.get("neg-name") == "none", "frpc with serverName wrong-name.invalid")
check("verified wss: a certificate from another CA does not log in", negs.get("neg-ca") == "none", "frpc trusting another CA")
check("control: the right CA and name log in within 8 s", negs.get("pos") == "login", "so the two lines above are not vacuous")
dom = open(work + "/domain-check").read().split()
check("tcpmux routes the Instance UID and refuses another domain", dom[:1] == ["200"] and dom[1:2] != ["200"],
      "CONNECT <uid>: %s, CONNECT <other uid>: %s" % tuple((dom + ["?", "?"])[:2]))
check("step 0: both paths passed a verified TLS exchange via %s" % variant,
      all(line(n).get("ok") is True and line(n).get("far_end") == "token" and line(n).get("via") == variant for n in ("path_ctl", "path_tun")),
      "path_ctl %s, path_tun %s" % (line("path_ctl").get("far_end"), line("path_tun").get("far_end")))
if variant == "ssh":
    rows = open(work + "/ssh-check").read().split("\n")
    est = rows[0].strip()
    procs = [r.split() for r in rows[1:3]]
    check("two separate ssh connections: two ssh processes with one -L each, two established connections to the sshd",
          est == "2" and len(procs) == 2 and all(len(p) == 3 and p[1] == "ssh" and p[2] == "1" for p in procs) and procs[0][0] != procs[1][0],
          "established %s, processes %s" % (est, procs))
    counters = [json.load(open(jd + "/export/ssh-%s-counters.json" % f)) for f in ("ctl", "tun")]
    check("each forward: one attempt, established once, no disconnect (the faults do not touch ssh)",
          all(c["attempts"] == 1 and c["established"] == 1 and c["disconnects"] == 0 for c in counters),
          json.dumps([dict((k, c[k]) for k in ("forward", "attempts", "established", "disconnects")) for c in counters]))
else:
    check("direct: no ssh was started", not os.path.exists(jd + "/ssh-tun-counters.json") and not os.path.exists(jd + "/ssh-probe-ctl.err"), "no ssh files")
rates = [p for p in probe if p.get("probe") == "path_rate"]
pairs = [(r.get("forward"), r.get("direction")) for r in rates]
check("path rate: tunnel up/down and control up/down, the tunnel first", pairs == [("tun", "up"), ("tun", "down"), ("ctl", "up"), ("ctl", "down")], str(pairs))
for r in rates:
    want_by = "receiver-vm" if r.get("direction") == "up" else "receiver-job"
    bps = r.get("bps") or 0
    check("path rate %s %s: ok, %d s asked, >= %d s timed by %s, within 15%% of the %d B/s cap" % (r.get("forward"), r.get("direction"), pr_s, pr_s - 1, want_by, cap),
          r.get("ok") is True and r.get("requested_s") == pr_s and (r.get("duration_s") or 0) >= pr_s - 1 and r.get("timed_by") == want_by
          and r.get("max_bps") == cap and abs(bps - cap) <= 0.15 * cap,
          "%.0f B/s, %s bytes in %s s, sender %s s" % (bps, r.get("bytes"), r.get("duration_s"), r.get("sender_s")))
check("the job exited 0 after TERM and left no child", job_rc == "0" and children_left == "0", "rc %s, children left %s" % (job_rc, children_left))
check("Login reached the stub", ops.get("Login", 0) >= 1, "%d Login" % ops.get("Login", 0))
newproxy = next((e for e in mine if e["op"] == "NewProxy"), {})
check("NewProxy reached the stub as a tcpmux/httpconnect proxy whose domain is the Instance UID",
      newproxy.get("proxy_type") == "tcpmux" and newproxy.get("multiplexer") == "httpconnect" and (newproxy.get("domains") or []) == [uid],
      json.dumps({k: newproxy.get(k) for k in ("proxy", "proxy_type", "multiplexer", "domains")}, sort_keys=True))
check("Ping reaches the stub (application heartbeat is on)", ops.get("Ping", 0) >= 3, "%d Ping" % ops.get("Ping", 0))
# C4a-c read the marks in the run book's order (freeze, engine, kill); the smoke takes a calibration mark first and the
# engine fault before the freeze, so it judges the three faults itself below, and takes from verdict.py only the rest
for cid in ("C1", "C5", "C6"):
    check("verdict %s passes" % cid, ("PASS %s" % cid) in verdict, next((l for l in verdict.splitlines() if (" %s " % cid) in l), "")[:150])
check("verdict C2 fails, as it must on a 120 s run", "FAIL C2" in verdict, "a stream of 40 s is not an hour")
check("verdict flags the capped path rate as not the link rate", "not the link rate" in verdict, "FLAG lines present")

s1 = summary["streams"][0] if summary["streams"] else {}
g = summary["gaps"]
check("first stream completed, no cut, no sequence break", bool(s1.get("completed")) and s1.get("seq_breaks") == 0,
      "%s tokens of %s in %s s, ttfb %s s" % (s1.get("tokens"), s1.get("requested_s"), s1.get("duration_s"), s1.get("ttfb_s")))
in_s1 = [o for o in g["over_threshold"] if o["stream"] == 1]
check("stream 1 has no token gap over 2 s (no fault-window exclusion applied)", not in_s1 and s1.get("tokens") == 40,
      "gaps over 2 s in stream 1: %d" % len(in_s1))
check("no token gap above 2 s anywhere outside fault windows", g["over_threshold_unfaulted"] == 0 and (g["stats_unfaulted_s"].get("max") or 0) <= 2.0,
      "max unfaulted gap %s s, p99 %s s" % (g["stats_unfaulted_s"].get("max"), g["stats_unfaulted_s"].get("p99")))
marks = summary["marks"]
check("four marks were taken (calibration plus three faults)", len(marks) == 4, "%d" % len(marks))
check("every fault was injected from an up path (marks valid)", all(x.get("valid") for x in marks),
      "preconditions %s" % json.dumps([x.get("precondition") for x in marks]))
stalls = [o for o in g["over_threshold"] if o["near_mark"] and 2.5 <= o["gap_s"] <= 5.0]
check("calibration: the gap detector reports the deliberate 3 s stall in stream 2", bool(stalls) and stalls[0]["stream"] == 2,
      "gaps over 2 s near a mark: %s" % json.dumps([o["gap_s"] for o in g["over_threshold"] if o["near_mark"]]))
if len(marks) == 4:
    _, m1, m2, m3 = marks
    check("fault 1, engine killed: /health declared down within 10 s, class no_response",
          (m1.get("probe_declared_down_s") or 99) <= 10 and m1.get("probe_class") == "no_response",
          "declared down after %s s, class %s" % (m1.get("probe_declared_down_s"), m1.get("probe_class")))
    check("fault 1: the path stays up (no CloseProxy, Pings keep arriving), /health recovers after the restart",
          m1.get("stub_close_proxy_s") is None and (m1.get("stub_pings_in_30s_after") or 0) >= 1 and m1.get("probe_recovered_s") is not None,
          "CloseProxy %s, %s Pings in 30 s, recovered after %s s" % (m1.get("stub_close_proxy_s"), m1.get("stub_pings_in_30s_after"), m1.get("probe_recovered_s")))
    check("fault 2, frpc frozen: /health down within 10 s, CloseProxy within the 30 s silent-loss budget",
          (m2.get("probe_declared_down_s") or 99) <= 10 and m2.get("stub_close_proxy_s") is not None and m2["stub_close_proxy_s"] <= 30,
          "down after %s s, CloseProxy after %s s" % (m2.get("probe_declared_down_s"), m2.get("stub_close_proxy_s")))
    check("fault 2: frpc reconnected after the thaw (new Login), /health recovered",
          m2.get("stub_login_after_s") is not None and m2.get("probe_recovered_s") is not None,
          "new Login after %s s, recovered after %s s" % (m2.get("stub_login_after_s"), m2.get("probe_recovered_s")))
    check("fault 3, frpc killed: CloseProxy within 5 s (the FIN crosses %s), /health down within 10 s" % ("ssh and Envoy" if variant == "ssh" else "Envoy"),
          m3.get("stub_close_proxy_s") is not None and m3["stub_close_proxy_s"] <= 5 and (m3.get("probe_declared_down_s") or 99) <= 10,
          "CloseProxy after %s s, down after %s s" % (m3.get("stub_close_proxy_s"), m3.get("probe_declared_down_s")))
print("loopback latency (tunnel minus direct, ms; meaningless as a network figure): %s" % json.dumps(summary.get("added_latency_ms"), sort_keys=True))
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
