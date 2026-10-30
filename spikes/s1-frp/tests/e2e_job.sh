#!/usr/bin/env bash
# e2e_job.sh -- the kit's real ssh-forward path, end to end, on this machine.
#
# The VM's sshd is a throwaway sshd run as this user (see throwaway-sshd.sh), authorising the README's key line.
# Everything else is real: probe_responder.py, the job template run as a plain script (real ssh, real frpc from
# dl/), frps, the plugin stub, stream_watch.py and verdict.py. It follows the run-book: probe with the responder,
# stop the responder, start frps and the watcher, then the job, then the three faults with a mark each, each from
# an 'up' path, then TERM.
#
# This is a test of the kit, not spike evidence: same host, about 4 minutes, no Slurm, a fake engine. It needs
# the pinned frp binaries (bash fetch-frp.sh) and /usr/sbin/sshd.
set -u
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
source "$TESTS_DIR/job_helpers.sh"

e2e_prepare() {   # the throwaway sshd authorising the README key line, ports, token and the rendered job; sets E2E_BIN FP TCPMUX PLUGIN
  local arch opts
  arch="$(host_frp_arch)"
  E2E_BIN="$(ls -d "$KIT"/dl/linux_"$arch"/frp_*_linux_"$arch" 2>/dev/null | head -n 1)"
  [ -n "$E2E_BIN" ] && [ -x "$E2E_BIN/frps" ] || { echo "run: bash fetch-frp.sh"; return 1; }
  mk_dummy_files
  tsshd_start || return 1
  FP="$(free_port)"; T_LPORT="$(free_port)"; T_FRPS_PORT="$FP"
  TCPMUX="$(free_port)"; PLUGIN="$(free_port)"
  opts="$(sed -n 's/^ *\(restrict,port-forwarding,[^ ]*\) ssh-ed25519 .*/\1/p' "$KIT/README.md" | head -n 1 | sed "s/127\.0\.0\.1:7000/127.0.0.1:$FP/")"
  expect "the README key line was found" test -n "$opts" || return 1
  tsshd_authorize "$opts"
  T_SSH_KEY="$TSSHD_KEY" T_KNOWN_HOSTS="$TSSHD_KNOWN_HOSTS" T_VM_USER="$TSSHD_USER" T_KIT="$KIT"
  umask 077; head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$W/token"; chmod 600 "$W/token"; T_TOKEN="$W/token"
  render_job "$W/job.sbatch" || return 1
}
e2e_start_frps() {   # the plugin stub and frps on the VM's loopback, listening on the tcpmux port
  python3 "$KIT/render_tmpl.py" "$KIT/frps.toml.tmpl" BIND_ADDR=127.0.0.1 BIND_PORT="$FP" TCPMUX_PORT="$TCPMUX" \
    PROXY_BIND_ADDR=127.0.0.1 TOKEN_FILE="$W/token" PLUGIN_ADDR="127.0.0.1:$PLUGIN" LOG_FILE="$W/frps.log" > "$W/frps.toml" || return 1
  bg python3 "$KIT/plugin_stub.py" --listen "127.0.0.1:$PLUGIN" --log "$W/stub.jsonl" >/dev/null 2>&1
  bg "$E2E_BIN/frps" -c "$W/frps.toml" >"$W/frps.out" 2>&1
  wait_for 10 timeout 1 bash -c "exec 3<>/dev/tcp/127.0.0.1/$TCPMUX" 2>/dev/null || { echo "frps did not start"; return 1; }
}

test_e2e_the_real_forward_carries_probe_login_stream_and_three_faults() {
  e2e_prepare || return 1

  # --- B: step 0 with the responder on the VM's loopback, at the port frps will use
  bg python3 "$KIT/probe_responder.py" --port "$FP" --max-seconds 300 2>/dev/null; RESP="$BG_LAST"
  sleep 0.5
  start_job 2001 PROBE_ONLY=1 VM_SSH_PORT="$TSSHD_PORT"; job_rc 40
  expect "probe-only exit 0 (got $RC)" test "$RC" = 0 || return 1
  expect "probe: token through the real forward" test "$(probe_field "$JD/probe.jsonl" forward_setup far_end)" = '"token"' || return 1
  kill "$RESP"; { wait "$RESP"; } 2>/dev/null

  # --- C: frps and the stub, then the watcher, then the job
  e2e_start_frps || return 1
  bg python3 "$KIT/stream_watch.py" --tcpmux "127.0.0.1:$TCPMUX" --domain s1t --wait-up 120 --duration 220 \
    --stream-seconds 18 --stub-log "$W/stub.jsonl" --out "$W/summary.json" >"$W/watch.out" 2>&1
  WATCH="$BG_LAST"
  start_job 2002 VM_SSH_PORT="$TSSHD_PORT" WATCHER_WAIT_S=90 FREEZE_SECONDS=40 ENGINE_DOWN_S=5 OUTAGE_DEADLINE_S=120
  expect "the run clock starts (the watcher saw a good probe through frps, frpc and the forward)" wait_for 90 file_has "$W/watch.out" "run clock started" || { tail -5 "$JD/job.log"; return 1; }
  expect "the job saw the watcher" wait_for 30 file_has "$JD/job.log" "watcher attached" || return 1

  ups() { grep -c 'path UP again' "$W/watch.out"; }
  fault() {   # SIGNAL: a mark, then the fault; waits until the path is up again
    local n; n="$(ups)"
    expect "the path is up before the fault" bash -c "! grep 'probe: path' '$W/watch.out' | tail -n 1 | grep -q 'path DOWN'" || { tail -n 6 "$W/watch.out"; return 1; }
    kill -USR1 "$WATCH"; kill -"$1" "$JOB_PID"
    [ "$1" = USR1 ] && return 0
    expect "the path comes back after SIG$1" wait_for 120 bash -c "[ \"\$(grep -c 'path UP again' '$W/watch.out')\" -gt $n ]" || return 1
    sleep 14       # the real run waits minutes; here at least one Ping (every 10 s) must fall before the next mark
  }
  sleep 24                                     # one full stream before the first mark
  fault HUP  || return 1                       # frpc frozen for 40 s
  fault USR2 || return 1                       # engine killed, restarted on its port after 5 s
  fault USR1 || return 1                       # frpc killed, from an up path
  sleep 14
  kill -TERM "$WATCH"; wait "$WATCH"; wrc=$?
  expect "the watcher exited 0 on TERM and wrote its summary (got $wrc)" test "$wrc" = 0 || return 1
  wait_for 20 test -s "$W/summary.json" || { echo "no summary"; tail -5 "$W/watch.out"; return 1; }
  kill -TERM "$JOB_PID"; job_rc 40
  expect "the job exits 0 on TERM (got $RC)" test "$RC" = 0 || return 1
  expect "no child left" no_children_left || return 1

  # --- what the files say
  expect "frpc logged in through the forward" test "$(probe_field "$JD/probe.jsonl" frpc_login ok)" = true || return 1
  expect "one established session, no disconnect (faults do not touch ssh)" bash -c "grep -q '\"established\":1,\"disconnects\":0' '$JD/ssh-counters.json'" || return 1
  expect "export/ was written" test -s "$JD/export/job.log" || return 1
  python3 - "$W/summary.json" <<'PY' || return 1
import json, sys
s = json.load(open(sys.argv[1]))
m = s["marks"]
assert len(m) == 3, "marks: %d" % len(m)
assert all(x["valid"] for x in m), "marks valid: %r" % [(x["precondition"], x["valid"]) for x in m]
assert s["streams"][0]["completed"], s["streams"][0]
assert m[0]["stub_close_proxy_s"] is not None and m[0]["stub_close_proxy_s"] <= 30, m[0]
assert m[1]["probe_class"] == "no_response" and m[1]["stub_close_proxy_s"] is None and m[1]["stub_pings_in_30s_after"] >= 1, m[1]
assert m[2]["stub_close_proxy_s"] is not None and m[2]["stub_close_proxy_s"] <= 5, m[2]
assert m[2]["probe_declared_down_s"] <= 10, m[2]
print("marks: " + json.dumps([(x["precondition"], x.get("probe_declared_down_s"), x.get("stub_close_proxy_s")) for x in m]))
PY
  python3 "$KIT/verdict.py" --summary "$W/summary.json" --job-log "$JD/export/job.log" --probe "$JD/export/probe.jsonl" \
    --ssh-counters "$JD/export/ssh-counters.json" > "$W/verdict.out"
  cat "$W/verdict.out"
  for c in C1 C4a C4b C4c C5; do expect "verdict $c passes" grep -q "^PASS $c" "$W/verdict.out" || return 1; done
  expect "verdict C2 fails: a 3-minute run is not an hour" grep -q "^FAIL C2" "$W/verdict.out"
}

# Three streams at once through one frpc and the real ssh forward, with no faults. The watcher's own duration ends the
# run. The summary is checked, not OVERALL: C2 fails on length by design (20 s is not an hour). Three streams run one
# after another would also each "complete", so the checks that tell them apart are the overlap in time, on the watcher's
# side and on the engine's.
test_e2e_three_concurrent_streams_complete_through_one_frpc_and_the_real_forward() {
  e2e_prepare || return 1
  e2e_start_frps || return 1
  bg python3 "$KIT/stream_watch.py" --tcpmux "127.0.0.1:$TCPMUX" --domain s1t --wait-up 120 --duration 24 \
    --streams 3 --stream-seconds 20 --stub-log "$W/stub.jsonl" --out "$W/summary.json" >"$W/watch.out" 2>&1
  WATCH="$BG_LAST"
  start_job 2003 VM_SSH_PORT="$TSSHD_PORT" WATCHER_WAIT_S=90 OUTAGE_DEADLINE_S=120
  expect "the run clock starts (a good probe through frps, frpc and the forward)" wait_for 90 file_has "$W/watch.out" "run clock started" || { tail -5 "$JD/job.log"; return 1; }
  expect "the job saw the watcher" wait_for 30 file_has "$JD/job.log" "watcher attached" || return 1
  expect "the watcher wrote its summary by itself, at the end of its duration" wait_for 80 test -s "$W/summary.json" || { tail -5 "$W/watch.out"; return 1; }
  wait "$WATCH"; wrc=$?
  expect "the watcher exited 0 (got $wrc)" test "$wrc" = 0 || return 1
  kill -TERM "$JOB_PID"; job_rc 40
  expect "the job exits 0 on TERM (got $RC)" test "$RC" = 0 || return 1
  expect "no child left" no_children_left || return 1

  python3 - "$W/summary.json" <<'PY' || return 1
import json, sys
s = json.load(open(sys.argv[1]))
assert s["streams_requested"] == 3, s["streams_requested"]
recs = s["streams"]
assert sorted(r["lane"] for r in recs) == [1, 2, 3], [(r["lane"], r["id"]) for r in recs]
for r in recs:
    assert r["completed"] and not r["interrupted"], r
    assert r["tokens"] == r["requested_s"] == 20, r            # every token asked for arrived
assert s["cuts"] == [], s["cuts"]
assert all(p["first_stream_completed"] and p["cuts"] == 0 for p in s["per_stream"]), s["per_stream"]
assert s["overall"]["streams_whose_first_stream_completed"] == 3 and s["overall"]["cuts"] == 0, s["overall"]
starts = [r["end_epoch"] - r["duration_s"] for r in recs]
ends = [r["end_epoch"] for r in recs]
assert max(starts) < min(ends) - 5, "the streams did not overlap: starts %r ends %r" % (starts, ends)
print("lanes: " + json.dumps([(p["lane"], p["tokens"], p["max_gap_s"]) for p in s["per_stream"]]))
PY
  # the engine's side: all three streams had started before any of them ended (streamer.log is private to the job dir)
  expect "the engine started three streams" test "$(grep -c 'stream start' "$JD/streamer.log")" = 3 || return 1
  expect "the engine had started all three before the first one ended" \
    awk '/stream start/ {s++} /stream end/ {if (s < 3) bad = 1} END {exit bad}' "$JD/streamer.log" || return 1
  python3 "$KIT/verdict.py" --summary "$W/summary.json" --job-log "$JD/export/job.log" --probe "$JD/export/probe.jsonl" \
    --ssh-counters "$JD/export/ssh-counters.json" > "$W/verdict.out"
  cat "$W/verdict.out"
  expect "verdict C1 passes (frpc logged in through the forward)" grep -q "^PASS C1" "$W/verdict.out" || return 1
  expect "verdict C2 fails, for the length of all three streams and for nothing else" \
    bash -c "grep '^FAIL C2' '$W/verdict.out' | grep -q '3 of 3 stream' && ! grep '^FAIL C2' '$W/verdict.out' | grep -q 'cut before'"
}

ALL="$(declare -F | sed -n 's/^declare -f test_//p' | tr '\n' ' ')"
[ "$#" -gt 0 ] && ALL="$*"
for n in $ALL; do run_test "$n"; done
