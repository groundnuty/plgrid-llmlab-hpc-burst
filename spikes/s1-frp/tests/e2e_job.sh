#!/usr/bin/env bash
# e2e_job.sh -- the kit's real ssh-forward path, end to end, on this machine.
#
# The VM's sshd is a throwaway sshd run as this user (see throwaway-sshd.sh), authorising the README's key line with
# its two ports swapped for test ports. Everything else is real: the VM side (Envoy with a test CA's certificate,
# frps behind it, the plugin stub, the agent stand-in and the sink: path_rate.py serve), the job template run as a
# plain script (two real ssh forwards, the path rate, real frpc from dl/ speaking verified wss), stream_watch.py and
# verdict.py. It follows the run-book: start the VM side, probe, start the watcher, then the job, then the three
# faults with a mark each, each from an 'up' path, then TERM.
#
# This is a test of the kit, not spike evidence: same host, about 4 minutes, no Slurm, a fake engine. It needs
# the pinned frp binaries (bash fetch-frp.sh), Envoy 1.33.2 at ENVOY_BIN (default dl/envoy/envoy-1.33.2, see
# fetch-envoy.sh) and /usr/sbin/sshd.
set -u
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
source "$TESTS_DIR/job_helpers.sh"

e2e_prepare() {   # the throwaway sshd authorising the README key line, ports, token, CA and the rendered job; sets E2E_BIN
  local arch opts
  arch="$(host_frp_arch)"
  E2E_BIN="$(ls -d "$KIT"/dl/linux_"$arch"/frp_*_linux_"$arch" 2>/dev/null | head -n 1)"
  [ -n "$E2E_BIN" ] && [ -x "$E2E_BIN/frps" ] || { echo "run: bash fetch-frp.sh"; return 1; }
  E2E_ENVOY="${ENVOY_BIN:-$KIT/dl/envoy/envoy-1.33.2}"
  bash "$KIT/fetch-envoy.sh" verify "$E2E_ENVOY" >/dev/null || { echo "set ENVOY_BIN to the pinned Envoy 1.33.2 (bash fetch-envoy.sh)"; return 1; }
  mk_dummy_files || return 1
  tsshd_start || return 1
  pick_ports
  FP="$(free_port)"; TCPMUX="$(free_port)"; PLUGIN="$(free_port)"; SINK="$(free_port)"
  opts="$(sed -n 's/^ *\(restrict,port-forwarding,[^ ]*\) ssh-ed25519 .*/\1/p' "$KIT/README.md" | head -n 1 \
    | sed "s/127\.0\.0\.1:29223/127.0.0.1:$T_AGENT_PORT/; s/127\.0\.0\.1:29227/127.0.0.1:$T_DIALHOME_PORT/")"
  expect "the README key line was found, with its two ports swapped" bash -c "case '$opts' in *permitopen=\\\"127.0.0.1:$T_AGENT_PORT\\\",permitopen=\\\"127.0.0.1:$T_DIALHOME_PORT\\\"*) ;; *) exit 1 ;; esac" || return 1
  tsshd_authorize "$opts"
  T_SSH_KEY="$TSSHD_KEY" T_KNOWN_HOSTS="$TSSHD_KNOWN_HOSTS" T_VM_USER="$TSSHD_USER" T_KIT="$KIT"
  T_VM_SSH_PORT="$TSSHD_PORT" T_HOSTKEY_FP="$(fp_of "$TSSHD_DIR/host_key.pub")"   # the VM's own host key, as the run book takes it
  umask 077; head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$W/token"; chmod 600 "$W/token"; T_TOKEN="$W/token"
  render_job "$W/job.sbatch" || return 1
}
e2e_start_vm_side() {   # the run book's A6: stub, frps, sink, agent stand-in and Envoy on the VM's loopback
  python3 "$KIT/render_tmpl.py" "$KIT/frps.toml.tmpl" BIND_ADDR=127.0.0.1 BIND_PORT="$FP" TCPMUX_PORT="$TCPMUX" \
    PROXY_BIND_ADDR=127.0.0.1 TOKEN_FILE="$W/token" PLUGIN_ADDR="127.0.0.1:$PLUGIN" LOG_FILE="$W/frps.log" > "$W/frps.toml" || return 1
  python3 "$KIT/render_tmpl.py" "$KIT/envoy.yaml.tmpl" LISTEN_PORT="$T_DIALHOME_PORT" CERT_FILE="$W/ca/server.crt" \
    KEY_FILE="$W/ca/server.key" FRPS_PORT="$FP" SINK_PORT="$SINK" > "$W/envoy.yaml" || return 1
  bg python3 "$KIT/plugin_stub.py" --listen "127.0.0.1:$PLUGIN" --log "$W/stub.jsonl" >/dev/null 2>&1
  bg "$E2E_BIN/frps" -c "$W/frps.toml" >"$W/frps.out" 2>&1
  bg python3 "$KIT/path_rate.py" serve --listen "127.0.0.1:$SINK" --max-seconds 900 2>/dev/null
  serve_far "$T_AGENT_PORT" 2>/dev/null || { echo "the agent stand-in did not start"; return 1; }
  bg nice -n 10 "$E2E_ENVOY" -c "$W/envoy.yaml" --disable-hot-restart --concurrency 2 --log-level warn >"$W/envoy.log" 2>&1
  wait_for 10 timeout 1 bash -c "exec 3<>/dev/tcp/127.0.0.1/$TCPMUX" 2>/dev/null || { echo "frps did not start"; return 1; }
  wait_for 15 python3 "$KIT/path_rate.py" probe --host 127.0.0.1 --port "$T_DIALHOME_PORT" --tls --ca "$T_CA" --server-name "$T_TLS_NAME" >/dev/null \
    || { echo "Envoy does not answer the verified probe"; tail -3 "$W/envoy.log"; return 1; }
}

test_e2e_the_real_forward_carries_probe_login_stream_and_three_faults() {
  e2e_prepare && e2e_start_vm_side || return 1

  # --- B: step 0 through both real forwards, with the VM side running
  start_job 2001 PROBE_ONLY=1 VM_SSH_PORT="$TSSHD_PORT"; job_rc 60
  expect "probe-only exit 0 (got $RC)" test "$RC" = 0 || return 1
  expect "probe: the sshd presents the VM's own host key" test "$(probe_field "$JD/probe.jsonl" hostkey state)" = '"match"' || return 1
  expect "probe: verified token through the control forward" test "$(probe_field "$JD/probe.jsonl" path_ctl far_end)" = '"token"' || return 1
  expect "probe: verified token through the tunnel forward and Envoy" test "$(probe_field "$JD/probe.jsonl" path_tun far_end)" = '"token"' || return 1

  # --- C: the watcher, then the job. The path rate is 3 s at a 300,000 B/s cap: under 6 MiB / 10 s on purpose, so
  # the verdict's flag must fire, and too short for C6, which must fail.
  bg python3 "$KIT/stream_watch.py" --tcpmux "127.0.0.1:$TCPMUX" --domain "$T_UID" --wait-up 180 --duration 220 \
    --stream-seconds 18 --stub-log "$W/stub.jsonl" --stub-user s1t --out "$W/summary.json" >"$W/watch.out" 2>&1
  WATCH="$BG_LAST"
  start_job 2002 VM_SSH_PORT="$TSSHD_PORT" WATCHER_WAIT_S=120 FREEZE_SECONDS=40 ENGINE_DOWN_S=5 OUTAGE_DEADLINE_S=120 \
    PATHRATE_S=3 PATHRATE_MAX_BPS=300000
  expect "the run clock starts (the watcher saw a good probe through frps, Envoy, frpc and the forward)" wait_for 120 file_has "$W/watch.out" "run clock started" || { tail -5 "$JD/job.log"; return 1; }
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
  expect "each forward: one established session, no disconnect (faults do not touch ssh)" bash -c "grep -q '\"established\":1,\"disconnects\":0' '$JD/ssh-ctl-counters.json' && grep -q '\"established\":1,\"disconnects\":0' '$JD/ssh-tun-counters.json'" || return 1
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
    --ssh-counters "$JD/export/ssh-ctl-counters.json" --ssh-counters "$JD/export/ssh-tun-counters.json" > "$W/verdict.out"
  cat "$W/verdict.out"
  for c in C1 C4a C4b C4c C5; do expect "verdict $c passes" grep -q "^PASS $c" "$W/verdict.out" || return 1; done
  expect "verdict C2 fails: a 3-minute run is not an hour" grep -q "^FAIL C2" "$W/verdict.out" || return 1
  expect "verdict C6 fails: a 3 s transfer is not a sustained rate" bash -c "grep '^FAIL C6' '$W/verdict.out' | grep -q 'at least 30 s'" || return 1
  expect "the measured 300,000 B/s is flagged as under 6 MiB / 10 s" bash -c "grep '^FLAG' '$W/verdict.out' | grep '^FLAG     tun up:' | grep -q 'under 6 MiB / 10 s'" || return 1
  expect "and as capped, not the link rate" grep -q "not the link rate" "$W/verdict.out"
}

# Three streams at once through one frpc and the real ssh forward, with no faults. The watcher's own duration ends the
# run. The summary is checked, not OVERALL: C2 fails on length by design (20 s is not an hour). Three streams run one
# after another would also each "complete", so the checks that tell them apart are the overlap in time, on the watcher's
# side and on the engine's.
test_e2e_three_concurrent_streams_complete_through_one_frpc_and_the_real_forward() {
  e2e_prepare && e2e_start_vm_side || return 1
  bg python3 "$KIT/stream_watch.py" --tcpmux "127.0.0.1:$TCPMUX" --domain "$T_UID" --wait-up 120 --duration 24 \
    --streams 3 --stream-seconds 20 --stub-log "$W/stub.jsonl" --stub-user s1t --out "$W/summary.json" >"$W/watch.out" 2>&1
  WATCH="$BG_LAST"
  start_job 2003 VM_SSH_PORT="$TSSHD_PORT" WATCHER_WAIT_S=90 OUTAGE_DEADLINE_S=120
  expect "the run clock starts (a good probe through frps, Envoy, frpc and the forward)" wait_for 90 file_has "$W/watch.out" "run clock started" || { tail -5 "$JD/job.log"; return 1; }
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
    --ssh-counters "$JD/export/ssh-ctl-counters.json" --ssh-counters "$JD/export/ssh-tun-counters.json" > "$W/verdict.out"
  cat "$W/verdict.out"
  expect "verdict C1 passes (frpc logged in through the forward)" grep -q "^PASS C1" "$W/verdict.out" || return 1
  expect "verdict C2 fails, for the length of all three streams and for nothing else" \
    bash -c "grep '^FAIL C2' '$W/verdict.out' | grep -q '3 of 3 stream' && ! grep '^FAIL C2' '$W/verdict.out' | grep -q 'cut before'"
}

ALL="$(declare -F | sed -n 's/^declare -f test_//p' | tr '\n' ' ')"
[ "$#" -gt 0 ] && ALL="$*"
for n in $ALL; do run_test "$n"; done
