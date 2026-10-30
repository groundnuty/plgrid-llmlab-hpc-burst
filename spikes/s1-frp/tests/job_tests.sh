#!/usr/bin/env bash
# job_tests.sh -- tests of job.sbatch.tmpl and the pieces around it.
#   job_tests.sh              every test
#   job_tests.sh NAME...      the named tests (without the test_ prefix)
# S1_KIT_DIR selects the kit under test (default: this one). Results: one PASS or FAIL line each.
# Expected values come from the review (s1kit2) and the decisions in the task brief, not from the code
# under test; each test says which case it stands for.
set -u
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

source "$TESTS_DIR/job_helpers.sh"

# ---- B1: the probe gate --------------------------------------------------------------------------------
test_b1_probe_passes_only_with_the_token() {
  sshd_env || return 1
  bg python3 "$KIT/probe_responder.py" --port "$FP" --max-seconds 120 2>/dev/null; sleep 0.5
  probe_only_run 1001; rc=$RC
  expect "exit 0 with the responder running (got $rc)" test "$rc" = 0 || return 1
  expect "forward_setup ok" test "$(probe_field "$JD/probe.jsonl" forward_setup ok)" = true || return 1
  expect "far_end token" test "$(probe_field "$JD/probe.jsonl" forward_setup far_end)" = '"token"' || return 1
  expect "transport_path_ok true" test "$(probe_field "$JD/probe.jsonl" summary transport_path_ok)" = true
}
test_b1_probe_fails_when_nothing_listens_on_the_far_port() {   # review B1: a closed far end counted as OK
  sshd_env || return 1
  probe_only_run 1002; rc=$RC
  expect "exit 3, not 0 (got $rc)" test "$rc" = 3 || return 1
  expect "forward_setup not ok" test "$(probe_field "$JD/probe.jsonl" forward_setup ok)" = false || return 1
  expect "far_end closed" test "$(probe_field "$JD/probe.jsonl" forward_setup far_end)" = '"closed"' || return 1
  expect "transport_path_ok false" test "$(probe_field "$JD/probe.jsonl" summary transport_path_ok)" = false
}
test_b1_probe_fails_when_the_far_end_is_silent() {   # a listener that accepts but is not the responder
  sshd_env || return 1
  start_far silent "$FP" || return 1
  probe_only_run 1003; rc=$RC
  expect "exit 3 (got $rc)" test "$rc" = 3 || return 1
  expect "far_end held" test "$(probe_field "$JD/probe.jsonl" forward_setup far_end)" = '"held"' || return 1
  expect "transport_path_ok false" test "$(probe_field "$JD/probe.jsonl" summary transport_path_ok)" = false
}
test_b1_probe_fails_on_a_wrong_token() {
  sshd_env || return 1
  start_far wrong "$FP" || return 1
  probe_only_run 1004; rc=$RC
  expect "exit 3 (got $rc)" test "$rc" = 3 || return 1
  expect "far_end data" test "$(probe_field "$JD/probe.jsonl" forward_setup far_end)" = '"data"' || return 1
}
test_b1_probe_fails_when_the_key_may_not_forward_to_the_port() {   # review B1: a denied permitopen
  PERMIT="$(free_port)" sshd_env || return 1
  bg python3 "$KIT/probe_responder.py" --port "$FP" --max-seconds 120 2>/dev/null; sleep 0.5
  probe_only_run 1005; rc=$RC
  expect "exit 3 although the responder runs (got $rc)" test "$rc" = 3 || return 1
  expect "transport_path_ok false" test "$(probe_field "$JD/probe.jsonl" summary transport_path_ok)" = false
}
test_b1_responder_and_job_share_the_token() {
  local tok
  tok="$(python3 -c 'import importlib.util,sys; s=importlib.util.spec_from_file_location("r", sys.argv[1]); m=importlib.util.module_from_spec(s); s.loader.exec_module(m); print(m.TOKEN.decode().strip())' "$KIT/probe_responder.py")"
  expect "the job holds the responder's token ($tok)" grep -q "^PROBE_TOKEN=\"$tok\"" "$KIT/job.sbatch.tmpl"
}
test_b1_full_run_without_a_login_exits_3_and_cleans_up() {   # the gate on frpc's own login line
  fake_env hold || return 1
  start_job 1010 FAKE_FRPC_LOGIN=never FRPC_LOGIN_WAIT_S=4 VM_SSH_PORT="$(free_port)"
  job_rc 40; rc=$RC
  expect "exit 3 (got $rc)" test "$rc" = 3 || return 1
  expect "frpc_login ok false recorded" test "$(probe_field "$JD/probe.jsonl" frpc_login ok)" = false || return 1
  expect "no child left" no_children_left
}
test_b1_full_run_far_end_closed_exits_3() {   # step 0 of a full run: a far end that closes is not frps
  fake_env closed || return 1
  start_job 1011 VM_SSH_PORT="$(free_port)"
  job_rc 40; rc=$RC
  expect "exit 3 (got $rc)" test "$rc" = 3 || return 1
  expect "forward_setup not ok" test "$(probe_field "$JD/probe.jsonl" forward_setup ok)" = false
}

# ---- B3: the engine restarts on its port; freeze and thaw ----------------------------------------------
full_fake_up() {   # JOBID [ENV...] PLAN...: a fake full run up to the armed main loop
  local jid="$1"; shift
  start_job "$jid" VM_SSH_PORT="$(free_port)" WATCHER_WAIT_S=60 "$@"
  wait_for 30 file_has "$JD/job.log" "frpc: login to server success" || { echo "no login in log"; tail -5 "$JD/job.log"; return 1; }
  attach_watcher || return 1
  wait_for 10 file_has "$JD/job.log" "watcher attached" || { echo "no attach"; return 1; }
  sleep 1.5
}
test_b3_engine_restarts_on_its_original_port() {   # review B3
  fake_env hold || return 1
  full_fake_up 1020 ENGINE_DOWN_S=3 || return 1
  port="$(cat "$JD/streamer.port")"
  kill -USR2 "$JOB_PID"
  wait_for 10 file_has "$JD/job.log" "FAULT usr2" || return 1
  expect "the engine is down" wait_for 5 bash -c "! timeout 1 bash -c 'exec 3<>/dev/tcp/127.0.0.1/$port' 2>/dev/null" || return 1
  expect "restarted on the same port" wait_for 15 file_has "$JD/job.log" "engine restarted on 127.0.0.1:$port and listening" || return 1
  body="$(python3 -c 'import sys,urllib.request; print(urllib.request.urlopen("http://127.0.0.1:%s/health" % sys.argv[1], timeout=5).read().decode())' "$port")"
  expect "health answers the exact body after the restart (got $body)" test "$body" = '{"status": "ok"}' || return 1
  expect "streamer.port unchanged" test "$(cat "$JD/streamer.port")" = "$port"
}
test_b3_freeze_stops_frpc_then_thaws_it() {
  fake_env hold || return 1
  full_fake_up 1021 FREEZE_SECONDS=4 || return 1
  frpc="$(sed -n 's/^frpc=\([0-9]*\) .*/\1/p' "$JD/pids")"
  kill -HUP "$JOB_PID"
  expect "frpc stopped (state T)" wait_for 5 bash -c "[ \"\$(awk '{print \$3}' /proc/$frpc/stat)\" = T ]" || return 1
  expect "frpc continues after the freeze" wait_for 12 bash -c "[ \"\$(awk '{print \$3}' /proc/$frpc/stat)\" != T ]"
}
test_b3_kill_frpc_clears_its_record() {
  fake_env hold || return 1
  full_fake_up 1022 || return 1
  frpc="$(sed -n 's/^frpc=\([0-9]*\) .*/\1/p' "$JD/pids")"
  kill -USR1 "$JOB_PID"
  expect "frpc killed" wait_for 5 bash -c "! kill -0 $frpc 2>/dev/null" || return 1
  expect "reported once and the record cleared" wait_for 6 file_has "$JD/job.log" "FRPC (pid $frpc) is no longer running"
}

# ---- B4: permanent ssh failures, deadlines, counters -----------------------------------------------------
test_b4_classify_samples() {   # the stderr phrases are those ssh and sshd print (seen on the throwaway sshd, or in OpenSSH sources)
  eval "$(sed -n '/^# >>> classify/,/^# <<< classify/p' "$KIT/job.sbatch.tmpl")" 2>/dev/null
  type classify_ssh_stderr >/dev/null 2>&1 || { echo "no classify block in the template"; return 1; }
  local bad=0 f="$W/err"
  chk() { printf '%s\n' "$1" > "$f"; got="$(classify_ssh_stderr "$f")"; [ "$got" = "$2" ] || { echo "'$1' -> $got, want $2"; bad=1; }; }
  chk "nobody@host: Permission denied (publickey)." auth
  chk "Too many authentication failures" auth
  chk "Host key verification failed." hostkey
  chk "@@@ WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED! @@@" hostkey
  chk "No ED25519 host key is known for [h]:22 and you have requested strict checking." hostkey
  chk "Could not request local forwarding." forwarding
  chk "Error: remote port forwarding failed for listen port 41931" forwarding
  chk "bind [127.0.0.1]:4000: Address already in use" forwarding
  chk "ssh: connect to host h port 22: Connection timed out" transient
  chk "ssh: connect to host h port 22: Connection refused" transient
  chk "Connection reset by peer" transient
  chk "kex_exchange_identification: Connection closed by remote host" transient
  chk "ssh: Could not resolve hostname h: Name or service not known" transient
  chk "Timeout, server h not responding." transient
  chk "Killed by signal 15." transient
  : > "$f"; got="$(classify_ssh_stderr "$f")"; [ "$got" = transient ] || { echo "empty -> $got"; bad=1; }
  return "$bad"
}
sshd_full_up() {   # JOBID: real sshd, a silent far end (frps stand-in), fake frpc, watcher simulated
  sshd_env || return 1
  start_far silent "$FP" || return 1
  start_job "$1" VM_SSH_PORT="$TSSHD_PORT" WATCHER_WAIT_S=60 OUTAGE_DEADLINE_S=60
  wait_for 40 file_has "$JD/job.log" "frpc: login to server success" || { echo "no login"; tail -8 "$JD/job.log"; return 1; }
  attach_watcher || return 1
  wait_for 10 file_has "$JD/job.log" "watcher attached" || return 1
  expect "established" wait_for 5 file_has "$JD/ssh-counters.json" '"established":1' || return 1
}
kill_current_ssh() { kill "$(cut -d' ' -f1 "$JD/ssh.pid")"; }
test_b4_a_revoked_key_stops_the_job_with_exit_4() {   # review B4: auth failure after a good probe
  sshd_full_up 1030 || return 1
  ssh-keygen -q -t ed25519 -N '' -f "$W/other" && cp "$W/other.pub" "$TSSHD_DIR/authorized_keys"
  kill_current_ssh
  job_rc 40; rc=$RC
  expect "exit 4 (got $rc)" test "$rc" = 4 || return 1
  expect "transport.failed says auth" test "$(cat "$JD/transport.failed")" = auth || return 1
  expect "the log names the failure" file_has "$JD/job.log" "RESULT: TRANSPORT FAILED (auth)" || return 1
  expect "no child left" no_children_left
}
test_b4_a_changed_host_key_stops_the_job_with_exit_4() {
  sshd_full_up 1031 || return 1
  ssh-keygen -q -t ed25519 -N '' -f "$W/otherhost"
  printf '[127.0.0.1]:%s %s\n' "$TSSHD_PORT" "$(cut -d' ' -f1,2 "$W/otherhost.pub")" > "$TSSHD_KNOWN_HOSTS"
  kill_current_ssh
  job_rc 40; rc=$RC
  expect "exit 4 (got $rc)" test "$rc" = 4 || return 1
  expect "transport.failed says hostkey" test "$(cat "$JD/transport.failed")" = hostkey
}
test_b4_a_forwarding_failure_stops_the_job_with_exit_4() {
  fake_env hold "hold_for:3:255:bind [127.0.0.1]:4000: Address already in use" "fail:255:Could not request local forwarding." || return 1
  start_job 1032 VM_SSH_PORT="$(free_port)" WATCHER_WAIT_S=60
  job_rc 60; rc=$RC
  expect "exit 4 (got $rc)" test "$rc" = 4 || return 1
  expect "transport.failed says forwarding" test "$(cat "$JD/transport.failed")" = forwarding
}
test_b4_an_outage_past_the_deadline_stops_the_job_with_exit_4() {
  fake_env hold "hold_for:3:255:Connection reset by peer" "fail:255:ssh: connect to host h port 22: Connection timed out" || return 1
  start_job 1033 VM_SSH_PORT="$(free_port)" WATCHER_WAIT_S=60 OUTAGE_DEADLINE_S=4
  job_rc 60; rc=$RC
  expect "exit 4 (got $rc)" test "$rc" = 4 || return 1
  expect "class outage_deadline" test "$(cat "$JD/transport.failed")" = outage_deadline || return 1
  expect "no child left" no_children_left
}
test_b4_a_startup_failure_is_fatal_not_a_warning() {
  fake_env hold "fail:255:ssh: connect to host h port 22: Connection timed out" || return 1
  start_job 1034 VM_SSH_PORT="$(free_port)" FWD_STARTUP_WAIT_S=3 OUTAGE_DEADLINE_S=60
  job_rc 60; rc=$RC
  expect "exit 4 (got $rc)" test "$rc" = 4 || return 1
  expect "class startup_timeout" test "$(cat "$JD/transport.failed")" = startup_timeout || return 1
  expect "frpc never started" bash -c "! grep -q 'frpc pid=' '$JD/job.log'"
}
test_b4_attempts_established_disconnects_recoveries_are_counted_apart() {   # nit
  fake_env hold "hold_for:2:255:Connection reset by peer" "fail:255:Connection timed out" hold || return 1
  start_job 1035 VM_SSH_PORT="$(free_port)" WATCHER_WAIT_S=60 OUTAGE_DEADLINE_S=60
  wait_for 40 file_has "$JD/ssh-counters.json" '"recoveries":1' || { echo "counters: $(cat "$JD/ssh-counters.json")"; return 1; }
  kill -TERM "$JOB_PID"; job_rc 20 >/dev/null
  python3 - "$JD/ssh-counters.json" <<'PY' || return 1
import json, sys
c = json.load(open(sys.argv[1]))
want = {"attempts": 3, "established": 2, "disconnects": 1, "recoveries": 1}
got = dict((k, c[k]) for k in want)
assert got == want, "counters %r, want %r" % (got, want)
PY
}
test_b4_exit_lines_hold_only_allowlisted_fields() {   # B5
  fake_env hold "hold_for:2:255:Connection reset by peer" hold || return 1
  FAKE_SSH_LEAK="Permission denied for user someone from an address" start_job 1036 VM_SSH_PORT="$(free_port)" WATCHER_WAIT_S=60 OUTAGE_DEADLINE_S=60
  wait_for 30 test -s "$JD/ssh-exits.jsonl" || return 1
  python3 - "$JD/ssh-exits.jsonl" <<'PY'
import json, sys
allowed = {"ts", "attempt", "rc", "class", "established", "up_s", "restart_in_s"}
for line in open(sys.argv[1]):
    keys = set(json.loads(line))
    assert keys == allowed, "fields %r, want exactly %r" % (sorted(keys), sorted(allowed))
PY
}

# ---- B5: redaction -----------------------------------------------------------------------------------------
mk_secret() { printf '%s' "$1" ; }
test_b5_redaction_removes_addresses_users_and_paths() {
  # the values are built at run time so that no address or name is written in this file
  local ip4 ip6 ll6 full6 vmu key kh out
  ip4="$(printf '%s.%s.%s.%s' 203 0 113 77)"; ip6="$(printf '%s:%s::%s' 2001 db8 77)"
  ll6="$(printf 'fe%s::1%%eth0' 80)"; full6="$(printf '%s:%s:%s:%s:%s:%s:%s:%s' 2001 0db8 85a3 0000 0000 8a2e 0370 7334)"
  vmu="vmacct$$x"; key="/data/priv/k$$/id_s1"; kh="/data/priv/k$$/kh_s1"
  mk_dummy_files
  T_VM_USER="$vmu" T_SSH_KEY="$key" T_KNOWN_HOSTS="$kh" T_VM_ADDR="vmhost$$.$(printf '%s' inval)id"
  render_job "$W/job.sbatch" || return 1
  eval "$(sed -n '/^# >>> redact/,/^# <<< redact/p' "$W/job.sbatch")" 2>/dev/null
  type build_redact >/dev/null 2>&1 || { echo "no redact block in the template"; return 1; }
  VM_ADDR="$T_VM_ADDR" VM_USER="$vmu" SSH_KEY="$key" SSH_KNOWN_HOSTS="$kh" TOKEN_FILE=/nonexistent JOB_DIR="$W/jd" KIT_DIR="$W/kit" \
    RUN_ROOT="$W/scratchroot" LOCAL_USER="localacct$$" HOME="$W/home" NODE_NAME="node$$x" JOB_ID=98765432
  REDACT_SED="$W/r.sed"
  build_redact || return 1
  out="$(printf '%s\n' \
    "connect to host $T_VM_ADDR port 22: Connection timed out" \
    "debug: connect $ip4 port 22 then $ip6 and [::1]:22 and $ll6 and $full6" \
    "Warning: identity file $key not accessible; $kh; user $vmu@host; local localacct$$; node node$$x; job 98765432" \
    "ssh-forward: local end 127.0.0.1:44571 at 2026-10-30T12:00:07Z" | redact)"
  local bad=0 s
  for s in "$ip4" "${ip6%%:*}" "db8" "${ll6%%::*}" "8a2e" "$vmu" "$key" "$kh" "vmhost$$" "localacct$$" "node$$x" "98765432"; do
    case "$out" in *"$s"*) echo "still in the output: $s"; bad=1 ;; esac
  done
  case "$out" in *127.0.0.1:44571*) ;; *) echo "loopback was not kept"; bad=1 ;; esac
  case "$out" in *2026-10-30T12:00:07Z*) ;; *) echo "the timestamp was damaged"; bad=1 ;; esac
  return "$bad"
}
test_b5_exported_files_carry_none_of_the_identifying_strings() {   # review B5: collected logs bypassed redaction
  local ip4 ip6 vmu vmh
  ip4="$(printf '%s.%s.%s.%s' 203 0 113 77)"; ip6="$(printf '%s:%s::%s' 2001 db8 77)"
  vmu="vmacct$$y"; vmh="vmhost$$y.$(printf '%s' inval)id"
  fake_env hold "hold_for:2:255:Connection reset by peer" hold || return 1
  T_VM_USER="$vmu" T_VM_ADDR="$vmh"; render_job "$W/job.sbatch" || return 1
  FAKE_SSH_LEAK="ssh: connect to host $vmh port 22 ($ip4 $ip6) as $vmu key $T_SSH_KEY known_hosts $T_KNOWN_HOSTS" \
    start_job 1040 VM_SSH_PORT="$(free_port)" WATCHER_WAIT_S=60 OUTAGE_DEADLINE_S=60
  wait_for 30 file_has "$JD/ssh-counters.json" '"established":2' || { echo "no second session"; return 1; }
  kill -TERM "$JOB_PID"; job_rc 20 >/dev/null
  expect "export/ exists" test -d "$JD/export" || return 1
  local bad=0 s
  expect "the raw private file does hold the address (so the check is not vacuous)" file_has "$JD/ssh-raw.err" "$ip4" || return 1
  for s in "$ip4" "db8" "$vmu" "$vmh" "$T_SSH_KEY" "$T_KNOWN_HOSTS" "$W/scratch" "$(hostname -s)" "1040"; do
    if grep -rqF -- "$s" "$JD/export"; then echo "leaked into export/: $s"; bad=1; fi
  done
  return "$bad"
}

# ---- B6: requeue -------------------------------------------------------------------------------------------
test_b6_the_job_forbids_requeue() {
  mk_dummy_files; render_job "$W/job.sbatch" || return 1
  expect "#SBATCH --no-requeue is in the rendered job" grep -qx '#SBATCH --no-requeue' "$W/job.sbatch"
}
test_b6_a_restarted_allocation_is_refused() {
  fake_env hold || return 1
  start_job 1050 SLURM_RESTART_COUNT=1
  job_rc 20; rc=$RC
  expect "non-zero exit (got $rc)" test "$rc" != 0 -a "$rc" != running || return 1
  expect "nothing was created" bash -c "! ls '$W/scratch/s1-frp' 2>/dev/null | grep -q ."  || return 1
  expect "the reason is stated" file_has "$W/job.out" "restart 1"
}
test_b6_an_existing_run_directory_is_never_reused() {
  fake_env closed || return 1
  start_job 1051 VM_SSH_PORT="$(free_port)"; job_rc 40; rc1=$RC
  expect "first run ended (got $rc1)" test "$rc1" != running || return 1
  sum="$(cksum "$JD/job.log")"
  start_job 1051 VM_SSH_PORT="$(free_port)"; job_rc 20; rc2=$RC
  expect "second run with the same job id exits 1 (got $rc2)" test "$rc2" = 1 || return 1
  expect "the first run's log is untouched" test "$(cksum "$JD/job.log")" = "$sum" || return 1
  expect "the refusal is stated" file_has "$W/job.out" "already exists"
}

# ---- B7: PID identity ---------------------------------------------------------------------------------------
load_pidrec() {   # the pidrec block of the template, with the log function it needs
  JOB_DIR="$W/jd"; mkdir -p "$JOB_DIR"
  log() { echo "$*" >> "$W/log"; }
  local n
  for n in FRPC STREAMER FWD_LOOP PSSH SSHF SLP CUR; do printf -v "${n}_PID" '%s' ""; printf -v "${n}_ST" '%s' ""; done
  eval "$(sed -n '/^# >>> pidrec/,/^# <<< pidrec/p' "$KIT/job.sbatch.tmpl")"
  type stop_rec >/dev/null 2>&1 || { echo "no pidrec block in the template"; return 1; }
}
decoy() {   # NAME: a long sleeper whose comm is NAME (a copy of sleep); sets DECOY_PID
  cp "$(command -v sleep)" "$W/$1"
  "$W/$1" 300 &
  DECOY_PID=$!
  BG_PIDS+=("$DECOY_PID"); BG_STS+=("$(_start_time "$DECOY_PID")")
}
test_b7_a_reused_pid_is_not_signalled() {   # review B7: records held PIDs with no identity
  load_pidrec || return 1
  decoy other; true_st="$(proc_start "$DECOY_PID")"
  FRPC_PID="$DECOY_PID"; FRPC_ST=$(( true_st + 1 ))          # the record says a different process had this pid
  stop_rec FRPC
  expect "the other process survived" kill -0 "$DECOY_PID" || return 1
  expect "the record was cleared" test -z "$FRPC_PID$FRPC_ST" || return 1
  expect "the refusal was logged" file_has "$W/log" "not signalling"
}
test_b7_the_recorded_process_is_stopped_and_the_record_cleared() {
  load_pidrec || return 1
  decoy mine; rec_set FRPC "$DECOY_PID"
  expect "the record holds a start time" test -n "$FRPC_ST" || return 1
  stop_rec FRPC
  expect "the process is gone" bash -c "! kill -0 $DECOY_PID 2>/dev/null" || return 1
  expect "the record is cleared after reaping" test -z "$FRPC_PID$FRPC_ST"
}
test_b7_signal_rec_checks_identity_before_each_signal() {
  load_pidrec || return 1
  decoy other; FRPC_PID="$DECOY_PID"; FRPC_ST=1
  signal_rec FRPC KILL && { echo "signal_rec signalled a mismatching process"; return 1; }
  expect "the process survived" kill -0 "$DECOY_PID" || return 1
  rec_set FRPC "$DECOY_PID"; signal_rec FRPC STOP
  expect "a matching record is signalled" wait_for 3 bash -c "[ \"\$(awk '{print \$3}' /proc/$DECOY_PID/stat)\" = T ]"
}
test_b7_a_process_named_ssh_with_another_start_time_is_left_alone() {   # review B7: 'accepts any later process named ssh'
  load_pidrec || return 1
  decoy ssh                                                  # comm is ssh, as the old check required
  printf '%s %s\n' "$DECOY_PID" 1 > "$JOB_DIR/ssh.pid"       # a stale record: right pid, wrong start time
  printf '%s\n' "$DECOY_PID" > "$JOB_DIR/ssh.pid.old"
  stop_forward
  expect "the unrelated ssh survived" kill -0 "$DECOY_PID" || return 1
  expect "ssh.pid was cleared" test ! -s "$JOB_DIR/ssh.pid" || return 1
  st="$(proc_start "$DECOY_PID")"; printf '%s %s\n' "$DECOY_PID" "$st" > "$JOB_DIR/ssh.pid"
  stop_forward
  expect "a record with the right start time is stopped" bash -c "! kill -0 $DECOY_PID 2>/dev/null"
}
test_b7_job_records_pid_and_start_time_for_every_child() {
  fake_env hold || return 1
  full_fake_up 1060 || return 1
  local name pid st bad=0
  while IFS='=' read -r name rest; do
    pid="${rest%% *}"; st="${rest#* }"
    [ "$st" = "$(_start_time "$pid")" ] || { echo "$name: record '$rest' does not match /proc"; bad=1; }
  done < "$JD/pids"
  read -r spid sst < "$JD/ssh.pid"
  [ "$sst" = "$(_start_time "$spid")" ] || { echo "ssh.pid '$spid $sst' does not match /proc"; bad=1; }
  return "$bad"
}
test_b7_term_leaves_nothing_running_and_spares_other_processes() {
  fake_env hold || return 1
  decoy ssh                                                  # an unrelated process of the same user, named ssh
  full_fake_up 1061 || return 1
  kill -TERM "$JOB_PID"
  job_rc 30; rc=$RC
  expect "exit 0 after TERM (got $rc)" test "$rc" = 0 || return 1
  expect "no child left" no_children_left || return 1
  expect "the unrelated process survived" kill -0 "$DECOY_PID" || return 1
  expect "export/job.log written at exit" test -s "$JD/export/job.log"
}

# ---- B9: the watcher ----------------------------------------------------------------------------------------
test_b9_a_job_without_a_watcher_exits_5_and_cleans_up() {   # review B9: a paid job with nobody attached
  fake_env hold || return 1
  start_job 1070 VM_SSH_PORT="$(free_port)" WATCHER_WAIT_S=5
  job_rc 60; rc=$RC
  expect "exit 5 (got $rc)" test "$rc" = 5 || return 1
  expect "the log says why" file_has "$JD/job.log" "RESULT: NO WATCHER" || return 1
  expect "watcher_attach ok false recorded" test "$(probe_field "$JD/probe.jsonl" watcher_attach ok)" = false || return 1
  expect "no child left" no_children_left
}
test_b9_a_watcher_request_lets_the_job_proceed() {
  fake_env hold || return 1
  full_fake_up 1071 || return 1
  sleep 3
  job_rc 0
  expect "still running after the attach" test "$RC" = running || return 1
  expect "watcher_attach ok true recorded" test "$(probe_field "$JD/probe.jsonl" watcher_attach ok)" = true
}
test_b9_the_jobs_own_baseline_does_not_count_as_a_watcher() {
  fake_env hold || return 1
  start_job 1072 VM_SSH_PORT="$(free_port)" WATCHER_WAIT_S=6
  wait_for 30 file_has "$JD/job.log" "frpc: login to server success" || return 1
  expect "the baseline requests are in the engine's request log" test -s "$JD/streamer.requests" || return 1
  job_rc 60; rc=$RC
  expect "exit 5 even though the engine saw the baseline requests (got $rc)" test "$rc" = 5
}

# ---- the job directory root (RUN_ROOT) ----------------------------------------------------------------------
# Step 0 found operators could not put the job directory under their own subdirectory without overriding SCRATCH.
test_run_root_unset_means_scratch_slash_s1_frp() {
  fake_env closed || return 1
  start_job 1090 VM_SSH_PORT="$(free_port)"; job_rc 40
  expect "the job ended (got $RC)" test "$RC" != running || return 1
  expect "the job directory is SCRATCH/s1-frp/<jobid>-r0" test -s "$W/scratch/s1-frp/1090-r0/job.log"
}
test_run_root_override_puts_the_job_directory_there() {
  fake_env closed || return 1
  # ssh's own message names the override and the bare SCRATCH (not under RUN_ROOT): both must be scrubbed from what leaves the job
  FAKE_SSH_LEAK="ssh: root $W/mine/sub/s1/x and scratch $W/scratch/y" \
    start_job 1091 VM_SSH_PORT="$(free_port)" RUN_ROOT="$W/mine/sub/s1"; JD="$W/mine/sub/s1/1091-r0"
  job_rc 40
  expect "the job ended (got $RC)" test "$RC" != running || return 1
  expect "the job directory is RUN_ROOT/<jobid>-r0, created with its parents" test -s "$JD/job.log" || return 1
  expect "nothing was put under SCRATCH" bash -c "! ls '$W/scratch/s1-frp' 2>/dev/null | grep -q ." || return 1
  expect "the raw ssh stderr holds both paths (so the leak check below is not vacuous)" bash -c "grep -qF '$W/mine' '$JD/ssh-probe.err' && grep -qF '$W/scratch' '$JD/ssh-probe.err'" || return 1
  expect "the message itself reached export/probe.jsonl (so the check can fail)" file_has "$JD/export/probe.jsonl" "ssh said: ssh: root" || return 1
  local s bad=0
  for s in "$W/mine" "$W/scratch"; do
    if grep -rqF -- "$s" "$JD/export"; then echo "leaked into export/: $s"; bad=1; fi
  done
  return "$bad"
}
test_run_root_must_be_absolute_and_without_whitespace() {
  fake_env closed || return 1
  cd "$W" || return 1          # a broken check must not create the relative directory inside the repository
  start_job 1092 VM_SSH_PORT="$(free_port)" RUN_ROOT="relative/dir"; job_rc 20
  expect "a relative RUN_ROOT exits 1 (got $RC)" test "$RC" = 1 || return 1
  expect "the reason is stated" file_has "$W/job.out" "absolute" || return 1
  start_job 1093 VM_SSH_PORT="$(free_port)" RUN_ROOT="$W/my dir"; job_rc 20
  expect "a RUN_ROOT with a space exits 1 (got $RC)" test "$RC" = 1 || return 1
  expect "the reason is stated" file_has "$W/job.out" "whitespace" || return 1
  expect "nothing was created" bash -c "! ls '$W/my dir' '$W/relative' '$W/scratch/s1-frp' 2>/dev/null | grep -q ."
}

# ---- the run-book -------------------------------------------------------------------------------------------
test_docs_runbook_says_what_the_review_asked() {
  local bad=0 r="$KIT/README.md"
  chk() { grep -qiE -- "$1" "$r" || { echo "README lacks: $2"; bad=1; }; }
  chk 'probe_responder\.py' "the probe responder (B1)"
  chk 'start the watcher before' "start the watcher before submitting (B9)"
  chk 'cancels? the job' "the operator cancels the job when the watcher expires while it is pending (B9)"
  chk 'ssh-raw\.err' "the raw ssh stderr is private (B5)"
  chk 'never be copied' "never copy the raw file out (B5)"
  chk 'export/' "collect export/ only (B5)"
  chk 'permitlisten=' "the permitlisten key line (B8)"
  chk 'AllowStreamLocalForwarding' "server-side hardening (B8)"
  chk 'AllowTcpForwarding local' "server-side hardening (B8)"
  chk 'verdict\.py' "the verdict checker (nit)"
  chk 'attempts.*established.*disconnects.*recoveries' "the four counts (nit)"
  chk 'expected case' "20 s as the expected case (nit)"
  chk 'invalid' "an invalid fault measurement (B3)"
  grep -qE '^\s*restrict,port-forwarding,permitopen="127.0.0.1:7000",command="/bin/false"' "$r" && { echo "README still shows the old key line"; bad=1; }
  return "$bad"
}

test_docs_runbook_matches_the_step0_findings() {
  local bad=0 r="$KIT/README.md"
  chk() { grep -qiE -- "$1" "$r" || { echo "README lacks: $2"; bad=1; }; }
  chk 'ACCOUNT_ATHENA_A100' "the Athena settings variable ACCOUNT_ATHENA_A100"
  chk 'ACCOUNT_HELIOS_GH200' "the Helios GH200 settings variable"
  chk 'RUN_ROOT' "the configurable job directory root"
  chk '--streams 4' "the 1 h runs use four concurrent streams"
  chk 'does not test' "what --streams does not test"
  chk 'throughput at many streams' "throughput is not tested"
  chk 'wss' "the wss path is not tested"
  grep -qE 'ACCOUNT_ATHENA([^_]|$)' "$r" && { echo "README still has the old name ACCOUNT_ATHENA"; bad=1; }
  grep -qE -- '--partition=plgrid-now' "$r" && { echo "README still tells Athena runs to use plgrid-now"; bad=1; }
  return "$bad"
}

ALL="$(declare -F | sed -n 's/^declare -f test_//p' | tr '\n' ' ')"
[ "$#" -gt 0 ] && ALL="$*"
for n in $ALL; do run_test "$n"; done
