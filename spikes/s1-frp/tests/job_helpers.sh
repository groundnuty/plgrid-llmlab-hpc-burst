#!/usr/bin/env bash
# job_helpers.sh -- shared by job_tests.sh and e2e_job.sh. Source it after lib.sh.
# ---- shared setup ------------------------------------------------------------------------------------
mk_dummy_files() {
  mkdir -p "$W/bin" "$W/scratch"
  echo dummy > "$W/key"; chmod 600 "$W/key"
  echo "dummy known host" > "$W/known_hosts"
  echo tok > "$W/token"; chmod 600 "$W/token"
  T_SSH_KEY="$W/key" T_KNOWN_HOSTS="$W/known_hosts" T_TOKEN="$W/token"
  # offline: curl is a double that reports HTTP 200, so step 0 never waits on the network
  printf '#!/bin/bash\ncase "$*" in *--version*) echo "curl fake";; *) printf 200;; esac\n' > "$W/bin/curl"; chmod +x "$W/bin/curl"
}
fake_env() {   # a full run with a double for ssh; the arguments are the lines of its plan
  mk_dummy_files
  mkdir -p "$W/fakessh"
  printf '#!/bin/bash\nexec python3 %q "$@"\n' "$TESTS_DIR/fake_ssh.py" > "$W/bin/ssh"; chmod +x "$W/bin/ssh"
  printf '%s\n' "$@" > "$W/fakessh/plan"
  export FAKE_SSH_DIR="$W/fakessh"
  make_fake_kit "$W/kit"
  T_KIT="$W/kit" T_LPORT="$(free_port)" T_FRPS_PORT=7000
  render_job "$W/job.sbatch" || return 1
  bash -n "$W/job.sbatch" || return 1
}
sshd_env() {   # a real throwaway sshd; the key may forward to FP only (or to $PERMIT); the full-run kit has a fake frpc
  mk_dummy_files
  tsshd_start || return 1
  FP="$(free_port)"; T_LPORT="$(free_port)"; T_FRPS_PORT="$FP"
  tsshd_authorize "restrict,port-forwarding,permitopen=\"127.0.0.1:${PERMIT:-$FP}\",permitlisten=\"127.0.0.1:1\",command=\"/bin/false\""
  T_SSH_KEY="$TSSHD_KEY" T_KNOWN_HOSTS="$TSSHD_KNOWN_HOSTS" T_VM_USER="$TSSHD_USER"
  make_fake_kit "$W/kit"; T_KIT="$W/kit"
  render_job "$W/job.sbatch" || return 1
}
start_job() {   # JOBID [ENV=VALUE...]: the job in its own session; sets JOB_PID and JD
  local jid="$1"; shift
  JD="$W/scratch/s1-frp/$jid-r0"
  bg_session env SCRATCH="$W/scratch" SLURM_JOB_ID="$jid" PATH="$W/bin:$PATH" "$@" bash "$W/job.sbatch" > "$W/job.out" 2>&1
  JOB_PID=$BG_LAST
}
job_rc() {   # SECONDS: waits for the job to exit; sets RC to its exit code, or 'running'
  local i
  RC=running
  for i in $(seq 1 $(( $1 * 5 ))); do
    if pid_gone "$JOB_PID" "$(_start_time "$JOB_PID")"; then
      wait "$JOB_PID" 2>/dev/null; RC=$?; return 0
    fi
    sleep 0.2
  done
}
attach_watcher() {   # stands in for stream_watch.py: one request to the engine, as if it came through the tunnel
  wait_for 30 test -s "$JD/streamer.port" || return 1
  python3 - "$(cat "$JD/streamer.port")" <<'PY'
import sys, urllib.request
urllib.request.urlopen("http://127.0.0.1:%s/health" % sys.argv[1], timeout=5).read()
PY
}
probe_field() {   # FILE PROBE FIELD: a field of the named probe line in probe.jsonl
  python3 - "$1" "$2" "$3" <<'PY'
import json, sys
for line in open(sys.argv[1]):
    d = json.loads(line)
    if d.get("probe") == sys.argv[2]:
        print(json.dumps(d.get(sys.argv[3])))
        break
PY
}
no_children_left() {   # every pid in $JD/pids is gone
  local line pid st
  while read -r line; do
    pid="${line#*=}"; st="${pid#* }"; pid="${pid%% *}"
    [ -n "$pid" ] || continue
    pid_gone "$pid" "$st" || { echo "still running: ${line%%=*} pid $pid"; return 1; }
  done < "$JD/pids"
}
start_far() { bg python3 "$TESTS_DIR/far_listener.py" "$1" "$2"; wait_for 5 timeout 1 bash -c "exec 3<>/dev/tcp/127.0.0.1/$2" 2>/dev/null; }
probe_only_run() {   # JOBID: PROBE_ONLY against the real sshd; sets RC
  start_job "$1" PROBE_ONLY=1 VM_SSH_PORT="$TSSHD_PORT"
  job_rc 40
}

