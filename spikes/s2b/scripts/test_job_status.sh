#!/usr/bin/env bash
# job.sh's exit status, with stub servers instead of vLLM and no router binaries (loopback only, about half a minute).
#
#   clean run                 -> exit 0, no failures.txt
#   a client that fails       -> exit 1, that arm and the llm-d arm that cannot start are in failures.txt, the arm
#                                that succeeded keeps its results
#   a prefix-cache reset that fails -> exit 1, the arm is not run (no meta.json), SKIPPED says why
# The expected status of each case comes from the rules in job.sh's header, not from running job.sh.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
STAGE=$(cd "$HERE/.." && pwd)    # holds py/s2b_listeners.py, which job.sh runs
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
export STUB_PIDS=$T/pids; : > "$STUB_PIDS"

cat > "$T/stub.py" <<'PY'
#!/usr/bin/env python3
# A worker or a router that only answers /health, /metrics and /reset_prefix_cache, on the port it is given.
import http.server, os, sys
port = int(sys.argv[sys.argv.index("--port") + 1])
with open(os.environ["STUB_PIDS"], "a") as f:
    f.write("%d\n" % os.getpid())

class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass
    def reply(self, code, body):
        self.send_response(code); self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
    def do_GET(self):
        if self.path == "/metrics":
            self.reply(200, b"vllm:prefix_cache_hits_total 1.0\nvllm:prefix_cache_queries_total 2.0\n")
        else:
            self.reply(200, b"ok")
    def do_POST(self):
        if os.environ.get("STUB_RESET") == "fail":
            self.reply(500, b'{"success":false}')
        else:
            self.reply(200, b'{"success":true}')

http.server.ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()
PY
cat > "$T/load.sh" <<'SH'
#!/usr/bin/env bash
# A client: writes its output file and a RESULT line, and fails for the run named in STUB_FAIL_RUN.
while [[ $# -gt 0 ]]; do case $1 in -run) run=$2; shift 2 ;; -out) out=$2; shift 2 ;; *) shift ;; esac; done
: > "$out"
if [[ $run == "${STUB_FAIL_RUN:-}" ]]; then
  echo "RESULT run=$run sessions=1 turns=1 concurrency=1 wall_s=0.1 failed_attempts=2 aborted_sessions=1"; exit 3
fi
echo "RESULT run=$run sessions=1 turns=1 concurrency=1 wall_s=0.1 failed_attempts=0 aborted_sessions=0"
SH
chmod +x "$T/stub.py" "$T/load.sh"

# run_job <case> <plan> [VAR=value...]: runs job.sh against the stubs; sets RC and OUT.
run_job() {
  local name=$1 plan=$2; shift 2
  mkdir -p "$T/$name/memfs"
  env "$@" MODE=sim S2B_HOME="$T/$name/work" MEMFS="$T/$name/memfs" STAGE="$STAGE" SIM_BIN="$T/stub.py" VR_CMD="$T/stub.py" \
    LOAD_BIN="$T/load.sh" ENVOY_BIN="$T/no-such-envoy" PLAN="$plan" LOAD_ARGS="" NWORK=2 RESULT_TAG=t \
    bash "$HERE/job.sh" > "$T/$name.log" 2>&1
  RC=$?; OUT=$T/$name/work/results/t
}
fails=0
check() { if eval "$2"; then echo "ok   $1"; else echo "FAIL $1"; fails=$((fails + 1)); fi; }

run_job clean "B:1:rr B:1:vr-hash"
check "clean run exits 0" '[[ $RC == 0 ]]'
check "clean run writes no failures.txt" '[[ ! -e $OUT/failures.txt ]]'
check "clean run recorded both arms" '[[ -f $OUT/arms/B1-rr/meta.json && -f $OUT/arms/B1-vr-hash/meta.json ]]'

run_job badclient "B:1:rr B:1:vr-hash B:1:llmd-sess" STUB_FAIL_RUN=B1-vr-hash
check "a failing client and an unstartable llm-d arm exit 1" '[[ $RC == 1 ]]'
check "failures.txt names the failed client" 'grep -q "B1-vr-hash: client exit status 3" "$OUT/failures.txt"'
check "failures.txt names the skipped llm-d arm" 'grep -q "B1-llmd-sess skipped" "$OUT/failures.txt"'
check "the arm that succeeded is not listed as failed" '! grep -q "B1-rr" "$OUT/failures.txt"'
check "partial results are kept: the good arm's meta.json" 'grep -q "\"client_rc\":0" "$OUT/arms/B1-rr/meta.json"'
check "partial results are kept: the failed arm's meta.json" 'grep -q "\"client_rc\":3" "$OUT/arms/B1-vr-hash/meta.json"'

run_job badreset "B:1:rr" STUB_RESET=fail
check "a failed reset exits 1" '[[ $RC == 1 ]]'
check "the arm was not measured on an unreset cache" '[[ ! -e $OUT/arms/B1-rr/meta.json && -f $OUT/arms/B1-rr/SKIPPED ]]'
check "failures.txt names the reset" 'grep -q "prefix-cache reset failed" "$OUT/failures.txt"'

left=0; while read -r pid; do [[ -n $pid ]] && kill -0 "$pid" 2> /dev/null && left=$((left + 1)); done < "$STUB_PIDS"
check "no stub process is left running" '[[ $left == 0 ]]'
if (( fails > 0 )); then echo "test_job_status: $fails check(s) failed; logs kept in $T" >&2; trap - EXIT; exit 1; fi
echo "test_job_status: all checks passed"
