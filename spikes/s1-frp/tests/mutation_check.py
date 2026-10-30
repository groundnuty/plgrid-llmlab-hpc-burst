#!/usr/bin/env python3
"""Mutation check: break the kit on purpose and require the named test to notice.

For each mutation this copies the kit to a temp directory (dl/ linked), replaces one exact string, and runs one
test against the copy with S1_KIT_DIR pointing at it. CAUGHT means the test failed on the mutant, as it must.
MISSED means it still passed: the test does not guard that line. Every mutation first checks that its old
string occurs exactly once, and that the same test passes on the unmutated kit.

    mutation_check.py [ID ...]          default: every mutation
"""
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
KIT = os.path.dirname(HERE)

J = "job.sbatch.tmpl"
# (id, review item, file, old, new, runner, test)
M = [
    ("M01", "B1", J, 'if [ "$FAR_STATE" = token ]; then FORWARD_STATE=true', 'if [ "$FAR_STATE" != unknown ]; then FORWARD_STATE=true',
     "job", "b1_probe_fails_when_nothing_listens_on_the_far_port"),
    ("M02", "B1", J, 'if [ "$FAR_STATE" = token ]; then FORWARD_STATE=true', 'if [ "$FAR_STATE" != unknown ]; then FORWARD_STATE=true',
     "job", "b1_probe_fails_when_the_far_end_is_silent"),
    ("M03", "B1", J, '    emit frpc_login \',"ok":false,"detail":"no login to server success from frpc in time"\'\n    EXIT_CODE=3; exit 3',
     '    emit frpc_login \',"ok":false,"detail":"no login to server success from frpc in time"\'\n    EXIT_CODE=3',
     "job", "b1_full_run_without_a_login_exits_3_and_cleans_up"),
    ("M04", "B2", "stream_watch.py", 'raise Fail("cut", "EOF before the blank line that ends the stream")', 'break',
     "py", "test_stream_watch.StreamFramingIsStrict.test_eof_before_the_final_blank_line_is_a_cut"),
    ("M05", "B2", "stream_watch.py", '    if len(body) != n:\n        raise Fail("no_response", "EOF after', '    if False:\n        raise Fail("no_response", "EOF after',
     "py", "test_stream_watch.HealthIsStrict.test_content_length_100_then_eof_is_not_healthy"),
    ("M06", "B2", "stream_watch.py", 'if expect is not None and body != expect:', 'if False:',
     "py", "test_stream_watch.HealthIsStrict.test_a_different_body_is_not_healthy"),
    ("M07", "B3", "stream_watch.py", '"valid": pre == "up"}\n        if pre != "up":', '"valid": True}\n        if False:',
     "py", "test_stream_watch.FaultPreconditions.test_the_review_sequence_gives_an_invalid_frpc_mark"),
    ("M08", "B3", J, '  start_engine "$LOCAL_PORT"\n', '  start_engine 0\n',
     "job", "b3_engine_restarts_on_its_original_port"),
    ("M09", "B4", J, '*"permission denied (publickey"*|', '',
     "job", "b4_classify_samples"),
    ("M10", "B4", J, '*"permission denied (publickey"*|', '',
     "job", "b4_a_revoked_key_stops_the_job_with_exit_4"),
    ("M11", "B4", J, 'if [ $(( now - down_since )) -ge "$OUTAGE_DEADLINE_S" ]; then', 'if [ $(( now - down_since )) -ge 999999 ]; then',
     "job", "b4_an_outage_past_the_deadline_stops_the_job_with_exit_4"),
    ("M12", "B4", J, '    [ -s "$JOB_DIR/transport.failed" ] || echo startup_timeout > "$JOB_DIR/transport.failed"\n', '    return 0\n',
     "job", "b4_a_startup_failure_is_fatal_not_a_warning"),
    ("M13", "B5", J, 'redact < "$JOB_DIR/$f" > "$d/$f"', 'cat < "$JOB_DIR/$f" > "$d/$f"',
     "job", "b5_exported_files_carry_none_of_the_identifying_strings"),
    ("M14", "B5", J, 's|([0-9]{1,3}\\.){3}[0-9]{1,3}|<ip4>|g\n', '',
     "job", "b5_redaction_removes_addresses_users_and_paths"),
    ("M15", "B5", J, "printf '{\"ts\":\"%s\",\"attempt\":%s,\"rc\":%s,\"class\":\"%s\",\"established\":%s,\"up_s\":%s,\"restart_in_s\":%s}\\n' \\\n        \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\" \"$n\" \"$rc\" \"$cls\" \"$established\" \"$up\" \"$delay\"",
     "printf '{\"ts\":\"%s\",\"attempt\":%s,\"rc\":%s,\"class\":\"%s\",\"established\":%s,\"up_s\":%s,\"restart_in_s\":%s,\"ssh_said\":\"%s\"}\\n' \\\n        \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\" \"$n\" \"$rc\" \"$cls\" \"$established\" \"$up\" \"$delay\" \"$(tail -n 1 \"$JOB_DIR/ssh-cur.err\")\"",
     "job", "b4_exit_lines_hold_only_allowlisted_fields"),
    ("M16", "B6", J, '#SBATCH --no-requeue\n', '', "job", "b6_the_job_forbids_requeue"),
    ("M17", "B6", J, '[ "$RESTART_COUNT" -eq 0 ] || die', 'true || die', "job", "b6_a_restarted_allocation_is_refused"),
    ("M18", "B6", J, 'mkdir "$JOB_DIR" 2>/dev/null || die', 'mkdir -p "$JOB_DIR" 2>/dev/null || die', "job", "b6_an_existing_run_directory_is_never_reused"),
    ("M19", "B7", J, '  [ "${i%% *}" != Z ] && [ "${i#* }" = "$2" ]', '  [ "${i%% *}" != Z ]', "job", "b7_a_reused_pid_is_not_signalled"),
    ("M20", "B7", J, '  [ "${i%% *}" != Z ] && [ "${i#* }" = "$2" ]', '  [ "${i%% *}" != Z ]', "job", "b7_a_process_named_ssh_with_another_start_time_is_left_alone"),
    ("M21", "B7", J, 'rec_set() { printf -v "${1}_PID" \'%s\' "$2"; printf -v "${1}_ST" \'%s\' "$(proc_start "$2" 2>/dev/null || true)"; }',
     'rec_set() { printf -v "${1}_PID" \'%s\' "$2"; printf -v "${1}_ST" \'%s\' "1"; }', "job", "b7_job_records_pid_and_start_time_for_every_child"),
    ("M22", "B9", J, 'if [ "$(req_count)" -gt "$BASE_REQS" ]; then attached=1; break; fi', 'attached=1; break', "job", "b9_a_job_without_a_watcher_exits_5_and_cleans_up"),
    ("M23", "B9", J, 'BASE_REQS="$(req_count)"', 'BASE_REQS=0', "job", "b9_the_jobs_own_baseline_does_not_count_as_a_watcher"),
    ("M24", "nit", "stream_watch.py", '                cur[0].shutdown(socket.SHUT_RDWR)', '                close_quiet(*cur)',
     "py", "test_stream_watch.SignalHandlerSurvives.test_sigterm_during_a_blocked_stream_read_writes_an_interrupted_summary"),
    ("M25", "nit", "verdict.py", "FREEZE_CLOSE_S = 30", "FREEZE_CLOSE_S = 300", "py", "test_verdict.Criteria.test_c4a_freeze_budget_is_30_s_for_closeproxy_and_10_s_for_the_probe"),
    ("M26", "nit", "verdict.py", "    ok = close is not None and close <= KILL_CLOSE_S and down is not None and down <= DOWN_S\n    return ok, \"CloseProxy after %s s (budget %d), probe down after %s s (budget %d)\" % (close, KILL_CLOSE_S, down, DOWN_S)",
     "    ok = True\n    return ok, \"CloseProxy after %s s (budget %d), probe down after %s s (budget %d)\" % (close, KILL_CLOSE_S, down, DOWN_S)",
     "py", "test_verdict.Criteria.test_c4c_frpc_kill_budget_is_5_s_for_closeproxy_and_10_s_for_the_probe"),
    ("M27", "B8", "README.md", 'permitlisten="127.0.0.1:1",', '', "key", "b8_remote_forward_is_refused"),
    ("M28", "B8", "README.md", 'permitlisten="127.0.0.1:1",', '', "key", "b8_the_readme_key_line_carries_the_restrictions"),
    ("M29", "B1", "probe_responder.py", 'TOKEN = b"S1-PROBE-TOKEN-OK\\n"', 'TOKEN = b"S1-PROBE-TOKEN-NO\\n"', "job", "b1_responder_and_job_share_the_token"),
    ("M30", "nit", "fake_streamer.py", "                    fh.write(\"%.3f GET %s\\n\" % (time.time(), self.path.split(\"?\")[0]))", "                    pass", "py",
     "test_engine_and_responder.Engine.test_every_request_is_appended_to_the_request_log"),
    ("M31", "streams", "verdict.py", "    for lane in range(1, n + 1):\n        problems = lane_problems", "    for lane in range(1, 2):\n        problems = lane_problems",
     "py", "test_verdict.PerStreamC2.test_a_cut_in_one_stream_fails_c2_even_when_the_others_complete"),
    ("M32", "streams", "stream_watch.py", "        lt.daemon = True\n        lt.start()\n", "        lt.daemon = True\n        lt.start()\n        lt.join()\n",
     "py", "test_stream_watch.ThreeStreamsLive.test_three_streams_run_at_once_and_each_completes"),
    ("M33", "streams", "stream_watch.py", "reconnects += sum(1 for s in mine[:-1] if", "reconnects += sum(1 for s in st.streams[:-1] if",
     "py", "test_stream_watch.PerStreamReport.test_a_lane_that_was_cut_last_has_not_reconnected"),
    ("M34", "streams", "fake_streamer.py", "class Server(ThreadingHTTPServer):", "class Server(HTTPServer):",
     "py", "test_engine_and_responder.Engine.test_three_concurrent_streams_take_as_long_as_one_and_health_keeps_answering"),
    ("M35", "run_root", J, """case "$RUN_ROOT" in /*) ;; *) die "RUN_ROOT must be an absolute path, got '$RUN_ROOT'" ;; esac""", ":",
     "job", "run_root_must_be_absolute_and_without_whitespace"),
    ("M36", "run_root", J, "RUN_ROOT:run-root RUN_BASE:scratch SCRATCH:scratch VM_ADDR:vm", "RUN_ROOT:run-root VM_ADDR:vm",
     "job", "run_root_override_puts_the_job_directory_there"),
    ("M37", "streams", "stream_watch.py", "        for cur in list(self.current.values()):\n", "        for cur in list(self.current.values())[:1]:\n",
     "py", "test_stream_watch.SignalHandlerSurvives.test_sigterm_with_three_streams_interrupts_every_one_of_them"),
]


def copy_kit(dest):
    for name in os.listdir(KIT):
        src = os.path.join(KIT, name)
        if name in ("dl", "__pycache__", "run"):
            continue
        if os.path.isdir(src):
            shutil.copytree(src, os.path.join(dest, name), ignore=shutil.ignore_patterns("__pycache__"))
        else:
            shutil.copy2(src, os.path.join(dest, name))
    os.symlink(os.path.join(KIT, "dl"), os.path.join(dest, "dl"))


def run(kit, runner, test):
    env = dict(os.environ, S1_KIT_DIR=kit)
    if runner == "py":
        cmd = [sys.executable, "-W", "ignore", "-m", "unittest", test]
        p = subprocess.run(cmd, cwd=HERE, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=300)
        return p.returncode == 0
    script = {"job": "job_tests.sh", "key": "sshd_key_tests.sh"}[runner]
    p = subprocess.run(["bash", os.path.join(HERE, script), test], cwd=HERE, env=env, stdout=subprocess.PIPE,
                       stderr=subprocess.STDOUT, timeout=400)
    out = p.stdout.decode("utf-8", "replace")
    return ("PASS  %s" % test) in out


def main(argv):
    want = set(argv)
    rows, bad = [], 0
    for mid, item, fname, old, new, runner, test in M:
        if want and mid not in want:
            continue
        tmp = tempfile.mkdtemp(prefix="s1-mut-")
        try:
            copy_kit(tmp)
            path = os.path.join(tmp, fname)
            with open(path) as fh:
                text = fh.read()
            if text.count(old) != 1:
                rows.append((mid, item, test, "BAD MUTATION (old string occurs %d times)" % text.count(old)))
                bad += 1
                continue
            if not run(KIT, runner, test):
                rows.append((mid, item, test, "BASELINE FAILS (the test fails on the unmutated kit)"))
                bad += 1
                continue
            with open(path, "w") as fh:
                fh.write(text.replace(old, new))
            caught = not run(tmp, runner, test)
            rows.append((mid, item, test, "CAUGHT" if caught else "MISSED"))
            if not caught:
                bad += 1
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
        print("%-4s %-4s %-8s %s" % (rows[-1][0], rows[-1][1], rows[-1][3], rows[-1][2]), flush=True)
    print("\n%d mutation(s), %d not caught or broken" % (len(rows), bad))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
