#!/usr/bin/env bash
# queue-check.sh against stubs: a PATH ssh that runs the remote script here, and a PATH sbatch. Nothing leaves this machine.
# The same file also checks submit-line.sh (the line that submits the job of the SAME shape; it contacts nothing): see the end.
#
# Every stub reply is seeded with a job ID, a host, an account, a URL and a token: the things queue-check.sh must never
# print. The expected values come from CONTRACT.md's job shape and the brief, written out below, never from shape.sh
# (a shape read from shape.sh and compared with shape.sh would agree whatever it held).
#
#   estimate         stdout is exactly the `to start at <time>` text, the seeded values are in neither stream, the stub
#                    sbatch saw every option of the shape, the seeded account and --test-only and nothing else, nothing
#                    was submitted, ssh was called once as `ssh athena bash -s` without the account, and the lock was held
#                    while the remote script ran
#   login banner     a banner carrying the account and the host on ssh's stdout does not reach stdout
#   refused shape    sbatch exits 1 with seeded stderr: `no estimate`, exit 3, nothing seeded in either stream
#   no start time    sbatch exits 0 with no start time: `no estimate`, exit 0
#   ssh fails        exit 1, empty stdout, nothing seeded in stderr
#   settings         both required variables named at once when missing (exit 2); a settings file without the account (exit 2)
#   submit line      parsed with the rules of internal/slurm/fakeslurm's lexer and option table (lex.go, sbatch.go): one sbatch
#                    command, only options the fake knows, the contract's shape and nothing more, the account only from the settings file
#                    (and a refusal, not a line without one, when the file lacks it), the job's environment inside one --wrap word; a missing variable, a value that could end the word and
#                    a relative path are refused (exit 2) without echoing the value
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
mkdir -p "$T/bin" "$T/state"
export STUB_STATE=$T/state

SEED_ACCT=seedacct-x1
printf 'export ACCOUNT_ATHENA_A100="%s"\nOTHER=1\n' "$SEED_ACCT" > "$T/settings.env"
printf 'OTHER=1\n' > "$T/settings-noacct.env"
LOCK=$T/athena.lock

cat > "$T/bin/ssh" <<'SH'
#!/bin/bash
# Only `ssh athena bash -s` is a call this stub answers; the remote script arrives on stdin and runs here.
printf '%s\n' "$*" >> "$STUB_STATE/ssh-argv"
[ "$*" = "athena bash -s" ] || { echo "stub ssh: unexpected arguments" >&2; exit 97; }
[ -z "${STUB_SSH_FAIL:-}" ] || { echo "ssh: connect to host seedhost01: Connection refused (account seedacct-x1)" >&2; exit 255; }
if flock -n "$S2C_ATHENA_LOCK" true; then echo free >> "$STUB_STATE/lock-state"; else echo held >> "$STUB_STATE/lock-state"; fi
[ -z "${STUB_BANNER:-}" ] || echo "welcome to seedhost01, account seedacct-x1, job 424242 https://seed.example/x?token=SEEDTOKEN"
exec bash -s
SH
cat > "$T/bin/sbatch" <<'SH'
#!/bin/bash
# Answers only the test-only call of the S2c shape; any other word, a missing word or a real submit is a recorded failure.
printf '%s\n' "$*" >> "$STUB_STATE/sbatch-argv"
want=(--test-only --job-name=hb-s2c --no-requeue --partition=plgrid-gpu-a100 --time=01:15:00 --nodes=1 --gres=gpu:4
  --cpus-per-task=64 --mem=500G --account=seedacct-x1 --wrap true)
bad=0
[ $# -eq ${#want[@]} ] || bad=1
for w in "${want[@]}"; do case " $* " in *" $w "*) ;; *) bad=1 ;; esac; done
if [ $bad = 1 ]; then echo "BAD $*" >> "$STUB_STATE/events"; echo "sbatch: error: unexpected call (job 424242 seedhost01)" >&2; exit 64; fi
if [ -n "${STUB_REJECT:-}" ]; then
  echo "sbatch: error: Batch job submission failed: Requested node configuration is not available (account seedacct-x1, seedhost01, job 424242, https://seed.example/x?token=SEEDTOKEN)" >&2
  exit 1
fi
if [ -n "${STUB_NOEST:-}" ]; then echo "sbatch: Job 424242 will be queued, see https://seed.example/x?token=SEEDTOKEN" >&2; exit 0; fi
echo "sbatch: Job 424242 to start at 2030-01-01T00:00:00 using 64 processors on nodes seedhost01 in partition p [account seedacct-x1] see https://seed.example/x?token=SEEDTOKEN" >&2
exit 0
SH
chmod +x "$T/bin/ssh" "$T/bin/sbatch"

fails=0
check() { if eval "$2"; then echo "ok   $1"; else echo "FAIL $1"; fails=$((fails + 1)); fi; }
# run_qc <case> [VAR=value...]: runs queue-check.sh with the stubs first on PATH; sets RC, OUT, ERR; resets the stub state.
run_qc() {
  local name=$1; shift
  rm -f "$STUB_STATE"/*
  env PATH="$T/bin:/usr/bin:/bin" S2C_PRIVATE_SETTINGS="$T/settings.env" S2C_ATHENA_LOCK="$LOCK" "$@" \
    bash "$HERE/queue-check.sh" > "$T/$name.out" 2> "$T/$name.err"
  RC=$?; OUT=$T/$name.out; ERR=$T/$name.err
}
# no_seed <file...>: none of the seeded values is in any of the files.
no_seed() { ! grep -q -E '424242|seedhost01|seedacct-x1|SEEDTOKEN|seed\.example|seedcluster' "$@"; }

run_qc ok
check "estimate: exit 0" '[[ $RC == 0 ]]'
check "estimate: stdout is exactly the start time" '[[ $(cat "$OUT") == "to start at 2030-01-01T00:00:00" ]]'
check "estimate: no seeded value in stdout or stderr" 'no_seed "$OUT" "$ERR"'
check "estimate: the stub sbatch saw the whole shape, the account and --test-only, once" '[[ $(wc -l < "$STUB_STATE/sbatch-argv") == 1 && ! -e $STUB_STATE/events ]]'
check "estimate: ssh was called once, as athena bash -s, without the account" '[[ $(cat "$STUB_STATE/ssh-argv") == "athena bash -s" ]]'
check "estimate: the lock was held while the remote script ran" '[[ $(cat "$STUB_STATE/lock-state") == held ]]'

run_qc banner STUB_BANNER=1
check "login banner: exit 0 and stdout is exactly the start time" '[[ $RC == 0 && $(cat "$OUT") == "to start at 2030-01-01T00:00:00" ]]'
check "login banner: no seeded value in stdout or stderr" 'no_seed "$OUT" "$ERR"'

run_qc reject STUB_REJECT=1
check "refused shape: exit 3" '[[ $RC == 3 ]]'
check "refused shape: stdout is only 'no estimate'" '[[ $(cat "$OUT") == "no estimate" ]]'
check "refused shape: no seeded value in stdout or stderr" 'no_seed "$OUT" "$ERR"'
check "refused shape: sbatch was asked once with the shape, nothing submitted" '[[ $(wc -l < "$STUB_STATE/sbatch-argv") == 1 && ! -e $STUB_STATE/events ]]'

run_qc noest STUB_NOEST=1
check "no start time: exit 0 and 'no estimate'" '[[ $RC == 0 && $(cat "$OUT") == "no estimate" ]] && no_seed "$OUT" "$ERR"'

run_qc sshfail STUB_SSH_FAIL=1
check "ssh fails: exit 1 and empty stdout" '[[ $RC == 1 && ! -s $OUT ]]'
check "ssh fails: nothing seeded in stderr" 'no_seed "$ERR"'

run_qc nosettings S2C_PRIVATE_SETTINGS= S2C_ATHENA_LOCK=
check "missing variables: exit 2 and both named" '[[ $RC == 2 ]] && grep -q S2C_PRIVATE_SETTINGS "$ERR" && grep -q S2C_ATHENA_LOCK "$ERR"'
check "missing variables: nothing was called" '[[ ! -e $STUB_STATE/ssh-argv ]]'
run_qc noacct S2C_PRIVATE_SETTINGS="$T/settings-noacct.env"
check "settings without the account: exit 2, nothing called" '[[ $RC == 2 && ! -e $STUB_STATE/ssh-argv ]]'

# ---- submit-line.sh: the line, read the way the fake Slurm reads it ----
cat > "$T/lexcheck.py" <<'PY'
# Reads a submit line with the rules of internal/slurm/fakeslurm/lex.go (plain characters or single-quoted words, no
# other character outside quotes) and sbatch.go's option table, and prints "<name>=<value>" per option, one per line.
import sys
PLAIN = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:=,/@%+-")
OPTS = {"parsable": 0, "no-requeue": 0, "requeue": 0, "hold": 0, "job-name": 1, "partition": 1, "account": 1, "qos": 1, "time": 1,
        "nodes": 1, "gres": 1, "cpus-per-task": 1, "mem": 1, "output": 1, "error": 1, "wrap": 1, "constraint": 1}
line = sys.stdin.read().rstrip("\n")
words, cur, inq, inw = [], [], False, False
for c in line:
    if inq:
        if c == "'":
            inq = False
        else:
            cur.append(c)
    elif c == "'":
        inq = inw = True
    elif c == " ":
        if inw:
            words.append("".join(cur)); cur = []; inw = False
    elif c in PLAIN:
        cur.append(c); inw = True
    else:
        sys.exit("lexer: %r outside quotes" % c)
if inq:
    sys.exit("lexer: unterminated quote")
if inw:
    words.append("".join(cur))
if not words or words[0] != "sbatch":
    sys.exit("not an sbatch command")
for w in words[1:]:
    if not w.startswith("--"):
        sys.exit("positional word %r: the fake takes only --wrap" % w[:20])
    name, eq, val = w[2:].partition("=")
    if name not in OPTS or (OPTS[name] == 1 and not eq) or (OPTS[name] == 0 and eq):
        sys.exit("option %r is not one the fake reads in this form" % name)
    print("%s=%s" % (name, val))
PY
SHAPE_WANT=$(printf '%s\n' parsable= job-name=hb-s2c no-requeue= partition=plgrid-gpu-a100 time=01:15:00 nodes=1 gres=gpu:4 cpus-per-task=64 mem=500G | sort)
mkdir -p "$T/h/img" "$T/h/ckpt"; : > "$T/h/vr"
SL_ENV=(S2C_HOME="$T/h" IMAGE="$T/h/img" CHECKPOINT_DIR="$T/h/ckpt" VR_BIN="$T/h/vr")
# run_sl <case> [VAR=value...]: submit-line.sh with the environment above and no private settings unless the case gives them
run_sl() { local name=$1; shift; env -u S2C_PRIVATE_SETTINGS "${SL_ENV[@]}" "$@" bash "$HERE/submit-line.sh" > "$T/$name.out" 2> "$T/$name.err"; RC=$?; OUT=$T/$name.out; ERR=$T/$name.err; }
opts_of() { python3 -I "$T/lexcheck.py" < "$OUT"; }

run_sl sl MODE=double PORT_BASE=28420 PLAN="B:1:rr B:1:envoy-bound" LOAD_ARGS="-sessions 4 -agents 0.5,0.3,0.2"
check "submit line: exit 0, one line, and the fake's lexer and option table accept it" '[[ $RC == 0 && $(wc -l < "$OUT") == 1 ]] && opts_of > "$T/sl.opts"'
check "submit line: the options are exactly the contract's shape (plus --parsable and --wrap), with no account" '[[ $(grep -v "^wrap=" "$T/sl.opts" | sort) == "$SHAPE_WANT" ]]'
check "submit line: the --wrap word carries the required variables, the optional ones that were set, and runs job.sh from the stage" 'w=$(grep "^wrap=" "$T/sl.opts"); [[ $w == *"S2C_HOME=\"$T/h\" IMAGE=\"$T/h/img\" CHECKPOINT_DIR=\"$T/h/ckpt\" VR_BIN=\"$T/h/vr\" "* && $w == *"MODE=\"double\""* && $w == *"PORT_BASE=\"28420\""* && $w == *"PLAN=\"B:1:rr B:1:envoy-bound\""* && $w == *"LOAD_ARGS=\"-sessions 4 -agents 0.5,0.3,0.2\""* && $w == *" bash $T/h/stage/scripts/job.sh" && $w != *RESULT_TAG* ]]'
run_sl slacct S2C_PRIVATE_SETTINGS="$T/settings.env"
check "submit line: with S2C_PRIVATE_SETTINGS the account of the file is added as --account=<it>, and nothing else changes" '[[ $RC == 0 ]] && opts_of | grep -q -x "account=$SEED_ACCT" && [[ $(opts_of | grep -v -e "^wrap=" -e "^account=" | sort) == "$SHAPE_WANT" ]]'
run_sl slstage STAGE="$T/h/other-stage"
check "submit line: STAGE moves the job.sh path" '[[ $RC == 0 ]] && opts_of | grep "^wrap=" | grep -q " bash $T/h/other-stage/scripts/job.sh\$"'
pre=(env -u S2C_HOME -u VR_BIN IMAGE="$T/h/img" CHECKPOINT_DIR="$T/h/ckpt")
"${pre[@]}" bash "$HERE/submit-line.sh" > "$T/slmiss.out" 2> "$T/slmiss.err"; RC=$?
check "submit line: missing required variables are all named at once (exit 2, no output)" '[[ $RC == 2 && ! -s $T/slmiss.out ]] && grep -q "S2C_HOME" "$T/slmiss.err" && grep -q "VR_BIN" "$T/slmiss.err" && ! grep -q "CHECKPOINT_DIR" "$T/slmiss.err"'
run_sl slquote PLAN="B:1:rr'; touch $T/pwned; echo '"
check "submit line: a value with a single quote is refused without being echoed" '[[ $RC == 2 && ! -s $OUT && ! -e $T/pwned ]] && grep -q "PLAN" "$ERR" && ! grep -q "pwned" "$ERR"'
run_sl slsubst RESULT_TAG='run-$(id -un)'
check "submit line: a value that could open a substitution is refused" '[[ $RC == 2 && ! -s $OUT ]] && grep -q "RESULT_TAG" "$ERR" && ! grep -q "id -un" "$ERR"'
run_sl slrel S2C_HOME=relative/dir
check "submit line: a relative path is refused" '[[ $RC == 2 && ! -s $OUT ]] && grep -q "S2C_HOME" "$ERR"'
printf 'ACCOUNT_ATHENA_A100="a b"\n' > "$T/settings-badacct.env"
run_sl slnoacct S2C_PRIVATE_SETTINGS="$T/settings-noacct.env"
check "submit line: settings without the account are refused, never a line that silently has none" '[[ $RC == 2 && ! -s $OUT ]]'
run_sl slbadacct S2C_PRIVATE_SETTINGS="$T/settings-badacct.env"
check "submit line: an account with a space is refused" '[[ $RC == 2 && ! -s $OUT ]]'
run_sl slunread S2C_PRIVATE_SETTINGS="$T/no-such-settings.env"
check "submit line: an unreadable settings file is refused" '[[ $RC == 2 && ! -s $OUT ]]'

if (( fails > 0 )); then echo "test_queue_check: $fails check(s) failed; logs kept in $T" >&2; trap - EXIT; exit 1; fi
echo "test_queue_check: all checks passed"
