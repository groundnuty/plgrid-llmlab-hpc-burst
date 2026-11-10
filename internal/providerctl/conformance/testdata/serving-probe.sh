# The job side of the reach-host-dialed rows' serving check (issue #305; HBEP-6, Dial-home modes, host-dialed mode). It
# runs as a step of the row's own job, beside the agent: srun --overlap --jobid=<job> --ntasks=1 bash -s -- <Allocation
# directory>, with this script on standard input. It is what frpc would be, which the agent starts only for an Instance: a
# client of the agent's serving listener, whose connection the agent carries over the serving relay's session to the
# control-plane host's serving port, where the test's responder answers.
#
# What it does, and every line it writes, one per line, in this order, nothing else:
#   probe=1
#   tmpdir-step=set | unset         whether this --overlap step has TMPDIR set
#   tmpdir=<path>                   that TMPDIR, only when it is set (private: the test keeps it in its record only)
#   socket=<path>                   the first line of the Allocation directory's relay-socket, which the agent wrote
#                                   (empty when the file is missing; private, as tmpdir)
#   socket-dir-mode=<octal> | unknown   the mode of the socket's directory
#   agent-log=ok | unreadable       whether the Allocation directory's job.err, the agent's output, could be read
#   agent-tmpdir=<path>             the TMPDIR the agent's "host-dialed: listening for the relays" line names, the one its
#                                   socket's directory is under (empty when no such line names one; private, as tmpdir)
#   serving-port=found | missing    whether the agent's line names its serving listener's port
#   connect=ok | failed             (only with a port) whether the exchange below ran to its end
#   got-bytes=<n>                   (only with a port) how many bytes came back from the host
#   got-sha256=<64 hex> | none      (only with a port) the sha256 of them
#   done=1
#
# The exchange: one TCP connection to the agent's serving listener on 127.0.0.1. It first sends the 256 byte values in
# ascending order, made here by the shell's printf from octal escapes (the host checks each byte in its place); then it
# reads the 256 bytes the host sends back, the values in descending order, and writes down their sha256, which the test
# compares with the digest it holds as a literal, waiting @READ_SECONDS@ s at most (60 live). Neither side's check is built
# from what the other side sent. The sending
# and the reading are cat and head, so that a dry run can stand a node's corruption in for them; nothing else of the node
# is replaced.
#
# Every tool is in a compute node's base system: bash (for /dev/tcp), cat, head, timeout, sha256sum, cut, sed, stat,
# mktemp, dirname, wc. Its own files go in a directory of its own under TMPDIR (or /tmp), removed when it ends.
set -u
dir=${1:?the Allocation directory}
echo "probe=1"
if [ -n "${TMPDIR:-}" ]; then
  echo "tmpdir-step=set"
  echo "tmpdir=$TMPDIR"
else
  echo "tmpdir-step=unset"
fi
sock=""
if [ -f "$dir/relay-socket" ]; then sock=$(sed -n 1p "$dir/relay-socket" 2>/dev/null); fi
echo "socket=$sock"
mode=""
if [ -n "$sock" ]; then mode=$(stat -c %a "$(dirname "$sock")" 2>/dev/null); fi
echo "socket-dir-mode=${mode:-unknown}"
# The agent's own line, read affirmatively: a job.err that cannot be read says so, and a line that is not there leaves the
# TMPDIR empty, which the test reads as not shown, never as a pass.
line=""
if [ -r "$dir/job.err" ]; then
  echo "agent-log=ok"
  line=$(sed -n '/host-dialed: listening for the relays/p' "$dir/job.err" | tail -n 1)
else
  echo "agent-log=unreadable"
fi
echo "agent-tmpdir=$(printf '%s\n' "$line" | sed -n 's/.* tmpdir=\([^ ]*\).*/\1/p')"
port=$(printf '%s\n' "$line" | sed -n 's/.* serving=\([0-9][0-9]*\).*/\1/p')
if [ -z "$port" ]; then
  echo "serving-port=missing"
  echo "done=1"
  exit 0
fi
echo "serving-port=found"
work=$(mktemp -d "${TMPDIR:-/tmp}/hb-probe-XXXXXX" 2>/dev/null) || work=$(mktemp -d /tmp/hb-probe-XXXXXX)
trap 'rm -rf "$work"' EXIT
esc=""
i=0
while [ "$i" -lt 256 ]; do
  esc="$esc\\$(printf '%03o' "$i")"
  i=$((i + 1))
done
printf "$esc" >"$work/sent"
# A function, so that a connect that fails is a command that fails, not the end of the shell.
exchange() {
  cat "$work/sent" >&3 || return 1
  timeout @READ_SECONDS@ head -c 256 <&3 >"$work/got"
}
if exchange 3<>"/dev/tcp/127.0.0.1/$port" 2>/dev/null; then echo "connect=ok"; else echo "connect=failed"; fi
got=0
if [ -f "$work/got" ]; then got=$(($(wc -c <"$work/got"))); fi
echo "got-bytes=$got"
if [ "$got" -gt 0 ]; then echo "got-sha256=$(sha256sum "$work/got" | cut -c1-64)"; else echo "got-sha256=none"; fi
echo "done=1"
