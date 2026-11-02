#!/bin/bash
# The reach probe (issue #225), run on a compute node as the Allocation's agent stand-in by the bootstrap, with the
# Allocation's directory as its argument. It tries a TCP connect to the gateway's forwarded port and leaves one line in
# <dir>/reach that says what came of it. The line is made of fixed words only: never the host, the port, a banner or any
# text the other side sent. The test renders the @...@ placeholders (reach_test.go) and reads the line back by its sha256.
#
#   outcome=reachable   the connect succeeded, and answer= says what answered, as seen on a second connection:
#       ssh      the other side sent an SSH banner (SSH-) before it was asked anything
#       tls      it answered a TLS ClientHello with a TLS record (a handshake or an alert)
#       data     it sent something else first, or answered the ClientHello with something that is not TLS
#       silent   it accepted the connection and sent nothing to either
#       unknown  the second connection could not be made (the first one is what counts as reached)
#   outcome=unresolved  the name did not resolve
#   outcome=refused     the connect was refused: the network is open and nothing listens there
#   outcome=timeout     no answer to the connect within the limit: filtered or dropped
#   outcome=unreachable the network said there is no route (ICMP unreachable)
#   outcome=error       anything else, or a tool the probe needs is missing
#
# The method is spike S1 step 0's (issue #18, branch spike/s1-frp-dial-home, spikes/s1-frp/job.sbatch.tmpl at 01c2ecd,
# tcp_probe): bash's /dev/tcp inside timeout(1), with the refused / timed-out / answered split and the SSH banner read.
# New here: the name is resolved apart first, the ClientHello that tells a TLS endpoint from a silent one, and the
# outcome as fixed words. An answer proves only that something answers there, not that it is the gateway (lesson L-17.7:
# an SSH banner was once taken for the VM's sshd while the host key was a gateway's); the post says so.

export LC_ALL=C
host='@HOST@'
port='@PORT@'
connect_s=@CONNECT_SECONDS@
read_s=@READ_SECONDS@
out="$1/reach"

# finish OUTCOME ANSWER: the one line this job leaves, and the end of the job.
finish() {
  printf 'outcome=%s answer=%s\n' "$1" "$2" > "$out.part" && mv "$out.part" "$out"
  exit 0
}

command -v timeout >/dev/null 2>&1 || finish error none

# The name first, by the resolver the connect will use. An address passes. getent exits 2 for a name it cannot resolve.
if command -v getent >/dev/null 2>&1; then
  timeout 10 getent ahosts "$host" >/dev/null 2>&1 || finish unresolved none
fi

# The connect. Nothing the shell says about it is kept: the text is read here and dropped, because it names the host. The
# outcome is read from the reasons alone: the text after the last ": " of each line (bash's "connect: Connection refused",
# "/dev/tcp/HOST/PORT: No route to host"). A host holds no colon and no space, so no part of the target's name can be in a
# reason, and a host called refused.gateway.example does not turn a "No route to host" into a refusal.
err=$(H=$host P=$port timeout "$connect_s" bash -c 'exec 3<>"/dev/tcp/$H/$P"' 2>&1); rc=$?
if [ "$rc" -eq 124 ]; then
  finish timeout none
elif [ "$rc" -ne 0 ]; then
  reasons=$(printf '%s\n' "$err" | sed -n 's/^.*: //p')
  case $reasons in
    *[Rr]efused*) finish refused none ;;
    *"Name or service not known"*|*"Temporary failure in name resolution"*|*"lookup failed"*|*"Unknown host"*) finish unresolved none ;;
    *"No route"*|*"unreachable"*|*"Network is down"*) finish unreachable none ;;
    *) finish error none ;;
  esac
fi

# What answers, on a second connection: the banner first (an SSH server speaks first), then, if it was silent, a TLS
# ClientHello. Every reply is reduced to one fixed word inside the shell, so no byte the other side sent leaves it.
ask='
trap "" PIPE   # a server that has closed makes the write fail, which the probe answers (silent), and not kill the shell
exec 3<>"/dev/tcp/$H/$P" || exit 11
IFS= read -r -t "$R" -N 4 first <&3
case $first in
  SSH-) echo ssh; exit 0 ;;
  ?*) echo data; exit 0 ;;
esac
printf "@HELLO@" >&3 || { echo silent; exit 0; }
IFS= read -r -t "$R" -N 2 second <&3
if [ "${#second}" -lt 2 ]; then echo silent; exit 0; fi
type=$(printf "%d" "'"'"'${second:0:1}")
version=$(printf "%d" "'"'"'${second:1:1}")
if { [ "$type" -eq 22 ] || [ "$type" -eq 21 ]; } && [ "$version" -eq 3 ]; then echo tls; else echo data; fi
'
answer=$(H=$host P=$port R=$read_s timeout $((connect_s + 2 * read_s + 2)) bash -c "$ask" 2>/dev/null </dev/null | tail -n 1)
case $answer in
  ssh|tls|data|silent) finish reachable "$answer" ;;
  *) finish reachable unknown ;;
esac
