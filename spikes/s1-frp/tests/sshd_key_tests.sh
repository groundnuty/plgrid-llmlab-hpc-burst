#!/usr/bin/env bash
# sshd_key_tests.sh -- what the README's authorized_keys line really allows, on a throwaway sshd run as this user
# (see throwaway-sshd.sh: own host key, own AuthorizedKeysFile, high loopback port, nothing under ~/.ssh).
#   sshd_key_tests.sh [NAME...]
# The key line is taken from the README itself, with its two ports (29223, the agent endpoint; 29227, the dial-home
# listener) swapped for free test ports in 29400-29499, so a README that drifts from what was tested is caught. The -R and streamlocal cases carry controls (the same attempt with
# no restriction succeeds), otherwise a refusal could be a broken test.
set -u
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

readme_key_options() { sed -n 's/^ *\(restrict,port-forwarding,[^ ]*\) ssh-ed25519 .*/\1/p' "$KIT/README.md" | head -n 1; }
sshopts() {   # SSHO: the client options for the throwaway sshd (no ~/.ssh: -F /dev/null, pinned host key); the host comes last
  SSHO=(-F /dev/null -p "$TSSHD_PORT" -i "$TSSHD_KEY" -o BatchMode=yes -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes
        -o "UserKnownHostsFile=$TSSHD_KNOWN_HOSTS" -o GlobalKnownHostsFile=/dev/null -o ConnectTimeout=8)
  SSHH="$TSSHD_USER@127.0.0.1"
}
token_from() {   # PORT: the line a connection to 127.0.0.1:PORT yields within 3 s, or nothing
  python3 - "$1" <<'PY'
import socket, sys
try:
    s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), 3); s.settimeout(3)
    data = b""
    while not data.endswith(b"\n"):
        chunk = s.recv(100)
        if not chunk:
            break
        data += chunk
    sys.stdout.write(data.decode("ascii", "replace").strip())
except OSError:
    pass
PY
}
key_env() {   # starts sshd (TSSHD_HARDEN as given), three token servers, a unix-socket server; authorises the README line (or $1)
  tsshd_start || return 1
  sshopts
  OK="$(free_port)"; OK2="$(free_port)"; OTHER="$(free_port)"; LP="$(free_port)"; RP="$(free_port)"
  SOCKDIR="$(mktemp -d "${XDG_RUNTIME_DIR:-/tmp}/s1s.XXXXXX")"   # short path: unix socket paths are limited to ~108 bytes
  trap 'bg_cleanup; tsshd_stop; rm -rf "$SOCKDIR" "$W"' EXIT
  bg python3 "$KIT/probe_responder.py" --port "$OK" --max-seconds 120 2>/dev/null
  bg python3 "$KIT/probe_responder.py" --port "$OK2" --max-seconds 120 2>/dev/null
  bg python3 "$KIT/probe_responder.py" --port "$OTHER" --max-seconds 120 2>/dev/null
  bg python3 "$TESTS_DIR/far_listener.py" unix "$SOCKDIR/sock"
  local opts="${1-$(readme_key_options | sed "s/127\.0\.0\.1:29223/127.0.0.1:$OK/; s/127\.0\.0\.1:29227/127.0.0.1:$OK2/")}"
  [ -n "$opts" ] || { echo "no key line found in the README"; return 1; }
  KEY_OPTS="$opts"
  tsshd_authorize "$opts"
  wait_for 5 test -S "$SOCKDIR/sock"
}
via_L() {   # DEST: an ssh -N -L to DEST on the key; prints the token read through it ('' if none); the ssh is stopped by pid
  case "$1" in /*) fwd="$LP:$1" ;; *) fwd="127.0.0.1:$LP:$1" ;; esac
  bg ssh "${SSHO[@]}" -N -o ExitOnForwardFailure=yes -L "$fwd" "$SSHH" 2>/dev/null
  local p="$BG_LAST" out
  wait_for 8 timeout 1 bash -c "exec 3<>/dev/tcp/127.0.0.1/$LP" 2>/dev/null
  out="$(token_from "$LP")"
  kill "$p" 2>/dev/null; { wait "$p"; } 2>/dev/null
  printf '%s' "$out"
}

test_b8_the_readme_key_line_carries_the_restrictions() {
  local o; o="$(readme_key_options)"
  expect "the line was found" test -n "$o" || return 1
  for want in restrict port-forwarding 'permitopen="127.0.0.1:29223"' 'permitopen="127.0.0.1:29227"' 'permitlisten="127.0.0.1:1"' 'command="/bin/false"'; do
    case "$o" in *"$want"*) ;; *) echo "key line lacks $want: $o"; return 1 ;; esac
  done
  expect "exactly two permitopen targets" test "$(printf '%s' "$o" | grep -o 'permitopen=' | wc -l)" = 2
}
test_b8_forward_to_each_permitted_port_works() {   # the agent endpoint (control) and the dial-home listener (tunnel)
  key_env || return 1
  expect "the token came through -L to the first permitted port" test "$(via_L "127.0.0.1:$OK")" = S1-PROBE-TOKEN-OK || return 1
  expect "the token came through -L to the second permitted port" test "$(via_L "127.0.0.1:$OK2")" = S1-PROBE-TOKEN-OK
}
test_b8_forward_to_another_port_is_refused() {
  key_env || return 1
  expect "control: the other port answers when the key is unrestricted" bash -c "true" || return 1
  tsshd_authorize ""; expect "control token" test "$(via_L "127.0.0.1:$OTHER")" = S1-PROBE-TOKEN-OK || return 1
  tsshd_authorize "$KEY_OPTS"
  expect "no token through -L to another port" test "$(via_L "127.0.0.1:$OTHER")" = ""
}
test_b8_remote_forward_is_refused() {
  key_env || return 1
  timeout 10 ssh "${SSHO[@]}" -N -o ExitOnForwardFailure=yes -R 127.0.0.1:$RP:127.0.0.1:$OK "$SSHH" 2>"$W/r.err"; rc=$?
  expect "ssh exits 255 (got $rc)" test "$rc" = 255 || return 1
  expect "it says the remote forward failed" file_has "$W/r.err" "remote port forwarding failed" || return 1
  expect "nothing listens on the requested port" bash -c "! timeout 1 bash -c 'exec 3<>/dev/tcp/127.0.0.1/$RP' 2>/dev/null"
}
test_b8_control_without_permitlisten_allows_the_remote_forward() {   # the review's point: permitopen does not limit -R
  key_env || return 1
  tsshd_authorize "restrict,port-forwarding,permitopen=\"127.0.0.1:$OK\",command=\"/bin/false\""
  bg ssh "${SSHO[@]}" -N -o ExitOnForwardFailure=yes -R "127.0.0.1:$RP:127.0.0.1:$OK" "$SSHH" 2>/dev/null; p="$BG_LAST"
  expect "without permitlisten the -R listener opens" wait_for 8 timeout 1 bash -c "exec 3<>/dev/tcp/127.0.0.1/$RP" 2>/dev/null
  rc=$?; kill "$p" 2>/dev/null; return "$rc"
}
test_b8_a_unix_socket_on_the_far_side_is_not_reachable_with_the_key_line() {   # direct-streamlocal, -L to a socket
  key_env || return 1
  tsshd_authorize ""
  expect "control: the socket answers when the key is unrestricted" test "$(via_L "$SOCKDIR/sock")" = UNIX-TOKEN || return 1
  tsshd_authorize "$KEY_OPTS"
  expect "no answer from the socket with the key line" test "$(via_L "$SOCKDIR/sock")" = ""
}
test_b8_a_remote_unix_socket_listener_needs_the_server_side_setting() {
  # With the key line alone, ssh -R /path:... still works: permitlisten covers TCP listeners only. That is why the
  # README documents AllowStreamLocalForwarding no (and AllowTcpForwarding local) for the dedicated account.
  key_env || return 1
  rm -f "$SOCKDIR/rsock"
  timeout 6 ssh "${SSHO[@]}" -N -o ExitOnForwardFailure=yes -R $SOCKDIR/rsock:127.0.0.1:$OK "$SSHH" 2>/dev/null; rc=$?
  expect "key line alone: the remote socket forward is NOT refused (it ran until the timeout, rc 124; got $rc)" test "$rc" = 124 || return 1
  tsshd_stop; rm -rf "$SOCKDIR"; TSSHD_HARDEN=1 key_env || return 1
  rm -f "$SOCKDIR/rsock"
  timeout 6 ssh "${SSHO[@]}" -N -o ExitOnForwardFailure=yes -R $SOCKDIR/rsock:127.0.0.1:$OK "$SSHH" 2>"$W/s.err"; rc=$?
  expect "with the server-side hardening it is refused (rc 255; got $rc)" test "$rc" = 255 || return 1
  expect "and no socket was created" test ! -S "$SOCKDIR/rsock"
}
test_b8_hardening_leaves_the_permitted_forward_working_and_refuses_tcp_remote() {
  TSSHD_HARDEN=1 key_env || return 1
  expect "the permitted -L still works on the hardened server" test "$(via_L "127.0.0.1:$OK")" = S1-PROBE-TOKEN-OK || return 1
  tsshd_authorize "restrict,port-forwarding,permitopen=\"127.0.0.1:$OK\",command=\"/bin/false\""   # even without permitlisten
  timeout 8 ssh "${SSHO[@]}" -N -o ExitOnForwardFailure=yes -R 127.0.0.1:$RP:127.0.0.1:$OK "$SSHH" 2>/dev/null; rc=$?
  expect "AllowTcpForwarding local refuses -R by itself (rc 255; got $rc)" test "$rc" = 255
}
test_b8_a_shell_command_gets_nothing() {
  key_env || return 1
  out="$(ssh "${SSHO[@]}" "$SSHH" "echo SHELL-RAN; touch $TSSHD_DIR/marker" 2>&1)"; rc=$?
  expect "no output (got '$out')" test -z "$out" || return 1
  expect "not run: no marker file" test ! -e "$TSSHD_DIR/marker" || return 1
  expect "non-zero exit, as /bin/false (got $rc)" test "$rc" != 0
}

ALL="$(declare -F | sed -n 's/^declare -f test_//p' | tr '\n' ' ')"
[ "$#" -gt 0 ] && ALL="$*"
for n in $ALL; do run_test "$n"; done
