#!/bin/bash
# fake_keyscan.sh -- a test double for ssh-keyscan, used by the job-template tests with the ssh double (fake_ssh.py).
# It is NOT the kit. It prints the line the real "ssh-keyscan -t ed25519 -p PORT HOST" prints for the key in
# $FAKE_SSH_DIR/hostkey.pub ([HOST]:PORT form when PORT is not 22). Calls are counted in $FAKE_SSH_DIR/keyscan-calls.
#   FAKE_KEYSCAN_OTHER_FROM=N   from call N on, present $FAKE_SSH_DIR/otherhost.pub instead (a host key that changes)
#   FAKE_KEYSCAN_NONE=1         print nothing (no answer)
port=22 host=""
while [ $# -gt 0 ]; do
  case "$1" in
    -p) port="$2"; shift 2 ;;
    -T|-t) shift 2 ;;
    *) host="$1"; shift ;;
  esac
done
n=$(( $(cat "$FAKE_SSH_DIR/keyscan-calls" 2>/dev/null || echo 0) + 1 )); echo "$n" > "$FAKE_SSH_DIR/keyscan-calls"
[ -z "${FAKE_KEYSCAN_NONE:-}" ] || exit 0
key="$FAKE_SSH_DIR/hostkey.pub"
[ -n "${FAKE_KEYSCAN_OTHER_FROM:-}" ] && [ "$n" -ge "$FAKE_KEYSCAN_OTHER_FROM" ] && key="$FAKE_SSH_DIR/otherhost.pub"
if [ "$port" = 22 ]; then h="$host"; else h="[$host]:$port"; fi
printf '%s %s\n' "$h" "$(cut -d' ' -f1,2 "$key")"
