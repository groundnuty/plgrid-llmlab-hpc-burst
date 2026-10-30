#!/usr/bin/env bash
# throwaway-sshd.sh -- source this file. A private sshd for tests, run as THIS user (not root).
#
# Everything lives in one mktemp -d directory: its own host key, its own client key, its own
# AuthorizedKeysFile, its own known_hosts, a high loopback port. It never reads or writes
# ~/.ssh (clients run with -F /dev/null) and never touches /etc/ssh or the system sshd.
#
#   tsshd_start            create the directory, keys and config, start sshd; sets TSSHD_DIR,
#                          TSSHD_PORT, TSSHD_PID, TSSHD_USER, TSSHD_KEY, TSSHD_KNOWN_HOSTS
#   tsshd_authorize OPTS   write authorized_keys: one line, OPTS then the client public key
#   tsshd_stop             stop sshd by its recorded PID (verified by start time) and delete the directory
#
# sshd is started with its absolute path (it re-executes itself) and, being unprivileged, with
# UsePAM no and StrictModes no. Returns non-zero (and prints why) when sshd cannot run here.

TSSHD_SSHD="${TSSHD_SSHD:-/usr/sbin/sshd}"
TSSHD_DIR="" TSSHD_PORT="" TSSHD_PID="" TSSHD_PID_ST="" TSSHD_USER="" TSSHD_KEY="" TSSHD_KNOWN_HOSTS=""

_tsshd_start_time() { local s; s="$(cat "/proc/$1/stat" 2>/dev/null)" || return 1; s="${s##*) }"; set -- $s; echo "${20}"; }

tsshd_free_port() {
  python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'
}

tsshd_start() {
  [ -x "$TSSHD_SSHD" ] || { echo "throwaway-sshd: no sshd at $TSSHD_SSHD" >&2; return 1; }
  command -v ssh-keygen >/dev/null 2>&1 || { echo "throwaway-sshd: no ssh-keygen" >&2; return 1; }
  [ -d /run/sshd ] || [ -d /var/empty ] || { echo "throwaway-sshd: no privilege separation directory (/run/sshd)" >&2; return 1; }
  TSSHD_DIR="$(mktemp -d "${TMPDIR:-/tmp}/s1-tsshd.XXXXXX")" || return 1
  chmod 700 "$TSSHD_DIR"
  TSSHD_USER="$(id -un)"
  TSSHD_PORT="$(tsshd_free_port)"
  ssh-keygen -q -t ed25519 -N '' -C s1-test-host -f "$TSSHD_DIR/host_key" || return 1
  ssh-keygen -q -t ed25519 -N '' -C s1-spike -f "$TSSHD_DIR/client_key" || return 1
  TSSHD_KEY="$TSSHD_DIR/client_key"
  : > "$TSSHD_DIR/authorized_keys"
  cat > "$TSSHD_DIR/sshd_config" <<CFG
Port $TSSHD_PORT
ListenAddress 127.0.0.1
HostKey $TSSHD_DIR/host_key
PidFile $TSSHD_DIR/sshd.pid
AuthorizedKeysFile $TSSHD_DIR/authorized_keys
UsePAM no
StrictModes no
PasswordAuthentication no
KbdInteractiveAuthentication no
PubkeyAuthentication yes
PermitRootLogin no
AllowUsers $TSSHD_USER
LogLevel VERBOSE
PrintMotd no
PrintLastLog no
CFG
  # TSSHD_HARDEN=1 adds the optional server-side restrictions the README documents for a dedicated account
  if [ "${TSSHD_HARDEN:-0}" = 1 ]; then
    cat >> "$TSSHD_DIR/sshd_config" <<CFG
Match User $TSSHD_USER
  AllowTcpForwarding local
  AllowStreamLocalForwarding no
  GatewayPorts no
  X11Forwarding no
  PermitTTY no
CFG
  fi
  printf '[127.0.0.1]:%s %s\n' "$TSSHD_PORT" "$(cut -d' ' -f1,2 "$TSSHD_DIR/host_key.pub")" > "$TSSHD_DIR/known_hosts"
  TSSHD_KNOWN_HOSTS="$TSSHD_DIR/known_hosts"
  "$TSSHD_SSHD" -t -f "$TSSHD_DIR/sshd_config" 2> "$TSSHD_DIR/sshd-t.err" || { echo "throwaway-sshd: sshd -t failed: $(head -3 "$TSSHD_DIR/sshd-t.err")" >&2; return 1; }
  "$TSSHD_SSHD" -D -e -f "$TSSHD_DIR/sshd_config" > "$TSSHD_DIR/sshd.log" 2>&1 &
  TSSHD_PID=$!
  TSSHD_PID_ST="$(_tsshd_start_time "$TSSHD_PID")"
  local i
  for i in $(seq 1 50); do
    kill -0 "$TSSHD_PID" 2>/dev/null || { echo "throwaway-sshd: sshd exited: $(tail -3 "$TSSHD_DIR/sshd.log")" >&2; return 1; }
    timeout 1 bash -c 'exec 3<>"/dev/tcp/127.0.0.1/$1"' _ "$TSSHD_PORT" 2>/dev/null && return 0
    sleep 0.1
  done
  echo "throwaway-sshd: sshd did not listen on 127.0.0.1:$TSSHD_PORT" >&2
  return 1
}

tsshd_authorize() {   # the options string (may be empty), then the client's public key
  local opts="$1" pub
  pub="$(cat "$TSSHD_DIR/client_key.pub")"
  if [ -n "$opts" ]; then printf '%s %s\n' "$opts" "$pub" > "$TSSHD_DIR/authorized_keys"
  else printf '%s\n' "$pub" > "$TSSHD_DIR/authorized_keys"; fi
}

tsshd_stop() {
  if [ -n "$TSSHD_PID" ] && [ "$(_tsshd_start_time "$TSSHD_PID")" = "$TSSHD_PID_ST" ]; then
    kill -TERM "$TSSHD_PID" 2>/dev/null
    local i
    for i in $(seq 1 25); do kill -0 "$TSSHD_PID" 2>/dev/null || break; sleep 0.2; done
    [ "$(_tsshd_start_time "$TSSHD_PID")" = "$TSSHD_PID_ST" ] && kill -KILL "$TSSHD_PID" 2>/dev/null
    { wait "$TSSHD_PID"; } 2>/dev/null || true
  fi
  [ -n "$TSSHD_DIR" ] && [ -d "$TSSHD_DIR" ] && case "$TSSHD_DIR" in */s1-tsshd.*) rm -rf "$TSSHD_DIR" ;; esac
  TSSHD_PID="" TSSHD_PID_ST="" TSSHD_DIR=""
}
