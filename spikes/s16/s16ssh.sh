#!/bin/sh
# Spike S16's ssh for the relays (s16 owner -ssh): the system's OpenSSH client, unchanged, with
# "-v -E <log>" first, so each relay's negotiated KEX, cipher and MAC are in its own log, and, when
# S16_CIPHER is set, "-o Ciphers=<it>". Both come before sshrun's arguments, so they win (ssh takes
# the first value it obtains); the -F file the Provider writes sets no cipher. exec: the process
# the owner starts, signals and samples is ssh itself.
set -eu
: "${S16_SSH_LOGDIR:?}"
if [ -n "${S16_CIPHER:-}" ]; then
  exec ssh -v -E "$S16_SSH_LOGDIR/ssh-$$.log" -o "Ciphers=$S16_CIPHER" "$@"
fi
exec ssh -v -E "$S16_SSH_LOGDIR/ssh-$$.log" "$@"
