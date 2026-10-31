#!/usr/bin/env bash
# Spike S1: fetch one pinned Envoy release for the VM side (linux x86_64 only).
#
#   bash fetch-envoy.sh                  download into dl/envoy/envoy-<version>, verify, then mark it executable
#   ENVOY_SRC=<file> bash fetch-envoy.sh copy a local binary there instead of downloading, after verifying it
#   bash fetch-envoy.sh verify <file>    check an existing binary against the pinned sha256 (exit 0/1)
#
# The file is verified BEFORE it is marked executable. A mismatch deletes the download, prints both
# values and exits 1; nothing is left in the target directory. Binaries are never committed (dl/ is
# git-ignored). Override the directory with ENVOY_DL_DIR (the result is $ENVOY_DL_DIR/envoy-<version>).
set -euo pipefail

ENVOY_VERSION="1.33.2"
ASSET="envoy-${ENVOY_VERSION}-linux-x86_64"
# ENVOY_BASE_URL exists so the mismatch path can be tested with a file:// directory.
BASE_URL="${ENVOY_BASE_URL:-https://github.com/envoyproxy/envoy/releases/download/v${ENVOY_VERSION}}"

# Source: the release's checksums.txt.asc (v1.33.2); its PGP signature is NOT verified here. This is
# the same binary spikes S1c and S2 ran. To bump the version, replace both values from the new release.
EXPECTED_SHA256="bd2ff87c5efc95fd7880d710915f573c1aab4d9c55d8da886dcd91df43fad741"

PART=""
MADE_DIR=""

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1
  elif command -v shasum >/dev/null 2>&1; then shasum -a 256 "$1" | cut -d' ' -f1
  else echo "fetch-envoy: need sha256sum or shasum" >&2; return 2; fi
}

# verify_file <file>: exit 0 when the sha256 matches, 1 when it does not.
verify_file() {
  local file="$1" got
  [ -f "$file" ] || { echo "fetch-envoy: no such file: $file" >&2; return 1; }
  got="$(sha256_of "$file")" || return 1
  if [ "$got" = "$EXPECTED_SHA256" ]; then
    echo "OK   envoy-${ENVOY_VERSION}  sha256 ${got}"
    return 0
  fi
  echo "FAIL envoy-${ENVOY_VERSION}  sha256 mismatch for ${file}" >&2
  echo "     expected ${EXPECTED_SHA256}" >&2
  echo "     got      ${got}" >&2
  return 1
}

# The highest directory above $1 that does not exist yet (empty when $1 exists).
highest_missing() {
  local d="$1" top=""
  while [ ! -d "$d" ]; do top="$d"; d="$(dirname "$d")"; done
  echo "$top"
}

# On any exit with a half-written download, remove it, and the directories this run created if they are empty.
cleanup() {
  local d="${ENVOY_DL_DIR:-}"
  if [ -n "$PART" ]; then rm -f "$PART"; fi
  while [ -n "$MADE_DIR" ] && [ -n "$d" ] && rmdir "$d" 2>/dev/null; do
    [ "$d" = "$MADE_DIR" ] && break
    d="$(dirname "$d")"
  done
}

fetch() {
  local dir="${ENVOY_DL_DIR}" dest="${ENVOY_DL_DIR}/envoy-${ENVOY_VERSION}"
  if [ -f "$dest" ] && verify_file "$dest" >/dev/null 2>&1; then
    chmod 755 "$dest"
    echo "have ${dest} (verified)"
    return 0
  fi
  MADE_DIR="$(highest_missing "$dir")"
  mkdir -p "$dir"
  PART="${dest}.part"
  if [ -n "${ENVOY_SRC:-}" ]; then
    echo "copy ${ENVOY_SRC}"
    cp "$ENVOY_SRC" "$PART"
  else
    command -v curl >/dev/null 2>&1 || { echo "fetch-envoy: curl is required" >&2; exit 2; }
    echo "download ${BASE_URL}/${ASSET}"
    if ! curl -fsSL --retry 3 --connect-timeout 15 -o "$PART" "${BASE_URL}/${ASSET}"; then
      echo "fetch-envoy: download failed; nothing was installed" >&2
      return 1
    fi
  fi
  if ! verify_file "$PART"; then
    echo "fetch-envoy: deleted the bad file; nothing was installed" >&2
    return 1
  fi
  chmod 755 "$PART"
  mv "$PART" "$dest"
  PART=""
  MADE_DIR=""
  echo "ready ${dest}"
}

main() {
  local here
  here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  ENVOY_DL_DIR="${ENVOY_DL_DIR:-${here}/dl/envoy}"
  trap cleanup EXIT
  case "${1:-fetch}" in
    verify)
      [ "$#" -eq 2 ] || { echo "usage: fetch-envoy.sh verify <file>" >&2; exit 2; }
      verify_file "$2"
      ;;
    fetch)
      fetch
      ;;
    *)
      echo "usage: fetch-envoy.sh [fetch | verify <file>]   (ENVOY_SRC=<file> copies a local binary)" >&2; exit 2 ;;
  esac
}

main "$@"
