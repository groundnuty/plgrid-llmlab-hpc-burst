#!/usr/bin/env bash
# Spike S1: fetch one pinned frp release for linux_arm64 and linux_amd64.
#
#   bash fetch-frp.sh                       download, verify and extract both architectures
#   bash fetch-frp.sh verify <file> <arch>  verify one file against the pinned sha256 (arch: amd64|arm64)
#
# Nothing is extracted before its tarball matches the sha256 written below. A mismatch deletes
# the download, prints both values and exits 1. Binaries are never committed (see .gitignore).
#
# Layout: dl/linux_<arch>/frp_<version>_linux_<arch>.tar.gz and, next to it, the extracted
# dl/linux_<arch>/frp_<version>_linux_<arch>/{frps,frpc,...}. Override the base with FRP_DL_DIR.
set -euo pipefail

FRP_VERSION="0.71.1"   # latest stable on 2026-10-30; the spike's floor is v0.71.1
# FRP_BASE_URL exists so the corrupted-download path can be tested with a file:// directory.
BASE_URL="${FRP_BASE_URL:-https://github.com/fatedier/frp/releases/download/v${FRP_VERSION}}"

# sha256 of the release tarballs. Source: the release's own checksum asset
# frp_sha256_checksums.txt (v0.71.1, itself sha256 1f501ae5...8ff), cross-checked against the
# "digest" field GitHub shows for each asset (`gh api repos/fatedier/frp/releases/tags/v0.71.1`).
# The two sources agree for both files. To bump the version, replace these from the new release.
expected_sha256() {
  case "$1" in
    amd64) echo "1d9875210972a5d1a62b2dc4c48df45ab0af05d3ca1b5e494a014428259df803" ;;
    arm64) echo "8edf590b0c396fa42f9c147121a76af83cfc2002b08f5aef0ea1008c43482019" ;;
    *) echo "fetch-frp: unknown arch '$1' (want amd64 or arm64)" >&2; return 1 ;;
  esac
}

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1
  elif command -v shasum >/dev/null 2>&1; then shasum -a 256 "$1" | cut -d' ' -f1
  else echo "fetch-frp: need sha256sum or shasum" >&2; return 2; fi
}

# verify_file <file> <arch>: exit 0 when the sha256 matches, 1 when it does not.
verify_file() {
  local file="$1" arch="$2" want got
  [ -f "$file" ] || { echo "fetch-frp: no such file: $file" >&2; return 1; }
  want="$(expected_sha256 "$arch")" || return 1
  got="$(sha256_of "$file")" || return 1
  if [ "$got" = "$want" ]; then
    echo "OK   linux_${arch}  sha256 ${got}"
    return 0
  fi
  echo "FAIL linux_${arch}  sha256 mismatch for ${file}" >&2
  echo "     expected ${want}" >&2
  echo "     got      ${got}" >&2
  return 1
}

fetch_arch() {
  local arch="$1" dir name tarball
  name="frp_${FRP_VERSION}_linux_${arch}"
  dir="${FRP_DL_DIR}/linux_${arch}"
  tarball="${dir}/${name}.tar.gz"
  mkdir -p "$dir"
  if [ -f "$tarball" ] && verify_file "$tarball" "$arch" >/dev/null 2>&1; then
    echo "have ${tarball} (verified)"
  else
    echo "download ${BASE_URL}/${name}.tar.gz"
    curl -fsSL --retry 3 --connect-timeout 15 -o "${tarball}.part" "${BASE_URL}/${name}.tar.gz"
    if ! verify_file "${tarball}.part" "$arch"; then
      rm -f "${tarball}.part"
      echo "fetch-frp: deleted the bad download; nothing was extracted" >&2
      return 1
    fi
    mv "${tarball}.part" "$tarball"
  fi
  if [ ! -x "${dir}/${name}/frpc" ]; then
    tar -xzf "$tarball" -C "$dir"
  fi
  [ -x "${dir}/${name}/frpc" ] && [ -x "${dir}/${name}/frps" ] || {
    echo "fetch-frp: frpc/frps missing after extracting ${tarball}" >&2; return 1; }
  echo "ready ${dir}/${name}/{frps,frpc}"
}

main() {
  local here
  here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  FRP_DL_DIR="${FRP_DL_DIR:-${here}/dl}"
  case "${1:-fetch}" in
    verify)
      [ "$#" -eq 3 ] || { echo "usage: fetch-frp.sh verify <file> <amd64|arm64>" >&2; exit 2; }
      verify_file "$2" "$3"
      ;;
    fetch)
      command -v curl >/dev/null 2>&1 || { echo "fetch-frp: curl is required" >&2; exit 2; }
      fetch_arch amd64
      fetch_arch arm64
      echo "frp v${FRP_VERSION}: both architectures verified and extracted under ${FRP_DL_DIR}"
      ;;
    *)
      echo "usage: fetch-frp.sh [fetch | verify <file> <amd64|arm64>]" >&2; exit 2 ;;
  esac
}

main "$@"
