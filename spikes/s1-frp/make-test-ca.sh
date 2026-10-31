#!/usr/bin/env bash
# Spike S1: make a throwaway test CA and one server certificate signed by it (openssl, EC P-256).
#
#   bash make-test-ca.sh DIR [SERVER_NAME]
#
# Writes into DIR (created mode 700 if missing): ca.crt, ca.key (600), server.crt, server.key (600).
# The server certificate carries SAN DNS:<SERVER_NAME> (default dial-home.hpcburst.invalid) and
# IP:127.0.0.1, usage serverAuth, and is not a CA. Both certificates are valid for 30 days.
# Clients verify with ca.crt and the fixed server name, so the connect address and the name differ.
#
# It refuses to overwrite an existing ca.key or server.key (exit 1). The CSR, the serial file and the
# extension file live in a temporary directory that is removed on exit. It prints the server name and
# the SHA-256 fingerprints of the two certificates, never key material.
set -euo pipefail

DEFAULT_NAME="dial-home.hpcburst.invalid"
TMP_DIR=""

cleanup() {
  if [ -n "$TMP_DIR" ]; then rm -rf "$TMP_DIR"; fi
}

main() {
  [ "$#" -ge 1 ] && [ "$#" -le 2 ] || { echo "usage: make-test-ca.sh DIR [SERVER_NAME]" >&2; exit 2; }
  local dir="$1" name="${2:-$DEFAULT_NAME}" tmp
  command -v openssl >/dev/null 2>&1 || { echo "make-test-ca: openssl is required" >&2; exit 2; }
  # The name goes into an extension file, so accept only what a DNS name is made of.
  [[ "$name" =~ ^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$ ]] || {
    echo "make-test-ca: bad SERVER_NAME '$name' (letters, digits, dots and hyphens only)" >&2; exit 2; }
  for f in ca.key server.key; do
    [ ! -e "$dir/$f" ] || { echo "make-test-ca: $dir/$f exists; refusing to overwrite it" >&2; exit 1; }
  done

  umask 077
  [ -d "$dir" ] || mkdir -m 700 -p "$dir"
  trap cleanup EXIT
  TMP_DIR="$(mktemp -d "$dir/.tmp.XXXXXX")"
  tmp="$TMP_DIR"

  # CA: self-signed, usable only to sign certificates.
  openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$tmp/ca.key"
  openssl req -x509 -new -key "$tmp/ca.key" -sha256 -days 30 -subj "/CN=hpc-burst S1 test CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -addext "subjectKeyIdentifier=hash" \
    -out "$tmp/ca.crt"

  # Server certificate: the key ids are explicit because Python 3.13+ verifies in strict X.509 mode.
  openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$tmp/server.key"
  openssl req -new -key "$tmp/server.key" -subj "/CN=${name}" -out "$tmp/server.csr"
  cat > "$tmp/server.ext" <<EOF
basicConstraints=CA:FALSE
extendedKeyUsage=serverAuth
subjectAltName=DNS:${name},IP:127.0.0.1
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid:always
EOF
  openssl x509 -req -in "$tmp/server.csr" -CA "$tmp/ca.crt" -CAkey "$tmp/ca.key" -CAserial "$tmp/ca.srl" \
    -CAcreateserial -days 30 -sha256 -extfile "$tmp/server.ext" -out "$tmp/server.crt"

  chmod 600 "$tmp/ca.key" "$tmp/server.key"
  chmod 644 "$tmp/ca.crt" "$tmp/server.crt"
  mv "$tmp/ca.key" "$tmp/ca.crt" "$tmp/server.key" "$tmp/server.crt" "$dir/"

  echo "server name: ${name}"
  echo "ca.crt     $(openssl x509 -in "$dir/ca.crt" -noout -fingerprint -sha256 | sed 's/^.*=//')"
  echo "server.crt $(openssl x509 -in "$dir/server.crt" -noout -fingerprint -sha256 | sed 's/^.*=//')"
  echo "wrote ca.crt ca.key server.crt server.key in ${dir}"
}

main "$@"
