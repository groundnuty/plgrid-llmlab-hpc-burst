#!/usr/bin/env bash
# Spike S12: reproduce the SBOM comparison and the licence check (HBEP-15, open question 4).
#
# Usage: spikes/s12/reproduce.sh <work dir>      (the work dir must be empty or not exist;
#                                                 it needs about 500 MB, mostly the tools)
# Needs: go (the version go.mod asks for), curl, tar, sha256sum, python3 and uvx (or pipx; set
# PYSPDX="pipx run --spec spdx-tools==0.8.5 pyspdxtools" to use pipx instead).
# Optional: CHECK_SPIKES=1 also runs the licence check in each spikes/*/ module (downloads
# their dependencies).
#
# Everything the script writes for reading goes to <work dir>/results. Exit status: 0 when both
# SBOMs validate, match "go version -m", and the licence check passes for everything except the
# repository's own packages; 1 otherwise.
set -euo pipefail

# ---- pins -------------------------------------------------------------------------------
BOM_VERSION=v0.8.0
BOM_SHA256=cafd00b75a8df860fec54d19b9e24fb48556c0848d0abf9e40cd2eabb11a8710           # bom-amd64-linux
SYFT_VERSION=1.55.0
SYFT_SHA256=bb65249542db5fedeeb4d3f9b018b75e7264dbfbbb10486f044581a92f90490d           # syft_1.55.0_linux_amd64.tar.gz
SPDX_TOOLS_VERSION=0.8.5
GO_LICENSES_VERSION=v2.0.1
CHECK_JSONSCHEMA_VERSION=0.39.0
SPDX_SCHEMA_URL=https://raw.githubusercontent.com/spdx/spdx-spec/v2.3/schemas/spdx-schema.json
SPDX_SCHEMA_SHA256=239208b7ac287b3cf5d9a9af23f9d69863971102a5e1587a27a398b43490b89b
# ------------------------------------------------------------------------------------------

here=$(cd "$(dirname "$0")" && pwd)
root=$(git -C "$here" rev-parse --show-toplevel)
W=${1:?usage: $0 <work dir>}
[ -n "$(ls -A "$W" 2>/dev/null)" ] && { echo "work dir $W is not empty" >&2; exit 2; }
mkdir -p "$W"/{bin,dl-bom,dl-syft,gobin,results,uv}
W=$(cd "$W" && pwd)
R=$W/results
export SYFT_CHECK_FOR_APP_UPDATE=false XDG_CACHE_HOME=$W/uv UV_CACHE_DIR=$W/uv/cache UV_TOOL_DIR=$W/uv/tools UV_PYTHON_INSTALL_DIR=$W/uv/python UV_PYTHON_DOWNLOADS=never
PYSPDX=${PYSPDX:-uvx --from spdx-tools==$SPDX_TOOLS_VERSION pyspdxtools}
status=0
step() { printf '\n##### %s\n' "$*"; }

fetch() { # url dest expected-sha256
  curl -fsSL "$1" -o "$2"
  echo "$3  $2" | sha256sum -c - >/dev/null || { echo "sha256 mismatch for $2" >&2; exit 1; }
}

step "1. Build hpcburst from the checked-out commit, and take the reference from the toolchain"
# Go stamps "+dirty" on the main module's version when the tree has uncommitted or untracked
# files. bom then writes the main module without a version (see the README), so compare.py fails.
[ -z "$(git -C "$root" status --porcelain)" ] || echo "WARNING: the working tree is not clean; the binary's version will end in +dirty"
(cd "$root" && go build -trimpath -o "$W/bin/hpcburst" ./cmd/hpcburst)
# Run from the binary's directory with a relative name, so no absolute path lands in the output.
(cd "$W/bin" && go version -m hpcburst) > "$R/go-version-m.txt"
echo "$(grep -c -P '^\tdep\t' "$R/go-version-m.txt") dependencies in the reference; $(head -1 "$R/go-version-m.txt")"

step "2. Download bom $BOM_VERSION and syft $SYFT_VERSION, verify sha256"
fetch "https://github.com/kubernetes-sigs/bom/releases/download/$BOM_VERSION/bom-amd64-linux" "$W/dl-bom/bom" "$BOM_SHA256"
# Also compare with the checksum file the release publishes (the pin above is the same value).
curl -fsSL "https://github.com/kubernetes-sigs/bom/releases/download/$BOM_VERSION/checksums.txt" |
  grep -q "^$BOM_SHA256  bom-amd64-linux$" || { echo "pin differs from the release's checksums.txt" >&2; exit 1; }
fetch "https://github.com/anchore/syft/releases/download/v$SYFT_VERSION/syft_${SYFT_VERSION}_linux_amd64.tar.gz" \
  "$W/dl-syft/syft.tar.gz" "$SYFT_SHA256"
curl -fsSL "https://github.com/anchore/syft/releases/download/v$SYFT_VERSION/syft_${SYFT_VERSION}_checksums.txt" |
  grep -q "^$SYFT_SHA256  syft_${SYFT_VERSION}_linux_amd64.tar.gz$" || { echo "pin differs from the release's checksums" >&2; exit 1; }
tar -xzf "$W/dl-syft/syft.tar.gz" -C "$W/dl-syft" syft && rm "$W/dl-syft/syft.tar.gz"
chmod +x "$W/dl-bom/bom"
"$W/dl-bom/bom" version 2>&1 | grep -E 'GitVersion' ; "$W/dl-syft/syft" version | grep -E '^Version'

step "3. Generate SPDX 2.3 SBOMs of the binary"
(cd "$W/bin" &&
  "$W/dl-bom/bom" generate --file hpcburst --format json --name hpcburst -o "$R/bom-binary.spdx.json" 2>&1 | tail -1 &&
  "$W/dl-syft/syft" scan file:hpcburst --source-name hpcburst -o "spdx-json@2.3=$R/syft-binary.spdx.json" 2>&1 | tail -1)
for t in bom syft; do printf '%s: spdxVersion=%s\n' "$t" "$(python3 -I -c "import json,sys; print(json.load(open(sys.argv[1]))['spdxVersion'])" "$R/$t-binary.spdx.json")"; done

step "4. Validate with pyspdxtools $SPDX_TOOLS_VERSION (and prove it can fail)"
{
  for t in bom syft; do
    if $PYSPDX -i "$R/$t-binary.spdx.json"; then echo "$t-binary.spdx.json: valid (exit 0)"; else echo "$t-binary.spdx.json: INVALID"; status=1; fi
  done
  # Negative control: the same document with a required field removed must be rejected.
  python3 -I - "$R/bom-binary.spdx.json" "$W/broken.spdx.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1])); del d["documentNamespace"]; json.dump(d, open(sys.argv[2], "w"))
PY
  if $PYSPDX -i "$W/broken.spdx.json" >/dev/null 2>&1; then echo "NEGATIVE CONTROL FAILED: a document without documentNamespace was accepted"; status=1
  else echo "negative control: a document without documentNamespace is rejected, as it should be"; fi
  # Second validator: the official SPDX 2.3 JSON schema. It is looser than pyspdxtools: see the README.
  fetch "$SPDX_SCHEMA_URL" "$W/spdx-schema-2.3.json" "$SPDX_SCHEMA_SHA256"
  for t in bom syft; do
    if uvx --from "check-jsonschema==$CHECK_JSONSCHEMA_VERSION" check-jsonschema --schemafile "$W/spdx-schema-2.3.json" "$R/$t-binary.spdx.json" >/dev/null 2>&1
    then echo "$t-binary.spdx.json: valid against the SPDX 2.3 JSON schema"; else echo "$t-binary.spdx.json: INVALID against the SPDX 2.3 JSON schema"; status=1; fi
  done
  python3 -I - "$R/bom-binary.spdx.json" "$W/broken-version.spdx.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1])); del d["spdxVersion"]; json.dump(d, open(sys.argv[2], "w"))
PY
  if uvx --from "check-jsonschema==$CHECK_JSONSCHEMA_VERSION" check-jsonschema --schemafile "$W/spdx-schema-2.3.json" "$W/broken-version.spdx.json" >/dev/null 2>&1
  then echo "NEGATIVE CONTROL FAILED: the schema accepted a document without spdxVersion"; status=1
  else echo "negative control: the schema rejects a document without spdxVersion"; fi
  if uvx --from "check-jsonschema==$CHECK_JSONSCHEMA_VERSION" check-jsonschema --schemafile "$W/spdx-schema-2.3.json" "$W/broken.spdx.json" >/dev/null 2>&1
  then echo "note: the schema accepts the document without documentNamespace that pyspdxtools rejects"; fi
} > "$R/validate.txt" 2>&1; cat "$R/validate.txt"

step "5. Compare the Go modules of each SBOM with go version -m"
if python3 -I "$here/compare.py" --reference "$R/go-version-m.txt" \
     --sbom bom="$R/bom-binary.spdx.json" --sbom syft="$R/syft-binary.spdx.json" | tee "$R/compare-binary.txt"; then :; else status=1; fi

step "6. Secondary: SBOMs of the source tree (a different population than the binary)"
mkdir -p "$W/src" "$W/tmp"
(cd "$root" && git archive HEAD -- go.mod go.sum cmd internal api pkg hack doc.go | tar -x -C "$W/src")
(cd "$W/src" && TMPDIR=$W/tmp "$W/dl-bom/bom" generate --dirs . --no-transient --format json --name hpcburst-src -o "$W/bom-source.spdx.json" >/dev/null 2>&1 &&
  "$W/dl-syft/syft" scan dir:. --source-name hpcburst-src -o "spdx-json@2.3=$W/syft-source.spdx.json" >/dev/null 2>&1)
python3 -I "$here/compare.py" --reference "$R/go-version-m.txt" \
  --sbom bom-source="$W/bom-source.spdx.json" --sbom syft-source="$W/syft-source.spdx.json" > "$R/compare-source.txt" || true
grep -E '^==|extra Go packages|wrong version|missing' "$R/compare-source.txt"

step "7. Licence check with go-licenses $GO_LICENSES_VERSION and HBEP-15's allowlist"
GOBIN=$W/gobin go install "github.com/google/go-licenses/v2@$GO_LICENSES_VERSION"
GL=$W/gobin/go-licenses
LIST=$(grep -v '^#' "$here/allowed-licenses.txt" | paste -sd, -)
# Drop go-licenses' warnings about assembly files (they carry module cache paths) and hide the cache path.
clean() { { grep -v -e "non-Go code" -e '\.s$' || true; } | sed "s#$(go env GOMODCACHE)#<modcache>#g"; }
own=github.com/groundnuty/plgrid-llmlab-hpc-burst
cat > "$W/report.tmpl" <<'EOF'
{{ range . }}{{ .Name }},{{ .Version }},{{ .LicenseURL }},{{ .LicenseName }}
{{ end }}
EOF
(cd "$root" && "$GL" report ./... --template "$W/report.tmpl" 2>/dev/null | sed '/^$/d' > "$R/go-licenses-report.csv")
rows=$(wc -l < "$R/go-licenses-report.csv")
[ "$rows" -gt 0 ] || { echo "go-licenses report is empty: a check over zero packages would also pass"; exit 1; }
chk() { # label, then the arguments of go-licenses check; sets rc
  echo; echo "== $1"; shift
  local out; rc=0
  out=$(cd "$root" && "$GL" check "$@" --allowed_licenses="$LIST" 2>&1) || rc=$?
  printf '%s\n' "$out" | clean | cut -c1-160
  echo "exit=$rc"
}
{
  echo "allowlist: $LIST"
  echo "report rows: $rows"
  chk "check ./... (our own packages included)" ./...
  chk "check ./... --ignore $own (everything we depend on)" ./... --ignore "$own"
  [ "$rc" = 0 ] || status=1
  chk "same, with --include_tests (modules only tests import)" ./... --include_tests --ignore "$own"
  # Negative control: take BSD-3-Clause off the list. The check must fail and name BSD-3-Clause packages.
  LIST=${LIST/BSD-3-Clause,/}
  chk "negative control: BSD-3-Clause taken off the list (must fail)" ./... --ignore "$own"
  [ "$rc" != 0 ] || { echo "NEGATIVE CONTROL FAILED"; status=1; }
} > "$R/go-licenses-check.txt" 2>&1; cat "$R/go-licenses-check.txt"
LIST=$(grep -v '^#' "$here/allowed-licenses.txt" | paste -sd, -)

step "8. Separate modules: are third_party/forked/* and the spikes covered by ./... ?"
{
  echo "packages under spikes/ or third_party/ matched by ./... at the root: $(cd "$root" && go list ./... | grep -c -E '/(spikes|third_party)/' || true)"
  for mod in $(cd "$root" && find third_party spikes -name go.mod -printf '%h\n' | sort); do
    case $mod in spikes/*) [ "${CHECK_SPIKES:-0}" = 1 ] || { echo "$mod: skipped (CHECK_SPIKES=1 runs it)"; continue; };; esac
    n=$(cd "$root/$mod" && "$GL" report ./... --template "$W/report.tmpl" 2>/dev/null | sed '/^$/d' | wc -l)
    rc=0; out=$(cd "$root/$mod" && "$GL" check ./... --allowed_licenses="$LIST" 2>&1) || rc=$?
    echo "$mod: report rows=$n, check exit=$rc"
    # The packages of the module itself have no LICENSE above them; list anything else separately.
    printf '%s\n' "$out" | clean | { grep -v -e '^$' -e "$own/" || true; } | sed 's/^/    NOT OUR OWN PACKAGE: /' | head -20
  done
} > "$R/go-licenses-modules.txt" 2>&1; cat "$R/go-licenses-modules.txt"

step "9. Fixtures: an off-list licence and an unknown licence, both written by hand"
"$here/licence-fixtures.sh" "$GL" "$W/fixture-work" 2>&1 | tee "$R/licence-fixtures.txt" || status=1

step "done"
echo "results are in $R; exit status $status"
exit "$status"
