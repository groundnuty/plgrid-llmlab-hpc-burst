#!/usr/bin/env bash
# Tests that setup.sh and buildstamp.sh never reuse a binary that is not built from the pinned,
# patched sources. Nothing is compiled: a stub go on PATH records each build request and writes a marker.
#
#   test-setup.sh <dir with the pinned clones llm-d-router and llm-d-inference-sim>   (for example $S2_HOME/src)
#
# The clones are copied (about 30 MB) into a temporary S2_HOME and reset to the pinned commit, so the
# real S2_HOME is not touched. Network use: none.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
FROM=${1:?usage: test-setup.sh <dir with pinned clones of llm-d-router and llm-d-inference-sim>}
T=$(mktemp -d "${TMPDIR:-/tmp}/s2-test-setup.XXXXXX"); trap 'rm -rf "$T"' EXIT
fails=0
check() { # check <description> <command...>
  local d=$1; shift
  if "$@"; then echo "ok   $d"; else echo "FAIL $d"; fails=$((fails + 1)); fi
}
builds() { grep -c . "$T/stub.log" 2>/dev/null || true; }

mkdir -p "$T/stub" "$T/home/src" "$T/home/bin"
cat > "$T/stub/go" <<'STUB'
#!/usr/bin/env bash
echo "$PWD $*" >> "$STUB_LOG"
[[ ${STUB_FAIL:-0} == 1 ]] && exit 1
out=; while [[ $# -gt 0 ]]; do [[ $1 == -o ]] && { out=$2; shift; }; shift; done
echo "built-by-stub" > "$out"; chmod +x "$out"
STUB
chmod +x "$T/stub/go"
export STUB_LOG="$T/stub.log" PATH="$T/stub:$PATH"
: > "$STUB_LOG"

echo "== end to end: setup.sh patched, with unpatched binaries left over from an earlier S2_HOME"
for r in llm-d-router llm-d-inference-sim; do
  cp -a "$FROM/$r" "$T/home/src/$r" && git -C "$T/home/src/$r" checkout --quiet -- . || { echo "cannot prepare $r"; exit 2; }
done
printf '#!/bin/sh\necho unpatched-old\n' | tee "$T/home/bin/epp" > "$T/home/bin/llm-d-inference-sim"; chmod +x "$T"/home/bin/*
check "sources start unpatched" test -z "$(git -C "$T/home/src/llm-d-router" diff HEAD --stat)"
S2_HOME="$T/home" "$HERE/setup.sh" patched > "$T/setup1.out" 2>&1; rc=$?
check "setup.sh patched exits 0" test $rc -eq 0
check "both existing binaries were rebuilt, not reused" test "$(builds)" -eq 2
check "the simulator binary is the rebuilt one" grep -q built-by-stub "$T/home/bin/llm-d-inference-sim"
check "the EPP binary is the rebuilt one" grep -q built-by-stub "$T/home/bin/epp"
check "stamps were written" test -s "$T/home/bin/epp.stamp" -a -s "$T/home/bin/llm-d-inference-sim.stamp"
check "the sources got the loopback patch and the notice" grep -q 'Modified from upstream' "$T/home/src/llm-d-router/internal/runnable/grpc.go" "$T/home/src/llm-d-inference-sim/pkg/communication/http.go"
check "the EPP source binds 127.0.0.1" grep -q '"127.0.0.1:%d"' "$T/home/src/llm-d-router/internal/runnable/grpc.go"
S2_HOME="$T/home" "$HERE/setup.sh" patched > "$T/setup2.out" 2>&1
check "a second run with matching stamps builds nothing" test "$(builds)" -eq 2
echo "// edited" >> "$T/home/src/llm-d-router/internal/runnable/grpc.go"
S2_HOME="$T/home" "$HERE/setup.sh" patched > "$T/setup3.out" 2>&1
check "an edited source tree rebuilds the EPP only" test "$(builds)" -eq 3
S2_HOME="$T/home" STUB_FAIL=1 "$HERE/setup.sh" patched > /dev/null 2>&1
check "with every stamp current, go is not invoked even when it would fail" test "$(builds)" -eq 3
rm -f "$T/home/bin/epp.stamp"
S2_HOME="$T/home" STUB_FAIL=1 "$HERE/setup.sh" patched > "$T/setup4.out" 2>&1; rc=$?
check "a failing build makes setup.sh fail" test $rc -ne 0
check "a failed build leaves no stamp" test ! -e "$T/home/bin/epp.stamp"
check "a stamp-less binary is rebuilt on the next run" bash -c "S2_HOME='$T/home' '$HERE/setup.sh' patched >/dev/null 2>&1 && test -s '$T/home/bin/epp.stamp'"

echo "== unit: what makes a stamp stale"
. "$HERE/buildstamp.sh"
R="$T/repo"; mkdir -p "$R" && git -C "$R" init -q && echo "a" > "$R/f.txt" && git -C "$R" add f.txt \
  && git -C "$R" -c user.name=t -c user.email=t@t commit -qm one
printf 'patch one\n' > "$T/p1"; printf 'patch two\n' > "$T/p2"
BIN="$T/bin-x"; : > "$STUB_LOG"
build_patched "$R" "$T/p1" "$BIN" ./x > /dev/null; check "first build" test "$(builds)" -eq 1
build_patched "$R" "$T/p1" "$BIN" ./x > /dev/null; check "same commit, patch and tree: no build" test "$(builds)" -eq 1
build_patched "$R" "$T/p2" "$BIN" ./x > /dev/null; check "different patch contents: rebuild" test "$(builds)" -eq 2
echo "b" > "$R/f.txt"
build_patched "$R" "$T/p2" "$BIN" ./x > /dev/null; check "edited tree: rebuild" test "$(builds)" -eq 3
git -C "$R" -c user.name=t -c user.email=t@t commit -qam two
build_patched "$R" "$T/p2" "$BIN" ./x > /dev/null; check "different source commit: rebuild" test "$(builds)" -eq 4
rm -f "$BIN"
build_patched "$R" "$T/p2" "$BIN" ./x > /dev/null; check "stamp without a binary: rebuild" test "$(builds)" -eq 5

echo; if [[ $fails -eq 0 ]]; then echo "test-setup: all passed"; else echo "test-setup: $fails FAILED"; fi
exit $((fails > 0))
