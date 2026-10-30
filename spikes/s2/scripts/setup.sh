#!/usr/bin/env bash
# Fetch the pinned components of spike S2 into $S2_HOME and build what has no release binary.
# Idempotent. Needs: git, curl, go (>= the go.mod directive of the sources), python3 with venv.
# Builds run with nice and use whatever GOCACHE and GOMODCACHE the caller has set.
#
#   setup.sh [phase...]   phases: sources patched envoy venv client (default: all, in that order)
#
# The patched simulator and EPP are rebuilt whenever their build stamp (source commit, patch file,
# patched tree) does not match, so a binary left over from an earlier S2_HOME is never reused
# unpatched. See buildstamp.sh.
set -euo pipefail
S2_HOME=${S2_HOME:?set S2_HOME to an empty or previous work directory}
HERE=$(cd "$(dirname "$0")" && pwd)
. "$HERE/buildstamp.sh"
phases=("$@"); [[ ${#phases[@]} -gt 0 ]] || phases=(sources patched envoy venv client)
for p in "${phases[@]}"; do case $p in sources|patched|envoy|venv|client) ;; *) echo "unknown phase '$p'" >&2; exit 64 ;; esac; done
want() { local p; for p in "${phases[@]}"; do [[ $p == "$1" ]] && return 0; done; return 1; }
mkdir -p "$S2_HOME"/{src,bin,run,results}

pin() { # pin <repo-url> <tag> <commit> <dir>
  local url=$1 tag=$2 commit=$3 dir=$S2_HOME/src/$4
  [[ -d $dir ]] || git clone --quiet --depth 1 --branch "$tag" "$url" "$dir"
  [[ $(git -C "$dir" rev-parse HEAD) == "$commit" ]] || { echo "$4: HEAD is not the pinned commit $commit" >&2; exit 1; }
}
pin_sim_and_epp() {
  pin https://github.com/llm-d/llm-d-router.git        v1.0.0  b9393d1a9f1d5a72ecd86f8d2b643ab54c2a9b92 llm-d-router
  pin https://github.com/llm-d/llm-d-inference-sim.git v1.0.0  7bda03f9fef85139617cb4f724dccafe1cf8cbbf llm-d-inference-sim
}
if want sources; then
  pin_sim_and_epp
  pin https://github.com/llm-d/llm-d.git               v1.0.0  378b42d18dabe0b9c4ced4e6f3670d93b52db08e llm-d
  pin https://github.com/vllm-project/router.git       v0.1.16 db02f358da401b60d4ae14eb286163763a0705cc vllm-router
fi

# Neither the simulator nor the EPP can be told which address to listen on; both bind every interface.
# A one-line-per-listener patch (third_party/forked/*/loopback-bind.patch) makes them bind 127.0.0.1.
# stack.sh checks the listener addresses of every process it starts, so an unpatched binary that
# slipped through would still be refused there.
REPO_ROOT=$(cd "$HERE/../../.." && pwd)
apply_patch() { # apply_patch <dir under src> <patch file>; idempotent
  local dir=$S2_HOME/src/$1
  if git -C "$dir" apply --check "$2" 2>/dev/null; then git -C "$dir" apply "$2"
  else git -C "$dir" apply --check --reverse "$2" || { echo "$1: loopback patch neither applies nor is applied" >&2; exit 1; }; fi
}
if want patched; then
  pin_sim_and_epp
  SIM_PATCH=$REPO_ROOT/third_party/forked/llm-d-inference-sim/loopback-bind.patch
  EPP_PATCH=$REPO_ROOT/third_party/forked/llm-d-router/loopback-bind.patch
  apply_patch llm-d-inference-sim "$SIM_PATCH"
  apply_patch llm-d-router "$EPP_PATCH"
  export CGO_ENABLED=0
  build_patched "$S2_HOME/src/llm-d-inference-sim" "$SIM_PATCH" "$S2_HOME/bin/llm-d-inference-sim" ./cmd/llm-d-inference-sim
  build_patched "$S2_HOME/src/llm-d-router" "$EPP_PATCH" "$S2_HOME/bin/epp" ./cmd/epp
fi

# Envoy 1.33.2 is the version llm-d's no-Kubernetes guide pins (docker.io/envoyproxy/envoy:distroless-v1.33.2).
ENVOY_SHA=bd2ff87c5efc95fd7880d710915f573c1aab4d9c55d8da886dcd91df43fad741
if want envoy && [[ ! -x $S2_HOME/bin/envoy-1.33.2 ]]; then
  curl -sSfL -o "$S2_HOME/bin/envoy-1.33.2.part" https://github.com/envoyproxy/envoy/releases/download/v1.33.2/envoy-1.33.2-linux-x86_64
  echo "$ENVOY_SHA  $S2_HOME/bin/envoy-1.33.2.part" | sha256sum -c -
  chmod +x "$S2_HOME/bin/envoy-1.33.2.part"; mv "$S2_HOME/bin/envoy-1.33.2.part" "$S2_HOME/bin/envoy-1.33.2"
fi

# vllm-router 0.1.16 from PyPI (a PyO3 wheel plus its Python launcher), in a venv.
if want venv && [[ ! -x $S2_HOME/venv/bin/vllm-router ]]; then
  python3 -m venv "$S2_HOME/venv"
  "$S2_HOME/venv/bin/python" -m pip install --quiet vllm-router==0.1.16 setproctitle==1.3.8 aiohttp==3.14.5 orjson==3.13.0 uvicorn==0.54.0 fastapi==0.162.0 requests==2.34.2
fi
want venv && { "$S2_HOME/venv/bin/python" -m pip list 2>/dev/null | grep -i '^vllm.router'; }

# The test client.
if want client; then (cd "$HERE/.." && go build -o "$S2_HOME/bin/s2probe" ./cmd/s2probe); fi
echo "setup ok: $(ls "$S2_HOME/bin" | tr '\n' ' ')"
