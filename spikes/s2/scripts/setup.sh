#!/usr/bin/env bash
# Fetch the pinned components of spike S2 into $S2_HOME and build what has no release binary.
# Idempotent. Needs: git, curl, go (>= the go.mod directive of the sources), python3 with venv.
# Builds run with nice and use whatever GOCACHE and GOMODCACHE the caller has set.
set -euo pipefail
S2_HOME=${S2_HOME:?set S2_HOME to an empty or previous work directory}
mkdir -p "$S2_HOME"/{src,bin,run,results}

pin() { # pin <repo-url> <tag> <commit> <dir>
  local url=$1 tag=$2 commit=$3 dir=$S2_HOME/src/$4
  [[ -d $dir ]] || git clone --quiet --depth 1 --branch "$tag" "$url" "$dir"
  [[ $(git -C "$dir" rev-parse HEAD) == "$commit" ]] || { echo "$4: HEAD is not the pinned commit $commit" >&2; exit 1; }
}
pin https://github.com/llm-d/llm-d-router.git        v1.0.0  b9393d1a9f1d5a72ecd86f8d2b643ab54c2a9b92 llm-d-router
pin https://github.com/llm-d/llm-d.git               v1.0.0  378b42d18dabe0b9c4ced4e6f3670d93b52db08e llm-d
pin https://github.com/llm-d/llm-d-inference-sim.git v1.0.0  7bda03f9fef85139617cb4f724dccafe1cf8cbbf llm-d-inference-sim
pin https://github.com/vllm-project/router.git       v0.1.16 db02f358da401b60d4ae14eb286163763a0705cc vllm-router

export CGO_ENABLED=0
[[ -x $S2_HOME/bin/llm-d-inference-sim ]] || (cd "$S2_HOME/src/llm-d-inference-sim" && nice -n 10 go build -o "$S2_HOME/bin/llm-d-inference-sim" ./cmd/llm-d-inference-sim)
[[ -x $S2_HOME/bin/epp ]] || (cd "$S2_HOME/src/llm-d-router" && nice -n 10 go build -o "$S2_HOME/bin/epp" ./cmd/epp)

# Envoy 1.33.2 is the version llm-d's no-Kubernetes guide pins (docker.io/envoyproxy/envoy:distroless-v1.33.2).
ENVOY_SHA=bd2ff87c5efc95fd7880d710915f573c1aab4d9c55d8da886dcd91df43fad741
if [[ ! -x $S2_HOME/bin/envoy-1.33.2 ]]; then
  curl -sSfL -o "$S2_HOME/bin/envoy-1.33.2.part" https://github.com/envoyproxy/envoy/releases/download/v1.33.2/envoy-1.33.2-linux-x86_64
  echo "$ENVOY_SHA  $S2_HOME/bin/envoy-1.33.2.part" | sha256sum -c -
  chmod +x "$S2_HOME/bin/envoy-1.33.2.part"; mv "$S2_HOME/bin/envoy-1.33.2.part" "$S2_HOME/bin/envoy-1.33.2"
fi

# vllm-router 0.1.16 from PyPI (a PyO3 wheel plus its Python launcher), in a venv.
if [[ ! -x $S2_HOME/venv/bin/vllm-router ]]; then
  python3 -m venv "$S2_HOME/venv"
  "$S2_HOME/venv/bin/python" -m pip install --quiet vllm-router==0.1.16 setproctitle aiohttp orjson uvicorn fastapi requests
fi
"$S2_HOME/venv/bin/python" -m pip list 2>/dev/null | grep -i '^vllm.router'

# The test client.
(cd "$(dirname "$0")/.." && go build -o "$S2_HOME/bin/s2probe" ./cmd/s2probe)
echo "setup ok: $(ls "$S2_HOME/bin" | tr '\n' ' ')"
