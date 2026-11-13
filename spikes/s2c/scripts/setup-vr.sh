#!/usr/bin/env bash
# setup-vr.sh: vllm-router 0.1.17 in a venv, for the dry run on an x86_64 host with CPython 3.15.
#
#   S2C_VR_HOME=<scratch dir> spikes/s2c/scripts/setup-vr.sh     # prints the vllm-router path on success
#
# The wheel and its sha256 are spike S13's pins (spikes/s13/scripts/setup.sh), the launcher's
# dependencies S13's hash-pinned list (spikes/s13/scripts/launcher-deps.txt, CPython 3.15, x86_64),
# installed with --require-hashes --no-deps. Idempotent. Athena's compute node needs its own venv for
# its own Python: a pre-run staging step with its own go (README, Running it live), not this script.
set -euo pipefail
S2C_VR_HOME=${S2C_VR_HOME:?set S2C_VR_HOME to a scratch directory}
HERE=$(cd "$(dirname "$0")" && pwd)
DEPS=$HERE/../../s13/scripts/launcher-deps.txt
URL=https://files.pythonhosted.org/packages/26/99/48e47ba5847cbb3c3fabc65973a8de7c00ad429f29c3aace954b56771431/vllm_router-0.1.17-cp38-abi3-manylinux_2_28_x86_64.whl
SHA=85c96bea4d573a49b5a8ae4c72ee3dfe733aba26b6f1ff1256e6142f1f42694d
[[ $(uname -m) == x86_64 ]] || { echo "setup-vr.sh: the pinned wheel is x86_64" >&2; exit 1; }
export PIP_CACHE_DIR=$S2C_VR_HOME/pipcache PIP_DISABLE_PIP_VERSION_CHECK=1
mkdir -p "$S2C_VR_HOME/dl/deps"
wheel=$S2C_VR_HOME/dl/$(basename "$URL")
if [[ ! -f $wheel ]]; then curl -sSfL -o "$wheel.part" "$URL"; mv "$wheel.part" "$wheel"; fi
echo "$SHA  $wheel" | sha256sum -c --quiet - || { echo "setup-vr.sh: $wheel is not the pinned $SHA" >&2; exit 1; }
venv=$S2C_VR_HOME/venv-0.1.17
if [[ ! -x $venv/bin/vllm-router ]]; then
  python3 -m venv "$venv"
  "$venv/bin/python" -m pip download --quiet --no-deps --require-hashes --dest "$S2C_VR_HOME/dl/deps" -r "$DEPS"
  "$venv/bin/python" -m pip install --quiet --no-index --no-deps --require-hashes --find-links "$S2C_VR_HOME/dl/deps" -r "$DEPS"
  "$venv/bin/python" -m pip install --quiet --no-index --no-deps "$wheel"
fi
got=$("$venv/bin/python" -I -c 'import importlib.metadata as m; print(m.version("vllm-router"))')
[[ $got == 0.1.17 ]] || { echo "setup-vr.sh: $venv holds vllm-router $got, not 0.1.17" >&2; exit 1; }
echo "$venv/bin/vllm-router"
