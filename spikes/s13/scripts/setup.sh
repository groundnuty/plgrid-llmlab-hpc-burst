#!/usr/bin/env bash
# Fetch the pinned pieces of spike S13 into $S13_HOME, check every hash and commit, re-run the field
# enumeration against the committed list, and build the harness. Idempotent: what is there and
# matches its pin is kept.
#
#   S13_HOME=<scratch dir> setup.sh
#
# Needs: curl, git, sha256sum, python3 with venv (the launcher dependencies are pinned for CPython
# 3.15 on x86_64, see launcher-deps.txt), go.
set -euo pipefail
S13_HOME=${S13_HOME:?set S13_HOME to a scratch directory}
HERE=$(cd "$(dirname "$0")" && pwd)
export PIP_CACHE_DIR=$S13_HOME/pipcache PIP_DISABLE_PIP_VERSION_CHECK=1
mkdir -p "$S13_HOME"/{dl/deps,bin,src}

fetch() { # fetch <url> <sha256> <file>: download unless present, then check the hash either way
  if [[ ! -f $3 ]]; then curl -sSfL -o "$3.part" "$1"; mv "$3.part" "$3"; fi
  echo "$2  $3" | sha256sum -c --quiet - || { echo "$3: sha256 is not the pinned $2" >&2; exit 1; }
}

pin() { # pin <repo-url> <tag> <commit> <dir> [sparse paths...]: a shallow clone of the tag, at that commit
  local url=$1 tag=$2 commit=$3 dir=$S13_HOME/src/$4; shift 4
  if [[ ! -d $dir ]]; then
    if [[ $# -gt 0 ]]; then
      git -c advice.detachedHead=false clone --quiet --depth 1 --branch "$tag" --filter=blob:none --sparse "$url" "$dir"
      git -C "$dir" sparse-checkout set --no-cone "$@"
    else
      git -c advice.detachedHead=false clone --quiet --depth 1 --branch "$tag" "$url" "$dir"
    fi
  fi
  [[ $(git -C "$dir" rev-parse HEAD) == "$commit" ]] || { echo "$dir: HEAD is not the pinned $commit" >&2; exit 1; }
}

# vllm-router: the PyPI wheels (a PyO3 extension holding the Rust router, plus its Python launcher).
declare -A URL SHA
URL[0.1.16]=https://files.pythonhosted.org/packages/54/c0/0bb281992cdc35b5944929794b470d5e3af37fd0ff80ce6d6da2bbcaf6e8/vllm_router-0.1.16-cp38-abi3-manylinux_2_28_x86_64.whl
SHA[0.1.16]=f400a6d7ba80ab613f28b915a7b81ba8cddcb85732470e3c9737d3430ed6d251
URL[0.1.17]=https://files.pythonhosted.org/packages/26/99/48e47ba5847cbb3c3fabc65973a8de7c00ad429f29c3aace954b56771431/vllm_router-0.1.17-cp38-abi3-manylinux_2_28_x86_64.whl
SHA[0.1.17]=85c96bea4d573a49b5a8ae4c72ee3dfe733aba26b6f1ff1256e6142f1f42694d
for v in 0.1.16 0.1.17; do
  wheel=$S13_HOME/dl/$(basename "${URL[$v]}")
  fetch "${URL[$v]}" "${SHA[$v]}" "$wheel"
  venv=$S13_HOME/venv-$v
  if [[ ! -x $venv/bin/vllm-router ]]; then
    python3 -m venv "$venv"
    "$venv/bin/python" -m pip download --quiet --no-deps --require-hashes --dest "$S13_HOME/dl/deps" -r "$HERE/launcher-deps.txt"
    "$venv/bin/python" -m pip install --quiet --no-index --no-deps --require-hashes --find-links "$S13_HOME/dl/deps" -r "$HERE/launcher-deps.txt"
    "$venv/bin/python" -m pip install --quiet --no-index --no-deps "$wheel"
  fi
  got=$("$venv/bin/python" -I -c 'import importlib.metadata as m; print(m.version("vllm-router"))')
  [[ $got == "$v" ]] || { echo "$venv holds vllm-router $got, not $v" >&2; exit 1; }
  "$venv/bin/python" -m pip check >/dev/null
done

# Sources, for the citations and the enumeration.
pin https://github.com/vllm-project/router.git v0.1.16 db02f358da401b60d4ae14eb286163763a0705cc router-0.1.16
pin https://github.com/vllm-project/router.git v0.1.17 3451e66a8ba5416d3e3e8eb3d9ee604211cde042 router-0.1.17
pin https://github.com/vllm-project/vllm.git v0.32.0 5df63a7203cdb9328d824a27b682e77c7b9928be vllm-0.32.0 \
  /vllm/entrypoints/ /vllm/renderers/ /vllm/sampling_params.py /requirements/
# openai 2.25.0, the floor vLLM v0.32.0 sets (requirements/common.txt: openai >= 2.25.0), for the
# message and content-part TypedDicts of vLLM's union.
OA=https://files.pythonhosted.org/packages/5b/e6/ca496976bacdd7f86d4fa69359c2d9b5a547d7acbf0ae5cac7bff107ff50/openai-2.25.0-py3-none-any.whl
fetch "$OA" 7c7c01a0a4b69771a29913e28d06bc1e9cee8781b4d206bb56cb946d8d2fcb23 "$S13_HOME/dl/openai-2.25.0-py3-none-any.whl"
[[ -d $S13_HOME/src/openai-2.25.0/openai ]] || python3 -I -m zipfile -e "$S13_HOME/dl/openai-2.25.0-py3-none-any.whl" "$S13_HOME/src/openai-2.25.0"

python3 -I "$HERE/enumerate_fields.py" --vllm "$S13_HOME/src/vllm-0.32.0" --openai "$S13_HOME/src/openai-2.25.0" > "$S13_HOME/fields.tsv"
cmp "$S13_HOME/fields.tsv" "$HERE/../fields/vllm-v0.32.0.tsv" || { echo "the enumeration differs from fields/vllm-v0.32.0.tsv" >&2; exit 1; }

(cd "$HERE/.." && go build -o "$S13_HOME/bin/s13fields" ./cmd/s13fields)
echo "setup ok: vllm-router 0.1.16 and 0.1.17, sources pinned, enumeration matches, harness built"
