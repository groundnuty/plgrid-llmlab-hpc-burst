#!/usr/bin/env python3
"""Verify the golden vectors of pkg/deploymentid with a library written elsewhere.

The committed canonical.json files are the fixtures. This script never writes
them. For each vectors/<name>/ directory it reads expected-object.json (the v1
object, written by hand from HBEP-3's rules), canonicalises it with the
rfc8785 PyPI package (Trail of Bits, Apache-2.0), and checks that

  - the bytes equal the committed canonical.json;
  - sha256.txt names canonical.json and holds the SHA-256 of those bytes;
  - every key of the object is ASCII (HBEP-3), and for a vector with a
    tokenizer_config.json, chat_template_sha256 is the SHA-256 of its
    "default" template, taken with Python's own JSON and hashing.

It exits 1 on any mismatch. No code of the Go package is involved.

Usage (the version that checked the committed files is in README.md):

    python3 -m venv /tmp/venv && /tmp/venv/bin/pip install rfc8785==0.1.4
    /tmp/venv/bin/python3 verify_vectors.py [vectors-dir]
"""
import hashlib
import json
import pathlib
import sys

import rfc8785

DEFAULT_ROOT = pathlib.Path(__file__).resolve().parent / "vectors"


def default_template_sha256(tokenizer_config: pathlib.Path) -> str:
    """SHA-256 of the "default" entry of tokenizer_config.json's chat_template."""
    cfg = json.loads(tokenizer_config.read_text(encoding="utf-8"))
    template = cfg["chat_template"]
    if isinstance(template, list):
        template = next(t["template"] for t in template if t["name"] == "default")
    return hashlib.sha256(template.encode("utf-8")).hexdigest()


def keys(obj):
    """Every object key in obj, at any depth."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield k
            yield from keys(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from keys(v)


def problems(vec: pathlib.Path) -> list[str]:
    obj = json.loads((vec / "expected-object.json").read_text(encoding="utf-8"))
    committed = (vec / "canonical.json").read_bytes()
    out = []
    if rfc8785.dumps(obj) != committed:
        out.append("rfc8785 gives other bytes than canonical.json")
    digest = hashlib.sha256(committed).hexdigest()
    if (vec / "sha256.txt").read_text(encoding="ascii") != f"{digest}  canonical.json\n":
        out.append(f"sha256.txt is not '{digest}  canonical.json'")
    if not all(k.isascii() for k in keys(obj)):
        out.append("a key of the object is not ASCII")
    cfg = vec / "tokenizer_config.json"
    if cfg.exists() and obj["chat_template_sha256"] != default_template_sha256(cfg):
        out.append("chat_template_sha256 is not the SHA-256 of the default template")
    return out


def main() -> int:
    root = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_ROOT
    failed = 0
    for vec in sorted(p for p in root.iterdir() if p.is_dir()):
        bad = problems(vec)
        failed += bool(bad)
        print(f"{'MISMATCH' if bad else 'match   '} {vec.name}", *bad, sep="\n    " if bad else "")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
