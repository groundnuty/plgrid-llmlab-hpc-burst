#!/usr/bin/env python3
"""Compute the golden vectors of pkg/deploymentid outside the Go package.

For each vectors/<name>/ directory this reads expected-object.json, the v1
object written by hand from HBEP-3's rules, canonicalises it with the
rfc8785 PyPI package (Trail of Bits, Apache-2.0) and hashes the bytes with
hashlib. It writes canonical.json (the exact bytes, no trailing newline) and
sha256.txt (in `sha256sum` format). No code of the Go package is involved:
the Go tests read these files and compare.

Usage (the version that produced the checked-in files is in README.md):

    python3 -m venv /tmp/venv && /tmp/venv/bin/pip install rfc8785==0.1.4
    /tmp/venv/bin/python3 gen_vectors.py
"""
import hashlib
import json
import pathlib
import sys

import rfc8785

ROOT = pathlib.Path(__file__).resolve().parent / "vectors"
EMPTY_TEMPLATE_SHA256 = hashlib.sha256(b"").hexdigest()


def default_template_sha256(tokenizer_config: pathlib.Path) -> str:
    """SHA-256 of the "default" entry of tokenizer_config.json's chat_template."""
    cfg = json.loads(tokenizer_config.read_text(encoding="utf-8"))
    template = cfg["chat_template"]
    if isinstance(template, list):
        template = next(t["template"] for t in template if t["name"] == "default")
    return hashlib.sha256(template.encode("utf-8")).hexdigest()


def utf16_differs_from_codepoint_order(obj) -> bool:
    """True if some object in obj has keys that UTF-16 order sorts differently
    from code point order (Python's sort_keys), i.e. the vector needs JCS."""
    if isinstance(obj, dict):
        keys = list(obj)
        by_codepoint = sorted(keys)
        by_utf16 = sorted(keys, key=lambda k: k.encode("utf-16-be"))
        if by_codepoint != by_utf16:
            return True
        return any(utf16_differs_from_codepoint_order(v) for v in obj.values())
    if isinstance(obj, list):
        return any(utf16_differs_from_codepoint_order(v) for v in obj)
    return False


def main() -> int:
    for vec in sorted(p for p in ROOT.iterdir() if p.is_dir()):
        obj = json.loads((vec / "expected-object.json").read_text(encoding="utf-8"))
        canonical = rfc8785.dumps(obj)
        assert json.loads(canonical) == obj, vec.name
        cfg = vec / "tokenizer_config.json"
        if cfg.exists():
            want = default_template_sha256(cfg)
            assert obj["chat_template_sha256"] == want, (vec.name, want)
        (vec / "canonical.json").write_bytes(canonical)
        digest = hashlib.sha256(canonical).hexdigest()
        (vec / "sha256.txt").write_text(f"{digest}  canonical.json\n", encoding="ascii")
        note = " (UTF-16 key order differs from code point order)" if utf16_differs_from_codepoint_order(obj) else ""
        print(f"{vec.name}: {len(canonical)} bytes, {digest}{note}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
