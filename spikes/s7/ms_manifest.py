#!/usr/bin/env python3
"""Build a model_signing file-mode manifest of a checkpoint directory and measure the cost.

Needs Python 3.9+ and model-signing (https://github.com/sigstore/model-transparency,
Apache-2.0). On Helios that is `module load Python/3.11.5` plus the wheels in PYTHONPATH.

HBEP-9's file mode: a per-file SHA-256 manifest over an explicit list of files
(`files_to_hash`), with no root digest involved. Three library behaviours matter here:
  * symlinks are refused unless Config.set_allow_symlinks(True) is called on the Config;
    passing allow_symlinks=True to use_file_serialization() is overridden by Config.hash(),
    so a Hugging Face cache snapshot (all symlinks) fails without the explicit call;
  * the paths .git/, .gitattributes, .github/ and .gitignore are ignored by default, so a
    Hub file named .gitattributes never appears in the manifest;
  * chunk_size=0 means one read() of the whole file. We pass a fixed chunk size.

Output: one JSON file with the serialization parameters, one resource per file
(name, algorithm, digest) and the cost of the pass (wall, CPU, /proc/self/io deltas).
"""
import argparse
import json
import os
import pathlib
import resource
import sys
import time


def read_proc_io():
    out = {}
    with open("/proc/self/io") as f:
        for line in f:
            key, _, value = line.partition(":")
            out[key.strip()] = int(value)
    return out


def list_files(root):
    found = []
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        for name in filenames:
            found.append(pathlib.Path(dirpath) / name)
    return sorted(found)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", required=True)
    ap.add_argument("--label", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--chunk", type=int, default=8 * 1024 * 1024)
    args = ap.parse_args(argv)
    if args.chunk <= 0:
        sys.exit("--chunk must be positive: 0 would read each whole file in one call")

    import model_signing  # noqa: F401  (import first so its start-up cost is not timed)
    from model_signing import hashing

    root = pathlib.Path(args.root)
    files = list_files(root)
    cfg = hashing.Config().use_file_serialization(
        hashing_algorithm="sha256", chunk_size=args.chunk, max_workers=args.workers)
    cfg.set_allow_symlinks(True)

    io0, ru0, t0 = read_proc_io(), resource.getrusage(resource.RUSAGE_SELF), time.perf_counter()
    manifest = cfg.hash(root, files_to_hash=files)
    wall = time.perf_counter() - t0
    io1, ru1 = read_proc_io(), resource.getrusage(resource.RUSAGE_SELF)

    resources = [{"name": r.identifier, "algorithm": r.digest.algorithm, "digest": r.digest.digest_hex}
                 for r in manifest.resource_descriptors()]
    result = {
        "label": args.label,
        "model_signing_version": getattr(model_signing, "__version__", None),
        "serialization": manifest.serialization_type,
        "files_offered": len(files),
        "resources": resources,
        "wall_s": wall,
        "cpu_s": (ru1.ru_utime + ru1.ru_stime) - (ru0.ru_utime + ru0.ru_stime),
        "rchar": io1["rchar"] - io0["rchar"],
        "read_bytes": io1["read_bytes"] - io0["read_bytes"],
        "workers": args.workers,
        "chunk": args.chunk,
    }
    with open(args.out, "w") as f:
        json.dump(result, f, indent=1, sort_keys=True)
    print(json.dumps({k: v for k, v in result.items() if k != "resources"}, sort_keys=True))


if __name__ == "__main__":
    main()
