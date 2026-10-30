#!/usr/bin/env python3
"""Compare staged-file hashes with the Hugging Face Hub's published hashes.

Inputs: the Hub listing written by fetch_ref.py, and the JSON-lines file written by
hash_tree.py. For each path in the union of both:
  lfs-ok / lfs-MISMATCH   Hub has lfs.sha256: compared with the SHA-256 of the staged content
  git-ok / git-MISMATCH   non-LFS file: Hub's `oid` compared with the git blob SHA-1 of the content
  unchecked               present on both sides, but neither hash is available for it
  size-MISMATCH           sizes differ (reported before any hash comparison)
  not-staged              on the Hub, absent from the staged directory
  not-on-hub              staged, absent from the Hub listing at this revision

Nothing here reads the hash from a blob name: hash_tree.py hashes content only.
"""
import argparse
import json
import sys


def load_jsonl(path):
    rows, summary = [], None
    with open(path) as f:
        for line in f:
            rec = json.loads(line)
            if rec.get("kind") == "summary":
                summary = rec
            else:
                rows.append(rec)
    return rows, summary


def classify(hub_file, row):
    """Return the verdict for one path present in the Hub listing and/or the staged rows."""
    if row is None:
        return "not-staged"
    if hub_file is None:
        return "not-on-hub"
    if "error" in row:
        return "read-error"
    if hub_file["size"] != row["size"]:
        return "size-MISMATCH"
    if hub_file["lfs_sha256"]:
        return "lfs-ok" if hub_file["lfs_sha256"] == row["sha256"] else "lfs-MISMATCH"
    if row.get("git_blob_sha1"):
        return "git-ok" if hub_file["git_oid"] == row["git_blob_sha1"] else "git-MISMATCH"
    return "unchecked"


def compare(hub, rows):
    hub_by_path = {f["path"]: f for f in hub["files"]}
    row_by_path = {r["path"]: r for r in rows}
    results = []
    for path in sorted(set(hub_by_path) | set(row_by_path)):
        h, r = hub_by_path.get(path), row_by_path.get(path)
        results.append({"path": path, "verdict": classify(h, r),
                        "size": (r or h)["size"]})
    counts = {}
    for item in results:
        counts[item["verdict"]] = counts.get(item["verdict"], 0) + 1
    ok = {"lfs-ok", "git-ok"}
    return {
        "repo": hub["repo"], "revision": hub["revision"],
        "hub_files": len(hub_by_path), "staged_files": len(row_by_path),
        "counts": counts,
        "all_match": all(i["verdict"] in ok for i in results),
        "lfs_bytes_matched": sum(i["size"] for i in results if i["verdict"] == "lfs-ok"),
        "files": results,
    }


def compare_manifest(rows, manifest):
    """Check a model_signing manifest against hash_tree.py's SHA-256 values for the same files."""
    sha_by_path = {r["path"]: r["sha256"] for r in rows if "sha256" in r}
    only_manifest, differ, same = [], [], 0
    for res in manifest["resources"]:
        if res["algorithm"] != "sha256":
            return {"error": "algorithm %r" % res["algorithm"]}
        mine = sha_by_path.get(res["name"])
        if mine is None:
            only_manifest.append(res["name"])
        elif mine == res["digest"]:
            same += 1
        else:
            differ.append(res["name"])
    in_manifest = set(res["name"] for res in manifest["resources"])
    return {"equal": same, "differ": differ, "only_in_manifest": only_manifest,
            "not_in_manifest": sorted(set(sha_by_path) - in_manifest)}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--hub", required=True)
    ap.add_argument("--hashes", required=True)
    ap.add_argument("--manifest")
    ap.add_argument("--out", required=True)
    args = ap.parse_args(argv)
    hub = json.load(open(args.hub))
    rows, _ = load_jsonl(args.hashes)
    report = compare(hub, rows)
    if args.manifest:
        report["model_signing_vs_own"] = compare_manifest(rows, json.load(open(args.manifest)))
    with open(args.out, "w") as f:
        json.dump(report, f, indent=1, sort_keys=True)
    print(json.dumps({k: v for k, v in report.items() if k != "files"}, sort_keys=True))
    return 0 if report["all_match"] else 1


if __name__ == "__main__":
    sys.exit(main())
