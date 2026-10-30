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
  duplicate-path          the path occurs twice in the staged records or in the Hub listing

A passing run needs more than matching hashes. The run must be valid (run_problems()): its
summary line exists and is complete, no file record shows a read error or a change during the
read, no entry was skipped (a directory symlink is not followed, so its files are not hashed),
neither input is empty and no path occurs twice. An invalid run fails whatever its hashes say,
and the failure is the exit status of main(). With --manifest the model_signing comparison
must pass as well (compare_manifest()).

Nothing here reads the hash from a blob name: hash_tree.py hashes content only.
"""
import argparse
import collections
import json
import sys

# What a hash_tree.py summary must carry for the run to count as finished and complete.
SUMMARY_KEYS = ("files", "errors", "changed_during_read", "skipped")
# model_signing ignores .git/, .gitattributes, .github/ and .gitignore by default, so a Hub
# file named .gitattributes is not in its manifest. This is the one documented exception.
MANIFEST_EXPECTED_ABSENT = frozenset({".gitattributes"})


def load_jsonl(path):
    """Return (records, summary). The first summary line is the summary; any further one stays
    among the records, where run_problems() reports it (a run has exactly one summary)."""
    rows, summary = [], None
    with open(path) as f:
        for line in f:
            rec = json.loads(line)
            if rec.get("kind") == "summary" and summary is None:
                summary = rec
            else:
                rows.append(rec)
    return rows, summary


def duplicates(paths):
    return sorted(p for p, n in collections.Counter(paths).items() if n > 1)


def run_problems(rows, summary):
    """Return the reasons this hash run cannot support a passing verdict; empty means usable.

    Rows published by summarize_run.publish() have no per-row changed_during_read flag, so
    the summary's list is the authority; a true row flag counts as well when it is there.
    """
    problems = []
    if summary is None:
        problems.append("no summary line: the run did not finish")
    else:
        if summary.get("mode") != "hash":
            problems.append("the summary is not from a hash pass (mode %r)" % (summary.get("mode"),))
        absent = [k for k in SUMMARY_KEYS if k not in summary]
        if absent:
            problems.append("the summary lacks %s" % ", ".join(absent))
        else:
            if summary["files"] != len(rows):
                problems.append("the summary counts %s files, the file holds %d records"
                                % (summary["files"], len(rows)))
            if summary["errors"]:
                problems.append("the summary records %d read error(s)" % len(summary["errors"]))
            if summary["changed_during_read"]:
                problems.append("%d file(s) changed during the read: %s" % (
                    len(summary["changed_during_read"]), ", ".join(summary["changed_during_read"])))
            if summary["skipped"]:
                problems.append("%d entr(ies) skipped, not hashed: %s" % (
                    len(summary["skipped"]), ", ".join(summary["skipped"])))
    if not rows:
        problems.append("no file records")
    stray = [r for r in rows if r.get("kind") != "file"]
    if stray:
        problems.append("%d record(s) that are not file records (a second summary line?)" % len(stray))
    files = [r for r in rows if r.get("kind") == "file"]
    errored = [r.get("path") for r in files if "error" in r]
    if errored:
        problems.append("read error on %d file(s): %s" % (len(errored), ", ".join(map(str, errored))))
    changed = [r.get("path") for r in files if r.get("changed_during_read")]
    if changed:
        problems.append("%d file record(s) flag a change during the read: %s" % (
            len(changed), ", ".join(map(str, changed))))
    dup = duplicates([r.get("path") for r in files])
    if dup:
        problems.append("path(s) occur more than once in the staged records: %s" % ", ".join(map(str, dup)))
    return problems


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


def compare(hub, rows, summary=None):
    """Compare the staged records with the Hub listing. `summary` is the run's summary line;
    without it (None) the run counts as unfinished and cannot pass."""
    problems = run_problems(rows, summary)
    files = [r for r in rows if r.get("kind") == "file"]
    hub_dups = duplicates([f["path"] for f in hub["files"]])
    if not hub["files"]:
        problems.append("the Hub listing is empty")
    if hub_dups:
        problems.append("path(s) occur more than once in the Hub listing: %s" % ", ".join(hub_dups))
    dup_paths = set(hub_dups) | set(duplicates([r["path"] for r in files]))
    hub_by_path = {f["path"]: f for f in hub["files"]}
    row_by_path = {r["path"]: r for r in files}
    results = []
    for path in sorted(set(hub_by_path) | set(row_by_path)):
        h, r = hub_by_path.get(path), row_by_path.get(path)
        verdict = "duplicate-path" if path in dup_paths else classify(h, r)
        # an error record has no size (hash_one failed before it could stat the file)
        results.append({"path": path, "verdict": verdict, "size": (r or {}).get("size", (h or {}).get("size"))})
    counts = {}
    for item in results:
        counts[item["verdict"]] = counts.get(item["verdict"], 0) + 1
    ok = {"lfs-ok", "git-ok"}
    return {
        "repo": hub["repo"], "revision": hub["revision"],
        "hub_files": len(hub_by_path), "staged_files": len(row_by_path),
        "counts": counts,
        "valid": not problems,
        "invalid_reasons": problems,
        "all_match": not problems and bool(results) and all(i["verdict"] in ok for i in results),
        "lfs_bytes_matched": sum(i["size"] for i in results if i["verdict"] == "lfs-ok"),
        "files": results,
    }


def compare_manifest(rows, manifest):
    """Check a model_signing manifest against hash_tree.py's SHA-256 values for the same files.

    all_match is the verdict: every manifest entry equals our SHA-256 of that file, none is
    missing from our pass or repeated, the manifest is not empty, and the only file of ours
    that the manifest may lack is the one in MANIFEST_EXPECTED_ABSENT.
    """
    resources = manifest["resources"]
    failed = {"equal": 0, "differ": [], "only_in_manifest": [], "not_in_manifest": [],
              "unexpected_not_in_manifest": [], "duplicate_in_manifest": [], "all_match": False}
    algorithms = sorted(set(res["algorithm"] for res in resources) - {"sha256"})
    if algorithms:
        return dict(failed, error="algorithm %s" % ", ".join(map(repr, algorithms)))
    sha_by_path = {r["path"]: r["sha256"] for r in rows if "sha256" in r}
    only_manifest, differ, same = [], [], 0
    for res in resources:
        mine = sha_by_path.get(res["name"])
        if mine is None:
            only_manifest.append(res["name"])
        elif mine == res["digest"]:
            same += 1
        else:
            differ.append(res["name"])
    in_manifest = set(res["name"] for res in resources)
    not_in_manifest = sorted(set(sha_by_path) - in_manifest)
    unexpected = [p for p in not_in_manifest if p not in MANIFEST_EXPECTED_ABSENT]
    repeated = duplicates([res["name"] for res in resources])
    return {"equal": same, "differ": differ, "only_in_manifest": only_manifest,
            "not_in_manifest": not_in_manifest, "unexpected_not_in_manifest": unexpected,
            "duplicate_in_manifest": repeated,
            "all_match": bool(same) and not differ and not only_manifest and not unexpected and not repeated}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--hub", required=True)
    ap.add_argument("--hashes", required=True)
    ap.add_argument("--manifest")
    ap.add_argument("--out", required=True)
    args = ap.parse_args(argv)
    with open(args.hub) as f:
        hub = json.load(f)
    rows, summary = load_jsonl(args.hashes)
    report = compare(hub, rows, summary)
    passed = report["all_match"]
    if args.manifest:
        with open(args.manifest) as f:
            report["model_signing_vs_own"] = compare_manifest(rows, json.load(f))
        passed = passed and report["model_signing_vs_own"]["all_match"]
    with open(args.out, "w") as f:
        json.dump(report, f, indent=1, sort_keys=True)
    print(json.dumps({k: v for k, v in report.items() if k != "files"}, sort_keys=True))
    for reason in report["invalid_reasons"]:
        print("compare: invalid run: %s" % reason, file=sys.stderr)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
