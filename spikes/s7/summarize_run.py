#!/usr/bin/env python3
"""Turn the files of one hash_job.sh run into the tables of the README.

Usage: summarize_run.py <run_dir> <hub_ref_dir> [<publish_dir>]
  run_dir      the directory hash_job.sh wrote (hash-*.jsonl, stat-*.jsonl, sweep-*.jsonl, ms-*.json, ...)
  hub_ref_dir  holds <label>.json written by fetch_ref.py, one per checkpoint label

  publish_dir  optional: write a copy of the results that is safe to commit (see publish())

Prints Markdown. Also writes <run_dir>/compare-<label>.json for each checkpoint.

Exit status: 0 only when every checkpoint's comparison passed (compare.py: a valid run, every
file equal to the Hub, and the model_signing manifest, when there is one, equal to our hashes);
1 otherwise. With publish_dir, the sanitized copy is written in either case.
"""
import glob
import json
import os
import re
import shutil
import statistics
import sys

import compare


def mb(x):
    return "%.0f" % (x / 1e6)


def gb(x):
    return "%.1f" % (x / 1e9)


def mb_or_na(mb_per_s):
    return "n/a" if mb_per_s is None else "%.0f" % mb_per_s


def hash_tables(run, refs):
    print("### Hash pass (4 processes, 8 MiB reads, eviction from the page cache requested after each file)\n")
    print("| checkpoint | files | size GB | wall s | MB/s | CPU s | CPU busy | rchar / size | read_bytes | files changed during read |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    details, invalid = [], []
    for path in sorted(glob.glob(os.path.join(run, "hash-*.jsonl"))):
        label = os.path.basename(path)[5:-6]
        rows, s = compare.load_jsonl(path)
        problems = compare.run_problems(rows, s)
        if problems:  # no table row for a run that did not finish cleanly; compare_tables fails it
            invalid.append((label, problems))
            continue
        ok = [r for r in rows if "error" not in r]
        busy = s["cpu_s"] / (s["wall_s"] * s["workers"])
        print("| %s | %d | %s | %.1f | %s | %.1f | %.0f%% | %.4f | %s | %d |" % (
            label, s["files"], gb(s["bytes_on_disk"]), s["wall_s"], mb(s["throughput_MBps"] * 1e6), s["cpu_s"],
            100 * busy, s["rchar_sum"] / s["bytes_on_disk"], s["read_bytes_sum"], len(s["changed_during_read"])))
        big = sorted(ok, key=lambda r: -r["size"])
        thr = [r["size"] / r["wall_s"] / 1e6 for r in big if r["size"] > 100e6]
        details.append((label, big[0], statistics.median(thr) if thr else None, min(thr) if thr else None,
                        max(thr) if thr else None, s["lustre_node_wide"], s["errors"]))
    print("\nPer-file view (files over 100 MB, one process each):\n")
    print("| checkpoint | largest file GB | its wall s | per-file MB/s min / median / max |")
    print("|---|---|---|---|")
    for label, big, med, lo, hi, _, _ in details:
        print("| %s | %s | %.1f | %s / %s / %s |" % (label, gb(big["size"]), big["wall_s"], mb_or_na(lo), mb_or_na(med), mb_or_na(hi)))
    print("\nLustre client read RPC counters over the pass (node-wide, includes other jobs on the node):\n")
    for label, _, _, _, _, lustre, errors in details:
        d = lustre.get("delta")
        unavailable = json.dumps(public_record(lustre.get("after", lustre)), sort_keys=True)  # error text reduced to its type
        print("- %s: %s; read errors: %d" % (label, json.dumps(d) if d else "unavailable (%s)" % unavailable, len(errors)))
    for label, problems in invalid:
        print("- %s: INVALID RUN, not tabulated: %s" % (label, "; ".join(problems)))
    print()


def compare_tables(run, refs):
    """Print the comparison tables; return True only if every comparison passed."""
    passed = True
    print("### Staged hashes against the Hub\n")
    print("| checkpoint | repo @ revision | Hub files | staged files | lfs-ok | git-ok | MISMATCH | not-staged | not-on-hub | unchecked | LFS GB matched |")
    print("|---|---|---|---|---|---|---|---|---|---|---|")
    for path in sorted(glob.glob(os.path.join(run, "hash-*.jsonl"))):
        label = os.path.basename(path)[5:-6]
        hub = json.load(open(os.path.join(refs, label + ".json")))
        rows, summary = compare.load_jsonl(path)
        report = compare.compare(hub, rows, summary)
        ms_path = os.path.join(run, "ms-%s.json" % label)
        if os.path.exists(ms_path):
            report["model_signing_vs_own"] = compare.compare_manifest(rows, json.load(open(ms_path)))
        json.dump(report, open(os.path.join(run, "compare-%s.json" % label), "w"), indent=1, sort_keys=True)
        c = report["counts"]
        mism = sum(v for k, v in c.items() if "MISMATCH" in k)
        print("| %s | %s @ %s | %d | %d | %d | %d | %d | %d | %d | %d | %s |" % (
            label, hub["repo"], hub["revision"][:12], report["hub_files"], report["staged_files"], c.get("lfs-ok", 0),
            c.get("git-ok", 0), mism, c.get("not-staged", 0), c.get("not-on-hub", 0), c.get("unchecked", 0),
            gb(report["lfs_bytes_matched"])))
        for item in report["files"]:
            if item["verdict"] not in ("lfs-ok", "git-ok"):
                print("  - %s: %s" % (item["path"], item["verdict"]))
        for reason in report["invalid_reasons"]:
            print("  - INVALID RUN: %s" % reason)
        manifest = report.get("model_signing_vs_own")
        if manifest and not manifest["all_match"]:
            print("  - model_signing manifest does not match: %s" % json.dumps(
                {k: v for k, v in manifest.items() if v}, sort_keys=True))
        passed = passed and report["all_match"] and (manifest is None or manifest["all_match"])
    print()
    return passed


def other_tables(run):
    cpu = os.path.join(run, "cpu-ceiling.json")
    if os.path.exists(cpu):
        c = json.load(open(cpu))
        print("### CPU ceiling\n\nSHA-256 of zeroed memory, one core, 8 MiB buffer: %.0f MB/s.\n" % c["single_core_MBps"])
    print("### Single-stream read-size sweep (one process, one ~4 GB shard each)\n")
    print("| run | buffer MiB | wall s | MB/s | CPU s | rchar / size | read_bytes |")
    print("|---|---|---|---|---|---|---|")
    for path in sorted(glob.glob(os.path.join(run, "sweep-*.jsonl"))):
        rows, s = compare.load_jsonl(path)
        r = rows[0]
        print("| %s | %d | %.1f | %s | %.1f | %.4f | %s |" % (
            s["label"][6:], s["bufsize"] // 2 ** 20, r["wall_s"], mb(r["size"] / r["wall_s"]), r["cpu_s"],
            r["rchar"] / r["size"], r["read_bytes"]))
    print()
    print("### Cheap check (stat calls only; the directory listing is not in the times)\n")
    print("| checkpoint | files | stat wall s before | stat wall s after | size, mtime, ctime, inode all unchanged between passes |")
    print("|---|---|---|---|---|")
    for path in sorted(glob.glob(os.path.join(run, "stat-before-*.jsonl"))):
        label = os.path.basename(path)[12:-6]
        rows0, s0 = compare.load_jsonl(path)
        rows1, s1 = compare.load_jsonl(os.path.join(run, "stat-after-%s.jsonl" % label))
        print("| %s | %d | %.3f | %.3f | %s |" % (label, s0["files"], s0["wall_s"], s1["wall_s"], rows0 == rows1))
    print()
    probe = os.path.join(run, "probe-cheap-check.json")
    if os.path.exists(probe):
        print("### What size, mtime and ctime see (one 4 KiB file changed in place, Lustre)\n")
        print("| case | size changed | mtime changed | ctime changed | inode changed | content changed |")
        print("|---|---|---|---|---|---|")
        for case, r in sorted(json.load(open(probe)).items()):
            print("| %s | %s | %s | %s | %s | %s |" % (case, r["size_changed"], r["mtime_changed"], r["ctime_changed"],
                                                       r["ino_changed"], r["content_changed"]))
        print()
    print("### model_signing file mode (4 threads, 8 MiB chunks)\n")
    print("| checkpoint | resources | wall s | MB/s | CPU s | rchar / size | read_bytes | equal to own SHA-256 | differ | in own pass, not in manifest |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    for path in sorted(glob.glob(os.path.join(run, "ms-*.json"))):
        label = os.path.basename(path)[3:-5]
        m = json.load(open(path))
        c = json.load(open(os.path.join(run, "compare-%s.json" % label)))["model_signing_vs_own"]
        size = sum(r["size"] for r in compare.load_jsonl(os.path.join(run, "hash-%s.jsonl" % label))[0] if "size" in r)
        print("| %s | %d | %.1f | %s | %.1f | %.4f | %s | %d | %d | %s |" % (
            label, len(m["resources"]), m["wall_s"], mb(size / m["wall_s"]), m["cpu_s"], m["rchar"] / size,
            m["read_bytes"], c["equal"], len(c["differ"]), ", ".join(c["not_in_manifest"]) or "none"))
    for path in sorted(glob.glob(os.path.join(run, "ms-*.skipped"))):
        print("- %s: %s" % (os.path.basename(path), open(path).read().strip()))
    print()


PUBLIC_ROW_FIELDS = ("kind", "path", "size", "sha256", "git_blob_sha1", "bytes_hashed", "wall_s", "cpu_s",
                     "rchar", "read_bytes", "done_at_s", "changed_during_read", "error")
PRIVATE_ENV_PREFIXES = ("node=",)
# An error string is "<ExceptionClass>: <message>" (hash_tree.hash_one) or repr(exception)
# (hash_tree.lustre_counters). The message carries absolute paths and tool output; the class does not.
ERROR_CLASS = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*(?:Error|Exception|Expired))(?=[:(]|$)")
RELATIVE_PATH_LISTS = ("changed_during_read", "skipped")


def error_type(text):
    """Return the exception class name at the start of an error string, or "UnknownError"."""
    match = ERROR_CLASS.match(text) if isinstance(text, str) else None
    return match.group(1) if match else "UnknownError"


def check_relative(path):
    """Refuse a path that is not relative to the checkpoint directory."""
    if not isinstance(path, str) or os.path.isabs(path) or ".." in path.split("/"):
        raise ValueError("refusing to publish a path that is not checkpoint-relative: %r" % (path,))


def public_record(rec):
    """Return a copy of a hash_tree.py record or summary that is safe to commit.

    File records keep only PUBLIC_ROW_FIELDS (no inode, mtime, ctime or process id). Every
    "error" value, at any depth (a file record, an entry of the summary's errors list, a Lustre
    counter failure), is reduced to its exception class. Paths must be checkpoint-relative.
    """
    if isinstance(rec, list):
        return [public_record(item) for item in rec]
    if not isinstance(rec, dict):
        return rec
    if rec.get("kind") == "file":
        check_relative(rec["path"])
        rec = {k: v for k, v in rec.items() if k in PUBLIC_ROW_FIELDS}
    out = {}
    for key, value in rec.items():
        if key in RELATIVE_PATH_LISTS and isinstance(value, list):
            for entry in value:
                check_relative(entry)
        out[key] = error_type(value) if key == "error" else public_record(value)
    return out


def public_jsonl(src, dst):
    """Copy a JSON-lines file with every record passed through public_record()."""
    with open(src) as fin, open(dst, "w") as fout:
        for line in fin:
            fout.write(json.dumps(public_record(json.loads(line)), sort_keys=True) + "\n")


def publish(run, refs, out):
    """Write a committable copy of a run: no node name, inode, process id or filesystem path."""
    os.makedirs(os.path.join(out, "hub"))
    for path in glob.glob(os.path.join(run, "hash-*.jsonl")):
        label = os.path.basename(path)[5:-6]
        shutil.copy(os.path.join(refs, label + ".json"), os.path.join(out, "hub"))
    for path in glob.glob(os.path.join(run, "hash-*.jsonl")) + glob.glob(os.path.join(run, "sweep-*.jsonl")):
        public_jsonl(path, os.path.join(out, os.path.basename(path)))
    for pattern in ("ms-*.json", "ms-*.skipped", "compare-*.json", "cpu-ceiling.json", "probe-cheap-check.json"):
        for path in glob.glob(os.path.join(run, pattern)):
            shutil.copy(path, out)
    stat = {}
    for path in sorted(glob.glob(os.path.join(run, "stat-*.jsonl"))):
        with open(path) as f:
            stat[os.path.basename(path)[:-6]] = public_record(json.loads(f.readlines()[-1]))
    with open(os.path.join(out, "stat-timings.json"), "w") as f:
        json.dump(stat, f, indent=1, sort_keys=True)
    with open(os.path.join(run, "env.txt")) as fin, open(os.path.join(out, "env.txt"), "w") as fout:
        fout.writelines(l for l in fin if not l.startswith(PRIVATE_ENV_PREFIXES))


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) not in (2, 3):
        sys.exit(__doc__)
    run, refs = argv[:2]
    hash_tables(run, refs)
    passed = compare_tables(run, refs)
    other_tables(run)
    if len(argv) == 3:
        publish(run, refs, argv[2])
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
