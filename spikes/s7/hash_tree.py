#!/usr/bin/env python3
"""Hash every file of a checkpoint directory and measure what the reads cost.

Runs on the Helios host Python (3.6), so it uses no 3.7+ feature.

Per file it records: size, SHA-256 of the content, the git blob SHA-1 for files up to
--git-oid-max bytes (the Hub publishes that as `oid` for files it does not store in LFS),
wall time, CPU time, and /proc/self/io `rchar` and `read_bytes` taken before and after the
file. Symlinks (a Hugging Face cache snapshot is made of them) are followed when reading
but the result is keyed by the path inside the checkpoint directory. The name of the blob
a symlink points to is never used as a hash: in a Hugging Face cache it equals the Hub's
hash, so using it would compare the Hub with itself.

Reads use one reusable buffer (--bufsize, default 8 MiB) through readinto(): no read
request is ever sized by a cap, which is what over-read on Lustre once (lesson L-12.5).

Each file's line is flushed to the --out JSON-lines file as soon as it is done, so a job
that hits its time limit still leaves the finished files. The last line is a summary.

Modes:
  default       hash all files (or --only the named ones) with --workers processes
  --stat-only   stat all files (the "cheap check") and time it; no file is opened
  --cpu-ceiling hash zeroed memory only, to show the SHA-256 rate the CPU can reach
"""
import argparse
import hashlib
import json
import multiprocessing
import os
import re
import resource
import subprocess
import sys
import time

GIT_OID_MAX_DEFAULT = 64 * 1024 * 1024

_buf = None  # one reused read buffer per worker process; mutation is the point


def read_proc_io():
    """Return /proc/self/io as a dict of ints, or {} when it cannot be read."""
    out = {}
    try:
        with open("/proc/self/io") as f:
            for line in f:
                key, _, value = line.partition(":")
                out[key.strip()] = int(value)
    except (OSError, ValueError):
        return {}
    return out


def proc_io_overhead():
    """Bytes of rchar that one read of /proc/self/io adds to the next snapshot.

    Reading /proc/self/io is itself a read() the kernel counts in rchar, so a before/after
    difference is too large by about this many bytes (about 100). The caller subtracts it.
    """
    first, second = read_proc_io(), read_proc_io()
    if not first or not second:
        return 0
    return second.get("rchar", 0) - first.get("rchar", 0)


def cpu_seconds(who=resource.RUSAGE_SELF):
    ru = resource.getrusage(who)
    return ru.ru_utime + ru.ru_stime


RPC_ROW = re.compile(r"^(\d+):\s+(\d+)\s+\d+\s+\d+\s+\|\s+(\d+)")


def parse_rpc_stats(text, page_size):
    """Total read RPCs and bounds on the bytes they carried, from `lctl get_param osc.*.rpc_stats`.

    Uses the "pages per rpc" histogram (left column = read). Lustre puts an RPC of p pages in
    the bucket labelled 2^ceil(log2 p), so bucket N holds RPCs of N/2+1 .. N pages (1 for N=1).
    Full-size RPCs, which bulk reads produce, sit exactly on a bucket label.
    """
    rpcs = low = high = 0
    in_pages = False
    for line in text.splitlines():
        if line.startswith("pages per rpc"):
            in_pages = True
            continue
        if not line.strip() or line.startswith(("rpcs in flight", "offset")):
            in_pages = False
            continue
        match = RPC_ROW.match(line) if in_pages else None
        if match:
            pages, count = int(match.group(1)), int(match.group(2))
            rpcs += count
            high += count * pages * page_size
            low += count * (pages // 2 + 1 if pages > 1 else 1) * page_size
    return {"osc_read_rpcs": rpcs, "osc_read_bytes_low": low, "osc_read_bytes_high": high}


def lustre_counters():
    """Node-wide Lustre client read RPC counters, or {"error": ...} when lctl cannot give them.

    Users cannot read the byte counters (osc.*.stats is root-only), but osc.*.rpc_stats is
    readable. It counts every process on the node since the client mounted, so a before/after
    difference includes other jobs on the node and is an upper bound for this process.
    """
    try:
        proc = subprocess.run(["lctl", "get_param", "osc.*.rpc_stats"], stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, universal_newlines=True, timeout=60)
    except (OSError, subprocess.SubprocessError) as exc:
        return {"error": repr(exc)}
    if not proc.stdout:
        return {"error": (proc.stderr or "no output").strip()[:200]}
    return parse_rpc_stats(proc.stdout, os.sysconf("SC_PAGE_SIZE"))


def list_files(root):
    """Return sorted (relpath, abspath) for every file under root, symlinks to files included."""
    found, skipped = [], []
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        for name in list(dirnames):
            if os.path.islink(os.path.join(dirpath, name)):
                skipped.append(os.path.relpath(os.path.join(dirpath, name), root) + " (symlink to directory)")
                dirnames.remove(name)
        for name in filenames:
            path = os.path.join(dirpath, name)
            found.append((os.path.relpath(path, root).replace(os.sep, "/"), path))
    found.sort()
    return found, skipped


def stat_record(path):
    st = os.stat(path)  # follows a symlink: the size and times of the content file
    return {"size": st.st_size, "mtime_ns": st.st_mtime_ns, "ctime_ns": st.st_ctime_ns, "ino": st.st_ino}


def hash_one(task):
    """Hash one file. Runs in a worker process; the file is read by this process only."""
    global _buf
    relpath, path, bufsize, git_oid_max, dontneed = task
    if _buf is None or len(_buf) != bufsize:
        _buf = bytearray(bufsize)
    view = memoryview(_buf)
    rec = {"kind": "file", "path": relpath}
    try:
        before = stat_record(path)
        size = before["size"]
        sha = hashlib.sha256()
        git = hashlib.sha1(b"blob %d\0" % size) if size <= git_oid_max else None
        overhead = proc_io_overhead()
        io0, cpu0, t0 = read_proc_io(), cpu_seconds(), time.perf_counter()
        total = 0
        with open(path, "rb", buffering=0) as f:
            while True:
                n = f.readinto(view)
                if not n:
                    break
                sha.update(view[:n])
                if git is not None:
                    git.update(view[:n])
                total += n
            if dontneed:
                os.posix_fadvise(f.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
        wall = time.perf_counter() - t0
        cpu = cpu_seconds() - cpu0
        io1 = read_proc_io()
        after = stat_record(path)
        rec.update(before)
        rec.update({
            "sha256": sha.hexdigest(),
            "git_blob_sha1": git.hexdigest() if git is not None else None,
            "bytes_hashed": total,
            "wall_s": wall,
            "cpu_s": cpu,
            "rchar": io1.get("rchar", 0) - io0.get("rchar", 0) - overhead if io0 and io1 else None,
            "read_bytes": io1.get("read_bytes", 0) - io0.get("read_bytes", 0) if io0 and io1 else None,
            "changed_during_read": before != after,
            "pid": os.getpid(),
        })
    except OSError as exc:
        rec["error"] = "%s: %s" % (type(exc).__name__, exc)
    return rec


def run_hash(args):
    files, skipped = list_files(args.root)
    if args.only:
        wanted = set(args.only)
        files = [f for f in files if f[0] in wanted]
        missing = wanted - set(f[0] for f in files)
        if missing:
            sys.exit("--only names not found under root: %s" % sorted(missing))
    def size_of(path):
        try:
            return os.stat(path).st_size
        except OSError:
            return -1  # reported as an error by hash_one

    sizes = {rel: size_of(path) for rel, path in files}
    files.sort(key=lambda f: -sizes[f[0]])  # largest first: the long files start early
    tasks = [(rel, path, args.bufsize, args.git_oid_max, args.fadvise_dontneed) for rel, path in files]

    lustre0 = lustre_counters()
    child_cpu0 = cpu_seconds(resource.RUSAGE_CHILDREN)
    t0 = time.perf_counter()
    records = []
    with open(args.out, "w") as out:
        pool = multiprocessing.Pool(args.workers)
        try:
            for rec in pool.imap_unordered(hash_one, tasks, chunksize=1):
                rec["done_at_s"] = time.perf_counter() - t0
                out.write(json.dumps(rec, sort_keys=True) + "\n")
                out.flush()
                records.append(rec)
        finally:
            pool.close()
            pool.join()
        wall = time.perf_counter() - t0
        lustre1 = lustre_counters()
        summary = summarise(args, records, wall, cpu_seconds(resource.RUSAGE_CHILDREN) - child_cpu0,
                            lustre0, lustre1, skipped)
        out.write(json.dumps(summary, sort_keys=True) + "\n")
    with open(args.out + ".summary.json", "w") as f:
        json.dump(summary, f, indent=1, sort_keys=True)
    print(json.dumps(summary, sort_keys=True))


def summarise(args, records, wall, cpu, lustre0, lustre1, skipped):
    ok = [r for r in records if "error" not in r]
    size_sum = sum(r["size"] for r in ok)
    rchar = [r["rchar"] for r in ok]
    rbytes = [r["read_bytes"] for r in ok]
    lustre = {"before": lustre0, "after": lustre1}
    if "error" not in lustre0 and "error" not in lustre1:
        lustre["delta"] = {k: lustre1[k] - lustre0[k] for k in lustre1}
    return {
        "kind": "summary",
        "label": args.label,
        "mode": "hash",
        "workers": args.workers,
        "bufsize": args.bufsize,
        "fadvise_dontneed": args.fadvise_dontneed,
        "files": len(records),
        "errors": [r for r in records if "error" in r],
        "bytes_on_disk": size_sum,
        "bytes_hashed": sum(r["bytes_hashed"] for r in ok),
        "rchar_sum": None if None in rchar else sum(rchar),
        "read_bytes_sum": None if None in rbytes else sum(rbytes),
        "wall_s": wall,
        "cpu_s": cpu,
        "throughput_MBps": size_sum / wall / 1e6 if wall > 0 else None,
        "changed_during_read": [r["path"] for r in ok if r["changed_during_read"]],
        "lustre_node_wide": lustre,
        "skipped": skipped,
    }


def run_stat_only(args):
    files, skipped = list_files(args.root)
    t0 = time.perf_counter()
    recs = []
    for rel, path in files:
        rec = {"kind": "file", "path": rel}
        rec.update(stat_record(path))
        recs.append(rec)
    wall = time.perf_counter() - t0
    summary = {"kind": "summary", "label": args.label, "mode": "stat-only", "files": len(recs),
               "bytes_on_disk": sum(r["size"] for r in recs), "wall_s": wall, "skipped": skipped}
    with open(args.out, "w") as out:
        for rec in recs:
            out.write(json.dumps(rec, sort_keys=True) + "\n")
        out.write(json.dumps(summary, sort_keys=True) + "\n")
    print(json.dumps(summary, sort_keys=True))


def run_cpu_ceiling(args):
    """SHA-256 rate with no I/O: hash one zeroed buffer repeatedly."""
    buf = bytes(args.bufsize)
    rounds = max(1, args.ceiling_bytes // args.bufsize)
    sha = hashlib.sha256()
    t0 = time.perf_counter()
    for _ in range(rounds):
        sha.update(buf)
    wall = time.perf_counter() - t0
    summary = {"kind": "summary", "label": args.label, "mode": "cpu-ceiling",
               "bytes": rounds * args.bufsize, "wall_s": wall,
               "single_core_MBps": rounds * args.bufsize / wall / 1e6}
    with open(args.out, "w") as out:
        out.write(json.dumps(summary, sort_keys=True) + "\n")
    print(json.dumps(summary, sort_keys=True))


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", help="checkpoint directory (a Hugging Face cache snapshot, or any directory)")
    ap.add_argument("--label", required=True)
    ap.add_argument("--out", required=True, help="JSON-lines output file; <out>.summary.json is written too")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--bufsize", type=int, default=8 * 1024 * 1024)
    ap.add_argument("--git-oid-max", type=int, default=GIT_OID_MAX_DEFAULT)
    ap.add_argument("--only", nargs="*", help="relative paths to hash; default is every file")
    ap.add_argument("--fadvise-dontneed", action="store_true",
                    help="drop each file from the page cache after hashing it, to keep later passes cold")
    ap.add_argument("--stat-only", action="store_true")
    ap.add_argument("--cpu-ceiling", action="store_true")
    ap.add_argument("--ceiling-bytes", type=int, default=4 * 1024 ** 3)
    args = ap.parse_args(argv)
    if args.bufsize <= 0 or args.workers <= 0:
        sys.exit("--bufsize and --workers must be positive")
    if args.cpu_ceiling:
        return run_cpu_ceiling(args)
    if not args.root or not os.path.isdir(args.root):
        sys.exit("--root must be an existing directory")
    if args.stat_only:
        return run_stat_only(args)
    return run_hash(args)


if __name__ == "__main__":
    main()
