#!/usr/bin/env python3
"""Fetch the Hugging Face Hub's per-file listing for one repo at one revision.

The listing is the independent reference of spike S7: for each file, the Hub's size, its
git blob id (`oid`, a SHA-1) and, for files stored in LFS, `lfs.oid` (the SHA-256 of the
content). Run it on the agents VM, never on the cluster.

The tree endpoint returns 50 entries per page unless `limit` is given, and a longer
listing is cut off without any error: the first fetch of a 74-file repo showed 47 files.
This script asks for limit=1000 and also follows the Link rel="next" header, and it
refuses to write a listing whose page chain it could not follow to the end.

Usage: fetch_ref.py <repo_id> <40-hex revision> <out.json>
"""
import json
import re
import sys
import urllib.request

MAX_PAGES = 50
REVISION_RE = re.compile(r"^[0-9a-f]{40}$")


def next_link(header):
    """Return the URL of rel="next" in a Link header, or None."""
    match = re.search(r'<([^>]+)>\s*;\s*rel="next"', header or "")
    return match.group(1) if match else None


def fetch(repo, revision):
    url = "https://huggingface.co/api/models/%s/tree/%s?recursive=1&limit=1000" % (repo, revision)
    entries, pages = [], 0
    while url:
        pages += 1
        if pages > MAX_PAGES:
            raise RuntimeError("more than %d pages; refusing to continue" % MAX_PAGES)
        request = urllib.request.Request(url, headers={"User-Agent": "hpc-burst-s7"})
        with urllib.request.urlopen(request, timeout=60) as resp:
            entries.extend(json.load(resp))
            url = next_link(resp.headers.get("Link"))
    return entries, pages


def to_files(entries):
    files = []
    for e in entries:
        if e["type"] != "file":
            continue
        files.append({
            "path": e["path"], "size": e["size"], "git_oid": e["oid"],
            "lfs_sha256": e.get("lfs", {}).get("oid"), "lfs_size": e.get("lfs", {}).get("size"),
        })
    return sorted(files, key=lambda f: f["path"])


def main():
    if len(sys.argv) != 4:
        sys.exit(__doc__)
    repo, revision, out = sys.argv[1:]
    if not REVISION_RE.match(revision):
        sys.exit("revision must be a full 40-hex commit id, not a branch or tag: %r" % revision)
    entries, pages = fetch(repo, revision)
    files = to_files(entries)
    if not files:
        sys.exit("the Hub returned no files for %s@%s" % (repo, revision))
    with open(out, "w") as f:
        json.dump({"repo": repo, "revision": revision, "pages": pages, "files": files}, f, indent=1)
    print("%s@%s pages=%d files=%d lfs=%d bytes=%d" % (
        repo, revision[:12], pages, len(files), sum(1 for x in files if x["lfs_sha256"]),
        sum(x["size"] for x in files)))


if __name__ == "__main__":
    main()
