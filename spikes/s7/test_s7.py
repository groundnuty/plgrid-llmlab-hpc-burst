"""Tests for the S7 tools. Run from the repository root:

    python3 -m unittest discover -s spikes/s7 -v

Expected values are external: published test vectors (SHA-256 of "abc" and of the empty
string, the git blob ids of the empty file and of "hello\\n"), and the coreutils sha256sum
and git hash-object programs, which are independent implementations. No expected value is
produced by the code under test, and no test needs the network or the cluster.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import compare  # noqa: E402
import fetch_ref  # noqa: E402
import hash_tree  # noqa: E402

HASH_TREE = os.path.join(HERE, "hash_tree.py")
SHA256_ABC = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
SHA256_EMPTY = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
GIT_EMPTY = "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"
GIT_HELLO = "ce013625030ba8dba906f756967f9e9ca394464a"


def write(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)


def run_hash_tree(root, out, *extra):
    subprocess.run([sys.executable, HASH_TREE, "--root", root, "--label", "t", "--out", out,
                    "--workers", "2", *extra], check=True, stdout=subprocess.DEVNULL)
    return compare.load_jsonl(out)


class TempDirCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="s7-test-")
        self.addCleanup(shutil.rmtree, self.tmp)

    def p(self, *parts):
        return os.path.join(self.tmp, *parts)


class HashTreeTests(TempDirCase):
    def test_published_vectors(self):
        write(self.p("m", "abc"), b"abc")
        write(self.p("m", "empty"), b"")
        write(self.p("m", "sub", "hello"), b"hello\n")
        rows, summary = run_hash_tree(self.p("m"), self.p("out.jsonl"))
        by = {r["path"]: r for r in rows}
        self.assertEqual(by["abc"]["sha256"], SHA256_ABC)
        self.assertEqual(by["empty"]["sha256"], SHA256_EMPTY)
        self.assertEqual(by["empty"]["git_blob_sha1"], GIT_EMPTY)
        self.assertEqual(by["sub/hello"]["git_blob_sha1"], GIT_HELLO)
        self.assertEqual(summary["files"], 3)
        self.assertEqual(summary["errors"], [])

    def test_symlink_is_hashed_by_content_not_by_blob_name(self):
        # A Hugging Face cache names the blob after the Hub's hash. Here the name is a lie:
        # a result that followed the name would not be the SHA-256 of "abc".
        lying_name = "0" * 64
        write(self.p("cache", "blobs", lying_name), b"abc")
        os.makedirs(self.p("cache", "snap"))
        os.symlink(os.path.join("..", "blobs", lying_name), self.p("cache", "snap", "model.safetensors"))
        rows, _ = run_hash_tree(self.p("cache", "snap"), self.p("out.jsonl"))
        self.assertEqual([r["path"] for r in rows], ["model.safetensors"])
        self.assertEqual(rows[0]["sha256"], SHA256_ABC)
        self.assertNotEqual(rows[0]["sha256"], lying_name)

    def test_multi_buffer_file_matches_coreutils_and_git(self):
        data = os.urandom(5 * 1024 * 1024 + 123)  # not a multiple of the 1 MiB buffer
        write(self.p("m", "big.bin"), data)
        rows, _ = run_hash_tree(self.p("m"), self.p("out.jsonl"), "--bufsize", str(1024 * 1024))
        expected_sha = subprocess.run(["sha256sum", self.p("m", "big.bin")], check=True,
                                      stdout=subprocess.PIPE, universal_newlines=True).stdout.split()[0]
        expected_git = subprocess.run(["git", "hash-object", self.p("m", "big.bin")], check=True,
                                      stdout=subprocess.PIPE, universal_newlines=True).stdout.strip()
        self.assertEqual(rows[0]["sha256"], expected_sha)
        self.assertEqual(rows[0]["git_blob_sha1"], expected_git)
        self.assertEqual(rows[0]["bytes_hashed"], len(data))

    def test_rchar_equals_file_size_with_a_small_buffer(self):
        write(self.p("m", "f.bin"), os.urandom(3 * 1024 * 1024 + 7))
        rows, summary = run_hash_tree(self.p("m"), self.p("out.jsonl"), "--bufsize", "65536")
        # rchar counts every read() syscall; the measurement's own reads of /proc/self/io are
        # subtracted, so what is left is the file, give or take a digit's worth of bytes.
        self.assertLessEqual(abs(rows[0]["rchar"] - rows[0]["size"]), 16)
        self.assertLessEqual(abs(summary["rchar_sum"] - summary["bytes_on_disk"]), 16)

    def test_git_oid_skipped_above_the_limit(self):
        write(self.p("m", "f.bin"), b"x" * 100)
        rows, _ = run_hash_tree(self.p("m"), self.p("out.jsonl"), "--git-oid-max", "10")
        self.assertIsNone(rows[0]["git_blob_sha1"])

    def test_only_hashes_the_named_file(self):
        write(self.p("m", "a"), b"abc")
        write(self.p("m", "b"), b"def")
        rows, _ = run_hash_tree(self.p("m"), self.p("out.jsonl"), "--only", "a")
        self.assertEqual([r["path"] for r in rows], ["a"])

    def test_unreadable_file_is_reported_not_skipped(self):
        write(self.p("m", "a"), b"abc")
        os.symlink("does-not-exist", self.p("m", "dangling"))
        rows, summary = run_hash_tree(self.p("m"), self.p("out.jsonl"))
        self.assertEqual(len(summary["errors"]), 1)
        self.assertEqual(summary["errors"][0]["path"], "dangling")

    def test_stat_only_does_not_hash(self):
        write(self.p("m", "a"), b"abc")
        subprocess.run([sys.executable, HASH_TREE, "--root", self.p("m"), "--label", "t", "--stat-only",
                        "--out", self.p("s.jsonl")], check=True, stdout=subprocess.DEVNULL)
        rows, summary = compare.load_jsonl(self.p("s.jsonl"))
        self.assertNotIn("sha256", rows[0])
        self.assertEqual(rows[0]["size"], 3)
        self.assertEqual(summary["mode"], "stat-only")


class CompareTests(TempDirCase):
    def hub(self, files):
        return {"repo": "example/repo", "revision": "0" * 40, "files": files}

    def staged(self, files):
        for name, data in files.items():
            write(self.p("m", name), data)
        rows, _ = run_hash_tree(self.p("m"), self.p("out.jsonl"))
        return rows

    def verdicts(self, report):
        return {f["path"]: f["verdict"] for f in report["files"]}

    def test_every_verdict(self):
        rows = self.staged({
            "good.bin": b"abc", "changed.bin": b"abd", "good.json": b"hello\n",
            "bad.json": b"hello\n", "short.bin": b"abc", "extra.txt": b"x"})
        hub = self.hub([
            {"path": "good.bin", "size": 3, "git_oid": "x", "lfs_sha256": SHA256_ABC},
            {"path": "changed.bin", "size": 3, "git_oid": "x", "lfs_sha256": SHA256_ABC},
            {"path": "good.json", "size": 6, "git_oid": GIT_HELLO, "lfs_sha256": None},
            {"path": "bad.json", "size": 6, "git_oid": GIT_EMPTY, "lfs_sha256": None},
            {"path": "short.bin", "size": 4, "git_oid": "x", "lfs_sha256": SHA256_ABC},
            {"path": "gone.bin", "size": 9, "git_oid": "x", "lfs_sha256": SHA256_ABC},
        ])
        report = compare.compare(hub, rows)
        self.assertEqual(self.verdicts(report), {
            "good.bin": "lfs-ok", "changed.bin": "lfs-MISMATCH", "good.json": "git-ok",
            "bad.json": "git-MISMATCH", "short.bin": "size-MISMATCH", "gone.bin": "not-staged",
            "extra.txt": "not-on-hub"})
        self.assertFalse(report["all_match"])

    def test_all_match(self):
        rows = self.staged({"a.bin": b"abc", "b.json": b"hello\n"})
        hub = self.hub([
            {"path": "a.bin", "size": 3, "git_oid": "x", "lfs_sha256": SHA256_ABC},
            {"path": "b.json", "size": 6, "git_oid": GIT_HELLO, "lfs_sha256": None}])
        report = compare.compare(hub, rows)
        self.assertTrue(report["all_match"])
        self.assertEqual(report["lfs_bytes_matched"], 3)

    def test_a_file_without_any_reference_hash_is_unchecked_not_ok(self):
        rows = self.staged({"a.json": b"hello\n"})
        for r in rows:
            r["git_blob_sha1"] = None
        hub = self.hub([{"path": "a.json", "size": 6, "git_oid": GIT_HELLO, "lfs_sha256": None}])
        report = compare.compare(hub, rows)
        self.assertEqual(self.verdicts(report), {"a.json": "unchecked"})
        self.assertFalse(report["all_match"])

    def test_manifest_comparison(self):
        rows = self.staged({"a.bin": b"abc", "b.bin": b"abd"})
        manifest = {"resources": [
            {"name": "a.bin", "algorithm": "sha256", "digest": SHA256_ABC},
            {"name": "b.bin", "algorithm": "sha256", "digest": SHA256_ABC},
            {"name": "c.bin", "algorithm": "sha256", "digest": SHA256_ABC}]}
        result = compare.compare_manifest(rows, manifest)
        self.assertEqual(result["equal"], 1)
        self.assertEqual(result["differ"], ["b.bin"])
        self.assertEqual(result["only_in_manifest"], ["c.bin"])


RPC_STATS_SAMPLE = """osc.fs-OST0000-osc-ffff.rpc_stats=
snapshot_time:            1793349522.077815253 secs.nsecs
read RPCs in flight:  0
write RPCs in flight: 0

\t\t\tread\t\t\twrite
pages per rpc         rpcs   % cum % |       rpcs   % cum %
1:\t\t         10  50  50   |          7   0   0
4:\t\t          5  25  75   |          1   0   0
1024:\t\t          2  10  85   |          3   0   0

\t\t\tread\t\t\twrite
rpcs in flight        rpcs   % cum % |       rpcs   % cum %
1:\t\t        999  97  97   |      22528  31  31

osc.fs-OST0001-osc-ffff.rpc_stats=
\t\t\tread\t\t\twrite
pages per rpc         rpcs   % cum % |       rpcs   % cum %
2:\t\t          4 100 100   |          0   0   0
"""


class RpcStatsTests(unittest.TestCase):
    def test_read_rpcs_and_byte_bounds(self):
        # Worked by hand from the sample above, 4096-byte pages. Read column only; the
        # "rpcs in flight" row (999) and the write column are not counted.
        got = hash_tree.parse_rpc_stats(RPC_STATS_SAMPLE, 4096)
        self.assertEqual(got["osc_read_rpcs"], 10 + 5 + 2 + 4)
        # upper: 10 one-page, 5 of at most 4, 2 of at most 1024, 4 of at most 2 pages
        self.assertEqual(got["osc_read_bytes_high"], (10 * 1 + 5 * 4 + 2 * 1024 + 4 * 2) * 4096)
        # lower: 10 one-page, 5 of at least 3, 2 of at least 513, 4 of at least 2 pages
        self.assertEqual(got["osc_read_bytes_low"], (10 * 1 + 5 * 3 + 2 * 513 + 4 * 2) * 4096)


class FetchRefTests(unittest.TestCase):
    def test_next_link(self):
        header = '<https://huggingface.co/api/models/a/b/tree/c?cursor=XYZ&limit=50>; rel="next"'
        self.assertEqual(fetch_ref.next_link(header),
                         "https://huggingface.co/api/models/a/b/tree/c?cursor=XYZ&limit=50")
        self.assertIsNone(fetch_ref.next_link(None))
        self.assertIsNone(fetch_ref.next_link('<https://x/y>; rel="prev"'))

    def test_to_files_keeps_files_and_lfs_hash_only(self):
        entries = [
            {"type": "directory", "path": "sub", "oid": "d", "size": 0},
            {"type": "file", "path": "w.safetensors", "oid": "g1", "size": 7,
             "lfs": {"oid": SHA256_ABC, "size": 7}},
            {"type": "file", "path": "config.json", "oid": GIT_HELLO, "size": 6}]
        files = fetch_ref.to_files(entries)
        self.assertEqual([f["path"] for f in files], ["config.json", "w.safetensors"])
        self.assertIsNone(files[0]["lfs_sha256"])
        self.assertEqual(files[1]["lfs_sha256"], SHA256_ABC)

    def test_revision_must_be_a_full_commit_id(self):
        self.assertTrue(fetch_ref.REVISION_RE.match("7872f01b1d1fe23eabc4c98b48bffcef5a386062"))
        self.assertFalse(fetch_ref.REVISION_RE.match("main"))
        self.assertFalse(fetch_ref.REVISION_RE.match("7872f01"))


if __name__ == "__main__":
    unittest.main()
