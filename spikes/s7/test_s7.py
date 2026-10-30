"""Tests for the S7 tools. Run from the repository root:

    python3 -m unittest discover -s spikes/s7 -v

Expected values are external: published test vectors (SHA-256 of "abc" and of the empty
string, the git blob ids of the empty file and of "hello\\n"), and the coreutils sha256sum
and git hash-object programs, which are independent implementations. No expected value is
produced by the code under test, and no test needs the network or the cluster.
"""
import argparse
import contextlib
import copy
import io
import json
import os
import re
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
import summarize_run  # noqa: E402

HASH_TREE = os.path.join(HERE, "hash_tree.py")
SUMMARIZE_RUN = os.path.join(HERE, "summarize_run.py")
SHA256_ABC = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
SHA256_EMPTY = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
GIT_EMPTY = "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"
GIT_HELLO = "ce013625030ba8dba906f756967f9e9ca394464a"
SHA256_HELLO = "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03"  # sha256sum of "hello\n"
GITATTRIBUTES = b"*.bin filter=lfs\n"
GIT_GITATTRIBUTES = "20a784076630cd588ee19ff1266102c33e2e6fe3"  # git hash-object of GITATTRIBUTES


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
        rows, self.summary = run_hash_tree(self.p("m"), self.p("out.jsonl"))
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
        report = compare.compare(hub, rows, self.summary)
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
        report = compare.compare(hub, rows, self.summary)
        self.assertTrue(report["all_match"])
        self.assertEqual(report["lfs_bytes_matched"], 3)

    def test_a_file_without_any_reference_hash_is_unchecked_not_ok(self):
        rows = self.staged({"a.json": b"hello\n"})
        for r in rows:
            r["git_blob_sha1"] = None
        hub = self.hub([{"path": "a.json", "size": 6, "git_oid": GIT_HELLO, "lfs_sha256": None}])
        report = compare.compare(hub, rows, self.summary)
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
        self.assertFalse(result["all_match"])

    def test_manifest_verdict_allows_only_gitattributes_to_be_absent(self):
        rows = self.staged({"a.bin": b"abc", ".gitattributes": GITATTRIBUTES})
        sha_gitattributes = "86266214dfd066dbd4d776951ab1a676ed6f3f6af050e020eac5a8b21b7e5245"  # sha256sum
        full = {"resources": [{"name": "a.bin", "algorithm": "sha256", "digest": SHA256_ABC}]}
        result = compare.compare_manifest(rows, full)
        self.assertEqual(result["not_in_manifest"], [".gitattributes"])
        self.assertTrue(result["all_match"])
        # the exception is for that one name: another file left out of the manifest is a failure
        result = compare.compare_manifest(rows, {"resources": [
            {"name": ".gitattributes", "algorithm": "sha256", "digest": sha_gitattributes}]})
        self.assertEqual(result["unexpected_not_in_manifest"], ["a.bin"])
        self.assertFalse(result["all_match"])
        # an empty manifest, a repeated entry and another algorithm fail too
        self.assertFalse(compare.compare_manifest(rows, {"resources": []})["all_match"])
        twice = {"resources": full["resources"] * 2}
        self.assertEqual(compare.compare_manifest(rows, twice)["duplicate_in_manifest"], ["a.bin"])
        self.assertFalse(compare.compare_manifest(rows, twice)["all_match"])
        other = {"resources": [{"name": "a.bin", "algorithm": "sha512", "digest": "00"}]}
        self.assertFalse(compare.compare_manifest(rows, other)["all_match"])


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


class PublishTests(TempDirCase):
    """summarize_run.publish() is the only step between raw run files and the public repository."""

    def fixture_run(self):
        run, refs = self.p("run"), self.p("refs")
        os.makedirs(run)
        os.makedirs(refs)
        private_row = {"kind": "file", "path": "model.safetensors", "size": 3, "sha256": SHA256_ABC,
                       "git_blob_sha1": None, "bytes_hashed": 3, "wall_s": 0.5, "cpu_s": 0.4, "rchar": 3,
                       "read_bytes": 0, "done_at_s": 0.6, "ino": 144115238826934273, "pid": 424242,
                       "mtime_ns": 1761000000123456789, "ctime_ns": 1761000001123456789,
                       "changed_during_read": False}
        summary = {"kind": "summary", "label": "gemma", "files": 1, "wall_s": 0.6}
        for name in ("hash-gemma.jsonl", "sweep-8MiB.jsonl"):
            with open(os.path.join(run, name), "w") as f:
                f.write(json.dumps(private_row) + "\n" + json.dumps(summary) + "\n")
        with open(os.path.join(run, "stat-before-gemma.jsonl"), "w") as f:
            f.write(json.dumps({"kind": "file", "path": "a", "size": 3, "ino": 987654321, "mtime_ns": 5,
                                "ctime_ns": 6}) + "\n")
            f.write(json.dumps({"kind": "summary", "label": "gemma", "mode": "stat-only", "files": 1,
                                "wall_s": 0.001}) + "\n")
        with open(os.path.join(run, "env.txt"), "w") as f:
            f.write("date_utc=2026-10-30T08:41:08Z\nnode=x1234c0s0b0n0\nkernel=5.14.0\n")
        for label in ("gemma", "deepseek"):  # deepseek was never hashed in this run
            with open(os.path.join(refs, label + ".json"), "w") as f:
                json.dump({"repo": "example/" + label, "revision": "0" * 40, "files": []}, f)
        return run, refs

    def test_no_host_identifiers_survive_and_only_hashed_labels_are_copied(self):
        run, refs = self.fixture_run()
        out = self.p("public")
        summarize_run.publish(run, refs, out)
        text = ""
        for dirpath, _, names in os.walk(out):
            for name in names:
                with open(os.path.join(dirpath, name)) as f:
                    text += f.read()
        for private in ("144115238826934273", "424242", "1761000000123456789", "1761000001123456789",
                        "987654321", "x1234c0s0b0n0", "node=", '"ino"', '"pid"', "mtime_ns", "ctime_ns"):
            self.assertNotIn(private, text)
        self.assertIn(SHA256_ABC, text)  # the public part is kept
        self.assertEqual(os.listdir(os.path.join(out, "hub")), ["gemma.json"])
        self.assertIn("kernel=5.14.0", text)
        with open(os.path.join(out, "stat-timings.json")) as f:
            self.assertEqual(json.load(f)["stat-before-gemma"]["wall_s"], 0.001)

    def real_failure_run(self):
        """A run directory whose hash and sweep files hold real hash_one() failure records."""
        run, refs = self.fixture_run()
        write(self.p("m", "ok.bin"), b"abc")
        os.symlink("does-not-exist", self.p("m", "dangling"))  # stat() on it raises, naming the full path
        task = lambda rel: (rel, self.p("m", rel), 1024, 1024, False)
        good, bad = hash_tree.hash_one(task("ok.bin")), hash_tree.hash_one(task("dangling"))
        # Not vacuous: the raw failure record does hold the temp directory.
        self.assertIn("error", bad)
        self.assertIn(self.tmp, bad["error"])
        args = argparse.Namespace(label="gemma", workers=1, bufsize=1024, fadvise_dontneed=False)
        lustre0 = {"error": "FileNotFoundError(2, 'No such file or directory: %s')" % self.p("lctl")}
        lustre1 = {"error": "lctl said %s is broken" % self.tmp}  # free text: no class to keep
        summary = hash_tree.summarise(args, [good, bad], 1.0, 1.0, lustre0, lustre1, [])
        self.assertIn(self.tmp, json.dumps(summary))
        for name in ("hash-gemma.jsonl", "sweep-8MiB.jsonl"):
            with open(os.path.join(run, name), "w") as f:
                f.write("".join(json.dumps(r) + "\n" for r in (good, bad, summary)))
        return run, refs, good, bad

    def published_text(self, out):
        text = ""
        for dirpath, _, names in os.walk(out):
            for name in names:
                with open(os.path.join(dirpath, name)) as f:
                    text += f.read()
        return text

    def test_published_failures_are_error_types_with_relative_paths(self):
        run, refs, _, _ = self.real_failure_run()
        out = self.p("public")
        summarize_run.publish(run, refs, out)
        text = self.published_text(out)
        for leak in (self.tmp, os.path.basename(self.tmp), "No such file", "Errno", "does-not-exist", "lctl"):
            self.assertNotIn(leak, text)
        self.assertIsNone(re.search(r'"/', text), "an absolute path survived")
        for name in ("hash-gemma.jsonl", "sweep-8MiB.jsonl"):
            with open(os.path.join(out, name)) as f:
                records = [json.loads(line) for line in f]
            # the failure is still there, as a type and a relative path: dropping it would hide it
            self.assertEqual(records[1], {"kind": "file", "path": "dangling", "error": "FileNotFoundError"})
            summary = records[2]
            self.assertEqual(summary["errors"], [records[1]])
            self.assertEqual(summary["lustre_node_wide"]["before"], {"error": "FileNotFoundError"})
            self.assertEqual(summary["lustre_node_wide"]["after"], {"error": "UnknownError"})
            self.assertEqual(summary["files"], 2)

    def test_public_jsonl_refuses_a_path_that_is_not_checkpoint_relative(self):
        for bad_path in ("/abs/model.bin", "../model.bin", "sub/../../model.bin"):
            src, dst = self.p("src.jsonl"), self.p("dst.jsonl")
            with open(src, "w") as f:
                f.write(json.dumps({"kind": "file", "path": bad_path, "size": 1}) + "\n")
            with self.assertRaises(ValueError):
                summarize_run.public_jsonl(src, dst)
        with open(src, "w") as f:
            f.write(json.dumps({"kind": "summary", "changed_during_read": [self.p("model.bin")]}) + "\n")
        with self.assertRaises(ValueError):
            summarize_run.public_jsonl(src, dst)

    def test_error_type_keeps_only_a_class_name(self):
        self.assertEqual(summarize_run.error_type("FileNotFoundError: [Errno 2] x: '/a/b'"), "FileNotFoundError")
        self.assertEqual(summarize_run.error_type("OSError(2, 'x')"), "OSError")
        self.assertEqual(summarize_run.error_type("PermissionError"), "PermissionError")
        self.assertEqual(summarize_run.error_type("x1234c0s0b0n0: unreachable"), "UnknownError")
        self.assertEqual(summarize_run.error_type("/abs/path: oops"), "UnknownError")
        self.assertEqual(summarize_run.error_type(None), "UnknownError")


KEEP = object()  # "leave this part of the run as hash_tree.py wrote it"


class CompareCliTests(TempDirCase):
    """compare.main() is the entry point whose exit status decides whether a run passes.

    Every case starts from the same clean run, which must pass (the positive control), and
    changes one thing. The summary is what hash_tree.py wrote, so a case that edits it models
    a run that really recorded that event.
    """

    def setUp(self):
        super().setUp()
        write(self.p("m", "a.bin"), b"abc")
        write(self.p("m", "b.json"), b"hello\n")
        write(self.p("m", ".gitattributes"), GITATTRIBUTES)
        self.rows, self.summary = run_hash_tree(self.p("m"), self.p("clean.jsonl"))
        self.hub = {"repo": "example/repo", "revision": "0" * 40, "files": [
            {"path": "a.bin", "size": 3, "git_oid": "x", "lfs_sha256": SHA256_ABC},
            {"path": "b.json", "size": 6, "git_oid": GIT_HELLO, "lfs_sha256": None},
            {"path": ".gitattributes", "size": len(GITATTRIBUTES), "git_oid": GIT_GITATTRIBUTES,
             "lfs_sha256": None}]}
        self.manifest = {"resources": [
            {"name": "a.bin", "algorithm": "sha256", "digest": SHA256_ABC},
            {"name": "b.json", "algorithm": "sha256", "digest": SHA256_HELLO}]}

    def row(self, rows, path):
        return [r for r in rows if r["path"] == path][0]

    def cli(self, rows=KEEP, summary=KEEP, hub=KEEP, manifest=None):
        """Run compare.main() on files; return (exit status, report, stderr)."""
        rows = copy.deepcopy(self.rows if rows is KEEP else rows)
        summary = copy.deepcopy(self.summary if summary is KEEP else summary)
        hub = copy.deepcopy(self.hub if hub is KEEP else hub)
        with open(self.p("hashes.jsonl"), "w") as f:
            for rec in rows + ([summary] if summary is not None else []):
                f.write(json.dumps(rec) + "\n")
        with open(self.p("hub.json"), "w") as f:
            json.dump(hub, f)
        argv = ["--hub", self.p("hub.json"), "--hashes", self.p("hashes.jsonl"), "--out", self.p("report.json")]
        if manifest is not None:
            with open(self.p("manifest.json"), "w") as f:
                json.dump(manifest, f)
            argv += ["--manifest", self.p("manifest.json")]
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = compare.main(argv)
        with open(self.p("report.json")) as f:
            return code, json.load(f), err.getvalue()

    def assert_invalid(self, reason, **change):
        code, report, err = self.cli(**change)
        self.assertEqual(code, 1, "exit status; report: %s" % report["invalid_reasons"])
        self.assertFalse(report["all_match"])
        self.assertFalse(report["valid"])
        self.assertTrue(any(reason in r for r in report["invalid_reasons"]),
                        "%r not in %r" % (reason, report["invalid_reasons"]))
        self.assertIn(reason, err)  # and the reason reaches the operator

    def test_a_clean_run_passes(self):
        code, report, err = self.cli(manifest=self.manifest)
        self.assertEqual(code, 0)
        self.assertEqual(report["counts"], {"lfs-ok": 1, "git-ok": 2})
        self.assertTrue(report["valid"] and report["all_match"] and not report["invalid_reasons"])
        self.assertEqual(report["model_signing_vs_own"]["not_in_manifest"], [".gitattributes"])
        self.assertEqual(err, "")

    def test_a_file_that_changed_during_the_read_fails_the_run(self):
        # The recorded hash still equals the Hub's: the digest alone cannot show the change.
        # A published run has no per-row flag, only the summary's list.
        summary = dict(self.summary, changed_during_read=["a.bin"])
        rows = copy.deepcopy(self.rows)
        for r in rows:
            r.pop("changed_during_read")
        self.assert_invalid("changed during the read", rows=rows, summary=summary)

    def test_a_row_flagged_as_changed_fails_the_run(self):
        rows = copy.deepcopy(self.rows)
        self.row(rows, "a.bin")["changed_during_read"] = True
        self.assert_invalid("flag a change", rows=rows)

    def test_an_error_in_the_summary_fails_the_run(self):
        # The rows are clean and equal to the Hub; only the summary records the failure. (A row
        # that itself carries an error is rejected by the read-error verdict even without this.)
        failed = {"kind": "file", "path": "a.bin", "error": "OSError: x"}
        self.assert_invalid("read error", summary=dict(self.summary, errors=[failed]))

    def test_an_error_row_fails_the_run(self):
        rows = [r for r in self.rows if r["path"] != "a.bin"] + [
            {"kind": "file", "path": "a.bin", "error": "OSError: x"}]
        self.assert_invalid("read error on 1 file", rows=rows)

    def test_a_skipped_directory_symlink_fails_the_run(self):
        summary = dict(self.summary, skipped=["linked (symlink to directory)"])
        self.assert_invalid("skipped", summary=summary)

    def test_no_summary_fails_the_run(self):
        self.assert_invalid("no summary line", summary=None)

    def test_an_incomplete_summary_fails_the_run(self):
        self.assert_invalid("counts 4 files", summary=dict(self.summary, files=4))
        lacking = {k: v for k, v in self.summary.items() if k != "skipped"}
        self.assert_invalid("lacks skipped", summary=lacking)
        self.assert_invalid("not from a hash pass", summary=dict(self.summary, mode="stat-only"))

    def test_a_second_summary_line_fails_the_run(self):
        self.assert_invalid("not file records", rows=self.rows + [dict(self.summary)],
                            summary=dict(self.summary, files=len(self.rows) + 1))

    def test_empty_input_fails_the_run(self):
        # Both sides empty, so no file can mismatch: all([]) is True.
        summary = dict(self.summary, files=0)
        self.assert_invalid("no file records", rows=[], summary=summary, hub=dict(self.hub, files=[]))
        self.assert_invalid("Hub listing is empty", rows=[], summary=summary, hub=dict(self.hub, files=[]))

    def test_a_duplicate_path_fails_the_run_even_when_the_mismatch_comes_first(self):
        wrong = dict(self.row(self.rows, "a.bin"), sha256="0" * 64)
        rows = [wrong] + self.rows  # the mismatching a.bin row is read first, the matching one later
        code, report, _ = self.cli(rows=rows, summary=dict(self.summary, files=len(rows)))
        self.assertEqual(code, 1)
        self.assertFalse(report["valid"])
        self.assertTrue(any("more than once" in r for r in report["invalid_reasons"]))
        self.assertEqual({f["path"]: f["verdict"] for f in report["files"]}["a.bin"], "duplicate-path")

    def test_a_duplicate_hub_entry_fails_the_run(self):
        hub = dict(self.hub, files=self.hub["files"] + [self.hub["files"][0]])
        self.assert_invalid("Hub listing", hub=hub)

    def test_the_manifest_verdict_is_part_of_the_exit_status(self):
        # The run itself is valid and equals the Hub; only the manifest is wrong.
        differs = copy.deepcopy(self.manifest)
        differs["resources"][1]["digest"] = SHA256_ABC
        for label, manifest in (("a differing digest", differs),
                                ("a missing file", {"resources": self.manifest["resources"][:1]}),
                                ("an extra file", {"resources": self.manifest["resources"] + [
                                    {"name": "c.bin", "algorithm": "sha256", "digest": SHA256_ABC}]})):
            code, report, _ = self.cli(manifest=manifest)
            self.assertTrue(report["all_match"], label)
            self.assertFalse(report["model_signing_vs_own"]["all_match"], label)
            self.assertEqual(code, 1, label)


class SummarizeRunExitStatusTests(TempDirCase):
    """summarize_run.py is the second comparison entry point; its exit status must follow compare."""

    def setUp(self):
        super().setUp()
        self.run, self.refs = self.p("run"), self.p("refs")
        os.makedirs(self.refs)
        write(self.p("m", "a.bin"), b"abc")
        write(self.p("m", "b.json"), b"hello\n")
        os.makedirs(self.run)
        run_hash_tree(self.p("m"), os.path.join(self.run, "hash-gemma.jsonl"))
        with open(os.path.join(self.refs, "gemma.json"), "w") as f:
            json.dump({"repo": "example/gemma", "revision": "0" * 40, "files": [
                {"path": "a.bin", "size": 3, "git_oid": "x", "lfs_sha256": SHA256_ABC},
                {"path": "b.json", "size": 6, "git_oid": GIT_HELLO, "lfs_sha256": None}]}, f)

    def edit_summary(self, **change):
        path = os.path.join(self.run, "hash-gemma.jsonl")
        with open(path) as f:
            records = [json.loads(line) for line in f]
        records[-1].update(change)
        with open(path, "w") as f:
            f.writelines(json.dumps(r) + "\n" for r in records)

    def summarize(self):
        proc = subprocess.run([sys.executable, SUMMARIZE_RUN, self.run, self.refs], stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, universal_newlines=True)
        return proc.returncode, proc.stdout, proc.stderr

    def test_a_clean_run_exits_zero(self):
        code, out, err = self.summarize()
        self.assertEqual((code, err), (0, ""))
        self.assertIn("| example/gemma @ 000000000000 | 2 | 2 | 1 | 1 |", out)

    def test_a_run_with_a_change_during_the_read_exits_one(self):
        self.edit_summary(changed_during_read=["a.bin"])
        code, out, _ = self.summarize()
        self.assertEqual(code, 1)
        self.assertIn("INVALID RUN", out)

    def test_a_run_with_a_skipped_entry_exits_one(self):
        self.edit_summary(skipped=["linked (symlink to directory)"])
        self.assertEqual(self.summarize()[0], 1)

    def test_a_differing_manifest_exits_one(self):
        with open(os.path.join(self.run, "ms-gemma.json"), "w") as f:
            json.dump({"label": "gemma", "wall_s": 1.0, "cpu_s": 1.0, "rchar": 9, "read_bytes": 0, "resources": [
                {"name": "a.bin", "algorithm": "sha256", "digest": SHA256_ABC},
                {"name": "b.json", "algorithm": "sha256", "digest": SHA256_ABC}]}, f)
        code, out, _ = self.summarize()
        self.assertEqual(code, 1)
        self.assertIn("model_signing manifest does not match", out)

    def test_the_markdown_output_carries_no_error_text(self):
        # A Lustre counter failure in the raw summary can quote paths; the tables print its type only.
        self.edit_summary(lustre_node_wide={
            "before": {"error": "lctl said %s is broken" % self.tmp},
            "after": {"error": "OSError(2, 'No such file or directory: %s')" % self.p("lctl")}})
        code, out, _ = self.summarize()
        self.assertEqual(code, 0)
        self.assertIn("unavailable", out)
        self.assertNotIn(self.tmp, out)
        self.assertNotIn(os.path.basename(self.tmp), out)


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
