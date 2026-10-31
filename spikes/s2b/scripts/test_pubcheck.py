"""Tests of pubcheck.py. Every fixture is built here at run time, so this file holds no refused text itself
(each refused string is assembled from pieces) and the publication check can scan it like any other file.

Run: python3 -I -B spikes/s2b/scripts/test_pubcheck.py
"""
import bz2
import gzip
import io
import lzma
import os
import py_compile
import subprocess
import sys
import tarfile
import tempfile
import unittest
import zipfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import pubcheck  # noqa: E402

PUBCHECK = os.path.join(HERE, "pubcheck.py")
SECRET = "zq7-distinct-token"  # a name the output must never echo
PRIV = "/ho" + "me/" + SECRET + "/work/file.py"  # a private path
PFX = "p" + "lg"
ACCOUNT = PFX + SECRET  # an account name


def put(path, data):
    with open(path, "wb" if isinstance(data, bytes) else "w") as f:
        f.write(data)


def scan(data, **kw):
    sc = pubcheck.Scanner(environ=kw.pop("environ", {}), **kw)
    sc.scan("f", data if isinstance(data, bytes) else data.encode())
    return sc


def rules(sc):
    return sorted({f[1] for f in sc.findings})


def zstd_compress(data):
    try:
        from compression import zstd
        return zstd.compress(data)
    except ImportError:
        pass
    import shutil
    exe = shutil.which("zstd")
    if exe:
        return subprocess.run([exe, "-c", "-q"], input=data, capture_output=True, check=True).stdout
    return None


def tar_of(members):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as t:
        for name, blob in members:
            ti = tarfile.TarInfo(name)
            ti.size = len(blob)
            t.addfile(ti, io.BytesIO(blob))
    return buf.getvalue()


class RuleTest(unittest.TestCase):
    def test_each_private_directory_is_refused(self):
        for d in ("ho" + "me", "Us" + "ers", "ne" + "t", "tm" + "p"):
            with self.subTest(d):
                self.assertEqual(rules(scan("see /%s/%s/x here" % (d, SECRET))), ["path"])

    def test_public_names_pass(self):
        ok = ("open('/proc/net/tcp'); module github.com/groundnuty/" + PFX + "rid-llmlab-hpc-burst/spikes; "
              "sbatch -p " + PFX + "rid-gpu-gh200 --no-requeue; the home and tmp words, S2B_HOME/x and $TMPDIR/y")
        self.assertEqual(scan(ok).findings, [])

    def test_account_prefixes_are_refused(self):
        self.assertEqual(rules(scan("-A " + ACCOUNT + "-gpu-gh200")), ["account"])
        self.assertEqual(rules(scan("-A " + PFX + "code2026-gpu")), ["account"])
        # a public name does not shield an account that follows it
        self.assertEqual(rules(scan(PFX + "rid-gpu-gh200 " + ACCOUNT)), ["account"])

    def test_a_private_path_after_an_allowed_name_is_still_refused(self):
        self.assertEqual(rules(scan("/proc/net/tcp " + PRIV)), ["path"])

    def test_extra_literals_from_the_environment(self):
        sc = scan("the grant " + SECRET.upper() + " ran", environ={"ACCOUNT_X": SECRET})
        self.assertEqual(rules(sc), ["extra"])
        self.assertEqual(rules(scan("x " + SECRET, environ={"PUBCHECK_EXTRA": "other " + SECRET})), ["extra"])
        self.assertEqual(rules(scan("abcdefgh", environ={"USER": "abcdefgh"})), ["extra"])
        self.assertEqual(scan("abc", environ={"USER": "abc"}).findings, [], "a short login is not a pattern")

    def test_addresses_only_with_ips_flag(self):
        text = "listen 127.0.0.1:80 and 10.20." + "30.40:80 and version 1.33.2 and 0.32.0+cu129"
        self.assertEqual(scan(text).findings, [])
        self.assertEqual(rules(scan(text, ips=True)), ["ip"])
        self.assertEqual(scan("only 127.0.0.1:80", ips=True).findings, [])


class BinaryTest(unittest.TestCase):
    def test_path_after_any_byte_is_found(self):
        # The leak in a .pyc follows a length byte, which can be a letter or a digit.
        for before in (b"Z", b"x", b"7", b"\x00", b"."):
            with self.subTest(before=before):
                self.assertEqual(rules(scan(b"\x01\x02" + before + PRIV.encode() + b"\x00\xff")), ["path"])

    def test_a_compiled_module_with_a_private_source_path(self):
        with tempfile.TemporaryDirectory() as d:
            src = os.path.join(d, "m.py")
            with open(src, "w") as f:
                f.write("x = 1\n")
            leaky, clean = os.path.join(d, "leaky.pyc"), os.path.join(d, "clean.pyc")
            py_compile.compile(src, cfile=leaky, dfile=PRIV, doraise=True)
            py_compile.compile(src, cfile=clean, dfile="m.py", doraise=True)
            with open(leaky, "rb") as f:
                self.assertEqual(rules(scan(f.read())), ["path"])
            with open(clean, "rb") as f:
                self.assertEqual(scan(f.read()).findings, [])


class ContainerTest(unittest.TestCase):
    def test_single_stream_compressors(self):
        raw = b"line\n" + PRIV.encode() + b"\n"
        cases = {"gz": gzip.compress(raw), "xz": lzma.compress(raw), "bz2": bz2.compress(raw)}
        z = zstd_compress(raw)
        if z is None:
            self.skipTest("no zstd decoder or encoder on this machine; the zstd case did not run")
        cases["zst"] = z
        for kind, blob in cases.items():
            with self.subTest(kind):
                sc = scan(blob)
                self.assertEqual(rules(sc), ["path"], "the contents of a %s file must be searched" % kind)
                self.assertEqual(sc.layers, {kind: 1})

    def test_clean_contents_pass_through_every_layer(self):
        sc = scan(gzip.compress(tar_of([("a.txt", b"nothing here"), ("b/c.txt", b"nor here")])))
        self.assertEqual((sc.findings, sc.errors, sc.layers), ([], [], {"gz": 1, "tar": 1}))

    def test_nested_containers(self):
        blob = gzip.compress(tar_of([("inner.gz", gzip.compress(b"x " + PRIV.encode()))]))
        sc = scan(blob)
        self.assertEqual(rules(sc), ["path"])
        self.assertEqual(sc.layers, {"gz": 2, "tar": 1})

    def test_tar_member_name_alone_is_refused(self):
        sc = scan(tar_of([(PRIV.lstrip("/"), b"clean body")]))  # an archive stores the name without its slash
        self.assertEqual(rules(sc), ["path"])
        self.assertEqual(scan(tar_of([("spikes/s2b/readme.txt", b"clean body")])).findings, [])

    def test_zip_members(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr("a.txt", "fine")
            z.writestr("b.txt", "x " + PRIV)
        sc = scan(buf.getvalue())
        self.assertEqual(rules(sc), ["path"])
        self.assertEqual(sc.layers, {"zip": 1})

    def test_corrupt_container_is_refused_not_passed(self):
        sc = scan(b"\x1f\x8b" + b"not really gzip data " * 5)
        self.assertEqual(sc.findings, [])
        self.assertEqual(len(sc.errors), 1)

    def test_nesting_beyond_the_limit_is_refused(self):
        blob = b"x " + PRIV.encode()
        for _ in range(pubcheck.MAX_DEPTH + 2):
            blob = gzip.compress(blob)
        sc = scan(blob)
        self.assertTrue(sc.errors, "layers that were not unpacked must be reported")

    def test_a_decompression_bomb_is_refused(self):
        old = pubcheck.MAX_BYTES
        pubcheck.MAX_BYTES = 1000
        try:
            sc = scan(gzip.compress(b"\0" * 5000))
        finally:
            pubcheck.MAX_BYTES = old
        self.assertEqual(len(sc.errors), 1)

    def test_labels_never_carry_member_names(self):
        sc = scan(tar_of([(PRIV.lstrip("/"), b"body " + PRIV.encode())]))
        for label, _, _, _ in sc.findings:
            self.assertNotIn(SECRET, label)


class CommandLineTest(unittest.TestCase):
    def run_cli(self, args, cwd=None, env=None):
        full = dict(os.environ, **(env or {}))
        full.pop("PUBCHECK_EXTRA", None)
        return subprocess.run([sys.executable, "-I", "-B", PUBCHECK, *args], cwd=cwd, env=full, capture_output=True, text=True)

    def test_exit_status_and_silence_about_the_match(self):
        with tempfile.TemporaryDirectory() as d:
            bad, good = os.path.join(d, "bad.txt.gz"), os.path.join(d, "good.txt")
            put(bad, gzip.compress(("x " + PRIV + " " + ACCOUNT).encode()))
            put(good, "nothing private\n")
            r = self.run_cli([good])
            self.assertEqual((r.returncode, r.stderr), (0, ""))
            r = self.run_cli([d])
            self.assertEqual(r.returncode, 1)
            self.assertIn("rule path", r.stderr)
            self.assertIn("rule account", r.stderr)
            self.assertNotIn(SECRET, r.stdout + r.stderr)
            self.assertEqual(self.run_cli([]).returncode, 2)

    def test_an_unreadable_path_is_not_a_pass(self):
        r = self.run_cli([os.path.join(tempfile.gettempdir(), "no-such-file-" + SECRET)])
        self.assertEqual(r.returncode, 2)

    def test_git_modes(self):
        with tempfile.TemporaryDirectory() as d:
            def git(*a):
                subprocess.run(["git", "-c", "commit.gpgsign=false", "-c", "user.name=t", "-c", "user.email=t@example.invalid",
                                *a], cwd=d, check=True, capture_output=True)
            git("init", "-q")
            put(os.path.join(d, ".gitignore"), "__pycache__/\n")
            put(os.path.join(d, "ok.txt"), "fine\n")
            git("add", ".")
            git("commit", "-q", "-m", "base")
            git("tag", "base")
            # commit 1 adds a leaking blob; commit 2 removes it again
            os.makedirs(os.path.join(d, "__pycache__"))
            put(os.path.join(d, "leak.bin"), b"\x00Z" + PRIV.encode() + b"\x00")
            git("add", "leak.bin")
            git("commit", "-q", "-m", "add")
            git("rm", "-q", "leak.bin")
            git("commit", "-q", "-m", "remove")
            # an ignored file with a leak is not committed, so --git does not read it
            put(os.path.join(d, "__pycache__", "m.pyc"), PRIV.encode())
            self.assertEqual(self.run_cli(["--git"], cwd=d).returncode, 0)
            self.assertEqual(self.run_cli(["--diff", "base"], cwd=d).returncode, 0, "the tip no longer holds the blob")
            r = self.run_cli(["--history", "base"], cwd=d)
            self.assertEqual(r.returncode, 1, "history still holds the blob: " + r.stderr)
            self.assertNotIn(SECRET, r.stdout + r.stderr)
            # an untracked file that is not ignored would be committed by git add -A
            put(os.path.join(d, "new.txt"), ACCOUNT)
            self.assertEqual(self.run_cli(["--git"], cwd=d).returncode, 1)

    def test_history_checks_commit_messages(self):
        with tempfile.TemporaryDirectory() as d:
            def git(*a):
                subprocess.run(["git", "-c", "commit.gpgsign=false", "-c", "user.name=t", "-c", "user.email=t@example.invalid",
                                *a], cwd=d, check=True, capture_output=True)
            git("init", "-q")
            git("commit", "-q", "--allow-empty", "-m", "base")
            git("tag", "base")
            git("commit", "-q", "--allow-empty", "-m", "wrote results under " + PRIV)
            r = self.run_cli(["--history", "base"], cwd=d)
            self.assertEqual(r.returncode, 1)
            self.assertNotIn(SECRET, r.stdout + r.stderr)


class CollectTest(unittest.TestCase):
    """collect.sh must not finish when what it keeps holds a private path, even inside a file it compresses."""

    def collect(self, leak_in=None):
        with tempfile.TemporaryDirectory() as raw, tempfile.TemporaryDirectory() as dest:
            arm = os.path.join(raw, "arms", "B1-rr")
            os.makedirs(arm)
            files = {"meta.json": "{}\n", "client.out": "RESULT ok\n", "client.jsonl": '{"session":0}\n',
                     "samples.tsv": "1.0\t0\tvllm:kv_cache_usage_perc\t0.1\n",
                     "before-w0.prom": "vllm:prefix_cache_hits_total 1\n",
                     "listeners.txt": "router 127.0.0.1:31010\nw0 10.9." + "8.7:5555\n"}
            if leak_in:
                files[leak_in] += "run from " + PRIV + "\n"
            for name, text in files.items():
                put(os.path.join(arm, name), text)
            r = subprocess.run(["bash", os.path.join(HERE, "collect.sh"), raw, os.path.join(dest, "out")],
                               capture_output=True, text=True)
            kept = os.path.join(dest, "out", "arms", "B1-rr", "listeners.txt")
            listeners = ""
            if os.path.exists(kept):
                with open(kept) as f:
                    listeners = f.read()
            return r, listeners

    def test_clean_results_are_collected_and_node_addresses_scrubbed(self):
        r, listeners = self.collect()
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("router 127.0.0.1:31010", listeners)
        self.assertNotIn("10.9.", listeners)

    def test_a_path_in_a_plain_file_is_refused(self):
        r, _ = self.collect("client.out")
        self.assertEqual(r.returncode, 1)
        self.assertNotIn(SECRET, r.stdout + r.stderr)

    def test_a_path_inside_a_file_that_collect_compresses_is_refused(self):
        r, _ = self.collect("client.jsonl")
        self.assertEqual(r.returncode, 1, "collect.sh gzips client.jsonl; the check must read inside the gzip")
        self.assertNotIn(SECRET, r.stdout + r.stderr)


if __name__ == "__main__":
    unittest.main()
