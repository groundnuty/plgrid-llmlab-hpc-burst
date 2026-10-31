#!/usr/bin/env python3
"""Publication check: refuse files that carry private paths or accounts. Standard library only.

usage: pubcheck.py [--ips] PATH...           files, and every file under a directory
       pubcheck.py [--ips] --git [PATH...]    the files git would commit (tracked, plus untracked and not ignored)
       pubcheck.py [--ips] --diff BASE        the files changed in BASE...HEAD, read as committed in HEAD
       pubcheck.py [--ips] --history BASE     every blob of every commit in BASE..HEAD (what a push publishes)

Every file is searched as raw bytes, so a binary (a .pyc, an executable) is covered as well as text.
A file that is a gzip, bzip2, xz, zstd, zip or tar container is unpacked and its contents searched too,
to any depth up to a limit (members of a tar or zip are searched under their names as well).
A container that cannot be unpacked is refused: what cannot be seen is not a pass.

Refused:
  path     an absolute path under one of the home, Users, net or tmp directories (the system's own
           /proc/net/ is exempt)
  account  the account prefix of the grant system, except the public repository and partition
           names listed in ALLOWED
  extra    any literal in $PUBCHECK_EXTRA (whitespace separated), the values of $ACCOUNT_* variables,
           and the login name of the user running the check (when at least six characters)
  ip       with --ips: any IPv4 address except 127.0.0.1

A finding names the file, the rule and the position. It never prints the text that matched, and it
names a member of a container by its number, because member names can carry what is refused.
Exit status: 0 clean, 1 something refused, 2 usage error or a file that could not be read.
"""
import bz2
import gzip
import io
import lzma
import os
import re
import shutil
import subprocess
import sys
import tarfile
import zipfile

MAX_DEPTH = 5
MAX_BYTES = 512 << 20  # decompressed size limit per layer (a bomb is refused, not followed)
# Public names that would otherwise trip the account rule. The prefix is split so this file does
# not contain it either.
PFX = "p" + "l" + "g"
ALLOWED = [b"/proc/net/", (PFX + "rid-llmlab-hpc-burst").encode(), (PFX + "rid-gpu-gh200").encode()]
ALLOWED_RE = re.compile(b"|".join(re.escape(a) for a in ALLOWED))
RULES = [
    ("path", re.compile(rb"/(?:home|Users|net|tmp)/")),
    ("account", re.compile(PFX.encode())),
]
IP_RE = re.compile(rb"(?<![0-9.])(?:(?:25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])\.){3}(?:25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])(?![0-9])")


def extra_rules(environ):
    """Literal patterns from the environment: never hard-coded here, so this file stays publishable."""
    lits = set(environ.get("PUBCHECK_EXTRA", "").split())
    lits.update(v for k, v in environ.items() if k.startswith("ACCOUNT_") and v)
    login = environ.get("USER") or environ.get("LOGNAME") or ""
    if len(login) >= 6:
        lits.add(login)
    return [("extra", re.compile(re.escape(x.encode()), re.I)) for x in sorted(lits)]


class Scanner:
    def __init__(self, ips=False, environ=None):
        self.rules = list(RULES) + extra_rules(os.environ if environ is None else environ)
        self.ips = ips
        self.findings = []  # (label, rule, line, offset)
        self.errors = []  # (label, why)
        self.files = 0
        self.layers = {}  # container kind -> layers unpacked

    def search(self, label, data):
        clean = ALLOWED_RE.sub(lambda m: b" " * len(m.group()), data)
        rules = self.rules + ([("ip", IP_RE)] if self.ips else [])
        for name, rx in rules:
            for m in rx.finditer(clean):
                if name == "ip" and m.group() == b"127.0.0.1":
                    continue
                self.findings.append((label, name, data.count(b"\n", 0, m.start()) + 1, m.start()))

    def scan(self, label, data, depth=0):
        self.search(label, data)
        if depth >= MAX_DEPTH:
            if container_kind(data):
                self.errors.append((label, "containers nested deeper than %d" % MAX_DEPTH))
            return
        kind = container_kind(data)
        if not kind:
            return
        try:
            members = unpack(kind, data)
        except Exception as e:  # any failure to unpack leaves the contents unseen
            self.errors.append((label, "cannot unpack %s container (%s)" % (kind, type(e).__name__)))
            return
        self.layers[kind] = self.layers.get(kind, 0) + 1
        for i, (name, blob) in enumerate(members):
            inner = "%s|%s#%d" % (label, kind, i)
            # archives store absolute names without the leading slash, so search the name with one
            self.search(inner + " (member name)", b"/" + name.encode("utf-8", "replace"))
            self.scan(inner, blob, depth + 1)

    def scan_file(self, path, label=None):
        label = label or path
        try:
            if os.path.islink(path):
                data = os.readlink(path).encode()
            else:
                with open(path, "rb") as f:
                    data = f.read()
        except OSError as e:
            self.errors.append((label, "cannot read (%s)" % type(e).__name__))
            return
        self.files += 1
        self.scan(label, data)


def container_kind(data):
    if data[:2] == b"\x1f\x8b":
        return "gz"
    if data[:6] == b"\xfd7zXZ\x00":
        return "xz"
    if re.match(rb"BZh[1-9]1AY&SY", data[:10]):
        return "bz2"
    if data[:4] == b"\x28\xb5\x2f\xfd":
        return "zst"
    if data[:4] == b"PK\x03\x04":
        return "zip"
    if data[257:262] == b"ustar":
        return "tar"
    return ""


def bounded(stream):
    out = stream.read(MAX_BYTES + 1)
    if len(out) > MAX_BYTES:
        raise ValueError("decompressed size over the limit")
    return out


def unzstd(data):
    try:
        from compression import zstd  # Python 3.14 and later
        return bounded(zstd.ZstdFile(io.BytesIO(data)))
    except ImportError:
        exe = shutil.which("zstd")
        if not exe:
            raise RuntimeError("no zstd decoder available")
        p = subprocess.run([exe, "-dc", "--no-progress"], input=data, capture_output=True, check=True)
        if len(p.stdout) > MAX_BYTES:
            raise ValueError("decompressed size over the limit")
        return p.stdout


def unpack(kind, data):
    """Return [(member name, bytes)] of a container; a single-stream compressor has one nameless member."""
    if kind == "gz":
        return [("", bounded(gzip.GzipFile(fileobj=io.BytesIO(data))))]
    if kind == "xz":
        return [("", bounded(lzma.LZMAFile(io.BytesIO(data))))]
    if kind == "bz2":
        return [("", bounded(bz2.BZ2File(io.BytesIO(data))))]
    if kind == "zst":
        return [("", unzstd(data))]
    if kind == "zip":
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            return [(i.filename, bounded(z.open(i))) for i in z.infolist() if not i.is_dir()]
    with tarfile.open(fileobj=io.BytesIO(data)) as t:
        return [(m.name, bounded(t.extractfile(m))) for m in t.getmembers() if m.isfile()] + \
               [(m.name + " -> " + m.linkname, b"") for m in t.getmembers() if m.issym() or m.islnk()]


def git(*args):
    return subprocess.run(["git", *args], capture_output=True, check=True).stdout


def walk(paths):
    for p in paths:
        if os.path.isdir(p):
            for root, dirs, files in os.walk(p):
                dirs[:] = [d for d in dirs if d != ".git"]
                for f in sorted(files):
                    yield os.path.join(root, f)
        else:
            yield p


def report(sc, what):
    for label, rule, line, off in sc.findings:
        print("pubcheck: REFUSED %s: rule %s at line %d, byte %d" % (label, rule, line, off), file=sys.stderr)
    for label, why in sc.errors:
        print("pubcheck: UNREADABLE %s: %s" % (label, why), file=sys.stderr)
    layers = ", ".join("%s %d" % kv for kv in sorted(sc.layers.items())) or "none"
    print("pubcheck: %s: %d files, unpacked layers: %s; %d refused, %d unreadable"
          % (what, sc.files, layers, len({f[0] for f in sc.findings}), len(sc.errors)))
    return 2 if sc.errors and not sc.findings else 1 if sc.findings or sc.errors else 0


def blobs_of_history(base):
    """(blob id, path) for every blob introduced by the commits in base..HEAD, each id once."""
    listed = git("rev-list", "--objects", base + "..HEAD").decode().splitlines()
    shas = [ln.partition(" ")[0] for ln in listed]
    kinds = subprocess.run(["git", "cat-file", "--batch-check=%(objecttype)"], input="\n".join(shas).encode(),
                           capture_output=True, check=True).stdout.decode().split("\n")
    seen, out = set(), []
    for ln, kind in zip(listed, kinds):
        sha, _, name = ln.partition(" ")
        if kind == "blob" and sha not in seen:
            seen.add(sha)
            out.append((sha, name))
    return out


def main(argv):
    args = [a for a in argv[1:] if a != "--ips"]
    sc = Scanner(ips="--ips" in argv[1:])
    if args[:1] == ["--git"]:
        listed = git("ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", *args[1:]).split(b"\0")
        for f in sorted(f.decode() for f in listed if f and os.path.lexists(f.decode())):
            sc.search(f + " (file name)", f.encode())
            sc.scan_file(f)
        return report(sc, "files git would commit")
    if args[:1] == ["--diff"] and len(args) == 2:
        for f in git("diff", "-z", "--name-only", "--diff-filter=ACMR", args[1] + "...HEAD").split(b"\0"):
            if f:
                sc.files += 1
                sc.search(f.decode() + " (file name)", f)
                sc.scan(f.decode(), git("cat-file", "blob", "HEAD:" + f.decode()))
        return report(sc, "diff %s...HEAD" % args[1])
    if args[:1] == ["--history"] and len(args) == 2:
        sc.scan("commit messages", git("log", "--format=%B", args[1] + "..HEAD"))
        for sha, name in blobs_of_history(args[1]):
            sc.files += 1
            label = "blob %s" % sha[:10]
            sc.search(label + " (path of its first occurrence: " + name + ")", name.encode())
            sc.scan(label, git("cat-file", "blob", sha))
        return report(sc, "history %s..HEAD" % args[1])
    if not args or args[0].startswith("--"):
        print(__doc__, file=sys.stderr)
        return 2
    for f in walk(args):
        sc.scan_file(f)
    return report(sc, "paths")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
