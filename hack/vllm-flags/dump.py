#!/usr/bin/env python3
"""Dump vLLM's boolean command-line flags from the engine image's own parser.

Run it INSIDE the engine image, as `python3 -I dump.py ...`: it needs the
standard library and vllm only. hack/vllm-flags/generate.sh does that.

The controller renders a spec's boolean flags (internal/lifecycle/spec.go)
from the data file this writes, because vLLM's argparse refuses an explicit
value on any flag that takes none: `--disable-custom-all-reduce=true` fails
the engine at start, while `--disable-custom-all-reduce` works.

The parser is the real `vllm serve` parser: dump.py runs vllm's own CLI
entry point (vllm.entrypoints.cli.main.main) with argv ["vllm", "serve", "x"]
and takes the top-level parser at the moment main() first calls parse_args,
so the parser is built exactly as that version's own CLI builds it. It then
walks the serve sub-parser's actions and sorts every long option into one of
three kinds:

  store_true             argparse's _StoreTrueAction (no --no- form)
  store_false            argparse's _StoreFalseAction
  BooleanOptionalAction  argparse.BooleanOptionalAction, recorded under its
                         positive name; the --no- strings it generates are
                         dropped

It FAILS (non-zero exit, with a message) on anything it does not model: any
other action that takes no value, a store_true whose default is not False, a
store_false whose default is not True, two boolean actions with one dest, a
boolean name with an underscore or other odd character, a default that is not
a bool or None, and the optional-value shape (nargs="?" with const=True) on any
name but hf-token. It never relaxes a check: a failure is a finding to report.

Then, in the same run, it PROBES the parser. For every boolean row x it
parses, with the real parse_args (the top-level parser with "serve" first, as
`vllm serve` is run):

  --x          must parse, and set the dest to True (False for a store_false)
  --no-x       must parse and set the dest to False for a BooleanOptionalAction,
               and must FAIL for the other two kinds
  --x=true     must FAIL (this is the bug the data file exists to avoid)

Any surprise fails the run.

  python3 -I dump.py --expect-version V --image I --image-digest D \\
      --provenance TEXT --out FILE [--source-commit C] [--note TEXT]
  python3 -I dump.py --expect-version V --probe-argv FILE --out FILE \\
      [--probe-dests a,b] [--probe-expect JSON]
  python3 -I dump.py --check FILE

--probe-argv takes a JSON array of argument lists (what follows `vllm serve`),
parses each, and writes, for each, whether it parsed and the values of the
named dests, to --out. --probe-expect is a JSON object of dest values that
every parse must show. --check validates a data file without importing vllm.

The parser is built on vLLM's CPU platform (VLLM_TARGET_DEVICE=cpu, set here,
and ensure_a_platform for a version that ignores it): building it runs
VllmConfig's default factories, and those infer a device type and fail on a node
with no GPU. The flags do not depend on the platform: the
in-tree platforms' pre_register_and_update(parser) hook adds none, and the
options come from dataclass field types and explicit add_argument calls. The
probe below checks the result against the same parser it was built from.

vLLM logs to stdout, so results go to --out and never to stdout.
"""
import argparse
import contextlib
import io
import json
import os
import platform
import re
import sys

FORMAT = "hpcburst-engine-booleans/1"
KINDS = ("BooleanOptionalAction", "store_false", "store_true")
GENERATOR = "hack/vllm-flags/generate.sh"
OPTIONAL_VALUE_OK = frozenset({"hf-token"})  # a credential: refused by the API anyway
NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")
DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
# What the dest holds after the bare flag, by kind. Written here, not read from
# the action, so the probe is an independent statement of argparse's behaviour.
BARE_VALUE = {"store_true": True, "store_false": False, "BooleanOptionalAction": True}
MODEL = "/hpcburst/model"  # a stand-in for the positional the real command has


class Failure(Exception):
    """A finding that stops the run."""


class _Captured(BaseException):
    """Raised by the patched parse_args once the top-level parser is in hand."""


def classify(action):
    """The boolean kind of an action, by its exact class, or None."""
    t = type(action)
    if t is argparse._StoreTrueAction:
        return "store_true"
    if t is argparse._StoreFalseAction:
        return "store_false"
    if t is argparse.BooleanOptionalAction:
        return "BooleanOptionalAction"
    return None


def long_names(action, kind):
    """The long option names a boolean action is given under, without "--".

    A BooleanOptionalAction lists each name it was given plus a generated
    "--no-" twin; only the given names are returned. A "--no-" string is a twin
    when the string with that prefix removed is also an option string.
    """
    opts = list(action.option_strings)
    if kind == "BooleanOptionalAction":
        have = set(opts)
        opts = [o for o in opts if not (o.startswith("--no-") and "--" + o[5:] in have)]
    return [o[2:] for o in opts if o.startswith("--") and len(o) > 2]


def walk(serve):
    """Rows for every boolean long option of the serve parser; raises Failure."""
    rows = []
    dest_owner = {}
    for action in serve._actions:
        kind = classify(action)
        opts = list(action.option_strings)
        if kind is None:
            if type(action) is argparse._HelpAction and set(opts) <= {"-h", "--help"}:
                continue
            if action.nargs == 0:
                raise Failure(
                    "an action that takes no value and is none of the three kinds: %s %s"
                    % (type(action).__name__, opts)
                )
            if action.nargs == "?" and action.const is True:
                bad = [o for o in opts if o.startswith("--") and o[2:] not in OPTIONAL_VALUE_OK]
                if bad:
                    raise Failure(
                        "the optional-value shape (nargs='?', const=True) on %s: a bare flag "
                        "means True and a value means a string, which no boolean rule models" % bad
                    )
            continue
        default = action.default
        if kind == "store_true" and default is not False:
            raise Failure("store_true %s has default %r, not False: omitting it for \"false\" would be wrong" % (opts, default))
        if kind == "store_false" and default is not True:
            raise Failure("store_false %s has default %r, not True" % (opts, default))
        if kind == "BooleanOptionalAction" and not (default is None or isinstance(default, bool)):
            raise Failure("BooleanOptionalAction %s has default %r, neither a bool nor None" % (opts, default))
        names = long_names(action, kind)
        if not names:
            continue  # only a short option: no spec can name it
        for n in names:
            if not NAME_RE.match(n):
                raise Failure("boolean name %r is not lower-case letters, digits and '-'" % n)
        if action.dest in dest_owner:
            raise Failure("two boolean actions share the dest %r: %s and %s" % (action.dest, dest_owner[action.dest], names))
        dest_owner[action.dest] = names
        for n in names:
            rows.append({"name": n, "kind": kind, "default": default, "dest": action.dest})
    rows.sort(key=lambda r: r["name"])
    seen = set()
    for r in rows:
        if r["name"] in seen:
            raise Failure("boolean name %r appears twice" % r["name"])
        seen.add(r["name"])
    if not rows:
        raise Failure("no boolean flags found: the parser walk is wrong")
    return rows


def ensure_a_platform():
    """Make sure vLLM has a platform, the CPU one on a node with no GPU; returns its name.

    VLLM_TARGET_DEVICE=cpu does it in v0.32.0; an older vLLM ignores it and
    finds no platform. Then the CPU platform is put in place as vLLM's own
    `vllm bench` does on a node with no GPU (vllm/entrypoints/cli/main.py). It
    must be in place before the CLI modules are imported.
    """
    from vllm import platforms

    current = platforms.current_platform
    if getattr(current, "device_type", ""):
        return type(current).__name__
    from vllm.platforms.cpu import CpuPlatform

    platforms.current_platform = CpuPlatform()
    return "CpuPlatform (put in place, as `vllm bench` does)"


def capture_top_parser():
    """Run vllm's own CLI main() up to its first parse_args and return the top-level parser."""
    holder = {}
    holder["platform"] = ensure_a_platform()
    orig = argparse.ArgumentParser.parse_args

    def capture(self, args=None, namespace=None):
        holder["top"] = self
        raise _Captured()

    saved_argv = sys.argv
    argparse.ArgumentParser.parse_args = capture
    sys.argv = ["vllm", "serve", "x"]
    try:
        from vllm.entrypoints.cli.main import main

        try:
            main()
        except _Captured:
            pass
    finally:
        argparse.ArgumentParser.parse_args = orig
        sys.argv = saved_argv
    if "top" not in holder:
        raise Failure("vllm's main() returned without parsing: the CLI parser was not captured")
    print("dump.py: the parser was built on the platform %s" % holder["platform"], file=sys.stderr)
    return holder["top"]


def serve_parser_of(top):
    for a in top._actions:
        if isinstance(a, argparse._SubParsersAction):
            if "serve" not in a.choices:
                raise Failure("the CLI has no serve sub-parser")
            return a.choices["serve"]
    raise Failure("the CLI parser has no sub-parsers")


def parse(top, argv):
    """Parse `vllm serve <argv>` with the real parser: (namespace, None) or (None, why)."""
    err = io.StringIO()
    try:
        with contextlib.redirect_stderr(err):
            ns = top.parse_args(["serve", *argv])
    except SystemExit:
        lines = [l for l in err.getvalue().strip().splitlines() if l.strip()]
        return None, (lines[-1] if lines else "exit")[:240]
    except (ValueError, argparse.ArgumentError, argparse.ArgumentTypeError) as e:
        return None, ("%s: %s" % (type(e).__name__, e))[:240]
    return ns, None


def probe_rows(top, rows):
    """The acceptance probe: every boolean row, three command lines each."""
    bad = []
    for r in rows:
        n, dest, kind = r["name"], r["dest"], r["kind"]
        ns, why = parse(top, [MODEL, "--" + n])
        if ns is None:
            bad.append("--%s did not parse: %s" % (n, why))
        elif getattr(ns, dest, "<missing>") is not BARE_VALUE[kind]:
            bad.append("--%s left %s = %r, want %r" % (n, dest, getattr(ns, dest, "<missing>"), BARE_VALUE[kind]))
        ns, why = parse(top, [MODEL, "--no-" + n])
        if kind == "BooleanOptionalAction":
            if ns is None:
                bad.append("--no-%s did not parse: %s" % (n, why))
            elif getattr(ns, dest, "<missing>") is not False:
                bad.append("--no-%s left %s = %r, want False" % (n, dest, getattr(ns, dest, "<missing>")))
        elif ns is not None:
            bad.append("--no-%s parsed, but %s is a %s with no such form (the dest reads %r)" % (n, n, kind, getattr(ns, dest, "<missing>")))
        ns, _ = parse(top, [MODEL, "--%s=true" % n])
        if ns is not None:
            bad.append("--%s=true parsed: the bug this file exists for is not reproduced" % n)
    if bad:
        raise Failure("probe: %d surprises:\n  %s" % (len(bad), "\n  ".join(bad[:40])))


def jsonable(v):
    return v if v is None or isinstance(v, (bool, int, float, str)) else repr(v)


def probe_argv(top, path, dests, expect):
    with open(path, encoding="utf-8") as f:
        lines = json.load(f)
    if not isinstance(lines, list) or not all(isinstance(l, list) and all(isinstance(a, str) for a in l) for l in lines):
        raise Failure("--probe-argv wants a JSON array of arrays of strings")
    results, bad = [], []
    for argv in lines:
        ns, why = parse(top, list(argv))
        if ns is None:
            results.append({"argv": argv, "parsed": False, "error": why})
            bad.append("did not parse: %s" % why)
            continue
        vals = {d: jsonable(getattr(ns, d, "<missing>")) for d in dests}
        results.append({"argv": argv, "parsed": True, "values": vals})
        for d, want in expect.items():
            got = getattr(ns, d, "<missing>")
            if got != want or type(got) is not type(want):
                bad.append("%s = %r, want %r" % (d, got, want))
    return results, bad


def validate(doc):
    """Check a data file's shape; raises Failure. Stdlib only."""
    want = ["format", "engine", "version", "image", "imageDigest", "provenance", "python", "generator", "flags"]
    allowed = set(want) | {"sourceCommit", "note"}
    if not isinstance(doc, dict):
        raise Failure("the data file is not an object")
    missing = [k for k in want if k not in doc]
    extra = sorted(set(doc) - allowed)
    if missing or extra:
        raise Failure("data file fields: missing %s, unexpected %s" % (missing, extra))
    if doc["format"] != FORMAT:
        raise Failure("format %r, want %r" % (doc["format"], FORMAT))
    if doc["engine"] != "vllm":
        raise Failure("engine %r, want vllm" % (doc["engine"],))
    if not re.match(r"^[0-9]+\.[0-9]+\.[0-9]+$", str(doc["version"])):
        raise Failure("version %r is not x.y.z" % (doc["version"],))
    if not DIGEST_RE.match(str(doc["imageDigest"])):
        raise Failure("imageDigest %r is not sha256:<64 hex>" % (doc["imageDigest"],))
    for k in ("image", "provenance", "python", "generator"):
        if not isinstance(doc[k], str) or not doc[k]:
            raise Failure("%s is empty" % k)
    flags = doc["flags"]
    if not isinstance(flags, list) or not flags:
        raise Failure("flags is empty")
    names = []
    for f in flags:
        if not isinstance(f, dict) or sorted(f) != ["default", "kind", "name"]:
            raise Failure("a flag row is not {name, kind, default}: %r" % (f,))
        if f["kind"] not in KINDS:
            raise Failure("flag %r has kind %r" % (f["name"], f["kind"]))
        if not isinstance(f["name"], str) or not NAME_RE.match(f["name"]):
            raise Failure("flag name %r is not a dashed lower-case name" % (f["name"],))
        if not (f["default"] is None or isinstance(f["default"], bool)):
            raise Failure("flag %r has default %r" % (f["name"], f["default"]))
        if f["kind"] == "store_true" and f["default"] is not False:
            raise Failure("store_true %r has default %r" % (f["name"], f["default"]))
        if f["kind"] == "store_false" and f["default"] is not True:
            raise Failure("store_false %r has default %r" % (f["name"], f["default"]))
        names.append(f["name"])
    if names != sorted(set(names)):
        raise Failure("flag names are not sorted and unique")


def render(meta, rows):
    """The data file's text: one flag per line, keys in a fixed order."""
    head = [("format", FORMAT), ("engine", "vllm"), ("version", meta["version"]), ("image", meta["image"]),
            ("imageDigest", meta["image_digest"])]
    if meta.get("source_commit"):
        head.append(("sourceCommit", meta["source_commit"]))
    head.append(("provenance", meta["provenance"]))
    if meta.get("note"):
        head.append(("note", meta["note"]))
    head += [("python", platform.python_version()), ("generator", GENERATOR)]
    out = ["{"]
    for k, v in head:
        out.append("  %s: %s," % (json.dumps(k), json.dumps(v)))
    out.append('  "flags": [')
    for i, r in enumerate(rows):
        row = '{"name": %s, "kind": %s, "default": %s}' % (json.dumps(r["name"]), json.dumps(r["kind"]), json.dumps(r["default"]))
        out.append("    %s%s" % (row, "," if i < len(rows) - 1 else ""))
    out += ["  ]", "}", ""]
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--expect-version")
    ap.add_argument("--out")
    ap.add_argument("--image")
    ap.add_argument("--image-digest")
    ap.add_argument("--source-commit")
    ap.add_argument("--provenance")
    ap.add_argument("--note")
    ap.add_argument("--probe-argv")
    ap.add_argument("--probe-dests", default="")
    ap.add_argument("--probe-expect", default="{}")
    ap.add_argument("--check")
    a = ap.parse_args()
    try:
        if a.check:
            with open(a.check, encoding="utf-8") as f:
                validate(json.load(f))
            print("dump.py: the data file is valid", file=sys.stderr)
            return 0
        if not a.expect_version or not a.out:
            raise Failure("--expect-version and --out are required")
        if not a.probe_argv:
            for k in ("image", "image_digest", "provenance"):
                if not getattr(a, k):
                    raise Failure("--%s is required" % k.replace("_", "-"))
            if not DIGEST_RE.match(a.image_digest):
                raise Failure("--image-digest is not sha256:<64 hex>")
        os.environ["VLLM_TARGET_DEVICE"] = "cpu"
        try:
            import vllm
        except Exception as e:  # not a finding about flags: the image or the node cannot import vllm
            print("dump.py: IMPORT FAILED: %s: %s" % (type(e).__name__, e), file=sys.stderr)
            return 3

        if vllm.__version__ != a.expect_version:
            raise Failure("vllm.__version__ is %r, expected %r" % (vllm.__version__, a.expect_version))
        try:
            top = capture_top_parser()
        except ImportError as e:
            print("dump.py: IMPORT FAILED: %s: %s" % (type(e).__name__, e), file=sys.stderr)
            return 3
        serve = serve_parser_of(top)
        if a.probe_argv:
            dests = [d for d in a.probe_dests.split(",") if d]
            expect = json.loads(a.probe_expect)
            results, bad = probe_argv(top, a.probe_argv, dests, expect)
            with open(a.out, "w", encoding="utf-8") as f:
                json.dump({"version": vllm.__version__, "python": platform.python_version(), "results": results}, f, indent=1)
                f.write("\n")
            if bad:
                raise Failure("probe-argv: %s" % "; ".join(bad))
            print("dump.py: probe-argv: %d command lines parsed as expected" % len(results), file=sys.stderr)
            return 0
        rows = walk(serve)
        probe_rows(top, rows)
        counts = {k: sum(1 for r in rows if r["kind"] == k) for k in KINDS}
        meta = {"version": vllm.__version__, "image": a.image, "image_digest": a.image_digest,
                "source_commit": a.source_commit, "provenance": a.provenance, "note": a.note}
        text = render(meta, rows)
        validate(json.loads(text))
        with open(a.out, "w", encoding="utf-8") as f:
            f.write(text)
        print("dump.py: vllm %s, python %s, %d boolean names %s; probe passed" % (vllm.__version__, platform.python_version(), len(rows), counts), file=sys.stderr)
        boa = argparse.BooleanOptionalAction
        print("dump.py: the optional-boolean class is %s.%s" % (boa.__module__, boa.__qualname__), file=sys.stderr)
        return 0
    except Failure as e:
        print("dump.py: FAIL: %s" % e, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
