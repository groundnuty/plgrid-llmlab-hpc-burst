#!/usr/bin/env python3
"""Compare the Go modules in SPDX JSON SBOMs with the output of "go version -m <binary>".

The reference is the Go toolchain's own record of what it linked into the binary (the "dep"
lines, the "mod" line and the Go version on the first line). It does not come from either SBOM
tool, so agreement means something. Never pass one SBOM as the reference for another.

Usage:
  compare.py --reference go-version-m.txt --sbom bom=bom.spdx.json --sbom syft=syft.spdx.json

Exit status: 0 when every SBOM lists every reference entry at the reference version;
1 when any SBOM misses an entry, reports a different version, or (with --fail-on-extra)
lists a module the toolchain did not report; 2 on a malformed input.

A module the go command replaced ("dep A v1" followed by "=> B v2") is accepted under either
path and version, and reported as such.
"""
import argparse
import json
import sys
import urllib.parse


def parse_reference(text):
    """Return (go_version, main_module, main_version, deps).

    deps maps a module path to a list of acceptable (path, version) pairs: the module as the
    go.mod file names it, and its replacement if there is one.
    """
    lines = text.splitlines()
    if not lines or ":" not in lines[0]:
        raise ValueError("first line is not '<file>: <go version>'")
    go_version = lines[0].split(":", 1)[1].strip()
    if not go_version.startswith("go"):
        raise ValueError("first line has no Go version: %r" % lines[0])
    main_module = main_version = None
    deps = {}
    last = None
    for line in lines[1:]:
        if not line.startswith("\t"):
            continue
        fields = line.strip().split("\t")
        kind = fields[0]
        if kind == "mod":
            main_module = fields[1]
            main_version = fields[2] if len(fields) > 2 else ""
        elif kind == "dep":
            if len(fields) < 3:
                raise ValueError("dep line without a version: %r" % line)
            last = fields[1]
            deps[last] = [(fields[1], fields[2])]
        elif kind == "=>":
            if last is None or len(fields) < 3:
                raise ValueError("replacement line without a module: %r" % line)
            deps[last].append((fields[1], fields[2]))
    if main_module is None:
        raise ValueError("no 'mod' line: not a module-aware build")
    return go_version, main_module, main_version, deps


def parse_sbom(path):
    """Return (spdx_version, entries) where entries is a list of (name, version, purl_or_None)."""
    with open(path) as f:
        doc = json.load(f)
    entries = []
    for pkg in doc.get("packages", []):
        purl = None
        for ref in pkg.get("externalRefs", []):
            if ref.get("referenceType") == "purl":
                purl = ref.get("referenceLocator")
        entries.append((pkg.get("name", ""), pkg.get("versionInfo", ""), purl))
    return doc.get("spdxVersion"), entries


def purl_path_version(purl):
    """pkg:golang/<namespace>/<name>@<version> -> (path, version), or None for other types."""
    if not purl or not purl.startswith("pkg:golang/"):
        return None
    rest = purl[len("pkg:golang/"):].split("?", 1)[0].split("#", 1)[0]
    path, _, version = rest.rpartition("@")
    return urllib.parse.unquote(path), urllib.parse.unquote(version)


def strip_go(v):
    return v[2:] if v.startswith("go") else v


def compare(tool, entries, go_version, main_module, main_version, deps):
    """Return a dict with the findings for one SBOM."""
    by_name = {}
    for name, version, purl in entries:
        by_name.setdefault(name, []).append((version, purl))

    missing, wrong_version, replaced_ok = [], [], []
    purl_issues = []
    expected_names = set()

    def check(ref_name, accepted, label):
        for name, ver in accepted:
            expected_names.add(name)
        found = None
        for name, ver in accepted:
            for got_ver, purl in by_name.get(name, []):
                if ver is None or got_ver == ver or strip_go(got_ver) == strip_go(ver):
                    found = (name, ver, purl)
                    break
            if found:
                break
        if found:
            name, ver, purl = found
            if (name, ver) != accepted[0]:
                replaced_ok.append((ref_name, name, ver))
            pv = purl_path_version(purl)
            # Compare purls case-insensitively: the purl spec lower-cases Go paths.
            if pv is not None and (pv[0].lower() != name.lower() or (ver is not None and strip_go(pv[1]) != strip_go(ver))):
                purl_issues.append((label, name, ver, purl))
            return
        for name, ver in accepted:
            if name in by_name:
                wrong_version.append((ref_name, ver, [v for v, _ in by_name[name]]))
                return
        missing.append((ref_name, accepted[0][1]))

    # "(devel)" means the build stamped no version, so any version of the main module will do.
    check(main_module, [(main_module, None if main_version in ("", "(devel)") else main_version)], "main module")
    check("stdlib", [("stdlib", go_version)], "toolchain")
    for ref_name, accepted in sorted(deps.items()):
        check(ref_name, accepted, "dep")

    # Extra: golang packages (by purl or by being a module-shaped name) that the reference lacks.
    extra = []
    for name, version, purl in entries:
        if name in expected_names:
            continue
        if purl_path_version(purl) is not None:
            extra.append((name, version))
    return {
        "tool": tool,
        "packages": len(entries),
        "missing": missing,
        "wrong_version": wrong_version,
        "replaced_ok": replaced_ok,
        "extra": sorted(extra),
        "purl_issues": purl_issues,
        "main_module_present": main_module in by_name,
        "stdlib_present": "stdlib" in by_name,
    }


def main(argv):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--reference", required=True, help="output of: go version -m <binary>")
    ap.add_argument("--sbom", action="append", required=True, metavar="NAME=FILE",
                    help="an SPDX JSON SBOM; repeat for each tool")
    ap.add_argument("--fail-on-extra", action="store_true",
                    help="also fail when an SBOM lists a Go module the toolchain did not report")
    args = ap.parse_args(argv)

    try:
        with open(args.reference) as f:
            go_version, main_module, main_version, deps = parse_reference(f.read())
    except (OSError, ValueError) as e:
        print("compare: bad reference %s: %s" % (args.reference, e), file=sys.stderr)
        return 2
    print("reference: %s, main module %s %s, %d dependencies" % (go_version, main_module, main_version, len(deps)))

    failed = False
    for spec in args.sbom:
        tool, _, path = spec.partition("=")
        if not path:
            print("compare: --sbom wants NAME=FILE, got %r" % spec, file=sys.stderr)
            return 2
        try:
            spdx_version, entries = parse_sbom(path)
        except (OSError, ValueError) as e:
            print("compare: bad SBOM %s: %s" % (path, e), file=sys.stderr)
            return 2
        r = compare(tool, entries, go_version, main_module, main_version, deps)
        print("\n== %s (%s, %d packages)" % (tool, spdx_version, r["packages"]))
        print("  main module named:        %s" % ("yes" if r["main_module_present"] else "NO"))
        print("  Go standard library named: %s" % ("yes" if r["stdlib_present"] else "NO"))
        print("  missing (%d):" % len(r["missing"]))
        for name, ver in r["missing"]:
            print("    %s %s" % (name, ver))
        print("  wrong version (%d):" % len(r["wrong_version"]))
        for name, ver, got in r["wrong_version"]:
            print("    %s: reference %s, SBOM %s" % (name, ver, ",".join(got)))
        print("  replaced modules reported under the replacement (%d)" % len(r["replaced_ok"]))
        print("  extra Go packages not in the reference (%d):" % len(r["extra"]))
        for name, ver in r["extra"]:
            print("    %s %s" % (name, ver))
        print("  purl disagrees with name/version (%d)" % len(r["purl_issues"]))
        for item in r["purl_issues"]:
            print("    %s" % (item,))
        if r["missing"] or r["wrong_version"] or r["purl_issues"] or (args.fail_on_extra and r["extra"]):
            failed = True
            print("  RESULT: FAIL")
        else:
            print("  RESULT: ok, all %d reference entries present at the reference version" % (len(deps) + 2))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
