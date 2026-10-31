#!/usr/bin/env python3
"""Compare the Go modules in SPDX JSON SBOMs with the output of "go version -m <binary>".

The reference is the Go toolchain's own record of what it linked into the binary (the "dep"
lines, the "mod" line and the Go version on the first line). It does not come from either SBOM
tool, so agreement means something. Never pass one SBOM as the reference for another.

Usage:
  compare.py --reference go-version-m.txt --sbom bom=bom.spdx.json --sbom syft=syft.spdx.json

Exit status: 0 when every SBOM lists every reference entry at the reference version and has no
entry the reference cannot account for; 1 when it fails that; 2 on a malformed input.

What fails, always:
  - a reference entry the SBOM does not list, or lists at another version;
  - a module the SBOM lists twice at different versions (the SBOM then contradicts itself);
  - a purl that disagrees with the entry's name or version;
  - a main module whose version equality cannot be established (see below).
What fails only with --fail-on-extra:
  - a Go module entry the reference does not contain, with or without a purl.
Every Go-module entry of the SBOM is sorted into one of: matched, tolerated original of a
replaced module, conflicting, wrong version, or extra. An entry counts as a Go module when it
has a pkg:golang purl, or has no purl and a module-shaped name (its first path element holds a
dot) or a module-shaped version (v1.2.3). The FILE package that syft writes for the scanned
binary has neither and is ignored, and the output says so.

Replaced modules. "dep A v1" followed by "=> B v2" means the binary contains B v2. The SBOM
must identify B v2. An entry for A v1 does not stand for it. If the SBOM lists A v1 as well as
B v2 (a tool may record the original), that exact entry is tolerated and reported. Any other
version of A, or of B, is a conflict. No SBOM tool is known to write a machine-readable link
between the original and the replacement, so this program does not accept such evidence.

Versions. They are compared as exact strings, except for two cases:
  - the Go standard library: the toolchain says "go1.27.3" and the tools write "go1.27.3" or
    "1.27.3"; the "go" prefix is ignored for that one entry and for no other;
  - the main module when the build stamped no version ("(devel)" or empty in the reference):
    the SBOM entry must then say the same, i.e. have no version or "(devel)". Any other
    version cannot be compared with nothing, so the comparison reports that version equality
    cannot be established, and fails.
"""
import argparse
import collections
import json
import re
import sys
import urllib.parse

# What the reference and an SBOM may say for "no version stamped".
UNVERSIONED = ("", "(devel)")

Entry = collections.namedtuple("Entry", "name version purl purpose")
# name/version are the effective module: the replacement when there is one. original is the
# replaced (path, version), or None. version is None for an unstamped main module.
Expect = collections.namedtuple("Expect", "label ref_name name version original stdlib")


def parse_reference(text):
    """Return (go_version, main_module, main_version, deps).

    deps maps a module path to a list of (path, version) pairs: the module as the go.mod file
    names it, and its replacement if there is one. The last pair is what the binary contains.
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
            if len(deps[last]) > 1:
                raise ValueError("two replacement lines for %s" % last)
            deps[last].append((fields[1], fields[2]))
    if main_module is None:
        raise ValueError("no 'mod' line: not a module-aware build")
    return go_version, main_module, main_version, deps


def build_expectations(go_version, main_module, main_version, deps):
    """Return the list of Expect the SBOM must satisfy. Raises ValueError when two reference
    entries would be indistinguishable in an SBOM."""
    exps = [Expect("main module", main_module, main_module,
                   None if main_version in UNVERSIONED else main_version, None, False),
            Expect("toolchain", "stdlib", "stdlib", go_version, None, True)]
    for ref_name, accepted in sorted(deps.items()):
        eff = accepted[-1]
        original = accepted[0] if len(accepted) > 1 else None
        exps.append(Expect("dep", ref_name, eff[0], eff[1], original, False))
    seen = {}
    for x in exps:
        keys = [x.name.lower()]
        if x.original and x.original[0] != x.name:
            keys.append(x.original[0].lower())
        for key in keys:
            if key in seen and seen[key] != x.ref_name:
                raise ValueError("%s and %s resolve to the same module path %s; an SBOM cannot tell them apart"
                                 % (seen[key], x.ref_name, key))
            seen[key] = x.ref_name
    return exps


def parse_sbom(path):
    """Return (spdx_version, entries) where entries is a list of Entry."""
    with open(path) as f:
        doc = json.load(f)
    entries = []
    for pkg in doc.get("packages", []):
        purl = None
        for ref in pkg.get("externalRefs", []):
            if ref.get("referenceType") == "purl":
                purl = ref.get("referenceLocator")
        entries.append(Entry(pkg.get("name", ""), pkg.get("versionInfo", ""), purl,
                             pkg.get("primaryPackagePurpose")))
    return doc.get("spdxVersion"), entries


def purl_path_version(purl):
    """pkg:golang/<namespace>/<name>[@<version>] -> (path, version), or None for other types."""
    if not purl or not purl.startswith("pkg:golang/"):
        return None
    rest = purl[len("pkg:golang/"):].split("?", 1)[0].split("#", 1)[0]
    path, at, version = rest.rpartition("@")
    if not at:
        path, version = version, ""
    return urllib.parse.unquote(path), urllib.parse.unquote(version)


def strip_go(v):
    """Only ever applied to the standard library's version."""
    return v[2:] if v.startswith("go") else v


def version_ok(exp, got):
    if exp.version is None:
        return got in UNVERSIONED
    if exp.stdlib:
        return strip_go(got) == strip_go(exp.version)
    return got == exp.version


def is_go_module(entry, purl_pv):
    """Does this SBOM entry claim to be a Go module (or the standard library)?"""
    if purl_pv is not None or entry.name == "stdlib":
        return True
    if entry.purpose == "FILE":
        return False  # the scanned binary itself
    if "." in entry.name.split("/", 1)[0]:
        return True
    return entry.purl is None and re.match(r"v\d+\.\d+\.\d+", entry.version) is not None


def compare(tool, entries, exps):
    """Return a dict with the findings for one SBOM."""
    by_key, orig_key = {}, {}
    for i, x in enumerate(exps):
        by_key[x.name.lower()] = i
        if x.original and x.original[0].lower() != x.name.lower():
            orig_key[x.original[0].lower()] = i
    matched = [[] for _ in exps]
    other = [[] for _ in exps]       # entries of an expected module at an unaccepted version
    originals = [[] for _ in exps]   # tolerated entries of the replaced original
    extra, ignored, purl_issues = [], [], []
    module_entries = 0
    for e in entries:
        pv = purl_path_version(e.purl)
        if not is_go_module(e, pv):
            ignored.append(e)
            continue
        module_entries += 1
        keys = [e.name.lower()] + ([pv[0].lower()] if pv else [])
        idx = next((by_key[k] for k in keys if k in by_key), None)
        oidx = next((orig_key[k] for k in keys if k in orig_key), None)
        if idx is not None:
            x = exps[idx]
            if e.name == x.name and version_ok(x, e.version):
                matched[idx].append(e)
                # Compare purls case-insensitively: the purl spec lower-cases Go paths.
                if pv is not None and (pv[0].lower() != x.name.lower() or not version_ok(x, pv[1])):
                    purl_issues.append((x.label, e.name, e.version, e.purl))
            else:
                other[idx].append(e)
        elif oidx is not None:
            if (e.name, e.version) == exps[oidx].original:
                originals[oidx].append(e)
            else:
                other[oidx].append(e)
        else:
            extra.append(e)

    missing, wrong_version, conflicting, unestablished, replaced_ok = [], [], [], [], []
    accounted = 0
    for i, x in enumerate(exps):
        shown = x.version if x.original is None else "=> %s %s" % (x.name, x.version)
        got = [e.version for e in other[i]]
        if matched[i]:
            accounted += len(matched[i]) + len(originals[i])
            if got:
                conflicting.append((x.ref_name, x.version, [matched[i][0].version] + got))
            if x.original is not None:
                replaced_ok.append((x.ref_name, x.name, x.version))
        elif got and x.version is None:
            unestablished.append((x.ref_name, got))
        elif got:
            wrong_version.append((x.ref_name, x.version, got))
        else:
            if originals[i]:
                shown += " (the SBOM names the original %s %s only)" % x.original
            missing.append((x.ref_name, shown))
    return {
        "tool": tool,
        "packages": len(entries),
        "module_entries": module_entries,
        "accounted": accounted,
        "ignored": ignored,
        "missing": missing,
        "wrong_version": wrong_version,
        "conflicting": conflicting,
        "unestablished": unestablished,
        "replaced_ok": replaced_ok,
        "originals": sorted((e.name, e.version) for lst in originals for e in lst),
        "extra": sorted((e.name, e.version, e.purl is None) for e in extra),
        "purl_issues": purl_issues,
        "main_module_present": bool(matched[0] or other[0]),
        "stdlib_present": any(e.name == "stdlib" for e in entries),
    }


def report(r, n_ref, fail_on_extra):
    """Print the findings and return True when this SBOM fails."""
    print("\n== %s (%s, %d packages)" % (r["tool"], r["spdx_version"], r["packages"]))
    print("  main module named:        %s" % ("yes" if r["main_module_present"] else "NO"))
    print("  Go standard library named: %s" % ("yes" if r["stdlib_present"] else "NO"))
    print("  Go module entries: %d, accounted for: %d; other entries ignored: %d%s" % (
        r["module_entries"], r["accounted"], len(r["ignored"]),
        "".join(" [%s, %s]" % (e.name, e.purpose) for e in r["ignored"])))
    print("  missing (%d):" % len(r["missing"]))
    for name, ver in r["missing"]:
        print("    %s %s" % (name, ver))
    print("  wrong version (%d):" % len(r["wrong_version"]))
    for name, ver, got in r["wrong_version"]:
        print("    %s: reference %s, SBOM %s" % (name, ver, ",".join(got)))
    print("  listed twice at different versions (%d):" % len(r["conflicting"]))
    for name, ver, got in r["conflicting"]:
        print("    %s: reference %s, SBOM %s" % (name, ver, ",".join(got)))
    print("  version equality cannot be established (%d):" % len(r["unestablished"]))
    for name, got in r["unestablished"]:
        print("    %s: the build stamped no version, SBOM says %s" % (name, ",".join(got)))
    print("  replaced modules reported under the replacement (%d)" % len(r["replaced_ok"]))
    print("  originals of replaced modules also listed, tolerated (%d)" % len(r["originals"]))
    print("  extra Go module entries not in the reference (%d):" % len(r["extra"]))
    for name, ver, nopurl in r["extra"]:
        print("    %s %s%s" % (name, ver, " (no purl)" if nopurl else ""))
    print("  purl disagrees with name/version (%d)" % len(r["purl_issues"]))
    for item in r["purl_issues"]:
        print("    %s" % (item,))
    bad = (r["missing"] or r["wrong_version"] or r["conflicting"] or r["unestablished"] or r["purl_issues"]
           or (fail_on_extra and r["extra"]))
    if bad:
        print("  RESULT: FAIL")
    else:
        print("  RESULT: ok, all %d reference entries present at the reference version" % n_ref)
    return bool(bad)


def main(argv):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--reference", required=True, help="output of: go version -m <binary>")
    ap.add_argument("--sbom", action="append", required=True, metavar="NAME=FILE",
                    help="an SPDX JSON SBOM; repeat for each tool")
    ap.add_argument("--fail-on-extra", action="store_true",
                    help="also fail when an SBOM lists a Go module entry the toolchain did not report")
    args = ap.parse_args(argv)

    try:
        with open(args.reference) as f:
            go_version, main_module, main_version, deps = parse_reference(f.read())
        exps = build_expectations(go_version, main_module, main_version, deps)
    except (OSError, ValueError) as e:
        print("compare: bad reference %s: %s" % (args.reference, e), file=sys.stderr)
        return 2
    print("reference: %s, main module %s %s, %d dependencies" % (
        go_version, main_module, main_version or "(no version)", len(deps)))

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
        r = compare(tool, entries, exps)
        r["spdx_version"] = spdx_version
        failed = report(r, len(exps), args.fail_on_extra) or failed
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
