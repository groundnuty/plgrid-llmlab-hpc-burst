"""Tests for compare.py. The inputs are written by hand here, not produced by an SBOM tool or
by compare.py: the expected result of each case is stated in the case. The tests of
CommittedResultsTest start from the SBOMs and the "go version -m" output in results/ and change
them by hand; the expected result comes from the change, not from compare.py.

Run: python3 -m unittest -v test_compare   (from this directory)
"""
import copy
import json
import os
import subprocess
import sys
import tempfile
import unittest
import urllib.parse

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import compare  # noqa: E402

RESULTS = os.path.join(HERE, "results")

REFERENCE_HEAD = "hpcburst: go1.27.3\n\tpath\texample.test/m/cmd/x\n"
REFERENCE_DEPS = (
    "\tdep\texample.test/a\tv1.0.0\th1:aaa=\n"
    "\tdep\texample.test/b\tv2.0.0\th1:bbb=\n"
    "\t=>\texample.test/b-fork\tv2.0.1\th1:ccc=\n"          # a replacement under another path
    "\tdep\texample.test/Upper\tv0.1.0\th1:ddd=\n"
    "\tdep\texample.test/c\tv1.0.0\th1:eee=\n"
    "\t=>\texample.test/c\tv1.1.0\th1:fff=\n"                # a replacement under the same path
)
BUILD = "\tbuild\t-trimpath=true\n"
STAMP = "v0.0.0-20260101000000-abcdef123456"
# The build stamped no version.
REFERENCE = REFERENCE_HEAD + "\tmod\texample.test/m\t(devel)\t\n" + REFERENCE_DEPS + BUILD
# The build stamped a (pseudo-)version.
REFERENCE_STAMPED = REFERENCE_HEAD + "\tmod\texample.test/m\t" + STAMP + "\t\n" + REFERENCE_DEPS + BUILD


def write(path, text):
    with open(path, "w") as f:
        f.write(text)


def sbom(*pkgs, version="SPDX-2.3"):
    """pkgs: (name, version) pairs, each with a purl; a name starting with "!" gets no purl."""
    out = []
    for name, ver in pkgs:
        if name.startswith("!"):
            out.append({"name": name[1:], "versionInfo": ver})
            continue
        purl = "pkg:golang/%s%s" % (name.lower(), "@" + urllib.parse.quote(ver) if ver else "")
        out.append({"name": name, "versionInfo": ver,
                    "externalRefs": [{"referenceType": "purl", "referenceLocator": purl}]})
    return {"spdxVersion": version, "packages": out}


def full(main="(devel)"):
    """The packages of a correct SBOM for REFERENCE (or for REFERENCE_STAMPED with main=STAMP)."""
    return [("example.test/m", main), ("stdlib", "1.27.3"), ("example.test/a", "v1.0.0"),
            ("example.test/b-fork", "v2.0.1"), ("example.test/Upper", "v0.1.0"), ("example.test/c", "v1.1.0")]


FULL = full()


def run_cli(reference, doc, *extra):
    with tempfile.TemporaryDirectory() as d:
        ref, sb = os.path.join(d, "ref.txt"), os.path.join(d, "sbom.json")
        write(ref, reference)
        write(sb, json.dumps(doc))
        p = subprocess.run([sys.executable, "-I", os.path.join(HERE, "compare.py"), "--reference", ref,
                            "--sbom", "t=" + sb, *extra], capture_output=True, text=True)
    return p.returncode, p.stdout + p.stderr


def without(entries, name):
    return [p for p in entries if p[0] != name]


def swap(entries, name, version):
    return [(n, version if n == name else v) for n, v in entries]


class CompareTest(unittest.TestCase):
    def run_compare(self, doc, *extra, reference=REFERENCE):
        return run_cli(reference, doc, *extra)

    # ---- reference parsing ------------------------------------------------------------
    def test_reference_parsing(self):
        go, mod, ver, deps = compare.parse_reference(REFERENCE)
        self.assertEqual((go, mod, ver), ("go1.27.3", "example.test/m", "(devel)"))
        self.assertEqual(deps["example.test/b"], [("example.test/b", "v2.0.0"), ("example.test/b-fork", "v2.0.1")])
        self.assertEqual(deps["example.test/c"], [("example.test/c", "v1.0.0"), ("example.test/c", "v1.1.0")])
        self.assertEqual(len(deps), 4)

    def test_reference_with_two_replacements_for_one_dep_is_rejected(self):
        bad = REFERENCE.replace(BUILD, "\t=>\texample.test/zzz\tv9.9.9\th1:zzz=\n" + BUILD)
        rc, out = self.run_compare(sbom(*FULL), reference=bad)
        self.assertEqual(rc, 2, out)

    def test_reference_where_two_entries_collapse_to_one_module_is_rejected(self):
        # example.test/z is replaced by example.test/a, which is also a dependency: an SBOM cannot
        # tell the two entries apart, so the comparison refuses to run.
        bad = REFERENCE.replace(BUILD, "\tdep\texample.test/z\tv1.0.0\th1:zzz=\n\t=>\texample.test/a\tv1.0.0\th1:aaa=\n" + BUILD)
        rc, out = self.run_compare(sbom(*FULL), reference=bad)
        self.assertEqual(rc, 2, out)

    # ---- the basic cases --------------------------------------------------------------
    def test_complete_sbom_passes_and_replacements_are_accepted(self):
        rc, out = self.run_compare(sbom(*FULL), "--fail-on-extra")
        self.assertEqual(rc, 0, out)
        self.assertIn("replaced modules reported under the replacement (2)", out)

    def test_missing_module_fails(self):
        rc, out = self.run_compare(sbom(*without(FULL, "example.test/a")))
        self.assertEqual(rc, 1, out)
        self.assertIn("example.test/a v1.0.0", out)

    def test_wrong_version_fails(self):
        rc, out = self.run_compare(sbom(*swap(FULL, "example.test/a", "v9.9.9")))
        self.assertEqual(rc, 1, out)
        self.assertIn("reference v1.0.0, SBOM v9.9.9", out)

    def test_missing_stdlib_fails(self):
        rc, out = self.run_compare(sbom(*without(FULL, "stdlib")))
        self.assertEqual(rc, 1, out)

    def test_uppercase_path_matches_lowercased_purl(self):
        # sbom() lower-cases the purl as the purl spec does; the name keeps its case.
        rc, out = self.run_compare(sbom(*FULL), "--fail-on-extra")
        self.assertEqual(rc, 0, out)

    def test_malformed_inputs_exit_2(self):
        rc, out = self.run_compare(sbom(*FULL), reference="not a go version -m output\n")
        self.assertEqual(rc, 2, out)

    # ---- B1: replacements -------------------------------------------------------------
    def test_replacement_omitted_fails(self):
        # b => b-fork v2.0.1: the SBOM names the original b v2.0.0 and not the fork.
        doc = sbom(*without(FULL, "example.test/b-fork"), ("example.test/b", "v2.0.0"))
        for flags in ((), ("--fail-on-extra",)):
            rc, out = self.run_compare(doc, *flags)
            self.assertEqual(rc, 1, out)
            self.assertIn("example.test/b => example.test/b-fork v2.0.1 (the SBOM names the original", out)

    def test_replacement_at_wrong_version_fails(self):
        rc, out = self.run_compare(sbom(*swap(FULL, "example.test/b-fork", "v2.0.9")))
        self.assertEqual(rc, 1, out)
        self.assertIn("reference v2.0.1, SBOM v2.0.9", out)

    def test_replacement_missing_entirely_fails(self):
        rc, out = self.run_compare(sbom(*without(FULL, "example.test/b-fork")))
        self.assertEqual(rc, 1, out)

    def test_same_path_replacement_original_version_alone_fails(self):
        # c v1.0.0 => c v1.1.0: the binary has v1.1.0; an SBOM with v1.0.0 describes other code.
        rc, out = self.run_compare(sbom(*swap(FULL, "example.test/c", "v1.0.0")))
        self.assertEqual(rc, 1, out)
        self.assertIn("reference v1.1.0, SBOM v1.0.0", out)

    def test_same_path_replacement_listed_at_both_versions_fails(self):
        rc, out = self.run_compare(sbom(*FULL, ("example.test/c", "v1.0.0")))
        self.assertEqual(rc, 1, out)
        self.assertIn("listed twice at different versions", out)

    def test_original_of_a_replaced_module_is_tolerated_next_to_the_replacement(self):
        rc, out = self.run_compare(sbom(*FULL, ("example.test/b", "v2.0.0")), "--fail-on-extra")
        self.assertEqual(rc, 0, out)
        self.assertIn("originals of replaced modules also listed, tolerated (1)", out)

    def test_original_of_a_replaced_module_at_another_version_fails(self):
        rc, out = self.run_compare(sbom(*FULL, ("example.test/b", "v2.5.0")))
        self.assertEqual(rc, 1, out)

    # ---- B2: extra and conflicting entries ----------------------------------------------
    def test_extra_module_only_fails_with_flag(self):
        doc = sbom(*FULL, ("example.test/extra", "v1.2.3"))
        rc, out = self.run_compare(doc)
        self.assertEqual(rc, 0, out)
        self.assertIn("example.test/extra v1.2.3", out)
        rc, _ = self.run_compare(doc, "--fail-on-extra")
        self.assertEqual(rc, 1)

    def test_second_conflicting_version_fails_even_without_the_flag(self):
        for flags in ((), ("--fail-on-extra",)):
            rc, out = self.run_compare(sbom(*FULL, ("example.test/a", "v9.9.9")), *flags)
            self.assertEqual(rc, 1, out)
            self.assertIn("listed twice at different versions (1)", out)
            self.assertIn("example.test/a: reference v1.0.0, SBOM v1.0.0,v9.9.9", out)

    def test_second_conflicting_version_without_a_purl_fails(self):
        rc, out = self.run_compare(sbom(*FULL, ("!example.test/a", "v9.9.9")))
        self.assertEqual(rc, 1, out)

    def test_conflicting_entry_hiding_behind_another_name_fails(self):
        # The entry's name says "decoy" but its purl says example.test/a at v9.9.9.
        doc = sbom(*FULL)
        doc["packages"].append({"name": "decoy", "versionInfo": "v9.9.9", "externalRefs": [
            {"referenceType": "purl", "referenceLocator": "pkg:golang/example.test/a@v9.9.9"}]})
        rc, out = self.run_compare(doc)
        self.assertEqual(rc, 1, out)

    def test_module_shaped_entry_without_purl_is_extra(self):
        doc = sbom(*FULL, ("!example.test/sneaky", "v1.0.0"))
        rc, out = self.run_compare(doc)
        self.assertEqual(rc, 0, out)
        self.assertIn("example.test/sneaky v1.0.0 (no purl)", out)
        rc, _ = self.run_compare(doc, "--fail-on-extra")
        self.assertEqual(rc, 1)

    def test_versioned_entry_without_purl_or_dotted_name_is_extra(self):
        rc, out = self.run_compare(sbom(*FULL, ("!sneaky", "v1.0.0")), "--fail-on-extra")
        self.assertEqual(rc, 1, out)

    def test_file_package_of_the_scanned_binary_is_ignored(self):
        doc = sbom(*FULL)
        doc["packages"].append({"name": "hpcburst", "versionInfo": "sha256:" + "0" * 64, "primaryPackagePurpose": "FILE"})
        rc, out = self.run_compare(doc, "--fail-on-extra")
        self.assertEqual(rc, 0, out)
        self.assertIn("other entries ignored: 1 [hpcburst, FILE]", out)

    # ---- B3: no wildcard versions -------------------------------------------------------
    def test_unstamped_main_module_accepts_only_unversioned_entries(self):
        for main in ("", "(devel)"):
            rc, out = self.run_compare(sbom(*full(main=main)), "--fail-on-extra")
            self.assertEqual(rc, 0, (main, out))

    def test_unstamped_main_module_with_any_version_cannot_be_compared(self):
        rc, out = self.run_compare(sbom(*full(main="v999.0.0")))
        self.assertEqual(rc, 1, out)
        self.assertIn("version equality cannot be established (1)", out)
        self.assertIn("example.test/m: the build stamped no version, SBOM says v999.0.0", out)

    def test_stamped_main_module_must_match_exactly(self):
        rc, out = self.run_compare(sbom(*full(main=STAMP)), "--fail-on-extra", reference=REFERENCE_STAMPED)
        self.assertEqual(rc, 0, out)
        for wrong in ("v999.0.0", "", "(devel)", STAMP + "+dirty"):
            rc, out = self.run_compare(sbom(*full(main=wrong)), reference=REFERENCE_STAMPED)
            self.assertEqual(rc, 1, (wrong, out))

    def test_go_prefix_is_ignored_for_the_standard_library_only(self):
        for std in ("1.27.3", "go1.27.3"):
            rc, out = self.run_compare(sbom(*swap(FULL, "stdlib", std)))
            self.assertEqual(rc, 0, (std, out))
        rc, out = self.run_compare(sbom(*swap(FULL, "stdlib", "1.27.4")))
        self.assertEqual(rc, 1, out)
        # A dependency at "gov1.0.0" is not the dependency at "v1.0.0".
        rc, out = self.run_compare(sbom(*swap(FULL, "example.test/a", "gov1.0.0")))
        self.assertEqual(rc, 1, out)
        self.assertIn("reference v1.0.0, SBOM gov1.0.0", out)

    def test_purl_version_must_agree_too(self):
        doc = sbom(*FULL)
        for p in doc["packages"]:
            if p["name"] == "example.test/a":
                p["externalRefs"][0]["referenceLocator"] = "pkg:golang/example.test/a@gov1.0.0"
        rc, out = self.run_compare(doc)
        self.assertEqual(rc, 1, out)
        self.assertIn("purl disagrees with name/version (1)", out)


class CommittedResultsTest(unittest.TestCase):
    """Start from the tool-made SBOMs and the toolchain's output in results/, change one thing."""

    def setUp(self):
        with open(os.path.join(RESULTS, "go-version-m.txt")) as f:
            self.reference = f.read()
        self.sboms = {}
        for tool in ("bom", "syft"):
            with open(os.path.join(RESULTS, "%s-binary.spdx.json" % tool)) as f:
                self.sboms[tool] = json.load(f)

    def test_committed_sboms_pass_with_fail_on_extra(self):
        # The positive control for the mutation tests below.
        for tool, doc in self.sboms.items():
            rc, out = run_cli(self.reference, doc, "--fail-on-extra")
            self.assertEqual(rc, 0, (tool, out))
            self.assertIn("RESULT: ok, all 49 reference entries", out)

    def test_committed_sbom_plus_a_wrong_second_version_fails(self):
        for tool, doc in self.sboms.items():
            doc = copy.deepcopy(doc)
            victim = next(p for p in doc["packages"] if p["name"] == "k8s.io/api")
            twin = copy.deepcopy(victim)
            twin["versionInfo"] = "v0.1.0"
            twin["SPDXID"] = victim["SPDXID"] + "-twin"
            for ref in twin.get("externalRefs", []):
                if ref.get("referenceType") == "purl":
                    ref["referenceLocator"] = ref["referenceLocator"].replace("@" + victim["versionInfo"], "@v0.1.0")
            doc["packages"].append(twin)
            for flags in (("--fail-on-extra",), ()):
                rc, out = run_cli(self.reference, doc, *flags)
                self.assertEqual(rc, 1, (tool, flags, out))
                self.assertIn("k8s.io/api: reference %s, SBOM %s,v0.1.0" % ((victim["versionInfo"],) * 2), out)

    def test_committed_sbom_plus_a_module_without_a_purl_fails(self):
        for tool, doc in self.sboms.items():
            doc = copy.deepcopy(doc)
            doc["packages"].append({"name": "github.com/example/unlisted", "versionInfo": "v1.2.3"})
            rc, out = run_cli(self.reference, doc, "--fail-on-extra")
            self.assertEqual(rc, 1, (tool, out))
            self.assertIn("github.com/example/unlisted v1.2.3 (no purl)", out)


if __name__ == "__main__":
    unittest.main()
