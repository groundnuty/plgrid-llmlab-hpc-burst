"""Tests for compare.py. The inputs are written by hand here, not produced by an SBOM tool or
by compare.py: the expected result of each case is stated in the case.

Run: python3 -m unittest -v test_compare   (from this directory)
"""
import json
import os
import subprocess
import sys
import tempfile
import unittest

import compare

HERE = os.path.dirname(os.path.abspath(__file__))

REFERENCE = (
    "hpcburst: go1.27.3\n"
    "\tpath\texample.test/m/cmd/x\n"
    "\tmod\texample.test/m\t(devel)\t\n"
    "\tdep\texample.test/a\tv1.0.0\th1:aaa=\n"
    "\tdep\texample.test/b\tv2.0.0\th1:bbb=\n"
    "\t=>\texample.test/b-fork\tv2.0.1\th1:ccc=\n"
    "\tdep\texample.test/Upper\tv0.1.0\th1:ddd=\n"
    "\tbuild\t-trimpath=true\n"
)


def write(path, text):
    with open(path, "w") as f:
        f.write(text)


def sbom(*pkgs, version="SPDX-2.3"):
    out = []
    for name, ver in pkgs:
        purl = "pkg:golang/%s@%s" % (name.lower(), ver)
        out.append({"name": name, "versionInfo": ver,
                    "externalRefs": [{"referenceType": "purl", "referenceLocator": purl}]})
    return {"spdxVersion": version, "packages": out}


FULL = [("example.test/m", "v0.0.0-x"), ("stdlib", "1.27.3"), ("example.test/a", "v1.0.0"),
        ("example.test/b-fork", "v2.0.1"), ("example.test/Upper", "v0.1.0")]


class CompareTest(unittest.TestCase):
    def run_compare(self, doc, *extra):
        with tempfile.TemporaryDirectory() as d:
            ref, sb = os.path.join(d, "ref.txt"), os.path.join(d, "sbom.json")
            write(ref, REFERENCE)
            write(sb, json.dumps(doc))
            p = subprocess.run([sys.executable, os.path.join(HERE, "compare.py"), "--reference", ref,
                                "--sbom", "t=" + sb, *extra], capture_output=True, text=True)
        return p.returncode, p.stdout

    def test_reference_parsing(self):
        go, mod, ver, deps = compare.parse_reference(REFERENCE)
        self.assertEqual((go, mod, ver), ("go1.27.3", "example.test/m", "(devel)"))
        self.assertEqual(deps["example.test/b"], [("example.test/b", "v2.0.0"), ("example.test/b-fork", "v2.0.1")])
        self.assertEqual(len(deps), 3)

    def test_complete_sbom_passes_and_replacement_is_accepted(self):
        rc, out = self.run_compare(sbom(*FULL))
        self.assertEqual(rc, 0, out)
        self.assertIn("replaced modules reported under the replacement (1)", out)

    def test_missing_module_fails(self):
        rc, out = self.run_compare(sbom(*[p for p in FULL if p[0] != "example.test/a"]))
        self.assertEqual(rc, 1, out)
        self.assertIn("example.test/a v1.0.0", out)

    def test_wrong_version_fails(self):
        rc, out = self.run_compare(sbom(*[(n, "v9.9.9" if n == "example.test/a" else v) for n, v in FULL]))
        self.assertEqual(rc, 1, out)
        self.assertIn("reference v1.0.0, SBOM v9.9.9", out)

    def test_missing_stdlib_fails(self):
        rc, out = self.run_compare(sbom(*[p for p in FULL if p[0] != "stdlib"]))
        self.assertEqual(rc, 1, out)

    def test_extra_module_only_fails_with_flag(self):
        doc = sbom(*FULL, ("example.test/extra", "v1.2.3"))
        rc, out = self.run_compare(doc)
        self.assertEqual(rc, 0, out)
        self.assertIn("example.test/extra v1.2.3", out)
        rc, _ = self.run_compare(doc, "--fail-on-extra")
        self.assertEqual(rc, 1)

    def test_uppercase_path_matches_lowercased_purl(self):
        # sbom() lower-cases the purl as the purl spec does; the name keeps its case.
        rc, out = self.run_compare(sbom(*FULL))
        self.assertEqual(rc, 0, out)

    def test_malformed_inputs_exit_2(self):
        with tempfile.TemporaryDirectory() as d:
            ref = os.path.join(d, "ref.txt")
            write(ref, "not a go version -m output\n")
            sb = os.path.join(d, "s.json")
            write(sb, json.dumps(sbom(*FULL)))
            p = subprocess.run([sys.executable, os.path.join(HERE, "compare.py"), "--reference", ref,
                                "--sbom", "t=" + sb], capture_output=True, text=True)
        self.assertEqual(p.returncode, 2, p.stderr)


if __name__ == "__main__":
    unittest.main()
