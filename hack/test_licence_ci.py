"""Tests that the CI workflow runs the licence fixtures (hack/test-licence-fixtures.sh) with the same
go-licenses version as the licences job: a fixture run on another version would test another scanner.

Run: python3 -I -m unittest discover -s hack -p 'test_licence*.py' -v   (from the repo root)
"""
import os
import re
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


class CiWiringTest(unittest.TestCase):
    """The fixture test runs in the licence-modules job with the go-licenses the licences job runs."""

    def test_the_fixtures_and_the_check_use_the_same_go_licenses_version(self):
        with open(os.path.join(HERE, "..", ".github", "workflows", "ci.yml")) as f:
            text = f.read()
        versions = re.findall(r"^\s+GO_LICENSES_VERSION: (\S+)$", text, re.M)
        self.assertEqual(len(versions), 2, "one in licence-modules and one in licences: %r" % versions)
        self.assertEqual(len(set(versions)), 1, versions)
        self.assertIn('run: hack/test-licence-fixtures.sh "$(go env GOPATH)/bin/go-licenses"', text)


if __name__ == "__main__":
    unittest.main()
