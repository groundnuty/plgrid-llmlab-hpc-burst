#!/usr/bin/env python3
"""The one change from S2b's publication check: the A100 partition's public name is allowed, and the
account rule still refuses the prefix anywhere else. S2b's own tests cover the rest of the copy."""
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
PFX = "p" + "l" + "g"


def check(text):
    with tempfile.TemporaryDirectory() as d:
        p = os.path.join(d, "f.txt")
        with open(p, "w") as f:
            f.write(text)
        env = {k: v for k, v in os.environ.items() if not k.startswith("ACCOUNT_") and k != "PUBCHECK_EXTRA"}
        env["USER"] = env["LOGNAME"] = "x"
        return subprocess.run([sys.executable, "-I", "-B", os.path.join(HERE, "pubcheck.py"), p], env=env, capture_output=True).returncode


fails = 0
for text, want in [("partition " + PFX + "rid-gpu-a100\n", 0), ("partition " + PFX + "rid-gpu-gh200\n", 0),
                   ("account " + PFX + "example-a100\n", 1), ("a path /" + "home/someone/x\n", 1)]:
    got = check(text)
    ok = got == want
    fails += not ok
    print(("ok  " if ok else "FAIL"), repr(text.strip()), "exit", got, "want", want)
sys.exit(1 if fails else 0)
