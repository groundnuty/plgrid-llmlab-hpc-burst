#!/usr/bin/env python3
"""Print a table of s2probe stream results: summarize.py DIR [DIR...] (reads every *.json with a 'complete' key)."""
import json
import pathlib
import sys

rows = []
for d in sys.argv[1:]:
    for p in sorted(pathlib.Path(d).glob("*.json")):
        try:
            r = json.loads(p.read_text())
        except ValueError:
            continue
        if "complete" not in r or "tokens" not in r:
            continue
        rows.append(r)
print(f"{'label':28} {'complete':8} {'tokens':>13} {'elapsed_s':>10} {'cut_after_s':>11} {'max_gap_ms':>10} {'gaps>2s':>7}  error")
for r in rows:
    print(f"{r['label']:28} {str(r['complete']):8} {r['tokens']:>6}/{r['expect_tokens']:<6} {r['elapsed_sec']:>10.0f} "
          f"{r.get('cut_after_sec', 0):>11.0f} {r.get('gap_max_ms', 0):>10.0f} {r.get('gaps_over_2s', 0):>7}  {r.get('error', '')}")
