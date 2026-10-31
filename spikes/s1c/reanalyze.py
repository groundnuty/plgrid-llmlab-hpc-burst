#!/usr/bin/env python3
"""Spike S1c: re-derive the review's corrected columns from the raw data the runs left behind, and
rewrite results/*.csv with them. Nothing is re-measured.

    S1C_SCRATCH=<dir> python3 reanalyze.py

- matrix.csv: CPU per 1,000 tokens/s over ONE interval (the load run, from its raw JSON: wall time
  and every token that arrived), and "wire bytes" renamed to TCP payload bytes.
- hol.csv: whether the bulk injection did what its variant needs (status, full body, or a slow
  reader abandoned by design), and the stall measured from the bulk start to 5 s after its end.
- heartbeat.csv: detection only from explicit offline/absent API observations, API errors counted,
  the probes cut by their own 15 s deadline counted.

A raw file is used only when it reproduces the CSV row it belongs to (retries overwrite a cell's
raw file, so only the last attempt of each cell has one); other rows get blanks.
"""
import csv
import glob
import json
import os
import sys

from s1c_analysis import FRONT, PATH, detection, hol_validity, per_ktok
from s1c_heartbeat import read_probe

HERE = os.path.dirname(os.path.abspath(__file__))
RES = os.path.join(HERE, "results")
RAW = os.path.join(os.environ.get("S1C_SCRATCH") or os.path.join(os.environ.get("TMPDIR", "/tmp"), "s1c"), "raw")
HOL_VARIANTS = {"bulk": (4 << 20, 0, 20), "slow": (64 << 20, 64 << 10, 12), "nocap": (4 << 20, 0, 20)}


def load_csv(name):
    with open(os.path.join(RES, name), newline="") as fh:
        return list(csv.DictReader(fh))


def save_csv(name, rows):
    with open(os.path.join(RES, name), "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def matrix():
    out, matched = [], 0
    for r in load_csv("matrix.csv"):
        r = {("tcp_payload_B_tok" if k == "wire_B_tok" else k): v for k, v in r.items()}
        path = os.path.join(RAW, "p%s_%s_%s_%s.json" % (r["pass_"], r["arm"], r["cond"], r["n"]))
        run_s = run_tokens = front = whole = ""
        if os.path.exists(path):
            with open(path) as fh:
                j = json.load(fh)
            if (round(j["ttft_ms"]["p50"], 1) == num(r["ttft_p50"]) and round(j["tokens_per_s"], 1) == num(r["tok_s"])
                    and j["requests"] == int(r["requests"])):
                matched += 1
                run_s, run_tokens = round(j["wall_s"], 2), j["all_tokens"]
                if FRONT[r["arm"]]:
                    f = sum(num(r.get("cpu_" + k)) for k in FRONT[r["arm"]])
                    p = sum(num(r.get("cpu_" + k)) for k in PATH)
                    front = round(per_ktok(f, run_s, run_tokens), 2)
                    whole = round(per_ktok(p, run_s, run_tokens), 2)
        r.update(run_s=run_s, run_tokens=run_tokens, meter_s="", front_cpu_per_ktok=front, path_cpu_per_ktok=whole)
        out.append(r)
    save_csv("matrix.csv", out)
    print("matrix.csv: %d rows, %d reproduced by their raw JSON and given run-window CPU per token" % (len(out), matched))


def path_unmatched(path, r):
    if not os.path.exists(path):
        return True
    with open(path) as fh:
        j = json.load(fh)
    return not (round(j["bulk_Bps"]) == round(num(r["bulk_Bps"])) and j["bulk_bytes_read"] == int(num(r["bulk_read"])))


def hol():
    out, matched, invalid = [], 0, []
    for r in load_csv("hol.csv"):
        nbytes, rate, gave = HOL_VARIANTS[r["variant"]]
        tag = "hol_p%s_%s_%s" % (r["pass_"], r["config"], r["variant"])
        summary = {"bulk_err": "", "bulk_bytes_read": int(num(r["bulk_read"]))}
        stall = max(num(r["during_late_max"]), num(r["after_late_max"]))
        path = os.path.join(RAW, tag + ".json")
        if os.path.exists(path):
            with open(path) as fh:
                j = json.load(fh)
            if round(j["bulk_Bps"]) == round(num(r["bulk_Bps"])) and j["bulk_bytes_read"] == int(num(r["bulk_read"])):
                matched += 1
                summary["bulk_err"] = j.get("bulk_err") or ""
                lo, hi = int(j["bulk_start_s"] / 0.25), int((j["bulk_end_s"] + 5) / 0.25) + 1
                stall = max(stall, max(j["max_lateness_ms_per_250ms"][lo:hi] or [0]))
        if summary["bulk_err"] == "" and rate and path_unmatched(path, r):
            # This attempt's raw file was overwritten by a later one. A slow reader that stopped short
            # of the reply after about rate x give-up bytes was abandoned by design.
            got = summary["bulk_bytes_read"]
            if got < nbytes and 0.7 * rate * gave <= got <= 1.1 * rate * gave:
                summary["bulk_err"] = "gave up (inferred from the bytes read; raw file overwritten)"
        valid = hol_validity(summary, nbytes, rate, gave) or "yes"
        if valid != "yes":
            invalid.append((tag, valid))
        log = os.path.join(RAW, tag + ".frps-frp.log")
        frps_hb = ""
        if os.path.exists(log):
            with open(log, errors="replace") as fh:
                frps_hb = sum(1 for l in fh if "heartbeat timeout" in l)
        r.update(bulk_err=summary["bulk_err"][:100], valid=valid, frps_hb_timeouts=frps_hb,
                 stall_late_max=round(stall, 1), stall_gap_max=round(max(num(r["during_gap_max"]), num(r["after_gap_max"])), 1))
        out.append(r)
    save_csv("hol.csv", out)
    print("hol.csv: %d rows, %d reproduced by their raw JSON; invalid: %s" % (len(out), matched, invalid or "none"))


def heartbeat():
    out, ok, notes = [], 0, []
    all_api = set()
    for r in load_csv("heartbeat.csv"):
        rows = read_probe(os.path.join(RAW, "hb_%s_%s_%s.csv" % (r["setting"], r["scenario"], r["rep"])))
        all_api |= {o for k, _, _, o, _ in rows if k == "api"}
        # t0 is not stored: recover it from the first offline/absent observation and its recorded delay
        offl = [a for k, a, _, o, _ in rows if k == "api" and o in ("offline", "absent")]
        t0 = offl[0] - num(r["api_gone_s"]) if offl else None
        heal = None
        if r["heal_online_s"] not in ("", None) and offl:
            on = [a for k, a, _, o, _ in rows if k == "api" and o == "online" and a > offl[0]]
            heal = on[0] - num(r["heal_online_s"]) if on else None
        d = detection(rows, t0, heal) if t0 is not None else None
        # t0 comes from a value rounded to 0.01 s, so two roundings separate the recomputed times
        same = d is not None and abs((d["api_gone"] - t0) - num(r["api_gone_s"])) < 0.011 and d["hung_n"] == int(r["hung_n"]) \
            and (r["first_refused_s"] == "" or abs((d["first_refused"] - t0) - num(r["first_refused_s"])) < 0.011)
        ok += same
        if not same:
            notes.append("%s/%s/%s" % (r["setting"], r["scenario"], r["rep"]))
        times = [num(r[k]) for k in ("api_gone_s", "log_gone_s", "first_refused_s") if r[k] != ""]
        valid = "" if d and d["api_gone"] is not None else "censored: no offline/absent observation"
        if not valid and max(times) - min(times) > 0.5:
            valid = "inconsistent: API, log and first refusal differ by %.2f s" % (max(times) - min(times))
        new = dict(r)
        new.update(api_errors=d["api_errors"] if d else "", hung_timeouts=d["hung_timeouts"] if d else "",
                   hung_max_s=round(d["hung_max"], 2) if d and d["hung_max"] is not None else r["hung_max_s"],
                   valid=valid)
        out.append(new)
    order = ["setting", "scenario", "rep", "phase_s", "api_gone_s", "api_errors", "log_gone_s", "first_refused_s",
             "hung_n", "hung_med_s", "hung_max_s", "hung_timeouts", "ok_after_failure", "heal_online_s",
             "heal_probe_ok_s", "valid", "frps_cause", "frpc_detect_s", "frpc_cause", "load_end"]
    save_csv("heartbeat.csv", [{k: x[k] for k in order} for x in out])
    print("heartbeat.csv: %d runs, %d recomputed identically under the explicit-observation rule; differing: %s; "
          "API states seen in all raw files: %s" % (len(out), ok, notes or "none", sorted(all_api)))


if __name__ == "__main__":
    if not os.path.isdir(RAW):
        sys.exit("reanalyze: no raw directory %s (set S1C_SCRATCH)" % RAW)
    matrix()
    hol()
    heartbeat()
