#!/usr/bin/env python3
"""Spike S17: the README's tables, from the committed results.

    python3 summarize.py extract <raw dir> <rep dir>   raw files -> meta, streams, transfers, bulk, links, queues, conns, listener CSVs
    python3 summarize.py tables [results dir]          the tables, as Markdown, from results/rep*/
    python3 summarize.py verify [results dir]          every value the tables take from runs.csv, recomputed from the committed files
    python3 summarize.py compress [results dir]        the large CSVs to .csv.xz, the committed form

extract keeps, from each run's raw file, every token's arrival per stream request (streams.csv), every
transfer with its receiver-side steps (transfers.csv, S15's, with the batch it ran in) or every bulk
request of the many-Instances run (bulk.csv), the link's per-connection bytes (links.csv) and queue
statistics (queues.csv), the dial-home listener's TLS counters (listener.csv) and each run's constants
(meta.csv), so the tables recompute the gaps, validity, times and rates from the arrivals. runs.csv,
samples.csv and host.csv are the driver's. Adapted from spike S15's summarize.py.
"""
import csv
import glob
import json
import lzma
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import analysis  # noqa: E402

TR_FIELDS = ["kind", "start_ns", "sent_ns", "resp_ns", "recv_first_ns", "recv_last_ns", "end_ns", "bytes", "status", "err"]
BULK_FIELDS = ["instance", "start_ns", "resp_ns", "first_ns", "end_ns", "bytes", "status", "err"]
FRONT = ("frps", "frps2", "listener", "owner", "bridges")
MIB = 1 << 20


def rep_dirs(root):
    """The repetitions' result directories: results/<group>/rep<N> (one per group and repetition, as the hosted
    jobs upload them), or results/rep<N>."""
    return sorted(glob.glob(os.path.join(root, "*", "rep*"))) + sorted(glob.glob(os.path.join(root, "rep*")))


def write_csv(path, rows):
    if not rows:
        return
    fields = []
    for r in rows:
        for k in r:
            if k not in fields:
                fields.append(k)
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def extract(raw_dir, rep_dir):
    os.makedirs(rep_dir, exist_ok=True)
    meta, srows, trows, brows, lrows, qrows, crows, hrows, krows = [], [], [], [], [], [], [], [], []
    for path in sorted(glob.glob(os.path.join(raw_dir, "*.json"))):
        with open(path) as fh:
            raw = json.load(fh)
        rid, load = raw["row"]["run_id"], raw.get("load")
        if not load:
            continue
        many = "bulk" in load
        m = {"run_id": rid, "scenario": "many" if many else "single", "t0_unix_ns": load["t0_unix_ns"], "end_ns": load["end_ns"],
             "streams": len(load["streams_runs"]) if many else load["streams"]}
        if many:
            m.update({"window_start_ns": load["window_start_ns"], "window_end_ns": load["window_end_ns"], "interval_ns": load["interval_ns"],
                      "bytes": load["bytes"], "instances": load["instances"], "streams_per_instance": load["streams_per_instance"]})
        else:
            m.update({"reference_start_ns": load["reference_start_ns"], "reference_end_ns": load["reference_end_ns"]})
        meta.append(m)
        for j, runs in enumerate(load["streams_runs"]):
            for k, r in enumerate(runs):
                srows.append({"run_id": rid, "stream": j, "request": k, "start_ns": r["start_ns"], "end_ns": r["end_ns"],
                              "status": r["status"], "err": r.get("err", ""), "tokens_ns": " ".join(str(t) for t in r["tokens_ns"])})
        for i, tr in enumerate(load.get("transfers") or []):
            row = {"run_id": rid, "transfer": i, "batch": tr.get("batch", 0)}
            row.update({k: tr.get(k, "") for k in TR_FIELDS})
            row["steps_bytes"] = " ".join(str(x) for x in tr.get("steps_bytes") or [])
            trows.append(row)
        for i, b in enumerate(load.get("bulk") or []):
            row = {"run_id": rid, "request": i}
            row.update({k: b.get(k, "") for k in BULK_FIELDS})
            brows.append(row)
        for c in raw.get("link") or []:
            lrows.append({"run_id": rid, **c})
        for d, q in sorted((raw.get("link_queue") or {}).items()):
            qrows.append({"run_id": rid, "direction": d, **q})
        for c in raw.get("conns") or []:
            for role, n in sorted(c["conns"].items()):
                crows.append({"run_id": rid, "t": "%.6f" % c["t"], "role": role, "established": n})
        for t_ns, accepts, hellos in raw.get("listener_stats") or []:
            hrows.append({"run_id": rid, "t_ns": t_ns, "accepts": accepts, "hellos": hellos})
        for k in raw.get("ssh_kex") or []:
            first = (k.get("kex") or [{}])[0]
            ins = [r for r in k.get("rekeys", []) if r["dir"] == "in"]
            krows.append({"run_id": rid, "client": k.get("client", ""), "channel": k.get("channel", ""),
                          "local_version": k.get("local_version", ""), "remote_version": k.get("remote_version", ""),
                          "exchanges": k.get("exchanges", 0), "initiators": " ".join(k.get("initiator", [])),
                          "kex_algorithm": first.get("algorithm", ""), "host_key": first.get("host_key", ""),
                          "cipher_in": first.get("cipher_in", ""), "mac_in": first.get("mac_in", ""),
                          "cipher_out": first.get("cipher_out", ""), "mac_out": first.get("mac_out", ""),
                          "rekey_in_blocks": (k.get("rekey_in") or [""])[0], "rekey_out_blocks": (k.get("rekey_out") or [""])[0],
                          "rekey_input_bytes": " ".join(str(r["input_bytes"]) for r in ins),
                          "rekey_output_bytes": " ".join(str(r["output_bytes"]) for r in ins)})
    for name, rows in (("meta.csv", meta), ("streams.csv", srows), ("transfers.csv", trows), ("bulk.csv", brows),
                       ("links.csv", lrows), ("queues.csv", qrows), ("conns.csv", crows), ("listener.csv", hrows), ("ssh.csv", krows)):
        write_csv(os.path.join(rep_dir, name), rows)
    print("%s: %d runs, %d stream requests, %d transfers, %d bulk requests, %d link connections, %d connection samples, %d listener rows, %d ssh clients" % (
        rep_dir, len(meta), len(srows), len(trows), len(brows), len(lrows), len(crows), len(hrows), len(krows)))


# The large files are committed xz-compressed (compress); every reader takes either form.
COMPRESSED = ("samples.csv", "host.csv", "streams.csv", "conns.csv", "listener.csv")


def open_csv(path):
    """A result CSV as text: path itself, or path.xz when only the compressed form exists."""
    if os.path.exists(path):
        return open(path, newline="")
    if os.path.exists(path + ".xz"):
        return lzma.open(path + ".xz", "rt", newline="")
    raise FileNotFoundError(path)


def exists_csv(path):
    return os.path.exists(path) or os.path.exists(path + ".xz")


def compress(root):
    """Writes each repetition's large CSVs as .csv.xz (xz preset 6) and removes the plain ones,
    after checking that the compressed file reads back byte for byte."""
    for d in rep_dirs(root):
        for name in COMPRESSED:
            src = os.path.join(d, name)
            if not os.path.exists(src):
                continue
            with open(src, "rb") as fh:
                data = fh.read()
            with lzma.open(src + ".xz", "wb", preset=6) as out:
                out.write(data)
            with lzma.open(src + ".xz", "rb") as back:
                if back.read() != data:
                    raise RuntimeError("%s.xz does not read back as %s" % (src, src))
            os.remove(src)
            print("%s: %d -> %d bytes" % (src + ".xz", len(data), os.path.getsize(src + ".xz")))


def samples_by_run(d):
    """A repetition's committed samples, by run and role, in time order."""
    by = {}
    with open_csv(os.path.join(d, "samples.csv")) as fh:
        for x in csv.DictReader(fh):
            by.setdefault(x["run_id"], {}).setdefault(x["role"], []).append((float(x["t"]), int(x["ticks"])))
    return by


def cores_between(ss, a, b):
    """A role's cores between a and b (unix s), from its samples' summed ticks (None with fewer than 2)."""
    w = [x for x in ss if a <= x[0] <= b]
    if len(w) < 2 or w[-1][0] <= w[0][0]:
        return None
    return (w[-1][1] - w[0][1]) / 100 / (w[-1][0] - w[0][0])


def active_front_cores(by_role, t0, transfers):
    """The front's cores over the transfers' own spans only, the pauses between them left out: the
    front roles' ticks between the first and last sample inside each span, summed, over the summed
    time between those samples. Spans holding fewer than two samples (every 0.5 s) are skipped."""
    ticks, secs = 0, 0.0
    for tr in transfers:
        ws, we = analysis.transfer_window(tr)
        a, b = t0 + ws / 1e9, t0 + we / 1e9
        span = None
        for role in FRONT:
            w = [x for x in by_role.get(role, []) if a <= x[0] <= b]
            if len(w) < 2:
                continue
            ticks += w[-1][1] - w[0][1]
            span = max(span or 0, w[-1][0] - w[0][0])
        if span:
            secs += span
    return ticks / 100 / secs if secs else None


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def loads(root):
    """Each run's load, rebuilt from meta.csv, streams.csv, transfers.csv and bulk.csv, by run_id; and the
    runs.csv rows (each with its repetition)."""
    runs, out = [], {}
    for d in rep_dirs(root):
        with open(os.path.join(d, "runs.csv")) as fh:
            runs += list(csv.DictReader(fh))
        with open(os.path.join(d, "meta.csv")) as fh:
            for m in csv.DictReader(fh):
                L = {"scenario": m["scenario"], "streams": int(m["streams"]), "t0_unix_ns": int(m["t0_unix_ns"]),
                     "streams_runs": [[] for _ in range(int(m["streams"]))], "transfers": [], "bulk": []}
                if m["scenario"] == "many":
                    for k in ("window_start_ns", "window_end_ns", "interval_ns", "bytes", "instances"):
                        L[k] = int(m[k])
                else:
                    L["reference_start_ns"], L["reference_end_ns"] = int(m["reference_start_ns"]), int(m["reference_end_ns"])
                out[m["run_id"]] = L
        if exists_csv(os.path.join(d, "transfers.csv")):
            with open_csv(os.path.join(d, "transfers.csv")) as fh:
                for r in csv.DictReader(fh):
                    tr = {k: r[k] for k in TR_FIELDS}
                    for k in ("start_ns", "sent_ns", "resp_ns", "recv_first_ns", "recv_last_ns", "end_ns", "bytes", "status"):
                        tr[k] = int(tr[k]) if tr[k] not in ("", None) else 0
                    tr["batch"] = int(r.get("batch") or 0)
                    tr["steps_bytes"] = [int(x) for x in r["steps_bytes"].split()] if r.get("steps_bytes") else []
                    out[r["run_id"]]["transfers"].append(tr)
        if exists_csv(os.path.join(d, "bulk.csv")):
            with open_csv(os.path.join(d, "bulk.csv")) as fh:
                for r in csv.DictReader(fh):
                    b = {k: r[k] for k in BULK_FIELDS}
                    for k in ("instance", "start_ns", "resp_ns", "first_ns", "end_ns", "bytes", "status"):
                        b[k] = int(b[k]) if b[k] not in ("", None) else 0
                    out[r["run_id"]]["bulk"].append(b)
        with open_csv(os.path.join(d, "streams.csv")) as fh:
            for r in csv.DictReader(fh):
                L = out.get(r["run_id"])
                if L is None:
                    continue
                L["streams_runs"][int(r["stream"])].append(
                    {"start_ns": int(r["start_ns"]), "end_ns": int(r["end_ns"]), "status": int(r["status"]),
                     "tokens_ns": [int(t) for t in r["tokens_ns"].split()]})
    return runs, out


def ssh_clients(root):
    """The relay clients' parsed debug logs (ssh.csv), by run_id."""
    out = {}
    for d in rep_dirs(root):
        p = os.path.join(d, "ssh.csv")
        if os.path.exists(p):
            with open(p, newline="") as fh:
                for r in csv.DictReader(fh):
                    out.setdefault(r["run_id"], []).append(r)
    return out


def block_bytes(cipher):
    """The cipher's block size, which the rekey limit counts in: 8 bytes for chacha20-poly1305, 16 for the AES modes."""
    return 8 if cipher.startswith("chacha20") else 16


def collections_counter(xs):
    out = {}
    for x in xs:
        out[x] = out.get(x, 0) + 1
    return out


def human_bytes(n):
    return "%d GiB" % (n >> 30) if n % (1 << 30) == 0 else "%d MiB" % (n >> 20) if n % (1 << 20) == 0 else "%d B" % n


def pooled_gaps(lds):
    """The gaps of several single-scenario runs pooled: (reference gaps, gaps during a transfer), in ms."""
    ref, bulk = [], []
    for L in lds:
        spans = [analysis.transfer_window(tr) for tr in L["transfers"]]
        rs0, re0 = L["reference_start_ns"], L["reference_end_ns"]
        for rs in L["streams_runs"]:
            for r in rs:
                tk = r["tokens_ns"]
                for a, b in zip(tk, tk[1:]):
                    g = (b - a) / 1e6
                    if any(a < we and b > ws for ws, we in spans):
                        bulk.append(g)
                    elif rs0 <= a and b <= re0:
                        ref.append(g)
    return ref, bulk


def pooled_many_gaps(lds):
    out = []
    for L in lds:
        w0, w1 = L["window_start_ns"], L["window_end_ns"]
        for rs in L["streams_runs"]:
            for r in rs:
                tk = r["tokens_ns"]
                out += [(b - a) / 1e6 for a, b in zip(tk, tk[1:]) if w0 <= a and b <= w1]
    return out


def verify(root):
    """Recomputes from the committed files every value the tables take from runs.csv, and reports the
    largest difference or each mismatch: the CPU figures (samples.csv.xz), the transfers' rates, times and
    batch metrics (transfers.csv), the validity and the gaps (streams.csv.xz and transfers.csv or bulk.csv),
    the many-Instances run's times to first byte (bulk.csv) and TLS rates (listener.csv.xz), the link's
    connection counts (links.csv) and queue statistics (queues.csv) and the frpc's established connections
    (conns.csv)."""
    runs, lds = loads(root)
    sshc = ssh_clients(root)
    by_id = {r["run_id"]: r for r in runs}
    bad, checked = [], {"cpu": 0, "transfer": 0, "validity": 0, "gap": 0, "many": 0, "tls": 0, "link": 0, "queue": 0, "conns": 0, "ssh": 0}
    worst_cpu = 0.0

    def same(rid, key, got, want, tol=0.0):
        if want in ("", None) and got in ("", None):
            return
        if want in ("", None) or got in ("", None) or abs(float(got) - float(want)) > tol:
            bad.append("%s %s: recomputed %s, runs.csv %s" % (rid, key, got, want))

    for d in rep_dirs(root):
        sb = samples_by_run(d)
        links, queues, conns, tls = {}, {}, {}, {}
        with open_csv(os.path.join(d, "links.csv")) as fh:
            for c in csv.DictReader(fh):
                links.setdefault(c["run_id"], []).append(int(c["bytes_up"]) + int(c["bytes_down"]))
        if exists_csv(os.path.join(d, "queues.csv")):
            with open_csv(os.path.join(d, "queues.csv")) as fh:
                for c in csv.DictReader(fh):
                    queues[(c["run_id"], c["direction"])] = c
        if exists_csv(os.path.join(d, "conns.csv")):
            with open_csv(os.path.join(d, "conns.csv")) as fh:
                for c in csv.DictReader(fh):
                    k = (c["run_id"], c["role"])
                    conns[k] = max(conns.get(k, 0), int(c["established"]))
        if exists_csv(os.path.join(d, "listener.csv")):
            with open_csv(os.path.join(d, "listener.csv")) as fh:
                for c in csv.DictReader(fh):
                    tls.setdefault(c["run_id"], []).append((int(c["t_ns"]), int(c["accepts"]), int(c["hellos"])))
        for rid in sorted(sb):
            r = by_id.get(rid)
            if r is None:
                continue
            for role, ss in sb[rid].items():
                for label in ("ref", "bulk"):
                    want, a, b = r.get("cpu_%s_%s" % (label, role)), r.get(label + "_start_s"), r.get(label + "_end_s")
                    if want in ("", None) or not a or not b:
                        continue
                    got = cores_between(ss, float(a), float(b))
                    if got is not None:
                        worst_cpu = max(worst_cpu, abs(got - float(want)))
                        checked["cpu"] += 1
            L = lds.get(rid)
            if L is None:
                continue
            kind, size = r["kind"], int(float(r.get("bytes_asked") or 64 * MIB))
            if L["scenario"] == "many":
                v = analysis.validity_many(L)
                if str(v["valid"]) != r.get("valid"):
                    bad.append("%s valid: recomputed %s, runs.csv %s" % (rid, v["valid"], r.get("valid")))
                checked["validity"] += 1
                for k, val in analysis.many_metrics(L).items():
                    same(rid, k, val, r.get(k), tol=0.001)
                checked["many"] += 1
                hs = analysis.handshake_rates(tls.get(rid, []), float(r["bulk_start_s"]), float(r["bulk_end_s"])) if tls.get(rid) else None
                if hs:
                    same(rid, "tls_accepts_per_s", round(hs["accepts_per_s"], 3), r.get("tls_accepts_per_s"), tol=0.0005)
                    same(rid, "tls_hellos_per_s", round(hs["hellos_per_s"], 3), r.get("tls_hellos_per_s"), tol=0.0005)
                    checked["tls"] += 1
            else:
                conc = int(r.get("concurrent") or 1)
                m = analysis.transfer_metrics(L["transfers"], kind, size)
                for k, v in m.items():
                    same(rid, k, v, r.get(k), tol=1.0 if "rate" in k or "bytes" in k else 0.001)
                checked["transfer"] += 1
                if r["layout"] != "cal":
                    for k, v in analysis.batch_metrics(L["transfers"], conc).items():
                        same(rid, k, v, r.get(k), tol=1.0 if "rate" in k else 0.001)
                    v = analysis.validity(L, kind, size, conc)
                    if str(v["valid"]) != r.get("valid"):
                        bad.append("%s valid: recomputed %s, runs.csv %s" % (rid, v["valid"], r.get("valid")))
                    checked["validity"] += 1
                    for k, val in analysis.gap_stats(L).items():
                        same(rid, k, val, r.get(k), tol=0.001)
                    checked["gap"] += 1
            lk = links.get(rid, [])
            same(rid, "link_conns", len(lk), r.get("link_conns"))
            same(rid, "link_conns_over_1mib", sum(1 for x in lk if x > MIB), r.get("link_conns_over_1mib"))
            checked["link"] += 1
            for dname in ("up", "down"):
                q = queues.get((rid, dname))
                if q:
                    same(rid, "queue_%s_max_bytes" % dname, q["max_occupancy_bytes"], r.get("queue_%s_max_bytes" % dname))
                    same(rid, "queue_%s_seconds_full" % dname, round(float(q["seconds_full"]), 3), r.get("queue_%s_seconds_full" % dname), tol=0.0005)
                    checked["queue"] += 1
            ks = [k for k in sshc.get(rid, []) if k["channel"] == "serving"]
            if ks:
                same(rid, "ssh_key_exchanges", ks[0]["exchanges"], r.get("ssh_key_exchanges"))
                checked["ssh"] += 1
            for role in ("frpc", "frpc-bulk"):
                want = r.get("conns_max_" + role)
                if want not in ("", None):
                    same(rid, "conns_max_" + role, conns.get((rid, role)), want)
                    checked["conns"] += 1
    print("recomputed from the committed files: %s" % ", ".join("%d %s" % (n, k) for k, n in checked.items()))
    print("CPU: the largest difference from runs.csv is %.6f cores" % worst_cpu)
    print("mismatches: %s" % ("none" if not bad else "\n  " + "\n  ".join(bad[:30])))
    return 1 if bad else 0


def rng(xs, fmt):
    xs = [x for x in xs if x is not None]
    if not xs:
        return "-"
    lo, mid, hi = min(xs), statistics.median(xs), max(xs)
    if fmt.format(lo) == fmt.format(hi):
        return fmt.format(mid)
    return (fmt + " ({}–{})").format(mid, fmt.format(lo), fmt.format(hi))


def f1(x):
    return "-" if x is None else "%.1f" % x


def q3(xs):
    return "%s / %s / %s" % (f1(analysis.pct(xs, 50)), f1(analysis.pct(xs, 99)), f1(max(xs) if xs else None))


def depth(r):
    if r.get("queue") != "fifo":
        return "fair (per-connection turns)"
    b = int(float(r["queue_bytes"]))
    return "FIFO %s" % ("1 MiB" if b == MIB else "6 MiB (frp's window)" if b == 6 * MIB else "%.0f KB (bandwidth-delay product)" % (b / 1000) if b < 200_000 else "%d B" % b)


def tables(root):
    runs, lds = loads(root)
    meas = [r for r in runs if r["layout"] != "cal"]
    cal = [r for r in runs if r["layout"] == "cal"]
    p = print
    p("### Path rate (calibration)\n")
    p("One uncapped 64 MiB download in layout (a), with no stream, on the fair link, per mode and repetition; the median of its transfers at the receiver.\n")
    p("| mode | path rate, MB/s (reps) |")
    p("|---|---|")
    for mode in sorted({r["mode"] for r in cal}):
        p("| %s | %s |" % (mode, " / ".join("%.1f" % (num(r["recv_rate_median"]) / 1e6) for r in cal if r["mode"] == mode and num(r.get("recv_rate_median")))))

    def valid_runs(rs):
        return [r for r in rs if r.get("valid") == "True" and r["run_id"] in lds]

    def group(name):
        return [r for r in meas if r.get("group") == name]

    def single_rows(rs, keyf, labelf, header, extra=None):
        g = {}
        for r in rs:
            g.setdefault(keyf(r), []).append(r)
        for k in sorted(g):
            rows = g[k]
            v = valid_runs(rows)
            ref, bulk = pooled_gaps([lds[r["run_id"]] for r in v])
            cells = labelf(k) + ["%d/%d" % (len(v), len(rows)), q3(ref), q3(bulk), str(len(bulk))]
            cells += [rng([(num(r.get("recv_rate_median")) or 0) / 1e6 or None for r in rows if r["status"] == "ok"], "{:.1f}"),
                      rng([num(r.get("recv_time_median_s")) for r in rows if r["status"] == "ok"], "{:.2f}")]
            if extra:
                cells += extra(rows)
            p("| " + " | ".join(cells) + " |")

    fifo = group("fifo")
    if fifo:
        p("\n### A shared FIFO bottleneck (direct mode)\n")
        p("The two frpc's connections through one emulated link of 48 MB/s and 2.3 ms, per queue discipline, during a 64 MiB download beside 8 token streams (5 downloads per run). Gaps pool the repetitions' valid runs: during a transfer (a gap overlapping a transfer's span) against the same run's reference window with no transfer. The queue columns are the link's statistics for the upstream direction, the one the downloads cross (node to front): the largest occupancy over the runs and the time the queue was full (less room than one chunk) over the run.\n")
        p("| layout | link queue | runs valid | reference gap p50 / p99 / max, ms | gap during the download p50 / p99 / max, ms | gaps during | rate, MB/s | time, s | queue max, KB | queue full, s |")
        p("|---|---|---:|---|---|---:|---:|---:|---:|---:|")
        order = lambda r: (r["layout"], r.get("queue") != "fair", int(float(r.get("queue_bytes") or 0)))  # noqa: E731
        single_rows(fifo, order, lambda k: [{"a": "(a) one frpc", "c": "(c) a separate frpc"}[k[0]], depth({"queue": "fifo" if k[1] else "fair", "queue_bytes": k[2]})],
                    None, lambda rows: [rng([(num(r.get("queue_up_max_bytes")) or 0) / 1000 or None for r in rows], "{:.0f}"),
                                        rng([num(r.get("queue_up_seconds_full")) for r in rows if r.get("queue_up_seconds_full") not in ("", None)], "{:.1f}")])
    ssh = group("ssh")
    if ssh:
        p("\n### Real relay buffers (host-dialed mode)\n")
        p("The relay through S15's bridge (a TCP stand-in for ssh, the control) and through a real `ssh` client, a real sshd and the fake Slurm's srun standard streams, during a 64 MiB download beside 8 token streams. Front CPU is frps, the second frps, the listener, the relay owner and the ssh clients (or the bridges), in cores, over the transfers' phase; `ssh` is the ssh clients alone.\n")
        p("| layout | relay | runs valid | reference gap p50 / p99 / max, ms | gap during the download p50 / p99 / max, ms | gaps during | rate, MB/s | time, s | front cores, transfers' phase | ssh or bridge cores |")
        p("|---|---|---:|---|---|---:|---:|---:|---:|---:|")
        single_rows(ssh, lambda r: (r["layout"], r["mode"]),
                    lambda k: [{"a": "(a) one frpc", "c": "(c) a separate frpc"}[k[0]], {"ssh": "real ssh, sshd and srun streams", "host-dialed": "S15's bridge"}[k[1]]], None,
                    lambda rows: [rng([num(r.get("cpu_bulk_front")) for r in rows], "{:.3f}"), rng([num(r.get("cpu_bulk_bridges")) for r in rows], "{:.3f}")])
    conc = group("concurrent")
    if conc:
        p("\n### Concurrent bulk transfers\n")
        p("1, 2 and 4 downloads of 64 MiB started together beside 8 token streams (5 batches per run). Every batch's transfers overlapped in time: the valid runs' least overlap, the share of the shortest transfer's span that all of a batch's transfers had in common, is in the last column. Each transfer's time is from its first to its last byte at the client; the batch's is from the first start to the last end.\n")
        p("| mode | layout | transfers at once | runs valid | reference gap p50 / p99 / max, ms | gap during the transfers p50 / p99 / max, ms | gaps during | each transfer's time, s | batch time, s | aggregate rate, MB/s | least overlap |")
        p("|---|---|---:|---:|---|---|---:|---:|---:|---:|---:|")
        g = {}
        for r in conc:
            g.setdefault((r["mode"], r["layout"], int(r["concurrent"])), []).append(r)
        for k in sorted(g):
            rows = g[k]
            v = valid_runs(rows)
            ref, bulk = pooled_gaps([lds[r["run_id"]] for r in v])
            ok = [r for r in rows if r["status"] == "ok"]
            p("| %s | %s | %d | %d/%d | %s | %s | %d | %s | %s | %s | %s |" % (
                k[0], {"a": "(a) one frpc", "c": "(c) a separate frpc"}[k[1]], k[2], len(v), len(rows), q3(ref), q3(bulk), len(bulk),
                rng([num(r.get("recv_time_median_s")) for r in ok], "{:.2f}"), rng([num(r.get("batch_time_median_s")) for r in ok], "{:.2f}"),
                rng([(num(r.get("batch_rate_median")) or 0) / 1e6 or None for r in ok], "{:.1f}"), rng([num(r.get("overlap_min")) for r in ok], "{:.3f}")))
    if conc:
        p("\n#### Stalls at the end of a batch\n")
        p("A stall of the token streams near a batch's end: at least 6 of the 8 streams with a gap of more than 100 ms inside the batch whose end falls in the batch's last 400 ms. Per row, over the three repetitions: the batches with such a stall of the 15 (5 per run), which batches they were (0 is the first of 5), the bytes the transfers had moved through the relay's one connection by the end of those batches, the stall's gap per stream (smallest–largest) and how long before the batch's end the stall ended. Layout (a)'s own head-of-line stalls (S15: its 6 MiB window) recur through a transfer, so its rows at one transfer at a time, in either mode, trip the rule at any batch and are not the event; the rows that carry the finding are (c)'s, which has no such stall, and (a)'s at four at once, where direct mode shows none.\n")
        p("| mode | layout | transfers at once | batches with a stall | which batches | bytes moved by then, GiB | gap per stream, ms | ended before the batch's end, ms |")
        p("|---|---|---:|---:|---|---|---|---|")
        g = {}
        for r in conc:
            g.setdefault((r["mode"], r["layout"], int(r["concurrent"])), []).append(r)
        for k in sorted(g):
            ev = [(b, e) for r in g[k] if r["run_id"] in lds for b, e in sorted(analysis.late_batch_stalls(lds[r["run_id"]]).items())]
            moved = [sum(tr["bytes"] for tr in lds[r["run_id"]]["transfers"]) / (1 << 30) for r in g[k] if r["run_id"] in lds]
            p("| %s | %s | %d | %d of %d | %s | %s | %s | %s |" % (
                k[0], {"a": "(a) one frpc", "c": "(c) a separate frpc"}[k[1]], k[2], len(ev), 5 * len(g[k]),
                ", ".join(sorted({str(b) for b, _ in ev})) or "-", ", ".join(sorted({"%.3f" % e["gib"] for _, e in ev})) or "- (all five batches moved %s GiB in all)" % "/".join(sorted({"%.2f" % m for m in moved})),
                "%d–%d" % (min(e["min_ms"] for _, e in ev), max(e["max_ms"] for _, e in ev)) if ev else "-",
                "%d–%d" % (min(e["before_end_ms"] for _, e in ev), max(e["before_end_ms"] for _, e in ev)) if ev else "-"))
        p("\n#### Time to first byte of concurrent downloads\n")
        p("A download's time to first byte (from before its dial to its first body byte at the client), by its place in its batch: the median over the three repetitions' batches of the first to answer, the second, and so on, and the p99 of the last to answer. Layout (c)'s transfers each need a work connection from the second frpc: one spare is waiting for the first, and the others wait for a spare that frps asks for after it hands one out.\n")
        p("| mode | layout | transfers at once | first-byte time of each, in the order they answered, p50, ms | p99 of the last, ms |")
        p("|---|---|---:|---|---:|")
        for k in sorted(g):
            if k[2] == 1:
                continue
            pos = {}
            for r in g[k]:
                if r["run_id"] in lds:
                    for b, v in analysis.batch_ttfb(lds[r["run_id"]]["transfers"]).items():
                        for i, x in enumerate(v):
                            pos.setdefault(i, []).append(x)
            if pos:
                p("| %s | %s | %d | %s | %.1f |" % (k[0], {"a": "(a) one frpc", "c": "(c) a separate frpc"}[k[1]], k[2],
                                                    " / ".join("%.1f" % statistics.median(pos[i]) for i in sorted(pos)), analysis.pct(pos[max(pos)], 99)))
    many = group("many")
    if many:
        p("\n### Many Instances behind one front (direct mode)\n")
        p("N Instances, each with a token stream and one bulk download of the size shown every 10 s (the Instances' requests staggered evenly over the interval), over a 60 s window. Layout (c) gives each Instance a second frpc with `tcpMux = false` (a new TLS connection through the dial-home listener for every bulk request); layout (a), the control, carries the bulk request on the token frpc's session. Front CPU is frps, the second frps and the listener, in cores, over the window (the host's busy share in the last column). TLS rates are from the listener's counters: connections accepted and ClientHellos received per second over the window. The time to first byte runs from before the request's dial to the first body byte.\n")
        p("| layout | Instances | request | runs valid | front cores | TLS connections/s | ClientHellos/s | TTFB p50 / p99 / max, ms | request time p50 / p99, ms | token gap p50 / p99 / max, ms | host busy |")
        p("|---|---:|---:|---:|---:|---:|---:|---|---|---|---:|")
        g = {}
        for r in many:
            g.setdefault((r["layout"], int(r["instances"]), int(float(r["bytes_asked"]))), []).append(r)
        for k in sorted(g, key=lambda k: (k[0], k[1], k[2])):
            rows = g[k]
            v = valid_runs(rows)
            ok = [r for r in rows if r["status"] == "ok"]
            gaps = pooled_many_gaps([lds[r["run_id"]] for r in v])
            ttfb = [(b["first_ns"] - b["start_ns"]) / 1e6 for r in v for b in lds[r["run_id"]]["bulk"] if not b["err"] and b["first_ns"]]
            tot = [(b["end_ns"] - b["start_ns"]) / 1e6 for r in v for b in lds[r["run_id"]]["bulk"] if not b["err"]]
            p("| %s | %d | %s | %d/%d | %s | %s | %s | %s | %s / %s | %s | %s |" % (
                {"a": "(a) one frpc", "c": "(c) a second frpc each"}[k[0]], k[1], "%d KiB" % (k[2] // 1024) if k[2] < MIB else "%d MiB" % (k[2] // MIB),
                len(v), len(rows), rng([num(r.get("cpu_bulk_front")) for r in ok], "{:.3f}"),
                rng([num(r.get("tls_accepts_per_s")) for r in ok], "{:.2f}"), rng([num(r.get("tls_hellos_per_s")) for r in ok], "{:.2f}"),
                q3(ttfb), f1(analysis.pct(tot, 50)), f1(analysis.pct(tot, 99)), q3(gaps), rng([num(r.get("host_busy_bulk")) for r in ok], "{:.2f}")))
    rk = group("rekey")
    if rk:
        sc = ssh_clients(root)
        p("\n### The relay's ssh and its rekeys (layout (c), four downloads at once, through real ssh)\n")
        p("The review of PR 416 found an all-stream stall at 1 GiB in the ssh runs and read it as OpenSSH's default rekey. These runs log it: each relay client ran with `ssh -v`, and its debug log's key exchanges and negotiated algorithms are parsed (nothing else of the log is kept: it names the host and the user). *default* is the relay's ssh as `sshrun` starts it; *RekeyLimit 256M* adds `-o RekeyLimit=256M`. Downloads of 40 MiB (160 MiB a batch, so the rekey points fall inside batches, not at their ends) or 64 MiB, nine batches of four.\n")
        p("| run | repetition | OpenSSH (client / server) | key exchange | host key | cipher, MAC (receive / send) | rekey limit | key exchanges on the serving relay | started by | bytes the connection had received at each rekey, MiB |")
        p("|---|---:|---|---|---|---|---|---:|---|---|")
        rows = sorted(rk, key=lambda r: (r["variant"], r["rep"]))
        for r in rows:
            ks = [k for k in sc.get(r["run_id"], []) if k["channel"] == "serving"]
            if not ks:
                p("| %s | %s | (no ssh log) | | | | | | | |" % (r["variant"], r["rep"]))
                continue
            k = ks[0]
            blocks = int(k["rekey_in_blocks"]) if k["rekey_in_blocks"] else 0
            lim = human_bytes(blocks * block_bytes(k["cipher_in"])) if blocks else "-"
            ini = collections_counter(k["initiators"].split())
            p("| %s | %s | %s / %s | %s | %s | %s, %s / %s, %s | %d blocks of %d B = %s | %s | %s | %s |" % (
                r["variant"], r["rep"], k["local_version"].split()[0], k["remote_version"].split()[0], k["kex_algorithm"], k["host_key"],
                k["cipher_in"], k["mac_in"], k["cipher_out"], k["mac_out"], blocks, block_bytes(k["cipher_in"]), lim, k["exchanges"],
                ", ".join("%s %d" % (a, n) for a, n in sorted(ini.items())),
                ", ".join("%.0f" % (int(x) / (1 << 20)) for x in k["rekey_input_bytes"].split()) or "-"))
        p("\n### All-stream stalls against the rekeys\n")
        p("An all-stream stall is a gap of more than 100 ms in at least 6 of the 8 token streams, starting within 0.3 s of one another, while the transfers ran. Its bytes are the transfers' payload at the client when it began (the steps give a range, shown as the midpoint); the rekey's are what the ssh connection had received when its new keys took effect, which runs ahead of the client by what sits in the buffers between (frp's 6 MiB window, the relay's, the sockets'). A stall matches a rekey when its bytes lie from 16 MiB below the rekey's to 2 MiB above (`analysis.match_rekeys`).\n")
        p("The first columns use the rule as set before the runs (a gap of more than 100 ms); the last two repeat the match with a gap of more than 80 ms, to show how near the threshold the stalls are and that a looser one finds no stall that is not a rekey.\n")
        p("| run | repetition | valid | rekeys | all-stream stalls | rekeys with a stall | stalls with no rekey | rekey at MiB (connection) → stall at MiB (payload), gap per stream ms | rekeys with a stall, over 80 ms | stalls with no rekey, over 80 ms |")
        p("|---|---:|---|---:|---:|---:|---:|---|---:|---:|")
        for r in rows:
            L = lds.get(r["run_id"])
            ks = [k for k in sc.get(r["run_id"], []) if k["channel"] == "serving"]
            if L is None or not ks:
                continue
            stalls = analysis.all_stream_stalls(L)
            rb = [int(x) for x in ks[0]["rekey_input_bytes"].split()]
            pairs, lonely, free = analysis.match_rekeys(stalls, rb)
            loose = analysis.all_stream_stalls(L, min_gap_ms=80)
            lp, ll, lf = analysis.match_rekeys(loose, rb)
            p("| %s | %s | %s | %d | %d | %d | %d | %s | %d | %d |" % (
                r["variant"], r["rep"], r.get("valid"), len(rb), len(stalls), len(pairs), len(free),
                "; ".join("%.0f → %.0f, %d–%d" % (x / (1 << 20), (st["moved_lo"] + st["moved_hi"]) / 2 / (1 << 20), st["min_ms"], st["max_ms"]) for x, st in pairs) or "-",
                len(lp), len(lf)))
    p("\n### Tunnel losses\n")
    p("Summed over all %d runs: frps's lists (a proxy not online or registered again), frpc logins beyond the first (every frpc), relay sessions beyond the first, and heartbeat-timeout lines.\n" % len(runs))
    s = lambda c: sum(int(float(r[c])) for r in runs if r.get(c) not in (None, ""))  # noqa: E731
    p("| drops | frpc re-logins | relay restarts | heartbeat-timeout lines | unreadable lists | runs not ok | processes the ssh node left |")
    p("|---:|---:|---:|---:|---:|---:|---:|")
    p("| %d | %d | %d | %d | %d | %d | %d |" % (s("drops_api"), s("frpc_relogins"), s("relay_restarts"), s("hb_timeout_lines"), s("api_unreadable"),
                                              sum(1 for r in runs if r["status"] != "ok"), s("node_leaked")))
    bad = [r for r in meas if r.get("valid") != "True"]
    if bad:
        p("\nRuns not valid:")
        for r in bad:
            p("- rep %s, %s, %s, %s, %s: %s" % (r["rep"], r.get("group"), r["mode"], r["layout"], r["kind"], r.get("invalid_reason") or r["status"]))
    p("\n### Runners\n")
    for rep, model, cpus, mem, kernel in sorted({(r["rep"], r["cpu_model"], r["cpus"], r["mem_total_mb"], r["kernel"]) for r in runs}):
        p("- rep %s: %s, %s CPUs, %s MB, Linux %s" % (rep, model, cpus, mem, kernel))


if __name__ == "__main__":
    here = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
    if len(sys.argv) >= 4 and sys.argv[1] == "extract":
        extract(sys.argv[2], sys.argv[3])
    elif len(sys.argv) >= 2 and sys.argv[1] == "verify":
        sys.exit(verify(sys.argv[2] if len(sys.argv) > 2 else here))
    elif len(sys.argv) >= 2 and sys.argv[1] == "compress":
        compress(sys.argv[2] if len(sys.argv) > 2 else here)
    else:
        tables(sys.argv[2] if len(sys.argv) > 2 else here)
