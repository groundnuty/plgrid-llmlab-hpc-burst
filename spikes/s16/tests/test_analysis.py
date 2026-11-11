"""Unit tests of spike S16's result rules (analysis.py). Expected values come from the rules' own
definitions in the issue and HBEP-6 (every second a token; the 1 s and 60 s rules; the cap), worked
by hand on small fixtures, never from the code's output."""
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import analysis  # noqa: E402


def req(i, start, offsets_s, end=None, done=True, err=""):
    """A load record: tunnel i, started at start (s), tokens at start + offsets."""
    return {"i": i, "s": int(start * 1e9), "k": [int(o * 1e4) for o in offsets_s],
            "end": int((end if end is not None else start + (offsets_s[-1] if offsets_s else 0)) * 1e9),
            "done": done, "e": err, "st": 200}


def sample(t, ticks, stat):
    return {"t": t, "ticks": ticks, "stat": stat}


class Validity(unittest.TestCase):
    def test_every_second_needs_a_token(self):
        # Tunnel 0 has a token in each of the 3 seconds; tunnel 1 misses the second one.
        load = [req(0, 100.0, [0.2, 1.3, 2.4]), req(1, 100.0, [0.5, 2.5])]
        valid, emptiest, empty = analysis.tunnel_validity(load, 2, 100.0, 103.0)
        self.assertFalse(valid)
        self.assertEqual(emptiest, 0)
        self.assertEqual(empty, [1])

    def test_all_seconds_covered(self):
        load = [req(0, 100.0, [0.1, 1.1, 2.1]), req(1, 100.0, [0.9, 1.9, 2.9])]
        self.assertEqual(analysis.tunnel_validity(load, 2, 100.0, 103.0), (True, 1, []))

    def test_gaps_only_inside_the_window(self):
        load = [req(0, 100.0, [0.5, 1.0, 1.25, 3.5])]
        gaps = analysis.gaps_ms(load, 100.0, 102.0)  # tokens at 100.5, 101.0, 101.25 inside
        self.assertEqual([round(g) for g in gaps], [500, 250])


class Cpu(unittest.TestCase):
    def test_cores_per_pid_between_its_samples(self):
        # Over 10 s: pid 1 uses 500 ticks (5 s of CPU), pid 2 appears halfway and uses 100.
        s = [sample(0.0, {"front": {"1": 0}}, [0] * 8), sample(5.0, {"front": {"1": 250, "2": 0}}, [0] * 8),
             sample(10.0, {"front": {"1": 500, "2": 100}}, [0] * 8)]
        self.assertAlmostEqual(analysis.cores(s, "front", 0.0, 10.0), 0.6)

    def test_host_busy_is_one_minus_idle_and_iowait(self):
        a = [0, 0, 0, 0, 0, 0, 0, 0]
        b = [300, 0, 100, 500, 100, 0, 0, 0]  # 1000 ticks, 600 idle + iowait
        s = [sample(0.0, {}, a), sample(10.0, {}, b)]
        self.assertAlmostEqual(analysis.host_busy(s, 0.0, 10.0), 0.4)

    def test_peak_takes_the_busiest_interval(self):
        s = [sample(0.0, {"frps": {"1": 0}}, [0] * 8), sample(1.0, {"frps": {"1": 50}}, [0] * 8),
             sample(2.0, {"frps": {"1": 60}}, [0] * 8)]
        self.assertAlmostEqual(analysis.peak_cores(s, ("frps",), 0.0, 2.0), 0.5)


class Relogin(unittest.TestCase):
    def write(self, d, name, rows):
        with open(os.path.join(d, name), "w") as fh:
            for r in rows:
                fh.write(json.dumps(r) + "\n")

    def test_times_from_the_cut_and_the_60s_rule(self):
        with tempfile.TemporaryDirectory() as d:
            cut = 1000.0
            m = {"run_id": "x", "n": 2, "event_kind": "frps", "t_event": cut - 0.02, "pre_start": cut - 10, "pre_end": cut - 0.05,
                 "post_end": cut + 100, "events": [], "uids": ["u0", "u1"], "status": "ok"}
            with open(os.path.join(d, "manifest.json"), "w") as fh:
                json.dump(m, fh)
            self.write(d, "frps-events.jsonl", [{"event": "stopped", "t_ns": int(cut * 1e9)}])
            # Both tunnels streamed every second before the cut, with a request in flight across it.
            pre = [req(i, cut - 10.5, [x * 0.5 for x in range(1, 25)], end=cut + 0.01, done=False, err="cut") for i in (0, 1)]
            self.write(d, "load.jsonl", pre)
            # Tunnel 0 re-registered at +2.1 s and routed at +2.2; tunnel 1 at +70 s (past the rule).
            self.write(d, "front-calls.jsonl", [
                {"plugin": "front", "op": "NewProxy", "proxy": "u0", "start_ns": int((cut + 2.0) * 1e9), "end_ns": int((cut + 2.1) * 1e9)},
                {"plugin": "front", "op": "NewProxy", "proxy": "u1", "start_ns": int((cut + 69.9) * 1e9), "end_ns": int((cut + 70.0) * 1e9)}])
            self.write(d, "admit-calls.jsonl", [
                {"plugin": "admit", "op": "Login", "reject": True, "start_ns": int((cut + 0.5) * 1e9), "end_ns": int((cut + 1.7) * 1e9)}])
            # Probes: before the cut both pass in epoch 1; after it, a pass still in epoch 1 (the
            # registration the restart replaced) does not count; the new epoch's passes do.
            self.write(d, "probes.jsonl", [
                {"t_ns": int((cut - 5) * 1e9), "i": 0, "pass": True, "reg": True, "ep": 1},
                {"t_ns": int((cut - 5) * 1e9), "i": 1, "pass": True, "reg": True, "ep": 1},
                {"t_ns": int((cut + 1.0) * 1e9), "i": 0, "pass": False, "reg": False, "ep": 1},
                {"t_ns": int((cut + 1.5) * 1e9), "i": 1, "pass": True, "reg": True, "ep": 1},
                {"t_ns": int((cut + 2.2) * 1e9), "i": 0, "pass": True, "reg": True, "ep": 3},
                {"t_ns": int((cut + 70.1) * 1e9), "i": 1, "pass": True, "reg": True, "ep": 4}])
            out = analysis.relogin(analysis.Run(d))
        self.assertTrue(out["pre_valid"])
        self.assertEqual(out["inflight_tunnels"], 2)
        self.assertAlmostEqual(out["routed_s"]["max"], 70.1, places=3)
        self.assertAlmostEqual(out["routed_s"]["p50"], 2.2, places=3)
        self.assertEqual(out["not_back_60s"], 1)
        self.assertEqual(out["refused"], {"admit Login": 1})
        self.assertEqual(out["plugin_ms"]["admit Login"]["over_rule"], 1)  # 1,200 ms > HBEP-6's 1 s
        self.assertTrue(out["valid"])

    def test_failure_kinds(self):
        self.assertEqual(analysis.failure_kind("dial frps: dial tcp 127.0.0.1:30140: connect: connection refused"),
                         "dial refused (frps down)")
        self.assertEqual(analysis.failure_kind("CONNECT answered 404 Not Found"), "set-up 404 (no proxy yet)")

    def test_scrub_masks_paths_and_numbers(self):
        got = analysis.scrub('dial unix /home/example-user/t/w/run-123/c/s/7: connect: no such file')
        self.assertEqual(got, "dial unix <path>: connect: no such file")
        self.assertEqual(analysis.failure_kind("read /tmp/x/y: reset"), "other: read <path>: reset")

    def test_parse_ts(self):
        self.assertAlmostEqual(analysis.parse_ts("1970-01-01T00:00:01.250Z"), 1.25)
        self.assertEqual(analysis.parse_ts("1970-01-01T00:01:00Z"), 60.0)


class CapLink(unittest.TestCase):
    def test_demand_binds_at_the_cap(self):
        # 6,000 tokens/s at 277 B each is 1.662 MB/s: it binds a 12.5 Mb/s cap (1.5625 MB/s), not 25.
        self.assertTrue(analysis.demand_binds(6000, 277, 12.5e6 / 8))
        self.assertFalse(analysis.demand_binds(6000, 277, 25e6 / 8))
        self.assertFalse(analysis.demand_binds(6000, None, 25e6 / 8))

    def test_link_fill_needs_every_second_at_95_percent(self):
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "manifest.json"), "w") as fh:
                json.dump({"rate_Bps": 1000.0, "n": 1}, fh)
            rows = [{"t": 101.0 + k, "up_bytes": b, "down_bytes": 1000} for k, b in enumerate([1000, 960, 940])]
            with open(os.path.join(d, "link-series.jsonl"), "w") as fh:
                fh.write("\n".join(json.dumps(r) for r in rows))
            run = analysis.Run(d)
            full = analysis.link_fill(run, 100.0, 103.0)   # the rows at 101-103: 1.00, 0.96, 0.94
            first = analysis.link_fill(run, 100.0, 102.0)  # 1.00, 0.96
        self.assertFalse(full["at_cap"])
        self.assertAlmostEqual(full["up_min"], 0.94)
        self.assertTrue(first["at_cap"])


class Ssh(unittest.TestCase):
    def test_negotiated_from_the_v_log(self):
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "ssh-1.log"), "w") as fh:
                fh.write("debug1: kex: algorithm: sntrup761x25519-sha512@openssh.com\n"
                         "debug1: kex: server->client cipher: aes128-gcm@openssh.com MAC: <implicit> compression: none\n"
                         "debug1: kex: client->server cipher: aes128-gcm@openssh.com MAC: <implicit> compression: none\n")
            with open(os.path.join(d, "ssh-2.log"), "w") as fh:
                fh.write("debug1: Reading configuration data\n")  # ssh -G: negotiates nothing
            got = analysis.ssh_negotiated(d)
        self.assertEqual(list(got.values()), [1])
        self.assertIn("kex=sntrup761x25519-sha512@openssh.com", list(got)[0])
        self.assertIn("server->client aes128-gcm@openssh.com <implicit> none", list(got)[0])

    def test_linfit(self):
        f = analysis.linfit([(0.0, 0.01), (1.0, 0.06), (2.0, 0.11)])
        self.assertAlmostEqual(f["slope"], 0.05)
        self.assertAlmostEqual(f["intercept"], 0.01)


class Front(unittest.TestCase):
    def test_r_front_is_the_difference_per_ktok(self):
        rows = [{"n": 10, "streams": 4, "rep": 1, "via": "front", "valid": True, "ktok_s": 0.8, "front_cores": 0.10},
                {"n": 10, "streams": 4, "rep": 1, "via": "tcpmux", "valid": True, "ktok_s": 0.8, "front_cores": 0.06}]
        p = analysis.rfront_pairs(rows)
        self.assertEqual(len(p), 1)
        self.assertAlmostEqual(p[0]["r_front_per_ktok"], 0.05)

    def test_an_invalid_arm_gives_no_pair(self):
        rows = [{"n": 10, "streams": 4, "rep": 1, "via": "front", "valid": False, "ktok_s": 0.8, "front_cores": 0.10},
                {"n": 10, "streams": 4, "rep": 1, "via": "tcpmux", "valid": True, "ktok_s": 0.8, "front_cores": 0.06}]
        self.assertEqual(analysis.rfront_pairs(rows), [])


if __name__ == "__main__":
    unittest.main()
