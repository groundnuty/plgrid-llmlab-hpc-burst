"""Unit tests of spike S17's result rules (analysis.py). Expected values come from the inputs each test
sets, not from the harness."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import analysis  # noqa: E402

S = 1_000_000_000
MS = 1_000_000


def stream(start, end, every_ms=50, stall=None):
    """One request streaming from start to end (ns), a token every every_ms, with an optional stall
    (from, to) during which no token arrives."""
    toks, t = [], start + 50 * MS
    while t < end:
        if not (stall and stall[0] <= t < stall[1]):
            toks.append(t)
        t += every_ms * MS
    return {"start_ns": start, "end_ns": end, "tokens_ns": toks, "status": 200}


def load(runs, transfers, ref=(1 * S, 11 * S)):
    return {"streams": len(runs), "streams_runs": runs, "transfers": transfers,
            "reference_start_ns": ref[0], "reference_end_ns": ref[1]}


def tr(start, end, nbytes=64 << 20, err="", status=200, first=None, last=None, sent=None, steps=None):
    return {"kind": "download", "start_ns": start, "end_ns": end, "bytes": nbytes, "err": err, "status": status,
            "recv_first_ns": first if first is not None else start, "recv_last_ns": last if last is not None else end,
            "sent_ns": sent, "steps_bytes": steps or []}


class Validity(unittest.TestCase):
    def test_a_stall_inside_a_transfer_is_the_measurement_not_a_pause(self):
        runs = [[stream(0, 40 * S, stall=(15 * S, 16 * S + 500 * MS))] for _ in range(2)]
        v = analysis.validity(load(runs, [tr(15 * S, 17 * S)]), "download", 64 << 20)
        self.assertTrue(v["valid"], v["reason"])

    def test_a_stream_between_requests_during_a_transfer_invalidates_it(self):
        runs = [[stream(0, 40 * S)], [stream(0, 15 * S + 200 * MS), stream(16 * S, 40 * S)]]
        v = analysis.validity(load(runs, [tr(15 * S, 17 * S)]), "download", 64 << 20)
        self.assertFalse(v["valid"])
        self.assertIn("stream 1", v["reason"])

    def test_a_transfer_that_ends_before_the_streams_start_invalidates_it(self):
        runs = [[stream(20 * S, 60 * S)]]
        v = analysis.validity(load(runs, [tr(12 * S, 14 * S)], ref=(21 * S, 31 * S)), "download", 64 << 20)
        self.assertFalse(v["valid"])

    def test_a_failed_or_short_transfer_invalidates_it(self):
        runs = [[stream(0, 40 * S)]]
        for bad in (tr(15 * S, 17 * S, err="cut"), tr(15 * S, 17 * S, nbytes=10), tr(15 * S, 17 * S, status=502)):
            self.assertFalse(analysis.validity(load(runs, [bad]), "download", 64 << 20)["valid"])

    def test_an_empty_second_in_the_reference_window_invalidates_it(self):
        runs = [[stream(0, 40 * S, stall=(3 * S, 4 * S + 200 * MS))]]
        v = analysis.validity(load(runs, [tr(15 * S, 17 * S)]), "download", 64 << 20)
        self.assertFalse(v["valid"])
        self.assertIn("reference", v["reason"])


class Gaps(unittest.TestCase):
    def test_gaps_overlapping_a_transfer_are_its_and_the_rest_are_the_references(self):
        runs = [[stream(0, 40 * S, stall=(15 * S, 15 * S + 300 * MS))]]
        g = analysis.gap_stats(load(runs, [tr(15 * S, 17 * S)]))
        self.assertEqual(g["gap_bulk_max_ms"], 350.0)  # the stall: 300 ms with no token, plus one 50 ms step
        self.assertEqual(g["gap_ref_max_ms"], 50.0)
        self.assertEqual(g["stall_max_ms"], 300.0)
        self.assertEqual(g["gap_ref_n"], 200)  # tokens at 1.00, 1.05, ... 11.00 s, both ends inside: 201 tokens, 200 gaps


class Metrics(unittest.TestCase):
    def test_rate_burst_and_the_rate_after_the_first_second_at_the_receiver(self):
        # 30 MB over 3 s at the receiver: 10 MB in the first second (a burst), then 10 MB/s.
        steps = [1_000_000 * (i + 1) for i in range(10)] + [10_000_000 + 1_000_000 * (i + 1) for i in range(20)]
        t = tr(0, 3 * S + 10 * MS, nbytes=30_000_000, first=0, last=3 * S, steps=steps)
        m = analysis.transfer_metrics([t], "embeddings", 64 << 20)
        self.assertEqual(m["transfers_ok"], 1)
        self.assertEqual(m["recv_rate_median"], 10_000_000)
        self.assertEqual(m["first_second_bytes_median"], 10_000_000)
        self.assertEqual(m["after_first_second_rate_median"], 10_000_000)

    def test_an_upload_reports_how_long_after_the_clients_last_write_the_receiver_had_it(self):
        t = tr(0, 2 * S, first=10 * MS, last=1500 * MS, sent=1200 * MS)
        t["kind"] = "upload"
        m = analysis.transfer_metrics([t], "upload", 64 << 20)
        self.assertEqual(m["upload_recv_last_after_sent_ms_median"], 300.0)

    def test_a_failed_transfer_is_not_counted(self):
        m = analysis.transfer_metrics([tr(0, S, err="cut")], "download", 64 << 20)
        self.assertEqual((m["transfers"], m["transfers_ok"], m["recv_rate_median"]), (1, 0, ""))



def btr(start, end, batch, nbytes=64 << 20, **kw):
    t = tr(start, end, nbytes, **kw)
    t["batch"] = batch
    return t


class Concurrent(unittest.TestCase):
    """Issue #413: a concurrent run whose transfers did not overlap in time proves nothing."""

    def runs(self):
        return [[stream(0, 60 * S)] for _ in range(2)]

    def test_transfers_that_overlap_fully_are_valid(self):
        batch = [btr(15 * S, 20 * S, 0), btr(15 * S + MS, 20 * S, 0), btr(15 * S, 19 * S + 800 * MS, 0), btr(15 * S + 2 * MS, 20 * S, 0)]
        v = analysis.validity(load(self.runs(), batch), "download", 64 << 20, concurrent=4)
        self.assertTrue(v["valid"], v["reason"])
        self.assertGreater(min(analysis.batch_overlap(batch).values()), 0.99)

    def test_transfers_one_after_the_other_are_not_valid(self):
        batch = [btr(15 * S, 17 * S, 0), btr(17 * S + MS, 19 * S, 0)]  # back to back: nothing in common
        v = analysis.validity(load(self.runs(), batch), "download", 64 << 20, concurrent=2)
        self.assertFalse(v["valid"])
        self.assertIn("overlapped", v["reason"])
        self.assertLessEqual(analysis.batch_overlap(batch)[0], 0.0)

    def test_a_staggered_batch_below_the_threshold_is_not_valid(self):
        # The second starts when the first has run 40% of its time: 60% in common, under 80%.
        batch = [btr(15 * S, 20 * S, 0), btr(17 * S, 20 * S, 0)]
        self.assertAlmostEqual(analysis.batch_overlap(batch)[0], 1.0, places=2)  # in common with the SHORTER span: all of it
        batch = [btr(15 * S, 20 * S, 0), btr(17 * S, 22 * S, 0)]  # equal spans, 3 s of 5 in common
        self.assertAlmostEqual(analysis.batch_overlap(batch)[0], 0.6, places=2)
        v = analysis.validity(load(self.runs(), batch), "download", 64 << 20, concurrent=2)
        self.assertFalse(v["valid"])

    def test_a_batch_with_a_missing_transfer_is_not_valid(self):
        v = analysis.validity(load(self.runs(), [btr(15 * S, 20 * S, 0)]), "download", 64 << 20, concurrent=2)
        self.assertFalse(v["valid"])
        self.assertIn("1 of 2", v["reason"])

    def test_every_batch_is_held_to_it(self):
        good = [btr(15 * S, 20 * S, 0), btr(15 * S, 20 * S, 0)]
        bad = [btr(25 * S, 27 * S, 1), btr(28 * S, 30 * S, 1)]
        v = analysis.validity(load(self.runs(), good + bad), "download", 64 << 20, concurrent=2)
        self.assertFalse(v["valid"])
        self.assertIn("batch 1", v["reason"])

    def test_a_single_transfer_batch_needs_no_overlap(self):
        v = analysis.validity(load(self.runs(), [btr(15 * S, 20 * S, 0), btr(25 * S, 30 * S, 1)]), "download", 64 << 20, concurrent=1)
        self.assertTrue(v["valid"], v["reason"])

    def test_batch_metrics_report_the_batch_time_and_aggregate_rate(self):
        batch = [btr(10 * S, 14 * S, 0, last=14 * S), btr(10 * S, 14 * S, 0, last=14 * S)]
        m = analysis.batch_metrics(batch, 2)
        self.assertEqual(m["batch_time_median_s"], 4.0)
        self.assertEqual(m["batch_rate_median"], round(2 * (64 << 20) / 4.0))
        self.assertEqual(m["overlap_min"], 1.0)


def breq(instance, start, end, first=None, nbytes=1 << 20, err="", status=200):
    return {"instance": instance, "start_ns": start, "resp_ns": start + MS, "first_ns": first if first is not None else start + 7 * MS,
            "end_ns": end, "bytes": nbytes, "status": status, "err": err}


def many(bulk, runs, window=(10 * S, 40 * S), interval=10 * S, nbytes=1 << 20):
    return {"bulk": bulk, "streams_runs": runs, "interval_ns": interval, "bytes": nbytes,
            "window_start_ns": window[0], "window_end_ns": window[1]}


class ManyInstances(unittest.TestCase):
    def runs(self, n=3):
        return [[stream(0, 100 * S)] for _ in range(n)]

    def test_requests_at_the_set_rate_beside_streaming_streams_are_valid(self):
        bulk = [breq(i, 11 * S + i * S, 11 * S + i * S + 50 * MS) for i in range(3)]
        self.assertTrue(analysis.validity_many(many(bulk, self.runs()))["valid"])

    def test_a_failed_request_is_not_valid(self):
        bulk = [breq(0, 11 * S, 11 * S + 50 * MS), breq(1, 12 * S, 12 * S + 50 * MS, err="EOF")]
        v = analysis.validity_many(many(bulk, self.runs()))
        self.assertFalse(v["valid"])
        self.assertIn("failed", v["reason"])

    def test_a_short_body_is_not_valid(self):
        v = analysis.validity_many(many([breq(0, 11 * S, 11 * S + 50 * MS, nbytes=1000)], self.runs()))
        self.assertFalse(v["valid"])

    def test_a_request_that_outlasts_the_interval_is_not_valid(self):
        v = analysis.validity_many(many([breq(0, 11 * S, 22 * S)], self.runs()))
        self.assertFalse(v["valid"])
        self.assertIn("interval", v["reason"])

    def test_a_stream_that_ended_before_the_last_request_is_not_valid(self):
        runs = self.runs()
        runs[1] = [stream(0, 15 * S)]
        v = analysis.validity_many(many([breq(0, 11 * S, 11 * S + 50 * MS), breq(1, 20 * S, 20 * S + 50 * MS)], runs))
        self.assertFalse(v["valid"])
        self.assertIn("stream 1", v["reason"])

    def test_metrics_are_the_time_to_first_byte_and_the_gaps_in_the_window(self):
        bulk = [breq(i, 11 * S + i * S, 11 * S + i * S + (20 + i) * MS, first=11 * S + i * S + (5 + 5 * i) * MS) for i in range(3)]
        m = analysis.many_metrics(many(bulk, self.runs()))
        self.assertEqual((m["bulk_requests"], m["bulk_ok"]), (3, 3))
        self.assertEqual(m["ttfb_p50_ms"], 10.0)
        self.assertEqual(m["ttfb_max_ms"], 15.0)
        self.assertEqual(m["total_max_ms"], 22.0)
        self.assertEqual(m["gap_p50_ms"], 50.0)

    def test_handshake_rates_are_the_counter_deltas_over_the_window(self):
        rows = [(int(t * 1e9), 2 * t, 3 * t) for t in range(100, 111)]  # accepts 2/s, hellos 3/s
        r = analysis.handshake_rates(rows, 102.0, 108.0)
        self.assertAlmostEqual(r["accepts_per_s"], 2.0)
        self.assertAlmostEqual(r["hellos_per_s"], 3.0)
        self.assertEqual((r["accepts"], r["hellos"]), (12, 18))
        self.assertIsNone(analysis.handshake_rates(rows, 500.0, 600.0))



class LateBatchStalls(unittest.TestCase):
    """The per-GiB stall of the real ssh path (the review of PR 416): every stream stalls just before a batch ends."""

    def streams(self, n=8, stall=None):
        return [[stream(0, 60 * S, stall=stall)] for _ in range(n)]

    def batches(self):
        # Four batches of four 64 MiB transfers, 10 s each, from 10 s: bytes moved by the end of batch b = (b+1) * 256 MiB.
        return [btr(10 * S + b * 10 * S, 19 * S + b * 10 * S, b) for b in range(4) for _ in range(4)]

    def test_all_streams_stalled_in_the_last_400_ms_of_a_batch_are_found_with_the_bytes_moved(self):
        # Batch 3 ends at 49 s; every stream's tokens stop from 48.7 s to 48.85 s: a gap of about 150 ms, ending 150 ms before it.
        runs = self.streams(stall=(48_650 * MS, 48_850 * MS))
        found = analysis.late_batch_stalls(load(runs, self.batches()))
        self.assertEqual(list(found), [3])
        self.assertEqual(found[3]["streams"], 8)
        self.assertGreater(found[3]["min_ms"], 150)
        self.assertLess(found[3]["max_ms"], 260)
        self.assertAlmostEqual(found[3]["gib"], 4 * 4 * 64 / 1024)
        self.assertGreater(found[3]["before_end_ms"], 100)
        self.assertLess(found[3]["before_end_ms"], 200)

    def test_a_stall_not_near_a_batch_end_is_not_found(self):
        runs = self.streams(stall=(14 * S, 14 * S + 200 * MS))  # in the middle of batch 0
        self.assertEqual(analysis.late_batch_stalls(load(runs, self.batches())), {})

    def test_a_stall_of_a_few_streams_is_not_found(self):
        runs = self.streams(stall=(48_650 * MS, 48_850 * MS))
        for rs in runs[:4]:
            rs[0] = stream(0, 60 * S)  # four of the eight streams do not stall
        self.assertEqual(analysis.late_batch_stalls(load(runs, self.batches())), {})

    def test_a_gap_of_100_ms_or_less_is_not_a_stall(self):
        runs = self.streams(stall=(48_700 * MS, 48_740 * MS))  # the gap is about 90 ms
        self.assertEqual(analysis.late_batch_stalls(load(runs, self.batches())), {})

    def test_batch_ttfb_is_each_transfers_first_byte_in_increasing_order(self):
        trs = [btr(10 * S, 12 * S, 0, first=10 * S + 9 * MS), btr(10 * S, 12 * S, 0, first=10 * S + 5 * MS),
               btr(20 * S, 22 * S, 1, first=20 * S + 7 * MS), btr(20 * S, 22 * S, 1, err="reset")]
        self.assertEqual(analysis.batch_ttfb(trs), {0: [5.0, 9.0], 1: [7.0]})



# A relay client's debug log (ssh -v), as OpenSSH 9.6 writes it, cut to the lines that matter and with the host, the
# user and the paths taken out: the serving relay's first exchange, and one rekey that the server started at 1 GiB.
SSH_LOG = """debug1: Reading configuration data /cfg
debug1: Local version string SSH-2.0-OpenSSH_9.6p1 Ubuntu-3ubuntu13.20
debug1: Remote protocol version 2.0, remote software version OpenSSH_9.6p1 Ubuntu-3ubuntu13.20
debug1: SSH2_MSG_KEXINIT sent
debug1: SSH2_MSG_KEXINIT received
debug1: kex: algorithm: sntrup761x25519-sha512@openssh.com
debug1: kex: host key algorithm: ssh-ed25519
debug1: kex: server->client cipher: chacha20-poly1305@openssh.com MAC: <implicit> compression: none
debug1: kex: client->server cipher: chacha20-poly1305@openssh.com MAC: <implicit> compression: none
debug1: rekey out after 134217728 blocks
debug1: SSH2_MSG_NEWKEYS sent
debug1: SSH2_MSG_NEWKEYS received
debug1: rekey in after 134217728 blocks
debug1: Sending command: srun -u --overlap --jobid=1 --ntasks=1 /stage/agent/x/hb-agent relay serving --dir /a --epoch 1
debug1: SSH2_MSG_KEXINIT received
debug1: SSH2_MSG_KEXINIT sent
debug1: kex: algorithm: sntrup761x25519-sha512@openssh.com
debug1: kex: host key algorithm: ssh-ed25519
debug1: kex: server->client cipher: chacha20-poly1305@openssh.com MAC: <implicit> compression: none
debug1: kex: client->server cipher: chacha20-poly1305@openssh.com MAC: <implicit> compression: none
debug1: rekeying in progress
debug1: ssh_set_newkeys: rekeying out, input 1073675124 bytes 134186899 blocks, output 580768 bytes 63363 blocks
debug1: rekey out after 134217728 blocks
debug1: SSH2_MSG_NEWKEYS sent
debug1: SSH2_MSG_NEWKEYS received
debug1: ssh_set_newkeys: rekeying in, input 1073675136 bytes 134186900 blocks, output 580788 bytes 2 blocks
debug1: rekey in after 134217728 blocks
"""


class SSHDebug(unittest.TestCase):
    def test_the_negotiated_algorithms_the_channel_and_the_rekey_are_read_from_the_log(self):
        r = analysis.parse_ssh_debug(SSH_LOG)
        self.assertEqual(r["local_version"], "OpenSSH_9.6p1 Ubuntu-3ubuntu13.20")
        self.assertEqual(r["remote_version"], "OpenSSH_9.6p1 Ubuntu-3ubuntu13.20")
        self.assertEqual(r["channel"], "serving")
        self.assertEqual(r["exchanges"], 2)  # the first and one rekey: NEWKEYS received twice
        self.assertEqual(len(r["kex"]), 2)
        want = {"algorithm": "sntrup761x25519-sha512@openssh.com", "host_key": "ssh-ed25519",
                "cipher_in": "chacha20-poly1305@openssh.com", "mac_in": "<implicit>", "compression_in": "none",
                "cipher_out": "chacha20-poly1305@openssh.com", "mac_out": "<implicit>", "compression_out": "none"}
        self.assertEqual(r["kex"][0], want)
        self.assertEqual(r["kex"][1], want)
        self.assertEqual(r["rekey_in"], [134217728, 134217728])
        self.assertEqual(r["rekey_out"], [134217728, 134217728])

    def test_who_started_each_exchange_is_the_side_whose_kexinit_came_first(self):
        # The first exchange: sent before received (the client). The rekey: received before sent (the server).
        self.assertEqual(analysis.parse_ssh_debug(SSH_LOG)["initiator"], ["client", "server"])

    def test_the_bytes_the_connection_had_moved_at_a_rekey_are_read(self):
        r = analysis.parse_ssh_debug(SSH_LOG)
        self.assertEqual(r["rekeys"], [{"dir": "out", "input_bytes": 1073675124, "output_bytes": 580768},
                                       {"dir": "in", "input_bytes": 1073675136, "output_bytes": 580788}])

    def test_a_control_relay_and_a_log_with_no_command_are_told_apart(self):
        ctl = SSH_LOG.replace("relay serving", "relay control")
        self.assertEqual(analysis.parse_ssh_debug(ctl)["channel"], "control")
        self.assertEqual(analysis.parse_ssh_debug("debug1: nothing\n")["channel"], "")
        self.assertEqual(analysis.parse_ssh_debug("")["exchanges"], 0)

    def test_nothing_of_the_host_or_the_user_is_kept(self):
        log = SSH_LOG + "debug1: Authenticating to 127.0.0.1:1234 as 'someone'\ndebug1: Server host key: ssh-ed25519 SHA256:abc\n"
        self.assertNotIn("someone", repr(analysis.parse_ssh_debug(log)))
        self.assertNotIn("127.0.0.1", repr(analysis.parse_ssh_debug(log)))


def dtr(start, first, last, nbytes, steps, batch=0):
    """A download: dialed at start, the first byte at first, the last at last (ns), with its 100 ms steps."""
    return {"kind": "download", "start_ns": start, "end_ns": last, "recv_first_ns": first, "recv_last_ns": last, "bytes": nbytes,
            "err": "", "status": 200, "steps_bytes": steps, "batch": batch}


class AllStreamStalls(unittest.TestCase):
    def test_bytes_moved_at_a_time_lie_between_the_steps_around_it(self):
        # 10 steps of 100 ms, 1,000,000 bytes each: at 250 ms after the first byte, between 200 and 300 ms of it.
        steps = [1_000_000 * (i + 1) for i in range(10)]
        load_ = {"transfers": [dtr(0, 1 * S, 2 * S, 10_000_000, steps)]}
        self.assertEqual(analysis.bytes_moved_at(load_, 1 * S + 250 * MS), (2_000_000, 3_000_000))
        self.assertEqual(analysis.bytes_moved_at(load_, 1 * S - MS), (0, 0))
        self.assertEqual(analysis.bytes_moved_at(load_, 2 * S + MS), (10_000_000, 10_000_000))
        self.assertEqual(analysis.bytes_moved_at(load_, 1 * S + 50 * MS), (0, 1_000_000))

    def test_bytes_moved_add_up_over_the_transfers(self):
        steps = [1_000_000 * (i + 1) for i in range(10)]
        load_ = {"transfers": [dtr(0, 1 * S, 2 * S, 10_000_000, steps), dtr(0, 1 * S, 2 * S, 10_000_000, steps)]}
        self.assertEqual(analysis.bytes_moved_at(load_, 1 * S + 250 * MS), (4_000_000, 6_000_000))

    def run_with_stalls(self, stalls_s, n=8):
        steps = [1_000_000 * (i + 1) for i in range(100)]
        trs = [dtr(10 * S, 10 * S + 5 * MS, 20 * S, 100_000_000, steps)]
        runs = []
        for _ in range(n):
            toks, t = [], 11 * S
            while t < 60 * S:
                if not any(a <= t < a + 200 * MS for a in stalls_s):
                    toks.append(t)
                t += 50 * MS
            runs.append([{"start_ns": 0, "end_ns": 60 * S, "tokens_ns": toks, "status": 200}])
        return load(runs, trs)

    def test_a_stall_of_every_stream_inside_the_transfers_is_found_with_its_bytes(self):
        found = analysis.all_stream_stalls(self.run_with_stalls([15 * S]))
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0]["streams"], 8)
        self.assertGreater(found[0]["min_ms"], 200)
        lo, hi = found[0]["moved_lo"], found[0]["moved_hi"]
        self.assertLessEqual(lo, 49_000_000)  # about 5 s of a 10 s, 100 MB transfer
        self.assertGreaterEqual(hi, 49_000_000)

    def test_several_stalls_are_found_in_time_order(self):
        found = analysis.all_stream_stalls(self.run_with_stalls([13 * S, 16 * S, 19 * S]))
        self.assertEqual([round((f["start_ns"]) / S) for f in found], [13, 16, 19])

    def test_a_stall_outside_the_transfers_is_not_found(self):
        self.assertEqual(analysis.all_stream_stalls(self.run_with_stalls([40 * S])), [])

    def test_a_stall_of_a_few_streams_is_not_found(self):
        ld = self.run_with_stalls([])
        runs = self.run_with_stalls([15 * S])["streams_runs"]
        ld["streams_runs"] = runs[:3] + ld["streams_runs"][3:]  # three streams stall, five do not
        self.assertEqual(analysis.all_stream_stalls(ld), [])

    def test_no_transfers_no_stalls(self):
        self.assertEqual(analysis.all_stream_stalls(load([[stream(0, 10 * S)]], [])), [])



def stall_at(mib_lo, mib_hi=None, ms=150):
    mib_hi = mib_lo if mib_hi is None else mib_hi
    return {"start_ns": 0, "streams": 8, "min_ms": ms, "max_ms": ms, "moved_lo": mib_lo * (1 << 20), "moved_hi": mib_hi * (1 << 20)}


class MatchRekeys(unittest.TestCase):
    MIB = 1 << 20

    def test_each_rekey_finds_the_stall_a_little_behind_its_byte_count(self):
        # The connection had received 257, 513 and 769 MiB at the rekeys; the client had 250, 507 and 767 MiB of payload.
        stalls = [stall_at(250, 252), stall_at(507, 509), stall_at(767, 769)]
        pairs, lonely, free = analysis.match_rekeys(stalls, [257 * self.MIB, 513 * self.MIB, 769 * self.MIB])
        self.assertEqual([p[0] // self.MIB for p in pairs], [257, 513, 769])
        self.assertEqual((lonely, free), ([], []))

    def test_a_stall_far_from_every_rekey_is_left_over_and_a_rekey_with_no_stall_is_named(self):
        stalls = [stall_at(250, 252), stall_at(400, 402)]  # 400 MiB: no rekey near it
        pairs, lonely, free = analysis.match_rekeys(stalls, [257 * self.MIB, 513 * self.MIB])
        self.assertEqual([p[0] // self.MIB for p in pairs], [257])
        self.assertEqual(lonely, [513 * self.MIB])
        self.assertEqual(free, [stalls[1]])

    def test_a_stall_ahead_of_the_rekey_by_more_than_the_window_does_not_match(self):
        pairs, lonely, free = analysis.match_rekeys([stall_at(230, 232)], [257 * self.MIB])  # 25 MiB behind: outside 16
        self.assertEqual((pairs, len(lonely), len(free)), ([], 1, 1))

    def test_a_stall_after_the_rekey_by_more_than_two_mib_does_not_match(self):
        pairs, lonely, free = analysis.match_rekeys([stall_at(262, 264)], [257 * self.MIB])
        self.assertEqual((pairs, len(lonely), len(free)), ([], 1, 1))

    def test_one_stall_pairs_with_one_rekey_only(self):
        pairs, lonely, free = analysis.match_rekeys([stall_at(250, 252)], [255 * self.MIB, 256 * self.MIB])
        self.assertEqual(len(pairs), 1)
        self.assertEqual(len(lonely), 1)



class MatchRekeysNearest(unittest.TestCase):
    def test_a_rekey_takes_the_nearest_stall_not_the_first_in_the_window(self):
        MIB = 1 << 20
        far, near = stall_at(245, 247), stall_at(254, 256)  # both inside the window of a rekey at 257 MiB
        pairs, lonely, free = analysis.match_rekeys([far, near], [257 * MIB])
        self.assertIs(pairs[0][1], near)
        self.assertEqual(free, [far])



class SSHDebugDirections(unittest.TestCase):
    def test_the_receive_and_send_algorithms_are_told_apart(self):
        log = ("debug1: kex: algorithm: curve25519-sha256\n"
               "debug1: kex: host key algorithm: ssh-ed25519\n"
               "debug1: kex: server->client cipher: aes128-ctr MAC: hmac-sha2-256-etm@openssh.com compression: none\n"
               "debug1: kex: client->server cipher: chacha20-poly1305@openssh.com MAC: <implicit> compression: zlib\n")
        k = analysis.parse_ssh_debug(log)["kex"][0]
        self.assertEqual((k["cipher_in"], k["mac_in"], k["compression_in"]), ("aes128-ctr", "hmac-sha2-256-etm@openssh.com", "none"))
        self.assertEqual((k["cipher_out"], k["mac_out"], k["compression_out"]), ("chacha20-poly1305@openssh.com", "<implicit>", "zlib"))

    def test_rekey_limits_in_and_out_are_kept_apart(self):
        log = "debug1: rekey out after 100 blocks\ndebug1: rekey in after 200 blocks\n"
        r = analysis.parse_ssh_debug(log)
        self.assertEqual((r["rekey_out"], r["rekey_in"]), ([100], [200]))

    def test_the_bytes_at_a_rekey_are_the_connections_received_bytes_not_its_sent_ones(self):
        log = "debug1: ssh_set_newkeys: rekeying in, input 5000 bytes 625 blocks, output 70 bytes 9 blocks\n"
        self.assertEqual(analysis.parse_ssh_debug(log)["rekeys"], [{"dir": "in", "input_bytes": 5000, "output_bytes": 70}])



class SSHDebugChannel(unittest.TestCase):
    def test_the_channel_is_the_one_the_sent_command_names_not_any_line_that_says_relay(self):
        log = "debug1: a note about relay control\ndebug1: Sending command: srun /x/hb-agent relay serving --dir /a\n"
        self.assertEqual(analysis.parse_ssh_debug(log)["channel"], "serving")
        self.assertEqual(analysis.parse_ssh_debug("debug1: a note about relay control\n")["channel"], "")


if __name__ == "__main__":
    unittest.main()
