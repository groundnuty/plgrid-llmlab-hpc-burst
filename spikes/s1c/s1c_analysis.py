"""Spike S1c: the pure rules that turn a measurement into a result, kept apart so they can be tested
(tests/test_analysis.py) and applied the same way by the driver and by reanalyze.py."""

GONE = ("offline", "absent")          # the only admin-API observations that count as detection
FRONT = {"tcpmux": ("frps",), "wss": ("frps", "envoy"), "ssh": ("frps", "sshd"), "direct": ()}
PATH = ("frpc", "frps", "envoy", "ssh", "sshd")


def per_ktok(cpu_pct, seconds, tokens):
    """Percent of one core per 1,000 tokens/s, with CPU and tokens over the SAME interval: the CPU
    measured over `seconds` and the tokens that arrived in that run."""
    if not tokens or not seconds:
        return None
    return cpu_pct * seconds * 1000.0 / tokens


def hol_validity(summary, nbytes, read_rate, give_up_s):
    """'' when the bulk injection did what its variant needs, else the reason the run is invalid.
    A large reply (read_rate 0) must arrive whole without error. A slow reader must be abandoned by
    design after give_up_s, having read about read_rate * give_up_s."""
    err = summary.get("bulk_err") or ""
    got = summary.get("bulk_bytes_read") or 0
    if read_rate == 0:
        if err:
            return "bulk error: %s" % err[:80]
        if got != nbytes:
            return "bulk read %d of %d bytes" % (got, nbytes)
        return ""
    if "gave up" not in err:
        return "slow reader was not abandoned by design: %r" % err[:80]
    want = read_rate * give_up_s
    if not 0.7 * want <= got <= 1.1 * want:
        return "slow reader read %d bytes, expected about %d" % (got, want)
    return ""


def detection(rows, t0, t_heal=None):
    """Detection of a dead tunnel from the probe's CSV rows (kind, start, end, outcome, detail).

    frps has noticed only when its admin API says the proxy is offline or absent. An API error, a
    refused login or a malformed answer is a failed measurement, counted in api_errors, never a
    detection. With no explicit observation the run is censored (api_gone None)."""
    api = [(a, o) for k, a, _, o, _ in rows if k == "api" and a >= t0]
    probes = [(a, b, o) for k, a, b, o, _ in rows if k == "probe" and a >= t0]
    gone = next((a for a, o in api if o in GONE), None)
    refused = next((a for a, _, o in probes if o == "refused"), None)
    waits = [(b - a, o) for a, b, o in probes
             if o in ("timeout", "closed") and (refused is None or a < refused)]
    w = sorted(d for d, _ in waits)
    return dict(
        api_gone=gone,
        api_errors=sum(1 for _, o in api if o != "online" and o not in GONE),
        first_refused=refused,
        hung_n=len(waits),
        hung_med=w[len(w) // 2] if w else None,
        hung_max=w[-1] if w else None,
        hung_timeouts=sum(1 for _, o in waits if o == "timeout"),   # cut by the probe's own deadline
        ok_after=sum(1 for a, _, o in probes if o == "ok" and (t_heal is None or a < t_heal)),
        heal_online=next((a for a, o in api if t_heal is not None and a >= t_heal and o == "online"), None),
        heal_probe_ok=next((a for a, _, o in probes if t_heal is not None and a >= t_heal and o == "ok"), None),
    )
