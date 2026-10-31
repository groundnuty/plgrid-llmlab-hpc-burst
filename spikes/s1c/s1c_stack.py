"""Spike S1c: bring the engine, the link and each tunnel arm up and down, and measure a cell."""
import json
import os
import subprocess
import threading
import time

from s1c_lib import (PORTS, Procs, assert_loopback_only, cpu_ticks, descendants, http_json, load1,
                     log, make_certs, make_ssh, render_envoy, render_frpc, render_frps, rss_kb,
                     clean_env, ssh_argv, tox_create_all, wait_port)

SIM_ARGS = ["--model", "sim", "--inter-token-latency", "50ms", "--time-to-first-token", "50ms",
            "--max-num-seqs", "1024", "--max-model-len", "4096", "--max-waiting-queue-length", "4000"]
CLK = os.sysconf("SC_CLK_TCK")
# Where frpc (or ssh, for the ssh arm) dials, per arm and per link ("tox" = toxiproxy, "shaper").
DIAL = {
    ("tcpmux", "tox"): PORTS["tox_frp"], ("tcpmux", "shaper"): PORTS["sh_frp"],
    ("wss", "tox"): PORTS["tox_tls"], ("wss", "shaper"): PORTS["sh_tls"],
    ("ssh", "tox"): PORTS["tox_ssh"], ("ssh", "shaper"): PORTS["sh_ssh"],
}
TOX_OF_ARM = {"direct": "direct", "tcpmux": "frp", "wss": "tls", "ssh": "ssh"}


class Stack:
    def __init__(self, env):
        self.env, self.P = env, Procs(env)
        self.user = None

    # --- always-on parts: engine, bulk engine, toxiproxy -------------------------------------
    def base_up(self, tox=True):
        e = self.env
        self.P.start("sim", [e.sim, "--port", str(PORTS["sim"])] + SIM_ARGS)
        self.P.start("bulk", [e.b("s1cbench"), "bulk", "-listen", "127.0.0.1:%d" % PORTS["bulk"]])
        wait_port(PORTS["sim"])
        wait_port(PORTS["bulk"])
        if tox:
            self.P.start("tox", [e.b("toxiproxy-server"), "-host", "127.0.0.1", "-port", str(PORTS["tox_api"])])
            wait_port(PORTS["tox_api"])
            tox_create_all()
        make_certs(e)
        self.user = make_ssh(e)
        assert_loopback_only()

    def shaper_up(self, rate):
        maps = [(PORTS["sh_frp"], PORTS["frps"]), (PORTS["sh_tls"], PORTS["envoy"]), (PORTS["sh_bulk"], PORTS["bulk"]),
                (PORTS["sh_sim"], PORTS["sim"]), (PORTS["sh_ssh"], PORTS["sshd"])]
        argv = [self.env.b("s1cbench"), "shaper", "-rate", str(rate), "-down-rate", str(rate)]
        for lp, up in maps:
            argv += ["-map", "127.0.0.1:%d=127.0.0.1:%d" % (lp, up)]
        self.P.start("shaper", argv)
        wait_port(PORTS["sh_frp"])

    # --- one tunnel arm ---------------------------------------------------------------------
    def arm_up(self, arm, via="tox", tcpmux=True, setting="s1", level="info", pool=0):
        if arm == "direct":
            return
        e = self.env
        for f in ("frps-frp.log", "frpc-frp.log"):
            if os.path.exists(e.p("logs", f)):
                os.replace(e.p("logs", f), e.p("logs", f + ".prev"))
        self.P.start("frps", [e.b("frps"), "-c", render_frps(e, setting, level)])
        wait_port(PORTS["mux"])
        wait_port(PORTS["dash"])
        port, proto = DIAL[(arm, via)], "tcp"
        if arm == "wss":
            self.P.start("envoy", [e.envoy, "-c", render_envoy(e), "--disable-hot-restart", "--concurrency", "2",
                                   "--log-level", "warn"])
            wait_port(PORTS["envoy"])
            proto = "wss"
        if arm == "ssh":
            self.P.start("sshd", ["/usr/sbin/sshd", "-D", "-e", "-f", e.p("ssh", "sshd_config")])
            wait_port(PORTS["sshd"])
            self.P.start("ssh", ssh_argv(e, self.user, port), env=clean_env())
            wait_port(PORTS["ssh_l"])
            port = PORTS["ssh_l"]
        self.P.start("frpc", [e.b("frpc"), "-c", render_frpc(e, port, proto, setting, tcpmux, pool, level)])
        self.wait_online(["inst.engine", "inst.bulk"])
        assert_loopback_only()

    def arm_down(self):
        for name in ("frpc", "ssh", "sshd", "envoy", "frps"):
            self.P.stop(name)

    def api(self):
        with open(self.env.p("run", "dash.pass")) as fh:
            pw = fh.read().strip()
        r = http_json("GET", "http://127.0.0.1:%d/api/proxy/tcpmux" % PORTS["dash"], auth=("admin", pw))
        return {p["name"]: p.get("status") for p in r.get("proxies") or []}

    def wait_online(self, names, timeout=30.0):
        end = time.time() + timeout
        while time.time() < end:
            try:
                st = self.api()
                if all(st.get(n) == "online" for n in names):
                    return
            except OSError:
                pass
            time.sleep(0.1)
        raise RuntimeError("proxies %s not online after %.0f s" % (names, timeout))

    # --- resource accounting ----------------------------------------------------------------
    def roles(self):
        """PIDs per role, taken now (sshd's per-connection children included)."""
        out = {}
        for name in ("frpc", "frps", "envoy", "ssh", "sshd", "tox", "shaper", "sim"):
            if self.P.alive(name):
                pid = self.P.pid(name)
                out[name] = [pid] + (descendants(pid) if name == "sshd" else [])
        return out


class Meter:
    """CPU (percent of one core) and peak RSS (MB) per role over one measurement."""

    def __init__(self, roles):
        self.roles, self.t0 = roles, time.time()
        self.c0 = {r: sum(cpu_ticks(p) or 0 for p in ps) for r, ps in roles.items()}
        self.peak = {r: 0 for r in roles}
        self.stop_ev = threading.Event()
        self.th = threading.Thread(target=self._sample, daemon=True)
        self.th.start()

    def _sample(self):
        while not self.stop_ev.is_set():
            for r, ps in self.roles.items():
                self.peak[r] = max(self.peak[r], sum(rss_kb(p) or 0 for p in ps))
            self.stop_ev.wait(0.5)

    def done(self):
        self.stop_ev.set()
        self.th.join()
        dt = time.time() - self.t0
        out = {}
        for r, ps in self.roles.items():
            c1 = sum(cpu_ticks(p) or 0 for p in ps)
            out["cpu_" + r] = round(100.0 * (c1 - self.c0[r]) / CLK / dt, 1)
            out["rss_" + r] = round(self.peak[r] / 1024.0, 1)
        return out


def wire_bytes(port):
    """bytes_received by the link's listening side, summed over its established sockets (from the
    kernel's tcp_info via ss): what frpc (or ssh) sent over the link."""
    out = subprocess.run(["ss", "-tinH", "state", "established", "( sport = :%d )" % port],
                         capture_output=True, text=True).stdout
    total = 0
    for tok in out.split():
        if tok.startswith("bytes_received:"):
            total += int(tok.split(":", 1)[1])
    return total


def run_bench(env, args, out_json, timeout=900):
    cmd = ["nice", "-n", "10", env.b("s1cbench")] + args + ["-out", out_json]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if r.returncode != 0:
        raise RuntimeError("s1cbench %s failed: %s" % (args[0], r.stderr.strip()[-500:]))
    with open(out_json) as fh:
        return json.load(fh)


def count_lines(path, needles):
    n = {k: 0 for k in needles}
    try:
        with open(path, errors="replace") as fh:
            for line in fh:
                for k in needles:
                    if k in line:
                        n[k] += 1
    except OSError:
        pass
    return n


def note_load(tag):
    l = load1()
    log("%s (load %.2f)" % (tag, l))
    return l
