#!/usr/bin/env python3
"""Spike S16 driver (issue #412, HBEP-6 open question 11): the four open items that need no GPU.

    python3 s16.py relogin --rep 1 [--sizes 1 10 100 300 600] [--events frps front none]
    python3 s16.py caplink --rep 1 --tunnels 100 --streams 4
    python3 s16.py rssh    --rep 1 [--ciphers default aes128-gcm@openssh.com]
    python3 s16.py rfront  --rep 1
    python3 s16.py host            the host's CPU model, count and memory, as JSON
    python3 s16.py leftovers       recorded processes still alive, listeners left in 30130-30159

Environment: S16_BIN (frps, frpc, s16, s16link, s16node.test, s16login.test, s16ssh), S16_WORK (a
short scratch directory: sockets live in it), S16_RESULTS (where raw/ goes). Every process is
started by this driver and signalled only through a pidfd opened right after its start, before
anything could reap it (os.pidfd_open, signal.pidfd_send_signal); no process is signalled by its
number or by name. The driver writes raw files only; analysis.py and summarize.py read them.
"""
import argparse
import base64
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import analysis  # noqa: E402

# Issue #412's block, 30130-30159, laid out by agentapi.Host at base 30130 (the launcher's offsets),
# plus the front's client listener, the link and the filler. Everything else is kernel-chosen.
BASE = 30130
PORTS = dict(control=BASE + 3, health=BASE + 4, dialhome=BASE + 7, plugin=BASE + 8, frps=BASE + 9, mux=BASE + 10,
             web=BASE + 11, client=BASE + 12, link=BASE + 13, fill_link=BASE + 14, fill_sink=BASE + 15)
PORT_RANGE = (30130, 30159)
CLK = os.sysconf("SC_CLK_TCK")
SAMPLE_S = 1.0          # the /proc sampler's period
POLL_S = 1.0            # frps's proxy list (the controller reads it every 10 s; finer here, for timing)
MEM_FLOOR_KB = 700_000  # a run stops if the host's available memory falls below this
FRONT_ROLES = ("frps", "admit", "front")


def log(msg):
    print("%s %s" % (time.strftime("%H:%M:%S", time.gmtime()), msg), flush=True)


class Env:
    def __init__(self):
        self.bin = os.environ["S16_BIN"]
        self.work = os.environ["S16_WORK"]
        self.results = os.environ["S16_RESULTS"]
        self.nice_node = int(os.environ.get("S16_NICE_NODE", "10"))
        os.makedirs(os.path.join(self.results, "raw"), exist_ok=True)

    def b(self, name):
        return os.path.join(self.bin, name)


# --- processes ------------------------------------------------------------------------------------

class Procs:
    """The run's processes. Each is signalled only through the pidfd opened right after its start
    (the driver never waits on a child before that, so its PID cannot be reused under the pidfd)."""

    def __init__(self, run_dir):
        self.run_dir, self.procs = run_dir, {}

    def start(self, name, argv, nice=0, env=None, logname=None, cwd=None):
        logf = open(os.path.join(self.run_dir, "logs", (logname or name) + ".out"), "ab")
        pre = (lambda: os.nice(nice)) if nice else None
        p = subprocess.Popen(argv, stdout=logf, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                             start_new_session=True, env=env, preexec_fn=pre, cwd=cwd)
        fd = os.pidfd_open(p.pid)
        logf.close()
        self.procs[name] = (p, fd)
        return p.pid

    def pid(self, name):
        return self.procs[name][0].pid

    def alive(self, name):
        return name in self.procs and self.procs[name][0].poll() is None

    def signal(self, name, sig):
        p, fd = self.procs[name]
        if p.poll() is None:
            signal.pidfd_send_signal(fd, sig)

    def _release(self, name):
        p, fd = self.procs.pop(name)
        os.close(fd)
        return p.returncode

    def stop(self, name, timeout=15.0):
        if name not in self.procs:
            return None
        p, fd = self.procs[name]
        if p.poll() is None:
            signal.pidfd_send_signal(fd, signal.SIGTERM)
            try:
                p.wait(timeout)
            except subprocess.TimeoutExpired:
                if p.poll() is None:
                    signal.pidfd_send_signal(fd, signal.SIGKILL)
                p.wait(5)
        return self._release(name)

    def wait(self, name, timeout):
        p, _ = self.procs[name]
        p.wait(timeout)
        return self._release(name)

    def stop_all(self, names, timeout=15.0):
        """TERM to all the named processes at once, then wait (frpc in parallel)."""
        live = [n for n in names if self.alive(n)]
        for n in live:
            self.signal(n, signal.SIGTERM)
        deadline = time.time() + timeout
        for n in live:
            p, fd = self.procs[n]
            try:
                p.wait(max(0.1, deadline - time.time()))
            except subprocess.TimeoutExpired:
                if p.poll() is None:
                    signal.pidfd_send_signal(fd, signal.SIGKILL)
                p.wait(5)
        for n in names:
            if n in self.procs:
                self._release(n)

    def leftovers(self):
        return [n for n in self.procs if self.alive(n)]


# --- /proc ----------------------------------------------------------------------------------------

def proc_stat(pid):
    """(ticks, comm, ppid) of a process, or None."""
    try:
        with open("/proc/%d/stat" % pid) as fh:
            s = fh.read()
        comm = s[s.index("(") + 1:s.rindex(")")]
        f = s.rsplit(")", 1)[1].split()
        return int(f[11]) + int(f[12]), comm, int(f[1])
    except (OSError, IndexError, ValueError):
        return None


def proc_mem(pid):
    rss = hwm = None
    try:
        with open("/proc/%d/status" % pid) as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    rss = int(line.split()[1])
                elif line.startswith("VmHWM:"):
                    hwm = int(line.split()[1])
    except OSError:
        pass
    return rss, hwm


def proc_fds(pid):
    try:
        return len(os.listdir("/proc/%d/fd" % pid))
    except OSError:
        return None


def children(pid):
    out = []
    try:
        for tid in os.listdir("/proc/%d/task" % pid):
            with open("/proc/%d/task/%s/children" % (pid, tid)) as fh:
                out += [int(x) for x in fh.read().split()]
    except OSError:
        pass
    return out


def descendants(pid):
    out, todo = [], [pid]
    while todo:
        c = children(todo.pop())
        out += c
        todo += c
    return out


def stat_cpu():
    with open("/proc/stat") as fh:
        return [int(x) for x in fh.readline().split()[1:]]


def mem_available_kb():
    with open("/proc/meminfo") as fh:
        for line in fh:
            if line.startswith("MemAvailable:"):
                return int(line.split()[1])
    return None


def loadavg():
    with open("/proc/loadavg") as fh:
        return [float(x) for x in fh.read().split()[:3]]


def listeners(pids=None):
    """Listening TCP sockets as (address, port, pid or None): all in the block, or those of pids."""
    out = subprocess.run(["ss", "-ltnpH"], capture_output=True, text=True, check=True).stdout
    res, want = [], set(pids or [])
    for line in out.splitlines():
        f = line.split()
        if len(f) < 4:
            continue
        addr, _, port = f[3].rpartition(":")
        owners = [int(part.split(",")[0]) for part in line.split("pid=")[1:]]
        if pids is None:
            if port.isdigit() and PORT_RANGE[0] <= int(port) <= PORT_RANGE[1]:
                res.append((addr, int(port), owners[0] if owners else None))
        else:
            for pid in owners:
                if pid in want:
                    res.append((addr, int(port), pid))
    return res


class Sampler(threading.Thread):
    """Every SAMPLE_S: per role, each PID's CPU ticks, the role's summed RSS and descriptors; the
    host's /proc/stat, available memory and load. Every POLL_S: frps's proxy list."""

    def __init__(self, roles_fn, poll_fn):
        super().__init__(daemon=True)
        self.roles_fn, self.poll_fn = roles_fn, poll_fn
        self.samples, self.polls, self.stop_ev = [], [], threading.Event()
        self.low_memory = None

    def sample(self):
        s = {"t": time.time(), "ticks": {}, "rss_kb": {}, "fds": {}, "stat": stat_cpu(),
             "mem_avail_kb": mem_available_kb(), "load": loadavg()}
        for role, pids in self.roles_fn().items():
            ticks, rss, fds = {}, 0, 0
            for pid in pids:
                st = proc_stat(pid)
                if st is None:
                    continue
                ticks[str(pid)] = st[0]
                rss += proc_mem(pid)[0] or 0
                fds += proc_fds(pid) or 0
            s["ticks"][role], s["rss_kb"][role], s["fds"][role] = ticks, rss, fds
        self.samples.append(s)
        if s["mem_avail_kb"] is not None and s["mem_avail_kb"] < MEM_FLOOR_KB and self.low_memory is None:
            self.low_memory = s["mem_avail_kb"]

    def run(self):
        next_poll = 0.0
        while not self.stop_ev.is_set():
            self.sample()
            if self.poll_fn and time.time() >= next_poll:
                next_poll = time.time() + POLL_S
                t = time.time()
                try:
                    self.polls.append({"t": t, "proxies": self.poll_fn()})
                except Exception as e:  # an unreadable list is counted, never read as online
                    self.polls.append({"t": t, "error": str(e)[:200]})
            self.stop_ev.wait(SAMPLE_S)

    def stop(self):
        self.stop_ev.set()
        self.join(10)
        self.sample()


# --- helpers --------------------------------------------------------------------------------------

def http_json(method, url, auth=None, timeout=5):
    req = urllib.request.Request(url, method=method)
    if auth:
        req.add_header("Authorization", "Basic " + base64.b64encode(("%s:%s" % auth).encode()).decode())
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def wait_for(what, cond, timeout, alive=None, step=0.05):
    end = time.time() + timeout
    while time.time() < end:
        v = cond()
        if v:
            return v
        if alive and not alive():
            raise RuntimeError("a process exited while waiting for %s" % what)
        time.sleep(step)
    raise RuntimeError("%s: not within %.0f s" % (what, timeout))


def read_json(path):
    try:
        with open(path) as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


def read_lines(path):
    out = []
    try:
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        out.append(json.loads(line))
                    except ValueError:
                        pass
    except OSError:
        pass
    return out


def host_info():
    model, n = None, 0
    with open("/proc/cpuinfo") as fh:
        for line in fh:
            if line.startswith("model name") and model is None:
                model = line.split(":", 1)[1].strip()
            if line.startswith("processor"):
                n += 1
    with open("/proc/meminfo") as fh:
        total = int(fh.readline().split()[1])
    return {"cpu_model": model, "cpus": n, "mem_total_mb": total // 1024, "kernel": os.uname().release}


def count_lines(path, needles):
    counts = {k: 0 for k in needles}
    try:
        with open(path, errors="replace") as fh:
            for line in fh:
                for k in needles:
                    if k in line:
                        counts[k] += 1
    except OSError:
        pass
    return counts


# --- one stack ------------------------------------------------------------------------------------

class Stack:
    """One configuration's processes: the link, the admission plugin, frps, the front, the node
    and every frpc, all on the front host's loopback, plus (host-dialed) the login node and the
    relay owner. It records every event it causes with its time."""

    def __init__(self, env, run_id, n, mode="direct", token_delay_ms=200, link_args=None, filler_conns=0):
        self.env, self.run_id, self.n, self.mode = env, run_id, n, mode
        self.run = os.path.join(env.work, run_id)
        for d in ("logs", "tmp", "ctl", "sshlog"):
            os.makedirs(os.path.join(self.run, d), mode=0o700)
        self.P = Procs(self.run)
        self.events = []
        self.link_args = link_args or []
        self.filler_conns = filler_conns
        self.token_delay_ms = token_delay_ms
        self.front_gen = 0
        self.sampler = None

    def f(self, name):
        return os.path.join(self.run, name)

    def event(self, ev, **kw):
        e = {"event": ev, "t": time.time()}
        e.update(kw)
        self.events.append(e)
        return e["t"]

    def setup(self):
        left = listeners()
        if left:
            raise RuntimeError("ports in 30130-30159 are taken before the run: %s" % left)
        subprocess.run([self.env.b("s16"), "setup", "-dir", self.f("c"), "-n", str(self.n), "-mode", self.mode,
                        "-base", str(BASE), "-server", "127.0.0.1:%d" % PORTS["link"],
                        "-client-listen", "127.0.0.1:%d" % PORTS["client"], "-token-delay", "%dms" % self.token_delay_ms],
                       check=True)
        self.spec_path = self.f("c/spec.json")
        self.spec = read_json(self.spec_path)

    def start_link(self):
        maps = ["-map", "127.0.0.1:%d=127.0.0.1:%d" % (PORTS["link"], PORTS["dialhome"])]
        if self.filler_conns:
            maps += ["-map", "127.0.0.1:%d=127.0.0.1:%d" % (PORTS["fill_link"], PORTS["fill_sink"])]
        self.P.start("link", [self.env.b("s16link")] + maps + self.link_args +
                     ["-stats", self.f("link-stats.json"), "-series", self.f("link-series.jsonl")])

    def start_core(self):
        """The link, the admission plugin, frps and the front, each answering before the next."""
        if self.mode == "direct":
            self.start_link()
        self.P.start("admit", [self.env.b("s16"), "admit", "-spec", self.spec_path, "-calls", self.f("admit-calls.jsonl"),
                               "-ready", self.f("admit-ready.json")])
        wait_for("the admission plugin", lambda: os.path.exists(self.f("admit-ready.json")), 30, lambda: self.P.alive("admit"))
        self.P.start("frps", [self.env.b("s16"), "frps", "-spec", self.spec_path, "-bin", self.env.b("frps"),
                              "-events", self.f("frps-events.jsonl")])
        wait_for("frps", lambda: any(e.get("event") == "ready" for e in read_lines(self.f("frps-events.jsonl"))), 30,
                 lambda: self.P.alive("frps"))
        self.start_front()

    def start_front(self):
        self.front_gen += 1
        g = self.front_gen
        self.P.start("front", [self.env.b("s16"), "front", "-spec", self.spec_path, "-probes", self.f("probes.jsonl"),
                               "-reg", self.f("reg-%d.jsonl" % g), "-calls", self.f("front-calls.jsonl"),
                               "-life", self.f("front-life.jsonl"), "-log", self.f("logs/front.log")])
        wait_for("the front", lambda: sum(1 for e in read_lines(self.f("front-life.jsonl")) if e.get("event") == "ready") >= g,
                 60, lambda: self.P.alive("front"))
        self.event("front-ready", gen=g)

    def start_node(self):
        env = dict(os.environ, S16_NODE="1")
        self.P.start("node", [self.env.b("s16node.test"), "-spec", self.spec_path, "-tmp", self.f("tmp"),
                              "-ready", self.f("node-ready.json"), "-state", self.f("node-state.json"),
                              "-log", self.f("logs/node.log")], nice=self.env.nice_node, env=env)
        wait_for("the node", lambda: os.path.exists(self.f("node-ready.json")), 60, lambda: self.P.alive("node"))
        if self.mode == "host-dialed":
            subprocess.run([self.env.b("s16"), "frpcconf", "-spec", self.spec_path, "-ready", self.f("node-ready.json")], check=True)
        if self.filler_conns:
            self.P.start("filler-sink", [self.env.b("s16"), "filler", "-role", "sink", "-addr", "127.0.0.1:%d" % PORTS["fill_sink"],
                                         "-out", self.f("filler-sink.json")], nice=self.env.nice_node)

    def frps_list(self):
        r = http_json("GET", "http://127.0.0.1:%d/api/proxy/tcpmux" % PORTS["web"], auth=("hpcburst", self.spec["frps_web_password"]))
        idx = {t["uid"]: t["index"] for t in self.spec["tunnels"]}
        return [[idx.get(p["name"], -1), p.get("status"), p.get("lastStartTime"), p.get("lastCloseTime")]
                for p in (r.get("proxies") or [])]

    def online(self):
        try:
            return sum(1 for p in self.frps_list() if p[0] >= 0 and p[1] == "online")
        except Exception:
            return 0

    def registered(self):
        """UIDs the current front routes to: its latest registration state per UID."""
        state = {}
        for e in read_lines(self.f("reg-%d.jsonl" % self.front_gen)):
            state[e["i"]] = e["reg"]
        return sum(1 for v in state.values() if v)

    def start_frpc(self, timeout=90):
        t0 = self.event("frpc-start")
        pathenv = {"PATH": os.environ.get("PATH", "/usr/bin:/bin")}
        for t in self.spec["tunnels"]:
            self.P.start("frpc-%03d" % t["index"], [self.env.b("frpc"), "-c", t["frpc"]], nice=self.env.nice_node,
                         env=pathenv, logname="frpc")
        wait_for("every proxy online", lambda: self.online() == self.n, timeout, step=0.25)
        self.event("all-online", s=round(time.time() - t0, 3))
        wait_for("every tunnel routed (a passing probe)", lambda: self.registered() == self.n, timeout, step=0.25)
        self.event("all-routed", s=round(time.time() - t0, 3))

    def roles(self):
        r = {"driver": [os.getpid()]}
        P = self.P
        for name in ("admit", "front", "node", "link", "load", "filler-src", "filler-sink", "owner", "login"):
            if P.alive(name):
                r[name] = [P.pid(name)]
        if P.alive("frps"):
            r["frps-sup"] = [P.pid("frps")]
            r["frps"] = children(P.pid("frps"))
        r["frpc"] = [P.pid(k) for k in list(P.procs) if k.startswith("frpc-") and P.alive(k)]
        if P.alive("owner"):
            kids = descendants(P.pid("owner"))
            r["ssh"] = [k for k in kids if (proc_stat(k) or (0, ""))[1] == "ssh"]
        if P.alive("login"):
            kids = descendants(P.pid("login"))
            r["sshd"] = [k for k in kids if (proc_stat(k) or (0, ""))[1].startswith("sshd")]
            r["login-rest"] = [k for k in kids if k not in r["sshd"]]
        return r

    def start_sampler(self):
        self.sampler = Sampler(self.roles, self.frps_list)
        self.sampler.start()

    def check_loopback(self):
        held = listeners([self.P.pid(k) for k in self.P.procs if self.P.alive(k)] +
                         [c for c in (children(self.P.pid("frps")) if self.P.alive("frps") else [])])
        off = [h for h in held if h[0] != "127.0.0.1"]
        if off:
            raise RuntimeError("listeners off loopback: %s" % off)
        return len(held)

    def start_load(self, via, streams, duration, ramp=5.0, max_tokens=60, name="load", holdoff=0.0):
        out = self.f("%s.jsonl" % name)
        self.P.start(name, [self.env.b("s16"), "load", "-spec", self.spec_path, "-via", via, "-streams", str(streams),
                            "-ramp", "%.3fs" % ramp, "-duration", "%.3fs" % duration, "-max-tokens", str(max_tokens),
                            "-holdoff", "%.3fs" % holdoff, "-out", out, "-ready", self.f("%s-ready.json" % name)])
        return self.event("load-start", name=name, via=via, streams=streams, ramp=ramp, duration=duration, holdoff=holdoff,
                          max_tokens=max_tokens)

    def restart_frps(self):
        t = self.event("frps-restart")
        self.P.signal("frps", signal.SIGUSR1)
        n0 = sum(1 for e in read_lines(self.f("frps-events.jsonl")) if e.get("event") == "ready")
        wait_for("frps back", lambda: sum(1 for e in read_lines(self.f("frps-events.jsonl")) if e.get("event") == "ready") > n0,
                 60, lambda: self.P.alive("frps"))
        return t

    def restart_front(self):
        t = self.event("front-term")
        self.P.signal("front", signal.SIGTERM)
        self.P.wait("front", 60)
        self.event("front-exited")
        self.start_front()
        return t

    def stop(self):
        P = self.P
        for name in ("load", "filler-src"):
            P.stop(name)
        P.stop("owner", 30)
        P.stop_all([k for k in list(P.procs) if k.startswith("frpc-")])
        for name in ("node", "filler-sink", "front", "frps", "admit", "link", "login"):
            P.stop(name, 30)
        if self.sampler and self.sampler.is_alive():
            self.sampler.stop()

    def collect(self, manifest, extra=()):
        """The raw files of the run, under S16_RESULTS/raw/<run_id>/; the logs stay in the work dir."""
        dst = os.path.join(self.env.results, "raw", self.run_id)
        os.makedirs(dst, exist_ok=True)
        names = ["admit-calls.jsonl", "front-calls.jsonl", "probes.jsonl", "front-life.jsonl", "frps-events.jsonl",
                 "link-series.jsonl", "link-stats.json", "filler-sink.json", "filler-src.json", "node-state.json",
                 "owner-state.json"] + ["reg-%d.jsonl" % g for g in range(1, self.front_gen + 1)] + list(extra)
        for nm in names:
            src = self.f(nm)
            if os.path.exists(src):
                shutil.copyfile(src, os.path.join(dst, nm))
        for nm in os.listdir(self.run):
            if nm.endswith(".jsonl") and nm.startswith("load"):
                shutil.copyfile(self.f(nm), os.path.join(dst, nm))
        # The front's own log (its pauses and failed connections) and its usage records (each
        # request's Instance, status and cutBy), for the pause analysis.
        if os.path.exists(self.f("logs/front.log")):
            shutil.copyfile(self.f("logs/front.log"), os.path.join(dst, "front.log"))
        udir = self.f("c/usage")
        if os.path.isdir(udir):
            with open(os.path.join(dst, "usage.jsonl"), "wb") as out:
                for nm in sorted(os.listdir(udir)):
                    if nm.startswith("usage-") and nm.endswith(".jsonl"):
                        with open(os.path.join(udir, nm), "rb") as fh:
                            shutil.copyfileobj(fh, out)
        manifest = dict(manifest)
        manifest.update({"run_id": self.run_id, "n": self.n, "mode": self.mode, "events": self.events,
                         "host": host_info(), "token_delay_ms": self.token_delay_ms,
                         "low_memory_kb": self.sampler.low_memory if self.sampler else None,
                         "uids": [t["uid"] for t in self.spec["tunnels"]] if getattr(self, "spec", None) else []})
        needles = ["login to server success", "heartbeat timeout", "login to the server failed", "connect to server error",
                   "start error", "start proxy success", "keepalive", "reject"]
        total = {k: 0 for k in needles}
        for t in self.spec["tunnels"] if getattr(self, "spec", None) else []:
            for k, v in count_lines(t["frpc_log"], needles).items():
                total[k] += v
        manifest["frpc_log"] = total
        frps_logs = [os.path.join(self.f("c/state"), d) for d in ("logs",)]
        frps_log = None
        for d in frps_logs:
            if os.path.isdir(d):
                for nm in os.listdir(d):
                    if nm.startswith("frps"):
                        frps_log = os.path.join(d, nm)
        manifest["frps_log"] = count_lines(frps_log, ["heartbeat timeout", "new proxy", "client login", "rejected",
                                                      "plugin", "control is closed"]) if frps_log else {}
        with open(os.path.join(dst, "manifest.json"), "w") as fh:
            json.dump(manifest, fh)
        with open(os.path.join(dst, "samples.json"), "w") as fh:
            json.dump(self.sampler.samples if self.sampler else [], fh)
        with open(os.path.join(dst, "polls.json"), "w") as fh:
            json.dump(self.sampler.polls if self.sampler else [], fh)
        return dst


def finish(stack, manifest, status, extra=()):
    stack.stop()
    manifest["status"] = status
    manifest["leftovers"] = stack.P.leftovers()
    manifest["leftover_ports"] = listeners()
    dst = stack.collect(manifest, extra)
    log("%s: %s -> %s" % (stack.run_id, status, dst))
    return dst


def run_id(item, *parts):
    return "-".join([item] + [str(p) for p in parts] + [time.strftime("%H%M%S", time.gmtime())])


# --- item 1: mass re-login ------------------------------------------------------------------------

def run_relogin(env, n, event, rep, args):
    rid = run_id("relogin", event, "n%03d" % n, "r%d" % rep)
    st = Stack(env, rid, n, token_delay_ms=args.token_delay_ms)
    holdoff = args.holdoff if event.endswith("-holdoff") else 0.0
    man = {"item": "relogin", "event_kind": event.replace("-holdoff", ""), "variant": event, "rep": rep, "streams": 1,
           "pre_s": args.pre, "warm_s": args.warm, "recover_timeout_s": args.recover, "post_s": args.post,
           "client": {"streams_per_tunnel": 1, "max_tokens": 60, "tokens_per_s": 1000.0 / args.token_delay_ms,
                      "retry": "at once after a request with tokens; 50 ms doubling to 500 ms after one without" if not holdoff
                      else "nothing for %.1f s after a failed or cut request" % holdoff, "holdoff_s": holdoff}}
    status = "ok"
    try:
        st.setup()
        st.start_core()
        st.start_node()
        st.start_frpc()
        man["listeners"] = st.check_loopback()
        st.start_sampler()
        duration = 5 + args.warm + args.pre + args.recover + args.post + 30
        st.start_load("front", 1, duration, holdoff=holdoff)
        time.sleep(5 + args.warm)
        man["pre_start"] = time.time()
        time.sleep(args.pre)
        man["pre_end"] = time.time()
        if event in ("frps", "frps-holdoff"):
            man["t_event"] = st.restart_frps()
        elif event == "front":
            man["t_event"] = st.restart_front()
        else:
            man["t_event"] = st.event("no-event")
        # Back: every proxy online again and every tunnel routed again by the (current) front, or
        # the recovery timeout. The 60 s rule is the analysis's; the wait runs on past it.
        try:
            wait_for("every tunnel back", lambda: time.time() > man["t_event"] + 1 and st.online() == n and st.registered() == n,
                     args.recover, step=0.2)
            man["t_back_seen"] = time.time()
        except RuntimeError:
            man["t_back_seen"] = None
        man["post_start"] = time.time()
        time.sleep(args.post)
        man["post_end"] = time.time()
        st.P.stop("load", 30)
    except Exception as e:
        status = "failed: " + str(e)[:300]
    return finish(st, man, status)


# --- item 2: the capped link ----------------------------------------------------------------------

def link_args(rate_bps, discipline, buffer):
    a = ["-up-rate", "%.0f" % rate_bps, "-down-rate", "%.0f" % rate_bps, "-discipline", discipline]
    if discipline == "fifo":
        a += ["-buffer-up", buffer, "-buffer-down", buffer]
    return a


def run_caplink_one(env, n, streams, mbps, discipline, buffer, rep, args):
    rate = mbps * 1e6 / 8 if mbps else 0
    rid = run_id("caplink", "n%03d" % n, "s%d" % streams, ("%gmbps" % mbps) if mbps else "uncapped",
                 discipline + (buffer if discipline == "fifo" else ""), "r%d" % rep)
    filler = args.filler_conns if mbps else 0
    st = Stack(env, rid, n, token_delay_ms=50, link_args=link_args(rate, discipline, buffer), filler_conns=filler)
    man = {"item": "caplink", "rep": rep, "streams": streams, "cap_mbps": mbps, "rate_Bps": rate, "discipline": discipline,
           "buffer": buffer if discipline == "fifo" else None, "filler_conns": filler, "warm_s": args.warm, "window_s": args.window}
    status = "ok"
    try:
        st.setup()
        st.start_core()
        st.start_node()
        st.start_frpc()
        man["listeners"] = st.check_loopback()
        st.start_sampler()
        if filler:
            st.P.start("filler-src", [env.b("s16"), "filler", "-role", "source", "-addr", "127.0.0.1:%d" % PORTS["fill_link"],
                                      "-conns", str(filler), "-out", st.f("filler-src.json")])
            st.event("filler-start")
        st.start_load("tcpmux", streams, 5 + args.warm + args.window + 5)
        time.sleep(5 + args.warm)
        man["window_start"] = time.time()
        time.sleep(args.window)
        man["window_end"] = time.time()
        st.P.wait("load", 60)
    except Exception as e:
        status = "failed: " + str(e)[:300]
    dst = finish(st, man, status)
    return dst


def run_caplink(env, args):
    """The uncapped control, then each discipline's ladder from the top cap down, until its first
    heartbeat failure or the cap where the tunnels' own demand binds (both included)."""
    ctl = run_caplink_one(env, args.tunnels, args.streams, 0, "fair", None, args.rep, args)
    out = [ctl]
    bpt = analysis.caplink(analysis.Run(ctl)).get("up_bytes_per_token")
    log("the uncapped control carried %s bytes per token up the link" % bpt)
    for disc, buf in args.disciplines:
        for mbps in args.caps:
            dst = run_caplink_one(env, args.tunnels, args.streams, mbps, disc, buf, args.rep, args)
            out.append(dst)
            verdict = analysis.caplink_stop(dst, bpt)
            log("%s: %s" % (os.path.basename(dst), verdict))
            if verdict["stop"]:
                break
    return out


# --- item 3: R_ssh --------------------------------------------------------------------------------

def run_rssh(env, cipher, rep, args):
    rid = run_id("rssh", cipher.replace("@openssh.com", "").replace("default", "dflt"), "r%d" % rep)
    st = Stack(env, rid, args.allocations, mode="host-dialed", token_delay_ms=50)
    man = {"item": "rssh", "rep": rep, "cipher_asked": cipher, "levels": [], "idle_s": args.idle, "window_s": args.window}
    status = "ok"
    try:
        st.setup()
        # The login node: internal/rehearsal's guarded sshd and run-mode fakeslurm with one held job.
        login_dir = os.path.join(st.run, "L")
        os.makedirs(login_dir, mode=0o700)
        st.P.start("login", [env.b("s16login.test"), "-dir", login_dir, "-ready", st.f("login-ready.json")],
                   env=dict(os.environ, S16_LOGIN="1"))
        wait_for("the login node", lambda: os.path.exists(st.f("login-ready.json")), 60, lambda: st.P.alive("login"))
        st.start_core()
        st.start_node()
        ssh_env = dict(os.environ, S16_SSH_LOGDIR=st.f("sshlog"))
        if cipher != "default":
            ssh_env["S16_CIPHER"] = cipher
        st.P.start("owner", [env.b("s16"), "owner", "-spec", st.spec_path, "-login", st.f("login-ready.json"),
                             "-ssh", env.b("s16ssh"), "-agent", env.b("s16node.test"), "-control-dir", st.f("ctl"),
                             "-state", st.f("owner-state.json"), "-log", st.f("logs/owner.log")], env=ssh_env)
        st.start_frpc(timeout=120)
        man["listeners"] = st.check_loopback()
        st.start_sampler()
        time.sleep(2)
        man["idle_start"] = time.time()
        time.sleep(args.idle)
        man["idle_end"] = time.time()
        for streams in args.streams:
            name = "load-s%d" % streams
            st.start_load("tcpmux", streams, 5 + args.window + 3, name=name)
            time.sleep(5)
            w0 = time.time()
            time.sleep(args.window)
            man["levels"].append({"streams": streams, "load": name, "window_start": w0, "window_end": time.time()})
            st.P.wait(name, 60)
            time.sleep(2)
        man["negotiated"] = analysis.ssh_negotiated(st.f("sshlog"))
    except Exception as e:
        status = "failed: " + str(e)[:300]
        man["negotiated"] = analysis.ssh_negotiated(st.f("sshlog"))
    return finish(st, man, status)


# --- item 4: R_front ------------------------------------------------------------------------------

def run_rfront(env, n, streams, via, rep, args):
    rid = run_id("rfront", "n%03d" % n, "s%d" % streams, via, "r%d" % rep)
    st = Stack(env, rid, n, token_delay_ms=50, link_args=link_args(0, "fair", None))
    man = {"item": "rfront", "rep": rep, "streams": streams, "via": via, "warm_s": args.warm, "window_s": args.window}
    status = "ok"
    try:
        st.setup()
        st.start_core()
        st.start_node()
        st.start_frpc()
        man["listeners"] = st.check_loopback()
        st.start_sampler()
        st.start_load(via, streams, 5 + args.warm + args.window + 3)
        time.sleep(5 + args.warm)
        man["window_start"] = time.time()
        time.sleep(args.window)
        man["window_end"] = time.time()
        st.P.wait("load", 60)
    except Exception as e:
        status = "failed: " + str(e)[:300]
    return finish(st, man, status)


# --- commands -------------------------------------------------------------------------------------

def cmd_relogin(args):
    env = Env()
    for n in args.sizes:
        for ev in args.events:
            run_relogin(env, n, ev, args.rep, args)


def cmd_caplink(args):
    env = Env()
    args.disciplines = [tuple(d.split(":")) if ":" in d else (d, None) for d in args.disciplines]
    run_caplink(env, args)


def cmd_rssh(args):
    env = Env()
    for c in args.ciphers:
        run_rssh(env, c, args.rep, args)


def cmd_rfront(args):
    env = Env()
    for spec in args.levels:
        n, s = (int(x) for x in spec.split("x"))
        for via in ("front", "tcpmux"):
            run_rfront(env, n, s, via, args.rep, args)


def cmd_leftovers(args):
    left = listeners()
    print(json.dumps({"listeners_in_block": left}))
    return 1 if left else 0


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("relogin")
    p.add_argument("--rep", type=int, default=1)
    p.add_argument("--sizes", type=int, nargs="+", default=[1, 10, 100, 300, 600])
    p.add_argument("--events", nargs="+", default=["none", "frps", "front", "frps-holdoff"])
    p.add_argument("--holdoff", type=float, default=3.0, help="the frps-holdoff control's client hold-off after a failure, s")
    p.add_argument("--token-delay-ms", type=int, default=200)
    p.add_argument("--warm", type=float, default=15)
    p.add_argument("--pre", type=float, default=20)
    p.add_argument("--recover", type=float, default=90)
    p.add_argument("--post", type=float, default=40)
    p.set_defaults(fn=cmd_relogin)
    p = sub.add_parser("caplink")
    p.add_argument("--rep", type=int, default=1)
    p.add_argument("--tunnels", type=int, default=100)
    p.add_argument("--streams", type=int, default=4)
    p.add_argument("--caps", type=float, nargs="+", default=[200, 100, 50, 25, 12.5, 6.25, 3.125])
    p.add_argument("--disciplines", nargs="+", default=["fifo:100ms", "fifo:1s", "fair"])
    p.add_argument("--filler-conns", type=int, default=4)
    p.add_argument("--warm", type=float, default=10)
    p.add_argument("--window", type=float, default=30)
    p.set_defaults(fn=cmd_caplink)
    p = sub.add_parser("rssh")
    p.add_argument("--rep", type=int, default=1)
    p.add_argument("--ciphers", nargs="+", default=["default", "aes128-gcm@openssh.com", "aes256-gcm@openssh.com"])
    p.add_argument("--allocations", type=int, default=4)
    p.add_argument("--streams", type=int, nargs="+", default=[1, 4, 16, 32, 64])
    p.add_argument("--idle", type=float, default=30)
    p.add_argument("--window", type=float, default=30)
    p.set_defaults(fn=cmd_rssh)
    p = sub.add_parser("rfront")
    p.add_argument("--rep", type=int, default=1)
    p.add_argument("--levels", nargs="+", default=["10x1", "10x8", "50x1", "50x4", "100x1", "100x4"])
    p.add_argument("--warm", type=float, default=10)
    p.add_argument("--window", type=float, default=30)
    p.set_defaults(fn=cmd_rfront)
    p = sub.add_parser("host")
    p.set_defaults(fn=lambda a: print(json.dumps(host_info())))
    p = sub.add_parser("leftovers")
    p.set_defaults(fn=cmd_leftovers)
    args = ap.parse_args()
    sys.exit(args.fn(args) or 0)


if __name__ == "__main__":
    main()
