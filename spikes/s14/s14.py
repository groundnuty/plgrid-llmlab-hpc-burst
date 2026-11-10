#!/usr/bin/env python3
"""Spike S14 driver (issue #313, HBEP-6 open question 11): many frpc tunnels on one front.

    python3 s14.py run --rep 1 [--modes ...] [--tunnels ...] [--streams ...]
    python3 s14.py host            the host's CPU model, count and memory, as JSON
    python3 s14.py leftovers       recorded processes still alive, listeners left in 29920-29939

Environment: S14_BIN (frps, frpc, toxiproxy-server, s14, s14bridge, s14node.test), S14_WORK (a
scratch directory), S14_RESULTS (where runs.csv and raw/ go). Every process is started by this
driver, recorded by PID and start time, and stopped by that PID; nothing is signalled by name.
"""
import argparse
import base64
import csv
import json
import os
import resource
import secrets
import signal
import subprocess
import sys
import threading
import time
import urllib.request
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import analysis  # noqa: E402

# Fixed ports, all on 127.0.0.1, in issue #313's block 29920-29939. Everything else is kernel-chosen.
PORTS = dict(frps=29920, mux=29921, web=29922, dialhome=29923, tox_api=29924, link=29925, relay=29926,
             control=29927)
PORT_RANGE = (29920, 29939)
CLK = os.sysconf("SC_CLK_TCK")
SERVER_NAME = "dial-home.hpcburst.invalid"
# HBEP-6's heartbeat set (Heartbeats; S1's), on both ends.
FRPS_HB = {"transport.tcpMuxKeepaliveInterval": 10, "transport.heartbeatTimeout": 25, "transport.tcpKeepalive": 30}
FRPC_HB = {"transport.tcpMuxKeepaliveInterval": 10, "transport.heartbeatInterval": 10,
           "transport.heartbeatTimeout": 25, "transport.dialServerKeepalive": 30}
SAMPLE_S = 2.0          # the /proc sampler's period
API_POLL_S = 10.0       # frps's proxy list, read as often as the lifecycle controller reads it (HBEP-6)
MEM_FLOOR_KB = 700_000  # a run stops if the host's available memory falls below this
FRONT_ROLES = ("frps", "listener", "owner", "bridges")
ALL_ROLES = FRONT_ROLES + ("frpc", "node", "client", "toxiproxy", "driver")


def log(msg):
    print(time.strftime("%H:%M:%S", time.gmtime()) + "Z " + msg, flush=True)


class Env:
    def __init__(self):
        self.bin = os.environ["S14_BIN"]
        self.work = os.environ["S14_WORK"]
        self.results = os.environ["S14_RESULTS"]
        self.nice_node = int(os.environ.get("S14_NICE_NODE", "10"))
        for d in (self.work, self.results, os.path.join(self.results, "raw")):
            os.makedirs(d, exist_ok=True)

    def b(self, name):
        return os.path.join(self.bin, name)


# --- processes ------------------------------------------------------------------------------------

def start_time(pid):
    try:
        with open("/proc/%d/stat" % pid) as fh:
            return fh.read().rsplit(")", 1)[1].split()[19]
    except OSError:
        return None


class Procs:
    """The run's processes, recorded by PID and start time (pids.json), stopped by that PID."""

    def __init__(self, run_dir):
        self.run_dir, self.procs, self.order = run_dir, {}, []
        self.record = os.path.join(run_dir, "pids.json")

    def start(self, name, argv, nice=0, env=None, logname=None):
        logf = open(os.path.join(self.run_dir, "logs", (logname or name) + ".out"), "ab")
        pre = (lambda: os.nice(nice)) if nice else None
        p = subprocess.Popen(argv, stdout=logf, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                             start_new_session=True, env=env, preexec_fn=pre)
        logf.close()
        self.procs[name] = (p, p.pid, start_time(p.pid))
        self.order.append(name)
        with open(self.record + ".tmp", "w") as fh:
            json.dump({k: v[1:] for k, v in self.procs.items()}, fh)
        os.replace(self.record + ".tmp", self.record)
        return p.pid

    def pid(self, name):
        return self.procs[name][1]

    def alive(self, name):
        p, pid, st = self.procs[name]
        return p.poll() is None and start_time(pid) == st

    def stop(self, name, timeout=15.0):
        if name not in self.procs:
            return None
        p, pid, st = self.procs[name]
        if p.poll() is None and start_time(pid) == st:
            os.kill(pid, signal.SIGTERM)
            try:
                p.wait(timeout)
            except subprocess.TimeoutExpired:
                if start_time(pid) == st:
                    os.kill(pid, signal.SIGKILL)
                p.wait(5)
        return p.returncode

    def stop_all(self, names):
        """Stops the named processes; frpc and other groups in parallel (TERM to all, then wait)."""
        live = [n for n in names if n in self.procs and self.alive(n)]
        for n in live:
            p, pid, st = self.procs[n]
            if start_time(pid) == st:
                os.kill(pid, signal.SIGTERM)
        deadline = time.time() + 15
        for n in live:
            p, pid, st = self.procs[n]
            try:
                p.wait(max(0.1, deadline - time.time()))
            except subprocess.TimeoutExpired:
                if start_time(pid) == st:
                    os.kill(pid, signal.SIGKILL)
                p.wait(5)

    def leftovers(self):
        return [n for n in self.procs if self.alive(n)]


# --- /proc ----------------------------------------------------------------------------------------

def proc_ticks(pid):
    try:
        with open("/proc/%d/stat" % pid) as fh:
            f = fh.read().rsplit(")", 1)[1].split()
        return int(f[11]) + int(f[12])
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


def listeners_in_range():
    """Listening TCP sockets in the port block, as (address, port)."""
    out = subprocess.run(["ss", "-ltnH"], capture_output=True, text=True, check=True).stdout
    res = []
    for line in out.splitlines():
        f = line.split()
        if len(f) < 4:
            continue
        addr, _, port = f[3].rpartition(":")
        if port.isdigit() and PORT_RANGE[0] <= int(port) <= PORT_RANGE[1]:
            res.append((addr, int(port)))
    return res


def listeners_of(pids):
    """Every listening TCP socket held by one of pids, as (address, port, pid)."""
    out = subprocess.run(["ss", "-ltnpH"], capture_output=True, text=True, check=True).stdout
    res, want = [], set(pids)
    for line in out.splitlines():
        f = line.split()
        if len(f) < 6:
            continue
        addr, _, port = f[3].rpartition(":")
        for part in line.split("pid=")[1:]:
            pid = int(part.split(",")[0])
            if pid in want:
                res.append((addr, int(port), pid))
    return res


class Sampler(threading.Thread):
    """Every SAMPLE_S: per role, each PID's CPU ticks and the role's summed RSS and descriptors;
    the host's /proc/stat and available memory. Every API_POLL_S: frps's proxy list."""

    def __init__(self, roles_fn, api_fn):
        super().__init__(daemon=True)
        self.roles_fn, self.api_fn = roles_fn, api_fn
        self.samples, self.polls, self.stop_ev = [], [], threading.Event()
        self.low_memory = None

    def sample(self):
        s = {"t": time.time(), "ticks": {}, "rss_kb": {}, "hwm_kb": {}, "fds": {}, "stat": stat_cpu(),
             "mem_avail_kb": mem_available_kb(), "load": loadavg()}
        for role, pids in self.roles_fn().items():
            ticks, rss, hwm, fds = {}, 0, 0, 0
            for pid in pids:
                t = proc_ticks(pid)
                if t is None:
                    continue
                ticks[pid] = t
                r, h = proc_mem(pid)
                rss += r or 0
                hwm = max(hwm, h or 0)
                fds += proc_fds(pid) or 0
            s["ticks"][role], s["rss_kb"][role], s["hwm_kb"][role], s["fds"][role] = ticks, rss, hwm, fds
        self.samples.append(s)
        if s["mem_avail_kb"] is not None and s["mem_avail_kb"] < MEM_FLOOR_KB and self.low_memory is None:
            self.low_memory = s["mem_avail_kb"]

    def run(self):
        next_poll = 0.0
        while not self.stop_ev.is_set():
            self.sample()
            if self.api_fn and time.time() >= next_poll:
                next_poll = time.time() + API_POLL_S
                t = time.time()
                try:
                    self.polls.append({"t": t, "proxies": self.api_fn()})
                except Exception as e:  # an unreadable list is counted, never read as online
                    self.polls.append({"t": t, "error": str(e)[:200]})
            self.stop_ev.wait(SAMPLE_S)

    def stop(self):
        self.stop_ev.set()
        self.join(10)
        self.sample()


# --- configuration files --------------------------------------------------------------------------

def toml_kv(d):
    return "".join("%s = %s\n" % (k, json.dumps(v)) for k, v in d.items())


def write_private(path, body):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.write(fd, body.encode())
    os.close(fd)


def render_frps(run, pw):
    """HBEP-6's frps.toml (frps on the control-plane host) with the spike's ports and without the
    admission plugin, which acts only at logins and new proxies."""
    body = toml_kv({
        "bindAddr": "127.0.0.1", "bindPort": PORTS["frps"], "proxyBindAddr": "127.0.0.1",
        "tcpmuxHTTPConnectPort": PORTS["mux"],
        "webServer.addr": "127.0.0.1", "webServer.port": PORTS["web"],
        "webServer.user": "hpcburst", "webServer.password": pw,
        "log.to": os.path.join(run, "logs", "frps.log"), "log.level": "info", "log.disablePrintColor": True,
    }) + toml_kv(FRPS_HB)
    path = os.path.join(run, "frps.toml")
    write_private(path, body)
    return path


def render_frpc(run, t, server_port, cert):
    """HBEP-6's frpc.toml (frpc in the job): wss to the dial-home listener, verified against the
    launcher's certificate by the synthetic server name, one tcpmux proxy named by the UID, served
    from the engine's Unix socket."""
    body = toml_kv({
        "serverAddr": "127.0.0.1", "serverPort": server_port, "loginFailExit": False,
        "log.to": os.path.join(run, "frpc", "%03d.log" % t["index"]), "log.level": "info",
        "log.disablePrintColor": True,
        "metadatas.token": secrets.token_hex(32),
        "transport.protocol": "wss", "transport.tls.trustedCaFile": cert,
        "transport.tls.serverName": SERVER_NAME, "transport.poolCount": 0,
    }) + toml_kv(FRPC_HB)
    body += ('\n[[proxies]]\nname = "%s"\ntype = "tcpmux"\nmultiplexer = "httpconnect"\ncustomDomains = ["%s"]\n'
             '[proxies.plugin]\ntype = "unix_domain_socket"\nunixPath = "%s"\n') % (t["uid"], t["uid"], t["socket"])
    path = os.path.join(run, "frpc", "%03d.toml" % t["index"])
    write_private(path, body)
    return path


def http_json(method, url, body=None, auth=None, timeout=5):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    if auth:
        req.add_header("Authorization", "Basic " + base64.b64encode(("%s:%s" % auth).encode()).decode())
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    return json.loads(raw) if raw.strip().startswith((b"{", b"[")) else raw.decode()


def wait_port(port, timeout=30.0):
    import socket
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(0.5)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.05)
    raise RuntimeError("nothing listens on 127.0.0.1:%d after %.0f s" % (port, timeout))


def wait_file(path, timeout=60.0, alive=None):
    end = time.time() + timeout
    while time.time() < end:
        if os.path.exists(path):
            return
        if alive and not alive():
            raise RuntimeError("the process exited before writing %s" % path)
        time.sleep(0.05)
    raise RuntimeError("%s was not written within %.0f s" % (path, timeout))


def read_json(path):
    try:
        with open(path) as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


# --- one configuration ----------------------------------------------------------------------------

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
    return {"cpu_model": model, "cpus": n, "nproc": os.cpu_count(), "mem_total_mb": total // 1024,
            "kernel": os.uname().release}


def run_config(env, mode, n, streams, rep, args):
    run_id = "%s-n%03d-s%d-r%d-%s" % (mode, n, streams, rep, time.strftime("%H%M%S", time.gmtime()))
    run = os.path.join(env.work, run_id)
    for d in ("logs", "frpc", "alloc", "e", "tmp"):
        os.makedirs(os.path.join(run, d), mode=0o700)
    left = listeners_in_range()
    if left:
        raise RuntimeError("ports in 29920-29939 are taken before the run: %s" % left)
    tunnels = []
    for i in range(n):
        tunnels.append({"index": i, "uid": str(uuid.uuid4()), "tag": "s14-%03d" % i, "job": str(1000 + i),
                        "dir": os.path.join(run, "alloc", "%03d" % i), "socket": os.path.join(run, "e", "%03d.sock" % i)})
    spec = os.path.join(run, "spec.json")
    with open(spec, "w") as fh:
        json.dump({"mode": mode, "tunnels": tunnels}, fh)
    P = Procs(run)
    pw = secrets.token_hex(16)
    row = {"run_id": run_id, "mode": mode, "tunnels": n, "streams": streams, "rep": rep, "status": "ok"}
    row.update(host_info())
    frpc_pids = []
    sampler = None
    t_ready = None
    try:
        # The link: toxiproxy 2.13.0 (S1c's emulator), one proxy, no toxic, on the hop between node
        # and front host: frpc -> listener (direct), or the ssh stand-in -> the node (host-dialed).
        if mode != "baseline":
            subprocess.run([env.b("s14"), "certs", "-dir", run], check=True)
            P.start("toxiproxy", [env.b("toxiproxy-server"), "-host", "127.0.0.1", "-port", str(PORTS["tox_api"])])
            wait_port(PORTS["tox_api"])
            upstream = PORTS["dialhome"] if mode == "direct" else PORTS["relay"]
            http_json("POST", "http://127.0.0.1:%d/proxies" % PORTS["tox_api"],
                      {"name": "link", "listen": "127.0.0.1:%d" % PORTS["link"],
                       "upstream": "127.0.0.1:%d" % upstream, "enabled": True})
            P.start("frps", [env.b("frps"), "-c", render_frps(run, pw)])
            wait_port(PORTS["mux"])
            wait_port(PORTS["web"])
            P.start("listener", [env.b("s14"), "listener", "-listen", "127.0.0.1:%d" % PORTS["dialhome"],
                                 "-frps", "127.0.0.1:%d" % PORTS["frps"], "-cert", os.path.join(run, "cert.pem"),
                                 "-key", os.path.join(run, "key.pem"), "-log", os.path.join(run, "logs", "listener.log")])
            wait_port(PORTS["dialhome"])
        node_env = dict(os.environ, S14_NODE="1")
        P.start("node", [env.b("s14node.test"), "-spec", spec, "-relay-listen", "127.0.0.1:%d" % PORTS["relay"],
                         "-tmp", os.path.join(run, "tmp"), "-ready", os.path.join(run, "node-ready.json"),
                         "-state", os.path.join(run, "node-state.json"), "-log", os.path.join(run, "logs", "node.log")],
                nice=env.nice_node, env=node_env)
        wait_file(os.path.join(run, "node-ready.json"), alive=lambda: P.alive("node"))
        ready = read_json(os.path.join(run, "node-ready.json"))
        if mode == "host-dialed":
            P.start("owner", [env.b("s14"), "owner", "-spec", spec, "-bridge", env.b("s14bridge"),
                              "-link", "127.0.0.1:%d" % PORTS["link"], "-control-port", str(PORTS["control"]),
                              "-serving-port", str(PORTS["dialhome"]), "-state", os.path.join(run, "owner-state.json"),
                              "-log", os.path.join(run, "logs", "owner.log")])
        api = None
        if mode != "baseline":
            def api():
                r = http_json("GET", "http://127.0.0.1:%d/api/proxy/tcpmux" % PORTS["web"], auth=("hpcburst", pw))
                return {p["name"]: {"status": p.get("status"), "lastStartTime": p.get("lastStartTime"),
                                    "curConns": p.get("curConns")} for p in (r.get("proxies") or [])}
            t_frpc = time.time()
            for t in tunnels:
                port = PORTS["link"] if mode == "direct" else ready["serving_ports"][t["index"]]
                name = "frpc-%03d" % t["index"]
                frpc_pids.append(P.start(name, [env.b("frpc"), "-c", render_frpc(run, t, port, os.path.join(run, "cert.pem"))],
                                         nice=env.nice_node, env={"PATH": os.environ.get("PATH", "/usr/bin:/bin")},
                                         logname="frpc"))
            uids = [t["uid"] for t in tunnels]
            end = time.time() + args.ready_timeout
            online = 0
            while time.time() < end:
                try:
                    st = api()
                    online = sum(1 for u in uids if (st.get(u) or {}).get("status") == "online")
                    if online == n:
                        break
                except Exception:
                    pass
                time.sleep(0.25)
            row["registered"] = online
            if online != n:
                raise RuntimeError("%d of %d proxies online after %.0f s" % (online, n, args.ready_timeout))
            t_ready = time.time()
            row["all_online_s"] = round(t_ready - t_frpc, 2)
        else:
            t_ready = time.time()

        def roles():
            r = {"node": [P.pid("node")], "driver": [os.getpid()], "frpc": frpc_pids}
            for name in ("frps", "listener", "owner", "toxiproxy", "client"):
                if name in P.procs:
                    r[name] = [P.pid(name)]
            if "owner" in P.procs:
                st = read_json(os.path.join(run, "owner-state.json")) or []
                r["bridges"] = [x["PID"] for x in st if x.get("PID")]
            return r

        # Every listener this run's processes hold must be on 127.0.0.1 (kernel-chosen ones too).
        held = listeners_of([P.pid(k) for k in P.procs])
        off = [h for h in held if h[0] != "127.0.0.1"]
        row["listeners"] = len(held)
        if off:
            raise RuntimeError("listeners off loopback: %s" % off)
        sampler = Sampler(roles, api)
        sampler.start()
        time.sleep(SAMPLE_S)  # an idle sample after readiness, before the load
        row["ready_s"] = round(t_ready, 3)
        # The idle cost: every tunnel registered, no stream, only the front's probe every 10 s, the
        # controller's list read, and the tunnels' own keepalives and heartbeats. Measured once per
        # tunnel count (at 1 stream), since streams change nothing while none runs.
        if args.idle > 0 and streams == 1 and mode != "baseline":
            idle_out = os.path.join(run, "idle.json")
            run_client(P, "idle-client", [env.b("s14"), "load", "-spec", spec, "-mux", "127.0.0.1:%d" % PORTS["mux"],
                                          "-streams", "0", "-ramp", "0s", "-window", "%ds" % args.idle, "-out", idle_out],
                       args.idle + 60, sampler)
        load_out = os.path.join(run, "load.json")
        run_client(P, "client", [env.b("s14"), "load", "-spec", spec, "-mux", "127.0.0.1:%d" % PORTS["mux"],
                                 "-streams", str(streams), "-ramp", "%ds" % args.ramp, "-window", "%ds" % args.window,
                                 "-max-tokens", str(args.max_tokens), "-out", load_out],
                   args.ramp + args.window + 240, sampler)
        time.sleep(SAMPLE_S)
        sampler.stop()
        load = read_json(load_out)
        summarize_run(row, load, sampler, tunnels, t_ready, run, mode)
        # Summarised once the sampler has stopped, so its window's every sample is in.
        idle = read_json(os.path.join(run, "idle.json"))
        if idle:
            summarize_idle(row, idle, sampler)
    except Exception as e:
        row["status"] = "failed: " + str(e)[:300]
        log("%s: %s" % (run_id, row["status"]))
        if sampler and sampler.is_alive():
            sampler.stop()
    finally:
        P.stop("client")
        P.stop("owner")  # stops its relays by PID first
        P.stop_all([k for k in P.procs if k.startswith("frpc-")])
        for name in ("node", "listener", "frps", "toxiproxy"):
            P.stop(name)
        row["leftovers"] = len(P.leftovers())
        row["leftover_ports"] = len(listeners_in_range())
    # Counted after every process stopped: the owner and the node write their last state on exit.
    count_drops(row, run, tunnels, mode, sampler, t_ready)
    samples = sampler.samples if sampler else []
    raw = {"row": row, "load": read_json(os.path.join(run, "load.json")), "idle": read_json(os.path.join(run, "idle.json")),
           "owner": read_json(os.path.join(run, "owner-state.json")), "node": read_json(os.path.join(run, "node-state.json")),
           "polls": sampler.polls if sampler else [], "uids": [t["uid"] for t in tunnels], "samples": samples}
    with open(os.path.join(env.results, "raw", run_id + ".json"), "w") as fh:
        json.dump(raw, fh)
    write_samples(env.results, run_id, samples)
    return row


def run_client(P, name, argv, timeout, sampler):
    """Starts the load client, waits for it, and fails the run if it fails or the host runs short of memory."""
    P.start(name, argv)
    p = P.procs[name][0]
    deadline = time.time() + timeout
    while p.poll() is None:
        if sampler.low_memory is not None:
            raise RuntimeError("the host's available memory fell to %d MB" % (sampler.low_memory // 1024))
        if time.time() > deadline:
            raise RuntimeError("the %s did not finish" % name)
        time.sleep(0.5)
    if p.returncode != 0:
        raise RuntimeError("the %s exited %d" % (name, p.returncode))


SAMPLE_FIELDS = ["run_id", "t", "role", "pids", "ticks", "rss_kb", "hwm_kb", "fds"]
HOST_FIELDS = ["run_id", "t", "user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal", "mem_avail_kb",
               "load1", "load5"]


def write_samples(results, run_id, samples):
    """Appends the run's /proc samples to samples.csv (per role: PIDs seen, summed ticks, RSS, peak RSS,
    descriptors) and host.csv (the host's /proc/stat counters, available memory, load), so the committed
    results keep what every CPU, memory and descriptor figure was computed from."""
    sp, hp = os.path.join(results, "samples.csv"), os.path.join(results, "host.csv")
    new_s, new_h = not os.path.exists(sp), not os.path.exists(hp)
    with open(sp, "a", newline="") as fs, open(hp, "a", newline="") as fh:
        ws, wh = csv.writer(fs), csv.writer(fh)
        if new_s:
            ws.writerow(SAMPLE_FIELDS)
        if new_h:
            wh.writerow(HOST_FIELDS)
        for s in samples:
            t = "%.6f" % s["t"]
            for role in sorted(s["ticks"]):
                ticks = s["ticks"][role]
                ws.writerow([run_id, t, role, len(ticks), sum(ticks.values()), s["rss_kb"].get(role, ""),
                             s["hwm_kb"].get(role, ""), s["fds"].get(role, "")])
            st = (s["stat"] + [0] * 8)[:8]
            wh.writerow([run_id, t] + st + [s["mem_avail_kb"], s["load"][0], s["load"][1]])


def summarize_idle(row, idle, sampler):
    """The idle phase's CPU per role, from the samples inside its window, and its probes. The window
    ends at the client's own end when that came first: the next phase starts then, and its samples
    are not idle. (The client now waits out its window, so this binds only for runs made before.)"""
    ws = idle["window_start_unix_ns"] / 1e9
    we = min(idle["window_end_unix_ns"], idle["end_unix_ns"]) / 1e9
    win = analysis.window_samples(sampler.samples, ws, we)
    row["idle_start_s"], row["idle_end_s"], row["idle_samples"] = round(ws, 6), round(we, 6), len(win)
    for role in ALL_ROLES:
        c = analysis.cores(win, role, CLK)
        row["idle_cpu_" + role] = round(c, 5) if c is not None else ""
    row["idle_probes_ok"], row["idle_probes_failed"] = idle["probes_ok"], idle["probes_failed"]
    row["idle_probes_min_per_tunnel"] = min(t["probes_ok"] for t in idle["per_tunnel"])
    row["idle_tokens"] = sum(t["tokens"] for t in idle["per_tunnel"])


def summarize_run(row, load, sampler, tunnels, t_ready, run, mode):
    ws, we = load["window_start_unix_ns"] / 1e9, load["window_end_unix_ns"] / 1e9
    win = analysis.window_samples(sampler.samples, ws, we)
    row["window_start_s"], row["window_end_s"], row["window_samples"] = round(ws, 6), round(we, 6), len(win)
    for role in ALL_ROLES:
        c = analysis.cores(win, role, CLK)
        row["cpu_" + role] = round(c, 4) if c is not None else ""
        r = analysis.peak(win, role, "rss_kb")
        row["rss_mb_" + role] = round(r / 1024, 1) if r is not None else ""
        f = analysis.peak(win, role, "fds")
        row["fds_" + role] = f if f is not None else ""
        hw = analysis.peak(win, role, "hwm_kb")
        row["hwm_mb_" + role] = round(hw / 1024, 1) if hw is not None else ""
    # The idle cost: the sample just before the load, after readiness.
    idle = [s for s in sampler.samples if t_ready <= s["t"] < load["t0_unix_ns"] / 1e9]
    if idle:
        for role in FRONT_ROLES:
            r = idle[-1]["rss_kb"].get(role)
            row["idle_rss_mb_" + role] = round(r / 1024, 1) if r is not None else ""
            row["idle_fds_" + role] = idle[-1]["fds"].get(role, "")
    busy = analysis.busy_cores(win[0]["stat"], win[-1]["stat"]) if len(win) >= 2 else None
    row["host_busy"] = round(busy, 3) if busy is not None else ""
    row["host_bound"] = analysis.host_bound(busy)
    row["load1_end"] = win[-1]["load"][0] if win else ""
    row["mem_avail_min_mb"] = min(s["mem_avail_kb"] for s in win) // 1024 if win else ""
    v = analysis.tunnel_validity(load["per_tunnel"], len(tunnels))
    row["valid"] = v["valid"]
    row["invalid_reason"] = v["reason"]
    row["tunnels_silent"], row["tunnels_gapped"] = v["silent"], v["gapped"]
    row["tunnel_tokens_min"], row["tunnel_tokens_max"] = v["tokens_min"], v["tokens_max"]
    row["tunnel_requests_min"], row["tunnel_requests_max"] = v["requests_min"], v["requests_max"]
    row["longest_silence_ms_max"] = max(t["longest_silence_ms"] for t in load["per_tunnel"])
    for k in ("requests", "completed", "failed", "all_requests", "all_failed", "tokens_per_s", "probes_ok", "probes_failed"):
        row[k] = load[k]
    for k in ("p50", "p99", "max"):
        row["ttft_%s_ms" % k] = load["ttft_ms"][k]
        row["gap_%s_ms" % k] = load["gap_ms"][k]
    row["probe_p99_ms"] = load["probe_ms"]["p99"]
    row["errors"] = json.dumps(load["errors"])[:300]
    row["window_s"] = load["window_s"]


def count_drops(row, run, tunnels, mode, sampler, t_ready):
    """The heartbeat-loss count: tunnels whose session dropped during the run, by any of four
    independent witnesses."""
    if mode == "baseline":
        for k in ("drops_api", "api_unreadable", "frpc_relogins", "relay_restarts", "job_resessions",
                  "hb_timeout_lines"):
            row[k] = ""
        return
    uids = [t["uid"] for t in tunnels]
    dropped, unreadable = analysis.drops(sampler.polls if sampler else [], uids, t_ready or time.time())
    row["drops_api"], row["api_unreadable"] = len(dropped), unreadable
    relogins, hb = 0, 0
    for t in tunnels:
        try:
            with open(os.path.join(run, "frpc", "%03d.log" % t["index"]), errors="replace") as fh:
                text = fh.read()
        except OSError:
            text = ""
        relogins += max(0, analysis.logins(text) - 1)
        hb += text.count("heartbeat timeout")
    try:
        with open(os.path.join(run, "logs", "frps.log"), errors="replace") as fh:
            hb += fh.read().count("heartbeat timeout")
    except OSError:
        pass
    row["frpc_relogins"], row["hb_timeout_lines"] = relogins, hb
    if mode == "host-dialed":
        st = read_json(os.path.join(run, "owner-state.json")) or []
        row["relay_restarts"] = sum(max(0, x.get("Sessions", 0) - 1) for x in st)
        row["relays_up"] = sum(1 for x in st if x.get("Sessions", 0) >= 1)
        ns = read_json(os.path.join(run, "node-state.json")) or {}
        row["job_resessions"] = sum(max(0, j["serving_sessions"] - 1) + max(0, j["control_sessions"] - 1)
                                    for j in ns.get("jobs", []))
    else:
        row["relay_restarts"] = row["job_resessions"] = ""


# --- commands -------------------------------------------------------------------------------------

def append_row(path, row):
    new = not os.path.exists(path)
    if not new:
        with open(path) as fh:
            header = next(csv.reader(fh))
        for k in row:
            if k not in header:
                header.append(k)
    else:
        header = list(row)
    rows = []
    if not new:
        with open(path) as fh:
            rows = list(csv.DictReader(fh))
    rows.append(row)
    with open(path + ".tmp", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    os.replace(path + ".tmp", path)


def cmd_run(args):
    env = Env()
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    log("host: %s; open files %d -> %d" % (json.dumps(host_info()), soft, hard))
    for n in args.tunnels:
        for s in args.streams:
            for mode in args.modes:
                la = loadavg()[0]
                if args.max_load and la > args.max_load:
                    log("skip %s n=%d s=%d: 1-min load %.2f above %.2f" % (mode, n, s, la, args.max_load))
                    continue
                log("run %s n=%d s=%d rep=%d" % (mode, n, s, args.rep))
                row = run_config(env, mode, n, s, args.rep, args)
                append_row(os.path.join(env.results, "runs.csv"), row)
                log("  %s valid=%s tok/s=%s ttft p50/p99=%s/%s gap p99=%s frps=%s cores busy=%s" % (
                    row["status"], row.get("valid"), row.get("tokens_per_s"), row.get("ttft_p50_ms"),
                    row.get("ttft_p99_ms"), row.get("gap_p99_ms"), row.get("cpu_frps"), row.get("host_busy")))


def cmd_leftovers(args):
    env = Env()
    alive = []
    for d in sorted(os.listdir(env.work)):
        rec = read_json(os.path.join(env.work, d, "pids.json")) or {}
        for name, (pid, st) in rec.items():
            if st is not None and start_time(pid) == st:
                alive.append((d, name, pid))
    print("recorded processes still alive: %s" % (alive or "none"))
    print("listeners in 29920-29939: %s" % (listeners_in_range() or "none"))
    return 1 if alive else 0


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--rep", type=int, required=True)
    r.add_argument("--modes", default="baseline,direct,host-dialed", type=lambda s: s.split(","))
    r.add_argument("--tunnels", default="1,10,50,100,300", type=lambda s: [int(x) for x in s.split(",")])
    r.add_argument("--streams", default="1,8", type=lambda s: [int(x) for x in s.split(",")])
    r.add_argument("--ramp", type=int, default=5)
    r.add_argument("--window", type=int, default=30)
    r.add_argument("--max-tokens", type=int, default=60)
    r.add_argument("--idle", type=int, default=30, help="seconds of the idle phase at 1 stream per tunnel (0: none)")
    r.add_argument("--ready-timeout", type=float, default=180.0)
    r.add_argument("--max-load", type=float, default=0.0, help="skip a configuration while the 1-min load is above this (0: never)")
    sub.add_parser("host")
    sub.add_parser("leftovers")
    args = ap.parse_args()
    if args.cmd == "run":
        cmd_run(args)
    elif args.cmd == "host":
        print(json.dumps(host_info()))
    else:
        sys.exit(cmd_leftovers(args))


if __name__ == "__main__":
    main()
