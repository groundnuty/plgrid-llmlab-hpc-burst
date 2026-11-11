#!/usr/bin/env python3
"""Spike S17 driver (issue #413, HBEP-6 open question 12): bulk transfers beside token streams in one
Instance's tunnel, in layouts (a) one proxy, (b) a second proxy with frp's bandwidthLimit, and (c) a
separate frpc with tcpMux off (and so a second frps), in direct and host-dialed mode.

    python3 s17.py run --rep 1 [--modes ...] [--layouts ...] [--kinds ...]
    python3 s17.py host            the host's CPU model, count and memory, as JSON
    python3 s17.py leftovers       recorded processes still alive, listeners left in 29980-29999

Environment: S17_BIN (frps, frpc, s17, s17link, s17bridge, s17node.test), S17_WORK (a scratch
directory), S17_RESULTS (runs.csv, samples.csv, host.csv, raw/). Every process is started by this
driver, recorded by PID and start time, and stopped by that PID; nothing is signalled by name. The
process records, the /proc sampler and the sample files are spike S14's (spikes/s14/s14.py at
3f863bad), copied below.
"""
import argparse
import base64
import csv
import json
import os
import resource
import secrets
import signal
import statistics
import subprocess
import sys
import threading
import time
import urllib.request
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import analysis  # noqa: E402

# Ports: everything this driver starts binds a kernel-chosen port on 127.0.0.1, which it reports in a file (the
# link, the dial-home listener, the node's relay acceptor) or, for frps, whose configuration needs it before it
# starts, takes from the kernel by a bind and close right before the start (free_ports), retried with fresh
# ports if frps does not come up listening (start_frps; the gap issue #384 describes, tolerable in a spike).
CLK = os.sysconf("SC_CLK_TCK")
SERVER_NAME = "dial-home.hpcburst.invalid"
BULK_SERVER_NAME = "bulk.dial-home.hpcburst.invalid"
# HBEP-6's heartbeat set (Heartbeats; S1's), on both ends. The second frps runs with tcpMux off, where
# frp's heartbeat timeout would default to 90 s (pkg/config/v1/server.go:183-191), so it is set.
FRPS_HB = {"transport.tcpMuxKeepaliveInterval": 10, "transport.heartbeatTimeout": 25, "transport.tcpKeepalive": 30}
FRPS2_HB = {"transport.tcpMux": False, "transport.heartbeatTimeout": 25, "transport.tcpKeepalive": 30}
FRPC_HB = {"transport.tcpMuxKeepaliveInterval": 10, "transport.heartbeatInterval": 10,
           "transport.heartbeatTimeout": 25, "transport.dialServerKeepalive": 30}
SAMPLE_S = 0.5          # the /proc sampler's period: transfers last seconds
API_POLL_S = 10.0       # frps's proxy lists, read as often as the lifecycle controller reads them (HBEP-6)
MEM_FLOOR_KB = 700_000
FRONT_ROLES = ("frps", "frps2", "listener", "owner", "bridges")
ALL_ROLES = FRONT_ROLES + ("frpc", "frpc-bulk", "node", "client", "link", "driver")
MIB = 1 << 20


def log(msg):
    print(time.strftime("%H:%M:%S", time.gmtime()) + "Z " + msg, flush=True)


class Env:
    def __init__(self):
        self.bin = os.environ["S17_BIN"]
        self.work = os.environ["S17_WORK"]
        self.results = os.environ["S17_RESULTS"]
        self.nice_node = int(os.environ.get("S17_NICE_NODE", "10"))
        self.tmp = os.environ.get("S17_TMP", os.environ.get("TMPDIR", "/tmp"))
        for d in (self.work, self.results, os.path.join(self.results, "raw")):
            os.makedirs(d, exist_ok=True)

    def b(self, name):
        return os.path.join(self.bin, name)


# ---- copied from spikes/s14/s14.py at 3f863bad ----
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


def free_ports(n):
    """n distinct ports the kernel just chose on 127.0.0.1 (all bound at once, then released)."""
    import socket
    socks, ports = [], []
    for _ in range(n):
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        ports.append(s.getsockname()[1])
        socks.append(s)
    for s in socks:
        s.close()
    return ports


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



def toml_kv(d):
    return "".join("%s = %s\n" % (k, json.dumps(v)) for k, v in d.items())


def write_private(path, body):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.write(fd, body.encode())
    os.close(fd)



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


def wait_port(port, timeout=30.0, alive=None):
    import socket
    end = time.time() + timeout
    while time.time() < end:
        if alive and not alive():
            raise RuntimeError("the process exited before listening on 127.0.0.1:%d" % port)
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




# --- S17: configuration files ---------------------------------------------------------------------

def render_frps(run, name, pw, bind, mux, web, hb):
    """HBEP-6's frps.toml with the spike's ports, without the admission plugin (it acts only at logins
    and new proxies)."""
    body = toml_kv({
        "bindAddr": "127.0.0.1", "bindPort": bind, "proxyBindAddr": "127.0.0.1", "tcpmuxHTTPConnectPort": mux,
        "webServer.addr": "127.0.0.1", "webServer.port": web, "webServer.user": "hpcburst", "webServer.password": pw,
        "log.to": os.path.join(run, "logs", name + ".log"), "log.level": "info", "log.disablePrintColor": True,
    }) + toml_kv(hb)
    path = os.path.join(run, name + ".toml")
    write_private(path, body)
    return path


def render_frpc(run, name, server_port, cert, server_name, proxies, tcpmux=True):
    """HBEP-6's frpc.toml: wss to the dial-home listener, verified against the launcher's certificate by
    its server name, poolCount 0, S1's heartbeats; proxies are (domain, extra transport keys), each a
    tcpmux proxy served from the engine's socket."""
    common = {
        "serverAddr": "127.0.0.1", "serverPort": server_port, "loginFailExit": False,
        "log.to": os.path.join(run, "logs", name + ".log"), "log.level": "info", "log.disablePrintColor": True,
        "metadatas.token": secrets.token_hex(32),
        "transport.protocol": "wss", "transport.tls.trustedCaFile": cert,
        "transport.tls.serverName": server_name, "transport.poolCount": 0,
    }
    if not tcpmux:
        common["transport.tcpMux"] = False
    body = toml_kv(common) + toml_kv(FRPC_HB)
    for domain, socket, extra in proxies:
        body += ('\n[[proxies]]\nname = "%s"\ntype = "tcpmux"\nmultiplexer = "httpconnect"\ncustomDomains = ["%s"]\n'
                 % (domain, domain))
        body += toml_kv(extra)
        body += '[proxies.plugin]\ntype = "unix_domain_socket"\nunixPath = "%s"\n' % socket
    path = os.path.join(run, name + ".toml")
    write_private(path, body)
    return path


def cap_string(cap_bytes):
    """frp's bandwidthLimit as it parses it: MB is 1,048,576 bytes (pkg/config/types/types.go:26-27, 73-77)."""
    return "%.2fMB" % (cap_bytes / MIB)


def cap_parsed(s):
    return float(s[:-2]) * MIB


# --- S17: connections per process -------------------------------------------------------------------

def established_by_pid(pids):
    """Established TCP connections each of pids holds, from ss."""
    out = subprocess.run(["ss", "-tnpH", "state", "established"], capture_output=True, text=True).stdout
    want, count = set(pids), {}
    for line in out.splitlines():
        for part in line.split("pid=")[1:]:
            pid = int(part.split(",")[0])
            if pid in want:
                count[pid] = count.get(pid, 0) + 1
    return count


class S17Sampler(Sampler):
    """S14's sampler, every SAMPLE_S, with each frpc's established connections too, and both frps lists."""

    def __init__(self, roles_fn, apis, conn_roles):
        super().__init__(roles_fn, None)
        self.apis, self.conn_roles, self.conns = apis, conn_roles, []
        self.next_poll = 0.0

    def sample(self):
        super().sample()
        roles = self.roles_fn()
        pids = {r: roles[r] for r in self.conn_roles if roles.get(r)}
        try:
            by = established_by_pid([p for ps in pids.values() for p in ps])
            self.conns.append({"t": self.samples[-1]["t"], "conns": {r: sum(by.get(p, 0) for p in ps) for r, ps in pids.items()}})
        except OSError:
            pass
        if time.time() >= self.next_poll:
            self.next_poll = time.time() + API_POLL_S
            for name, fn in self.apis.items():
                t = time.time()
                try:
                    self.polls.append({"t": t, "frps": name, "proxies": fn()})
                except Exception as e:  # an unreadable list is counted, never read as online
                    self.polls.append({"t": t, "frps": name, "error": str(e)[:200]})


def frps_api(port, pw):
    def api():
        r = http_json("GET", "http://127.0.0.1:%d/api/proxy/tcpmux" % port, auth=("hpcburst", pw))
        return {p["name"]: {"status": p.get("status"), "lastStartTime": p.get("lastStartTime")} for p in (r.get("proxies") or [])}
    return api


# --- S17: one configuration ---------------------------------------------------------------------------

def start_frps(env, P, run, name, hb, tries=6):
    """frps with kernel-chosen ports (its configuration needs them before it starts): three ports taken
    from the kernel, frps started on them, and, if it does not come up listening (another process took
    one in the gap), stopped and started again on fresh ports, up to `tries` times."""
    last = None
    for _ in range(tries):
        bind, mux, web = free_ports(3)
        pw = secrets.token_hex(16)
        P.start(name, [env.b("frps"), "-c", render_frps(run, name, pw, bind, mux, web, hb)])
        try:
            for port in (bind, mux, web):
                wait_port(port, timeout=10.0, alive=lambda: P.alive(name))
            return {"bind": bind, "mux": mux, "web": web, "pw": pw}
        except RuntimeError as e:
            last = e
            P.stop(name)
    raise RuntimeError("%s did not come up listening in %d tries: %s" % (name, tries, last))


def rewrite_ssh_config(ready, link_port, sshdir):
    """The relay clients' ssh configuration through the link: the login node's own (written by the ssh node,
    for the sshd's port) with the link's port and a known_hosts file whose entry names the link's port, so the
    host key is still checked (StrictHostKeyChecking yes)."""
    with open(ready["ssh_config"]) as fh:
        cfg = fh.read()
    with open(ready["known_hosts"]) as fh:
        known = fh.read()
    old = ready["ssh_port"]
    kh = os.path.join(sshdir, "known_hosts")
    write_private(kh, known.replace("[127.0.0.1]:%d" % old, "[127.0.0.1]:%d" % link_port))
    lines = []
    for line in cfg.splitlines():
        k = line.strip().split(None, 1)[0].lower() if line.strip() else ""
        if k == "port":
            line = "  Port %d" % link_port
        elif k == "userknownhostsfile":
            line = "  UserKnownHostsFile %s" % kh
        lines.append(line)
    path = os.path.join(sshdir, "config")
    write_private(path, "\n".join(lines) + "\n")
    return path


def link_argv(env, run, upstream_port, args, queue, queue_bytes):
    argv = [env.b("s17link"), "-map", "127.0.0.1:0=127.0.0.1:%d" % upstream_port,
            "-addrs", os.path.join(run, "link.addrs"),
            "-up-rate", str(args.rate), "-down-rate", str(args.rate), "-delay", "%dus" % args.delay_us,
            "-rcvbuf", str(args.rcvbuf), "-stats", os.path.join(run, "link.json")]
    if queue == "fifo":
        argv += ["-queue", "fifo", "-queue-bytes", str(queue_bytes), "-queue-stats", os.path.join(run, "link-queue.json")]
    return argv


def run_config(env, cfg, args, path_rate=None, calibrate=False):
    """One configuration. cfg: mode (direct | host-dialed), layout (a | c), kind, rep, queue (fair | fifo),
    queue_bytes, concurrent (transfers at once), instances (tunnels), streams (per tunnel), scenario
    (single | many)."""
    import copy
    args = copy.copy(args)
    if cfg.get("bytes"):
        args.bytes = int(cfg["bytes"])
    if cfg.get("scenario") == "many":
        args.streams = 1 if args.streams == 8 else args.streams  # a many-Instances run: one stream per Instance by default
    mode, layout, kind, rep = cfg["mode"], cfg["layout"], cfg["kind"], cfg["rep"]
    queue, queue_bytes = cfg.get("queue", "fair"), int(cfg.get("queue_bytes", 0))
    concurrent, n, scenario = int(cfg.get("concurrent", 1)), int(cfg.get("instances", 1)), cfg.get("scenario", "single")
    tag = "cal" if calibrate else layout
    qtag = "" if queue == "fair" else "-fifo%d" % (queue_bytes // 1024)
    run_id = "%s-%s%s-%s-k%d-n%d-r%d-%s" % (mode, tag, qtag, kind, concurrent, n, rep, time.strftime("%H%M%S", time.gmtime()))
    run = os.path.join(env.work, run_id)
    for d in ("logs", "alloc", "e", "tmp"):
        os.makedirs(os.path.join(run, d), mode=0o700)
    tunnels = []
    for i in range(n):
        tunnels.append({"index": i, "uid": str(uuid.uuid4()), "tag": "s17-%03d" % i, "job": str(1000 + i),
                        "dir": os.path.join(run, "alloc", "%03d" % i), "socket": os.path.join(run, "e", "%03d.sock" % i)})
    spec = os.path.join(run, "spec.json")
    with open(spec, "w") as fh:
        json.dump({"mode": mode, "layout": layout, "tunnels": tunnels}, fh)
    P = Procs(run)
    row = {"run_id": run_id, "mode": mode, "layout": tag, "kind": kind, "rep": rep, "status": "ok", "scenario": scenario,
           "queue": queue, "queue_bytes": queue_bytes if queue == "fifo" else "", "concurrent": concurrent,
           "instances": n, "streams_per_instance": args.streams, "bytes_asked": args.bytes,
           "link_rate": args.rate, "link_delay_us": args.delay_us, "link_rcvbuf": args.rcvbuf}
    row.update(host_info())
    if path_rate:
        row["path_rate"] = round(path_rate)
    sampler, t_ready, ports = None, None, {}
    try:
        subprocess.run([env.b("s17"), "certs", "-dir", run], check=True)
        cert = os.path.join(run, "cert.pem")
        f1 = start_frps(env, P, run, "frps", FRPS_HB)
        apis = {"frps": frps_api(f1["web"], f1["pw"])}
        f2 = None
        listener = [env.b("s17"), "listener", "-listen", "127.0.0.1:0", "-addr-file", os.path.join(run, "listener.addr"),
                    "-stats", os.path.join(run, "listener-stats.csv"),
                    "-frps", "127.0.0.1:%d" % f1["bind"], "-cert", cert, "-key", os.path.join(run, "key.pem"),
                    "-log", os.path.join(run, "logs", "listener.log")]
        if layout == "c":
            f2 = start_frps(env, P, run, "frps2", FRPS2_HB)
            apis["frps2"] = frps_api(f2["web"], f2["pw"])
            listener += ["-frps-bulk", "127.0.0.1:%d" % f2["bind"]]
        P.start("listener", listener)
        wait_file(os.path.join(run, "listener.addr"), alive=lambda: P.alive("listener"))
        dialhome = int(str(read_json(os.path.join(run, "listener.addr"))).rpartition(":")[2])
        ready = {}
        ssh_cfg = None
        if mode == "ssh":
            # The relays through the system ssh and a real sshd (internal/rehearsal's login node, a run-mode fake
            # Slurm whose srun step carries the session over its standard streams). The node binary links
            # internal/rehearsal, so on the development VM this mode is a smoke run only.
            P.start("node", [env.b("s17ssh.test"), "-spec", spec, "-ready", os.path.join(run, "node-ready.json"),
                             "-log", os.path.join(run, "logs", "node.log")],
                    nice=env.nice_node, env=dict(os.environ, S17_SSH="1", TMPDIR=env.tmp))
            wait_file(os.path.join(run, "node-ready.json"), timeout=180.0, alive=lambda: P.alive("node"))
            ready = read_json(os.path.join(run, "node-ready.json"))
            upstream = ready["ssh_port"]
        elif mode == "host-dialed":
            P.start("node", [env.b("s17node.test"), "-spec", spec, "-relay-listen", "127.0.0.1:0",
                             "-tmp", os.path.join(run, "tmp"), "-ready", os.path.join(run, "node-ready.json"),
                             "-state", os.path.join(run, "node-state.json"), "-log", os.path.join(run, "logs", "node.log")],
                    nice=env.nice_node, env=dict(os.environ, S17_NODE="1"))
            wait_file(os.path.join(run, "node-ready.json"), alive=lambda: P.alive("node"))
            ready = read_json(os.path.join(run, "node-ready.json"))
            upstream = ready["relay_port"]
        else:
            P.start("node", [env.b("s17node.test"), "-spec", spec, "-tmp", os.path.join(run, "tmp"),
                             "-ready", os.path.join(run, "node-ready.json"), "-state", os.path.join(run, "node-state.json"),
                             "-log", os.path.join(run, "logs", "node.log")],
                    nice=env.nice_node, env=dict(os.environ, S17_NODE="1"))
            wait_file(os.path.join(run, "node-ready.json"), alive=lambda: P.alive("node"))
            upstream = dialhome
        P.start("link", link_argv(env, run, upstream, args, queue, queue_bytes))
        wait_file(os.path.join(run, "link.addrs"), alive=lambda: P.alive("link"))
        link_port = int(read_json(os.path.join(run, "link.addrs"))[0].rpartition(":")[2])
        if mode == "ssh":
            sshdir = os.path.join(run, "ssh")
            os.makedirs(sshdir, mode=0o700, exist_ok=True)
            ssh_cfg = rewrite_ssh_config(ready, link_port, sshdir)
            P.start("owner", [env.b("s17"), "owner", "-spec", spec, "-ssh-config", ssh_cfg, "-node-ready", os.path.join(run, "node-ready.json"),
                              "-ssh-control-dir", ready["control_dir"], "-control-port", str(free_ports(1)[0]),
                              "-serving-port", str(dialhome), "-state", os.path.join(run, "owner-state.json"),
                              "-log", os.path.join(run, "logs", "owner.log")])
        elif mode == "host-dialed":
            P.start("owner", [env.b("s17"), "owner", "-spec", spec, "-bridge", env.b("s17bridge"),
                              "-link", "127.0.0.1:%d" % link_port, "-control-port", str(free_ports(1)[0]),
                              "-serving-port", str(dialhome), "-state", os.path.join(run, "owner-state.json"),
                              "-log", os.path.join(run, "logs", "owner.log")])
        frpc_env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin")}
        t_frpc = time.time()
        want = {"frps": [], "frps2": []}
        frpc_names, bulk_names = [], []
        for i, t in enumerate(tunnels):
            if mode == "direct":
                server_port = link_port
            elif mode == "ssh":
                server_port = ready["tunnels"][i]["serving_port"]
            else:
                server_port = ready["serving_ports"][i]
            name = "frpc-%03d" % i
            P.start(name, [env.b("frpc"), "-c", render_frpc(run, name, server_port, cert, SERVER_NAME, [(t["uid"], t["socket"], {})])],
                    nice=env.nice_node, env=frpc_env)
            frpc_names.append(name)
            want["frps"].append(t["uid"])
            if layout == "c":
                bname = "frpcb-%03d" % i
                P.start(bname, [env.b("frpc"), "-c", render_frpc(run, bname, server_port, cert, BULK_SERVER_NAME,
                                                                  [(t["uid"] + "-bulk", t["socket"], {})], tcpmux=False)],
                        nice=env.nice_node, env=frpc_env)
                bulk_names.append(bname)
                want["frps2"].append(t["uid"] + "-bulk")
        want = {f: us for f, us in want.items() if us}
        end = time.time() + args.ready_timeout
        while True:
            try:
                if all(all((apis[f]().get(u) or {}).get("status") == "online" for u in us) for f, us in want.items()):
                    break
            except Exception:
                pass
            if time.time() > end:
                raise RuntimeError("proxies not all online after %.0f s" % args.ready_timeout)
            time.sleep(0.25)
        t_ready = time.time()
        row["all_online_s"] = round(t_ready - t_frpc, 2)

        def roles():
            r = {"node": [P.pid("node")], "driver": [os.getpid()], "frpc": [P.pid(x) for x in frpc_names]}
            if bulk_names:
                r["frpc-bulk"] = [P.pid(x) for x in bulk_names]
            for name in ("frps", "frps2", "listener", "owner", "link", "client"):
                if name in P.procs:
                    r[name] = [P.pid(name)]
            if "owner" in P.procs:
                st = read_json(os.path.join(run, "owner-state.json")) or []
                r["bridges"] = [x["PID"] for x in st if x.get("PID")]
            return r

        held = listeners_of([P.pid(k) for k in P.procs])
        off = [h for h in held if h[0] != "127.0.0.1"]
        row["listeners"] = len(held)
        if off:
            raise RuntimeError("listeners off loopback: %s" % off)
        sampler = S17Sampler(roles, apis, ["frpc", "frpc-bulk"] if bulk_names else ["frpc"])
        sampler.start()
        time.sleep(1.0)
        load_out = os.path.join(run, "load.json")
        bulk_mux = f2["mux"] if f2 else f1["mux"]
        streams = 0 if calibrate else args.streams
        if scenario == "many":
            argv = [env.b("s17"), "many", "-spec", spec, "-mux", "127.0.0.1:%d" % f1["mux"], "-bulk-mux", "127.0.0.1:%d" % bulk_mux,
                    "-bulk-suffix", "-bulk" if layout == "c" else "", "-streams", str(streams),
                    "-stream-tokens", str(args.stream_tokens), "-bytes", str(args.bytes),
                    "-interval", "%ds" % args.interval, "-duration", "%ds" % args.duration, "-warmup", "%ds" % args.settle,
                    "-out", load_out]
        else:
            argv = [env.b("s17"), "load", "-spec", spec, "-mux", "127.0.0.1:%d" % f1["mux"],
                    "-bulk-mux", "127.0.0.1:%d" % bulk_mux, "-bulk-host", tunnels[0]["uid"] + ("-bulk" if layout == "c" else ""),
                    "-streams", str(streams), "-stream-tokens", str(args.stream_tokens), "-kind", kind,
                    "-count", str(args.cal_count if calibrate else args.count), "-concurrent", str(concurrent),
                    "-reference", "%ds" % (1 if calibrate else args.reference), "-settle", "%ds" % args.settle,
                    "-bytes", str(args.bytes), "-inputs", str(args.inputs), "-out", load_out]
        P.start("client", argv)
        p = P.procs["client"][0]
        deadline = time.time() + args.run_timeout
        while p.poll() is None:
            if sampler.low_memory is not None:
                raise RuntimeError("the host's available memory fell to %d MB" % (sampler.low_memory // 1024))
            if time.time() > deadline:
                raise RuntimeError("the load client did not finish")
            time.sleep(0.25)
        if p.returncode != 0:
            raise RuntimeError("the load client exited %d" % p.returncode)
        time.sleep(1.0)
        sampler.stop()
        load = read_json(load_out)
        summarize_run(row, load, sampler, kind, args, calibrate, scenario, concurrent)
    except Exception as e:
        row["status"] = "failed: " + str(e)[:300]
        log("%s: %s" % (run_id, row["status"]))
        if sampler and sampler.is_alive():
            sampler.stop()
    finally:
        P.stop("client")
        P.stop("owner")
        for name in [k for k in P.procs if k.startswith("frpc")] + ["node", "listener", "frps2", "frps", "link"]:
            P.stop(name)
        row["leftovers"] = len(P.leftovers())
        row["leftover_listeners"] = len(listeners_of([pid for _, pid, _ in P.procs.values() if start_time(pid)] or [0]))
    count_losses(row, run, mode, sampler, t_ready, {"frps": [t["uid"] for t in tunnels], "frps2": [t["uid"] + "-bulk" for t in tunnels] if layout == "c" else []})
    link = read_json(os.path.join(run, "link.json")) or []
    row["link_conns"] = len(link)
    row["link_conns_over_1mib"] = sum(1 for c in link if c["bytes_up"] + c["bytes_down"] > MIB)
    q = read_json(os.path.join(run, "link-queue.json")) or {}
    for d in ("up", "down"):
        if q.get(d):
            row["queue_%s_max_bytes" % d] = q[d]["max_occupancy_bytes"]
            row["queue_%s_seconds_full" % d] = round(q[d]["seconds_full"], 3)
            row["queue_%s_seconds_busy" % d] = round(q[d]["seconds_busy"], 3)
    if sampler:
        for r in ("frpc", "frpc-bulk"):
            vals = [c["conns"].get(r) for c in sampler.conns if c["conns"].get(r) is not None]
            row["conns_max_" + r] = max(vals) if vals else ""
    samples = sampler.samples if sampler else []
    lst = []
    try:
        with open(os.path.join(run, "listener-stats.csv")) as fh:
            lst = [tuple(int(x) for x in line.split(",")) for line in fh if line.strip()]
    except OSError:
        pass
    if lst and row.get("bulk_start_s") not in (None, ""):
        hs = analysis.handshake_rates(lst, row["bulk_start_s"], row["bulk_end_s"])
        if hs:
            row["tls_accepts_per_s"], row["tls_hellos_per_s"] = round(hs["accepts_per_s"], 3), round(hs["hellos_per_s"], 3)
            row["tls_accepts_window"], row["tls_hellos_window"] = hs["accepts"], hs["hellos"]
    raw = {"row": row, "load": read_json(os.path.join(run, "load.json")), "link": link, "link_queue": q, "listener_stats": lst,
           "owner": read_json(os.path.join(run, "owner-state.json")), "node": read_json(os.path.join(run, "node-state.json")),
           "polls": sampler.polls if sampler else [], "conns": sampler.conns if sampler else [], "samples": samples}
    with open(os.path.join(env.results, "raw", run_id + ".json"), "w") as fh:
        json.dump(raw, fh)
    write_samples(env.results, run_id, samples)
    return row


def summarize_run(row, load, sampler, kind, args, calibrate, scenario="single", concurrent=1, listener_stats=None):
    t0 = load["t0_unix_ns"] / 1e9
    if scenario == "many":
        return summarize_many(row, load, sampler, t0)
    trs = load["transfers"]
    m = analysis.transfer_metrics(trs, kind, args.bytes)
    row.update(m)
    if calibrate:
        return
    row.update(analysis.batch_metrics(trs, concurrent))
    v = analysis.validity(load, kind, args.bytes, concurrent)
    row["valid"], row["invalid_reason"] = v["valid"], v["reason"]
    row["streams_in_flight_all"] = v["in_flight_all"]
    g = analysis.gap_stats(load)
    row.update(g)
    # CPU over the reference window, and over the bulk phase (first transfer's start to last's end).
    ref = analysis.window_samples(sampler.samples, t0 + load["reference_start_ns"] / 1e9, t0 + load["reference_end_ns"] / 1e9)
    row["ref_start_s"], row["ref_end_s"] = round(t0 + load["reference_start_ns"] / 1e9, 6), round(t0 + load["reference_end_ns"] / 1e9, 6)
    if trs:
        bs, be = t0 + min(tr["start_ns"] for tr in trs) / 1e9, t0 + max(tr["end_ns"] for tr in trs) / 1e9
        row["bulk_start_s"], row["bulk_end_s"] = round(bs, 6), round(be, 6)
        bulk = analysis.window_samples(sampler.samples, bs, be)
    else:
        bulk = []
    cpu_rows(row, ref, bulk)


def cpu_rows(row, ref, bulk):
    for role in ALL_ROLES:
        for label, win in (("ref", ref), ("bulk", bulk)):
            c = analysis.cores(win, role, CLK)
            row["cpu_%s_%s" % (label, role)] = round(c, 4) if c is not None else ""
    for label, win in (("ref", ref), ("bulk", bulk)):
        fr = [analysis.cores(win, r, CLK) for r in FRONT_ROLES]
        fr = [x for x in fr if x is not None]
        row["cpu_%s_front" % label] = round(sum(fr), 4) if fr else ""
    busy = analysis.busy_cores(bulk[0]["stat"], bulk[-1]["stat"]) if len(bulk) >= 2 else None
    row["host_busy_bulk"] = round(busy, 3) if busy is not None else ""


def summarize_many(row, load, sampler, t0):
    """The many-Instances run: validity, the bulk requests' time to first byte, the streams' gaps, and the
    front host's CPU over the measured window."""
    v = analysis.validity_many(load)
    row["valid"], row["invalid_reason"] = v["valid"], v["reason"]
    row["streams_in_flight_all"] = v["in_flight_all"]
    row.update(analysis.many_metrics(load))
    w0, w1 = t0 + load["window_start_ns"] / 1e9, t0 + load["window_end_ns"] / 1e9
    row["ref_start_s"], row["ref_end_s"] = round(w0, 6), round(w1, 6)
    row["bulk_start_s"], row["bulk_end_s"] = round(w0, 6), round(w1, 6)
    win = analysis.window_samples(sampler.samples, w0, w1)
    cpu_rows(row, win, win)
    row["window_s"] = round(w1 - w0, 3)
    row["bulk_bytes_per_s"] = round(sum(r["bytes"] for r in load["bulk"] if not r.get("err")) / (w1 - w0))


def count_losses(row, run, mode, sampler, t_ready, uids):
    """Tunnel drops during the run, by frps's lists, the frpc logins and the relays' sessions."""
    dropped, unreadable = set(), 0
    for f, us in uids.items():
        if not us:
            continue
        polls = [p for p in (sampler.polls if sampler else []) if p.get("frps") == f]
        d, u = analysis.drops(polls, us, t_ready or time.time())
        dropped |= d
        unreadable += u
    row["drops_api"], row["api_unreadable"] = len(dropped), unreadable
    relogins, hb = 0, 0
    for fname in sorted(os.listdir(os.path.join(run, "logs"))):
        if not (fname.startswith("frpc") or fname.startswith("frps")) or not fname.endswith(".log"):
            continue
        try:
            with open(os.path.join(run, "logs", fname), errors="replace") as fh:
                text = fh.read()
        except OSError:
            continue
        if fname.startswith("frpc"):
            relogins += max(0, analysis.logins(text) - 1)
        hb += text.count("heartbeat timeout")
    row["frpc_relogins"], row["hb_timeout_lines"] = relogins, hb
    if mode in ("host-dialed", "ssh"):
        st = read_json(os.path.join(run, "owner-state.json")) or []
        row["relay_restarts"] = sum(max(0, x.get("Sessions", 0) - 1) for x in st)
    if mode == "ssh":
        try:
            with open(os.path.join(run, "logs", "node.out"), errors="replace") as fh:
                text = fh.read()
            import re
            m = re.search(r"rehearsal: (\d+) leaked", text)
            row["node_leaked"] = int(m.group(1)) if m else ""
        except OSError:
            row["node_leaked"] = ""


# --- commands ----------------------------------------------------------------------------------------

BDP_BYTES = 110_400      # 48 MB/s x 2.3 ms
FRP_WINDOW = 6 * MIB     # frp's stream window, S15's mechanism
GROUPS = ("fifo", "ssh", "concurrent", "many")


def configs_for(group, args):
    """The configurations of a group, in the order they run. Each is a cfg for run_config."""
    out = []
    if group == "fifo":
        # Issue #413, criterion 1: direct mode, a 64 MiB download beside 8 token streams, the shared link a
        # single FIFO at three queue depths (the bandwidth-delay product, 1 MiB, frp's 6 MiB window), layouts
        # (a) one frpc and (c) a separate frpc, and the same two layouts on the fair link as controls from this
        # same runner.
        for layout in args.layouts:
            out.append(dict(mode="direct", layout=layout, kind="download", queue="fair"))
        for q in args.queue_bytes:
            for layout in args.layouts:
                out.append(dict(mode="direct", layout=layout, kind="download", queue="fifo", queue_bytes=q))
    elif group == "ssh":
        # Criterion 2: host-dialed mode, a 64 MiB download beside 8 token streams, the relay through S15's bridge
        # (the control) and through a real ssh client, a real sshd and srun's standard streams, layouts (a), (c).
        for mode in args.modes:
            for layout in args.layouts:
                out.append(dict(mode=mode, layout=layout, kind="download", queue="fair"))
    elif group == "concurrent":
        # Criterion 3: 1, 2 and 4 downloads at once beside 8 token streams, layouts (a) and (c).
        for mode in args.modes:
            for layout in args.layouts:
                for k in args.concurrent:
                    out.append(dict(mode=mode, layout=layout, kind="download", queue="fair", concurrent=k))
    elif group == "many":
        # Criterion 4: N Instances, each with both frpc (layout c) or its one (layout a, the control), behind
        # one front, a bulk request each per interval.
        for mode in args.modes:
            for layout in args.layouts:
                for n in args.instances:
                    for size in args.many_bytes:
                        out.append(dict(mode=mode, layout=layout, kind="download", queue="fair", scenario="many", instances=n, bytes=size))
    else:
        raise SystemExit("unknown group %r" % group)
    return out


def calibrate(env, mode, rep, args):
    """The path rate: one uncapped download in layout (a), with no stream, on the fair link, at the receiver;
    the median of its transfers."""
    row = run_config(env, dict(mode=mode, layout="a", kind="download", rep=rep, queue="fair"), args, calibrate=True)
    row["group"] = "calibration"
    append_row(os.path.join(env.results, "runs.csv"), row)
    rate = row.get("recv_rate_median")
    if not rate:
        raise RuntimeError("calibration failed: %s" % row.get("status"))
    log("calibrated %s path rate %.1f MB/s" % (mode, rate / 1e6))
    return rate


def cmd_run(args):
    env = Env()
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    log("host: %s; open files %d -> %d" % (json.dumps(host_info()), soft, hard))
    cfgs = configs_for(args.group, args)
    rates = {}
    for cfg in cfgs:
        la = loadavg()[0]
        if args.max_load and la > args.max_load:
            log("skip %s: 1-min load %.2f above %.2f" % (cfg, la, args.max_load))
            continue
        cfg = dict(cfg, rep=args.rep)
        if cfg.get("scenario") != "many" and cfg["mode"] not in rates:
            rates[cfg["mode"]] = calibrate(env, cfg["mode"], args.rep, args)
        log("run %s" % json.dumps(cfg))
        row = run_config(env, cfg, args, path_rate=rates.get(cfg["mode"]))
        row["group"] = args.group
        append_row(os.path.join(env.results, "runs.csv"), row)
        log("  %s valid=%s %s" % (row["status"], row.get("valid"), json.dumps({k: row.get(k) for k in (
            "gap_ref_p99_ms", "gap_ref_max_ms", "gap_bulk_p99_ms", "gap_bulk_max_ms", "gap_p99_ms", "ttfb_p50_ms", "ttfb_p99_ms",
            "recv_rate_median", "batch_time_median_s", "overlap_min", "queue_up_max_bytes", "queue_down_max_bytes",
            "cpu_ref_front", "cpu_bulk_front", "invalid_reason")})))


def cmd_leftovers(args):
    env = Env()
    alive = []
    for d in sorted(os.listdir(env.work)):
        rec = read_json(os.path.join(env.work, d, "pids.json")) or {}
        for name, (pid, st) in rec.items():
            if st is not None and start_time(pid) == st:
                alive.append((d, name, pid))
    print("recorded processes still alive: %s" % (alive or "none"))
    held = listeners_of([pid for _, _, pid in alive]) if alive else []
    print("listeners held by them: %s" % (held or "none"))
    return 1 if alive else 0


def ints(s):
    return [int(x) for x in s.split(",") if x]


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--group", required=True, choices=GROUPS)
    r.add_argument("--rep", type=int, required=True)
    r.add_argument("--modes", default="direct", type=lambda s: s.split(","), help="direct, host-dialed (S15's bridge) or ssh (a real ssh and sshd; the ssh group defaults to host-dialed,ssh)")
    r.add_argument("--layouts", default="a,c", type=lambda s: s.split(","))
    r.add_argument("--queue-bytes", default="%d,%d,%d" % (BDP_BYTES, MIB, FRP_WINDOW), type=ints,
                   help="fifo group: the shared queue's size, per direction (default the bandwidth-delay product, 1 MiB, 6 MiB)")
    r.add_argument("--concurrent", default="1,2,4", type=ints, help="concurrent group: transfers at once")
    r.add_argument("--instances", default="10,50", type=ints, help="many group: Instances")
    r.add_argument("--many-bytes", default="262144,1048576", type=ints, help="many group: each bulk request's size")
    r.add_argument("--interval", type=int, default=10, help="many group: one bulk request per Instance each interval (s)")
    r.add_argument("--duration", type=int, default=60, help="many group: the measured window (s)")
    r.add_argument("--rate", type=int, default=48_000_000, help="the link's rate each way, bytes/s (S1's probes: 48-85 MB/s)")
    r.add_argument("--delay-us", type=int, default=1150, help="one-way delay, microseconds (S1's probes: about 2.3 ms RTT)")
    r.add_argument("--rcvbuf", type=int, default=256 * 1024)
    r.add_argument("--streams", type=int, default=8, help="token streams (the many group: per Instance; set 1)")
    r.add_argument("--stream-tokens", type=int, default=2000)
    r.add_argument("--count", type=int, default=5, help="batches of transfers")
    r.add_argument("--cal-count", type=int, default=3)
    r.add_argument("--reference", type=int, default=10)
    r.add_argument("--settle", type=int, default=4)
    r.add_argument("--bytes", type=int, default=64 * MIB)
    r.add_argument("--inputs", type=int, default=2000)
    r.add_argument("--ready-timeout", type=float, default=60.0)
    r.add_argument("--run-timeout", type=float, default=300.0)
    r.add_argument("--max-load", type=float, default=0.0)
    sub.add_parser("host")
    sub.add_parser("leftovers")
    args = ap.parse_args()
    if args.cmd == "run" and args.group == "ssh" and args.modes == ["direct"]:
        args.modes = ["host-dialed", "ssh"]
    if args.cmd == "run":
        cmd_run(args)
    elif args.cmd == "host":
        print(json.dumps(host_info()))
    else:
        sys.exit(cmd_leftovers(args))


if __name__ == "__main__":
    main()
