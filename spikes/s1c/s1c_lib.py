"""Spike S1c: shared helpers (processes, ports, toxiproxy, configs). Python standard library only.

Every process is started under `nice -n 10`, recorded with its PID and start time, and stopped by
that record, never by name. Every listener is on 127.0.0.1, inside PORT_RANGE.
"""
import base64
import json
import os
import secrets
import signal
import socket
import subprocess
import sys
import time
import urllib.request

PORT_RANGE = range(29400, 29500)
PORTS = dict(
    sim=29400, bulk=29401,
    frps=29410, mux=29411, dash=29412,
    tox_api=29420, tox_frp=29421, tox_tls=29422, tox_ssh=29423, tox_direct=29424, tox_bulk=29425, tox_fixture=29426, fixture_srv=29427,
    sshd=29430, ssh_l=29431,
    envoy=29440, envoy_admin=29441,
    sh_frp=29450, sh_tls=29451, sh_bulk=29452, sh_sim=29453, sh_ssh=29454,
)
TLS_NAME = "front.s1c.test"   # the name in the test certificate; never resolved, frpc dials 127.0.0.1
LOAD_MAX, MEM_MIN_GB = 4.0, 8.0

# Heartbeat settings. "default" sets nothing: frp v0.71.1 then uses tcpMux = true, yamux keepalive
# every 30 s on both sides and NO application heartbeat (pkg/config/v1/client.go:154-158,
# server.go:183-191). "s1" is spike S1's chosen set (its frpc/frps templates). "tight" leans on the
# application heartbeat, because the yamux ping timeout is fixed at 10 s (fatedier/yamux mux.go).
HB = {
    "default": dict(frps={}, frpc={}),
    "s1": dict(
        frps={"transport.tcpMuxKeepaliveInterval": 10, "transport.heartbeatTimeout": 25, "transport.tcpKeepalive": 30},
        frpc={"transport.tcpMuxKeepaliveInterval": 10, "transport.heartbeatInterval": 10,
              "transport.heartbeatTimeout": 25, "transport.dialServerKeepalive": 30}),
    "tight": dict(
        frps={"transport.tcpMuxKeepaliveInterval": 5, "transport.heartbeatTimeout": 6, "transport.tcpKeepalive": 30},
        frpc={"transport.tcpMuxKeepaliveInterval": 5, "transport.heartbeatInterval": 2,
              "transport.heartbeatTimeout": 6, "transport.dialServerKeepalive": 30}),
}


def log(msg):
    sys.stderr.write("%s %s\n" % (time.strftime("%H:%M:%S"), msg))
    sys.stderr.flush()


class Env:
    """Paths. S1C_SCRATCH holds binaries (bin/), generated configs, logs and raw data."""

    def __init__(self):
        self.scratch = os.environ.get("S1C_SCRATCH") or os.path.join(os.environ.get("TMPDIR", "/tmp"), "s1c")
        self.bin = os.path.join(self.scratch, "bin")
        self.sim = os.environ.get("S1C_SIM", os.path.join(self.bin, "llm-d-inference-sim"))
        self.envoy = os.environ.get("S1C_ENVOY", os.path.join(self.bin, "envoy"))
        for d in ("run", "logs", "raw", "certs", "ssh"):
            os.makedirs(os.path.join(self.scratch, d), exist_ok=True)

    def p(self, *parts):
        return os.path.join(self.scratch, *parts)

    def b(self, name):
        return os.path.join(self.bin, name)


def start_time(pid):
    try:
        with open("/proc/%d/stat" % pid) as fh:
            return fh.read().rsplit(")", 1)[1].split()[19]
    except OSError:
        return None


def descendants(pid):
    kids = {}
    for d in os.listdir("/proc"):
        if d.isdigit():
            try:
                with open("/proc/%s/stat" % d) as fh:
                    ppid = int(fh.read().rsplit(")", 1)[1].split()[1])
                kids.setdefault(ppid, []).append(int(d))
            except (OSError, ValueError, IndexError):
                pass
    out, todo = [], [pid]
    while todo:
        for c in kids.get(todo.pop(), []):
            out.append(c)
            todo.append(c)
    return out


class Procs:
    """Processes started by this run, by name. Stopped by recorded PID plus start time."""

    def __init__(self, env):
        self.env, self.procs = env, {}
        self.record = env.p("run", "pids.json")

    def _save(self):
        with open(self.record + ".tmp", "w") as fh:
            json.dump({k: v[1:] for k, v in self.procs.items()}, fh)
        os.replace(self.record + ".tmp", self.record)

    def start(self, name, argv, env=None):
        if name in self.procs:
            raise RuntimeError("%s already running" % name)
        logf = open(self.env.p("logs", name + ".log"), "ab")
        p = subprocess.Popen(["nice", "-n", "10"] + argv, stdout=logf, stderr=subprocess.STDOUT,
                             stdin=subprocess.DEVNULL, start_new_session=True, env=env)
        logf.close()
        time.sleep(0.05)
        self.procs[name] = (p, p.pid, start_time(p.pid))
        self._save()
        return p.pid

    def pid(self, name):
        return self.procs[name][1]

    def alive(self, name):
        if name not in self.procs:
            return False
        p, pid, st = self.procs[name]
        return p.poll() is None and start_time(pid) == st

    def signal(self, name, sig):
        p, pid, st = self.procs[name]
        if start_time(pid) != st:
            raise RuntimeError("%s: PID %d no longer has its recorded start time" % (name, pid))
        os.kill(pid, sig)

    def stop(self, name, timeout=5.0):
        if name not in self.procs:
            return
        p, pid, st = self.procs.pop(name)
        kids = [(k, start_time(k)) for k in descendants(pid)] if p.poll() is None else []
        if p.poll() is None and start_time(pid) == st:
            try:
                os.kill(pid, signal.SIGCONT)
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                p.wait(timeout)
            except subprocess.TimeoutExpired:
                os.kill(pid, signal.SIGKILL)
                p.wait(5)
        else:
            p.poll()
        for k, kst in kids:   # e.g. sshd's per-connection children
            if start_time(k) == kst and kst is not None:
                try:
                    os.kill(k, signal.SIGTERM)
                except ProcessLookupError:
                    pass
        self._save()

    def stop_all(self):
        for name in list(reversed(list(self.procs))):
            try:
                self.stop(name)
            except Exception as exc:  # report and keep stopping the rest
                log("stop %s: %s" % (name, exc))


def listeners():
    """(local address, port, process) of every TCP listener in PORT_RANGE, from ss."""
    out = subprocess.run(["ss", "-ltnpH"], capture_output=True, text=True, check=True).stdout
    rows = []
    for line in out.splitlines():
        f = line.split()
        if len(f) < 4:
            continue
        addr, _, port = f[3].rpartition(":")
        if port.isdigit() and int(port) in PORT_RANGE:
            rows.append((addr, int(port), f[5] if len(f) > 5 else ""))
    return rows


def assert_loopback_only():
    bad = [r for r in listeners() if r[0] != "127.0.0.1"]
    if bad:
        raise RuntimeError("listener off loopback: %s" % bad)


def wait_port(port, timeout=20.0):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(0.5)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.1)
    raise RuntimeError("nothing listens on 127.0.0.1:%d after %.0f s" % (port, timeout))


def load_gate(max_wait=1800):
    """Wait until the 1-minute load is <= 4.0 and >= 8 GB is available (SPIKE-COMMON)."""
    waited = 0
    while True:
        with open("/proc/loadavg") as fh:
            l1 = float(fh.read().split()[0])
        with open("/proc/meminfo") as fh:
            mem = {l.split(":")[0]: l.split()[1] for l in fh}
        avail = int(mem["MemAvailable"]) / 1024 / 1024
        if l1 <= LOAD_MAX and avail >= MEM_MIN_GB:
            return l1
        if waited >= max_wait:
            raise RuntimeError("load %.2f / %.1f GB available after %d s of waiting" % (l1, avail, waited))
        log("gate: load %.2f, %.1f GB available; waiting 60 s" % (l1, avail))
        time.sleep(60)
        waited += 60


def load1():
    with open("/proc/loadavg") as fh:
        return float(fh.read().split()[0])


def cpu_ticks(pid):
    try:
        with open("/proc/%d/stat" % pid) as fh:
            f = fh.read().rsplit(")", 1)[1].split()
        return int(f[11]) + int(f[12])
    except OSError:
        return None


def rss_kb(pid):
    try:
        with open("/proc/%d/status" % pid) as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except OSError:
        pass
    return None


# --- toxiproxy ------------------------------------------------------------------------------------

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


TOX = "http://127.0.0.1:%d" % PORTS["tox_api"]
TOX_PROXIES = {   # name: (listen, upstream)
    "frp": (PORTS["tox_frp"], PORTS["frps"]),
    "tls": (PORTS["tox_tls"], PORTS["envoy"]),
    "ssh": (PORTS["tox_ssh"], PORTS["sshd"]),
    "direct": (PORTS["tox_direct"], PORTS["sim"]),
    "bulkdirect": (PORTS["tox_bulk"], PORTS["bulk"]),
}


def tox_create_all():
    for name, (lp, up) in TOX_PROXIES.items():
        http_json("POST", TOX + "/proxies", {"name": name, "listen": "127.0.0.1:%d" % lp,
                                             "upstream": "127.0.0.1:%d" % up, "enabled": True})


def tox_clear(name):
    for t in http_json("GET", TOX + "/proxies/%s/toxics" % name):
        http_json("DELETE", TOX + "/proxies/%s/toxics/%s" % (name, t["name"]))


def tox_set(name, cond, bw_kBps):
    """Impairment of the link: clean, lat20 (10 ms each way = 20 ms added round trip) or bw (a
    bandwidth cap in both directions; toxiproxy's KB is 1000 bytes, toxics/bandwidth.go)."""
    tox_clear(name)
    if cond == "lat20":
        for stream in ("upstream", "downstream"):
            http_json("POST", TOX + "/proxies/%s/toxics" % name, {
                "name": "lat_" + stream, "type": "latency", "stream": stream, "toxicity": 1.0,
                "attributes": {"latency": 10, "jitter": 0}})
    elif cond == "bw":
        for stream in ("upstream", "downstream"):
            http_json("POST", TOX + "/proxies/%s/toxics" % name, {
                "name": "bw_" + stream, "type": "bandwidth", "stream": stream, "toxicity": 1.0,
                "attributes": {"rate": bw_kBps}})
    elif cond not in ("clean",):
        raise ValueError("unknown condition %r" % cond)


def tox_bytes():
    """Bytes toxiproxy received per (proxy, direction), from its Prometheus metrics."""
    text = http_json("GET", TOX + "/metrics")
    out = {}
    for line in text.splitlines():
        if line.startswith("toxiproxy_proxy_received_bytes_total{"):
            labels, val = line.rsplit(" ", 1)
            d = dict(kv.split("=", 1) for kv in labels[labels.index("{") + 1:-1].split(","))
            out[(d["proxy"].strip('"'), d["direction"].strip('"'))] = float(val)
    return out


# --- configs --------------------------------------------------------------------------------------

def toml_kv(d):
    return "".join("%s = %s\n" % (k, json.dumps(v)) for k, v in d.items())


def secret_file(path):
    if not os.path.exists(path):
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.write(fd, secrets.token_hex(24).encode())
        os.close(fd)
    with open(path) as fh:
        return fh.read().strip()


def render_frps(env, setting="s1", level="info", extra=None, tcpmux=True):
    secret_file(env.p("run", "token"))
    pw = secret_file(env.p("run", "dash.pass"))
    body = toml_kv({
        "bindAddr": "127.0.0.1", "bindPort": PORTS["frps"], "proxyBindAddr": "127.0.0.1",
        "tcpmuxHTTPConnectPort": PORTS["mux"], "transport.tcpMux": tcpmux,   # must match frpc's
        "log.to": env.p("logs", "frps-frp.log"), "log.level": level, "log.disablePrintColor": True,
        "auth.method": "token", "auth.tokenSource.type": "file",
        "auth.tokenSource.file.path": env.p("run", "token"),
        "webServer.addr": "127.0.0.1", "webServer.port": PORTS["dash"],
        "webServer.user": "admin", "webServer.password": pw,
    }) + toml_kv(HB[setting]["frps"]) + toml_kv(extra or {})
    path = env.p("run", "frps.toml")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.write(fd, body.encode())
    os.close(fd)
    return path


def render_frpc(env, server_port, protocol="tcp", setting="s1", tcpmux=True, pool=0, level="info"):
    d = {
        "serverAddr": "127.0.0.1", "serverPort": server_port, "user": "inst", "loginFailExit": False,
        "log.to": env.p("logs", "frpc-frp.log"), "log.level": level, "log.disablePrintColor": True,
        "auth.method": "token", "auth.tokenSource.type": "file",
        "auth.tokenSource.file.path": env.p("run", "token"),
        "transport.protocol": protocol, "transport.tcpMux": tcpmux, "transport.poolCount": pool,
    }
    if protocol == "wss":
        d["transport.tls.serverName"] = TLS_NAME
        d["transport.tls.trustedCaFile"] = env.p("certs", "ca.crt")
    body = toml_kv(d) + toml_kv(HB[setting]["frpc"])
    for name, dom, port in (("engine", "inst", PORTS["sim"]), ("bulk", "bulk", PORTS["bulk"])):
        body += ('\n[[proxies]]\nname = "%s"\ntype = "tcpmux"\nmultiplexer = "httpconnect"\n'
                 'customDomains = ["%s"]\nlocalIP = "127.0.0.1"\nlocalPort = %d\n') % (name, dom, port)
    path = env.p("run", "frpc.toml")
    with open(path, "w") as fh:
        fh.write(body)
    return path


ENVOY_YAML = """\
# TLS-terminating front for frp's wss transport: HTTPS on {envoy}, the websocket upgrade on
# /~!frp passed to frps's bind port (frps serves plain websocket, not wss).
admin:
  address: {{socket_address: {{address: 127.0.0.1, port_value: {admin}}}}}
static_resources:
  listeners:
  - name: front_tls
    address: {{socket_address: {{address: 127.0.0.1, port_value: {envoy}}}}}
    filter_chains:
    - transport_socket:
        name: envoy.transport_sockets.tls
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.DownstreamTlsContext
          common_tls_context:
            tls_certificates:
            - certificate_chain: {{filename: "{crt}"}}
              private_key: {{filename: "{key}"}}
      filters:
      - name: envoy.filters.network.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
          stat_prefix: front
          stream_idle_timeout: 0s
          upgrade_configs: [{{upgrade_type: websocket}}]
          route_config:
            virtual_hosts:
            - name: all
              domains: ["*"]
              routes:
              - match: {{path: "/~!frp"}}
                route: {{cluster: frps, timeout: 0s, idle_timeout: 0s}}
          http_filters:
          - name: envoy.filters.http.router
            typed_config: {{"@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router}}
  clusters:
  - name: frps
    type: STATIC
    connect_timeout: 5s
    load_assignment:
      cluster_name: frps
      endpoints: [{{lb_endpoints: [{{endpoint: {{address: {{socket_address: {{address: 127.0.0.1, port_value: {frps}}}}}}}}}]}}]
"""


def render_envoy(env):
    path = env.p("run", "envoy.yaml")
    with open(path, "w") as fh:
        fh.write(ENVOY_YAML.format(envoy=PORTS["envoy"], admin=PORTS["envoy_admin"], frps=PORTS["frps"],
                                   crt=env.p("certs", "server.crt"), key=env.p("certs", "server.key")))
    return path


def make_certs(env):
    """A throwaway CA and a server certificate for TLS_NAME, in scratch, valid 7 days."""
    c = env.p("certs")
    if os.path.exists(os.path.join(c, "server.crt")):
        return
    run = lambda *a: subprocess.run(list(a), check=True, capture_output=True)
    run("openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
        "-keyout", c + "/ca.key", "-out", c + "/ca.crt", "-days", "7", "-subj", "/CN=s1c test CA",
        "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign")
    run("openssl", "req", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
        "-keyout", c + "/server.key", "-out", c + "/server.csr", "-subj", "/CN=" + TLS_NAME)
    with open(c + "/server.ext", "w") as fh:
        fh.write("subjectAltName=DNS:%s\nbasicConstraints=CA:FALSE\nkeyUsage=digitalSignature\n"
                 "extendedKeyUsage=serverAuth\n" % TLS_NAME)
    run("openssl", "x509", "-req", "-in", c + "/server.csr", "-CA", c + "/ca.crt", "-CAkey", c + "/ca.key",
        "-CAcreateserial", "-out", c + "/server.crt", "-days", "7", "-extfile", c + "/server.ext")
    for f in ("ca.key", "server.key"):
        os.chmod(os.path.join(c, f), 0o600)


def make_ssh(env):
    """Keys and config for a private sshd run as this user on 127.0.0.1 (never ~/.ssh, never the
    system sshd). The client key is restricted to one forward, to frps's bind port."""
    d = env.p("ssh")
    if not os.path.exists(d + "/host_key"):
        for k, c in (("host_key", "s1c-host"), ("client_key", "s1c-client")):
            subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", c, "-f", d + "/" + k], check=True)
    with open(d + "/client_key.pub") as fh:
        pub = fh.read().strip()
    with open(d + "/authorized_keys", "w") as fh:
        fh.write('restrict,port-forwarding,permitopen="127.0.0.1:%d" %s\n' % (PORTS["frps"], pub))
    with open(d + "/host_key.pub") as fh:
        hk = " ".join(fh.read().split()[:2])
    with open(d + "/known_hosts", "w") as fh:
        fh.write("s1c-sshd %s\n" % hk)
    user = subprocess.run(["id", "-un"], capture_output=True, text=True, check=True).stdout.strip()
    with open(d + "/sshd_config", "w") as fh:
        fh.write("\n".join([
            "Port %d" % PORTS["sshd"], "ListenAddress 127.0.0.1", "HostKey %s/host_key" % d,
            "PidFile %s/sshd.pid" % d, "AuthorizedKeysFile %s/authorized_keys" % d, "UsePAM no",
            "StrictModes no", "PasswordAuthentication no", "KbdInteractiveAuthentication no",
            "PubkeyAuthentication yes", "PermitRootLogin no", "AllowUsers %s" % user,
            "AllowTcpForwarding local", "GatewayPorts no", "X11Forwarding no", "PermitTTY no",
            "PrintMotd no", "LogLevel INFO", ""]))
    return user


def ssh_argv(env, user, port):
    d = env.p("ssh")
    return ["ssh", "-F", "/dev/null", "-N", "-o", "BatchMode=yes", "-o", "IdentitiesOnly=yes",
            "-o", "ExitOnForwardFailure=yes", "-o", "ServerAliveInterval=10", "-o", "ServerAliveCountMax=3",
            "-o", "HostKeyAlias=s1c-sshd", "-o", "UserKnownHostsFile=%s/known_hosts" % d,
            "-o", "StrictHostKeyChecking=yes", "-i", d + "/client_key", "-p", str(port),
            "-L", "127.0.0.1:%d:127.0.0.1:%d" % (PORTS["ssh_l"], PORTS["frps"]), "%s@127.0.0.1" % user]


def clean_env():
    """The environment for ssh: no agent socket (the client key comes from scratch only)."""
    e = dict(os.environ)
    e.pop("SSH_AUTH_SOCK", None)
    return e
