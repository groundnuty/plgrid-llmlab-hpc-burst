#!/usr/bin/env bash
# job.sh on stubs (loopback only, Unix sockets under a mktemp dir, TCP ports 30430-30439 only): its exit status, what it
# records, and that nothing is left running. The stubs stand in for what the contract names:
#   s2crouter  worker (a vLLM server on a Unix socket), envoy (product smoke; an arm with READY, router and admin sockets), relay
#   vllm-router, s2cload, analyze.py --check-arm, apptainer, the staged Envoy binary, the venv's python next to VR_BIN
# Each stub answers only what CONTRACT.md says it is asked and fails anything else (a wrong flag, a missing file, an unknown URL),
# so a job.sh that calls them wrongly fails here instead of passing. Readiness is made to need observable state: a worker's
# /health answers before /v1/models lists the served name, and the router's active-worker gauge reads 0 for its first scrapes.
# The expected status of each case is worked out from the contract, never by running job.sh:
#
#   clean run                   exit 0, no failures.txt; every arm has the files, meta.json and check.txt the contract lists;
#                               the workers start in parallel, with the regime's KV override; env.txt carries the contract's
#                               fields and no seeded host, job ID, account or path
#   a client that fails         exit 1, failures.txt names that arm and no other
#   a reset that fails          exit 1, the arm is not measured (no meta.json, SKIPPED)
#   a failing --check-arm       exit 1, failures.txt names the arm, check.txt keeps the FAIL line
#   a refused product smoke     exit 1, the Envoy arms are skipped and named, the vllm-router arms run and pass
#   missing required variables  exit 2, every one named, nothing started
#   S2C_FAULT=client-to-other-router, strip-bound   exit 1, the check of the faulted arm fails (the seam reaches the arm)
#   workers that never go idle  exit 1, the arm is named; budget exhausted: every arm skipped, no worker started; unknown arm
#   hostile version output      env.txt keeps none of it; a venv without python gives "vllm_router unknown" and exit 0
# and finally no stub process is left running.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
CASES=${CASES:-all}   # CASES="clean badreset ..." runs only those sections (to try one change fast); the default is all
want() { [[ $CASES == all || " $CASES " == *" $1 "* ]]; }
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
export STUB_PIDS=$T/pids; : > "$STUB_PIDS"
export STUB_PY=$T/stubs.py
ENVOY_SHA_DIR=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef

cat > "$STUB_PY" <<'PY'
#!/usr/bin/env python3
# One file for every stub process: stubs.py <role> args... (roles: router, vr, load).
import hashlib, http.client, http.server, json, os, re, signal, socket, socketserver, sys, threading, time, urllib.request, zlib

SERVED = "Qwen/Qwen2.5-1.5B-Instruct"   # CONTRACT default for SERVED


def die(code, msg):
    sys.stderr.write("stub: %s\n" % msg)
    sys.exit(code)


def note_pid():
    with open(os.environ["STUB_PIDS"], "a") as f:
        f.write("%d\n" % os.getpid())


def parse_go(args, spec):
    """Go flag style: -name value, -name for a bool (spec[name] False); `--` ends the flags. Unknown flag: usage error 2."""
    out, i = {}, 0
    while i < len(args):
        t = args[i]
        if t == "--":
            return out, args[i + 1:]
        name = t.lstrip("-")
        if not t.startswith("-") or name not in spec:
            die(2, "usage: unknown or misplaced flag %r" % t)
        if spec[name]:
            i += 1
            if i >= len(args):
                die(2, "usage: -%s needs a value" % name)
            out[name] = args[i]
        else:
            out[name] = True
        i += 1
    return out, []


class H(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"
    routes = {}

    def log_message(self, *a):
        pass

    def _go(self, method):
        n = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(n) if n else b""
        fn = self.routes.get((method, self.path))
        code, out = (404, b"not found") if fn is None else fn(self, body)
        self.send_response(code)
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    def do_GET(self):
        self._go("GET")

    def do_POST(self):
        self._go("POST")


class UnixServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True

    def get_request(self):
        req, _ = super().get_request()
        return req, ["uds", 0]


def handler(routes):
    return type("R", (H,), {"routes": routes})


def serve_unix(path, routes):
    srv = UnixServer(path, handler(routes))
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def serve_tcp(host, port, routes):
    if host != "127.0.0.1":
        die(1, "listens on %s: loopback only" % host)
    srv = http.server.ThreadingHTTPServer((host, port), handler(routes))
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def unix_get(path, url_path, timeout=5):
    c = http.client.HTTPConnection("localhost", timeout=timeout)
    c.sock = socket.socket(socket.AF_UNIX)
    c.sock.settimeout(timeout)
    c.sock.connect(path)
    c.request("GET", url_path)
    r = c.getresponse()
    return r.status, r.read()


def wait_forever_until_term():
    signal.signal(signal.SIGTERM, lambda *a: sys.exit(0))
    threading.Event().wait()


# ---------------------------------------------------------------- s2crouter worker
def worker(argv):
    a, vllm = parse_go(argv, {k: True for k in ("index", "workers", "apptainer", "image", "checkpoint", "run-dir", "cache-dir",
                                                 "served", "max-num-seqs", "placement-log", "record")})
    for k in ("index", "workers", "apptainer", "image", "checkpoint", "run-dir", "cache-dir", "served", "max-num-seqs", "placement-log", "record"):
        if k not in a:
            die(2, "worker: -%s is required" % k)
    i, n, run = int(a["index"]), int(a["workers"]), a["run-dir"]
    devs = [d for d in os.environ.get("CUDA_VISIBLE_DEVICES", "").split(",") if d]
    if len(devs) < n:
        die(1, "worker %d: %d CUDA_VISIBLE_DEVICES entries for %d workers" % (i, len(devs), n))
    expect = {"apptainer": "STUB_EXPECT_APPTAINER", "image": "STUB_EXPECT_IMAGE", "checkpoint": "STUB_EXPECT_CKPT"}
    for k, e in expect.items():
        if a[k] != os.environ[e]:
            die(1, "worker %d: -%s is not the staged one" % (i, k))
    if a["served"] != SERVED or a["max-num-seqs"] != "256":
        die(1, "worker %d: -served/-max-num-seqs differ from the contract defaults" % i)
    if not re.fullmatch(r"/hpcburst/run/placement-[ABH]-w%d\.tsv" % i, a["placement-log"]):
        die(1, "worker %d: placement log %r" % (i, a["placement-log"]))
    m = re.fullmatch(r"--num-gpu-blocks-override=(6000|33010)", vllm[-1] if vllm else "")
    want = ["--gpu-memory-utilization=0.6", "--max-model-len=16384", "--enable-prefix-caching", "--enable-prompt-tokens-details",
            "--disable-uvicorn-access-log", "--middleware=s2c_placement.Placement"]
    if not m or vllm[:-1] != want:
        die(1, "worker %d: vllm flags %r differ from the contract's list" % (i, vllm))
    if not os.path.isfile(os.path.join(run, "py", "s2c_placement.py")):
        die(1, "worker %d: the placement middleware is not in <run-dir>/py" % i)
    note_pid()
    open(os.path.join(run, os.path.basename(a["placement-log"])), "a").close()
    with open(a["record"], "w") as f:
        json.dump({"argv": argv, "index": i, "start_unix": time.time(), "kv_blocks": int(m.group(1)), "device": devs[i]}, f)
    time.sleep(float(os.environ.get("STUB_WORKER_DELAY", "0")))   # model load time: nothing listens yet
    state = {"models": 0}
    lock = threading.Lock()

    def models(h, b):
        with lock:
            state["models"] += 1
            k = state["models"]
        return 200, json.dumps({"object": "list", "data": [] if k <= 2 else [{"id": SERVED, "object": "model"}]}).encode()

    def metrics(h, b):
        if state["models"] <= 2:
            return 503, b"model not loaded"   # only /health answers until /v1/models lists the served name
        running = "1.0" if os.environ.get("STUB_BUSY") else "0.0"
        lab = '{engine="0",model_name="m"}'
        return 200, ("vllm:kv_cache_usage_perc%s 0.0\nvllm:num_requests_running%s %s\nvllm:num_requests_waiting%s 0.0\n"
                     "vllm:num_preemptions_total%s 0.0\nvllm:prefix_cache_hits_total%s 1.0\nvllm:prefix_cache_queries_total%s 2.0\n"
                     % (lab, lab, running, lab, lab, lab, lab)).encode()

    def reset(h, b):
        if state["models"] <= 2:
            return 503, b'{"success":false}'
        mode = os.environ.get("STUB_RESET")
        if mode == "http500":
            return 500, b'{"success":true}'   # a body that says yes under a status that says no
        return (500, b'{"success":false}') if mode == "fail" else (200, b'{"success":true}')

    serve_unix(os.path.join(run, "worker-%d.sock" % i), {("GET", "/health"): lambda h, b: (200, b"ok"), ("GET", "/v1/models"): models,
                                                         ("GET", "/metrics"): metrics, ("POST", "/reset_prefix_cache"): reset})
    wait_forever_until_term()


# ---------------------------------------------------------------- s2crouter envoy
FACTOR_PATH = "clusters[0].common_lb_config.consistent_hashing_lb_config.hash_balance_factor"


def envoy(argv):
    a, _ = parse_go(argv, {"run-dir": True, "workers": True, "stage-dir": True, "bound": True, "out": True, "reference": True,
                           "fault": True, "product-smoke": False, "verify-dir": True})
    for k in ("run-dir", "workers", "stage-dir", "out"):
        if k not in a:
            die(2, "envoy: -%s is required" % k)
    run, n, out = a["run-dir"], int(a["workers"]), a["out"]
    if not run.startswith("/"):
        die(1, "envoy: -run-dir must be absolute")
    stage = a["stage-dir"]
    bins = [os.path.join(stage, "envoy", d, "envoy") for d in os.listdir(os.path.join(stage, "envoy"))]
    if not bins or not os.path.isfile(bins[0]):
        die(1, "envoy: no staged binary under -stage-dir")
    sha = hashlib.sha256(open(bins[0], "rb").read()).hexdigest()
    note_pid()
    if a.get("product-smoke"):
        if [f for f in os.listdir(run) if f.startswith("worker-") and f.endswith(".sock")]:
            die(1, "envoy smoke: it must run before the workers start")
        refused = os.environ.get("STUB_SMOKE") == "refused"
        with open(os.path.join(out, "product-start.json"), "w") as f:
            json.dump({"mode": "smoke", "result": "refused: stub" if refused else "started", "envoy_sha256": sha, "glibc": "2.34"}, f, indent=2)
        sys.exit(3 if refused else 0)
    if "bound" not in a:
        die(2, "envoy: -bound is required")
    bound, fault = a["bound"], a.get("fault")
    if bound != "none" and not bound.isdigit():
        die(2, "envoy: -bound %r" % bound)
    if fault not in (None, "strip-bound", "ignore-verify"):
        die(2, "envoy: unknown -fault")
    if "reference" in a:
        if json.load(open(a["reference"])).get("result") != "started":
            die(1, "envoy: the product smoke did not start Envoy: no arm runs it")
    deadline = time.time() + 30
    while True:
        try:
            if all(unix_get(os.path.join(run, "worker-%d.sock" % i), "/health")[0] == 200 for i in range(n)):
                break
        except OSError:
            pass
        if time.time() > deadline:
            die(1, "envoy: the workers never became healthy")
        time.sleep(0.1)
    factor = None if bound == "none" else int(bound)
    cfg = {"lb": "ring_hash"}
    if factor is not None and fault != "strip-bound":
        cfg["hash_balance_factor"] = factor
    run_bytes = json.dumps(cfg, sort_keys=True).encode()
    open(os.path.join(out, "rendered.json"), "w").write(json.dumps({"lb": "ring_hash", "hash_balance_factor": 200}))
    open(os.path.join(out, "run.json"), "wb").write(run_bytes)
    json.dump({"mode": "replica", "bound": bound, "factor": factor, "config_sha256": hashlib.sha256(run_bytes).hexdigest(), "pid": os.getpid(),
               "argv": [], "env": [], "diff": [FACTOR_PATH], "envoy_sha256": sha, "glibc": "2.34", "stub_args": argv},
              open(os.path.join(out, "start.json"), "w"))
    counts = {i: 0 for i in range(n)}
    nxt = [0]
    lock = threading.Lock()

    def chat(h, b):
        with lock:
            counts[nxt[0] % n] += 1
            nxt[0] += 1
        return 200, b'{"choices":[]}'

    def clusters(h, b):
        hosts = []
        for i in range(n):
            st = [{"name": "cx_total", "value": "1"}]
            if counts[i]:
                st.append({"name": "rq_total", "value": str(counts[i])})
            else:
                st.append({"name": "rq_total"})   # Envoy leaves the value out for zero
            hosts.append({"address": {"pipe": {"path": os.path.join(run, "worker-%d.sock" % i)}}, "stats": st,
                          "health_status": {"eds_health_status": "HEALTHY"}, "hostname": "worker-%d" % i})
        return 200, json.dumps({"cluster_statuses": [{"name": "workers", "host_statuses": hosts}]}).encode()

    serve_unix(os.path.join(run, "router.sock"), {("POST", "/v1/chat/completions"): chat})
    serve_unix(os.path.join(run, "envoy-admin.sock"), {("GET", "/clusters?format=json"): clusters})
    open(os.path.join(out, "READY"), "w").close()
    wait_forever_until_term()


# ---------------------------------------------------------------- s2crouter relay
def relay(argv):
    a, _ = parse_go(argv, {"map": True, "counts": True})
    if "map" not in a or "counts" not in a:
        die(2, "relay: -map and -counts are required")
    note_pid()
    listeners, counts = [], {}
    lock = threading.Lock()

    def pipe(src, dst):
        try:
            while True:
                d = src.recv(65536)
                if not d:
                    break
                dst.sendall(d)
        except OSError:
            pass
        try:
            dst.shutdown(socket.SHUT_WR)
        except OSError:
            pass

    def accept(ls, addr, sock):
        while True:
            c, _ = ls.accept()
            with lock:
                counts[addr] += 1
            u = socket.socket(socket.AF_UNIX)
            try:
                u.connect(sock)
            except OSError:
                c.close()
                continue
            threading.Thread(target=pipe, args=(c, u), daemon=True).start()
            threading.Thread(target=pipe, args=(u, c), daemon=True).start()

    for part in a["map"].split(","):
        addr, sock = part.split("=", 1)
        host, port = addr.rsplit(":", 1)
        if host != "127.0.0.1":
            die(1, "relay listens on %s: loopback only" % host)
        ls = socket.socket()
        ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ls.bind((host, int(port)))
        ls.listen(16)
        counts[addr] = 0
        listeners.append(ls)
        threading.Thread(target=accept, args=(ls, addr, sock), daemon=True).start()

    def term(*_):
        with open(a["counts"], "w") as f:
            for addr, c in counts.items():
                f.write("%s\t%d\n" % (addr, c))
        os._exit(0)

    signal.signal(signal.SIGTERM, term)
    open(a["counts"] + ".ready", "w").close()
    threading.Event().wait()


# ---------------------------------------------------------------- s2crouter probe
def probe(argv):
    """Kills the victim (SIGKILL through a pidfd), writes <out>/killed, waits for the job to restart it (its /health
    again), flags it verified, and writes probe.json: a passing verdict, or with STUB_PROBE=fail a failing one, exit 1."""
    a, _ = parse_go(argv, {k: True for k in ("run-dir", "workers", "victim", "victim-pid", "served", "verify-dir", "out", "arm",
                                             "placement", "hold", "stream-tokens")})
    for k in ("run-dir", "workers", "victim", "victim-pid", "served", "verify-dir", "out", "arm", "placement"):
        if k not in a:
            die(2, "probe: -%s is required" % k)
    note_pid()
    v, run = int(a["victim"]), a["run-dir"]
    flag = os.path.join(a["verify-dir"], "worker-%d.verified" % v)
    if not os.path.exists(flag):
        die(1, "probe: the victim was not verified before the kill")
    os.remove(flag)
    fd = os.pidfd_open(int(a["victim-pid"]))
    signal.pidfd_send_signal(fd, signal.SIGKILL)
    os.close(fd)
    open(os.path.join(a["out"], "killed"), "w").close()
    deadline = time.time() + 30
    while True:
        try:
            if unix_get(os.path.join(run, "worker-%d.sock" % v), "/health")[0] == 200:
                break
        except OSError:
            pass
        if time.time() > deadline:
            die(1, "probe: the victim never came back")
        time.sleep(0.1)
    open(flag, "w").close()
    bad = os.environ.get("STUB_PROBE") == "fail"
    checks = ["ok the victim was killed mid-stream", "ok siblings' streams uncut",
              "FAIL no 503 while the siblings are healthy: 1 of 9 probe requests failed" if bad else "ok no 503 while the siblings are healthy",
              "ok nothing reaches the restarted worker before its verification", "ok the verified worker takes its key's requests again"]
    json.dump({"victim": "worker-%d" % v, "checks": checks, "stub_args": argv}, open(os.path.join(a["out"], "probe.json"), "w"))
    print("\n".join(checks))
    sys.exit(1 if bad else 0)


def router(argv):
    {"worker": worker, "envoy": envoy, "relay": relay, "probe": probe}.get(argv[0] if argv else "", lambda _: die(2, "usage: s2crouter worker|envoy|relay|probe"))(argv[1:])


# ---------------------------------------------------------------- vllm-router
def vr(argv):
    args, key = {}, None
    for t in argv:
        if t.startswith("--"):
            key = t[2:]
            args[key] = []
        elif key is None:
            die(2, "vllm-router: stray argument %r" % t)
        else:
            args[key].append(t)
    known = {"host", "port", "worker-urls", "policy", "request-timeout-secs", "prometheus-host", "prometheus-port"}
    if set(args) != known:
        die(2, "vllm-router: flags %s, want %s" % (sorted(args), sorted(known)))
    one = {k: v[0] for k, v in args.items() if k != "worker-urls" and len(v) == 1}
    if len(one) != 6:
        die(2, "vllm-router: every flag but --worker-urls takes one value")
    base = int(os.environ["STUB_BASE"])
    nwork = int(os.environ.get("STUB_NWORK", "4"))
    want_urls = ["http://127.0.0.1:%d" % (base + 2 + i) for i in range(nwork)]
    if (one["host"], one["prometheus-host"]) != ("127.0.0.1", "127.0.0.1") or one["request-timeout-secs"] != "10800":
        die(1, "vllm-router: host, prometheus host or request timeout differ from the contract")
    if int(one["port"]) != base or int(one["prometheus-port"]) != base + 1 or args["worker-urls"] != want_urls:
        die(1, "vllm-router: ports or worker urls are not the first block's (RPORT, VR_PROM, one relay port per worker)")
    pol = one["policy"]
    if pol not in ("round_robin", "consistent_hash"):
        die(1, "vllm-router: policy %r" % pol)
    note_pid()
    urls = args["worker-urls"]
    proc = {u: 0 for u in urls}
    lock = threading.Lock()
    st = {"scrapes": 0, "rr": 0, "ready": False}

    def chat(h, b):
        with lock:
            if not st["ready"]:
                return 503, b"not ready"   # until a scrape has shown every worker active
            if pol == "round_robin":
                u = urls[st["rr"] % len(urls)]
                st["rr"] += 1
            else:
                u = urls[zlib.crc32((h.headers.get("x-session-id") or "").encode()) % len(urls)]
            proc[u] += 1
        return 200, b'{"choices":[]}'

    def healthy(u):
        try:
            return urllib.request.urlopen(u + "/health", timeout=5).status == 200
        except OSError:
            return False

    def metrics(h, b):
        with lock:
            st["scrapes"] += 1
            first = st["scrapes"] <= 2
        active = 0 if first else sum(1 for u in urls if healthy(u))   # the workers are reached through the relays
        if active == len(urls):
            with lock:
                st["ready"] = True
        out = ["# TYPE vllm_router_processed_requests_total counter"]
        out += ['vllm_router_processed_requests_total{worker="%s"} %d' % (u, proc[u]) for u in urls]
        out += ['vllm_router_policy_decisions_total{worker="%s",policy="%s"} %d' % (u, pol, proc[u]) for u in urls]
        out.append("vllm_router_active_workers %d" % active)
        return 200, ("\n".join(out) + "\n").encode()

    print("vllm-router stub: %s policy over %d workers" % (pol, len(urls)), flush=True)
    serve_tcp("127.0.0.1", int(one["port"]), {("GET", "/health"): lambda h, b: (200, b"ok"), ("POST", "/v1/chat/completions"): chat})
    serve_tcp("127.0.0.1", int(one["prometheus-port"]), {("GET", "/metrics"): metrics})
    wait_forever_until_term()


# ---------------------------------------------------------------- s2cload
def load(argv):
    a, i = {}, 0
    while i < len(argv):
        t = argv[i]
        if not t.startswith("-") or i + 1 >= len(argv):
            die(2, "s2cload: bad argument %r" % t)
        a[t[1:]] = argv[i + 1]
        i += 2
    for k in ("url", "model", "run", "out"):
        if k not in a:
            die(2, "s2cload: -%s is required" % k)
    run = a["run"]
    if a["model"] != SERVED or not re.fullmatch(r"[ABH][0-9]+-[a-z-]+", run) or not os.path.isdir(os.path.dirname(a["out"])):
        die(1, "s2cload: -model, -run or -out differ from the contract")
    m = re.fullmatch(r"http://([^/]+)(/v1/chat/completions)", a["url"])
    if not m:
        die(1, "s2cload: -url %r" % a["url"])
    if "unix" in a:
        if m.group(1) != "localhost":
            die(1, "s2cload: with -unix the url host is localhost")
    elif m.group(1) != "127.0.0.1:%s" % os.environ["STUB_BASE"]:
        die(1, "s2cload: the url must be http://127.0.0.1:RPORT/... of the first free block")
    nreq = int(os.environ.get("STUB_REQS", "3"))
    ok = 0
    recs = []
    if run != os.environ.get("STUB_FAIL_RUN"):
        for turn in range(nreq):
            body = json.dumps({"model": a["model"], "messages": [{"role": "user", "content": "[s2b:%s:0:%d] hi" % (run, turn)}]})
            hdr = {"Content-Type": "application/json", "x-session-id": "s%d" % turn}
            if "unix" in a:
                c = http.client.HTTPConnection("localhost", timeout=10)
                c.sock = socket.socket(socket.AF_UNIX)
                c.sock.settimeout(10)
                c.sock.connect(a["unix"])
            else:
                host, port = m.group(1).split(":")
                c = http.client.HTTPConnection(host, int(port), timeout=10)
            try:
                c.request("POST", m.group(2), body, hdr)
                status = c.getresponse().status
            except OSError:
                status = 0
            ok += status == 200
            recs.append({"run": run, "session": turn, "turn": 0, "status": status, "attempt": 1})
    if run == os.environ.get("STUB_EXTRA_RECORD"):
        recs.append({"run": run, "session": 99, "turn": 0, "status": 200, "attempt": 1})   # a record no request stands behind
    with open(a["out"], "w") as f:
        for r in recs:
            f.write(json.dumps(r) + "\n")
    failed = (nreq - ok) if run != os.environ.get("STUB_FAIL_RUN") else 2
    aborted = 1 if failed else 0
    print("RESULT run=%s sessions=%d turns=1 concurrency=1 wall_s=0.1 failed_attempts=%d aborted_sessions=%d" % (run, nreq, failed, aborted))
    sys.exit(3 if failed else 0)


if __name__ == "__main__":
    {"router": router, "vr": vr, "load": load}[sys.argv[1]](sys.argv[2:])
PY

cat > "$T/analyze.py" <<'PY'
#!/usr/bin/env python3
# Stub of analyze.py --check-arm, written from CONTRACT.md's "Checks" and "Per-arm directory". From files only.
import hashlib, json, os, re, sys

if len(sys.argv) != 3 or sys.argv[1] != "--check-arm" or not os.path.isdir(sys.argv[2]):
    sys.stderr.write("usage: analyze.py --check-arm ARMDIR\n")
    sys.exit(2)
d = os.path.abspath(sys.argv[2])
top = os.path.dirname(os.path.dirname(d))   # results/arms/<name> -> results
bad = []
lines = []


def check(name, ok, why=""):
    lines.append("ok %s" % name if ok else "FAIL %s: %s" % (name, why))
    if not ok:
        bad.append(name)


def rd(p):
    with open(os.path.join(d, p)) as f:
        return f.read()


meta = json.loads(rd("meta.json"))
keys = {"name", "regime", "rep", "arm", "router", "policy", "bound", "kv_blocks", "start_unix", "end_unix", "client_rc", "nwork", "load_args", "mode", "reset"}
table = {"rr": ("vllm-router", "round_robin", None), "vr-hash": ("vllm-router", "consistent_hash", None),
         "envoy-bound": ("envoy", None, "125"), "envoy-nobound": ("envoy", None, "none")}
arm = meta.get("arm")
check("meta-keys", set(meta) == keys, "keys %s" % sorted(set(meta) ^ keys))
check("meta-values", arm in table and (meta["router"], meta["policy"], meta["bound"]) == table[arm]
      and meta["name"] == "%s%s-%s" % (meta["regime"], meta["rep"], arm) == os.path.basename(d)
      and isinstance(meta["rep"], int) and isinstance(meta["kv_blocks"], int)
      and meta["kv_blocks"] == (6000 if meta["regime"] in ("B", "H") else 33010) and meta["nwork"] == 4
      and meta["reset"] == "post" and meta["mode"] in ("double", "vllm") and meta["end_unix"] >= meta["start_unix"], "meta %s" % meta)
nw = meta["nwork"]
check("worker-scrapes", all(os.path.getsize(os.path.join(d, "%s-w%d.prom" % (t, i))) > 0 for t in ("before", "after") for i in range(nw)), "a worker scrape is missing")
records = len([l for l in rd("client.jsonl").splitlines() if l.strip()])


def prom_delta(fn):
    out = {}
    for f, sign in (("vr-after.prom", 1), ("vr-before.prom", -1)):
        for m in re.finditer(r'^%s\{worker="http://127\.0\.0\.1:(\d+)"(?:,policy="(\w+)")?\} (\d+)$' % fn, rd(f), re.M):
            out[m.group(1)] = out.get(m.group(1), 0) + sign * int(m.group(3))
    return out


def clusters_total(f):
    j = json.loads(rd(f))
    return sum(int(next((s.get("value", "0") for s in h["stats"] if s["name"] == "rq_total"), "0"))
               for c in j["cluster_statuses"] if c["name"] == "workers" for h in c["host_statuses"])


have_vr = os.path.exists(os.path.join(d, "vr-after.prom"))
have_cl = os.path.exists(os.path.join(d, "clusters-after.json"))
vr_growth = sum(prom_delta("vllm_router_processed_requests_total").values()) if have_vr else 0
cl_growth = clusters_total("clusters-after.json") - clusters_total("clusters-before.json") if have_cl else 0
if meta["router"] == "envoy":
    check("router-count", cl_growth == records, "Envoy counted %d, the client has %d records" % (cl_growth, records))
    run = json.loads(rd("envoy/run.json"))
    start = json.loads(rd("envoy/start.json"))
    check("envoy-config", hashlib.sha256(open(os.path.join(d, "envoy/run.json"), "rb").read()).hexdigest() == start["config_sha256"]
          and run.get("hash_balance_factor") == (None if meta["bound"] == "none" else int(meta["bound"])) and len(start["diff"]) <= 1,
          "run.json holds factor %r, meta says bound %r" % (run.get("hash_balance_factor"), meta["bound"]))
    check("other-router", vr_growth == 0, "vllm-router counted %d requests during an Envoy arm" % vr_growth)
else:
    mp = {l.split("\t")[0]: int(l.split("\t")[1]) for l in rd("relay-map.tsv").split("\n") if l}
    check("router-count", vr_growth == records and set(mp) == set(prom_delta("vllm_router_processed_requests_total")),
          "vllm-router counted %d, the client has %d records" % (vr_growth, records))
    pd = prom_delta("vllm_router_policy_decisions_total")
    labels = set(re.findall(r'policy_decisions_total\{worker="[^"]*",policy="(\w+)"\}', rd("vr-after.prom")))
    check("policy-label", labels <= {meta["policy"]}, "labels %s" % sorted(labels))
    check("relay-counts", len([l for l in rd("relay-counts.tsv").split("\n") if l]) == nw, "relay-counts.tsv")
    check("other-router", cl_growth == 0, "Envoy counted %d requests during a vllm-router arm" % cl_growth)
print("\n".join(lines))
sys.exit(1 if bad else 0)
PY
chmod +x "$STUB_PY" "$T/analyze.py"

# ---- the staged tree and the binaries job.sh calls ----
STAGE=$T/stage
mkdir -p "$STAGE"/{bin,py,scripts,envoy/$ENVOY_SHA_DIR} "$T/bin" "$T/vr" "$T/vr2" "$T/img" "$T/ckpt"
for role in s2crouter:router s2cload:load; do
  printf '#!/bin/bash\nexec python3 "$STUB_PY" %s "$@"\n' "${role#*:}" > "$STAGE/bin/${role%%:*}"
done
cp "$T/analyze.py" "$STAGE/scripts/analyze.py"
echo "# placeholder middleware" > "$STAGE/py/s2c_placement.py"
printf '#!/bin/bash\n[ "$1" = --version ] && echo "${STUB_ENVOY_VERSION:-envoy  version: 0a1b2c3d4e5f60718293a4b5c6d7e8f901234567/1.40.2/Clean/RELEASE/BoringSSL}" || exit 2\n' > "$STAGE/envoy/$ENVOY_SHA_DIR/envoy"
printf '#!/bin/bash\n[ "$1" = --version ] && echo "${STUB_APPTAINER_VERSION:-apptainer version 1.3.6-1.el9}" || exit 2\n' > "$T/bin/apptainer"
printf '#!/bin/bash\nexec python3 "$STUB_PY" vr "$@"\n' > "$T/vr/vllm-router"; cp "$T/vr/vllm-router" "$T/vr2/vllm-router"
# the venv's python, next to VR_BIN: it answers only the metadata query job.sh is told to make
cat > "$T/vr/python" <<'SH'
#!/bin/bash
[ "$1" = -I ] && [ "$2" = -c ] && [[ $3 == *'m.version("vllm-router")'* ]] || { echo "stub python: unexpected call" >&2; exit 2; }
echo "vllm_router 0.1.17"
SH
chmod +x "$STAGE"/bin/* "$STAGE/envoy/$ENVOY_SHA_DIR/envoy" "$T/bin/apptainer" "$T/vr/vllm-router" "$T/vr2/vllm-router" "$T/vr/python"

# run_job <case> <plan> [VAR=value...]: runs job.sh against the stubs; sets RC and OUT (the results directory) and ERR (stderr file).
BASE_PORT=30430
JOB_PREFIX=()   # a command to run job.sh under (the termed case runs it under timeout)
run_job() {
  local name=$1 plan=$2; shift 2
  mkdir -p "$T/$name"
  env TMPDIR="$T/$name" S2C_HOME="$T/$name/h" RESULT_TAG=t MODE=double STAGE="$STAGE" IMAGE="$T/img" CHECKPOINT_DIR="$T/ckpt" VR_BIN="$T/vr/vllm-router" \
    APPTAINER="$T/bin/apptainer" PORT_BASE=$BASE_PORT PLAN="$plan" LOAD_ARGS="-sessions 3 -turns 1" \
    POLL_S=0.2 SAMPLE_S=0.5 WORKER_WAIT_S=30 ARM_MAX_S=30 DRAIN_WAIT_S=10 ENVOY_WAIT_S=30 VR_WAIT_S=30 RELAY_WAIT_S=20 \
    CUDA_VISIBLE_DEVICES=0,1,2,3 SLURM_CPUS_PER_TASK=64 SLURM_MEM_PER_NODE=512000 SLURM_JOB_ID=7654321 SLURM_JOB_ACCOUNT=seedacct-x1 \
    STUB_EXPECT_APPTAINER="$T/bin/apptainer" STUB_EXPECT_IMAGE="$T/img" STUB_EXPECT_CKPT="$T/ckpt" STUB_BASE=$BASE_PORT \
    "$@" ${JOB_PREFIX[@]+"${JOB_PREFIX[@]}"} bash "$HERE/job.sh" > "$T/$name.log" 2> "$T/$name.err"
  RC=$?; OUT=$T/$name/h/results/t; ERR=$T/$name.err
}
fails=0
check() { if (eval "$2"); then echo "ok   $1"; else echo "FAIL $1"; fails=$((fails + 1)); fi; }   # a subshell: an eval'd `exit 1` ends the check, not the test
leftovers() { local pid n=0; while read -r pid; do [[ -n $pid ]] && kill -0 "$pid" 2> /dev/null && n=$((n + 1)); done < "$STUB_PIDS"; echo $n; }
# meta <arm dir> <key>: one field of meta.json, as JSON text
meta() { python3 -I -c 'import json, sys; print(json.dumps(json.load(open(sys.argv[1] + "/meta.json"))[sys.argv[2]]))' "$1" "$2"; }

# ===================================================================== clean run
if want clean; then
PLAN4="B:1:rr B:1:envoy-bound B:1:envoy-nobound B:1:vr-hash A:1:rr"
run_job clean "$PLAN4" STUB_WORKER_DELAY=1.5
check "clean run exits 0" '[[ $RC == 0 ]]'
check "clean run writes no failures.txt" '[[ ! -e $OUT/failures.txt ]]'
for a in B1-rr B1-envoy-bound B1-envoy-nobound B1-vr-hash A1-rr; do
  check "$a: every file the contract lists for any arm" 'for f in meta.json reset.txt before-w0.prom before-w3.prom after-w0.prom after-w3.prom client.jsonl client.out ps.txt check.txt; do [[ -s $OUT/arms/$a/$f ]] || exit 1; done; [[ -s $OUT/arms/$a/samples.tsv && ! -e $OUT/arms/$a/SKIPPED ]]'
  check "$a: reset.txt has 4 answers, each HTTP 200 with success true" '[[ $(grep -c "success\":true} http=200" "$OUT/arms/$a/reset.txt") == 4 ]]'
  check "$a: check.txt is the analyzer output and every check is ok" 'grep -q "^ok router-count" "$OUT/arms/$a/check.txt" && ! grep -q "^FAIL" "$OUT/arms/$a/check.txt"'
done
for a in B1-rr B1-vr-hash A1-rr; do
  check "$a: vllm-router files: vr-before/after.prom, relay-map.tsv, relay-counts.tsv, router.log" 'for f in vr-before.prom vr-after.prom relay-map.tsv relay-counts.tsv router.log; do [[ -s $OUT/arms/$a/$f ]] || exit 1; done; [[ ! -e $OUT/arms/$a/clusters-after.json ]]'
  check "$a: relay-map.tsv is <relay port><TAB><worker>, ports from the first free block" '[[ $(cat "$OUT/arms/$a/relay-map.tsv") == "$(printf "30432\t0\n30433\t1\n30434\t2\n30435\t3")" ]]'
done
for a in B1-envoy-bound B1-envoy-nobound; do
  check "$a: Envoy files: clusters-before/after.json and the envoy/ records, no vllm-router files" 'for f in clusters-before.json clusters-after.json envoy/run.json envoy/start.json; do [[ -s $OUT/arms/$a/$f ]] || exit 1; done; [[ -e $OUT/arms/$a/envoy/READY && ! -e $OUT/arms/$a/vr-after.prom ]]'
done
check "meta B1-rr: policy round_robin, bound null, 6000 blocks, number types" '[[ $(meta "$OUT/arms/B1-rr" policy) == "\"round_robin\"" && $(meta "$OUT/arms/B1-rr" bound) == null && $(meta "$OUT/arms/B1-rr" kv_blocks) == 6000 && $(meta "$OUT/arms/B1-rr" rep) == 1 && $(meta "$OUT/arms/B1-rr" router) == "\"vllm-router\"" ]]'
check "meta B1-vr-hash: policy consistent_hash" '[[ $(meta "$OUT/arms/B1-vr-hash" policy) == "\"consistent_hash\"" ]]'
check "meta B1-envoy-bound: router envoy, bound the string 125, policy null" '[[ $(meta "$OUT/arms/B1-envoy-bound" router) == "\"envoy\"" && $(meta "$OUT/arms/B1-envoy-bound" bound) == "\"125\"" && $(meta "$OUT/arms/B1-envoy-bound" policy) == null ]]'
check "meta B1-envoy-nobound: bound the string none" '[[ $(meta "$OUT/arms/B1-envoy-nobound" bound) == "\"none\"" ]]'
check "meta A1-rr: regime A with 33010 blocks; mode double; reset post; nwork 4; client_rc 0" '[[ $(meta "$OUT/arms/A1-rr" regime) == "\"A\"" && $(meta "$OUT/arms/A1-rr" kv_blocks) == 33010 && $(meta "$OUT/arms/A1-rr" mode) == "\"double\"" && $(meta "$OUT/arms/A1-rr" reset) == "\"post\"" && $(meta "$OUT/arms/A1-rr" nwork) == 4 && $(meta "$OUT/arms/A1-rr" client_rc) == 0 ]]'
check "workers got the regime's KV override (B: 6000, then A: 33010 after the restart)" 'grep -q -- "--num-gpu-blocks-override=6000" "$OUT/worker-B-w2.json" && grep -q -- "--num-gpu-blocks-override=33010" "$OUT/worker-A-w2.json" && ! grep -q 33010 "$OUT/worker-B-w2.json"'
check "each worker was given the I-th entry of CUDA_VISIBLE_DEVICES and its own placement log" '[[ $(python3 -I -c "import json,sys; print(json.load(open(sys.argv[1]))[\"device\"])" "$OUT/worker-A-w3.json") == 3 ]] && grep -q "placement-A-w3.tsv" "$OUT/worker-A-w3.json"'
check "placement logs per regime and worker were copied to the results" '[[ -e $OUT/placement-B-w0.tsv && -e $OUT/placement-B-w3.tsv && -e $OUT/placement-A-w0.tsv ]]'
check "the four workers were started in parallel (start times within 1 s while each took 1.5 s to listen)" '[[ $(python3 -I -c "
import json, sys
t = [json.load(open(\"%s/worker-B-w%d.json\" % (sys.argv[1], i)))[\"start_unix\"] for i in range(4)]
print(1 if max(t) - min(t) < 1.0 else 0)" "$OUT") == 1 ]]'
check "product-start.json is at the top level and says started" 'grep -q "\"result\": \"started\"" "$OUT/product-start.json"'
check "the Envoy arms were given the product smoke record as -reference" 'grep -q "reference" "$OUT/arms/B1-envoy-bound/envoy/start.json" && grep -q "product-start.json" "$OUT/arms/B1-envoy-bound/envoy/start.json"'
check "the Envoy arm ran with -bound 125, the nobound arm with -bound none" 'python3 -I -c "
import json, sys
a = json.load(open(sys.argv[1]))[\"stub_args\"]; b = json.load(open(sys.argv[2]))[\"stub_args\"]
sys.exit(0 if a[a.index(\"-bound\") + 1] == \"125\" and b[b.index(\"-bound\") + 1] == \"none\" and \"-fault\" not in a + b else 1)" "$OUT/arms/B1-envoy-bound/envoy/start.json" "$OUT/arms/B1-envoy-nobound/envoy/start.json"'
# env.txt: the contract's fields, from independent sources, and nothing private
GLIBC=$(getconf GNU_LIBC_VERSION | awk '{print $2}')
ENVOY_SHA=$(sha256sum "$STAGE/envoy/$ENVOY_SHA_DIR/envoy" | awk '{print $1}')
check "env.txt: mode, arch, kernel, nproc, glibc version, Slurm cpus and memory, GPU entry count" 'grep -q "^mode=double arch=$(uname -m) kernel=$(uname -r) nproc=$(nproc) glibc=$GLIBC\$" "$OUT/env.txt" && grep -q "^slurm_cpus=64 slurm_mem_per_node=512000 gpu_entries=4\$" "$OUT/env.txt"'
check "env.txt: apptainer version, the Envoy binary's sha256 and version, vllm-router from the venv's python" 'grep -q "^apptainer: apptainer version 1.3.6-1.el9\$" "$OUT/env.txt" && grep -q "^envoy_sha256=$ENVOY_SHA\$" "$OUT/env.txt" && grep -q "^envoy: version: 0a1b2c3d4e5f60718293a4b5c6d7e8f901234567/1.40.2/Clean/RELEASE/BoringSSL\$" "$OUT/env.txt" && grep -q "^vllm_router 0.1.17\$" "$OUT/env.txt"'
check "env.txt: no host name, account, job ID, user name or path" '! grep -q -F -e "$T" -e 7654321 -e seedacct-x1 -e "$(hostname)" -e "$(id -un)" "$OUT/env.txt"'
check "no stub process is left after the clean run" '[[ $(leftovers) == 0 ]]'

fi

# ===================================================================== a client that fails
if want badclient; then
run_job badclient "B:1:rr B:1:vr-hash B:1:envoy-bound" STUB_FAIL_RUN=B1-vr-hash
check "a failing client exits 1" '[[ $RC == 1 ]]'
check "failures.txt names the failed client's arm" 'grep -q "B1-vr-hash: client exit status 3" "$OUT/failures.txt"'
check "the arms that succeeded are not in failures.txt" '! grep -q -e "B1-rr" -e "B1-envoy-bound" "$OUT/failures.txt"'
check "the failed arm keeps its meta.json with client_rc 3; the good arm has 0" '[[ $(meta "$OUT/arms/B1-vr-hash" client_rc) == 3 && $(meta "$OUT/arms/B1-rr" client_rc) == 0 ]]'

fi

# ===================================================================== a reset that fails
if want badreset; then
run_job badreset "B:1:rr" STUB_RESET=fail
check "a failed reset exits 1" '[[ $RC == 1 ]]'
check "the arm was not measured on an unreset cache: no meta.json, SKIPPED says so" '[[ ! -e $OUT/arms/B1-rr/meta.json && -s $OUT/arms/B1-rr/SKIPPED ]]'
check "failures.txt names the arm and the reset" 'grep -q "B1-rr skipped: prefix-cache reset failed" "$OUT/failures.txt"'
check "the failed answers are in reset.txt" 'grep -q "http=500" "$OUT/arms/B1-rr/reset.txt"'
run_job badreset2 "B:1:rr" STUB_RESET=http500
check "a reset that answers success true under HTTP 500 is a failed reset: exit 1, arm not measured" '[[ $RC == 1 && ! -e $OUT/arms/B1-rr/meta.json ]] && grep -q "B1-rr skipped: prefix-cache reset failed" "$OUT/failures.txt"'

fi

# ===================================================================== a failing --check-arm
if want badcheck; then
run_job badcheck "B:1:rr B:1:vr-hash" STUB_EXTRA_RECORD=B1-vr-hash
check "a failing --check-arm exits 1" '[[ $RC == 1 ]]'
check "failures.txt names the arm and the check" 'grep -q "B1-vr-hash: analyze.py --check-arm exit 1" "$OUT/failures.txt" && ! grep -q "B1-rr" "$OUT/failures.txt"'
check "check.txt keeps the analyzer's FAIL line; the other arm's check is clean" 'grep -q "^FAIL router-count" "$OUT/arms/B1-vr-hash/check.txt" && ! grep -q "^FAIL" "$OUT/arms/B1-rr/check.txt"'

fi

# ===================================================================== a refused product smoke
if want nosmoke; then
run_job nosmoke "B:1:rr B:1:envoy-bound B:1:envoy-nobound B:1:vr-hash" STUB_SMOKE=refused
check "a refused product smoke exits 1" '[[ $RC == 1 ]]'
check "both Envoy arms are skipped for the refusal and named in failures.txt" 'grep -q "B1-envoy-bound skipped: product smoke refused" "$OUT/failures.txt" && grep -q "B1-envoy-nobound skipped: product smoke refused" "$OUT/failures.txt" && grep -q "product smoke refused" "$OUT/arms/B1-envoy-bound/SKIPPED" && [[ -s $OUT/arms/B1-envoy-nobound/SKIPPED && ! -e $OUT/arms/B1-envoy-bound/meta.json ]]'
check "the vllm-router arms still ran, passed their checks and are not in failures.txt" '[[ -s $OUT/arms/B1-rr/meta.json && -s $OUT/arms/B1-vr-hash/meta.json ]] && ! grep -q -e "B1-rr" -e "B1-vr-hash" "$OUT/failures.txt" && ! grep -q "^FAIL" "$OUT/arms/B1-rr/check.txt" "$OUT/arms/B1-vr-hash/check.txt"'
check "the refusal is recorded in product-start.json" 'grep -q "refused" "$OUT/product-start.json"'

fi

# ===================================================================== required variables missing
if want noenv; then
pids_before=$(wc -l < "$STUB_PIDS")
run_job noenv "B:1:rr" S2C_HOME= IMAGE= VR_BIN=
check "missing required variables: exit 2, every missing one named at once" '[[ $RC == 2 ]] && grep -q "S2C_HOME" "$ERR" && grep -q "IMAGE" "$ERR" && grep -q "VR_BIN" "$ERR" && ! grep -q "CHECKPOINT_DIR" "$ERR"'
check "missing required variables: nothing was started" '[[ ! -e $T/noenv/s2c-run && $(wc -l < "$STUB_PIDS") == "$pids_before" ]]'

fi

# ===================================================================== the fault seams
if want faults; then
run_job faultclient "B:1:envoy-bound" S2C_FAULT=client-to-other-router
check "client-to-other-router: exit 1" '[[ $RC == 1 ]]'
check "client-to-other-router: the vllm-router stack was started in the Envoy arm" '[[ -s $OUT/arms/B1-envoy-bound/vr-after.prom && -s $OUT/arms/B1-envoy-bound/clusters-after.json ]]'
check "client-to-other-router: the arm's check fails and failures.txt records it" 'grep -q "^FAIL router-count" "$OUT/arms/B1-envoy-bound/check.txt" && grep -q "^FAIL other-router" "$OUT/arms/B1-envoy-bound/check.txt" && grep -q "B1-envoy-bound: analyze.py --check-arm exit 1" "$OUT/failures.txt"'
run_job faultbound "B:1:envoy-bound B:1:envoy-nobound" S2C_FAULT=strip-bound
check "strip-bound: exit 1" '[[ $RC == 1 ]]'
check "strip-bound: Envoy was started with -fault strip-bound for the bounded arm only" 'grep -q "strip-bound" "$OUT/arms/B1-envoy-bound/envoy/start.json" && ! grep -q "strip-bound" "$OUT/arms/B1-envoy-nobound/envoy/start.json"'
check "strip-bound: the bounded arm fails its config check and only that arm is in failures.txt" 'grep -q "^FAIL envoy-config" "$OUT/arms/B1-envoy-bound/check.txt" && grep -q "B1-envoy-bound: analyze.py --check-arm exit 1" "$OUT/failures.txt" && ! grep -q "B1-envoy-nobound" "$OUT/failures.txt"'
run_job badfault "B:1:rr" S2C_FAULT=no-such-fault
check "an unknown S2C_FAULT is refused with exit 2 before anything starts" '[[ $RC == 2 && ! -e $OUT/arms ]]'

fi

# ===================================================================== workers that never go idle, the budget, an unknown arm
if want misc; then
run_job busy "B:1:rr" STUB_BUSY=1 DRAIN_WAIT_S=2
check "workers that never go idle: exit 1 naming the arm" '[[ $RC == 1 ]] && grep -q "B1-rr: workers still had running requests" "$OUT/failures.txt"'
run_job budget "B:1:rr B:1:vr-hash" BUDGET_S=0
check "budget exhausted: exit 1, every arm skipped and named, no worker was started" '[[ $RC == 1 ]] && grep -q "B1-rr skipped: budget" "$OUT/failures.txt" && grep -q "B1-vr-hash skipped: budget" "$OUT/failures.txt" && [[ ! -e $OUT/worker-B-w0.json ]]'
run_job unknown "B:1:nope B:1:rr"
check "an unknown arm is skipped and named; the next arm still runs" '[[ $RC == 1 ]] && grep -q "B1-nope skipped: unknown arm" "$OUT/failures.txt" && [[ -s $OUT/arms/B1-rr/meta.json ]] && ! grep -q "B1-rr" "$OUT/failures.txt"'

fi

# ===================================================================== hostile version output, a venv without python
if want hostile; then
run_job hostile "B:1:rr" VR_BIN="$T/vr2/vllm-router" STUB_APPTAINER_VERSION="apptainer version 1.3.6 (built at seedpath-x /seed/dir/x on seedhost01)" \
  STUB_ENVOY_VERSION="envoy  version: /seed/dir/envoy"
check "version lines that are not plain versions are not written; no python next to VR_BIN gives vllm_router unknown; exit 0" '[[ $RC == 0 ]] && grep -q "^apptainer: unrecognised\$" "$OUT/env.txt" && grep -q "^envoy: unrecognised\$" "$OUT/env.txt" && grep -q "^vllm_router unknown\$" "$OUT/env.txt" && ! grep -q -e seed -e "/seed" "$OUT/env.txt"'
fi

# ===================================================================== a job that is terminated mid-run (timeout, scancel)
if want termed; then
JOB_PREFIX=(timeout 4)
run_job termed "B:1:rr B:1:vr-hash B:1:envoy-bound" STUB_WORKER_DELAY=1.5
JOB_PREFIX=()
check "a job terminated mid-run was really cut short (timeout's status 124, no complete results)" '[[ $RC == 124 && ! -s $OUT/arms/B1-envoy-bound/meta.json ]]'
check "a terminated job's cleanup stops every process it started" '[[ $(leftovers) == 0 && $(wc -l < "$STUB_PIDS") -ge 4 ]]'
fi

# ===================================================================== step H and the dead-worker probe
# Expected from the job's header: H uses KV_BLOCKS_B and LOAD_ARGS_H; the probe kills the last worker, the job restarts it
# (its own record, -restart), Envoy gets -verify-dir, and the probe's exit status is the arm's verdict.
if want probe; then
run_job probe "H:1:envoy-bound H:1:probe" LOAD_ARGS_H="-sessions 6 -turns 1 -concurrency 64"
check "step H and a passing probe exit 0" '[[ $RC == 0 && ! -e $OUT/failures.txt ]]'
check "step H: 6000 blocks and LOAD_ARGS_H in meta" '[[ $(meta "$OUT/arms/H1-envoy-bound" kv_blocks) == 6000 && $(meta "$OUT/arms/H1-envoy-bound" load_args) == "\"-sessions 6 -turns 1 -concurrency 64\"" ]]'
check "the probe arm's meta: arm probe, router envoy, bound 125, regime H, client_rc 0" '[[ $(meta "$OUT/arms/H1-probe" arm) == "\"probe\"" && $(meta "$OUT/arms/H1-probe" router) == "\"envoy\"" && $(meta "$OUT/arms/H1-probe" bound) == "\"125\"" && $(meta "$OUT/arms/H1-probe" regime) == "\"H\"" && $(meta "$OUT/arms/H1-probe" client_rc) == 0 ]]'
check "the probe's Envoy ran bound with -verify-dir" 'grep -q verify-dir "$OUT/arms/H1-probe/envoy/start.json" && grep -q "\"bound\": \"125\"" "$OUT/arms/H1-probe/envoy/start.json"'
check "the victim (worker 3) was restarted by the job: its own record" '[[ -s $OUT/worker-H-w3-restart.json ]] && [[ $(python3 -I -c "import json,sys; print(json.load(open(sys.argv[1]))[\"device\"])" "$OUT/worker-H-w3-restart.json") == 3 ]]'
check "probe.json and the probe's lines are in the results" '[[ -s $OUT/arms/H1-probe/probe.json ]] && grep -q "^ok no 503" "$OUT/arms/H1-probe/probe.out"'
check "the summary reads the probe arm" 'python3 -I -B "$HERE/analyze.py" "$OUT" > "$T/probe-summary.txt" 2>&1; grep -q "Dead-worker probe H1-probe (victim worker-3)" "$T/probe-summary.txt"'
check "no stub process is left after the probe" '[[ $(leftovers) == 0 ]]'
run_job badprobe "H:1:probe" STUB_PROBE=fail
check "a failing probe exits 1 and failures.txt names it" '[[ $RC == 1 ]] && grep -q "H1-probe: probe exit 1: FAIL no 503" "$OUT/failures.txt"'
run_job earlyadmit "H:1:probe" S2C_FAULT=early-admit
check "S2C_FAULT=early-admit gives the probe's Envoy -fault ignore-verify" 'grep -q ignore-verify "$OUT/arms/H1-probe/envoy/start.json"'
check "no stub process is left after the probe cases" '[[ $(leftovers) == 0 ]]'
fi

check "no stub process is left running after all cases" '[[ $(leftovers) == 0 ]]'
if (( fails > 0 )); then echo "test_job_status: $fails check(s) failed; logs kept in $T" >&2; trap - EXIT; exit 1; fi
echo "test_job_status: all checks passed"
