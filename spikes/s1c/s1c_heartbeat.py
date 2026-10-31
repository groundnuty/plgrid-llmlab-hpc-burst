"""Spike S1c: how long frps takes to notice a dead tunnel, per heartbeat setting.

The path is frpc -> toxiproxy -> frps. A SIGKILL'd frpc on a live host closes its socket, so frps sees
a FIN at once whatever the heartbeat; that case is measured ("kill") but says nothing about
heartbeats. The heartbeat-bound cases need a path that goes silent with every socket still open.
toxiproxy has no such toxic: its timeout toxic drops data but passes a close on (toxics/timeout.go,
`c == nil` -> stub.Close), a bandwidth rate of 0 means unlimited (toxics/bandwidth.go), and disabling
a proxy closes its connections. So the path is frozen by stopping the toxiproxy process (SIGSTOP):
the kernel keeps both TCP connections open and buffers what arrives, nothing is forwarded, and a
FIN from a killed frpc stays unread ("freeze", "crash"). The "disable" case shows the difference.
"""
import csv
import os
import random
import signal
import time

from s1c_lib import HB, PORTS, Env, http_json, load_gate, log, TOX
from s1c_stack import Stack, note_load

SCENARIOS = {
    "kill": "SIGKILL frpc; the path stays up, so the kernel's FIN reaches frps",
    "freeze": "freeze the path (SIGSTOP toxiproxy) with frpc alive; heal it after detection",
    "crash": "freeze the path, then SIGKILL frpc 0.5 s later: a node crash, no FIN reaches frps",
    "disable": "toxiproxy disables the proxy, which closes both sockets (evidence only, default setting)",
}
MAX_DETECT, MAX_HEAL = 90.0, 60.0


def log_events(path, since):
    """(epoch, line) for frp log lines at or after `since` (frp writes local time with ms)."""
    out = []
    try:
        with open(path, errors="replace") as fh:
            for line in fh:
                try:
                    t = time.mktime(time.strptime(line[:19], "%Y-%m-%d %H:%M:%S")) + int(line[20:23]) / 1000.0
                except ValueError:
                    continue
                if t >= since - 0.002:
                    out.append((t, line.strip()))
    except OSError:
        pass
    return out


def first(events, pred):
    for t, line in events:
        if pred(line):
            return t, line
    return None, ""


def read_probe(path):
    rows = []
    with open(path) as fh:
        for r in csv.reader(fh):
            if len(r) >= 4:
                rows.append((r[0], float(r[1]), float(r[2]), r[3], r[4] if len(r) > 4 else ""))
    return rows


def rel(t, t0):
    return "" if t is None else round(t - t0, 2)


def one(st, env, setting, scen, rep, rng, results):
    load_gate()
    st.arm_up("tcpmux", via="tox", setting=setting, level="trace")
    phase = round(rng.uniform(0, 10), 2)   # the keepalive timers' phase relative to the failure
    time.sleep(phase)
    csvp = env.p("raw", "hb_%s_%s_%d.csv" % (setting, scen, rep))
    st.P.start("probe", [env.b("s1cbench"), "probe", "-target", "127.0.0.1:%d" % PORTS["mux"], "-mux", "inst",
                         "-interval", "200ms", "-timeout", "15s", "-duration", "400s",
                         "-api", "http://127.0.0.1:%d/api/proxy/tcpmux" % PORTS["dash"],
                         "-api-pass-file", env.p("run", "dash.pass"), "-proxy", "inst.engine", "-out", csvp])
    time.sleep(3)
    t_heal = None
    frozen = False
    try:
        t0 = time.time()
        if scen == "kill":
            st.P.signal("frpc", signal.SIGKILL)
        elif scen in ("freeze", "crash"):
            st.P.signal("tox", signal.SIGSTOP)
            frozen = True
            if scen == "crash":
                time.sleep(0.5)
                st.P.signal("frpc", signal.SIGKILL)
        elif scen == "disable":
            http_json("POST", TOX + "/proxies/frp", {"enabled": False})
        gone = None
        while time.time() - t0 < MAX_DETECT:
            if st.api().get("inst.engine") != "online":
                gone = time.time()
                break
            time.sleep(0.1)
        time.sleep(2)
        if frozen:
            t_heal = time.time()
            st.P.signal("tox", signal.SIGCONT)
            frozen = False
        if scen == "disable":
            t_heal = time.time()
            http_json("POST", TOX + "/proxies/frp", {"enabled": True})
        if scen in ("freeze", "disable"):
            end = time.time() + MAX_HEAL
            while time.time() < end and st.api().get("inst.engine") != "online":
                time.sleep(0.1)
            time.sleep(2)
    finally:
        if frozen:
            st.P.signal("tox", signal.SIGCONT)
        st.P.stop("probe")
    row = summarize(env, setting, scen, rep, phase, t0, t_heal, csvp)
    st.arm_down()
    row["load_end"] = note_load("heartbeat %s/%s/%d: frps gone after %s s" % (setting, scen, rep, row["api_gone_s"]))
    path = os.path.join(results, "heartbeat.csv")
    new = not os.path.exists(path)
    with open(path, "a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(row))
        if new:
            w.writeheader()
        w.writerow(row)


def summarize(env, setting, scen, rep, phase, t0, t_heal, csvp):
    rows = read_probe(csvp)
    api = [(a, o) for k, a, _, o, _ in rows if k == "api" and a >= t0]
    probes = [(a, b, o) for k, a, b, o, _ in rows if k == "probe" and a >= t0]
    api_gone = next((a for a, o in api if o != "online"), None)
    refused = next((a for a, _, o in probes if o == "refused"), None)
    hung = [b - a for a, b, o in probes if o in ("timeout", "closed") and (refused is None or a < refused)]
    ok_after = [a for a, _, o in probes if o == "ok" and (t_heal is None or a < t_heal)]
    ev = log_events(env.p("logs", "frps-frp.log"), t0)
    t_gone, _ = first(ev, lambda l: "[inst.engine] proxy closing" in l)
    keys = ("keepalive", "heartbeat", "EOF", "reset", "broken pipe", "i/o timeout", "use of closed", "session shutdown")
    cause = [l.split("] ", 2)[-1][:110] for t, l in ev
             if (t_gone is None or t <= t_gone + 0.05) and any(k in l for k in keys)][:2]
    row = dict(setting=setting, scenario=scen, rep=rep, phase_s=phase,
               api_gone_s=rel(api_gone, t0), log_gone_s=rel(t_gone, t0), first_refused_s=rel(refused, t0),
               hung_n=len(hung), hung_med_s=round(sorted(hung)[len(hung) // 2], 2) if hung else "",
               hung_max_s=round(max(hung), 2) if hung else "", ok_after_failure=len(ok_after),
               frps_cause=" | ".join(cause))
    fev = log_events(env.p("logs", "frpc-frp.log"), t0)
    t_fd, fl = first(fev, lambda l: "[W]" in l or "[E]" in l or "keepalive failed" in l)
    row["frpc_detect_s"] = rel(t_fd, t0) if scen == "freeze" else ""
    row["frpc_cause"] = fl.split("] ", 2)[-1][:90] if scen == "freeze" else ""
    if t_heal is not None:
        online = next((a for k, a, _, o, _ in rows if k == "api" and a >= t_heal and o == "online"), None)
        ok = next((a for k, a, _, o, _ in rows if k == "probe" and a >= t_heal and o == "ok"), None)
        row["heal_online_s"], row["heal_probe_ok_s"] = rel(online, t_heal), rel(ok, t_heal)
    else:
        row["heal_online_s"] = row["heal_probe_ok_s"] = ""
    return row


def run(args, results):
    env = Env()
    st = Stack(env)
    os.makedirs(results, exist_ok=True)
    rng = random.Random(66)
    try:
        load_gate()
        st.base_up()
        for setting in args.settings:
            for scen in args.scenarios:
                if scen == "disable" and setting != "default":
                    continue
                for rep in range(1, (1 if scen == "disable" else args.reps) + 1):
                    one(st, env, setting, scen, rep, rng, results)
    finally:
        if st.P.alive("tox"):
            st.P.signal("tox", signal.SIGCONT)
        st.P.stop_all()
    log("heartbeat settings: %s" % HB)
