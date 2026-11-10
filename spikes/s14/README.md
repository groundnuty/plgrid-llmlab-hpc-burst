# Spike S14: many tunnels on one front (HBEP-6, open question 11)

Issue #313. One front host, one frps and the front's dial-home listener, with 1 to 300 frpc tunnels, in direct and host-dialed mode, on loopback through S1c's link emulator. The measurements ran on GitHub-hosted runners, three repetitions on three fresh runners; the shared development VM ran only builds and smoke runs at 1 and 10 tunnels.

## Findings

The answer to open question 11: on one front, CPU per token is the limit, not the number of tunnels. Up to 300 tunnels, the front's cost grew linearly with the token rate, plus a small cost per tunnel, and no tunnel dropped. Every number below is from three 4-CPU hosted runners (Environment); "valid" means every tunnel received a token in every second of the 30 s window.

- **The front's CPU is linear, with no bend through 300 tunnels while the host had CPU to spare.** Fitted over the valid runs whose host was not CPU-bound (`summarize.py fits`):
  - **direct mode** (frps plus the dial-home listener): 0.088 cores per 1,000 tokens/s, plus 0.68 thousandths of a core per tunnel, plus 0.014 (24 runs). At 50 tunnels × 8 streams that is 8.4% of a core per 1,000 tokens/s (8.1–11.6% over the repetitions), in line with S1c's 7.9–8.7% for one tunnel.
  - **host-dialed mode** (plus the relay owner and the ssh stand-ins): 0.124 cores per 1,000 tokens/s, plus 1.5 thousandths of a core per tunnel, plus 0.041 (22 runs). That is 1.4 times direct per token and 2.2 times per tunnel, without ssh's encryption, which the stand-in does not do.
- **Memory and descriptors are not limits at this scale.**
  - frps held 94 MB at 300 tunnels × 1 stream and 242 MB at 300 × 8. The listener held 54–57 MB.
  - Descriptors: frps holds one per tunnel plus one per stream in flight (2,730 at 300 × 8); the listener two per tunnel (608 at 300); the relay owner 9 per Allocation (2,707 at 300); each ssh stand-in 9 descriptors and 3 MB.
- **Latency: the tunnel adds little while the host has headroom.** Through 300 tunnels at 1 stream, and 50 tunnels at 8 streams:
  - direct mode added 1–3 ms to the median TTFT and at most 8 ms to its p99 over the no-tunnel baseline on the same host, and at most 3 ms to the gap's p99;
  - host-dialed mode added up to 16 ms at p99 at 50 × 8.
- **The bend came from the runner, not the front.** The runners carried both ends on 4 CPUs.
  - At 100 tunnels × 8 streams (15,800 tokens/s offered), the hosts were 94–99% busy. TTFT p99 rose to 124 ms in direct mode and 236 ms in host-dialed mode, and throughput fell 2–22% short.
  - At 300 × 8 (47,000 tokens/s offered), the tunnels delivered 11,800–20,600 tokens/s, and 5 of 6 tunnel runs failed the validity rule.
  - At 100 × 8 the front used 1.1 of the 3.8 busy cores in direct mode. frpc took 1.4, toxiproxy 0.5, and the engines and client 0.8.
  - The no-tunnel baseline delivered all 47,000 tokens/s at 36–50% busy.
- **Host-dialed mode bends earlier on the same host.** At 300 tunnels its relays add about a core: the owner, the stand-ins and the relay steps. Its TTFT p99 at 300 × 1 was 75–198 ms, against 58–62 ms in direct mode.
- **No heartbeat loss.** 90 runs, including the 300 × 8 runs where a tunnel went silent for up to 4.9 s, showed:
  - 0 drops in frps's list, 0 frpc re-logins, 0 relay restarts and 0 `heartbeat timeout` lines;
  - no failed probe (p99 at most 4.2 s, under the 10 s `probeTimeout`).
  - All 300 tunnels registered within 3.0 s of the first frpc's start.
- **Implication for a per-front limit:** size the front by tokens per second per front core, with a small term per tunnel, rather than by a tunnel count (Verdict). Hundreds of tunnels need no cap of their own, but they need an open-files limit to match.

## Question

HBEP-6's open question 11, which blocks beta: "S1c measured one frpc. frps's CPU and memory with hundreds of tunnels, and many tunnels sharing one link, need a measurement." Beta serves several deployments, each with several Instances, through one front, and host-dialed mode adds two relays per running Allocation. The issue asks:
- the CPU (cores) and RSS of frps, the listener and the relay owner, and file descriptors;
- the time to first token (TTFT) and the inter-token gap at p50 and p99;
- the heartbeat-loss count;
- where cost or latency bends, and what that implies for a per-front limit on tunnels.

## Method

### The path, per mode

```
direct       client --CONNECT uid--> frps tcpmux :29921
                                       | yamux stream in frpc's wss session
             frpc <==wss==> toxiproxy :29925 (the link) ==> listener :29923 (TLS, /~!frp) ==> frps bind :29920
             frpc --unix_domain_socket--> engine double (one per tunnel)

host-dialed  frpc --tcp--> job side's serving listener (hostdial.Job, kernel port)
                 --yamux stream--> relay socket --> relay step (hostdial.Relay)
                 --TCP--> toxiproxy :29925 (the link) --> ssh stand-in (s14bridge, stdio pipes)
                 --pipes--> relay owner (hostdial.Owner, hostdial.Serve) --tcp--> listener :29923 ==> frps :29920

baseline     client --unix--> engine double (no tunnel; the host-load control)
```

- **One tunnel is one Instance** with its own frpc, its own engine double and, in host-dialed mode, its own Allocation with two relays (control and serving). This is the most relays per tunnel. With several Instances in one Allocation, their frpc connections would share one serving relay as streams of one session; that was not measured.
- **What is real and what stands in:**

| Part | In this spike | Where it comes from |
|---|---|---|
| frps, frpc | frp v0.71.1 release binaries, sha256-checked | S1c's pin; frps and frpc against `internal/launcher/frp-pins.txt` |
| frps settings | HBEP-6's `frps.toml`: loopback binds, tcpmux httpconnect, web server with a password, S1's heartbeat set | HBEP-6, frps on the control-plane host |
| frpc settings | HBEP-6's `frpc.toml`: `wss`, `trustedCaFile` and `serverName` `dial-home.hpcburst.invalid`, `poolCount` 0, `loginFailExit` false, S1's heartbeats, one `tcpmux` proxy named by the UID with the `unix_domain_socket` plugin, `http_proxy` unset | HBEP-6, frpc in the job |
| the dial-home listener | TLS (at least 1.2) with a throwaway certificate, `ReadHeaderTimeout` 1 min, `/agent/v1/` 404, everything else to the `/~!frp` passthrough | `internal/front/frppass.go`, copied unchanged but for its package (`cmd/s14/frppass.go`); the server's settings from `internal/front/server.go` and `dialhome.go` |
| the relay owner | `internal/hostdial.Owner`, unchanged: its process start (own process group, `Pdeathsig`), first-start jitter, preamble check, `Serve` and the yamux session config (10 s keepalive, 10 s write timeout) | `internal/hostdial` at the base commit |
| the job side | `hostdial.StartJob`, unchanged: two loopback listeners, the relay socket, the rendezvous file, fencing | `internal/hostdial` |
| the relay step | `hostdial.Relay`, unchanged, run in the node process with a TCP connection as its standard streams | `internal/hostdial` |
| ssh and srun | `s14bridge`: dials the node through the link, sends the relay line, then copies standard input and output to the connection. The owner starts it exactly as it starts ssh | new; it carries no encryption (Unknowns) |
| the engine | `internal/agent/enginedouble`, 50 ms before each token (20 tokens/s), on a Unix socket | this project; linked only by `node/node_test.go`, as the double's import guard requires |
| the link | toxiproxy v2.13.0, one proxy, no toxic, carrying every tunnel's connection | S1c's emulator and pin |
| the front's transport and probe | the load client: `CONNECT <uid>:80` to frps's tcpmux port, then plain HTTP; and `GET /health` through the tunnel every 10 s per tunnel | HBEP-6, The front's half; dial and stream reader after S1c's `s1cbench` |
| the controller's list read | `GET /api/proxy/tcpmux` every 10 s | HBEP-6, Path state |

- **Not modelled:** the frps server plugin (it acts only at logins and new proxies, not in steady state); the agent's control traffic (the control relays carry an idle session with its keepalives, and in direct mode the listener's `/agent/v1/` passthrough is not built); the front's own HTTP proxy in front of the transport.

### Load

- **Configurations:** 1, 10, 50, 100 and 300 tunnels, at 1 and 8 streams per tunnel, in each of the three modes: 30 per repetition.
- **Closed loop, as in S1c:** each stream is a worker that starts a new streamed chat completion of exactly 60 tokens (`max_tokens` = `min_tokens` = 60, about 3 s) on a new connection as soon as its last one ends. Worker j of all starts at 5 s × j / (tunnels × streams).
- **The window:** 30 s after the 5 s ramp. Statistics cover the requests started, and the tokens that arrived, inside it.
- **TTFT** runs from before the client's connect, CONNECT included, to the first content token. **A gap** is the time between two consecutive content tokens of one stream. Both are pooled over every completed request started in the window, and over the three repetitions at 1 ms resolution (`hists.json`).
- **Fresh processes per configuration,** so a peak RSS is never the earlier configuration's.

### Measures

- **CPU:** per process, the ticks between the first and last `/proc` sample inside the window (every 2 s), over that time, in cores. **RSS and descriptors:** the window's peak of each role's sum over its processes (`/proc/<pid>/status` VmRSS, `/proc/<pid>/fd`).
- **The front host's roles:** frps, the listener, the relay owner and the ssh stand-ins (two per tunnel). Recorded too, but not front cost: frpc (summed), the node process (engine doubles, job sides, relay steps), the client, toxiproxy, the driver.
- **The host:** `/proc/stat` over the window. A run with all CPUs at least 85% busy is marked host-bound: its latency measures the runner, not the tunnel.
- **Heartbeat loss,** by four witnesses: frps's proxy list (a proxy not online, absent, or with a new `lastStartTime` after readiness); frpc logins beyond the first (its log); relay sessions beyond the first (the owner's `Relays()`); job-side sessions beyond the first (`Job.Sessions`). Plus every `heartbeat timeout` line in frps's and the frpc logs.
- **Readiness:** the time from the first frpc start until frps lists all N proxies online.

### Validity

- **Every tunnel streams throughout the window.** A configuration counts only if every tunnel received at least one token in every second of the window (`analysis.tunnel_validity`, with its unit tests). Registered tunnels that carry no stream prove nothing (issue #313), so the rule reads the tokens each tunnel received, not frps's list.
- The per-tunnel evidence is committed: `results/rep*/tunnels.csv` holds, per run and tunnel, the requests started, the tokens received, the emptiest second and the longest silence.
- **Load is real:** the tables show the requests and tokens per tunnel.
- **A verification that would prove nothing here, and is not used:** a count of registered proxies, or of relays up. Both are recorded, as readiness, but validity is the tokens.

## Results

All tables come from `results/rep{1,2,3}/`, rendered by `python3 summarize.py tables`. 30 configurations ran per repetition, 90 in all, and every run finished (status `ok`). On every runner nothing was left running afterwards, and no listener stayed in 29920–29939 (`s14.py leftovers`, in each job's log).

### Validity and load

Per configuration: the repetitions run, those valid (every tunnel received a token in every second of the 30 s window), whether any ran on a host-bound runner (all CPUs at least 85% busy), the tokens per second, and per tunnel the requests started in the window and the tokens received (minimum–maximum over all tunnels of all repetitions).

| mode | tunnels | streams/tunnel | runs | valid | host-bound | tok/s | requests per tunnel | tokens per tunnel | emptiest second, tokens | longest silence, ms | failed requests | probes failed |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline | 1 | 1 | 3 | 3 | 0 | 20 | 10–10 | 594–594 | 19–19 | 51–51 | 0 | 0 |
| direct | 1 | 1 | 3 | 3 | 0 | 20 | 10–10 | 594–595 | 19–19 | 52–53 | 0 | 0 |
| host-dialed | 1 | 1 | 3 | 3 | 0 | 20 | 10–10 | 592–593 | 19–19 | 52–53 | 0 | 0 |
| baseline | 10 | 1 | 3 | 3 | 0 | 198 (197–198) | 10–10 | 592–594 | 19–19 | 51–56 | 0 | 0 |
| direct | 10 | 1 | 3 | 3 | 0 | 197 | 10–10 | 591–593 | 19–19 | 52–56 | 0 | 0 |
| host-dialed | 10 | 1 | 3 | 3 | 0 | 197 | 10–10 | 591–593 | 19–19 | 52–56 | 0 | 0 |
| baseline | 50 | 1 | 3 | 3 | 0 | 988 (987–988) | 9–10 | 592–594 | 19–19 | 51–55 | 0 | 0 |
| direct | 50 | 1 | 3 | 3 | 0 | 988 (987–988) | 9–10 | 591–593 | 19–19 | 52–60 | 0 | 0 |
| host-dialed | 50 | 1 | 3 | 3 | 0 | 988 (988–989) | 9–10 | 592–594 | 19–19 | 52–60 | 0 | 0 |
| baseline | 100 | 1 | 3 | 3 | 0 | 1975 (1974–1976) | 9–10 | 592–593 | 19–19 | 51–56 | 0 | 0 |
| direct | 100 | 1 | 3 | 3 | 0 | 1976 | 9–10 | 592–593 | 19–19 | 52–59 | 0 | 0 |
| host-dialed | 100 | 1 | 3 | 3 | 0 | 1977 (1977–1978) | 9–10 | 592–594 | 19–19 | 53–63 | 0 | 0 |
| baseline | 300 | 1 | 3 | 3 | 0 | 5932 (5926–5932) | 9–10 | 592–594 | 19–19 | 51–57 | 0 | 0 |
| direct | 300 | 1 | 3 | 3 | 0 | 5921 (5913–5929) | 9–10 | 590–594 | 19–19 | 54–85 | 0 | 0 |
| host-dialed | 300 | 1 | 3 | 3 | 1 | 5896 (5706–5922) | 9–10 | 565–594 | 13–19 | 62–278 | 0 | 0 |
| baseline | 1 | 8 | 3 | 3 | 0 | 158 (157–159) | 79–79 | 4724–4758 | 154–157 | 15–31 | 0 | 0 |
| direct | 1 | 8 | 3 | 3 | 0 | 158 (158–159) | 79–79 | 4742–4759 | 154–157 | 16–29 | 0 | 0 |
| host-dialed | 1 | 8 | 3 | 3 | 0 | 158 (157–158) | 79–79 | 4722–4735 | 155–156 | 22–27 | 0 | 0 |
| baseline | 10 | 8 | 3 | 3 | 0 | 1580 (1579–1581) | 73–80 | 4737–4744 | 154–157 | 14–27 | 0 | 0 |
| direct | 10 | 8 | 3 | 3 | 0 | 1581 (1580–1582) | 73–80 | 4738–4746 | 152–156 | 22–51 | 0 | 0 |
| host-dialed | 10 | 8 | 3 | 3 | 0 | 1581 | 74–80 | 4741–4744 | 152–156 | 24–53 | 0 | 0 |
| baseline | 50 | 8 | 3 | 3 | 0 | 7904 (7903–7904) | 72–80 | 4739–4744 | 154–156 | 26–43 | 0 | 0 |
| direct | 50 | 8 | 3 | 3 | 0 | 7898 (7896–7907) | 72–80 | 4732–4747 | 152–157 | 21–56 | 0 | 0 |
| host-dialed | 50 | 8 | 3 | 3 | 1 | 7899 (7870–7908) | 72–80 | 4719–4749 | 152–156 | 25–53 | 0 | 0 |
| baseline | 100 | 8 | 3 | 3 | 0 | 15810 (15809–15817) | 72–80 | 4742–4748 | 152–157 | 22–50 | 0 | 0 |
| direct | 100 | 8 | 3 | 3 | 3 | 15307 (14059–15533) | 64–80 | 4195–4667 | 118–153 | 31–102 | 0 | 0 |
| host-dialed | 100 | 8 | 3 | 3 | 3 | 13938 (12333–14248) | 56–72 | 3674–4292 | 90–133 | 58–180 | 0 | 0 |
| baseline | 300 | 8 | 3 | 3 | 0 | 47352 (47177–47381) | 72–80 | 4710–4744 | 152–156 | 28–73 | 0 | 0 |
| direct | 300 | 8 | 3 | 1 | 3 | 16508 (11791–16982) | 12–33 | 964–1853 | 0–48 | 109–2982 | 0 | 0 |
| host-dialed | 300 | 8 | 3 | 0 | 3 | 19496 (14418–20625) | 16–40 | 1197–2227 | 0–42 | 577–4886 | 0 | 0 |

### The front host's cost

Median over the repetitions (range in brackets when they differ). CPU is cores averaged over the window; front = frps + listener (+ owner + the ssh stand-ins in host-dialed mode). RSS and descriptors are the window's peak, summed over a role's processes (2 per tunnel for the stand-ins).

| mode | tunnels | streams | frps cores | listener cores | owner cores | stand-ins cores | front cores | front % core per 1k tok/s | frps RSS MB | listener RSS MB | owner RSS MB | stand-ins RSS MB | frps fds | listener fds | owner fds | host busy |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| direct | 1 | 1 | 0.003 | 0.002 | - | - | 0.005 | 25.2 (23.7–26.8) | 26 (26–27) | 26 | - | - | 12 | 10 | - | 0.01 |
| host-dialed | 1 | 1 | 0.003 (0.003–0.004) | 0.002 (0.001–0.002) | 0.003 (0.002–0.003) | 0.002 (0.001–0.002) | 0.009 (0.008–0.010) | 43.6 (39.5–49.2) | 26 (26–27) | 26 | 26 | 6 | 12 | 10 | 16 | 0.01 |
| direct | 10 | 1 | 0.019 (0.019–0.026) | 0.014 (0.012–0.019) | - | - | 0.033 (0.031–0.045) | 16.5 (15.6–22.6) | 30 (30–31) | 27 (27–28) | - | - | 30 | 28 | - | 0.03 (0.02–0.03) |
| host-dialed | 10 | 1 | 0.020 (0.019–0.026) | 0.013 (0.013–0.018) | 0.015 (0.013–0.019) | 0.013 (0.013–0.016) | 0.061 (0.057–0.079) | 30.7 (29.1–39.9) | 31 | 27 (27–28) | 29 (28–29) | 60 | 30 | 28 | 97 | 0.04 (0.04–0.05) |
| direct | 50 | 1 | 0.074 (0.069–0.097) | 0.056 (0.049–0.073) | - | - | 0.130 (0.118–0.170) | 13.2 (12.0–17.2) | 40 | 31 | - | - | 110 (110–111) | 108 | - | 0.10 (0.10–0.13) |
| host-dialed | 50 | 1 | 0.074 (0.073–0.102) | 0.053 (0.050–0.074) | 0.054 (0.050–0.074) | 0.058 (0.058–0.085) | 0.238 (0.233–0.336) | 24.0 (23.5–34.0) | 40 (40–41) | 31 | 39 (39–40) | 300 (300–301) | 110 (110–111) | 108 | 457 | 0.15 (0.15–0.21) |
| direct | 100 | 1 | 0.136 (0.129–0.182) | 0.103 (0.093–0.141) | - | - | 0.239 (0.222–0.323) | 12.1 (11.2–16.3) | 51 (49–51) | 35 (35–36) | - | - | 210 (210–211) | 208 | - | 0.19 (0.18–0.26) |
| host-dialed | 100 | 1 | 0.135 (0.131–0.176) | 0.099 (0.094–0.132) | 0.094 (0.090–0.126) | 0.116 (0.114–0.160) | 0.438 (0.435–0.595) | 22.1 (22.0–30.1) | 50 | 35 (35–36) | 53 (50–53) | 602 (600–602) | 210 (210–211) | 208 | 907 | 0.28 (0.28–0.37) |
| direct | 300 | 1 | 0.371 (0.367–0.505) | 0.292 (0.273–0.405) | - | - | 0.660 (0.644–0.911) | 11.1 (10.9–15.4) | 94 (93–94) | 54 (53–54) | - | - | 611 (610–612) | 608 | - | 0.55 (0.54–0.76) |
| host-dialed | 300 | 1 | 0.351 (0.350–0.411) | 0.276 (0.255–0.332) | 0.243 (0.225–0.273) | 0.357 (0.337–0.459) | 1.227 (1.167–1.475) | 20.7 (19.8–25.9) | 94 (94–95) | 53 (52–54) | 106 (103–106) | 1811 (1810–1811) | 612 (611–614) | 608 | 2707 | 0.80 (0.76–0.96) |
| direct | 1 | 8 | 0.013 (0.013–0.018) | 0.009 (0.009–0.013) | - | - | 0.022 (0.021–0.031) | 13.7 (13.6–19.4) | 29 | 26 | - | - | 19 | 10 | - | 0.02 |
| host-dialed | 1 | 8 | 0.014 (0.013–0.018) | 0.009 (0.007–0.010) | 0.010 (0.009–0.011) | 0.007 (0.007–0.009) | 0.039 (0.036–0.048) | 24.7 (23.1–30.4) | 29 (28–29) | 26 | 26 (25–26) | 6 | 19 | 10 | 16 | 0.03 |
| direct | 10 | 8 | 0.097 (0.090–0.118) | 0.072 (0.061–0.087) | - | - | 0.169 (0.151–0.205) | 10.7 (9.5–13.0) | 38 (37–38) | 28 | - | - | 100 | 28 | - | 0.13 (0.12–0.16) |
| host-dialed | 10 | 8 | 0.086 (0.077–0.114) | 0.055 (0.054–0.080) | 0.050 (0.046–0.072) | 0.054 (0.045–0.069) | 0.244 (0.223–0.335) | 15.4 (14.1–21.2) | 38 | 27 (27–28) | 29 (29–30) | 60 | 100 | 28 | 97 | 0.15 (0.14–0.21) |
| direct | 50 | 8 | 0.373 (0.372–0.515) | 0.294 (0.268–0.402) | - | - | 0.666 (0.641–0.917) | 8.4 (8.1–11.6) | 67 (66–67) | 34 | - | - | 460 | 108 | - | 0.54 (0.54–0.75) |
| host-dialed | 50 | 8 | 0.357 (0.355–0.456) | 0.266 (0.248–0.345) | 0.214 (0.207–0.278) | 0.282 (0.261–0.345) | 1.095 (1.095–1.424) | 13.9 (13.9–18.1) | 67 (67–68) | 34 (34–35) | 41 (41–43) | 301 (300–301) | 460 (460–461) | 108 | 457 | 0.71 (0.71–0.95) |
| direct | 100 | 8 | 0.617 (0.594–0.622) | 0.483 (0.456–0.497) | - | - | 1.091 (1.073–1.105) | 7.1 (7.0–7.8) | 102 (102–103) | 39 (38–40) | - | - | 911 (910–911) | 208 | - | 0.95 (0.94–0.99) |
| host-dialed | 100 | 8 | 0.459 (0.445–0.460) | 0.338 (0.325–0.367) | 0.173 (0.152–0.179) | 0.338 (0.301–0.343) | 1.297 (1.279–1.305) | 9.3 (9.2–10.4) | 104 | 38 (37–39) | 54 (54–55) | 604 (602–604) | 912 (911–913) | 208 | 907 | 0.96 (0.96–0.99) |
| direct | 300 | 8 | 0.601 (0.579–0.602) | 0.508 (0.495–0.522) | - | - | 1.101 (1.097–1.109) | 6.7 (6.5–9.3) | 242 (239–248) | 57 (55–57) | - | - | 2730 (2730–2742) | 608 | - | 0.99 (0.99–1.00) |
| host-dialed | 300 | 8 | 0.336 (0.328–0.343) | 0.234 (0.226–0.258) | 0.077 (0.071–0.079) | 0.204 (0.201–0.211) | 0.845 (0.834–0.889) | 4.6 (4.0–5.9) | 250 (250–254) | 51 | 125 (115–132) | 1811 (1808–1811) | 2759 (2756–2785) | 608 | 2707 | 0.99 (0.97–0.99) |

### Latency

Pooled over the repetitions at 1 ms resolution (each value is a 1 ms bin's middle). TTFT runs from before the client's connect, CONNECT included, to the first content token; the engine double waits 50 ms before each token. The baseline dials each engine's socket directly, on the same host under the same load.

| tunnels | streams | mode | TTFT p50 ms | TTFT p99 ms | gap p50 ms | gap p99 ms | probe p99 ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 1 | baseline | 50.5 | 51.5 | 50.5 | 50.5 | 1 (0–1) |
| 1 | 1 | direct | 51.5 | 52.5 | 50.5 | 50.5 | 1 (1–2) |
| 1 | 1 | host-dialed | 52.5 | 52.5 | 50.5 | 51.5 | 2 |
| 10 | 1 | baseline | 50.5 | 51.5 | 50.5 | 51.5 | 1 (0–1) |
| 10 | 1 | direct | 51.5 | 53.5 | 50.5 | 51.5 | 2 |
| 10 | 1 | host-dialed | 52.5 | 53.5 | 50.5 | 51.5 | 3 (2–5) |
| 50 | 1 | baseline | 50.5 | 51.5 | 50.5 | 51.5 | 1 (0–1) |
| 50 | 1 | direct | 52.5 | 54.5 | 50.5 | 52.5 | 3 |
| 50 | 1 | host-dialed | 52.5 | 54.5 | 50.5 | 52.5 | 4 (3–7) |
| 100 | 1 | baseline | 50.5 | 51.5 | 50.5 | 51.5 | 0 (0–1) |
| 100 | 1 | direct | 52.5 | 55.5 | 50.5 | 52.5 | 3 (3–4) |
| 100 | 1 | host-dialed | 53.5 | 58.5 | 50.5 | 53.5 | 6 (6–8) |
| 300 | 1 | baseline | 50.5 | 52.5 | 50.5 | 51.5 | 1 |
| 300 | 1 | direct | 53.5 | 60.5 | 50.5 | 54.5 | 8 (6–13) |
| 300 | 1 | host-dialed | 56.5 | 145.5 | 50.5 | 73.5 | 59 (27–172) |
| 1 | 8 | baseline | 51.5 | 51.5 | 50.5 | 51.5 | 0 (0–1) |
| 1 | 8 | direct | 51.5 | 52.5 | 50.5 | 51.5 | 1 (1–2) |
| 1 | 8 | host-dialed | 52.5 | 53.5 | 50.5 | 51.5 | 2 |
| 10 | 8 | baseline | 50.5 | 51.5 | 50.5 | 51.5 | 0 (0–1) |
| 10 | 8 | direct | 52.5 | 55.5 | 50.5 | 52.5 | 2 (2–4) |
| 10 | 8 | host-dialed | 52.5 | 54.5 | 50.5 | 52.5 | 5 (3–5) |
| 50 | 8 | baseline | 50.5 | 52.5 | 50.5 | 51.5 | 1 (1–2) |
| 50 | 8 | direct | 52.5 | 59.5 | 50.5 | 53.5 | 7 (5–8) |
| 50 | 8 | host-dialed | 55.5 | 68.5 | 50.5 | 57.5 | 13 (11–26) |
| 100 | 8 | baseline | 50.5 | 53.5 | 50.5 | 52.5 | 2 (1–2) |
| 100 | 8 | direct | 61.5 | 123.5 | 51.5 | 81.5 | 42 (30–82) |
| 100 | 8 | host-dialed | 104.5 | 235.5 | 55.5 | 115.5 | 117 (80–209) |
| 300 | 8 | baseline | 51.5 | 62.5 | 50.5 | 59.5 | 4 (3–21) |
| 300 | 8 | direct | 485.5 | 2623.5 | 84.5 | 916.5 | 2322 (2005–2898) |
| 300 | 8 | host-dialed | 1533.5 | 3589.5 | 0.5 | 923.5 | 2723 (2433–4248) |

### Heartbeat loss

Summed over the repetitions. A drop is a tunnel frps's proxy list showed not online, or registered again, after readiness (read every 10 s); a re-login is an frpc login beyond its first; a relay restart a relay session beyond its first (the owner's count), and a job re-session the same seen by the job side.

| mode | tunnels | streams | time to all online, s | drops | frpc re-logins | relay restarts | job re-sessions | heartbeat-timeout lines | unreadable lists |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| direct | 1 | 1 | 0.3 (0.2–0.3) | 0 | 0 | - | - | 0 | 0 |
| host-dialed | 1 | 1 | 0.5 (0.5–1.0) | 0 | 0 | 0 | 0 | 0 | 0 |
| direct | 10 | 1 | 0.3 | 0 | 0 | - | - | 0 | 0 |
| host-dialed | 10 | 1 | 1.1 (1.0–1.1) | 0 | 0 | 0 | 0 | 0 | 0 |
| direct | 50 | 1 | 0.5 (0.4–0.5) | 0 | 0 | - | - | 0 | 0 |
| host-dialed | 50 | 1 | 1.2 | 0 | 0 | 0 | 0 | 0 | 0 |
| direct | 100 | 1 | 0.8 (0.6–0.9) | 0 | 0 | - | - | 0 | 0 |
| host-dialed | 100 | 1 | 1.1 (1.0–1.1) | 0 | 0 | 0 | 0 | 0 | 0 |
| direct | 300 | 1 | 1.9 (1.7–2.9) | 0 | 0 | - | - | 0 | 0 |
| host-dialed | 300 | 1 | 2.4 (2.2–3.0) | 0 | 0 | 0 | 0 | 0 | 0 |
| direct | 1 | 8 | 0.3 (0.2–0.3) | 0 | 0 | - | - | 0 | 0 |
| host-dialed | 1 | 8 | 0.5 | 0 | 0 | 0 | 0 | 0 | 0 |
| direct | 10 | 8 | 0.3 | 0 | 0 | - | - | 0 | 0 |
| host-dialed | 10 | 8 | 1.0 (0.8–1.0) | 0 | 0 | 0 | 0 | 0 | 0 |
| direct | 50 | 8 | 0.5 | 0 | 0 | - | - | 0 | 0 |
| host-dialed | 50 | 8 | 1.2 (1.1–1.2) | 0 | 0 | 0 | 0 | 0 | 0 |
| direct | 100 | 8 | 0.7 | 0 | 0 | - | - | 0 | 0 |
| host-dialed | 100 | 8 | 1.0 (1.0–1.1) | 0 | 0 | 0 | 0 | 0 | 0 |
| direct | 300 | 8 | 1.6 (1.3–2.0) | 0 | 0 | - | - | 0 | 0 |
| host-dialed | 300 | 8 | 2.4 (2.3–2.9) | 0 | 0 | 0 | 0 | 0 | 0 |

### Runners

- rep 1: AMD EPYC 9V45 96-Core Processor, 4 CPUs, 15983 MB, Linux 7.0.0-1017-azure
- rep 2: AMD EPYC 9V74 80-Core Processor, 4 CPUs, 15983 MB, Linux 7.0.0-1017-azure
- rep 3: AMD EPYC 9V74 80-Core Processor, 4 CPUs, 15983 MB, Linux 7.0.0-1017-azure

**How to read the tables:**
- **Validity.** Every configuration up to 50 × 8 and 300 × 1 is valid in all three repetitions.
  - In those, every tunnel received 13–19 tokens in its emptiest second at 1 stream, and 90–157 at 8 streams. Each started 9–10 requests (1 stream) or 56–80 (8 streams) in the window.
  - 300 × 8 is invalid in 5 of its 6 tunnel runs (4 to 275 tunnels had an empty second), and its runs delivered 12–40 requests per tunnel instead of 72–80. It is excluded from the fits and the verdict: those runs measure a saturated runner.
- **Host-bound runs.** The rule (all CPUs at least 85% busy) flagged both modes at 100 × 8 and 300 × 8 in every repetition, plus host-dialed 50 × 8 and 300 × 1 in repetition 3. Their latency is the runner's. The fits leave them out.
- **Repetition 3 ran hotter.** On all 16 cells that were not host-bound, the front took 9–44% more CPU on repetition 3 than the mean of reps 1 and 2, although reps 2 and 3 had the same CPU model. On the 4 host-bound cells it took the same (0.98–1.00 of that mean). The ranges in the cost table show this, and the fits include all three repetitions, so the fitted costs lean toward the hotter runner.
- **Host-dialed 300 × 1, per repetition:** TTFT p99 109 / 75 / 198 ms and gap p99 67 / 58 / 84 ms, at host busy 0.76 / 0.80 / 0.96. Direct mode at the same size: 60 / 58 / 62 ms TTFT p99, at 0.54 / 0.55 / 0.76 busy.

## Verdict

**Where cost bends.** It does not, through 300 tunnels, as far as the hosted runners could show it.
- The front's CPU follows `cores ≈ a × (tokens/s ÷ 1000) + b × tunnels + c`:
  - direct mode: a = 0.088, b = 0.00068, c = 0.014;
  - host-dialed mode: a = 0.124, b = 0.0015, c = 0.041.
- The per-tunnel term is the steady cost of an idle tunnel: two yamux keepalives and an application heartbeat every 10 s, the probe every 10 s, and the controller's list read. 300 tunnels cost about 0.2 cores in direct mode and 0.45 in host-dialed mode before any token flows.
- The per-token term dominates as soon as streams run. At 20 tokens/s a stream costs about 1.8 thousandths of a core in direct mode, and 2.5 thousandths in host-dialed mode.
- **The cost per token falls with streams per tunnel.** It was 11.1% of a core per 1,000 tokens/s at 300 × 1 (5,900 tokens/s over 300 tunnels) and 8.4% at 50 × 8 (7,900 tokens/s over 50). Busier tunnels amortise their fixed cost.
- **Memory grows linearly and stays small.** For the front's processes together, the fit is 0.22 MB per tunnel plus 0.09 MB per stream in flight, plus 54 MB, in direct mode. Host-dialed mode adds about 6.3 MB per tunnel, nearly all of it the two ssh stand-ins per Allocation. A real OpenSSH client is likely heavier than the 3 MB stand-in.

**Where latency bends.** Only where the host's CPUs approached saturation. On these runners that was at 8,000–16,000 tokens/s through tunnels, because the runner also carried the instance side; frpc alone took about 9.4% of a core per 1,000 tokens/s (0.74 cores at 50 × 8 in direct mode).
- Below that, tunnels added 1–3 ms to the median TTFT and a few ms to the tails, at any tunnel count.
- Host-dialed mode loads its host more, so it reaches the bend earlier.
- The front's own saturation was not reached: it used at most 1.1 cores in direct mode and 1.5 in host-dialed mode.

**What it implies for a per-front limit.**
1. **Size the front by token rate per front core, not by tunnels.** To keep the front's processes under 70% of C dedicated cores, the offered rate must satisfy `tokens/s ≤ 1000 × (0.7 C − c − b × tunnels) ÷ a`. On one front core:
   - direct mode: about 7,000 tokens/s at 100 tunnels and 5,400 at 300, that is 270–350 streams of 20 tokens/s;
   - host-dialed mode: about 4,100 tokens/s at 100 tunnels, before ssh's encryption.

   This is a linear extrapolation beyond the 1.1 cores measured in direct mode. S1c's link rule still applies on top: payload bytes/s ÷ about 305 bytes per token.
2. **No tunnel-count cap is needed for hundreds of tunnels.** No tunnel dropped and none was slow to register, even on a saturated host. A count cap would only stand in for the CPU budget above.
3. **Descriptors must fit.** frps needs at least tunnels + streams in flight + about 50 open files, the listener 2 × tunnels + about 50, and the controller 9 per host-dialed Allocation. Go programs raise their soft limit to the hard limit at start (since Go 1.19), so the hard limit is what counts. The runners' 65,536 was ample; with a hard limit of 4,096, frps would fit about 4,000 tunnels plus streams in flight.
4. **Host-dialed mode at this scale is hypothetical today.** HBEP-6 lets a host-dialed Provider hold 2 Allocations, with one such Provider per login host, so 300 host-dialed tunnels would need 150 login hosts. The measurement matters for a relay keeper (open question 13) or a raised cap. Per tunnel it costs about twice direct mode's fixed cost, 1.4 times its cost per token, and two ssh processes on the front host.

## Proposed HBEP-6 text for open question 11

For the science agent to amend. It replaces the item's current text.

> 11. **Many tunnels on one front.** **Answered for beta's sizing** (spike S14, issue #313): on loopback through S1c's emulator, on three 4-CPU hosted runners, with 1 to 300 frpc tunnels at 1 and 8 streams of 20 tokens/s each, in direct and host-dialed mode:
>     - **CPU is linear in the token rate, with a small term per tunnel, and does not bend up to 300 tunnels.** In direct mode, frps plus the dial-home listener take about 0.09 cores per 1,000 tokens/s plus 0.0007 cores per tunnel. S1c's 7.9–8.7% at one tunnel agrees. In host-dialed mode, the relay owner and its ssh processes raise that to about 0.12 cores per 1,000 tokens/s plus 0.0015 per tunnel, not counting ssh's encryption.
>     - **Memory and descriptors are not limits at this scale.** At 300 tunnels × 8 streams, frps held about 250 MB and 2,700 descriptors (one per tunnel plus one per stream in flight), and the listener 57 MB and 2 descriptors per tunnel. Host-dialed mode adds 9 descriptors in the controller and two ssh processes per Allocation.
>     - **No tunnel dropped.** In 90 runs there was no frps drop, no frpc re-login, no relay restart and no failed probe, including runs where tokens queued for seconds on a saturated host. All 300 tunnels registered within 3 s.
>     - **Latency bends only with the host's CPU.** Tunnels add 1–3 ms to the median time to first token while the host has headroom; the p99 grows once its CPUs near saturation.
>     - **The per-front limit is a token rate per front core, not a tunnel count.** Keep `0.09 × (tokens/s ÷ 1000) + 0.0007 × tunnels` (direct mode; `0.125` and `0.0015` in host-dialed mode, plus ssh) under 70% of the front's cores: about 5,000–7,000 tokens/s per core, 250–350 streams of 20 tokens/s, at 100–300 tunnels. The link still bounds the same rate at payload bytes/s ÷ about 305 bytes per token (operational-readiness item 5). The front's open-files hard limit must cover frps's tunnels plus streams in flight, and the listener's two per tunnel.
>     - **Still open:** the front near full CPU on a host that does not also carry the instances, since S14's runners saturated first; many tunnels sharing a capped link; ssh's encryption cost per relay; and the plugin endpoint under a mass re-login of hundreds of frpc at once.

## Environment

- **Runners** (GitHub-hosted `ubuntu-24.04`, one fresh runner per repetition, on draft PR #323, never merged):

| rep | CPU | CPUs | memory | kernel |
|---|---|---:|---:|---|
| 1 | AMD EPYC 9V45 96-Core Processor | 4 | 15,983 MB | 7.0.0-1017-azure |
| 2 | AMD EPYC 9V74 80-Core Processor | 4 | 15,983 MB | 7.0.0-1017-azure |
| 3 | AMD EPYC 9V74 80-Core Processor | 4 | 15,983 MB | 7.0.0-1017-azure |

- **Runs:** each job took 23–24 min, 10 Nov 2026, 12:20–12:44 UTC. Each runner had Go 1.26.0 (from `go.mod`), Python 3.12.3, open files 65,536 (soft raised to the hard limit), `tcp_tw_reuse` 2 and ports 32768–60999.
- **Downloads:** each runner downloaded the frp and toxiproxy tarballs and checked them against the pins. The extracted toxiproxy-server matched the sum recorded from S1c's extraction, so `TOXI_SERVER_SHA` was checked independently.
- **Priority:** frps, the listener, the owner (and the stand-ins it starts), toxiproxy and the client ran at nice 0; frpc and the node (engines, job sides, relay steps) at nice 10. The front's processes got CPU first, as on a front host that does not carry the instances. Under saturation the instance side starved first, so the latency there is the instance side's as much as the front's.
- **One host, both ends.** Everything ran on one 4-CPU runner: the front, 300 frpc, 300 engine doubles, the client and the emulator. That host's CPU, not the front's, ended every measurement above about 8,000 tokens/s.
- **The development VM** (6 cores, shared) ran only builds and the smoke runs: 1 and 10 tunnels × 1 and 8 streams, both modes, 10 s windows, at nice 19 under `systemd-run --user --scope -p MemoryMax=4G`. The 1-minute load was 1.97 at the start and 3.01 at the end, and every smoke configuration was valid with 0 drops. `/home` stayed above 35 GB free. Nothing that links `internal/rehearsal` was built into this spike or run (`go list -deps -test` counts 0 for all three packages).

## Provenance

| Component | Version and pin | Project and licence | Use |
|---|---|---|---|
| frp (frps, frpc) | v0.71.1 linux_amd64; tarball sha256 `1d987521…f803` (S1c's pin); frps `23227629…6ab3` and frpc `35c6054d…c28b` from `internal/launcher/frp-pins.txt` | [fatedier/frp](https://github.com/fatedier/frp), Apache-2.0 | the tunnels |
| toxiproxy | v2.13.0 linux_amd64; tarball sha256 `08fd7b3d…6b3b4d` (S1c's pin); toxiproxy-server `bf529e8b…de89`, computed on S1c's verified extraction | [Shopify/toxiproxy](https://github.com/Shopify/toxiproxy), MIT | the link |
| fatedier/yamux | v0.2.0, linked through `internal/hostdial` (the version frp v0.71.1 pins) | [fatedier/yamux](https://github.com/fatedier/yamux), MPL-2.0 | the relay sessions |
| `internal/hostdial` | this project, base commit `14f9a318` | this project | the relay owner, job side and relay step, unchanged |
| `internal/agent/enginedouble` | this project, base commit `14f9a318` | this project | the engine double |
| `cmd/s14/frppass.go` | a copy of `internal/front/frppass.go` at `14f9a318`, package renamed | this project | the dial-home listener's passthrough |
| S1c's harness | `spikes/s1c`: the dial, the stream reader, the closed loop, the CPU-over-the-window rule, the process records | this project | adapted in `cmd/s14` and `s14.py` |
| GitHub Actions | `actions/checkout` v7.0.1, `actions/setup-go` v7.0.0 (pinned as in `ci.yml`), `actions/upload-artifact` v7.0.2 (`cf430e03…`) | GitHub, MIT | the hosted runs (a one-off workflow on draft PR #323, never merged) |
| Go, Python | Go 1.27.3 on the VM, the `go.mod` version on the runners; Python 3 standard library only | | the harness and the driver |

- **New:** `cmd/s14` (certs, listener, owner, load), `cmd/s14bridge`, `node/`, `s14.py`, `analysis.py`, `summarize.py`.
- **No third-party code is copied.**

## Deviations from the issue

- **Where it ran.** The measurements ran on GitHub-hosted runners, as the brief asks, because a 300-tunnel load would breach the shared VM's limits. The VM ran only builds and smoke runs at 1 and 10 tunnels. The issue's load ceiling (12) and `/home` floor (20 GB) applied only to the VM: its load stayed at most 3.01, and `/home` above 35 GB.
- **Stand-ins for ssh and srun.** The relay owner is `internal/hostdial.Owner`, unchanged. The ssh process it starts is `s14bridge`, and the "login node, srun, relay step" chain is one TCP hop through the emulator to the node process, which runs `hostdial.Relay` with the connection as its standard streams. The owner and the step are separate processes joined by pipes and TCP, so this is not the "both ends in one process" case that HBEP-6's test plan says proves nothing.
- **The listener, not the whole front.** It is internal/front's `/~!frp` passthrough (copied, since its type is unexported) with the front's dial-home server settings. The whole front needs a control plane to run, and its client-facing proxy is not part of this measurement; the load client stands in for it.
- **The frps admission plugin is not configured.** It acts at logins and new proxies, not in steady state. Its load under a mass re-login is an unknown.
- **The link carries no toxic.** Every tunnel's connection went through one toxiproxy process with no latency and no cap: "many tunnels sharing one link" is the shared emulator here. A shared capped link is an unknown (toxiproxy caps per connection; S1c's shaper shares one rate).
- **Added to the briefed set:**
  - a no-tunnel baseline at every size, the control that separates the host's saturation from the tunnel's cost;
  - the front's probe every 10 s per tunnel and the controller's list read every 10 s, both part of a tunnel's steady cost in HBEP-6.
- **One tunnel per Allocation** in host-dialed mode, so two relays per tunnel, the most there can be. 300 host-dialed tunnels exceed what one front can serve under HBEP-6 today (2 Allocations per host-dialed Provider). They were measured for the per-tunnel cost.
- **New connection per request,** as in S1c. The front may reuse idle tcpmux connections (HBEP-6, The front's half), which would cost less per request than measured.

## Unknowns

- **The front near full CPU.** The runners saturated first, with the front at 1.1 cores (direct mode) or 1.5 (host-dialed). The per-front rate in the Verdict is a linear extrapolation. A front host that does not carry the instances, or a larger runner, would show where frps itself bends.
- **ssh's encryption and memory per relay.** The stand-in copies bytes in the clear. S1c's ssh arm (ssh plus sshd at 7.5–7.8% of a core per 1,000 tokens/s, both ends) suggests a few % more per 1,000 tokens/s on the front host in host-dialed mode.
- **A capped link shared by many tunnels.** Fairness between tunnels, and heartbeats behind a binding shared cap, are untested. S1c's rule for one tunnel (throughput = cap ÷ bytes per token) is expected to hold for the sum.
- **The plugin endpoint under a mass re-login.** After an frps or front restart, 300 frpc log in within about 3 s. Each Login and NewProxy is a call to the lifecycle controller's plugin endpoint, which was not in the path.
- **The agent's control traffic** per Allocation, through the listener (direct mode) or the control relay (host-dialed mode).
- **Several Instances per Allocation,** whose frpc connections would share one serving relay's session.
- **Real round trips.** Everything ran on loopback.

## Files and how to run

| File | What |
|---|---|
| `fetch.sh` | frp and toxiproxy, sha256-checked, into `$S14_BIN`; builds `s14`, `s14bridge` and `s14node.test` |
| `cmd/s14/` | `certs`, `listener`, `owner`, `load`, with unit tests |
| `cmd/s14bridge/` | the ssh stand-in (standard library only) |
| `node/` | the node side, a test binary (`go test -c`), with unit tests |
| `s14.py` | the driver: `run`, `host`, `leftovers` |
| `analysis.py`, `tests/test_analysis.py` | the result rules and their tests |
| `summarize.py` | `extract` (raw files to `tunnels.csv` and `hists.json`), `tables`, `fits` |
| `results/rep{1,2,3}/` | `runs.csv` (one row per configuration), `tunnels.csv`, `hists.json` |

```sh
export S14_BIN=<dir> S14_WORK=<scratch> S14_RESULTS=<dir>
bash fetch.sh
(go vet ./... && go test ./... && python3 -m unittest discover -s tests)
python3 s14.py run --rep 1            # every configuration, about 40 min on a 4-CPU runner
python3 s14.py leftovers
python3 summarize.py extract $S14_RESULTS/raw results/rep1 && cp $S14_RESULTS/runs.csv results/rep1/
python3 summarize.py tables; python3 summarize.py fits
```

Ports: 29920 frps bind, 29921 frps tcpmux, 29922 frps web, 29923 the dial-home listener, 29924 toxiproxy's API, 29925 the link, 29926 the node's relay acceptor, 29927 the control channel's port (nothing listens; nothing dials it). The job sides' listeners are kernel-chosen. Every run checks, with `ss`, that every listener its processes hold is on 127.0.0.1, and that nothing is left in 29920–29939 afterwards.
