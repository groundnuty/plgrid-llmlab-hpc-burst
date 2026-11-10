# Spike S14: many tunnels on one front (HBEP-6, open question 11)

Issue #313. One front host, one frps and the front's dial-home listener, with 1 to 300 frpc tunnels, in direct and host-dialed mode, on loopback through S1c's link emulator. The measurements ran on GitHub-hosted runners, three repetitions on three fresh runners; the shared development VM ran only builds and smoke runs at 1 and 10 tunnels.

## Findings

No steady-state tunnel-count knee was observed through 300 tunnels under these conditions: loopback, one unimpaired shared emulator, steady load after a fresh start, and no admission plugin. The measured stack's CPU grew linearly with the token rate, plus a small cost per tunnel, and no tunnel dropped. Every figure below is from three 4-CPU hosted runners (Environment): two AMD EPYC 9V74 and one Intel Xeon Platinum 8573C. "Valid" means every tunnel received a token in every second of the 30 s window.

- **The measured stack's CPU is linear in the token rate.** It is fitted per runner over the valid runs whose host was not CPU-bound (`summarize.py fits`), as `cores ≈ a × (tokens/s ÷ 1000) + b × tunnels + c`:
  - **direct mode** (frps plus the dial-home listener): a = 0.081–0.083 on the AMD runners and 0.049 on the Intel one; b = 0.39–0.64 thousandths of a core; c = 0.016–0.022. At 50 tunnels × 8 streams that is 8.7% of a core per 1,000 tokens/s on the AMD runners (5.8% on the Intel one), in line with S1c's 7.9–8.7% for one tunnel.
  - **host-dialed mode** (plus the relay owner and the ssh stand-ins): a = 0.128–0.134 (AMD) and 0.079 (Intel); b = 0.80–1.56 thousandths; c = 0.026–0.044. That is 1.6 times direct per token, without ssh's encryption, which the stand-in does not do.
- **b is a fitted coefficient, not the idle cost.** The idle phase measured the idle cost directly: every tunnel registered, no stream, only keepalives, heartbeats, the probe and the list read.
  - In direct mode, idle cost 0.21–0.23 thousandths of a core per tunnel at 10–300 tunnels (0.18–0.27 over the runners), 0.06 cores in all at 300.
  - In host-dialed mode, 0.31–0.46 thousandths, 0.09 cores at 300.
  - The fitted b is 2–3 times the direct idle cost and 2.5–5 times the host-dialed one. Under load a tunnel costs more than its idle traffic, for example because a tunnel with one stream batches less per write.
- **Memory and descriptors are not limits at this scale.**
  - frps held 96–97 MB at 300 tunnels × 1 stream and 243–257 MB at 300 × 8. The listener held 52–60 MB.
  - Descriptors: frps holds one per tunnel plus one per stream in flight (2,720–2,766 at 300 × 8); the listener two per tunnel (608 at 300); the relay owner 9 per Allocation (2,707 at 300); each ssh stand-in 9 descriptors and 3 MB.
- **Latency: the tunnel adds little while the host has headroom.** Through 300 tunnels at 1 stream and 50 tunnels at 8 streams, against the no-tunnel baseline on the same host:
  - direct mode added 1–2 ms to the median TTFT, at most 6 ms to its p99, and at most 2 ms to the gap's p99;
  - host-dialed mode added 2–4 ms to the median, and 10 ms (50 × 8) to 16 ms (300 × 1) to the p99.
- **The bend came from the runner, not the front.** The runners carried both ends on 4 CPUs.
  - At 100 tunnels × 8 streams (15,800 tokens/s offered), the AMD runners were 96–98% busy. They delivered 2–3% less than offered in direct mode and 11–12% less in host-dialed mode, and host-dialed TTFT p99 reached 157 ms.
  - On the faster Intel runner the same configuration delivered 99.8% of the offered tokens or more, at 71% busy (direct mode) and 90% (host-dialed mode).
  - At 100 × 8 on the AMD runners, the front used 1.1 of the 3.8–3.9 busy cores in direct mode. frpc took 1.4–1.5, toxiproxy 0.5, and the engines and client 0.8.
  - At 300 × 8 (47,000 tokens/s offered), the AMD runners delivered 16,000 (direct) and 19,100–19,300 (host-dialed) tokens/s, and those runs failed the validity rule. The Intel runner delivered 27,200 and 30,400, and passed.
  - The no-tunnel baseline delivered all 47,400 tokens/s at 28–40% busy.
- **Host-dialed mode bends earlier on the same host.** At 300 × 1 its front processes used 1.24 cores against 0.67 in direct mode (AMD runners), and the host was 81–82% busy against 57–59%.
- **No heartbeat loss.** 90 runs, as in the first run's 90, including the 300 × 8 runs where a tunnel went silent for up to 3.3 s, showed:
  - 0 drops in frps's list, 0 frpc re-logins, 0 relay restarts and 0 `heartbeat timeout` lines;
  - no failed probe, under load (p99 at most 2.5 s, under the 10 s `probeTimeout`) or idle (at least 3 probes per tunnel).
  - All 300 tunnels registered within 2.8 s of the first frpc's start.
- **Implication for a per-front limit:** the steady-state budget is CPU per token, with c and a per-tunnel term, plus reserves for what this spike did not run (Verdict). The rates are estimates for this workload, not guarantees. A tunnel-count cap is not needed for the steady-state cost, but restarts and a capped shared link are still open.

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
- **The idle phase:** at 1 stream per tunnel, every configuration first holds all its tunnels registered for 30 s with no stream: only the tunnels' keepalives and heartbeats, the front's probe of each tunnel every 10 s, and the controller's list read every 10 s. Its CPU is the measured idle cost, separate from the fitted per-tunnel coefficient.

### Measures

- **CPU:** per process, the ticks between the first and last `/proc` sample inside the window (every 2 s), over that time, in cores. **RSS and descriptors:** the window's peak of each role's sum over its processes (`/proc/<pid>/status` VmRSS, `/proc/<pid>/fd`).
- **The front host's roles:** frps, the listener, the relay owner and the ssh stand-ins (two per tunnel). Recorded too, but not front cost: frpc (summed), the node process (engine doubles, job sides, relay steps), the client, toxiproxy, the driver.
- **The host:** `/proc/stat` over the window. A run with all CPUs at least 85% busy is marked host-bound: its latency measures the runner, not the tunnel.
- **Heartbeat loss,** by four witnesses: frps's proxy list (a proxy not online, absent, or with a new `lastStartTime` after readiness); frpc logins beyond the first (its log); relay sessions beyond the first (the owner's `Relays()`); job-side sessions beyond the first (`Job.Sessions`). Plus every `heartbeat timeout` line in frps's and the frpc logs.
- **Readiness:** the time from the first frpc start until frps lists all N proxies online.
- **The samples are kept.** Every sample's per-role PID count, summed ticks, RSS, peak RSS and descriptors are in `results/rep*/samples.csv`, and the host's `/proc/stat` counters, available memory and load in `host.csv`, with each run's window boundaries in `runs.csv` (`window_start_s`, `window_end_s`, `idle_start_s`, `idle_end_s`). Every CPU, memory and descriptor figure can be recomputed from them, and a PID missing from a sample shows as a lower PID count.

### Validity

- **Every tunnel streams throughout the window.** A configuration counts only if every tunnel received at least one token in every second of the window (`analysis.tunnel_validity`, with its unit tests). Registered tunnels that carry no stream prove nothing (issue #313), so the rule reads the tokens each tunnel received, not frps's list.
- The per-tunnel evidence is committed: `results/rep*/tunnels.csv` holds, per run and tunnel, the requests started, the tokens received, the emptiest second and the longest silence.
- **Load is real:** the tables show the requests and tokens per tunnel.
- **A verification that would prove nothing here, and is not used:** a count of registered proxies, or of relays up. Both are recorded, as readiness, but validity is the tokens.

## Results

All tables come from `results/rep{1,2,3}/`, rendered by `python3 summarize.py tables`, and every CPU, memory and descriptor figure can be recomputed from `samples.csv` and `host.csv` beside them. 30 configurations ran per repetition, 90 in all, and every run finished (status `ok`). On every runner nothing was left running afterwards, and no listener stayed in 29920–29939 (`s14.py leftovers`, in each job's log).

### Validity and load

Per configuration: the repetitions run, those valid (every tunnel received a token in every second of the 30 s window), whether any ran on a host-bound runner (all CPUs at least 85% busy), the tokens per second, and per tunnel the requests started in the window and the tokens received (minimum–maximum over all tunnels of all repetitions).

| mode | tunnels | streams/tunnel | runs | valid | host-bound | tok/s | requests per tunnel | tokens per tunnel | emptiest second, tokens | longest silence, ms | failed requests | probes failed |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline | 1 | 1 | 3 | 3 | 0 | 20 | 10–10 | 594–594 | 19–19 | 51–51 | 0 | 0 |
| direct | 1 | 1 | 3 | 3 | 0 | 20 | 10–10 | 594–595 | 19–19 | 52–52 | 0 | 0 |
| host-dialed | 1 | 1 | 3 | 3 | 0 | 20 | 10–10 | 593–594 | 19–19 | 52–53 | 0 | 0 |
| baseline | 10 | 1 | 3 | 3 | 0 | 198 | 10–10 | 593–595 | 19–19 | 51–52 | 0 | 0 |
| direct | 10 | 1 | 3 | 3 | 0 | 197 (197–198) | 10–10 | 591–595 | 19–19 | 52–53 | 0 | 0 |
| host-dialed | 10 | 1 | 3 | 3 | 0 | 197 | 10–10 | 591–592 | 19–19 | 52–54 | 0 | 0 |
| baseline | 50 | 1 | 3 | 3 | 0 | 988 (987–989) | 9–10 | 592–594 | 19–19 | 51–52 | 0 | 0 |
| direct | 50 | 1 | 3 | 3 | 0 | 988 (988–989) | 9–10 | 592–594 | 19–19 | 52–55 | 0 | 0 |
| host-dialed | 50 | 1 | 3 | 3 | 0 | 988 (988–989) | 9–10 | 592–594 | 19–19 | 52–56 | 0 | 0 |
| baseline | 100 | 1 | 3 | 3 | 0 | 1975 (1974–1976) | 9–10 | 592–593 | 19–19 | 51–52 | 0 | 0 |
| direct | 100 | 1 | 3 | 3 | 0 | 1977 (1976–1977) | 9–10 | 592–594 | 19–19 | 52–59 | 0 | 0 |
| host-dialed | 100 | 1 | 3 | 3 | 0 | 1978 (1977–1978) | 9–10 | 592–594 | 19–19 | 52–61 | 0 | 0 |
| baseline | 300 | 1 | 3 | 3 | 0 | 5930 (5926–5935) | 9–10 | 592–594 | 19–19 | 51–54 | 0 | 0 |
| direct | 300 | 1 | 3 | 3 | 0 | 5929 (5928–5932) | 9–10 | 592–594 | 19–19 | 52–68 | 0 | 0 |
| host-dialed | 300 | 1 | 3 | 3 | 0 | 5929 (5922–5936) | 9–10 | 590–594 | 18–19 | 55–116 | 0 | 0 |
| baseline | 1 | 8 | 3 | 3 | 0 | 158 (157–158) | 79–79 | 4720–4727 | 153–155 | 24–34 | 0 | 0 |
| direct | 1 | 8 | 3 | 3 | 0 | 158 (158–159) | 79–79 | 4727–4765 | 155–158 | 16–31 | 0 | 0 |
| host-dialed | 1 | 8 | 3 | 3 | 0 | 158 (157–158) | 79–79 | 4722–4733 | 155–157 | 16–25 | 0 | 0 |
| baseline | 10 | 8 | 3 | 3 | 0 | 1583 (1581–1584) | 74–80 | 4743–4752 | 156–158 | 13–31 | 0 | 0 |
| direct | 10 | 8 | 3 | 3 | 0 | 1581 (1581–1582) | 74–80 | 4744–4746 | 153–156 | 21–39 | 0 | 0 |
| host-dialed | 10 | 8 | 3 | 3 | 0 | 1581 (1581–1582) | 74–80 | 4742–4746 | 153–157 | 23–44 | 0 | 0 |
| baseline | 50 | 8 | 3 | 3 | 0 | 7906 (7900–7913) | 72–80 | 4740–4750 | 152–158 | 20–44 | 0 | 0 |
| direct | 50 | 8 | 3 | 3 | 0 | 7904 (7901–7909) | 72–80 | 4738–4747 | 152–157 | 22–46 | 0 | 0 |
| host-dialed | 50 | 8 | 3 | 3 | 0 | 7907 (7906–7915) | 72–80 | 4740–4752 | 152–157 | 24–49 | 0 | 0 |
| baseline | 100 | 8 | 3 | 3 | 0 | 15815 (15809–15817) | 72–80 | 4742–4748 | 152–156 | 21–49 | 0 | 0 |
| direct | 100 | 8 | 3 | 3 | 2 | 15508 (15337–15805) | 72–80 | 4588–4744 | 144–157 | 22–62 | 0 | 0 |
| host-dialed | 100 | 8 | 3 | 3 | 3 | 14082 (13903–15785) | 64–80 | 4146–4739 | 115–156 | 29–121 | 0 | 0 |
| baseline | 300 | 8 | 3 | 3 | 0 | 47377 (47358–47391) | 72–80 | 4729–4745 | 152–156 | 25–62 | 0 | 0 |
| direct | 300 | 8 | 3 | 1 | 3 | 16036 (15971–27247) | 18–49 | 1316–2812 | 0–82 | 86–3340 | 0 | 0 |
| host-dialed | 300 | 8 | 3 | 1 | 3 | 19258 (19086–30422) | 24–56 | 1773–3122 | 0–74 | 379–2168 | 0 | 0 |

### The front host's cost

Median over the repetitions (range in brackets when they differ). CPU is cores averaged over the window; front = frps + listener (+ owner + the ssh stand-ins in host-dialed mode). RSS and descriptors are the window's peak, summed over a role's processes (2 per tunnel for the stand-ins).

| mode | tunnels | streams | frps cores | listener cores | owner cores | stand-ins cores | front cores | front % core per 1k tok/s | frps RSS MB | listener RSS MB | owner RSS MB | stand-ins RSS MB | frps fds | listener fds | owner fds | host busy |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| direct | 1 | 1 | 0.003 (0.002–0.003) | 0.002 (0.001–0.002) | - | - | 0.004 (0.003–0.005) | 21.2 (17.7–23.7) | 27 | 26 | - | - | 12 | 10 | - | 0.01 (0.00–0.01) |
| host-dialed | 1 | 1 | 0.003 | 0.001 (0.001–0.002) | 0.002 (0.002–0.003) | 0.001 | 0.008 (0.007–0.008) | 41.5 (35.9–41.5) | 26 (26–27) | 26 | 25 (25–26) | 6 | 12 | 10 | 16 | 0.01 |
| direct | 10 | 1 | 0.019 (0.018–0.020) | 0.013 (0.011–0.014) | - | - | 0.032 (0.029–0.034) | 16.3 (14.4–17.4) | 30 (30–31) | 27 (27–28) | - | - | 30 | 28 | - | 0.03 (0.02–0.03) |
| host-dialed | 10 | 1 | 0.019 (0.018–0.020) | 0.013 (0.011–0.013) | 0.015 (0.014–0.015) | 0.013 (0.010–0.013) | 0.060 (0.052–0.060) | 30.4 (26.6–30.5) | 31 (30–31) | 27 | 29 (29–30) | 60 | 30 | 28 | 97 | 0.04 (0.03–0.04) |
| direct | 50 | 1 | 0.076 (0.054–0.080) | 0.056 (0.036–0.059) | - | - | 0.133 (0.090–0.139) | 13.4 (9.1–14.1) | 40 (40–42) | 31 | - | - | 110 | 108 | - | 0.11 (0.07–0.11) |
| host-dialed | 50 | 1 | 0.075 (0.061–0.082) | 0.054 (0.039–0.058) | 0.054 (0.043–0.059) | 0.065 (0.042–0.069) | 0.248 (0.185–0.267) | 25.1 (18.7–27.0) | 41 (40–42) | 31 | 40 (40–41) | 301 | 110 | 108 | 457 | 0.15 (0.11–0.17) |
| direct | 100 | 1 | 0.141 (0.104–0.146) | 0.108 (0.069–0.111) | - | - | 0.249 (0.173–0.257) | 12.6 (8.7–13.0) | 53 | 36 (35–36) | - | - | 210 | 208 | - | 0.20 (0.14–0.21) |
| host-dialed | 100 | 1 | 0.135 (0.100–0.142) | 0.100 (0.064–0.104) | 0.096 (0.068–0.101) | 0.126 (0.078–0.130) | 0.457 (0.310–0.477) | 23.1 (15.7–24.1) | 52 (51–53) | 35 | 55 (55–58) | 602 (601–602) | 210 (210–211) | 208 | 907 | 0.29 (0.19–0.30) |
| direct | 300 | 1 | 0.374 (0.253–0.389) | 0.298 (0.174–0.307) | - | - | 0.673 (0.427–0.696) | 11.3 (7.2–11.7) | 97 (96–97) | 53 | - | - | 610 (610–611) | 608 | - | 0.57 (0.35–0.59) |
| host-dialed | 300 | 1 | 0.355 (0.230–0.362) | 0.279 (0.154–0.281) | 0.248 (0.145–0.249) | 0.357 (0.214–0.365) | 1.239 (0.743–1.256) | 20.9 (12.5–21.2) | 97 (96–97) | 54 (53–54) | 120 (107–121) | 1811 (1808–1815) | 610 (610–611) | 608 | 2707 | 0.81 (0.49–0.82) |
| direct | 1 | 8 | 0.013 (0.011–0.014) | 0.009 (0.006–0.010) | - | - | 0.021 (0.018–0.024) | 13.6 (11.1–14.8) | 28 (28–29) | 26 | - | - | 19 | 10 | - | 0.02 (0.01–0.02) |
| host-dialed | 1 | 8 | 0.014 (0.013–0.015) | 0.009 (0.007–0.009) | 0.010 | 0.007 (0.006–0.007) | 0.039 (0.036–0.041) | 24.7 (23.1–26.0) | 29 | 26 | 25 (25–26) | 6 | 19 | 10 | 16 | 0.03 (0.02–0.03) |
| direct | 10 | 8 | 0.095 (0.073–0.098) | 0.070 (0.049–0.072) | - | - | 0.164 (0.122–0.170) | 10.4 (7.7–10.8) | 38 | 28 | - | - | 100 | 28 | - | 0.12 (0.10–0.13) |
| host-dialed | 10 | 8 | 0.085 (0.077–0.085) | 0.059 (0.048–0.059) | 0.051 (0.050–0.051) | 0.051 (0.047–0.051) | 0.246 (0.223–0.246) | 15.5 (14.1–15.6) | 38 | 28 | 29 (29–30) | 60 | 100 | 28 | 97 | 0.15 (0.14–0.16) |
| direct | 50 | 8 | 0.389 (0.268–0.399) | 0.298 (0.187–0.302) | - | - | 0.687 (0.455–0.701) | 8.7 (5.8–8.9) | 67 | 34 | - | - | 460 | 108 | - | 0.56 (0.38–0.58) |
| host-dialed | 50 | 8 | 0.364 (0.235–0.381) | 0.268 (0.157–0.277) | 0.217 (0.134–0.227) | 0.261 (0.177–0.280) | 1.111 (0.704–1.164) | 14.0 (8.9–14.7) | 67 (67–68) | 34 | 42 (41–45) | 301 | 460 | 108 | 457 | 0.73 (0.47–0.76) |
| direct | 100 | 8 | 0.620 (0.481–0.626) | 0.480 (0.342–0.485) | - | - | 1.101 (0.823–1.111) | 7.2 (5.2–7.2) | 102 (102–103) | 41 (40–41) | - | - | 911 (910–911) | 208 | - | 0.96 (0.71–0.98) |
| host-dialed | 100 | 8 | 0.450 (0.434–0.452) | 0.339 (0.293–0.343) | 0.182 (0.177–0.240) | 0.337 (0.335–0.355) | 1.314 (1.301–1.322) | 9.3 (8.4–9.4) | 103 (102–104) | 39 (39–41) | 56 (55–59) | 601 (601–602) | 912 (910–913) | 208 | 907 | 0.97 (0.90–0.98) |
| direct | 300 | 8 | 0.594 (0.593–0.606) | 0.499 (0.448–0.505) | - | - | 1.092 (1.054–1.098) | 6.8 (3.9–6.8) | 246 (243–248) | 58 (58–60) | - | - | 2730 (2720–2736) | 608 | - | 0.99 (0.99–1.00) |
| host-dialed | 300 | 8 | 0.335 (0.334–0.462) | 0.249 (0.249–0.308) | 0.075 (0.075–0.091) | 0.203 (0.201–0.204) | 0.863 (0.862–1.062) | 4.5 (3.5–4.5) | 251 (250–257) | 52 (52–53) | 123 (119–127) | 1811 (1810–1813) | 2763 (2736–2766) | 608 | 2707 | 0.98 (0.97–0.99) |

### Latency

Pooled at 1 ms resolution (each value is a 1 ms bin's middle) over the repetitions whose host was not CPU-bound; where every repetition was host-bound, over all of them, marked "(host-bound)": those rows measure the runner. TTFT runs from before the client's connect, CONNECT included, to the first content token; the engine double waits 50 ms before each token. The baseline dials each engine's socket directly, on the same host under the same load.

| tunnels | streams | mode | reps pooled | TTFT p50 ms | TTFT p99 ms | gap p50 ms | gap p99 ms | probe p99 ms |
|---:|---:|---|---|---:|---:|---:|---:|---:|
| 1 | 1 | baseline | 1,2,3 | 50.5 | 50.5 | 50.5 | 50.5 | 0 (0–1) |
| 1 | 1 | direct | 1,2,3 | 51.5 | 51.5 | 50.5 | 50.5 | 1 |
| 1 | 1 | host-dialed | 1,2,3 | 52.5 | 52.5 | 50.5 | 50.5 | 2 |
| 10 | 1 | baseline | 1,2,3 | 50.5 | 51.5 | 50.5 | 51.5 | 1 (0–1) |
| 10 | 1 | direct | 1,2,3 | 51.5 | 52.5 | 50.5 | 51.5 | 1 (1–2) |
| 10 | 1 | host-dialed | 1,2,3 | 51.5 | 53.5 | 50.5 | 51.5 | 2 |
| 50 | 1 | baseline | 1,2,3 | 50.5 | 51.5 | 50.5 | 51.5 | 0 (0–1) |
| 50 | 1 | direct | 1,2,3 | 51.5 | 53.5 | 50.5 | 52.5 | 2 (2–3) |
| 50 | 1 | host-dialed | 1,2,3 | 52.5 | 54.5 | 50.5 | 52.5 | 4 (2–4) |
| 100 | 1 | baseline | 1,2,3 | 50.5 | 51.5 | 50.5 | 51.5 | 0 |
| 100 | 1 | direct | 1,2,3 | 51.5 | 54.5 | 50.5 | 52.5 | 3 (2–4) |
| 100 | 1 | host-dialed | 1,2,3 | 52.5 | 56.5 | 50.5 | 53.5 | 6 (4–6) |
| 300 | 1 | baseline | 1,2,3 | 50.5 | 51.5 | 50.5 | 51.5 | 1 |
| 300 | 1 | direct | 1,2,3 | 52.5 | 57.5 | 50.5 | 53.5 | 6 (5–7) |
| 300 | 1 | host-dialed | 1,2,3 | 54.5 | 67.5 | 50.5 | 56.5 | 19 (9–20) |
| 1 | 8 | baseline | 1,2,3 | 51.5 | 51.5 | 50.5 | 51.5 | 0 |
| 1 | 8 | direct | 1,2,3 | 51.5 | 52.5 | 50.5 | 51.5 | 1 |
| 1 | 8 | host-dialed | 1,2,3 | 52.5 | 52.5 | 50.5 | 51.5 | 2 |
| 10 | 8 | baseline | 1,2,3 | 50.5 | 51.5 | 50.5 | 51.5 | 0 |
| 10 | 8 | direct | 1,2,3 | 51.5 | 53.5 | 50.5 | 51.5 | 2 |
| 10 | 8 | host-dialed | 1,2,3 | 52.5 | 53.5 | 50.5 | 52.5 | 2 (2–4) |
| 50 | 8 | baseline | 1,2,3 | 50.5 | 52.5 | 50.5 | 51.5 | 1 (1–2) |
| 50 | 8 | direct | 1,2,3 | 52.5 | 57.5 | 50.5 | 53.5 | 5 (4–8) |
| 50 | 8 | host-dialed | 1,2,3 | 54.5 | 62.5 | 50.5 | 55.5 | 11 (9–12) |
| 100 | 8 | baseline | 1,2,3 | 50.5 | 53.5 | 50.5 | 52.5 | 1 |
| 100 | 8 | direct | 3 | 52.5 | 59.5 | 50.5 | 54.5 | 9 |
| 100 | 8 | host-dialed | 1,2,3 (host-bound) | 82.5 | 156.5 | 51.5 | 93.5 | 108 (21–112) |
| 300 | 8 | baseline | 1,2,3 | 50.5 | 59.5 | 50.5 | 57.5 | 4 (4–8) |
| 300 | 8 | direct | 1,2,3 (host-bound) | 363.5 | 1835.5 | 75.5 | 590.5 | 1988 (482–2151) |
| 300 | 8 | host-dialed | 1,2,3 (host-bound) | 1132.5 | 2575.5 | 0.5 | 706.5 | 2469 (931–2530) |

### Idle cost

Every tunnel registered and no stream running, for 30 s before the load at 1 stream per tunnel: the tunnels' keepalives and heartbeats, the front's probe of each tunnel every 10 s and the controller's list read every 10 s. Median over the repetitions (range when they differ), in thousandths of a core.

| mode | tunnels | frps | listener | owner | stand-ins | front | front per tunnel | probes failed | tokens |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| direct | 1 | 0.5 (0.5–0.9) | 0.0 | - | - | 0.5 (0.5–0.9) | 0.50 (0.50–0.91) | 0 | 0 |
| host-dialed | 1 | 0.0 (0.0–0.4) | 0.0 (0.0–0.4) | 0.9 (0.0–0.9) | 0.0 | 0.9 (0.8–0.9) | 0.91 (0.84–0.91) | 0 | 0 |
| direct | 10 | 1.9 (1.5–1.9) | 0.8 (0.4–0.8) | - | - | 2.3 (2.3–2.7) | 0.23 (0.23–0.27) | 0 | 0 |
| host-dialed | 10 | 2.7 (1.9–2.7) | 0.8 | 1.5 (1.1–1.5) | 0.0 | 4.6 (4.2–5.0) | 0.46 (0.42–0.50) | 0 | 0 |
| direct | 50 | 8.4 (7.7–8.8) | 2.7 (2.7–3.1) | - | - | 11.5 (10.4–11.5) | 0.23 (0.21–0.23) | 0 | 0 |
| host-dialed | 50 | 10.3 (9.2–11.5) | 3.1 (2.3–3.1) | 6.1 (5.7–6.1) | 0.0 | 19.5 (17.2–20.6) | 0.39 (0.34–0.41) | 0 | 0 |
| direct | 100 | 17.2 (13.4–18.0) | 6.1 (5.4–6.1) | - | - | 23.4 (18.8–24.1) | 0.23 (0.19–0.24) | 0 | 0 |
| host-dialed | 100 | 18.4 (15.6–18.7) | 5.6 (4.6–5.6) | 10.9 (10.7–12.0) | 0.0 (0.0–0.3) | 35.3 (30.9–36.4) | 0.35 (0.31–0.36) | 0 | 0 |
| direct | 300 | 45.6 (39.3–46.3) | 17.3 (14.5–17.7) | - | - | 62.9 (53.9–63.9) | 0.21 (0.18–0.21) | 0 | 0 |
| host-dialed | 300 | 45.8 (41.3–47.9) | 17.6 (14.6–18.2) | 30.0 (28.5–30.6) | 0.0 (0.0–0.7) | 94.0 (84.4–96.8) | 0.31 (0.28–0.32) | 0 | 0 |

### Heartbeat loss

Summed over the repetitions. A drop is a tunnel frps's proxy list showed not online, or registered again, after readiness (read every 10 s); a re-login is an frpc login beyond its first; a relay restart a relay session beyond its first (the owner's count), and a job re-session the same seen by the job side.

| mode | tunnels | streams | time to all online, s | drops | frpc re-logins | relay restarts | job re-sessions | heartbeat-timeout lines | unreadable lists |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| direct | 1 | 1 | 0.3 (0.2–0.3) | 0 | 0 | - | - | 0 | 0 |
| host-dialed | 1 | 1 | 0.8 (0.2–1.3) | 0 | 0 | 0 | 0 | 0 | 0 |
| direct | 10 | 1 | 0.3 | 0 | 0 | - | - | 0 | 0 |
| host-dialed | 10 | 1 | 1.0 | 0 | 0 | 0 | 0 | 0 | 0 |
| direct | 50 | 1 | 0.5 (0.4–0.5) | 0 | 0 | - | - | 0 | 0 |
| host-dialed | 50 | 1 | 1.2 | 0 | 0 | 0 | 0 | 0 | 0 |
| direct | 100 | 1 | 0.7 (0.6–1.4) | 0 | 0 | - | - | 0 | 0 |
| host-dialed | 100 | 1 | 1.2 (1.1–1.2) | 0 | 0 | 0 | 0 | 0 | 0 |
| direct | 300 | 1 | 1.8 (1.3–2.1) | 0 | 0 | - | - | 0 | 0 |
| host-dialed | 300 | 1 | 2.2 (1.9–2.4) | 0 | 0 | 0 | 0 | 0 | 0 |
| direct | 1 | 8 | 0.2 | 0 | 0 | - | - | 0 | 0 |
| host-dialed | 1 | 8 | 0.5 (0.3–1.0) | 0 | 0 | 0 | 0 | 0 | 0 |
| direct | 10 | 8 | 0.3 | 0 | 0 | - | - | 0 | 0 |
| host-dialed | 10 | 8 | 1.0 (0.8–1.1) | 0 | 0 | 0 | 0 | 0 | 0 |
| direct | 50 | 8 | 0.5 (0.4–0.7) | 0 | 0 | - | - | 0 | 0 |
| host-dialed | 50 | 8 | 1.2 | 0 | 0 | 0 | 0 | 0 | 0 |
| direct | 100 | 8 | 0.7 (0.6–1.0) | 0 | 0 | - | - | 0 | 0 |
| host-dialed | 100 | 8 | 1.1 (1.0–1.1) | 0 | 0 | 0 | 0 | 0 | 0 |
| direct | 300 | 8 | 1.7 (1.4–1.9) | 0 | 0 | - | - | 0 | 0 |
| host-dialed | 300 | 8 | 2.4 (1.9–2.8) | 0 | 0 | 0 | 0 | 0 | 0 |

### Runners

- rep 1: AMD EPYC 9V74 80-Core Processor, 4 CPUs, 15983 MB, Linux 7.0.0-1017-azure
- rep 2: AMD EPYC 9V74 80-Core Processor, 4 CPUs, 15983 MB, Linux 7.0.0-1017-azure
- rep 3: INTEL(R) XEON(R) PLATINUM 8573C, 4 CPUs, 15982 MB, Linux 7.0.0-1017-azure

**How to read the tables:**
- **Validity.** Every configuration up to 100 × 8 is valid in all three repetitions.
  - In those, every tunnel received 18–19 tokens in its emptiest second at 1 stream, and 115–158 at 8 streams. Each started 9–10 requests (1 stream) or 64–80 (8 streams) in the window.
  - 300 × 8 is valid only on the Intel runner, and over all three runners 18–56 requests per tunnel ran instead of 72–80. Those runs are host-bound, and are excluded from the fits and the latency claims.
- **Host-bound runs.** The rule (all CPUs at least 85% busy) flagged both modes at 300 × 8 in every repetition, host-dialed mode at 100 × 8 in every repetition, and direct mode at 100 × 8 on the two AMD runners. Their latency is the runner's. The fits leave them out.
- **The runners differ by CPU model.** The Intel runner (rep 3) took about 40% less CPU per token than the two AMD runners, which agree with each other within 5%. So the coefficients are given per runner, never pooled. In the first run, three AMD runners differed by up to 44% on the same CPU model: a runner's CPU cost is an estimate with that spread.
- **The idle phase** ran only at 1 stream per tunnel: streams change nothing while none runs. Its 30 s held 11–15 samples per run. The stand-ins' idle CPU was below one tick per process (0.36 thousandths of a core over 28 s).

## Verdict

**Where cost bends.** No steady-state tunnel-count knee was observed through 300 tunnels under these conditions. The measured stack's CPU follows `cores ≈ a × (tokens/s ÷ 1000) + b × tunnels + c`, with the per-runner coefficients above.
- **The per-tunnel cost.** The measured idle cost per tunnel is 0.18–0.27 thousandths of a core in direct mode and 0.28–0.50 in host-dialed mode, so 300 idle tunnels cost 0.05–0.06 and 0.08–0.10 cores.
- **Under load a tunnel costs more:** the fitted b (0.39–0.64 and 0.80–1.56 thousandths) is what to budget per tunnel while streams run.
- **The per-token cost** dominates as soon as streams run, and falls as streams per tunnel rise. In direct mode it was 11.3% of a core per 1,000 tokens/s at 300 × 1 and 8.7% at 50 × 8 (AMD runners).
- **Memory grows linearly and stays small.** For the front's processes together, the fit is 0.24 MB per tunnel plus 0.08 MB per stream in flight, plus 55 MB, in direct mode. Host-dialed mode adds about 6.3 MB per tunnel, nearly all of it the two ssh stand-ins per Allocation. A real OpenSSH client is likely heavier than the 3 MB stand-in.

**Where latency bends.** Only where the host's CPUs approached saturation.
- On the AMD runners, which also carried 300 frpc, the engines and the client, that was above about 8,000 tokens/s through tunnels in direct mode, and earlier in host-dialed mode. The Intel runner reached it later.
- Below that, tunnels added a few ms to the median TTFT and to its tails, at any tunnel count measured.
- The front's own saturation was not reached: it used at most 1.1 cores in direct mode and 1.3 in host-dialed mode.

**What it implies for a per-front limit.** The figures below are estimates for this workload: 60-token streams at 20 tokens/s, a new tcpmux connection per request, on loopback. They are not guarantees, and the front's own work and ssh are budgeted as named reserves until they are measured.
1. **Budget the front's CPU, including c and the reserves:**

   `a × (tokens/s ÷ 1000) + b × tunnels + c + R_front + R_ssh ≤ 0.7 × C`, for C front cores, where:
   - **a, b and c** are this spike's per-runner coefficients for the measured stack, b being the fitted per-tunnel term, not the idle cost;
   - **R_front** is the front's own work, which this spike did not run: the client-facing proxy (client TLS, HTTP streaming, usage records), its probe's own requests and, in direct mode, the `/agent/v1/` passthrough. Until it is measured, reserve the listener's share of the stack's per-token cost (39–44% here; use 0.45 × a). The front copies every token once more over TLS to the client, as the listener does for the tunnel, so this is a floor, not a measurement;
   - **R_ssh** is ssh's encryption on the front's relays, in host-dialed mode only. Until it is measured, reserve 0.045 cores per 1,000 tokens/s. That is S1c's ssh arm at its upper bound: ssh plus sshd took 20–21% of a core at about 4,500 tokens/s, and sshd alone added about 1% per 1,000 tokens/s on the front.
2. **On one front core, under that rule:**
   - direct mode: 5,100–9,000 tokens/s at 100 tunnels and 4,100–7,900 at 300 (200–450 streams of 20 tokens/s); the low end is the AMD runners, the high end the Intel one;
   - host-dialed mode: 2,400–3,900 tokens/s at 100 tunnels and 1,000–2,800 at 300.
   - The stack alone, with no reserves, would carry 7,500–13,000 and 5,900–11,400 tokens/s in direct mode.
   - All are linear extrapolations beyond the 1.1 cores measured. S1c's link rule bounds the same rate on top: payload bytes/s ÷ about 305 bytes per token.
3. **No tunnel-count cap follows from the steady-state cost.** Nothing in steady state needed one through 300 tunnels. Two things were not exercised and might:
   - a mass re-login through the admission plugin after a front or frps restart;
   - a capped link shared by many tunnels, which could starve heartbeats while CPU is spare.

   Until both are measured, keep a front at or below the 300 tunnels this spike tested.
4. **Descriptors must fit.**
   - frps needs at least tunnels + streams in flight + about 50 open files, the listener 2 × tunnels + about 50, and the controller 9 per host-dialed Allocation.
   - Go programs raise their soft limit to the hard limit at start (since Go 1.19), so the hard limit is what counts. The runners' 65,536 was ample; with a hard limit of 4,096, frps would fit about 4,000 tunnels plus streams in flight.
5. **Host-dialed mode at this scale is hypothetical today.** HBEP-6 lets a host-dialed Provider hold 2 Allocations, with one such Provider per login host, so 300 host-dialed tunnels would need 150 login hosts. The measurement matters for a relay keeper (open question 13) or a raised cap.

## Proposed HBEP-6 text for open question 11

For the science agent to amend. It replaces the item's current text.

> 11. **Many tunnels on one front.** **Partly answered: steady state only** (spike S14, issue #313). Setup: loopback through S1c's emulator with no impairment, on three 4-CPU hosted runners, with 1 to 300 frpc tunnels at 1 and 8 streams of 20 tokens/s, in direct and host-dialed mode.
>     - **No steady-state tunnel-count knee through 300 tunnels under these conditions.** The measured stack's CPU follows `a × (tokens/s ÷ 1000) + b × tunnels + c`, with coefficients that depend on the runner's CPU:
>       - direct mode (frps plus the dial-home listener): a = 0.05–0.08, b = 0.0004–0.0006 (fitted), c = 0.016–0.022 cores. S1c's 7.9–8.7% at one tunnel agrees.
>       - host-dialed mode (plus the relay owner and its ssh processes, without ssh's encryption): a = 0.08–0.13, b = 0.0008–0.0016, c = 0.026–0.044.
>       - The measured idle cost per tunnel is lower: 0.0002 cores in direct mode and 0.0003–0.0005 in host-dialed mode.
>     - **Memory and descriptors are not limits at this scale.** At 300 tunnels × 8 streams, frps held about 250 MB and 2,700 descriptors (one per tunnel plus one per stream in flight), and the listener 60 MB and 2 descriptors per tunnel. Host-dialed mode adds 9 descriptors in the controller and two ssh processes per Allocation.
>     - **No tunnel dropped in steady state.** In 180 runs there was no frps drop, no frpc re-login, no relay restart and no failed probe, including runs where tokens queued for seconds on a saturated host. All 300 tunnels registered within 3 s of a fresh start.
>     - **Latency bends with the host's CPU, not the tunnel count.** Tunnels add a few ms to the time to first token while the host has headroom; the tails grow once its CPUs near saturation.
>     - **The steady-state per-front budget** keeps `a × (tokens/s ÷ 1000) + b × tunnels + c + R_front + R_ssh` under 70% of the front's cores.
>       - R_front is the front's own proxy and control work, unmeasured; reserve 0.45 × a until it is measured.
>       - R_ssh is ssh's encryption in host-dialed mode, unmeasured; reserve 0.045 cores per 1,000 tokens/s until it is measured.
>       - That is an estimated 4,000–9,000 tokens/s per front core in direct mode at 100–300 tunnels, and 1,000–3,900 in host-dialed mode, for 60-token streams with a new connection per request.
>       - The link still bounds the same rate at payload bytes/s ÷ about 305 bytes per token (operational-readiness item 5). The front's open-files hard limit must cover frps's tunnels plus streams in flight, and the listener's two per tunnel.
>     - **Until the open items are measured, keep a front at or below 300 tunnels.**
>     - **Still open, and each could bound tunnels where steady state does not:**
>       - a mass re-login of hundreds of frpc through the admission plugin after a front or frps restart;
>       - many tunnels sharing a capped link, where heartbeats could starve while CPU is spare;
>       - a dedicated front host near full CPU, since S14's runners saturated first;
>       - ssh's encryption cost per relay;
>       - the front's own proxy cost (R_front).

## Environment

- **The run:** workflow run 42822586816 on draft PR #329, whose head is the spike's commit `b802edd5` plus a one-off workflow (each job logs `spike head b802edd5…`). It took 10 Nov 2026, 13:13–13:42 UTC, 28 min per job.
  - The commit that added these results changed only `README.md` and `results/`, so the harness in this directory is the one that ran.
  - The raw files, with every PID's ticks, stay in the run's artifacts until 24 Nov 2026.
  - The committed `samples.csv` and `host.csv` keep, per sample, each role's PID count, ticks, memory and descriptors, and the host's counters.
- **The first run:** run 42814868491 on draft #323 (head `e2b33f79`, three AMD runners) measured the same matrix without the idle phase. Its findings agree: 0 drops in 90 runs; direct mode a = 0.076–0.109; the same bend at 100 × 8. Its tables were replaced by these, from data that is kept.
- **Runners** (GitHub-hosted `ubuntu-24.04`, one fresh runner per repetition):

| rep | CPU | CPUs | memory | kernel |
|---|---|---:|---:|---|
| 1 | AMD EPYC 9V74 80-Core Processor | 4 | 15,983 MB | 7.0.0-1017-azure |
| 2 | AMD EPYC 9V74 80-Core Processor | 4 | 15,983 MB | 7.0.0-1017-azure |
| 3 | Intel Xeon Platinum 8573C | 4 | 15,982 MB | 7.0.0-1017-azure |

- **Settings:** Go 1.26.0 (from `go.mod`), Python 3.12.3, open files 65,536 (soft raised to the hard limit), `tcp_tw_reuse` 2, ports 32768–60999.
- **Downloads:** each runner downloaded the frp and toxiproxy tarballs and checked them against the pins. The extracted toxiproxy-server matched the sum recorded from S1c's extraction.
- **Priority:** frps, the listener, the owner (and the stand-ins it starts), toxiproxy and the client ran at nice 0; frpc and the node (engines, job sides, relay steps) at nice 10. The front's processes got CPU first, as on a front host that does not carry the instances. Under saturation the instance side starved first, so the latency there is the instance side's as much as the front's.
- **One host, both ends.** Everything ran on one 4-CPU runner: the front, 300 frpc, 300 engine doubles, the client and the emulator. That host's CPU, not the front's, ended every measurement that bent.
- **The development VM** (6 cores, shared) ran only builds and the smoke runs: 1 and 10 tunnels × 1 and 8 streams, both modes, 10 s windows, at nice 19 under `systemd-run --user --scope -p MemoryMax=4G`. The 1-minute load was at most 3.01, and every smoke configuration was valid with 0 drops. `/home` stayed above 35 GB free. Nothing that links `internal/rehearsal` was built into this spike or run (`go list -deps -test` counts 0 for all three packages).

## Provenance

| Component | Version and pin | Project and licence | Use |
|---|---|---|---|
| frp (frps, frpc) | v0.71.1 linux_amd64; tarball sha256 `1d987521…f803` (S1c's pin); frps `23227629…6ab3` and frpc `35c6054d…c28b` from `internal/launcher/frp-pins.txt` | [fatedier/frp](https://github.com/fatedier/frp), Apache-2.0 | the tunnels |
| toxiproxy | v2.13.0 linux_amd64; tarball sha256 `08fd7b3d…6b3b4d` (S1c's pin); toxiproxy-server `bf529e8b…de89`, computed on S1c's verified extraction | [Shopify/toxiproxy](https://github.com/Shopify/toxiproxy), MIT | the link |
| fatedier/yamux | v0.2.0, linked through `internal/hostdial` (the version frp v0.71.1 pins) | [fatedier/yamux](https://github.com/fatedier/yamux), MPL-2.0 | the relay sessions |
| `internal/hostdial` | this project, base commit `14f9a318` | this project | the relay owner, job side and relay step, unchanged |
| `internal/agent/enginedouble` | this project, base commit `14f9a318` | this project | the engine double |
| `cmd/s14/frppass.go` | a copy of `internal/front/frppass.go` at `14f9a318`, package renamed; `cmd/s14/parity_test.go` fails if the two diverge, so no rerun measures a passthrough the front no longer has | this project | the dial-home listener's passthrough |
| S1c's harness | `spikes/s1c`: the dial, the stream reader, the closed loop, the CPU-over-the-window rule, the process records | this project | adapted in `cmd/s14` and `s14.py` |
| GitHub Actions | `actions/checkout` v7.0.1, `actions/setup-go` v7.0.0 (pinned as in `ci.yml`), `actions/upload-artifact` v7.0.2 (`cf430e03…`) | GitHub, MIT | the hosted runs (one-off workflows on draft PRs #323 and #329, never merged) |
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

- **The front near full CPU.** The runners saturated first, with the front at 1.1 cores (direct mode) or 1.3 (host-dialed). The per-front rate in the Verdict is a linear extrapolation. A front host that does not carry the instances, or a larger runner, would show where frps itself bends.
- **ssh's encryption and memory per relay.** The stand-in copies bytes in the clear. The Verdict's reserve R_ssh (0.045 cores per 1,000 tokens/s) rests on S1c's ssh arm: frps plus sshd took 7.5–7.8% of a core per 1,000 tokens/s against 6.6–6.9% for frps alone, and ssh plus sshd together 20–21% of a core at about 4,500 tokens/s.
- **The front's own proxy and control work.** The client-facing proxy, its usage records and the agent passthrough were not run. The Verdict's reserve R_front (0.45 × a, the listener's share) is a floor, not a measurement.
- **A capped link shared by many tunnels.** Fairness between tunnels, and heartbeats behind a binding shared cap, are untested. S1c's rule for one tunnel (throughput = cap ÷ bytes per token) is expected to hold for the sum.
- **The plugin endpoint under a mass re-login.** After an frps or front restart, 300 frpc log in within about 3 s. Each Login and NewProxy is a call to the lifecycle controller's plugin endpoint, which was not in the path.
- **The agent's control traffic** per Allocation, through the listener (direct mode) or the control relay (host-dialed mode).
- **Several Instances per Allocation,** whose frpc connections would share one serving relay's session.
- **Real round trips.** Everything ran on loopback.

## Files and how to run

| File | What |
|---|---|
| `fetch.sh` | frp and toxiproxy, sha256-checked, into `$S14_BIN`; builds `s14`, `s14bridge` and `s14node.test` |
| `cmd/s14/` | `certs`, `listener`, `owner`, `load`, with unit tests and the passthrough's parity test |
| `cmd/s14bridge/` | the ssh stand-in (standard library only) |
| `node/` | the node side, a test binary (`go test -c`), with unit tests |
| `s14.py` | the driver: `run`, `host`, `leftovers` |
| `analysis.py`, `tests/` | the result rules and the sample writer, with their tests |
| `summarize.py` | `extract` (raw files to `tunnels.csv` and `hists.json`), `tables`, `fits` |
| `results/rep{1,2,3}/` | `runs.csv` (one row per configuration), `tunnels.csv`, `hists.json`, `samples.csv`, `host.csv` |

```sh
export S14_BIN=<dir> S14_WORK=<scratch> S14_RESULTS=<dir>
bash fetch.sh
(go vet ./... && go test ./... && python3 -m unittest discover -s tests)
python3 s14.py run --rep 1            # every configuration, about 40 min on a 4-CPU runner
python3 s14.py leftovers
python3 summarize.py extract $S14_RESULTS/raw results/rep1 && cp $S14_RESULTS/{runs,samples,host}.csv results/rep1/
python3 summarize.py tables; python3 summarize.py fits
```

Ports: 29920 frps bind, 29921 frps tcpmux, 29922 frps web, 29923 the dial-home listener, 29924 toxiproxy's API, 29925 the link, 29926 the node's relay acceptor, 29927 the control channel's port (nothing listens; nothing dials it). The job sides' listeners are kernel-chosen. Every run checks, with `ss`, that every listener its processes hold is on 127.0.0.1, and that nothing is left in 29920–29939 afterwards.
