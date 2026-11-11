# Spike S16: open question 11's open items (HBEP-6)

Issue #412. HBEP-6's open question 11 keeps a front at or below 300 tunnels until five items are measured; spike S14 measured steady state only. This spike measures the four that need no GPU, on one host, on loopback:

1. a mass re-login of 100, 300 and 600 frpc through both of frps's server plugins (the lifecycle controller's admission plugin and the front's), after an frps restart and, separately, after a front restart;
2. 100 and 300 tunnels sharing a capped link at 50, 100 and 200 Mb/s, and below, to the cap where heartbeats first fail;
3. real ssh's CPU per relay in host-dialed mode (`R_ssh`);
4. the front's own proxy cost (`R_front`).

The measurements ran on GitHub-hosted runners (Environment). The shared development VM ran only builds and smoke runs at 1 to 10 tunnels.

## Findings

No tunnel failed to come back, no plugin call came near HBEP-6's 1 s rule, and no heartbeat failed on a full shared link. What bounds a front's tunnel count is the front's own router: every registration change rebuilds its whole view under its lock (#417), so client-visible recovery after a mass re-login grows about as N². Every figure is from 15 jobs on fresh 4-CPU GitHub-hosted runners, 3 repetitions per item (Environment).

- **Mass re-login: every tunnel came back, at every size.** Three times per size, over the runs that count: 3 per cell, but 2 at 600 tunnels, since every 600-tunnel run on the Intel 8573C runner had a pre-window second in which no tunnel received a token (Validity):

  | tunnels | 1 | 10 | 100 | 300 | 600 |
  |---|---:|---:|---:|---:|---:|
  | re-registered, max s (both plugins answered `NewProxy`, so frps registers; witness: the plugins' call logs) | 2.0–2.1 | 2.2 | 2.2–2.3 | 2.4–2.8 | 2.7–3.4 |
  | routed again, max s, front restart (a probe passed in the new epoch; witness: the front's probe log) | 2.0–2.1 | 2.2–2.3 | 2.5–2.9 | 6.1–10.2 | 10.5 |
  | routed again, max s, frps restart | 2.1 | 2.2 | 3.0–3.7 | 7.0–8.0 | 5.0–6.0 |
  | first token through the front, max s, front restart (witness: the client) | 2.5–2.8 | 2.6–2.7 | 3.0–3.5 | 6.7–10.8 | 22.2–25.7 |
  | first token through the front, max s, frps restart | 30.2–30.6 | 30.6–30.7 | 30.9–31.3 | 34.4–35.8 | not reached in 44 s: 223 and 600 of 600 tunnels had none |
  | first token through the front, max s, frps restart, hold-off client (no request until 3 s after a cut) | 3.2 | 3.2 | 3.2–4.0 | 10.3–16.9 | 38.4; and in the other run not reached in 44 s by 600 of 600 |

  - **Re-registration is set by frpc's backoff:** its immediate retry finds frps or the front still down, and the next comes 2.0–2.2 s later (frp facts). Above that floor, 600 tunnels added 1.5 s.
  - **Routed again, past 100 tunnels, is set by the router.** The probe's pass is applied and reported only after its synchronous `Router.Refresh` (`internal/front/probe.go:434`, then `:464`; `tunnel.go:261-290`), and each Refresh rebuilds the whole view under the router's lock (`router.go:71`, `:84-88`, `:95-107`).
    - So N re-registrations run as N rebuilds in a row, each holding the lock; at 600 tunnels every probe's pass was reported at 10.5 s.
    - One Refresh cost 15, 45 and 79 ms at 100, 300 and 600 tunnels on the shared VM (`cmd/s16/refresh_test.go`, `S16_MEASURE_REFRESH=1`): 600 in a row is 47 s there.
  - **First token through the front is set by the same rebuilds, and after an frps restart also by HBEP-5's pause:**
    - **The rebuilds:** a client request needs the router's lock too, so the first request on each tunnel after the cut waited for the rebuilds. Its time to first token was 0.2–1.0 s at 100 tunnels, 3.8–19.3 s at 300 and 19.2–35.3 s at 600, after which each tunnel streamed normally. After an frps restart at 600 tunnels (two refreshes per tunnel, a Bump and a pass), the observed 44 s ended before most tunnels' first request was answered. Filed as #417.
    - **The pause, after an frps restart:** the cut stream, then dials refused while frps was down and set-up 404s before frpc re-registered, made three failures in a row. So the front paused every Instance with three such failures for 30 s (`pauseAfterFailures` 3, `pauseDuration` 30 s): all of them at 1, 10 and 100 tunnels, 166–282 of 300, and 58–221 of 600, starting 0.0–5 s after the cut (fewer at 600, where the router's lock slowed the requests that would have failed).
    - With the client sending 1 request per tunnel, retried at once after a cut and every 50–500 ms while refused, the first token came at 30–36 s up to 300 tunnels. At 600 it did not come within the 44 s observed for 223 and 600 of the 600 tunnels (the two runs that count).
    - The hold-off control paused nothing, and its first token came at 3.2–4.0 s up to 100 tunnels: the pause, not the re-login, cost the 30 s. The science agent ruled that a re-registration and its first passing probe clear the old epoch's pause (#418).
  - **A front restart's TERM to exit** took the 10 s shutdown grace while streams ran, or less when the closed loop's requests had fallen into step and ended sooner (1.7–10.3 s); the tunnels are cut at the exit.
- **The plugins answer far inside HBEP-6's 1 s rule.** Over every call from 1 s before the cut to 90 s after, both plugins, every size: p50 0.03–0.09 ms, p99 at most 38 ms, max 164 ms (the front's `NewProxy` at 600). No call took over 1 s, and none was refused, so the 30 s refused-proxy floor never applied.
- **The front host's CPU in the 10 s after a cut** (frps, the admission plugin and the front) was 0.2–0.4 cores at 100 tunnels, 0.8–1.4 at 300 and 1.1–1.5 at 600, against 0.1–0.2, 0.4–0.6 and 1.2–1.6 with no event. Its busiest second reached 1.9–2.1 cores at 600.
  - That includes the client's retries: the hold-off control, which sends none, took 1.1–1.4 cores at 600.
  - One recovery second at 600 crossed 85% busy.
- **No heartbeat failed on a full shared link,** under either queue, at any cap:
  - 100 tunnels × 4 streams and 300 × 1, at 200 down to 12.5 Mb/s, where the tunnels' own token bytes reached the cap;
  - with filler bytes holding the link at its cap: every counted run's every second at 95% of the cap or more, both ways;
  - 0 proxies dropped from frps's list, 0 frpc re-logins, 0 heartbeat-timeout lines and 0 failed probes, in 94 counted runs (88 capped, 6 uncapped controls);
  - two runs are excluded, each leaving its configuration 2 repetitions: one at 300 × 1, 200 Mb/s and per-connection queues, which never sat at its cap (the issue's "proves nothing"); and one at 100 × 4 on the same link, whose host was 86% busy (a heartbeat finding counts only under 85%).
- **The queue sets latency, not heartbeat loss.** With link capacity to spare for the tokens:
  - the gap p99 stayed at 55–96 ms at 25 Mb/s and above, under all three queues;
  - the 1 s buffer instead delayed each request: the closed loops delivered 59% of their offered tokens at every cap, as 3 s requests with about 2 s of queueing would (once up the link, once down), and the 300 × 1 runs failed S14's every-second rule, since each tunnel then waits about 2 s between requests;
  - at 12.5 Mb/s, where the tunnels' demand binds, the gap p99 rose to 136–531 ms;
  - yamux's 10 s ping timeout is far above any of these delays.
- **R_ssh, real OpenSSH in host-dialed mode, is a per-packet cost.**
  - The cipher made no difference: OpenSSH's default (chacha20-poly1305, KEX sntrup761x25519-sha512, as the runners' sshd negotiated), aes128-gcm and aes256-gcm agree within each runner's noise.
  - Per 1,000 tokens/s, ssh cost 0.05–0.08 cores with relays carrying 20 tokens/s each (one stream), 0.035–0.064 at 80, 0.022–0.046 at 320 and 0.012–0.022 at 1,260 (4 serving relays).
  - An idle relay cost nothing measurable.
  - ssh plus sshd together at one stream per relay was 0.12–0.16 per 1,000 tokens/s, as S1c's 0.14 at low load.
  - So S14's provisional 0.045 × q holds for relays carrying 80 tokens/s or more, and is too low by about 1.7× for relays that carry one stream.
- **R_front, the front's proxy in the request path, is 3–5 times S14's floor.**
  - The front process's added cores per 1,000 tokens/s were 0.10–0.17 on the AMD runner and 0.05–0.13 on the Intel ones; fitted, 0.12 and 0.11.
  - S14's provisional s × a is 0.037 (AMD) and 0.022 (Intel).
  - frps's own cost fell by 0.001–0.06 cores in the proxied arm, as the front reuses tcpmux connections, so front plus frps adds 0.05–0.15.
- **What it does to one front core** (HBEP-6's 70% rule, S14's per-runner stack fits, `python3 summarize.py budget`):
  - direct mode at 100, 300 and 600 tunnels: about 3,000, 2,400 and 1,450–1,570 tokens/s on the AMD fits, and 4,060, 3,560 and 2,820 on the Intel fit, against S14's provisional 5,150–5,280 and 8,980 at 100;
  - host-dialed mode, with one stream per relay: 1,550–1,570 tokens/s at 100 tunnels and 630–650 at 300 (AMD), and 2,320 and 1,680 (Intel);
  - at 600 host-dialed tunnels S14's per-tunnel term alone uses the AMD core's budget.

## Question

HBEP-6, open question 11, as amended by #342, lists what could bound tunnels where steady state does not: a mass re-login through the admission plugin after a front or frps restart, many tunnels sharing a capped link, ssh's encryption per relay, and the front's own proxy cost, each standing in S14's budget as a provisional term (`R_ssh = 0.045 × q`, `R_front = s × a × q`, with `q` = tokens/s ÷ 1000). The fifth item, a dedicated front host near full CPU, needs a larger host than a hosted runner and is left (issue #412).

## Method

### What is real and what stands in

| Part | In this spike | Where it comes from |
|---|---|---|
| frps, frpc | frp v0.71.1 release binaries, sha256-checked (`fetch.sh`) | S1c's pin; `internal/launcher/frp-pins.txt` |
| frps's configuration | `launcher.WriteFrpsConfigWithFront`, unchanged: HBEP-6's settings with both server plugins, the controller's at base+8 (`Login`, `NewProxy`, `CloseProxy`), then the front's on its health address, base+4 (`NewProxy`, `CloseProxy`) | `internal/launcher/frps.go` |
| frps's process | started, stopped and restarted by the launcher's own code: `ResolveFrpsFile` (the binary checked against the pins), `StartFrps`, `FrpsProc.Stop` (TERM, KILL after 5 s), in `s16 frps`; a restart is a stop and a start on the same file and ports | `internal/launcher/frps_proc.go` |
| frpc's configuration | `agent.FrpcConfig.Render`, unchanged: HBEP-6's `wss` to the dial-home listener, `trustedCaFile`, `serverName` `dial-home.hpcburst.invalid`, `metadatas.token`, one tcpmux proxy named by the Instance's UID with the `unix_domain_socket` plugin, S1's heartbeats | `internal/agent/frpcconf.go` |
| the admission plugin | `frpsplugin.NewHandler` with its defaults (700 ms deadline, 128 in flight), served as the controller serves it (5 s header and write timeouts, 2 min idle), on base+8 (`s16 admit`) | `internal/frpsplugin`, `cmd/hpcburst-controller/tunnel.go` |
| its Index | `frpsplugin.MemIndex`, fed one `Replace` of a fixture snapshot and marked synced: one live Allocation per tunnel, with a fresh `agentapi.NewKey` credential that the tunnel's frpc carries as `metadatas.token`, and that tunnel's Instance placed on it. Stands in for the controller's feeder over its caches, which reads the same from the API server | `internal/frpsplugin/index.go` |
| the front | `front.Run`, unchanged, in its own process (`s16 front`), direct mode (host-dialed in item 3): its Tunnel through frps, its Prober (10 s interval and timeout, 2 failures), its frps plugin on the health address, its proxy on the client listener, its dial-home listener (TLS, the `/~!frp` passthrough), its usage records, its pause rule | `internal/front` |
| the front's routing table | the front's own `inventory.Watch` over client-go's fake dynamic client, seeded with the fixture: one batch PlatformProfile and Provider, and per tunnel one served ModelDeployment with its own model name and one `ready` Instance whose UID is the tunnel's proxy name. No control plane and no Lease (`Kube` nil), as `internal/front/tunnel_run_test.go` runs it | `internal/front/inventory`, `k8s.io/client-go/dynamic/fake` |
| the engine | `internal/agent/enginedouble`, one per tunnel on a Unix socket, its served name the tunnel's model and its API key the Instance's key; 200 ms before each token (5 tokens/s) in item 1, 50 ms (20 tokens/s) elsewhere | this project; linked only by `node/node_test.go` |
| the link | `s16link`: S15's shared-rate shaper (`spikes/s15/cmd/s15link`, itself S1c's) with a shared FIFO queue of a stated buffer added beside its per-connection discipline, and per-second byte counters | new, from S15's |
| the filler (item 2) | `s16 filler`: a few TCP connections through the same link, each writing a fixed random block both ways as fast as the link takes it | new |
| ssh and sshd (item 3) | the system's OpenSSH client, started by `sshrun.NewRelay` and `Relay.Command` with the Provider's `-F` file and `RelayArgs`, unchanged, through a wrapper that only prepends `-v -E <log>` (and `-o Ciphers=` for the bracket); `internal/rehearsal`'s login node, unchanged: a user-level sshd in inetd mode on a kernel-chosen loopback port, its forced command, run-mode fakeslurm with one held job, whose streamed `srun` steps run `hostdial.Relay` | `internal/slurm/sshrun`, `internal/rehearsal` (with #309's fix), `internal/slurm/fakeslurm` |
| the relay owner and job side (item 3) | `hostdial.Owner`, `hostdial.StartJob`, `hostdial.Relay`, unchanged | `internal/hostdial` |
| the client | `s16 load`: closed loop, as in S1c and S14, through the front's client listener with a client key and the tunnel's model name, or (the controls) straight to frps's tcpmux port with the Instance's key | after S14's `cmd/s14/load.go` |
| the observation | the plugins' calls timed at each plugin's listener, the same way for both (`timing.go`); the front's probes and registrations through its `PathEvents` and its Tunnel's subscribers; frps's proxy list every second; `/proc` every second | new |

Not modelled: the controller's list reader and `TunnelConnected` condition (frps's list is read every second instead), the agent's control traffic, client TLS in front of the front, and real round trips.

### Item 1: mass re-login

- **Sizes and events:** 1, 10, 100, 300 and 600 tunnels, each once with no event (the no-restart window), once with an frps restart, once with a front restart, and once with an frps restart under the hold-off client (below).
- **Load:** 1 stream per tunnel through the front's proxy, 60-token requests at 5 tokens/s (12 s each), so a request is in flight on every tunnel across the event. The client retries at once after a request that delivered tokens, and 50 ms (doubling to 500 ms) after one that delivered none. The rate is lower than S14's 20 tokens/s because 600 × 20 tokens/s would saturate a 4-CPU runner (stage 0's extrapolation from S14's `runs.csv`: host busy ≈ 0.065 per 1,000 tokens/s + 0.00057 per tunnel on the AMD runners).
- **The timeline:** every frpc started, every proxy online and every tunnel routed (a passing probe); the client's 5 s ramp; 15 s warm-up; a 20 s pre-window; the event; the recovery (until every tunnel is registered and routed again, at most 90 s); a 40 s post-window.
- **The cut:** for an frps restart, frps's exit (the supervisor's `stopped`); for a front restart, the old front's exit, when its passthrough closes the tunnels after its shutdown grace (10 s by default). The time from TERM to exit is reported apart.
- **Per tunnel, three times from the cut:**
  - *re-registered*: the front's plugin answering an accepted `NewProxy` for the UID (frps registers the proxy only after both plugins answer); witness, the plugins' call logs;
  - *routed again*: the first probe after the cut that passed in the new epoch (the issue's definition); witness, the front's probe log. The prober reports a probe after applying its verdict, and applying it runs the router's Refresh synchronously (`internal/front/probe.go:434`, `:464`), so this time includes the router's rebuild (#417);
  - *first token*: the first content token through the front's proxy (what a client sees), and the time to first token of the first request started after the cut; witness, the client.
- **Not the registration-change log.** `reg-*.jsonl` (the Tunnel's subscriber scans) under-counts "routed again": a Bump and a re-probe that passes milliseconds later can both land before the scan their subscriber call starts, so the scan sees no change. The first tables, before the hosted run was folded in, read it and gave "2–4 s at every size"; the probe log gives 4–11 s at 300 and 600 tunnels.
- **The plugins:** every call's latency at its server, p50, p99 and max, against HBEP-6's 1 s rule; refusals per plugin and op. The latency runs from the request's first byte read to the reply's first write, so it leaves out what frps waits for before that: its connect and the listener's accept queue, both on loopback.
- **The front host's CPU:** frps, the admission plugin and the front, over the 10 s after the cut, and its busiest second.
- **The pause:** per Instance, from the front's own log and usage records: whether the front paused it (HBEP-5's `pauseAfterFailures` 3, `pauseDuration` 30 s; #418), when, until when, and which failures counted, each with its time relative to the cut and to the re-registration.
- **The hold-off control:** the same frps restart with a client that sends nothing for 3 s after a failed or cut request, so no request reaches an Instance between the cut and its re-registration. It shows what the pause costs.
- **Validity:** a run counts only if every tunnel received a token in every second of the pre-window and had a request in flight at the event. Any tunnel not back within 60 s is a finding.

### Item 2: the capped link

- **The emulator:** toxiproxy's bandwidth toxic caps each connection separately (S1c, Method), so 100 tunnels at 50 Mb/s would be 5 Gb/s in all and never a shared cap. The link is therefore S1c's shared-rate shaper as S15 extended it, `s16link`, with one rate per direction shared by every connection.
- **Its queue** (team-lead's ruling, 11 Nov 2026):
  - a shared FIFO queue is primary, at a buffer of 100 ms and of 1 s of the cap: a chunk waits for room in arrival order, then behind every byte booked before it;
  - the per-connection discipline, where each connection has at most one chunk waiting and connections take turns, is the control.

  The emulator terminates TCP, so a full queue holds the sender back rather than dropping.
- **Load:** 100 tunnels × 4 streams and 300 × 1, at 20 tokens/s, straight to frps's tcpmux port (S14's client), the front in the path as the dial-home listener and probe.
- **Feasibility (stage 0):** at about 300 bytes per token, 50, 100 and 200 Mb/s are 20,500, 41,000 and 82,000 tokens/s, and S14's 4-CPU runners were 96–98% busy at 15,800 tokens/s through tunnels. Tokens cannot fill any of the three caps below 85% busy.
  - So the link is filled by **filler bytes on the same link** (team-lead's approval): 4 connections, both ways, beside the tunnels' token streams.
  - "At its cap" is the link's per-second counters: every second of the window at 95% of the cap or more, each direction. The token load offered and the filler's bytes are stated per run.
  - A low cap's chunk is at most 1% of a second's bytes, so a chunk's booked second cannot swing a second's count across the 95% line.
- **The ladder:** an uncapped control, then each discipline from 200 Mb/s down by halves (200, 100, 50, 25, 12.5, 6.25, 3.125). Each ladder stops at its first heartbeat failure, or at the cap where the tunnels' own demand binds (the token bytes offered, at the uncapped control's bytes per token up the link, reach the cap). Both stopping runs are included.
- **A capped run counts** only if every second of its window was at 95% of the cap or more, both ways (the issue's "proves nothing"), and a heartbeat finding only where the host stayed under 85% busy (the brief).
- **Heartbeat witnesses (S14's that apply in direct mode):** a proxy that frps's list shows not online, or registered again, after readiness; frpc logins beyond one per tunnel; `heartbeat timeout` lines in frps's and the frpc logs. The front's failed probes are counted too. The relay witnesses are host-dialed mode's; item 3 runs it.
- **Measured besides:** the inter-token gap at p99, tokens delivered against offered, and host busy.

### Item 3: R_ssh

- **The path:** host-dialed mode, 4 Allocations (8 relays, the 4 serving ones carrying the streams):
  `frpc → job side → relay socket → hostdial.Relay (an srun step of fakeslurm's held job) → fakeslurm's stream → the forced command → sshd → loopback → ssh → hostdial.Owner → the front's dial-home listener → frps`.
- **The cipher:** the Provider's `-F` file sets none, so the cluster's sshd and OpenSSH's defaults decide, and this host cannot show what a cluster negotiates.
  - The bracket (k343's ruling): OpenSSH's default, `aes128-gcm@openssh.com` and `aes256-gcm@openssh.com`.
  - Every relay's negotiated KEX, cipher and MAC come from its own `ssh -v` log.
- **Loads:** a 30 s idle phase (relays up, no stream: the fixed cost per relay), then 1, 4, 16, 32 and 64 streams per tunnel at 20 tokens/s, 30 s each, straight to tcpmux.
- **Measured:** the ssh client processes' CPU (the front host's), and sshd's apart (the login node's). `R_ssh` is the slope of a least-squares fit of ssh cores against tokens/s ÷ 1000 over the idle phase and the valid levels; the intercept over 8 relays is the fixed cost per relay.

### Item 4: R_front

- **Arms:** at 10 × 1, 10 × 8, 50 × 1, 50 × 4, 100 × 1 and 100 × 4 tunnels × streams, at 20 tokens/s, the client goes through the front's proxy, and then, in the control, straight to frps's tcpmux port.
  - In both arms the front runs, with its dial-home listener and probes in the path.
  - In both arms S14's listener share sits inside the front's process.
- **`R_front`** is the front process's cores with the client through its proxy less the same in the control, per 1,000 tokens/s.
- **Also reported:** the change in frps's cores. The front keeps tcpmux connections alive between requests (`upstreamIdleTimeout`), which the control's client does not.

### Measures common to all

- **CPU:** S14's rule. Per process, the ticks between the first and last `/proc` sample inside the window (every 1 s), over that time, in cores. The host's busy fraction comes from `/proc/stat`.
- **Validity:** S14's rule. Every tunnel received a token in every second of the window (`analysis.tunnel_validity`).
- **Priorities:** frps, the admission plugin, the front, the link, the client and the filler run at nice 0; frpc and the node at nice 10, as in S14.
- **Signals:** every process is started by the driver and signalled only through a pidfd opened right after its start. No PID is signalled by its number, and no process by name.

### Proves nothing here, and not used

- A re-login time measured on tunnels that weren't streaming: a run counts only with every tunnel streaming in every second of the pre-window and a request in flight at the event.
- A capped-link run whose cap the emulator never reached: every capped row shows its link's smallest second against the cap.
- A count of registered proxies or relays standing in for tokens: validity reads the tokens.
- A fixed sleep as a wait: every wait is on observable state with a deadline (frps's list, the front's registrations, the plugins' calls, ready files). The driver's fixed spans are the measurement windows themselves.

## frp facts this rests on

Read at frp v0.71.1 (`fatedier/frp`, tag `v0.71.1`).

| Fact | Source |
|---|---|
| When a session ends, frpc logs in again at once; after a failed attempt it waits 2× the last wait with up to 10% jitter, from 1 s (so 2.0–2.2 s, 4.2–4.8 s, 8.4–10.6 s, …), at most 20 s | `client/service.go:289-329` (`keepControllerWorking`), `:331-390` (`loopLoginUntilSuccess`, `MaxDuration: maxInterval`, 20 s); `pkg/util/wait/backoff.go:77-123`, `:126-168` (`BackoffUntil`, sliding) |
| The 200 ms fast retries apply only after a login that succeeded and whose control then closed again | `client/service.go:300-326`; `pkg/util/wait/backoff.go:91-101` |
| So when frps or the front is back within about 2 s, every frpc is back 2.0–2.2 s after the cut: the floor under every re-login time here | from the two rows above |
| A refused proxy is retried only after 30 s | `client/proxy/proxy_wrapper.go:50`, `:205-213` |

## Results

All tables come from `results/`, rendered by `python3 summarize.py tables`. `python3 summarize.py verify` recomputes from `samples.csv.xz` and `host.csv.xz` every CPU figure in the run tables: 852 figures, 0 mismatches. 201 runs finished, every one with status `ok`: 60 re-login, 96 capped-link, 9 R_ssh and 36 R_front. On every runner nothing was left running, and no listener stayed in 30130–30159 (`s16.py leftovers`, in each job's log).

### Validity and the checks that prove nothing

- **Every tunnel streamed before each restart:** a re-login run counts only if every tunnel received a token in every second of the 20 s pre-window and had a request in flight at the event.
  - 56 of 60 runs count.
  - The 4 that don't are every 600-tunnel run on the Intel 8573C runner (one per event), where one pre-window second had no token on any tunnel.
  - Every counted run had a request in flight on every tunnel at the event, and its cut is recorded (the "cut" column; after a front restart the streams that ended within the shutdown grace are not cut).
- **Every second of each capped window at 95% of the cap or more,** both ways, from the link's own counters (the "link up min" and "link down min" columns, the smallest second's share of the cap). One run fails it and is excluded, as is one host-bound run (the counted table below).
- **No count of registered proxies or relays stands in for tokens:** validity reads the tokens. Registration is a witness to a time, not to a tunnel streaming.
- **No fixed sleep is a wait:** the driver waits on frps's list, the front's registrations, the plugins' calls and ready files, each with a deadline.
- **Host-bound levels** (all CPUs at least 85% busy) are left out of the R_ssh fits (the 64-stream level on the AMD 7763 runner) and the R_front pairs (100 × 4 on the AMD 7763 runner).

### Re-login

Ranges over the runs that count (valid); runs that don't are in the runs and valid columns.

| event | tunnels | runs | runners | valid | host-bound second in the recovery | emptiest pre-second | in flight at the cut | cut | registered again max s | routed again p50 s | routed again max s | first token p50 s | first token max s | first request's TTFT p50 s | first request's TTFT max s | not routed again in 60 s | paused | front TERM to exit s | front host cores (10 s) | peak front host cores | peak host busy |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| front | 1 | 3 | AMD/Intel | 3 | 0 | 5 | 1 | 0 | 2.00–2.10 | 2.01–2.10 | 2.01–2.10 | 2.50–2.75 | 2.50–2.75 | 0.20 | 0.20 | 0 | 0 | 8.3–8.5 | 0.00 | 0.01–0.02 | 0.01 |
| front | 10 | 3 | AMD/Intel | 3 | 0 | 4 | 10 | 4 | 2.16–2.20 | 2.05–2.14 | 2.17–2.25 | 2.58–2.63 | 2.62–2.65 | 0.20 | 0.20–0.21 | 0 | 0 | 10.0 | 0.02–0.03 | 0.05–0.07 | 0.02–0.04 |
| front | 100 | 3 | AMD/Intel | 3 | 0 | 4 | 100 | 40–41 | 2.24–2.26 | 2.31–2.48 | 2.53–2.85 | 2.69–3.09 | 2.96–3.54 | 0.42–0.81 | 0.45–0.88 | 0 | 0 | 10.0–10.1 | 0.18–0.26 | 0.81–0.95 | 0.29–0.35 |
| front | 300 | 3 | AMD/Intel | 3 | 0 | 4 | 300 | 0–231 | 2.44–2.62 | 4.13–6.28 | 6.05–10.21 | 6.10–10.40 | 6.70–10.76 | 3.83–8.24 | 4.14–8.59 | 0 | 0 | 1.7–10.1 | 0.78–1.25 | 1.48–1.52 | 0.52–0.68 |
| front | 600 | 3 | AMD/Intel | 2 | 1 | 4 | 600 | 0 | 2.69–3.05 | 10.46–10.47 | 10.53–10.54 | 21.49–25.57 | 22.22–25.74 | 19.15–22.11 | 19.74–23.74 | 0 | 0 | 3.3–9.0 | 1.24–1.34 | 1.75–1.95 | 0.74–0.94 |
| frps | 1 | 3 | AMD/Intel | 3 | 0 | 5 | 1 | 1 | 2.05–2.12 | 2.05–2.12 | 2.05–2.12 | 30.20–30.59 | 30.20–30.59 | 0.20 | 0.20 | 0 | 1 | - | 0.01 | 0.02–0.05 | 0.01–0.02 |
| frps | 10 | 3 | AMD/Intel | 3 | 0 | 4 | 10 | 10 | 2.17–2.20 | 2.06–2.14 | 2.17–2.21 | 30.36–30.56 | 30.55–30.68 | 0.20 | 0.21 | 0 | 10 | - | 0.01–0.03 | 0.06 | 0.02–0.03 |
| frps | 100 | 3 | AMD/Intel | 3 | 0 | 4 | 100 | 100 | 2.27–2.30 | 2.73–3.07 | 2.99–3.72 | 30.74–31.01 | 30.85–31.25 | 0.20–0.21 | 0.21–0.23 | 0 | 100 | - | 0.21–0.39 | 0.82–1.29 | 0.23–0.34 |
| frps | 300 | 3 | AMD/Intel | 3 | 0 | 4 | 300 | 300 | 2.51–2.81 | 6.93–7.98 | 6.96–8.00 | 31.07–32.48 | 34.40–35.76 | 0.22–0.25 | 11.77–19.30 | 0 | 166–282 | - | 1.36–1.41 | 1.69–1.75 | 0.62–0.64 |
| frps | 600 | 3 | AMD/Intel | 2 | 0 | 4 | 600 | 600 | 3.12–3.42 | 5.01–6.00 | 5.04–6.04 | 41.85 | 42.08 | 41.45 | 42.01 | 0 | 58–221 | - | 1.47–1.52 | 1.88–1.96 | 0.77–0.78 |
| frps-holdoff | 1 | 3 | AMD/Intel | 3 | 0 | 5 | 1 | 1 | 2.01–2.09 | 2.02–2.09 | 2.02–2.09 | 3.19 | 3.19 | 0.20 | 0.20 | 0 | 0 | - | 0.00–0.03 | 0.01–0.21 | 0.01–0.06 |
| frps-holdoff | 10 | 3 | AMD/Intel | 3 | 0 | 4 | 10 | 10 | 2.17–2.21 | 2.08–2.13 | 2.21–2.31 | 3.19–3.20 | 3.19–3.20 | 0.20–0.21 | 0.20–0.21 | 0 | 0 | - | 0.02–0.03 | 0.05–0.06 | 0.02–0.04 |
| frps-holdoff | 100 | 3 | AMD/Intel | 3 | 0 | 4 | 100 | 99–100 | 2.24–2.29 | 2.64–3.08 | 2.94–3.71 | 3.23–3.94 | 3.23–3.95 | 0.23–0.94 | 0.23–0.98 | 0 | 0 | - | 0.21–0.35 | 0.87–1.28 | 0.24–0.34 |
| frps-holdoff | 300 | 3 | AMD/Intel | 3 | 0 | 4 | 300 | 300 | 2.56–2.74 | 6.96–7.77 | 6.98–8.06 | 10.25–16.83 | 10.28–16.89 | 7.21–13.77 | 7.31–13.92 | 0 | 0 | - | 1.09–1.14 | 1.55–1.69 | 0.59–0.61 |
| frps-holdoff | 600 | 3 | AMD/Intel | 2 | 0 | 4 | 600 | 600 | 3.10–3.17 | 4.21–4.86 | 4.24–4.88 | 38.11 | 38.43 | 35.05 | 35.33 | 0 | 0 | - | 1.14–1.39 | 1.89–2.00 | 0.76–0.80 |
| none | 1 | 3 | AMD/Intel | 3 | 0 | 5 | 1 | 0 | - | - | - | - | - | - | - | - | 0 | - | 0.00–0.01 | 0.02 | 0.01 |
| none | 10 | 3 | AMD/Intel | 3 | 0 | 4 | 10 | 0 | - | - | - | - | - | - | - | - | 0 | - | 0.01–0.02 | 0.02–0.03 | 0.01–0.02 |
| none | 100 | 3 | AMD/Intel | 3 | 0 | 4 | 100 | 0 | - | - | - | - | - | - | - | - | 0 | - | 0.10–0.16 | 0.23–0.26 | 0.12–0.14 |
| none | 300 | 3 | AMD/Intel | 3 | 0 | 4 | 300 | 0 | - | - | - | - | - | - | - | - | 0 | - | 0.39–0.61 | 0.55–0.81 | 0.28–0.37 |
| none | 600 | 3 | AMD/Intel | 2 | 0 | 4 | 600 | 0 | - | - | - | - | - | - | - | - | 0 | - | 1.17–1.59 | 1.56–1.77 | 0.63–0.70 |

#### The plugins' calls, from 1 s before the cut to 90 s after (the valid runs pooled)

| event | tunnels | plugin | op | calls | p50 ms | p99 ms | max ms | over 1 s | refused |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| front | 1 | admit | CloseProxy | 6 | 0.18 | 0.23 | 0.23 | 0 | 0 |
| front | 1 | admit | Login | 3 | 0.14 | 0.22 | 0.22 | 0 | 0 |
| front | 1 | admit | NewProxy | 3 | 0.11 | 0.15 | 0.15 | 0 | 0 |
| front | 1 | front | CloseProxy | 3 | 0.09 | 0.27 | 0.27 | 0 | 0 |
| front | 1 | front | NewProxy | 3 | 0.14 | 0.20 | 0.20 | 0 | 0 |
| front | 10 | admit | CloseProxy | 60 | 0.06 | 0.62 | 0.62 | 0 | 0 |
| front | 10 | admit | Login | 30 | 0.09 | 0.26 | 0.26 | 0 | 0 |
| front | 10 | admit | NewProxy | 30 | 0.09 | 0.46 | 0.46 | 0 | 0 |
| front | 10 | front | CloseProxy | 30 | 0.06 | 1.21 | 1.21 | 0 | 0 |
| front | 10 | front | NewProxy | 30 | 0.07 | 0.74 | 0.74 | 0 | 0 |
| front | 100 | admit | CloseProxy | 600 | 0.04 | 5.98 | 16.60 | 0 | 0 |
| front | 100 | admit | Login | 300 | 0.07 | 0.93 | 1.38 | 0 | 0 |
| front | 100 | admit | NewProxy | 300 | 0.08 | 1.28 | 1.70 | 0 | 0 |
| front | 100 | front | CloseProxy | 300 | 0.03 | 0.50 | 4.52 | 0 | 0 |
| front | 100 | front | NewProxy | 300 | 0.06 | 3.56 | 7.59 | 0 | 0 |
| front | 300 | admit | CloseProxy | 1800 | 0.04 | 19.91 | 84.22 | 0 | 0 |
| front | 300 | admit | Login | 900 | 0.07 | 12.01 | 27.54 | 0 | 0 |
| front | 300 | admit | NewProxy | 900 | 0.07 | 3.18 | 27.22 | 0 | 0 |
| front | 300 | front | CloseProxy | 900 | 0.03 | 7.11 | 14.36 | 0 | 0 |
| front | 300 | front | NewProxy | 900 | 0.05 | 22.35 | 70.14 | 0 | 0 |
| front | 600 | admit | CloseProxy | 2400 | 0.04 | 22.42 | 25.84 | 0 | 0 |
| front | 600 | admit | Login | 1200 | 0.07 | 36.69 | 43.46 | 0 | 0 |
| front | 600 | admit | NewProxy | 1200 | 0.07 | 14.55 | 27.50 | 0 | 0 |
| front | 600 | front | CloseProxy | 651 | 0.04 | 25.12 | 119.95 | 0 | 0 |
| front | 600 | front | NewProxy | 1200 | 0.05 | 27.39 | 143.69 | 0 | 0 |
| frps | 1 | admit | CloseProxy | 3 | 0.20 | 0.22 | 0.22 | 0 | 0 |
| frps | 1 | admit | Login | 3 | 0.15 | 0.22 | 0.22 | 0 | 0 |
| frps | 1 | admit | NewProxy | 3 | 0.14 | 0.23 | 0.23 | 0 | 0 |
| frps | 1 | front | CloseProxy | 3 | 0.11 | 0.15 | 0.15 | 0 | 0 |
| frps | 1 | front | NewProxy | 3 | 0.12 | 0.19 | 0.19 | 0 | 0 |
| frps | 10 | admit | CloseProxy | 30 | 0.07 | 0.42 | 0.42 | 0 | 0 |
| frps | 10 | admit | Login | 30 | 0.10 | 0.28 | 0.28 | 0 | 0 |
| frps | 10 | admit | NewProxy | 30 | 0.08 | 0.13 | 0.13 | 0 | 0 |
| frps | 10 | front | CloseProxy | 30 | 0.06 | 1.22 | 1.22 | 0 | 0 |
| frps | 10 | front | NewProxy | 30 | 0.08 | 0.19 | 0.19 | 0 | 0 |
| frps | 100 | admit | CloseProxy | 300 | 0.05 | 4.20 | 6.98 | 0 | 0 |
| frps | 100 | admit | Login | 300 | 0.07 | 1.31 | 15.22 | 0 | 0 |
| frps | 100 | admit | NewProxy | 300 | 0.08 | 0.81 | 1.10 | 0 | 0 |
| frps | 100 | front | CloseProxy | 182 | 0.03 | 8.00 | 8.09 | 0 | 0 |
| frps | 100 | front | NewProxy | 300 | 0.06 | 1.97 | 6.10 | 0 | 0 |
| frps | 300 | admit | CloseProxy | 900 | 0.06 | 17.15 | 21.40 | 0 | 0 |
| frps | 300 | admit | Login | 900 | 0.07 | 18.77 | 24.97 | 0 | 0 |
| frps | 300 | admit | NewProxy | 900 | 0.07 | 13.18 | 24.39 | 0 | 0 |
| frps | 300 | front | CloseProxy | 900 | 0.04 | 9.46 | 11.95 | 0 | 0 |
| frps | 300 | front | NewProxy | 900 | 0.05 | 6.40 | 39.09 | 0 | 0 |
| frps | 600 | admit | CloseProxy | 1200 | 0.04 | 20.60 | 24.69 | 0 | 0 |
| frps | 600 | admit | Login | 1200 | 0.09 | 37.53 | 41.08 | 0 | 0 |
| frps | 600 | admit | NewProxy | 1200 | 0.07 | 21.49 | 66.42 | 0 | 0 |
| frps | 600 | front | CloseProxy | 1200 | 0.03 | 0.68 | 6.07 | 0 | 0 |
| frps | 600 | front | NewProxy | 1200 | 0.05 | 30.11 | 77.32 | 0 | 0 |
| frps-holdoff | 1 | admit | CloseProxy | 3 | 0.18 | 0.25 | 0.25 | 0 | 0 |
| frps-holdoff | 1 | admit | Login | 3 | 0.14 | 0.14 | 0.14 | 0 | 0 |
| frps-holdoff | 1 | admit | NewProxy | 3 | 0.08 | 0.14 | 0.14 | 0 | 0 |
| frps-holdoff | 1 | front | CloseProxy | 3 | 0.11 | 0.17 | 0.17 | 0 | 0 |
| frps-holdoff | 1 | front | NewProxy | 3 | 0.10 | 0.12 | 0.12 | 0 | 0 |
| frps-holdoff | 10 | admit | CloseProxy | 30 | 0.06 | 0.19 | 0.19 | 0 | 0 |
| frps-holdoff | 10 | admit | Login | 30 | 0.11 | 0.24 | 0.24 | 0 | 0 |
| frps-holdoff | 10 | admit | NewProxy | 30 | 0.09 | 0.25 | 0.25 | 0 | 0 |
| frps-holdoff | 10 | front | CloseProxy | 30 | 0.05 | 0.15 | 0.15 | 0 | 0 |
| frps-holdoff | 10 | front | NewProxy | 30 | 0.07 | 0.16 | 0.16 | 0 | 0 |
| frps-holdoff | 100 | admit | CloseProxy | 300 | 0.05 | 5.56 | 6.67 | 0 | 0 |
| frps-holdoff | 100 | admit | Login | 300 | 0.07 | 1.58 | 2.51 | 0 | 0 |
| frps-holdoff | 100 | admit | NewProxy | 300 | 0.08 | 0.84 | 3.29 | 0 | 0 |
| frps-holdoff | 100 | front | CloseProxy | 300 | 0.03 | 3.14 | 9.48 | 0 | 0 |
| frps-holdoff | 100 | front | NewProxy | 300 | 0.07 | 5.00 | 6.20 | 0 | 0 |
| frps-holdoff | 300 | admit | CloseProxy | 900 | 0.04 | 8.90 | 35.56 | 0 | 0 |
| frps-holdoff | 300 | admit | Login | 900 | 0.07 | 14.57 | 28.17 | 0 | 0 |
| frps-holdoff | 300 | admit | NewProxy | 900 | 0.08 | 16.18 | 44.97 | 0 | 0 |
| frps-holdoff | 300 | front | CloseProxy | 723 | 0.04 | 21.50 | 22.88 | 0 | 0 |
| frps-holdoff | 300 | front | NewProxy | 900 | 0.05 | 17.66 | 83.13 | 0 | 0 |
| frps-holdoff | 600 | admit | CloseProxy | 1200 | 0.03 | 0.87 | 19.00 | 0 | 0 |
| frps-holdoff | 600 | admit | Login | 1200 | 0.07 | 20.54 | 27.44 | 0 | 0 |
| frps-holdoff | 600 | admit | NewProxy | 1200 | 0.07 | 19.10 | 22.60 | 0 | 0 |
| frps-holdoff | 600 | front | CloseProxy | 1200 | 0.03 | 3.35 | 32.27 | 0 | 0 |
| frps-holdoff | 600 | front | NewProxy | 1200 | 0.05 | 16.05 | 79.11 | 0 | 0 |

### The capped link

| tunnels | streams | cap Mb/s | queue | rep (runner) | counted | valid | host-bound | offered tok/s | delivered tok/s | link up min | link down min | at cap | gap p99 ms | list-seconds not online | frpc re-logins | heartbeat timeouts | probes failed | host busy |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 | 4 | 200 | fair | 1 (AMD) | counted | True | no | 8000 | 7871 | 0.986 | 0.991 | True | 55.9 | 0 | 0 | 0 | 0 | 0.64 |
| 100 | 4 | 200 | fair | 2 (Intel) | counted | True | no | 8000 | 7870 | 0.999 | 0.998 | True | 56.0 | 0 | 0 | 0 | 0 | 0.65 |
| 100 | 4 | 200 | fair | 3 (AMD) | excluded: host-bound | True | yes | 8000 | 7828 | 0.982 | 0.984 | True | 59.2 | 0 | 0 | 0 | 0 | 0.86 |
| 100 | 4 | 100 | fair | 1 (AMD) | counted | True | no | 8000 | 7846 | 0.996 | 0.988 | True | 59.1 | 0 | 0 | 0 | 0 | 0.60 |
| 100 | 4 | 100 | fair | 2 (Intel) | counted | True | no | 8000 | 7847 | 0.992 | 0.989 | True | 59.2 | 0 | 0 | 0 | 0 | 0.62 |
| 100 | 4 | 100 | fair | 3 (AMD) | counted | True | no | 8000 | 7816 | 0.993 | 0.986 | True | 60.2 | 0 | 0 | 0 | 0 | 0.80 |
| 100 | 4 | 50 | fair | 1 (AMD) | counted | True | no | 8000 | 7791 | 0.998 | 0.998 | True | 68.1 | 0 | 0 | 0 | 0 | 0.54 |
| 100 | 4 | 50 | fair | 2 (Intel) | counted | True | no | 8000 | 7790 | 0.998 | 0.999 | True | 68.1 | 0 | 0 | 0 | 0 | 0.54 |
| 100 | 4 | 50 | fair | 3 (AMD) | counted | True | no | 8000 | 7763 | 0.997 | 0.998 | True | 68.6 | 0 | 0 | 0 | 0 | 0.74 |
| 100 | 4 | 25 | fair | 1 (AMD) | counted | True | no | 8000 | 7577 | 0.996 | 0.996 | True | 67.5 | 0 | 0 | 0 | 0 | 0.48 |
| 100 | 4 | 25 | fair | 2 (Intel) | counted | True | no | 8000 | 7568 | 0.999 | 0.995 | True | 67.0 | 0 | 0 | 0 | 0 | 0.48 |
| 100 | 4 | 25 | fair | 3 (AMD) | counted | True | no | 8000 | 7553 | 0.996 | 0.996 | True | 66.9 | 0 | 0 | 0 | 0 | 0.68 |
| 100 | 4 | 12.5 | fair | 1 (AMD) | counted | True | no | 8000 | 5424 | 0.996 | 0.995 | True | 520.8 | 0 | 0 | 0 | 0 | 0.30 |
| 100 | 4 | 12.5 | fair | 2 (Intel) | counted | True | no | 8000 | 5425 | 0.996 | 0.995 | True | 514.5 | 0 | 0 | 0 | 0 | 0.30 |
| 100 | 4 | 12.5 | fair | 3 (AMD) | counted | True | no | 8000 | 5424 | 0.995 | 0.997 | True | 514.5 | 0 | 0 | 0 | 0 | 0.41 |
| 100 | 4 | 0 | fair | 1 (AMD) | counted | True | no | 8000 | 7892 | - | - | False | 54.3 | 0 | 0 | 0 | 0 | 0.60 |
| 100 | 4 | 0 | fair | 2 (Intel) | counted | True | no | 8000 | 7892 | - | - | False | 55.2 | 0 | 0 | 0 | 0 | 0.59 |
| 100 | 4 | 0 | fair | 3 (AMD) | counted | True | no | 8000 | 7866 | - | - | False | 56.9 | 0 | 0 | 0 | 0 | 0.82 |
| 100 | 4 | 200 | fifo 100ms | 1 (AMD) | counted | True | no | 8000 | 7396 | 0.999 | 0.999 | True | 55.9 | 0 | 0 | 0 | 0 | 0.62 |
| 100 | 4 | 200 | fifo 100ms | 2 (Intel) | counted | True | no | 8000 | 7396 | 0.999 | 1.000 | True | 56.1 | 0 | 0 | 0 | 0 | 0.62 |
| 100 | 4 | 200 | fifo 100ms | 3 (AMD) | counted | True | no | 8000 | 7364 | 1.000 | 0.999 | True | 58.9 | 0 | 0 | 0 | 0 | 0.84 |
| 100 | 4 | 100 | fifo 100ms | 1 (AMD) | counted | True | no | 8000 | 7380 | 0.999 | 0.999 | True | 59.1 | 0 | 0 | 0 | 0 | 0.58 |
| 100 | 4 | 100 | fifo 100ms | 2 (Intel) | counted | True | no | 8000 | 7377 | 0.999 | 0.999 | True | 59.2 | 0 | 0 | 0 | 0 | 0.59 |
| 100 | 4 | 100 | fifo 100ms | 3 (AMD) | counted | True | no | 8000 | 7355 | 0.999 | 0.999 | True | 60.4 | 0 | 0 | 0 | 0 | 0.78 |
| 100 | 4 | 50 | fifo 100ms | 1 (AMD) | counted | True | no | 8000 | 7328 | 0.997 | 0.998 | True | 68.7 | 0 | 0 | 0 | 0 | 0.52 |
| 100 | 4 | 50 | fifo 100ms | 2 (Intel) | counted | True | no | 8000 | 7330 | 0.998 | 0.998 | True | 68.7 | 0 | 0 | 0 | 0 | 0.53 |
| 100 | 4 | 50 | fifo 100ms | 3 (AMD) | counted | True | no | 8000 | 7309 | 0.998 | 0.998 | True | 68.8 | 0 | 0 | 0 | 0 | 0.72 |
| 100 | 4 | 25 | fifo 100ms | 1 (AMD) | counted | True | no | 8000 | 7159 | 0.996 | 0.996 | True | 64.7 | 0 | 0 | 0 | 0 | 0.47 |
| 100 | 4 | 25 | fifo 100ms | 2 (Intel) | counted | True | no | 8000 | 7162 | 0.995 | 0.997 | True | 64.6 | 0 | 0 | 0 | 0 | 0.46 |
| 100 | 4 | 25 | fifo 100ms | 3 (AMD) | counted | True | no | 8000 | 7141 | 0.996 | 0.995 | True | 64.1 | 0 | 0 | 0 | 0 | 0.65 |
| 100 | 4 | 12.5 | fifo 100ms | 1 (AMD) | counted | False | no | 8000 | 5407 | 0.996 | 0.996 | True | 525.1 | 0 | 0 | 0 | 0 | 0.30 |
| 100 | 4 | 12.5 | fifo 100ms | 2 (Intel) | counted | False | no | 8000 | 5410 | 0.996 | 0.996 | True | 523.4 | 0 | 0 | 0 | 0 | 0.30 |
| 100 | 4 | 12.5 | fifo 100ms | 3 (AMD) | counted | False | no | 8000 | 5411 | 0.995 | 0.996 | True | 531.0 | 0 | 0 | 0 | 0 | 0.41 |
| 100 | 4 | 200 | fifo 1s | 1 (AMD) | counted | True | no | 8000 | 4758 | 1.000 | 1.000 | True | 55.5 | 0 | 0 | 0 | 0 | 0.45 |
| 100 | 4 | 200 | fifo 1s | 2 (Intel) | counted | True | no | 8000 | 4759 | 0.999 | 0.999 | True | 55.4 | 0 | 0 | 0 | 0 | 0.44 |
| 100 | 4 | 200 | fifo 1s | 3 (AMD) | counted | True | no | 8000 | 4754 | 0.999 | 1.000 | True | 56.3 | 0 | 0 | 0 | 0 | 0.60 |
| 100 | 4 | 100 | fifo 1s | 1 (AMD) | counted | True | no | 8000 | 4751 | 0.999 | 0.999 | True | 58.5 | 0 | 0 | 0 | 0 | 0.42 |
| 100 | 4 | 100 | fifo 1s | 2 (Intel) | counted | True | no | 8000 | 4750 | 0.999 | 0.999 | True | 58.4 | 0 | 0 | 0 | 0 | 0.43 |
| 100 | 4 | 100 | fifo 1s | 3 (AMD) | counted | True | no | 8000 | 4747 | 0.999 | 0.999 | True | 58.9 | 0 | 0 | 0 | 0 | 0.58 |
| 100 | 4 | 50 | fifo 1s | 1 (AMD) | counted | True | no | 8000 | 4735 | 0.998 | 0.998 | True | 66.4 | 0 | 0 | 0 | 0 | 0.38 |
| 100 | 4 | 50 | fifo 1s | 2 (Intel) | counted | True | no | 8000 | 4735 | 0.998 | 0.997 | True | 66.3 | 0 | 0 | 0 | 0 | 0.39 |
| 100 | 4 | 50 | fifo 1s | 3 (AMD) | counted | True | no | 8000 | 4732 | 0.998 | 0.998 | True | 66.6 | 0 | 0 | 0 | 0 | 0.52 |
| 100 | 4 | 25 | fifo 1s | 1 (AMD) | counted | True | no | 8000 | 4686 | 0.997 | 0.995 | True | 87.0 | 0 | 0 | 0 | 0 | 0.35 |
| 100 | 4 | 25 | fifo 1s | 2 (Intel) | counted | True | no | 8000 | 4685 | 0.997 | 0.996 | True | 87.1 | 0 | 0 | 0 | 0 | 0.37 |
| 100 | 4 | 25 | fifo 1s | 3 (AMD) | counted | True | no | 8000 | 4684 | 0.995 | 0.997 | True | 86.9 | 0 | 0 | 0 | 0 | 0.49 |
| 100 | 4 | 12.5 | fifo 1s | 1 (AMD) | counted | True | no | 8000 | 4575 | 0.997 | 0.997 | True | 140.8 | 0 | 0 | 0 | 0 | 0.31 |
| 100 | 4 | 12.5 | fifo 1s | 2 (Intel) | counted | True | no | 8000 | 4580 | 0.996 | 0.995 | True | 136.1 | 0 | 0 | 0 | 0 | 0.31 |
| 100 | 4 | 12.5 | fifo 1s | 3 (AMD) | counted | True | no | 8000 | 4562 | 0.995 | 0.996 | True | 156.3 | 0 | 0 | 0 | 0 | 0.42 |
| 300 | 1 | 200 | fair | 1 (AMD) | counted | True | no | 6000 | 5876 | 0.978 | 0.971 | True | 57.6 | 0 | 0 | 0 | 0 | 0.60 |
| 300 | 1 | 200 | fair | 2 (AMD) | counted | True | no | 6000 | 5886 | 0.982 | 0.980 | True | 57.1 | 0 | 0 | 0 | 0 | 0.50 |
| 300 | 1 | 200 | fair | 3 (AMD) | excluded: not at its cap | True | no | 6000 | 5814 | 0.952 | 0.938 | False | 76.6 | 0 | 0 | 0 | 0 | 0.83 |
| 300 | 1 | 100 | fair | 1 (AMD) | counted | True | no | 6000 | 5858 | 0.996 | 0.999 | True | 59.8 | 0 | 0 | 0 | 0 | 0.58 |
| 300 | 1 | 100 | fair | 2 (AMD) | counted | True | no | 6000 | 5874 | 0.998 | 0.998 | True | 59.8 | 0 | 0 | 0 | 0 | 0.50 |
| 300 | 1 | 100 | fair | 3 (AMD) | counted | True | no | 6000 | 5823 | 0.966 | 0.954 | True | 71.9 | 0 | 0 | 0 | 0 | 0.76 |
| 300 | 1 | 50 | fair | 1 (AMD) | counted | True | no | 6000 | 5829 | 0.997 | 0.998 | True | 68.1 | 0 | 0 | 0 | 0 | 0.55 |
| 300 | 1 | 50 | fair | 2 (AMD) | counted | True | no | 6000 | 5841 | 0.998 | 0.998 | True | 67.8 | 0 | 0 | 0 | 0 | 0.43 |
| 300 | 1 | 50 | fair | 3 (AMD) | counted | True | no | 6000 | 5789 | 0.998 | 0.996 | True | 73.8 | 0 | 0 | 0 | 0 | 0.73 |
| 300 | 1 | 25 | fair | 1 (AMD) | counted | True | no | 6000 | 5722 | 0.995 | 0.996 | True | 95.5 | 0 | 0 | 0 | 0 | 0.55 |
| 300 | 1 | 25 | fair | 2 (AMD) | counted | True | no | 6000 | 5737 | 0.995 | 0.996 | True | 95.6 | 0 | 0 | 0 | 0 | 0.42 |
| 300 | 1 | 25 | fair | 3 (AMD) | counted | True | no | 6000 | 5679 | 0.995 | 0.996 | True | 96.0 | 0 | 0 | 0 | 0 | 0.72 |
| 300 | 1 | 12.5 | fair | 1 (AMD) | counted | False | no | 6000 | 5167 | 0.996 | 0.995 | True | 302.6 | 0 | 0 | 0 | 0 | 0.41 |
| 300 | 1 | 12.5 | fair | 2 (AMD) | counted | False | no | 6000 | 5176 | 0.997 | 0.996 | True | 291.8 | 0 | 0 | 0 | 0 | 0.32 |
| 300 | 1 | 12.5 | fair | 3 (AMD) | counted | True | no | 6000 | 5157 | 0.999 | 0.996 | True | 301.5 | 0 | 0 | 0 | 0 | 0.60 |
| 300 | 1 | 0 | fair | 1 (AMD) | counted | True | no | 6000 | 5899 | - | - | False | 56.7 | 0 | 0 | 0 | 0 | 0.56 |
| 300 | 1 | 0 | fair | 2 (AMD) | counted | True | no | 6000 | 5907 | - | - | False | 54.5 | 0 | 0 | 0 | 0 | 0.41 |
| 300 | 1 | 0 | fair | 3 (AMD) | counted | True | no | 6000 | 5848 | - | - | False | 77.2 | 0 | 0 | 0 | 0 | 0.78 |
| 300 | 1 | 200 | fifo 100ms | 1 (AMD) | counted | True | no | 6000 | 5532 | 0.999 | 0.999 | True | 57.4 | 0 | 0 | 0 | 0 | 0.59 |
| 300 | 1 | 200 | fifo 100ms | 2 (AMD) | counted | True | no | 6000 | 5544 | 0.999 | 0.999 | True | 56.4 | 0 | 0 | 0 | 0 | 0.45 |
| 300 | 1 | 200 | fifo 100ms | 3 (AMD) | counted | True | no | 6000 | 5495 | 0.999 | 1.000 | True | 73.7 | 0 | 0 | 0 | 0 | 0.81 |
| 300 | 1 | 100 | fifo 100ms | 1 (AMD) | counted | True | no | 6000 | 5511 | 0.999 | 0.999 | True | 60.4 | 0 | 0 | 0 | 0 | 0.56 |
| 300 | 1 | 100 | fifo 100ms | 2 (AMD) | counted | True | no | 6000 | 5528 | 0.999 | 0.999 | True | 59.7 | 0 | 0 | 0 | 0 | 0.44 |
| 300 | 1 | 100 | fifo 100ms | 3 (AMD) | counted | True | no | 6000 | 5483 | 0.999 | 0.999 | True | 71.0 | 0 | 0 | 0 | 0 | 0.74 |
| 300 | 1 | 50 | fifo 100ms | 1 (AMD) | counted | True | no | 6000 | 5491 | 0.998 | 0.998 | True | 68.5 | 0 | 0 | 0 | 0 | 0.53 |
| 300 | 1 | 50 | fifo 100ms | 2 (AMD) | counted | True | no | 6000 | 5498 | 0.997 | 0.998 | True | 68.2 | 0 | 0 | 0 | 0 | 0.43 |
| 300 | 1 | 50 | fifo 100ms | 3 (AMD) | counted | True | no | 6000 | 5462 | 0.997 | 0.998 | True | 70.6 | 0 | 0 | 0 | 0 | 0.72 |
| 300 | 1 | 25 | fifo 100ms | 1 (AMD) | counted | True | no | 6000 | 5402 | 0.996 | 0.996 | True | 95.4 | 0 | 0 | 0 | 0 | 0.53 |
| 300 | 1 | 25 | fifo 100ms | 2 (AMD) | counted | True | no | 6000 | 5403 | 0.996 | 0.997 | True | 95.0 | 0 | 0 | 0 | 0 | 0.42 |
| 300 | 1 | 25 | fifo 100ms | 3 (AMD) | counted | True | no | 6000 | 5365 | 0.995 | 0.996 | True | 96.0 | 0 | 0 | 0 | 0 | 0.73 |
| 300 | 1 | 12.5 | fifo 100ms | 1 (AMD) | counted | False | no | 6000 | 4980 | 0.997 | 0.995 | True | 269.2 | 0 | 0 | 0 | 0 | 0.41 |
| 300 | 1 | 12.5 | fifo 100ms | 2 (AMD) | counted | True | no | 6000 | 4975 | 0.996 | 0.996 | True | 256.1 | 0 | 0 | 0 | 0 | 0.31 |
| 300 | 1 | 12.5 | fifo 100ms | 3 (AMD) | counted | True | no | 6000 | 4997 | 0.996 | 0.996 | True | 242.6 | 0 | 0 | 0 | 0 | 0.60 |
| 300 | 1 | 200 | fifo 1s | 1 (AMD) | counted | False | no | 6000 | 3571 | 0.999 | 1.000 | True | 62.8 | 0 | 0 | 0 | 0 | 0.42 |
| 300 | 1 | 200 | fifo 1s | 2 (AMD) | counted | False | no | 6000 | 3572 | 0.999 | 0.999 | True | 57.1 | 0 | 0 | 0 | 0 | 0.32 |
| 300 | 1 | 200 | fifo 1s | 3 (AMD) | counted | False | no | 6000 | 3562 | 1.000 | 0.999 | True | 71.9 | 0 | 0 | 0 | 0 | 0.57 |
| 300 | 1 | 100 | fifo 1s | 1 (AMD) | counted | False | no | 6000 | 3569 | 0.999 | 0.999 | True | 60.5 | 0 | 0 | 0 | 0 | 0.40 |
| 300 | 1 | 100 | fifo 1s | 2 (AMD) | counted | False | no | 6000 | 3571 | 0.999 | 0.999 | True | 59.4 | 0 | 0 | 0 | 0 | 0.30 |
| 300 | 1 | 100 | fifo 1s | 3 (AMD) | counted | False | no | 6000 | 3560 | 0.999 | 0.999 | True | 66.8 | 0 | 0 | 0 | 0 | 0.56 |
| 300 | 1 | 50 | fifo 1s | 1 (AMD) | counted | False | no | 6000 | 3560 | 0.997 | 0.998 | True | 67.9 | 0 | 0 | 0 | 0 | 0.37 |
| 300 | 1 | 50 | fifo 1s | 2 (AMD) | counted | False | no | 6000 | 3562 | 0.998 | 0.998 | True | 67.4 | 0 | 0 | 0 | 0 | 0.28 |
| 300 | 1 | 50 | fifo 1s | 3 (AMD) | counted | False | no | 6000 | 3571 | 0.997 | 0.999 | True | 79.8 | 0 | 0 | 0 | 0 | 0.55 |
| 300 | 1 | 25 | fifo 1s | 1 (AMD) | counted | False | no | 6000 | 3534 | 0.995 | 0.997 | True | 93.1 | 0 | 0 | 0 | 0 | 0.37 |
| 300 | 1 | 25 | fifo 1s | 2 (AMD) | counted | False | no | 6000 | 3471 | 0.996 | 0.996 | True | 92.3 | 0 | 0 | 0 | 0 | 0.30 |
| 300 | 1 | 25 | fifo 1s | 3 (AMD) | counted | False | no | 6000 | 3556 | 0.996 | 0.997 | True | 93.8 | 0 | 0 | 0 | 0 | 0.54 |
| 300 | 1 | 12.5 | fifo 1s | 1 (AMD) | counted | False | no | 6000 | 3285 | 0.996 | 0.997 | True | 249.4 | 0 | 0 | 0 | 0 | 0.32 |
| 300 | 1 | 12.5 | fifo 1s | 2 (AMD) | counted | False | no | 6000 | 3280 | 0.996 | 0.996 | True | 247.3 | 0 | 0 | 0 | 0 | 0.25 |
| 300 | 1 | 12.5 | fifo 1s | 3 (AMD) | counted | False | no | 6000 | 3368 | 0.996 | 0.995 | True | 240.6 | 0 | 0 | 0 | 0 | 0.48 |

#### Runs counted per configuration (a capped run counts only if every second of its window was at 95% of the cap or more, and a heartbeat finding only where the host stayed under 85% busy)

| tunnels | streams | cap Mb/s | queue | counted | excluded | counted with a heartbeat failure |
|---|---:|---:|---:|---:|---:|---:|
| 100 | 4 | 200 | fair | 2 | 1 | 0 |
| 100 | 4 | 100 | fair | 3 | 0 | 0 |
| 100 | 4 | 50 | fair | 3 | 0 | 0 |
| 100 | 4 | 25 | fair | 3 | 0 | 0 |
| 100 | 4 | 12.5 | fair | 3 | 0 | 0 |
| 100 | 4 | 0 | fair | 3 | 0 | 0 |
| 100 | 4 | 200 | fifo 100ms | 3 | 0 | 0 |
| 100 | 4 | 100 | fifo 100ms | 3 | 0 | 0 |
| 100 | 4 | 50 | fifo 100ms | 3 | 0 | 0 |
| 100 | 4 | 25 | fifo 100ms | 3 | 0 | 0 |
| 100 | 4 | 12.5 | fifo 100ms | 3 | 0 | 0 |
| 100 | 4 | 200 | fifo 1s | 3 | 0 | 0 |
| 100 | 4 | 100 | fifo 1s | 3 | 0 | 0 |
| 100 | 4 | 50 | fifo 1s | 3 | 0 | 0 |
| 100 | 4 | 25 | fifo 1s | 3 | 0 | 0 |
| 100 | 4 | 12.5 | fifo 1s | 3 | 0 | 0 |
| 300 | 1 | 200 | fair | 2 | 1 | 0 |
| 300 | 1 | 100 | fair | 3 | 0 | 0 |
| 300 | 1 | 50 | fair | 3 | 0 | 0 |
| 300 | 1 | 25 | fair | 3 | 0 | 0 |
| 300 | 1 | 12.5 | fair | 3 | 0 | 0 |
| 300 | 1 | 0 | fair | 3 | 0 | 0 |
| 300 | 1 | 200 | fifo 100ms | 3 | 0 | 0 |
| 300 | 1 | 100 | fifo 100ms | 3 | 0 | 0 |
| 300 | 1 | 50 | fifo 100ms | 3 | 0 | 0 |
| 300 | 1 | 25 | fifo 100ms | 3 | 0 | 0 |
| 300 | 1 | 12.5 | fifo 100ms | 3 | 0 | 0 |
| 300 | 1 | 200 | fifo 1s | 3 | 0 | 0 |
| 300 | 1 | 100 | fifo 1s | 3 | 0 | 0 |
| 300 | 1 | 50 | fifo 1s | 3 | 0 | 0 |
| 300 | 1 | 25 | fifo 1s | 3 | 0 | 0 |
| 300 | 1 | 12.5 | fifo 1s | 3 | 0 | 0 |

### R_ssh

| cipher asked | rep (runner) | phase | streams per tunnel | k tok/s | valid | host-bound | ssh cores | sshd cores | ssh cores per 1k tok/s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| aes128-gcm@openssh.com | 1 (Intel) | idle | 0 | 0.00 | True | no | 0.0014 | 0.0000 | - |
| aes128-gcm@openssh.com | 1 (Intel) | load | 1 | 0.08 | True | no | 0.0041 | 0.0041 | 0.052 |
| aes128-gcm@openssh.com | 1 (Intel) | load | 4 | 0.32 | True | no | 0.0116 | 0.0140 | 0.037 |
| aes128-gcm@openssh.com | 1 (Intel) | load | 16 | 1.26 | True | no | 0.0343 | 0.0410 | 0.027 |
| aes128-gcm@openssh.com | 1 (Intel) | load | 32 | 2.53 | True | no | 0.0444 | 0.0515 | 0.018 |
| aes128-gcm@openssh.com | 1 (Intel) | load | 64 | 5.06 | True | no | 0.0589 | 0.0674 | 0.012 |
| aes256-gcm@openssh.com | 1 (Intel) | idle | 0 | 0.00 | True | no | 0.0000 | 0.0000 | - |
| aes256-gcm@openssh.com | 1 (Intel) | load | 1 | 0.08 | True | no | 0.0044 | 0.0041 | 0.056 |
| aes256-gcm@openssh.com | 1 (Intel) | load | 4 | 0.32 | True | no | 0.0109 | 0.0133 | 0.035 |
| aes256-gcm@openssh.com | 1 (Intel) | load | 16 | 1.26 | True | no | 0.0272 | 0.0326 | 0.022 |
| aes256-gcm@openssh.com | 1 (Intel) | load | 32 | 2.53 | True | no | 0.0392 | 0.0450 | 0.016 |
| aes256-gcm@openssh.com | 1 (Intel) | load | 64 | 5.06 | True | no | 0.0589 | 0.0674 | 0.012 |
| default | 1 (Intel) | idle | 0 | 0.00 | True | no | 0.0000 | 0.0000 | - |
| default | 1 (Intel) | load | 1 | 0.08 | True | no | 0.0048 | 0.0048 | 0.061 |
| default | 1 (Intel) | load | 4 | 0.32 | True | no | 0.0116 | 0.0147 | 0.037 |
| default | 1 (Intel) | load | 16 | 1.26 | True | no | 0.0322 | 0.0378 | 0.025 |
| default | 1 (Intel) | load | 32 | 2.53 | True | no | 0.0467 | 0.0542 | 0.018 |
| default | 1 (Intel) | load | 64 | 5.06 | True | no | 0.0603 | 0.0685 | 0.012 |
| aes128-gcm@openssh.com | 2 (AMD) | idle | 0 | 0.00 | True | no | 0.0000 | 0.0000 | - |
| aes128-gcm@openssh.com | 2 (AMD) | load | 1 | 0.08 | True | no | 0.0055 | 0.0068 | 0.069 |
| aes128-gcm@openssh.com | 2 (AMD) | load | 4 | 0.32 | True | no | 0.0201 | 0.0236 | 0.064 |
| aes128-gcm@openssh.com | 2 (AMD) | load | 16 | 1.26 | True | no | 0.0580 | 0.0671 | 0.046 |
| aes128-gcm@openssh.com | 2 (AMD) | load | 32 | 2.53 | True | no | 0.0880 | 0.1025 | 0.035 |
| aes128-gcm@openssh.com | 2 (AMD) | load | 64 | 5.05 | True | yes | 0.1106 | 0.1292 | 0.022 |
| aes256-gcm@openssh.com | 2 (AMD) | idle | 0 | 0.00 | True | no | 0.0003 | 0.0000 | - |
| aes256-gcm@openssh.com | 2 (AMD) | load | 1 | 0.08 | True | no | 0.0061 | 0.0055 | 0.078 |
| aes256-gcm@openssh.com | 2 (AMD) | load | 4 | 0.32 | True | no | 0.0169 | 0.0208 | 0.054 |
| aes256-gcm@openssh.com | 2 (AMD) | load | 16 | 1.26 | True | no | 0.0492 | 0.0567 | 0.039 |
| aes256-gcm@openssh.com | 2 (AMD) | load | 32 | 2.53 | True | no | 0.0791 | 0.0934 | 0.031 |
| aes256-gcm@openssh.com | 2 (AMD) | load | 64 | 5.05 | True | yes | 0.1059 | 0.1218 | 0.021 |
| default | 2 (AMD) | idle | 0 | 0.00 | True | no | 0.0003 | 0.0000 | - |
| default | 2 (AMD) | load | 1 | 0.08 | True | no | 0.0058 | 0.0068 | 0.073 |
| default | 2 (AMD) | load | 4 | 0.32 | True | no | 0.0187 | 0.0226 | 0.059 |
| default | 2 (AMD) | load | 16 | 1.26 | True | no | 0.0533 | 0.0621 | 0.042 |
| default | 2 (AMD) | load | 32 | 2.53 | True | no | 0.0805 | 0.0928 | 0.032 |
| default | 2 (AMD) | load | 64 | 5.05 | True | yes | 0.1064 | 0.1223 | 0.021 |
| aes128-gcm@openssh.com | 3 (AMD) | idle | 0 | 0.00 | True | no | 0.0007 | 0.0000 | - |
| aes128-gcm@openssh.com | 3 (AMD) | load | 1 | 0.08 | True | no | 0.0048 | 0.0048 | 0.060 |
| aes128-gcm@openssh.com | 3 (AMD) | load | 4 | 0.32 | True | no | 0.0133 | 0.0147 | 0.042 |
| aes128-gcm@openssh.com | 3 (AMD) | load | 16 | 1.27 | True | no | 0.0321 | 0.0386 | 0.025 |
| aes128-gcm@openssh.com | 3 (AMD) | load | 32 | 2.53 | True | no | 0.0460 | 0.0518 | 0.018 |
| aes128-gcm@openssh.com | 3 (AMD) | load | 64 | 5.06 | True | no | 0.0687 | 0.0779 | 0.014 |
| aes256-gcm@openssh.com | 3 (AMD) | idle | 0 | 0.00 | True | no | 0.0003 | 0.0000 | - |
| aes256-gcm@openssh.com | 3 (AMD) | load | 1 | 0.08 | True | no | 0.0041 | 0.0044 | 0.052 |
| aes256-gcm@openssh.com | 3 (AMD) | load | 4 | 0.32 | True | no | 0.0137 | 0.0161 | 0.043 |
| aes256-gcm@openssh.com | 3 (AMD) | load | 16 | 1.26 | True | no | 0.0307 | 0.0365 | 0.024 |
| aes256-gcm@openssh.com | 3 (AMD) | load | 32 | 2.53 | True | no | 0.0487 | 0.0565 | 0.019 |
| aes256-gcm@openssh.com | 3 (AMD) | load | 64 | 5.06 | True | no | 0.0628 | 0.0702 | 0.012 |
| default | 3 (AMD) | idle | 0 | 0.00 | True | no | 0.0000 | 0.0000 | - |
| default | 3 (AMD) | load | 1 | 0.08 | True | no | 0.0055 | 0.0051 | 0.069 |
| default | 3 (AMD) | load | 4 | 0.32 | True | no | 0.0133 | 0.0167 | 0.042 |
| default | 3 (AMD) | load | 16 | 1.26 | True | no | 0.0382 | 0.0430 | 0.030 |
| default | 3 (AMD) | load | 32 | 2.53 | True | no | 0.0511 | 0.0582 | 0.020 |
| default | 3 (AMD) | load | 64 | 5.05 | True | no | 0.0683 | 0.0764 | 0.014 |

#### R_ssh fits: ssh cores = R_ssh × (tokens/s ÷ 1000) + fixed, over the idle phase and the valid levels that were not host-bound

| cipher asked | rep (runner) | R_ssh (cores per 1k tok/s) | fixed cores (8 relays) | fixed per relay | points |
|---|---:|---:|---:|---:|---:|
| aes128-gcm@openssh.com | 1 (Intel) | 0.0113 | 0.00840 | 0.00105 | 6 |
| aes128-gcm@openssh.com | 2 (AMD) | 0.0347 | 0.00531 | 0.00066 | 5 |
| aes128-gcm@openssh.com | 3 (AMD) | 0.0131 | 0.00746 | 0.00093 | 6 |
| aes256-gcm@openssh.com | 1 (Intel) | 0.0112 | 0.00614 | 0.00077 | 6 |
| aes256-gcm@openssh.com | 2 (AMD) | 0.0307 | 0.00462 | 0.00058 | 5 |
| aes256-gcm@openssh.com | 3 (AMD) | 0.0122 | 0.00790 | 0.00099 | 6 |
| default | 1 (Intel) | 0.0117 | 0.00785 | 0.00098 | 6 |
| default | 2 (AMD) | 0.0315 | 0.00537 | 0.00067 | 5 |
| default | 3 (AMD) | 0.0132 | 0.00910 | 0.00114 | 6 |

Negotiated (from every relay's ssh -v log):

- aes128-gcm@openssh.com: {"kex=sntrup761x25519-sha512@openssh.com; client->server aes128-gcm@openssh.com <implicit> none; server->client aes128-gcm@openssh.com <implicit> none": 8}
- aes256-gcm@openssh.com: {"kex=sntrup761x25519-sha512@openssh.com; client->server aes256-gcm@openssh.com <implicit> none; server->client aes256-gcm@openssh.com <implicit> none": 8}
- default: {"kex=sntrup761x25519-sha512@openssh.com; client->server chacha20-poly1305@openssh.com <implicit> none; server->client chacha20-poly1305@openssh.com <implicit> none": 8}

### R_front

Pairs whose arms were both valid and not host-bound (all CPUs at least 85% busy).

| tunnels | streams | rep (runner) | k tok/s | front cores via the front | front cores via tcpmux | R_front per 1k tok/s | frps cores difference | front + frps difference per 1k tok/s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 1 | 1 (AMD) | 0.20 | 0.044 | 0.021 | 0.113 | -0.003 | 0.099 |
| 10 | 1 | 2 (Intel) | 0.20 | 0.035 | 0.017 | 0.093 | -0.002 | 0.080 |
| 10 | 1 | 3 (Intel) | 0.20 | 0.025 | 0.012 | 0.070 | -0.002 | 0.058 |
| 10 | 8 | 1 (AMD) | 1.58 | 0.261 | 0.101 | 0.102 | -0.001 | 0.101 |
| 10 | 8 | 2 (Intel) | 1.58 | 0.183 | 0.071 | 0.071 | -0.010 | 0.065 |
| 10 | 8 | 3 (Intel) | 1.58 | 0.127 | 0.043 | 0.053 | -0.006 | 0.049 |
| 50 | 1 | 1 (AMD) | 0.99 | 0.213 | 0.088 | 0.127 | -0.019 | 0.107 |
| 50 | 1 | 2 (Intel) | 0.99 | 0.156 | 0.054 | 0.103 | -0.006 | 0.097 |
| 50 | 1 | 3 (Intel) | 0.99 | 0.116 | 0.039 | 0.078 | -0.007 | 0.071 |
| 50 | 4 | 1 (AMD) | 3.95 | 0.739 | 0.259 | 0.122 | -0.039 | 0.112 |
| 50 | 4 | 2 (Intel) | 3.95 | 0.500 | 0.151 | 0.089 | -0.035 | 0.080 |
| 50 | 4 | 3 (Intel) | 3.95 | 0.400 | 0.109 | 0.074 | -0.016 | 0.070 |
| 100 | 1 | 1 (AMD) | 1.97 | 0.491 | 0.165 | 0.165 | -0.027 | 0.151 |
| 100 | 1 | 2 (Intel) | 1.97 | 0.368 | 0.113 | 0.129 | -0.016 | 0.121 |
| 100 | 1 | 3 (Intel) | 1.98 | 0.274 | 0.070 | 0.103 | -0.008 | 0.099 |
| 100 | 4 | 2 (Intel) | 7.87 | 1.252 | 0.272 | 0.124 | -0.061 | 0.117 |
| 100 | 4 | 3 (Intel) | 7.90 | 0.926 | 0.191 | 0.093 | -0.031 | 0.089 |

## Verdict

**Mass re-login.**
- By the issue's definition (a probe passes in the new epoch), every size up to 600 tunnels was back within 11 s, and no tunnel took over 60 s. Both plugins answered every call within 164 ms, and refused none.
- What a client sees is set by the front, not by frp or the plugins:
  - **the router's O(N²) rebuild** (#417), which held the first request on each tunnel 4–19 s at 300 tunnels and 19–35 s at 600, and at 600 after an frps restart past the 44 s observed;
  - after an frps restart, **HBEP-5's 30 s pause** on every Instance that saw three failed requests while frps was down or before frpc re-registered (#418).
- Below about 100 tunnels the client is back at frpc's 2.0–2.2 s backoff floor plus a probe, but for the pause.

**A capped shared link** starves no heartbeat. No discipline, cap or size measured failed a heartbeat, down to the cap where the tunnels' own token bytes bind. A bloated FIFO buffer costs latency (about 2 s per request at 1 s of buffer, both ways), not tunnels. The link bounds the rate, as S1c's rule says, at about 280 bytes per token up the link in direct mode.

**R_ssh and R_front replace S14's provisional terms:**
- `R_ssh` is a per-packet cost, the cipher making no difference: 0.05–0.08 × q for relays at one stream (20 tokens/s), falling to 0.012–0.022 × q at 1,260 tokens/s per relay. S14's 0.045 × q is too low by about 1.7× for lightly loaded relays.
- `R_front` is 0.11–0.12 × q, fitted (0.05–0.17 × q per measured pair), 3–5 times S14's floor s × a.

**The per-front tunnel limit the measurements support:** measured to 600 by the probe; client-visible recovery grows about as N² until #417 is fixed. Until then, a front whose clients must see a restart's recovery within a few seconds stays at or below about 100 tunnels: at 300, the first token through the front took up to 11 s after a front restart, and 36 s after an frps restart (with #418's pause). The steady-state budget, with the measured reserves, gives one front core about:
- 3,000 (AMD fits) to 4,060 (Intel fit) tokens/s at 100 tunnels in direct mode, 2,370–3,560 at 300, and 1,450–2,820 at 600;
- 1,550–2,320 tokens/s at 100 host-dialed tunnels with one stream per relay, and 630–1,680 at 300.

These are about 0.45–0.62 of S14's estimates with the provisional reserves.

## Proposed HBEP-6 text for open question 11

For the science agent to amend. It replaces the item's last three bullets ("The steady-state per-front budget", "Until the open items are measured", "Still open"); the bullets on steady state, memory, drops and latency stand.

> - **The steady-state per-front budget** (S14, with S16's measured reserves; spike S16, issue #412), with `q = tokens/s ÷ 1000`, keeps `a × q + b × tunnels + c + R_front + R_ssh` under 70% of the cores left to these processes on the front's host:
>   - R_front = 0.11–0.12 × q is the front's own proxy (S16, fitted on 4-CPU hosted runners; 0.05–0.17 × q per measured pair), 3–5 times S14's floor;
>   - R_ssh, in host-dialed mode, is ssh's cost per relay, per packet rather than per token: 0.08 × q for relays carrying one stream (20 tokens/s), falling to 0.02 × q at about 1,300 tokens/s per relay, with no difference between OpenSSH's default (chacha20-poly1305) and aes128-gcm or aes256-gcm. The cluster's own negotiation is unknown.
>   - That is about 3,000–4,000 tokens/s per front core at 100 direct tunnels, 2,400–3,600 at 300 and 1,400–2,800 at 600, and 1,500–2,300 at 100 host-dialed tunnels with one stream per relay, for 60-token streams with a new connection per request.
> - **A mass re-login** (S16): after an frps or a front restart, every tunnel of 600 re-registered within 3.4 s (frpc's backoff floor is 2.0–2.2 s) and was routed again by the front's probe within 11 s. Both plugins answered every call within 0.2 s, far inside the 1 s rule.
> - **What the client sees after a restart is the front's, and is open:**
>   - the router rebuilds its whole view under its lock on every registration change, so recovery grows about as N²: the first request on a tunnel took 4–19 s at 300 tunnels and 19–35 s at 600, and after an frps restart at 600 most tunnels had served no token through the front 44 s after the cut (#417);
>   - after an frps restart, HBEP-5's pause holds every Instance with three failed requests in the restart's window for 30 s after its tunnel is back (#418).
> - **A capped shared link starves no heartbeat** (S16): 100 tunnels × 4 streams and 300 × 1, behind a shared FIFO queue of 100 ms and 1 s of the cap and behind per-connection queues, with filler bytes holding the link at its cap, lost no heartbeat from 200 down to 12.5 Mb/s, where the tunnels' own demand binds. A deep buffer adds its delay to every request, about 2 s at 1 s of buffer.
> - **The per-front limit:** measured to 600 tunnels by the probe; until #417 is fixed, client-visible recovery grows about as N², so keep a front at or below about 100 tunnels where clients must see a restart's recovery within a few seconds. It's an operating limit; no code enforces it.
> - **Still open:** a dedicated front host near full CPU, since the runners carried both ends; the client-facing TLS in front of the front; the cluster's ssh cipher.

## Environment

- **The run:** workflow run 43115471218 on draft PR #415, whose head merges the spike's commit `3ba7df22` with a one-off workflow, never merged. 15 jobs, 11 Nov 2026, 22:54–23:25 UTC: relogin 31 min, caplink 14 min, rssh 12 min and rfront 10 min per job.
  - The commits after `3ba7df22` change only `analysis.py` (the routed-again witness, the first request's TTFT, path masking, a guard), `summarize.py`, `tests/`, `README.md`, `results/` and `cmd/s16/refresh_test.go`, so the harness in this directory is the one that ran.
  - The raw files stay in the run's artifacts until 25 Nov 2026.
  - The committed files keep every measured value the tables use; the large ones are xz-compressed (`summarize.py extract`, preset 6, each read back byte for byte):
    - `calls.csv.xz`: every plugin call;
    - `relogin_instances.csv.xz`: per Instance, its times and pauses;
    - `link_series.csv.xz`: the link's per-second bytes;
    - `tunnels.csv.xz`: every tunnel's emptiest second;
    - `samples.csv.xz` and `host.csv.xz`: `/proc` every second.

    Together they are 2.1 MB, 30 MB as CSV. `summarize.py tables` rebuilds the tables from them byte for byte.
- **Runners:** fresh GitHub-hosted `ubuntu-24.04` runners, 4 CPUs, 15,982–15,983 MB, Linux 7.0.0-1017-azure, by job:

| job | rep 1 | rep 2 | rep 3 |
|---|---|---|---|
| relogin | Intel Xeon Platinum 8573C | AMD EPYC 9V45 | AMD EPYC 9V74 |
| caplink 100 × 4 | AMD EPYC 9V74 | Intel Xeon Platinum 8370C | AMD EPYC 7763 |
| caplink 300 × 1 | AMD EPYC 9V74 | AMD EPYC 9V45 | AMD EPYC 7763 |
| rssh | Intel Xeon Platinum 8370C | AMD EPYC 7763 | AMD EPYC 9V74 |
| rfront | AMD EPYC 7763 | Intel Xeon Platinum 8370C | Intel Xeon Platinum 8573C |

  - The tables name each repetition's class (AMD or Intel). S14 found coefficients that differ by class and spread up to 44% within one model, so the coefficients are given per class.
  - This run's runners span five models, three of which S14 did not see (AMD 9V45 and 7763, Intel 8370C), so the measured reserves are combined with S14's stack fits of other models of the same class.
- **Software:** OpenSSH 9.6p1 (the runners' ssh and sshd); Go from `hack/fetch-go.sh`, Python 3.12.3, open files raised to the hard limit.
- **Priority:** frps, the admission plugin, the front, the link, the client and the filler at nice 0; frpc and the node at nice 10, as in S14.
- **The development VM** (6 cores, shared) ran only builds and smoke runs at 1–10 tunnels, at nice 10 under `systemd-run --user --scope -p MemoryMax=4G`, with the 1-minute load at most 6.7 (other agents' work). Every smoke run finished; nothing was left running.
  - The R_ssh smokes started the rehearsal sshd only under `launcher.lock`, from a tree that contains #309.
  - `TestRouterRefreshCost` ran there too.

## Provenance

| Component | Version and pin | Project and licence | Use |
|---|---|---|---|
| frp (frps, frpc) | v0.71.1 linux_amd64; tarball sha256 `1d987521…f803` (S1c's pin); frps `23227629…6ab3` and frpc `35c6054d…c28b` from `internal/launcher/frp-pins.txt` | [fatedier/frp](https://github.com/fatedier/frp), Apache-2.0 | the tunnels; its source, for the facts above |
| OpenSSH | the runner's `ssh` and `sshd` (Ubuntu 24.04's openssh 9.6p1) | [OpenSSH](https://www.openssh.com/), BSD | item 3's relays and login node |
| client-go's fake dynamic client | `k8s.io/client-go` v0.35.3 (the repository's pin) | [kubernetes/client-go](https://github.com/kubernetes/client-go), Apache-2.0 | the front's API without a control plane |
| `internal/front`, `internal/frpsplugin`, `internal/launcher`, `internal/agent`, `internal/hostdial`, `internal/slurm/sshrun`, `internal/rehearsal`, `internal/slurm/fakeslurm`, `internal/agent/enginedouble` | this project, unchanged, at the branch's base `eaa225ed` | this project | the stack under test |
| S15's link emulator | `spikes/s15/cmd/s15link` at `eaa225ed` (S1c's shaper), with a FIFO queue, a stated buffer and per-second counters added | this project | `cmd/s16link` |
| S14's harness | `spikes/s14` at `eaa225ed`: the tunnel dial, the stats, the closed loop, the node, the driver's sampler and the CPU and validity rules | this project | adapted in `cmd/s16`, `node/`, `s16.py`, `analysis.py` |
| GitHub Actions | `actions/checkout` v7.0.1, `actions/cache/restore` v6.1.0, `actions/upload-artifact` v7.0.2, pinned as in `ci.yml`; Go from `hack/fetch-go.sh` | GitHub, MIT | the hosted runs (a one-off workflow on a draft PR, never merged) |
| Go, Python | Go 1.27.3 on the VM, `hack/fetch-go.sh`'s on the runners; Python 3 standard library only | | the harness and the driver |

- **New:** `cmd/s16` (setup, admit, frps, front, load, filler, owner, the plugin timing), the login and node test binaries, `s16.py`, `analysis.py`, `summarize.py`, `s16ssh.sh`.
- **No third-party code is copied.**

## Deviations from the issue

- **The link is S1c's shaper, not toxiproxy** (Method, item 2): toxiproxy's bandwidth toxic caps each connection separately, so it cannot make a shared cap. The FIFO queue and its buffers are team-lead's ruling.
- **The cap is filled by filler bytes beside the tokens,** not by token streams alone (team-lead's approval): a 4-CPU runner reaches 85% busy near 12,000 tokens/s through tunnels, below the 20,500 tokens/s of the lowest cap.
- **The ladder goes below 50 Mb/s,** to find the cap where heartbeats first fail, as the issue asks.
- **Item 1 runs at 5 tokens/s per stream** at every size, so 600 tunnels stay under 85% busy; validity is "every tunnel streamed before the restart" (approved by k343).
- **The routing table and the admission plugin's Index are fixtures,** not the controller's. The front's watch, router, probe, proxy and plugin, and the admission plugin's handler and Index type are the product's.
- **The cipher is bracketed,** since the cluster's negotiation is unknown (k343's ruling).
- **Added:** the hold-off control and the per-Instance pause records (k343, after the VM smoke showed the pause), the first request's time to first token after a cut, and the router's rebuild cost (`refresh_test.go`), after the hosted run showed the router's lag (#417).
- **The 600-tunnel re-login rests on 2 repetitions** per event, the third (the Intel 8573C runner's) failing the validity rule; and two capped-link configurations on 2 (Results). Nothing was rerun.

## Unknowns

- **The front near full CPU,** and a front host that does not also carry 600 frpc and their engines: the runners carried both ends, and one second of the 600-tunnel recovery crossed 85% busy.
- **Client-visible recovery at 600 tunnels after an frps restart:** the observed 44 s ended before it did; #417 and #418's fixes will show what remains.
- **Steady state at 600 tunnels on the Intel 8573C runner:** every 600-tunnel run there had a pre-window second in which no tunnel received a token, and is not counted.
  - The closed loop's requests had fallen into step: at 600 tunnels every first request after a registration wave waits for the router, so they all restart together. A whole-second gap then empties every tunnel at once.
  - Whether a real client mix shows this is unknown.
- **A drop-tail bottleneck.** The emulator terminates TCP, so a full queue holds the sender back, where a real router drops and TCP backs off; and its rates were set, with the round trip at 0.
- **The cluster's ssh negotiation** and its sshd's cost, and ssh's cost with several Instances per Allocation.
- **Client TLS in front of the front,** and the front's agent passthrough in direct mode.
- **A second frps's re-login** (S15's bulk layout), which was not run.

## Files and how to run

| File | What |
|---|---|
| `fetch.sh` | frp, sha256-checked, into `$S16_BIN`; builds `s16`, `s16link`, `s16node.test` and `s16login.test`; installs the ssh wrapper |
| `cmd/s16/` | `setup`, `frpcconf`, `admit`, `frps`, `front`, `load`, `filler`, `owner`, with unit tests; `refresh_test.go`, the router's rebuild cost (#417), gated by `S16_MEASURE_REFRESH=1` |
| `cmd/s16link/` | the link emulator, with unit tests |
| `node/` | the engine doubles, the job sides and the relay step, a test binary |
| `login/` | the rehearsal login node, a test binary |
| `s16ssh.sh` | the relays' ssh: the system's ssh with `-v -E <log>` |
| `s16.py` | the driver: `relogin`, `caplink`, `rssh`, `rfront`, `host`, `leftovers` |
| `analysis.py`, `tests/` | the result rules, with their tests |
| `summarize.py` | `extract` (raw files to `results/`, with a scan for host paths), `tables`, `verify` (every CPU figure from the samples), `budget` (one front core's tokens/s with the measured reserves) |
| `results/` | the committed results (Results) |

```sh
export S16_BIN=<dir> S16_WORK=<short scratch dir, mode 700> S16_RESULTS=<dir>
bash fetch.sh
(go vet ./... && go test ./... && python3 -m unittest discover -s tests)
python3 s16.py relogin --rep 1          # about 35 min on a 4-CPU runner
python3 s16.py caplink --rep 1 --tunnels 100 --streams 4
python3 s16.py caplink --rep 1 --tunnels 300 --streams 1
python3 s16.py rssh --rep 1             # needs OpenSSH's sshd (HPCBURST_SSHD)
python3 s16.py rfront --rep 1
python3 s16.py leftovers
python3 summarize.py extract <the runs' artifacts> results
python3 summarize.py tables; python3 summarize.py verify; python3 summarize.py budget
# the router's rebuild cost (#417), at 100, 300 and 600 tunnels: a measurement, skipped unless asked
(cd cmd/s16 && S16_MEASURE_REFRESH=1 go test -run TestRouterRefreshCost -v .)
```

Ports, all on 127.0.0.1, in #412's block 30130–30159, laid out by `agentapi.Host` at base 30130: 30134 the front's health address and plugin, 30137 its dial-home listener, 30138 the admission plugin, 30139–30141 frps (bind, tcpmux, web), 30142 the front's client listener, 30143 the link, 30144 the filler's link port, 30145 the filler's sink. The login node's sshd and the job sides take kernel-chosen ports. Every run checks with `ss` that every listener its processes hold is on 127.0.0.1, and `s16.py leftovers` that nothing is left in the block.
