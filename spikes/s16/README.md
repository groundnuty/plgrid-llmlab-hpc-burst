# Spike S16: open question 11's open items (HBEP-6)

Issue #412. HBEP-6's open question 11 keeps a front at or below 300 tunnels until five items are measured; spike S14 measured steady state only. This spike measures the four that need no GPU, on one host, on loopback:

1. a mass re-login of 100, 300 and 600 frpc through both of frps's server plugins (the lifecycle controller's admission plugin and the front's), after an frps restart and, separately, after a front restart;
2. 100 and 300 tunnels sharing a capped link at 50, 100 and 200 Mb/s, and below, to the cap where heartbeats first fail;
3. real ssh's CPU per relay in host-dialed mode (`R_ssh`);
4. the front's own proxy cost (`R_front`).

The measurements ran on GitHub-hosted runners (Environment). The shared development VM ran only builds and smoke runs at 1 to 10 tunnels.

## Findings

(Filled from the hosted run.)

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
- **Per tunnel:** *registered again*, the front's plugin answering an accepted `NewProxy` for the UID after the cut (frps registers the proxy only after both plugins answer); *routed again*, the front counting the UID registered again, a probe having passed in the new epoch (the issue's definition); and *first token*, the first content token through the front's proxy after the cut (what a client sees).
- **The plugins:** every call's latency at its server, p50, p99 and max, against HBEP-6's 1 s rule; refusals per plugin and op. The latency runs from the request's first byte read to the reply's first write, so it leaves out what frps waits for before that: its connect and the listener's accept queue, both on loopback.
- **The front host's CPU:** frps, the admission plugin and the front, over the 10 s after the cut, and its busiest second.
- **The pause:** per Instance, from the front's own log and usage records: whether the front paused it (HBEP-5's `pauseAfterFailures` 3, `pauseDuration` 30 s), when, until when, and which failures counted, each with its time relative to the cut and to the re-registration.
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

(Filled from the hosted run: `python3 summarize.py tables`.)

## Verdict

(Filled from the hosted run.)

## Proposed HBEP-6 text for open question 11

(Filled from the hosted run.)

## Environment

(Filled from the hosted run.)

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
- **Added:** the hold-off control and the per-Instance pause records (k343, after the VM smoke showed the pause).

## Unknowns

(Filled from the hosted run.)

## Files and how to run

| File | What |
|---|---|
| `fetch.sh` | frp, sha256-checked, into `$S16_BIN`; builds `s16`, `s16link`, `s16node.test` and `s16login.test`; installs the ssh wrapper |
| `cmd/s16/` | `setup`, `frpcconf`, `admit`, `frps`, `front`, `load`, `filler`, `owner`, with unit tests |
| `cmd/s16link/` | the link emulator, with unit tests |
| `node/` | the engine doubles, the job sides and the relay step, a test binary |
| `login/` | the rehearsal login node, a test binary |
| `s16ssh.sh` | the relays' ssh: the system's ssh with `-v -E <log>` |
| `s16.py` | the driver: `relogin`, `caplink`, `rssh`, `rfront`, `host`, `leftovers` |
| `analysis.py`, `tests/` | the result rules, with their tests |
| `summarize.py` | `extract` (raw files to `results/`), `tables`, `verify` |
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
python3 summarize.py tables; python3 summarize.py verify
```

Ports, all on 127.0.0.1, in #412's block 30130–30159, laid out by `agentapi.Host` at base 30130: 30134 the front's health address and plugin, 30137 its dial-home listener, 30138 the admission plugin, 30139–30141 frps (bind, tcpmux, web), 30142 the front's client listener, 30143 the link, 30144 the filler's link port, 30145 the filler's sink. The login node's sshd and the job sides take kernel-chosen ports. Every run checks with `ss` that every listener its processes hold is on 127.0.0.1, and `s16.py leftovers` that nothing is left in the block.
