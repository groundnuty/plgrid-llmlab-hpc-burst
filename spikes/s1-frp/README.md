# Spike S1: frp dial-home kit

Kit for spike S1 (issue #18): does frp work as the dial-home tunnel from compute nodes, for long streaming responses? It carries no run results. **Nothing in it has been run on a cluster.**

Pinned release: **frp v0.71.1** (Apache-2.0, https://github.com/fatedier/frp), the latest stable at 2026-10-30 (the brief for this kit set v0.71.1 as the minimum).

## The local smoke test is a config check, not evidence

`local-smoke.sh` runs everything on this VM over 127.0.0.1 for 120 s. It shows that the configs load and that registration, streaming and fault detection work end to end. It does **not** show that a compute node can reach this VM, and 120 s says nothing about a stream that lasts an hour. Issue #18 rules out both readings. Its numbers (for example detection times on loopback) are not results of S1.

## Files

| File | What it is |
|---|---|
| `fetch-frp.sh` | Downloads frp v0.71.1 `linux_amd64` and `linux_arm64` into `dl/` (ignored by git) and checks each against the sha256 written in the script. `fetch-frp.sh verify <file> <arch>` checks one file. A mismatch deletes the download and extracts nothing. |
| `render_tmpl.py` | Fills the double-underscore placeholders of a template. Fails on an unfilled placeholder or an unused key. Keeps `#@<platform>` lines only for that platform. |
| `frps.toml.tmpl` | Server config: token from a file, tcpmux on a loopback port, **no vhost**, liveness settings, server plugin for `Login`, `NewProxy`, `CloseProxy`, `Ping`. |
| `frpc.toml.tmpl` | Instance config: `loginFailExit = false`, transport protocol placeholder, one tcpmux proxy named by the instance, liveness settings. |
| `plugin_stub.py` | Answers frps plugin calls: allows all, logs one JSON line per call (UTC time, op, user, proxy, run id). Never logs the token or its derived key. |
| `fake_streamer.py` | Fake engine on 127.0.0.1: `GET /stream?seconds=N` (a token per second, chunked), `GET /health`. Python 3.6 compatible. |
| `stream_watch.py` | Runs on the frps host. Reaches the instance through tcpmux (HTTP CONNECT), records token gaps, cuts and reconnects, probes `/health`, takes marks for fault timing, writes a JSON summary. |
| `job.sbatch.tmpl` | One job template for all three runs. Step 0 reachability probe, then fake engine plus frpc, until TERM. `PROBE_ONLY=1` stops after step 0. |
| `local-smoke.sh` | The loopback config check above. |

Frp pieces are reused as they are (`frps`, `frpc`, tcpmux, server plugins). The scripts here are new code on the Python standard library. The kit needs no pip packages.

## What the owner's decisions fill in

| Decision | Where it lands | Rule |
|---|---|---|
| **frps host: this VM.** Whether a compute node can reach a port on it is part of the spike. | `FRPS_ADDR` (address a compute node uses), `FRPS_PORT` (the one port the nodes dial) | The address stays out of every committed file. The operator reads it from a private settings file and passes it to `render_tmpl.py`. The kit never opens a port. |
| **Grant per platform** (the account a job bills) | `ACCOUNT` | Placeholder in the committed files. GPU partitions need an account with the matching suffix. |

Settings file variables used below (names only; values stay private): `S1_SETTINGS`, `FRPS_BIND_ADDR`, `FRPS_PORT`, `FRPS_ADDR_FROM_CLUSTER`, `TCPMUX_PORT` and `PLUGIN_PORT` (loopback ports on this VM), `TOKEN_FILE_VM`, `TOKEN_FILE_CLUSTER`, `KIT_DIR_CLUSTER`, `MEM`, `ACCOUNT_HELIOS_CPU`, `ACCOUNT_HELIOS_GH200`, `ACCOUNT_ATHENA`.

## The three runs

| Run | `PLATFORM` | Partition | Account | CPU arch | frpc | GPU |
|---|---|---|---|---|---|---|
| 1 | `helios-cpu` | `plgrid` | placeholder | x86_64 | amd64 | none |
| 2 | `helios-gh200` | `plgrid-gpu-gh200` | needs the `-gpu-gh200` suffix | aarch64 | arm64 | `--gres=gpu:1`, left idle |
| 3 | `athena` | `plgrid-gpu-a100` | needs the `-gpu-a100` suffix | x86_64 | amd64 | `--gres=gpu:1`, left idle |

- **Run 2 is the production path.** A GPU partition's outbound rules may differ from the CPU partition's, so run 1 does not stand in for it.
- **Why Athena and GH200 request a GPU.** Athena accepts GPU jobs only: its documentation warns that "extensive workloads that do not use GPUs lead to account suspension", and S5 found that general users can submit only to `plgrid-gpu-a100` (48 h) and `plgrid-now` (1 h, one job and one GPU per user). frp does not use the GPU, so the job asks for exactly one and keeps the time short. The GH200 run asks for one GPU the same way, as the run plan requires.
- **The frpc binary is chosen by `uname -m` at run time.** The job refuses to start if the node's architecture does not match its `PLATFORM` (a wrong-partition result would be mislabelled).
- **Athena has no Apptainer on its login node.** The job runs frpc (a static binary) and the Python scripts directly on the node, with no container.
- **Python on the compute nodes.** The scripts need Python 3.6 or newer and are written to run on 3.6. The job logs the interpreter it uses (`python:` line in `job.log`). If the configured `PYTHON` is missing or older than 3.6 it tries `python3.15` down to `python3.6`, and a full run stops with a clear error if none fits. A missing Python does not cost the probe: the TCP probe needs only bash, so the interpreter is looked for after it (before it only when a TLS port is probed), and a probe-only job logs the interpreter too. On Helios only, `PYTHON` can be a wrapper such as `apptainer exec <image> python3` (the owner's Helios practice is an Ubuntu 24.04 image). The GH200 nodes' host Python version is unknown until the first job logs it.

## Run order

Work from a copy of this directory. Runtime files (token, configs, logs, pids) go in a private directory outside the repo.

**A. On this VM, once**

1. `bash fetch-frp.sh` (both architectures; the output must end with `both architectures verified`).
2. Create a throwaway token, mode 600, and copy it to each cluster, mode 600: `umask 077; head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$TOKEN_FILE_VM"`.
3. Stage the kit with `dl/` on a filesystem the compute nodes see, for each cluster (compute nodes may have no internet): `rsync -a spikes/s1-frp/ <cluster>:"$KIT_DIR_CLUSTER"/`.
4. `source "$S1_SETTINGS"`, then `RUN=$(mktemp -d)` and render and verify frps:
   ```
   python3 render_tmpl.py frps.toml.tmpl BIND_ADDR="$FRPS_BIND_ADDR" BIND_PORT="$FRPS_PORT" \
     TCPMUX_PORT="$TCPMUX_PORT" PROXY_BIND_ADDR=127.0.0.1 TOKEN_FILE="$TOKEN_FILE_VM" \
     PLUGIN_ADDR="127.0.0.1:$PLUGIN_PORT" LOG_FILE="$RUN/frps.log" > "$RUN/frps.toml"
   FRPS=$(echo dl/linux_amd64/frp_*/frps); "$FRPS" verify -c "$RUN/frps.toml"
   ```
5. Start the stub, then frps, and record their PIDs (stop them later by these PIDs only):
   ```
   python3 plugin_stub.py --listen "127.0.0.1:$PLUGIN_PORT" --log "$RUN/stub.jsonl" >/dev/null 2>"$RUN/stub.err" & echo $! > "$RUN/stub.pid"
   "$FRPS" -c "$RUN/frps.toml" > "$RUN/frps.out" 2>&1 & echo $! > "$RUN/frps.pid"
   ```

**B. Step 0, reachability probe, for each run (before any frp step).** From inside a job on a compute node, try plain TCP to `FRPS_ADDR:FRPS_PORT` with a 5 s limit (`bash /dev/tcp`, since `nc` may be missing). If an HTTPS or wss port is in play, list it in `PROBE_TLS_PORTS` and the job also tries a TLS handshake there.

1. Render the job. The values for `PLATFORM`, `PARTITION` and the account come from the table above:
   ```
   python3 render_tmpl.py job.sbatch.tmpl PLATFORM=helios-cpu ACCOUNT="$ACCOUNT_HELIOS_CPU" PARTITION=plgrid \
     TIME=01:20:00 MEM="$MEM" FRPS_ADDR="$FRPS_ADDR_FROM_CLUSTER" FRPS_PORT="$FRPS_PORT" \
     PROBE_TCP_PORTS="$FRPS_PORT" PROBE_TLS_PORTS=none TOKEN_FILE="$TOKEN_FILE_CLUSTER" PROTOCOL=tcp \
     INSTANCE_PREFIX=s1-helios-cpu KIT_DIR="$KIT_DIR_CLUSTER" PYTHON=python3 > job-helios-cpu.sbatch
   ```
2. Submit the probe from a scratch directory on the cluster. It needs no token and no binaries: `PROBE_ONLY=1 sbatch --time=00:10:00 job-helios-cpu.sbatch`. On Athena also pass `--partition=plgrid-now` (1 h limit, one job and one GPU per user).
3. Read `$SCRATCH/s1-frp/<jobid>/probe.txt` and `job.log`. Each line says `OK`, `FAIL timeout (filtered or dropped)` or `FAIL refused (network open, nothing listening)`. Record which protocol got through.
4. **If nothing got through, the job exits 3 and the spike records "not reachable" for that platform and stops.** Opening a firewall port, or any other access change, is the owner's decision. A refusal means the network is open and frps is not listening: check step A5.

**C. Full run for each platform that passed step 0** (one at a time)

1. Render the job as in B1 (a `TIME` of `01:20:00` is the stream, the fault tests, the start-up and a small margin; `01:10:00` is the floor for a run without fault tests) and submit it: `sbatch job-<run>.sbatch`. Note the job id. The instance name is `<INSTANCE_PREFIX>-<jobid>`; `job.log` repeats it.
2. On this VM, immediately start the watcher. `--wait-up` makes its clock start at the first good probe, so queue time does not matter:
   ```
   python3 stream_watch.py --tcpmux "127.0.0.1:$TCPMUX_PORT" --domain <INSTANCE_PREFIX>-<jobid> \
     --wait-up 7200 --duration 4260 --stream-seconds 3660 --stub-log "$RUN/stub.jsonl" \
     --out "$RUN/summary-<run>.json" > "$RUN/watch-<run>.out" 2>&1 & echo $! > "$RUN/watch.pid"
   ```
   `watch-<run>.out` prints the moment the run clock starts. Call it T+0.
3. T+0 to T+61 min: touch nothing. One stream of 3660 s must run uncut.
4. Fault tests, each with a mark first. On this VM `kill -USR1 "$(cat $RUN/watch.pid)"`, then at once on the cluster `scancel --batch --signal=<SIG> <jobid>`. Wait out each one before the next:

   | At | Signal | Fault | What to expect |
   |---|---|---|---|
   | T+62 min | `HUP` | frpc stopped for 60 s, socket open, no FIN (silent loss) | probe down, `CloseProxy` within the silent-loss budget, then a new `Login` after the thaw |
   | T+65 min | `USR2` | fake engine killed, frpc alive | probe fails as `no_response`, `Ping` lines keep coming, no `CloseProxy` |
   | T+68 min | `USR1` | frpc killed | `CloseProxy` within seconds, probe `path_down` |

5. At T+71 min the watcher exits and writes the summary. `scancel <jobid>` stops the job cleanly (TERM, the trap stops frpc and the engine).
6. Collect: the summary, `$RUN/stub.jsonl`, and from the cluster `$SCRATCH/s1-frp/<jobid>/{job.log,probe.txt,frpc.log,direct-baseline.json}`.
7. After the last run, stop frps and the stub by the recorded PIDs: `kill "$(cat $RUN/frps.pid)" "$(cat $RUN/stub.pid)"`.

Before anything goes into the PR, strip addresses, host names, account names and job ids from the logs (the job and frpc logs contain them).

## Heartbeat budget (fixed before any run)

Settings, the same on both sides: yamux keepalive every **10 s** (`tcpMuxKeepaliveInterval`), application heartbeat every **10 s** (`heartbeatInterval`), session dropped after **25 s** without a heartbeat (`heartbeatTimeout`), TCP keepalive 30 s.

Reasoning, from the v0.71.1 source:

- With `tcpMux = true` (the default) frp turns the application heartbeat off unless it is set (`pkg/config/v1/client.go` and `server.go` default it to -1). Then `frpc` never sends a Ping and the `Ping` plugin op never fires. We set it, because the Ping line is the instance's liveness feed: it keeps arriving when only the engine has died.
- Silent loss, no FIN (frozen process, dead network): the yamux keepalive pings every 10 s and gives up after yamux's fixed 10 s write timeout, so the session ends within 10 + 10 = **20 s**. The application heartbeat is the backstop: frps drops a control session after 25 s without a Ping (checked every second), so at most **26 s** after the fault. In heartbeat terms: two missed pings tolerated, the third ends the session.
- Process death (kill, crash, exit): the kernel closes the socket, frps sees EOF and closes the control session, which fires `CloseProxy` at once.
- The `CloseProxy` call runs when frps closes a client's control session, not only when frpc asks (`server/control.go`, `closeProxy` for every proxy of a closed control).

Budget:

| Fault | Signal | Budget | Expected |
|---|---|---|---|
| frpc killed | `CloseProxy` at the stub | 5 s | under 2 s |
| frpc killed | `/health` probe declares down (2 failures in a row, 2 s apart) | 10 s | about 4 s |
| engine killed, frpc alive | `/health` probe declares down, class `no_response` | 10 s | about 4 s |
| engine killed, frpc alive | the path stays up: `Ping` keeps arriving, no `CloseProxy` | yes or no | yes |
| frpc frozen (silent loss) | `CloseProxy` at the stub | **30 s** | 10 to 20 s |
| frpc frozen | `/health` probe declares down | 10 s | about 8 s |
| frpc thawed after the freeze | new `Login` and `NewProxy`, probe up again | recorded, no budget | a few seconds after the thaw |

Notes for reading the numbers:

- Marks are taken on this VM before the cluster command is sent, so every measured time includes the command's own latency. They overstate detection time, which is the safe side. All times come from this VM's clock (the watcher's marks and the stub's timestamps), so clock skew with the compute node does not enter. `clock_offset_plus_one_way_s` in the summary is an estimate, for information only.
- Killing a process measures the FIN path. Only the freeze exercises the heartbeat budget, so the two are reported separately.
- A frozen frpc shows as `no_response` at the probe, the same as a dead engine. The `Ping` and `CloseProxy` lines from the stub tell them apart: Pings continuing means the agent is alive.
- A session lost for no injected reason during the hour (a `Login` after the first one with no mark before it) is a finding: yamux gives up after 10 s of silence, which a starved compute node might cause.

## Pass criteria (issue #18)

For each platform that passes step 0:

1. frpc starts inside a Slurm job on a compute node, as a plain user, and registers through the plugin endpoint. Evidence: `job.log` (`frpc: login to server success`) and a `Login` plus `NewProxy` line in `stub.jsonl`.
2. A stream behind frpc stays reachable through frps for at least 1 h. Evidence: `streams[0]` has `completed: true`, `requested_s` of 3660 and `duration_s` of at least 3600.
3. No stream is cut over that hour. Evidence: `cuts` is empty before the first mark, `gaps.over_threshold_unfaulted` is 0, and no entry of `stream_open_failures` or `health.failures_sample` has a time before the first mark.
4. Killing frpc, or the engine, marks the path down within the budget above. Evidence: `marks[]` and the stub lines.
5. Recorded for the PR: outbound protocol, reconnects, added latency, centre-side blocking (table below).

Verifications that prove nothing, so none of them goes into the results:

- A same-host run, or a run from a login node: neither shows that a compute node can reach out. This includes `local-smoke.sh`.
- A stream shorter than about 30 min: the cuts seen before came from deadlines of 180 s, 900 s and 1,800 s.
- A process kill as the test of the heartbeat budget: it measures the FIN path. Use the freeze.
- A `Ping` line as proof that the engine is up: it shows the agent is alive. The probe through the path shows the engine.

## Results table (to fill in, one column per run)

| | Helios CPU | Helios GH200 | Athena |
|---|---|---|---|
| Step 0: TCP to the frps port | | | |
| Step 0: TLS port (if any) | | | |
| Outbound protocol used (tcp or wss) | | | |
| frpc and frps version, frpc arch | | | |
| Python on the node (from `job.log`) | | | |
| Registered through the stub (`Login`, `NewProxy`) | | | |
| 1 h stream: completed, seconds, tokens | | | |
| Stream cuts before the first mark | | | |
| Token gaps: max, p99, count over 2 s | | | |
| Reconnects (extra `Login` lines; stream reconnects) | | | |
| Unprompted session loss during the hour | | | |
| Added latency (tunnel minus loopback on the node), new connection and kept-alive, ms | | | |
| Frozen frpc: probe down after, `CloseProxy` after, `Login` after thaw (s) | | | |
| Engine killed: probe down after, `Ping` continued, `CloseProxy` absent | | | |
| frpc killed: probe down after, `CloseProxy` after (s) | | | |
| Centre-side blocking: refused or filtered ports, killed processes, login-node limits | | | |
| Job time limit used | | | |

Latency note: the watcher cannot reach a compute node directly, so `added_latency_ms` stays empty. The node's own loopback baseline is in `direct-baseline.json`; compare it with `latency_baseline.tunnel` in the summary. The difference includes the network between the node and this VM, which is the cost a real front would pay.

## Not verified here

- Python 3.6 itself. No 3.6 interpreter was available (no Docker; `uv` has no 3.6 build). What was done instead: `ast.parse(..., feature_version=(3, 6))` on every script (it rejects walrus and positional-only parameters, but does not look inside f-strings, so the scripts use no f-strings), plus a hand check of the stdlib calls: no `ThreadingHTTPServer`, `fromisoformat`, `time_ns`, `capture_output`, `nullcontext`, `dataclasses`, type annotations or `from __future__ import annotations`. The first job logs the real interpreter.
- `wss`. frps accepts websocket only as plain `ws` on its bind port, so `wss` needs a TLS-terminating reverse proxy in front of it, which is an owner decision. The kit passes the protocol through; only `tcp` was exercised locally.
- The arm64 frpc and Slurm itself (`scancel --batch --signal`, the GPU environment variable names) were not run. The job template ran locally as a plain script, on loopback, with the same signals sent by `kill`.
- A reading of one run on one machine is not a statistic: the budget numbers above are targets to confirm, not measurements.

## Provenance of what is reused

| What | Origin | Licence |
|---|---|---|
| `frps`, `frpc`, tcpmux, server plugins, token auth, yamux multiplexing | frp v0.71.1, https://github.com/fatedier/frp | Apache-2.0 |
| The keepalive timing used in the budget (10 s write timeout) | `fatedier/yamux` v0.2.0, a fork of `hashicorp/yamux`, built into frp | MPL-2.0 |
| The stub, streamer, watcher, renderer, job template, smoke test | new, Python standard library and bash | this repo |
| The long-stream cut deadlines the watcher guards against (180 s, 900 s, 1,800 s) and the HTTP vhost 60 s trap | www-2027 lessons, as quoted in issue #18 | n/a |

Claims about frp's defaults and behaviour come from the v0.71.1 sources: `doc/server_plugin.md`, `pkg/config/v1/{client,server}.go`, `server/control.go`, `server/service.go`, `pkg/util/vhost/vhost.go` and `pkg/util/tcpmux/httpconnect.go`.
