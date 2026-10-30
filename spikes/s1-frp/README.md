# Spike S1: frp dial-home kit

Kit for spike S1 (issue #18): does frp work as the dial-home tunnel from compute nodes, for long streaming responses? It carries no run results. **Nothing in it has been run on a cluster.**

**Transport: option B (owner's choice, in a direct session with the science agent, 2026-10-30).** A pre-check showed that the agents VM, where frps runs, lets only TCP port 22 in from the Cyfronet side. So frpc does not dial frps directly. The job opens `ssh -N -L` to the VM over port 22 and frpc dials the local end of that forward:

```
compute node                                              agents VM
frpc -> 127.0.0.1:LPORT --- ssh -N -L (TCP 22, restricted key) ---> sshd -> 127.0.0.1:FRPS_PORT (frps, loopback only)
```

`TRANSPORT=direct` is still in the job template, for the case that the owner opens a port later. It is not the default and nothing here asks for it.

Pinned release: **frp v0.71.1** (Apache-2.0, https://github.com/fatedier/frp), the latest stable at 2026-10-30 (the brief for this kit set v0.71.1 as the minimum).

## The local smoke test is a config check, not evidence

`local-smoke.sh` runs everything on this VM over 127.0.0.1 for 120 s. It shows that the configs load and that registration, streaming and fault detection work end to end. It has two variants. `direct` always runs. `ssh-forward` puts a real `ssh -N -L` between frpc and frps, through a throwaway sshd that the script starts as the current user (not root) on a high loopback port, with its own host key, its own `AuthorizedKeysFile` (holding the README's restricted key line) and a temporary directory that it deletes; it never reads or writes `~/.ssh`. If no sshd is installed it prints `SKIPPED ... this is not a pass` and exits 0. It does **not** show that a compute node can reach this VM, and 120 s says nothing about a stream that lasts an hour. Issue #18 rules out both readings. Its numbers (for example detection times on loopback) are not results of S1.

`tests/run-all.sh` runs the kit's own tests (job logic against a double and against the throwaway sshd, the watcher's HTTP checks, the verdict, the key line). They test the kit, not the spike; see "Not verified here".

## Files

| File | What it is |
|---|---|
| `fetch-frp.sh` | Downloads frp v0.71.1 `linux_amd64` and `linux_arm64` into `dl/` (ignored by git) and checks each against the sha256 written in the script. `fetch-frp.sh verify <file> <arch>` checks one file. A mismatch deletes the download and extracts nothing. |
| `render_tmpl.py` | Fills the double-underscore placeholders of a template. Fails on an unfilled placeholder or an unused key. Keeps `#@<platform>` lines only for that platform. |
| `frps.toml.tmpl` | Server config: token from a file, tcpmux on a loopback port, **no vhost**, liveness settings, server plugin for `Login`, `NewProxy`, `CloseProxy`, `Ping`. |
| `frpc.toml.tmpl` | Instance config: `loginFailExit = false`, transport protocol placeholder, one tcpmux proxy named by the instance, liveness settings. |
| `plugin_stub.py` | Answers frps plugin calls: allows all, logs one JSON line per call (UTC time, op, user, proxy, run id). Never logs the token or its derived key. |
| `fake_streamer.py` | Fake engine on 127.0.0.1: `GET /stream?seconds=N` (a token per second, chunked), `GET /health` (the fixed body `{"status": "ok"}`). `--request-log` records each request, which is how the job sees the watcher attach. Python 3.6 compatible. |
| `stream_watch.py` | Runs on the frps host. Reaches the instance through tcpmux (HTTP CONNECT), records token gaps, cuts and reconnects, probes `/health`, takes marks for fault timing, writes a JSON summary. It accepts a `/health` reply only when it is complete (status 200, the whole Content-Length body, the exact expected body) and a stream only when its chunk framing and final blank line arrive: a premature EOF is a cut or a failed probe. A mark taken while the probe is not `up` is recorded as `valid: false`. |
| `probe_responder.py` | Run by the operator on the VM for `PROBE_ONLY=1` jobs: a loopback listener (127.0.0.1, the port frps will use) that sends one fixed token line and closes. The job must read that token through its ssh forward. Stdlib only; it stops by itself after 2 h. |
| `verdict.py` | Reads the watcher summary and the job's logs and prints PASS or FAIL per issue #18 criterion. `stream_watch.py` exiting 0 means it collected, not that the experiment passed. |
| `job.sbatch.tmpl` | One job template for all three runs. Step 0 probes (one JSON line each), then fake engine, the ssh forward in a reconnect loop and frpc, a gate on frpc's login, a gate on the watcher, until TERM. `PROBE_ONLY=1` stops after step 0. `TRANSPORT=ssh-forward` (default) or `direct`. The job refuses a restart and never reuses a run directory. |
| `local-smoke.sh` | The loopback config check above, variants `direct`, `ssh-forward`, `all`. |
| `tests/` | The kit's own tests and the throwaway sshd they use (`run-all.sh`). |

Frp pieces are reused as they are (`frps`, `frpc`, tcpmux, server plugins). The scripts here are new code on the Python standard library. The kit needs no pip packages.

## What the owner's decisions fill in

| Decision | Where it lands | Rule |
|---|---|---|
| **frps host: this VM**, reached by an ssh forward over port 22 (option B). | `VM_ADDR` (address a compute node uses), `FRPS_PORT` (frps' port on the VM's loopback, 7000; the only port the key may forward to), `VM_USER`, `SSH_KEY`, `SSH_KNOWN_HOSTS`, `LPORT` (the node-local end) | The address and the user stay out of every committed file. The operator reads them from a private settings file and passes them to `render_tmpl.py`. The kit never opens a port. |
| **A dedicated, restricted ssh key** (owner's step) | `SSH_KEY`, plus one line in the VM's `authorized_keys` | See "Key and host-key handling". |
| **Grant per platform** (the account a job bills) | `ACCOUNT` | Placeholder in the committed files. GPU partitions need an account with the matching suffix. |

Settings file variables used below (names only; values stay private): `S1_SETTINGS`, `FRPS_PORT` (7000), `VM_ADDR`, `VM_USER`, `SSH_KEY_CLUSTER`, `KNOWN_HOSTS_CLUSTER`, `LPORT` (a free port on the node, 20000 to 60000), `TCPMUX_PORT` and `PLUGIN_PORT` (loopback ports on this VM), `TOKEN_FILE_VM`, `TOKEN_FILE_CLUSTER`, `KIT_DIR_CLUSTER`, `MEM`, `ACCOUNT_HELIOS_CPU`, `ACCOUNT_HELIOS_GH200`, `ACCOUNT_ATHENA`.

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

## Key and host-key handling (the owner's step, before step 0)

The kit never creates a key, never edits `authorized_keys` and never touches `~/.ssh`. The owner does these steps; the job only reads the files named in its placeholders.

1. **One dedicated key per cluster**, ed25519, no passphrase (jobs run unattended), generated on the cluster that uses it so the private half never travels. Helios (CPU and GH200 runs) is one cluster and shares a key; Athena has its own. Keep each key in a private mode-700 directory, the private file mode 600: `ssh-keygen -t ed25519 -N '' -C s1-spike -f "$SSH_KEY_CLUSTER"`. The comment `s1-spike` is how the key is found again at cleanup.
2. **Authorise each public key on the VM** for the VM account `VM_USER`, one line each, in that account's `authorized_keys`:
   ```
   restrict,port-forwarding,permitopen="127.0.0.1:7000",permitlisten="127.0.0.1:1",command="/bin/false" ssh-ed25519 <public key> s1-spike
   ```
   `restrict` switches every extra off (shell, pty, agent and X11 forwarding, user rc files); `port-forwarding` switches forwarding back on, both directions; `permitopen` allows exactly one `-L` destination, frps' loopback port, so the number must equal `FRPS_PORT`; `permitlisten` limits `-R` listeners (permitopen does not touch them) to 127.0.0.1 port 1, a privileged port that a non-root account cannot bind while `net.ipv4.ip_unprivileged_port_start` is 1024 (the default; check `sysctl net.ipv4.ip_unprivileged_port_start` on the VM; at 0 the key could open that one loopback port, which the optional server-side setting below closes), so no usable `-R` listener can open; `command="/bin/false"` leaves no shell even if a client asks for one. `permitlisten` needs OpenSSH 7.8 or newer; this VM runs OpenSSH 9.6p1 (`ssh -V`, and `man sshd` lists the option). No `from=` list is given because the compute-node source addresses are not known. The owner may add one if the Cyfronet ranges are known.

   What the line does and does not stop was run, not read: a throwaway sshd (a plain user process on a high loopback port, own host key, own temporary `AuthorizedKeysFile`) accepted this line and `tests/sshd_key_tests.sh` showed: `-L` to the permitted port carries data; `-L` to another port is refused (`administratively prohibited`); `-R` to any TCP port is refused; a shell command runs nothing and prints nothing (exit 1 from /bin/false); `-L` to a remote unix socket gets no answer. **A remote unix-socket listener (`ssh -R /path/sock:...`) is not stopped by the key line**: `permitlisten` covers TCP listeners only, and the same attempt succeeds. To stop it, set the server-side options below. The test also runs the control for each refusal (the same attempt with an unrestricted key succeeds), so a refusal is not a broken test.

   **Optional server-side hardening** for the dedicated account (the owner's call; security is not the main concern here, and the key line is enough for the stated purpose of forwarding to one port). In the VM's `sshd_config`, after all global settings:
   ```
   Match User <VM_USER>
     AllowTcpForwarding local
     AllowStreamLocalForwarding no
     GatewayPorts no
     X11Forwarding no
     PermitTTY no
   ```
   `AllowTcpForwarding local` refuses every `-R` even without `permitlisten`; `AllowStreamLocalForwarding no` refuses unix-socket forwarding in both directions. `tests/sshd_key_tests.sh` ran this block on the throwaway sshd: the permitted `-L` still works, `-R` over TCP and over a unix socket are refused, and no socket file is created.
3. **Pin the VM's host key.** Build `known_hosts` on the VM, from the VM's own public host key, not by scanning from the cluster (a scan trusts whatever answers):
   ```
   printf '%s %s\n' "$VM_ADDR" "$(cut -d' ' -f1,2 /etc/ssh/ssh_host_ed25519_key.pub)" > known_hosts
   ssh-keygen -lf known_hosts        # compare with: ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub
   ```
   Copy it to each cluster as `KNOWN_HOSTS_CLUSTER`. The first field must be exactly the `VM_ADDR` string the job uses. The job runs ssh with `StrictHostKeyChecking=yes`, only this file, and `-F /dev/null`, so no user or system ssh config can add a key, a proxy or a host alias. A mismatch ends the forward with `Host key verification failed` and is a stop, not something to click through.
4. **The key is handled by path only.** The job checks that the file is mode 600 or 400, passes `-i <path>` to ssh, and never reads, prints or logs its contents. Do not paste it, commit it, or put it on a command line.

## Run order

Work from a copy of this directory. Runtime files (token, configs, logs, pids) go in a private directory outside the repo. The order is: **A** on this VM, then **B** step 0 on each platform (three short jobs: `helios-cpu`, `helios-gh200`, `athena`), then **C** the three full runs, one at a time.

**A. On this VM, once**

1. `bash fetch-frp.sh` (both architectures; the output must end with `both architectures verified`).
2. Create a throwaway token, mode 600, and copy it to each cluster, mode 600: `umask 077; head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$TOKEN_FILE_VM"`.
3. Stage the kit with `dl/` on a filesystem the compute nodes see, for each cluster (compute nodes may have no internet): `rsync -a spikes/s1-frp/ <cluster>:"$KIT_DIR_CLUSTER"/`.
4. `source "$S1_SETTINGS"` and `RUN=$(mktemp -d)`. Runtime files live in `$RUN`; the raw logs and the ssh stderr stay private (see step 6 of C).

**B. Step 0, the reachability probe, on each platform (before any frp step).** Each job runs on a real compute node, takes about a minute and writes one JSON line per probe to `$SCRATCH/s1-frp/<jobid>-r0/probe.jsonl`.

The probe needs the far end of the forward to answer, not just to authenticate: an ssh forward opens its local listener as soon as the key authenticates, which proves the key and the host key and nothing about port 7000. So, **before submitting the probe jobs**, start the responder on this VM, on the port the key may forward to, and stop it before frps starts (frps needs the same port):
```
python3 probe_responder.py --port "$FRPS_PORT" --max-seconds 7200 2> "$RUN/responder.err" & echo $! > "$RUN/responder.pid"
```
It binds 127.0.0.1 only and answers every connection with one fixed line (`S1-PROBE-TOKEN-OK`) and closes. A `PROBE_ONLY=1` job counts the forward as working only if it reads exactly that line through it. A listener that accepts and says nothing, one that sends other bytes, a closed port and a port the key may not forward to all fail it. After the last probe job, stop it by the recorded PID: `kill "$(cat $RUN/responder.pid)"`.

| `probe` | What it does | Expected | Why it is there |
|---|---|---|---|
| `env` | Records the node's architecture, kernel, host `python3` version, and the ssh and curl versions. | n/a | Python 3.6 compatibility and the arch check. |
| `tcp_vm_22` | TCP connect to `VM_ADDR:22` (6 s limit), reads the banner's first 7 bytes. `ok` needs an `SSH-` banner, so a middlebox that accepts a connection and passes nothing on does not count. | ok, `SSH-2.0` | Option B rests on it. |
| `tcp_vm_frps` | TCP connect to `VM_ADDR:FRPS_PORT` (7000). | fail (timed out from the login nodes) | Records whether option A (an open port) would have worked from a compute node. Informational. |
| `https_github`, `https_pypi` | `curl` to `https://github.com/` and `https://pypi.org/simple/` on 443. "Reached" means curl got an HTTP status, any status. The pypi request asks for one byte (`--range 0-0`), because its index is about 48 MB. | an HTTP status | **HBEP-6 needs to know whether compute nodes reach the internet.** A production front may not sit behind this VM's NAT, and if nodes reach other hosts on 443 a front could be dialled there. `proxy_env` says whether a proxy variable is set (never its value), which would change what a 443 answer means. Informational. |
| `forward_setup` | A real `ssh -N -L` with the job's key, then one connection to the local end and one line read from it (3 s). `far_end` is `token` (the responder's line: the only pass in a `PROBE_ONLY=1` job), `held` (accepted, silent), `closed` (the far end closed it: nothing listens, or the key may not forward to that port), `data` (some other bytes). In a full run frps is the far end and does not speak first, so `held` is the expected answer, recorded as `ok: null` (unknown): the verdict on the path is the `frpc_login` line below. The ssh process is stopped by its recorded PID and start time afterwards. | `ok: true`, `far_end: token` | This is the transport. In a `PROBE_ONLY=1` job it decides the exit code. |
| `summary` | One line with the five results above and `transport_path_ok` (`true` only after the token exchange; `null` in a full run until frpc's login). | n/a | Fills the results table. |
| `frpc_login` | Full runs only. `ok: true` when frpc's own log shows `login to server success` within `FRPC_LOGIN_WAIT_S` (90 s), which means a Login reached frps through the forward and the plugin accepted it. | ok | The gate for a full run. No login in time ends the job with exit 3 and cleans up. |
| `watcher_attach` | Full runs only. `ok: true` when a request of `stream_watch.py` reached the fake engine through the tunnel within `WATCHER_WAIT_S` (900 s, 15 min) of the login. | ok | A paid allocation without a watcher is waste. No watcher in time ends the job with exit 5 and cleans up. |

1. Render the job. The values for `PLATFORM`, `PARTITION` and the account come from the table above:
   ```
   python3 render_tmpl.py job.sbatch.tmpl PLATFORM=helios-cpu ACCOUNT="$ACCOUNT_HELIOS_CPU" PARTITION=plgrid \
     TIME=01:20:00 MEM="$MEM" VM_ADDR="$VM_ADDR" FRPS_PORT="$FRPS_PORT" VM_USER="$VM_USER" \
     SSH_KEY="$SSH_KEY_CLUSTER" SSH_KNOWN_HOSTS="$KNOWN_HOSTS_CLUSTER" LPORT="$LPORT" \
     PROBE_TLS_PORTS=none TOKEN_FILE="$TOKEN_FILE_CLUSTER" PROTOCOL=tcp \
     INSTANCE_PREFIX=s1-helios-cpu KIT_DIR="$KIT_DIR_CLUSTER" PYTHON=python3 > job-helios-cpu.sbatch
   ```
   `TRANSPORT` is not a placeholder: it is an environment variable read at submit time, default `ssh-forward`. So are the limits `OUTAGE_DEADLINE_S` (300), `FWD_STARTUP_WAIT_S` (20), `FRPC_LOGIN_WAIT_S` (90), `WATCHER_WAIT_S` (900), `ENGINE_DOWN_S` (20), `FREEZE_SECONDS` (60) and `VM_SSH_PORT` (22); the defaults are placeholders for the owner to change. For `TRANSPORT=direct` (only if the owner opens a port) the four ssh values are passed as `none` and the job dials `VM_ADDR:FRPS_PORT` itself.
2. Submit the probe from a scratch directory on the cluster, with the responder running on the VM. It needs no token and no frp binary, only `ssh`, the key and the known_hosts file: `PROBE_ONLY=1 sbatch --time=00:10:00 job-helios-cpu.sbatch`. On Athena also pass `--partition=plgrid-now` (1 h limit, one job and one GPU per user). The job script has `#SBATCH --no-requeue`.
3. Read `$SCRATCH/s1-frp/<jobid>-r0/probe.jsonl` and `export/job.log`. The probe lines hold no address, node name or user (the VM is `vm`, and ssh's messages are scrubbed). When `forward_setup` fails, its `stage` says where: `precondition` (key file, mode, known_hosts, or a listener already on `LPORT`), `ssh_exited` (ssh's last stderr line is in `detail`: `Permission denied (publickey)` is an unauthorised key or wrong user, `Host key verification failed` a known_hosts mismatch, `Connection timed out` port 22 not reachable), `no_listener_in_15s`, or `ok` with `far_end` other than `token` (the far end is not the responder: closed, silent, other bytes).
4. **Exit codes.** 0: the transport path works (a `PROBE_ONLY=1` job: the token came back through the forward; a full run: it ran until TERM or the time limit). 3: it does not (step 0, or frpc's login did not reach frps in time), and the spike records "not reachable" for that platform and stops. 4: the ssh forward failed for good (authentication, host key or forwarding error, or no established forward for `OUTAGE_DEADLINE_S`) and the job cleaned up. 5: no watcher attached in time. 1: the job is misconfigured, or this is a restart of the allocation (refused). Ports 7000 and 443 never decide the exit code by themselves. Opening a firewall port, or any other access change, is the owner's decision.

**C. Full run for each platform that passed step 0** (one at a time: Helios CPU, Helios GH200, Athena)

1. On this VM render and verify frps. It listens on this VM's **loopback** only, on `FRPS_PORT`, the one port the key may forward to. The responder must be stopped by now:
   ```
   python3 render_tmpl.py frps.toml.tmpl BIND_ADDR=127.0.0.1 BIND_PORT="$FRPS_PORT" \
     TCPMUX_PORT="$TCPMUX_PORT" PROXY_BIND_ADDR=127.0.0.1 TOKEN_FILE="$TOKEN_FILE_VM" \
     PLUGIN_ADDR="127.0.0.1:$PLUGIN_PORT" LOG_FILE="$RUN/frps.log" > "$RUN/frps.toml"
   FRPS=$(echo dl/linux_amd64/frp_*/frps); "$FRPS" verify -c "$RUN/frps.toml"
   ```
   Start the stub, then frps, and record their PIDs (stop them later by these PIDs only):
   ```
   python3 plugin_stub.py --listen "127.0.0.1:$PLUGIN_PORT" --log "$RUN/stub.jsonl" >/dev/null 2>"$RUN/stub.err" & echo $! > "$RUN/stub.pid"
   "$FRPS" -c "$RUN/frps.toml" > "$RUN/frps.out" 2>&1 & echo $! > "$RUN/frps.pid"
   ```
2. **Start the watcher before submitting the job.** The instance name is `INSTANCE_PREFIX` itself (no job id), so it is known before `sbatch` runs: use one prefix per run (`s1-helios-cpu`, `s1-helios-gh200`, `s1-athena`) and never have two jobs with the same prefix pending or running. `--wait-up` makes the run clock start at the first good probe, so queue time does not matter. The job gives the watcher `WATCHER_WAIT_S` (15 min) to show up after frpc's login and ends itself with exit 5 if none does.
   ```
   python3 stream_watch.py --tcpmux "127.0.0.1:$TCPMUX_PORT" --domain <INSTANCE_PREFIX> \
     --wait-up 7200 --duration 4260 --stream-seconds 3660 --stub-log "$RUN/stub.jsonl" \
     --out "$RUN/summary-<run>.json" > "$RUN/watch-<run>.out" 2>&1 & echo $! > "$RUN/watch.pid"
   ```
   Then submit from the cluster scratch directory: `sbatch job-<run>.sbatch`, and note the job id. **If the watcher expires while the job is still pending** (its summary says `never_up`, exit 3), **the operator cancels the job** (`scancel <jobid>`): a job that starts later has nobody attached and would burn an allocation unobserved. A job that starts and sees no watcher request within 15 min of its login cancels itself, but a pending job is the operator's to cancel. `watch-<run>.out` prints the moment the run clock starts. Call it T+0.
3. T+0 to T+61 min: touch nothing. One stream of 3660 s must run uncut.
4. Fault tests, each with a mark first. **Before each mark, check that the path is up**: `grep 'probe: path' "$RUN/watch-<run>.out" | tail -n 1` must not be a `path DOWN` line, and after a fault the next one waits for `probe: path UP again`. On this VM `kill -USR1 "$(cat $RUN/watch.pid)"`, then at once on the cluster `scancel --batch --signal=<SIG> <jobid>`. A mark taken while the probe is not `up` is an invalid fault measurement: the summary records it as `valid: false` with the reason (`invalid_reason`), `verdict.py` fails its criterion, and it is never counted as a detection. Wait out each one before the next:

   | At | Signal | Fault | What to expect |
   |---|---|---|---|
   | T+62 min | `HUP` | frpc stopped for 60 s, socket open, no FIN (silent loss) | probe down, `CloseProxy` within the silent-loss budget, then a new `Login` after the thaw, then `path UP again` |
   | T+65 min | `USR2` | fake engine killed, frpc alive; **the job restarts the engine on its original port after 20 s** (`ENGINE_DOWN_S`) | probe fails as `no_response`, `Ping` lines keep coming, no `CloseProxy`, then `path UP again` once the engine is back |
   | T+68 min | `USR1` | frpc killed (only after `path UP again` from the engine fault) | `CloseProxy` within seconds, probe `path_down` |

   The engine is restarted so that the frpc fault starts from an up path. Without the restart the path is down at T+68, the watcher has nothing new to declare, and the measurement is empty.
5. At T+71 min the watcher exits and writes the summary. `scancel <jobid>` stops the job cleanly (TERM: the job stops frpc, the engine, then the ssh loop and its ssh, each by PID and start time, and writes `export/`).
6. **Collect only `export/`.** `$SCRATCH/s1-frp/<jobid>-r0/export/` holds redacted copies of `job.log`, `probe.jsonl`, `ssh-exits.jsonl`, `ssh-counters.json`, `frpc.log` and `direct-baseline.json`: no IPv4 or IPv6 address (loopback excepted), no VM account or local account, no key, known_hosts, token or scratch path, no VM address, node name or job id. **The raw files must never be copied out of the job directory**: `job.log` (it holds the node name and paths), `ssh-raw.err` and `ssh-cur.err` (ssh's own stderr, which names the VM address, the account and the key paths), `ssh-probe.err`, `.redact.sed`, `frpc.stdout`, `frpc.toml`, `streamer.log`, `pids`. From the VM, collect the summary and `$RUN/stub.jsonl`.
   - `ssh-exits.jsonl` has one line per attempt of the ssh forward with only these fields: UTC time, attempt number, exit code, class (`auth`, `hostkey`, `forwarding` or `transient`), whether the forward was established, seconds it lived, restart delay. `ssh-counters.json` keeps four counts apart: **attempts** (every start of ssh), **established** (sessions whose forward came up), **disconnects** (established sessions that ended on their own) and **recoveries** (sessions established after a disconnect). An attempt that fails to authenticate is an attempt and nothing else; it is not a reconnect.
   - Run the verdict on this VM: `python3 verdict.py --summary "$RUN/summary-<run>.json" --job-log export/job.log --probe export/probe.jsonl --ssh-counters export/ssh-counters.json`. It prints PASS or FAIL per criterion and `OVERALL: PASS` or `OVERALL: FAIL`.
7. After the last run, stop frps and the stub by the recorded PIDs: `kill "$(cat $RUN/frps.pid)" "$(cat $RUN/stub.pid)"`.

**D. Cleanup, when the spike ends** (the owner's step, as creating the key was). Leave nothing running and nothing authorised:

1. On the VM, remove every `authorized_keys` line that carries the comment `s1-spike`, and check that none is left: `grep -c ' s1-spike$' <authorized_keys file>` must print `0`.
2. On each cluster, delete the private key (`SSH_KEY_CLUSTER`) and its `.pub`, the known_hosts file and the token file.
3. On the VM, stop the probe responder if it is still running (`kill "$(cat $RUN/responder.pid)"`), delete the token file and the run directory once the logs are collected.
4. Say on issue #18 that the key has been removed.

Before anything goes into the PR, read the files of `export/` once more for addresses, host names, node names, user and account names and job ids. The job redacts them at exit, but a pattern list is not a proof; the raw files are never an input to the PR.

## Heartbeat budget (fixed before any run)

Settings, the same on both sides: yamux keepalive every **10 s** (`tcpMuxKeepaliveInterval`), application heartbeat every **10 s** (`heartbeatInterval`), session dropped after **25 s** without a heartbeat (`heartbeatTimeout`), TCP keepalive 30 s.

Reasoning, from the v0.71.1 source:

- With `tcpMux = true` (the default) frp turns the application heartbeat off unless it is set (`pkg/config/v1/client.go` and `server.go` default it to -1). Then `frpc` never sends a Ping and the `Ping` plugin op never fires. We set it, because the Ping line is the instance's liveness feed: it keeps arriving when only the engine has died.
- Silent loss, no FIN (frozen process, dead network): the yamux keepalive pings every 10 s and gives up after yamux's fixed 10 s write timeout, so the session ends within 10 + 10 = **20 s** *as the expected case*, when the ping is written promptly. It is not an upper bound: time spent sending the ping is not part of the separate 10 s response timeout (yamux v0.2.0, `session.go`), so a send that itself stalls adds to it. The application heartbeat is the backstop: frps drops a control session after 25 s without a Ping (checked every second), so at most **26 s** after the fault. In heartbeat terms: two missed pings tolerated, the third ends the session. The 30 s budget below is the number the run is judged against; what the plugin actually received is what `stub_close_proxy_s` measures.
- Process death (kill, crash, exit): the kernel closes the socket, frps sees EOF and closes the control session, which fires `CloseProxy` at once.
- The `CloseProxy` call runs when frps closes a client's control session, not only when frpc asks (`server/control.go`, `closeProxy` for every proxy of a closed control).

Budget:

| Fault | Signal | Budget | Expected |
|---|---|---|---|
| frpc killed | `CloseProxy` at the stub | 5 s | under 2 s |
| frpc killed | `/health` probe declares down (2 failures in a row, 2 s apart) | 10 s | about 4 s |
| engine killed, frpc alive | `/health` probe declares down, class `no_response` | 10 s | about 4 s |
| engine killed, frpc alive | the path stays up: `Ping` keeps arriving, no `CloseProxy` | yes or no | yes |
| frpc frozen (silent loss) | `CloseProxy` at the stub | **30 s** | 10 to 20 s (the expected case, not a bound) |
| frpc frozen | `/health` probe declares down | 10 s | about 8 s |
| frpc thawed after the freeze | new `Login` and `NewProxy`, probe up again | recorded, no budget | a few seconds after the thaw |

Notes for reading the numbers:

- **Option B adds a second liveness layer.** frpc's TCP connection ends at a local ssh process, and ssh carries it to sshd, which opens frps' loopback port. The three injected faults (frpc frozen, engine killed, frpc killed) do not touch ssh, so the rows above hold unchanged. A real network loss between the node and the VM is a different case: frp's heartbeat (25 s), yamux (20 s) and ssh's own keepalive (10 s, three misses, so 30 s) all run, and frpc can reconnect through the forward only after the loop has restarted ssh, which adds the reconnect delay (2 s or more). An `ssh-forward: attempt N exited` line in the exported `job.log` (or a line of `ssh-exits.jsonl`) with no injected fault before it is a finding. frps sees every instance as coming from 127.0.0.1, because sshd opens the connection.

- Marks are taken on this VM before the cluster command is sent, so every measured time includes the command's own latency. They overstate detection time, which is the safe side. All times come from this VM's clock (the watcher's marks and the stub's timestamps), so clock skew with the compute node does not enter. `clock_offset_plus_one_way_s` in the summary is an estimate, for information only.
- Killing a process measures the FIN path. Only the freeze exercises the heartbeat budget, so the two are reported separately.
- A frozen frpc shows as `no_response` at the probe, the same as a dead engine. The `Ping` and `CloseProxy` lines from the stub tell them apart: Pings continuing means the agent is alive.
- A session lost for no injected reason during the hour (a `Login` after the first one with no mark before it) is a finding: yamux gives up after 10 s of silence, which a starved compute node might cause.

## Pass criteria (issue #18)

`python3 verdict.py ...` (see C6 of the run order) prints PASS or FAIL for each of the criteria below (its ids C1 to C5) and `OVERALL`. `stream_watch.py` exits 0 whenever it collected a summary, even with cuts or an undetected fault; the exit status is not a verdict.

For each platform that passes step 0:

1. (C1) frpc starts inside a Slurm job on a compute node, as a plain user, and registers through the plugin endpoint. Evidence: `export/job.log` (`frpc: login to server success`) and a `Login` plus `NewProxy` line in `stub.jsonl`.
2. (C2) A stream behind frpc stays reachable through frps for at least 1 h. Evidence: `streams[0]` has `completed: true`, `requested_s` of 3660 and `duration_s` of at least 3600.
3. (C3) No stream is cut over that hour. Evidence: `cuts` is empty before the first mark, `gaps.over_threshold_unfaulted` is 0, and no entry of `stream_open_failures` or `health.failures_sample` has a time before the first mark.
4. (C4a, C4b, C4c: the frozen frpc, the killed engine, the killed frpc, in the order of the marks) Killing frpc, or the engine, marks the path down within the budget above. Evidence: `marks[]` and the stub lines. A mark taken while the path was already down or failing is `valid: false`: the criterion fails and a rerun from an up path is needed; it is never a detection.
5. (C5, with the informational counts) Recorded for the PR: outbound protocol (frp tcp inside an ssh forward), attempts, established sessions, disconnects and recoveries of the ssh forward (counted apart in `ssh-counters.json`), reconnects of frp, added latency, centre-side blocking, and the step-0 reach of the VM's ports 22 and 7000 and of 443 (table below). C5 passes when frpc's login through the transport is recorded and the job did not log a permanent transport failure.

Verifications that prove nothing, so none of them goes into the results:

- A same-host run, or a run from a login node: neither shows that a compute node can reach out. This includes `local-smoke.sh`.
- A stream shorter than about 30 min: the cuts seen before came from deadlines of 180 s, 900 s and 1,800 s.
- A process kill as the test of the heartbeat budget: it measures the FIN path. Use the freeze.
- A `Ping` line as proof that the engine is up: it shows the agent is alive. The probe through the path shows the engine.

## Results tables (to fill in)

**Step 0, one row per platform** (from `probe.jsonl`; the `summary` line holds the five results):

| Platform | VM:22 (banner) | VM:7000 (expected to fail; timeout or refused) | 443 (github, pypi, `proxy_env`) | Forward setup (ok, ms, `far_end` and `proof`, or `stage` and reason) | Node arch, kernel, host `python3` |
|---|---|---|---|---|---|
| Helios CPU | | | | | |
| Helios GH200 | | | | | |
| Athena | | | | | |

**The runs, one column per run:**

| | Helios CPU | Helios GH200 | Athena |
|---|---|---|---|
| Outbound protocol used (frp tcp inside the ssh forward, or direct) | | | |
| ssh forward during the hour: attempts, established, disconnects, recoveries (`ssh-counters.json`) | | | |
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
- **The ssh forward, against a real sshd, but not the VM's and not from a compute node.** The job's real ssh success path ran locally against a throwaway sshd (this user, not root, a high loopback port, its own host key, its own temporary `AuthorizedKeysFile` holding the README's key line, nothing under `~/.ssh`): step 0 with the responder (token through the forward, exit 0), step 0 with a closed, a silent and a wrong-token far end and with a port the key may not forward to (all exit 3), a full run through the forward with the real frps, frpc, stub and watcher (`tests/e2e_job.sh`), a revoked key mid-run (real `Permission denied (publickey)`, exit 4) and a changed host key mid-run (real `Host key verification failed`, exit 4). What this does not show: that the VM's own sshd, its `sshd_config` and the Cyfronet network treat the forward the same way, that a compute node can reach port 22 at all (that is what step 0 is for), and anything about the first real job. The ssh double (`tests/fake_ssh.py`) stands in only where a case cannot be made to happen on demand with a real sshd (a forwarding failure, an outage, a hang), and its stderr phrases are the ones OpenSSH prints.
- **The kit's own tests are not the spike.** `tests/run-all.sh` passing says the kit does what the review asked on this machine, with a fake engine, a fake frpc where a test needs a job that logs in on demand, and loopback. It says nothing about a compute node.
- **Compute-node tooling.** The probes assume bash 4.4 or newer, GNU `date` (for `%N`), `timeout`, `curl` and `ssh` on the node. Missing `curl` is recorded as `ok: null` (unknown), a missing `ssh` as a failed `forward_setup`. Neither was seen on a real node.
- A reading of one run on one machine is not a statistic: the budget numbers above are targets to confirm, not measurements.

## Provenance of what is reused

| What | Origin | Licence |
|---|---|---|
| `frps`, `frpc`, tcpmux, server plugins, token auth, yamux multiplexing | frp v0.71.1, https://github.com/fatedier/frp | Apache-2.0 |
| The keepalive timing used in the budget (10 s write timeout) | `fatedier/yamux` v0.2.0, a fork of `hashicorp/yamux`, built into frp | MPL-2.0 |
| The ssh forward, its restrictions (`restrict`, `port-forwarding`, `permitopen`, `permitlisten`, `command`; optional server-side `AllowTcpForwarding local`, `AllowStreamLocalForwarding no`), host-key pinning (`StrictHostKeyChecking`, `UserKnownHostsFile`) and the keepalives | The OpenSSH project (the ssh client and sshd already on every node and on the VM) | BSD and ISC style |
| The 443 reach probe | `curl`, as installed on the node | curl licence (MIT style) |
| The stub, streamer, watcher, renderer, job template (probes, reconnect loop, redaction, process records included), smoke test, probe responder, verdict, tests (including the throwaway-sshd helper and the ssh double) | new, Python standard library and bash | this repo |
| The long-stream cut deadlines the watcher guards against (180 s, 900 s, 1,800 s) and the HTTP vhost 60 s trap | www-2027 lessons, as quoted in issue #18 | n/a |

Claims about frp's defaults and behaviour come from the v0.71.1 sources: `doc/server_plugin.md`, `pkg/config/v1/{client,server}.go`, `server/control.go`, `server/service.go`, `pkg/util/vhost/vhost.go` and `pkg/util/tcpmux/httpconnect.go`.
