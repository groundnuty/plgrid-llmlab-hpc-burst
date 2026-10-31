# Spike S1: frp dial-home kit

Kit for spike S1 (issue #18): does frp work as the dial-home tunnel from compute nodes, for long streaming responses? It carries no run results. **Only step 0 (the reachability probe) has run on a cluster so far, in `TRANSPORT=direct` mode with no ssh forward, on the kit as committed at 72bfccd. No full run has.** Step 0's compute-node facts are in `profiles/`. The 1-hour runs wait for the owner to authorise the dial-home keys on the VM ("Key and host-key handling").

**Step 0's "VM:22 reached, SSH-2.0 banner" of 30 Oct was a false positive (lesson L-17.7, "check the host key, not just the banner").** On 31 Oct the code agent scanned the address the clusters dial: its port 22 presents another machine's ed25519 host key and an older OpenSSH, so it is a gateway, not this VM. The owner is deciding how compute nodes reach the VM's sshd (possibly a port forwarded on the gateway; hence `VM_SSH_PORT`). The kit now checks the host key itself before any forward starts.

**Wiring: the alpha wiring of HBEP-6 (being drafted), decided by the owner on 31 Oct 2026 (relayed by `@hpc-burst-science-agent`).** The agents VM, where the control plane runs, lets only TCP port 22 in from the Cyfronet side. In ssh mode the job opens **two separate** `ssh -N -L` connections over port 22, each in its own reconnect loop, with one restricted key per cluster that permits exactly these two forwards:

```
compute node                                                   agents VM (everything on 127.0.0.1)
control:  agent stand-in client -> 127.0.0.1:LPORT_CTL ==ssh 1==> 127.0.0.1:AGENT_PORT     (agent endpoint, HTTPS)
tunnel:   frpc (wss, verified)  -> 127.0.0.1:LPORT_TUN ==ssh 2==> 127.0.0.1:DIALHOME_PORT  (Envoy, TLS)
                                                                    |- GET /~!frp (websocket) -> frps (FRPS_PORT)
                                                                    |- /s1/ (path rate only)  -> sink (SINK_PORT)
front side:  stream_watch.py -> frps tcpmux (TCPMUX_PORT) "CONNECT <Instance UID>:80" -> frpc -> fake engine
```

- **Ports.** `AGENT_PORT` = 29223 (the launcher's port base + 3, `agentapi.AgentEndpointPortOffset`) and `DIALHOME_PORT` = 29227 (base + 7, `agentapi.DialHomePortOffset`, PR #79). Both are variables in every file here; the key line fixes them, so a launcher with another port base needs new keys.
- **TLS.** frps cannot serve wss itself (spike S1c), so Envoy 1.33.2 terminates TLS on the dial-home listener and passes `GET /~!frp` upgrades to frps on another loopback port, as S1c did. The agent endpoint and the dial-home listener present one launcher-style certificate: a self-signed test CA signs a server certificate for `TLS_SERVER_NAME` (`dial-home.hpcburst.invalid`). Every client in the job verifies it: frpc with `transport.tls.trustedCaFile` and `transport.tls.serverName`, the probes and the path-rate client with the same CA and name.
- **Routing.** tcpmux `httpconnect`, with the custom domain set to the Instance UID. The kit uses a fixed test UID (`INSTANCE_UID`, `5e1f0000-0000-4000-8000-000000000001`); the watcher's `--domain` takes it.
- **Heartbeats.** Unchanged from S1: yamux keepalive 10 s, application heartbeat 10 s with a 25 s timeout, TCP keepalive 30 s ("Heartbeat budget").
- **Path rate.** Before frpc starts, the job measures the sustained rate through each forward, mainly node to VM (the token direction). `verdict.py` reports the observed head-of-line scale 6 MiB / rate and the heartbeat timeout budget ("Path rate").
- **Host key.** In ssh mode, before any forward starts (in step 0, and again right before the full run's forwards), the job checks that the sshd at `VM_ADDR:VM_SSH_PORT` presents the VM's own ed25519 host key: `ssh-keyscan -t ed25519 -p VM_SSH_PORT` piped into `ssh-keygen -lf -`, compared with `VM_HOSTKEY_FP`, which comes from the VM's `/etc/ssh/ssh_host_ed25519_key.pub` through the settings, never from a scan on the cluster. The pinned `known_hosts` must hold the same key under the name ssh looks up (`[VM_ADDR]:VM_SSH_PORT` when the port is not 22). A mismatch exits 6, no answer exits 7; the messages name neither the address nor a key.
- `TRANSPORT=direct` (no ssh) stays: the job dials `VM_ADDR:AGENT_PORT` and `VM_ADDR:DIALHOME_PORT` itself, and frpc's wss goes straight to the dial-home listener. It needs those ports open to the nodes, which they are not on this VM.

Pinned releases: **frp v0.71.1** (Apache-2.0, https://github.com/fatedier/frp) and **Envoy 1.33.2** (Apache-2.0, https://github.com/envoyproxy/envoy), the binaries S1c used.

## The local smoke test is a config check, not evidence

`local-smoke.sh` runs the whole wiring on this VM over 127.0.0.1: the VM side (Envoy with the test certificate, frps, the plugin stub, the agent stand-in, the sink), and the job template itself as a plain script. The watcher runs 120 s with the fault timeline. It has two variants. `direct`: the job dials the VM side's ports. `ssh`: the job's two forwards go through a throwaway sshd that the script starts as the current user (not root) on a loopback port, with its own host key and its own `AuthorizedKeysFile` holding the README's key line (ports swapped for the smoke's), in a temporary directory it deletes; it never reads or writes `~/.ssh`. Every listener binds 127.0.0.1 on a port in 29400–29499. If no sshd can run, the `ssh` variant prints `SKIPPED ... this is not a pass` and exits 0. The smoke checks registration, a stream through tcpmux with the UID domain, the path-rate measurement (capped, so the expected rate is the cap), fault detection, and negative controls (a wrong CA, a wrong server name and a wrong domain must all fail).

It does **not** show that a compute node can reach this VM, and 120 s says nothing about a stream that lasts an hour. Issue #18 rules out both readings. Its numbers (detection times and rates on loopback) are not results of S1.

`tests/run-all.sh` runs the kit's own tests (the job logic against an ssh double and against the throwaway sshd, the path-rate tool, the watcher's HTTP checks, the verdict, the key line, an end-to-end run). They test the kit, not the spike; see "Not verified here".

## Files

| File | What it is |
|---|---|
| `fetch-frp.sh` | Downloads frp v0.71.1 `linux_amd64` and `linux_arm64` into `dl/` (ignored by git) and checks each against the sha256 written in the script. `fetch-frp.sh verify <file> <arch>` checks one file. A mismatch deletes the download and extracts nothing. |
| `fetch-envoy.sh` | The same for Envoy 1.33.2 `linux-x86_64` (the VM side only), into `dl/envoy/`. `ENVOY_SRC=<file>` copies a local binary after checking it; `fetch-envoy.sh verify <file>` checks one. |
| `make-test-ca.sh` | `make-test-ca.sh DIR [SERVER_NAME]`: a throwaway CA and a server certificate for `SERVER_NAME` (default `dial-home.hpcburst.invalid`, plus IP 127.0.0.1), EC P-256, 30 days, keys mode 600. Refuses to overwrite a key. |
| `envoy.yaml.tmpl` | The dial-home listener: TLS on `127.0.0.1:DIALHOME_PORT`; `GET /~!frp` websocket upgrades to frps; the prefix `/s1/` to the sink (spike only). No admin listener. Route timeouts off (`timeout: 0s`, `idle_timeout: 0s`), stream idle timeout off. |
| `path_rate.py` | `serve`: the VM side's HTTP(S) stand-in (`GET /s1/probe` returns the probe token, `POST /s1/sink` counts a body and times it at the receiver, `GET /s1/source` streams for N seconds). It is the agent endpoint's stand-in (TLS, on `AGENT_PORT`) and the sink behind Envoy (plain, on `SINK_PORT`). `probe`: one verified TLS `GET /s1/probe`. `measure`: one bulk transfer, one JSON line. Stdlib only. |
| `render_tmpl.py` | Fills the double-underscore placeholders of a template. Fails on an unfilled placeholder or an unused key. Keeps `#@<platform>` lines only for that platform. |
| `frps.toml.tmpl` | Server config: token from a file, tcpmux on a loopback port, **no vhost**, liveness settings, server plugin for `Login`, `NewProxy`, `CloseProxy`, `Ping`. frps serves plain websocket on its bind port; Envoy is the only client that reaches it. |
| `frpc.toml.tmpl` | Instance config: `loginFailExit = false`, wss with verified TLS (CA file, server name), one tcpmux proxy whose domain is the Instance UID, liveness settings. |
| `plugin_stub.py` | Answers frps plugin calls: allows all, logs one JSON line per call (UTC time, op, user, proxy, run id). Never logs the token or its derived key. |
| `fake_streamer.py` | Fake engine on 127.0.0.1: `GET /stream?seconds=N` (a token per second, chunked), `GET /health` (the fixed body `{"status": "ok"}`). Serves requests concurrently, so several streams run at once and `/health` keeps answering. `--request-log` records each request, which is how the job sees the watcher attach. Python 3.6 compatible. |
| `stream_watch.py` | Runs on the frps host. Reaches the instance through tcpmux (HTTP CONNECT to the UID), records token gaps, cuts and reconnects, probes `/health`, takes marks for fault timing, writes a JSON summary. `--stub-user` names the frp user (`INSTANCE_PREFIX`) whose plugin calls it reads; it is required with `--stub-log`, because Login and Ping carry the user, not the domain. It accepts a `/health` reply only when it is complete and a stream only when its chunk framing and final blank line arrive. A mark taken while the probe is not `up` is recorded as `valid: false`. `--streams N` keeps N streams open at once (see "Four streams at once"). |
| `probe_responder.py` | A loopback listener that sends one fixed token line and closes. The job no longer uses it (the VM side's `path_rate.py serve` answers step 0); `tests/sshd_key_tests.sh` uses it as the far end of the key-line tests. |
| `verdict.py` | Reads the watcher summary and the job's exported logs and prints PASS or FAIL per criterion (C1 to C6), FLAG lines for path-rate risks, and `OVERALL`. `stream_watch.py` exiting 0 means it collected, not that the experiment passed. |
| `job.sbatch.tmpl` | One job template for all three runs: step 0 (probes, one JSON line each), the fake engine, the two forwards in reconnect loops (ssh mode), the path-rate measurement, frpc, a gate on frpc's login, a gate on the watcher, until TERM. `PROBE_ONLY=1` stops after step 0. `TRANSPORT=ssh` (default) or `direct`. The job refuses a restart and never reuses a run directory. |
| `local-smoke.sh` | The loopback config check above, variants `direct`, `ssh`, `all`. |
| `tests/` | The kit's own tests, the throwaway sshd and the ssh double they use (`run-all.sh`). |

frp pieces are reused as they are (`frps`, `frpc`, tcpmux, server plugins), and so is Envoy. The scripts here are new code on the Python standard library and bash. The kit needs no pip packages.

## What the owner's decisions fill in

| Decision | Where it lands | Rule |
|---|---|---|
| **The control plane runs on this VM**, reached through ssh over port 22 (31 Oct 2026). | `VM_ADDR` (the address a compute node uses), `VM_USER` | Address and user stay out of every committed file. The operator reads them from a private settings file and passes them to `render_tmpl.py`. The kit never opens a port. |
| **Two loopback forwards per key**, to the agent endpoint and the dial-home listener; outbound tunnels from compute nodes are acceptable use (31 Oct 2026). | `AGENT_PORT` (29223), `DIALHOME_PORT` (29227), `LPORT_CTL`, `LPORT_TUN` (node-local ends) | The two ports in the key line must equal `AGENT_PORT` and `DIALHOME_PORT`. |
| **One restricted VM key per cluster** (Helios, Athena), serving S1's runs and alpha (31 Oct 2026). | `SSH_KEY` (`<home>/.hpcburst/dial-home/key`), `SSH_KNOWN_HOSTS` (`<home>/.hpcburst/dial-home/known_hosts`) | See "Key and host-key handling". |
| **Grant per platform** (the account a job bills) | `ACCOUNT` | Placeholder in the committed files. GPU partitions need an account with the matching suffix. |

Settings file variables used below (names only; values stay private): `S1_SETTINGS`, `VM_ADDR`, `VM_SSH_PORT` (22, or the port the owner chooses, for example one forwarded by the gateway), `VM_HOSTKEY_FP` (the SHA256 fingerprint of the VM's own ed25519 host key: `ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub | awk '{print $2}'` on the VM), `VM_USER`, `AGENT_PORT` (29223), `DIALHOME_PORT` (29227), `FRPS_PORT`, `TCPMUX_PORT`, `PLUGIN_PORT` and `SINK_PORT` (loopback ports on this VM, outside the launcher's block), `SSH_KEY_CLUSTER` and `KNOWN_HOSTS_CLUSTER` (the absolute paths of `.hpcburst/dial-home/key` and `.hpcburst/dial-home/known_hosts` under the cluster home), `LPORT_CTL` and `LPORT_TUN` (two free ports on the node, 20000 to 60000), `CA_FILE_CLUSTER` (the test CA's certificate, staged on the cluster), `TLS_SERVER_NAME` (`dial-home.hpcburst.invalid`), `INSTANCE_UID` (`5e1f0000-0000-4000-8000-000000000001`), `TOKEN_FILE_VM`, `TOKEN_FILE_CLUSTER`, `KIT_DIR_CLUSTER`, `ENVOY_BIN`, `MEM`, `ACCOUNT_HELIOS_CPU`, `ACCOUNT_HELIOS_GH200`, `ACCOUNT_ATHENA_A100`. The job directory root `RUN_ROOT` (see B) is optional.

## The three runs

| Run | `PLATFORM` | Partition | Account | CPU arch | frpc | GPU |
|---|---|---|---|---|---|---|
| 1 | `helios-cpu` | `plgrid` | `ACCOUNT_HELIOS_CPU` | x86_64 | amd64 | none |
| 2 | `helios-gh200` | `plgrid-gpu-gh200` | `ACCOUNT_HELIOS_GH200` (needs the `-gpu-gh200` suffix) | aarch64 | arm64 | `--gres=gpu:1`, left idle |
| 3 | `athena` | `plgrid-gpu-a100` | `ACCOUNT_ATHENA_A100` (needs the `-gpu-a100` suffix) | x86_64 | amd64 | `--gres=gpu:1`, left idle |

- **Run 2 is the production path.** A GPU partition's outbound rules may differ from the CPU partition's, so run 1 does not stand in for it.
- **Why Athena and GH200 request a GPU.** Athena accepts GPU jobs only: its documentation warns that "extensive workloads that do not use GPUs lead to account suspension", and S5 found that general users submit to `plgrid-gpu-a100` (48 h). Every Athena job of this kit, step 0 included, uses `plgrid-gpu-a100`; `plgrid-now` (1 h, one job and one GPU per user) is not used, because the run's 1 h 25 min limit does not fit in it. frp does not use the GPU, so the job asks for exactly one and keeps the time short. The GH200 run asks for one GPU the same way, as the run plan requires.
- **The frpc binary is chosen by `uname -m` at run time.** The job refuses to start if the node's architecture does not match its `PLATFORM` (a wrong-partition result would be mislabelled).
- **Athena has no Apptainer on its login node.** The job runs frpc (a static binary) and the Python scripts directly on the node, with no container.
- **Python on the compute nodes.** The scripts need Python 3.6 or newer and are written to run on 3.6. Step 0 now needs Python too: its two path probes are verified TLS exchanges (`path_rate.py probe`). The job logs the interpreter it uses (`python:` line in `job.log`). If the configured `PYTHON` is missing or older than 3.6 it tries `python3.15` down to `python3.6`; without one, the TCP and 443 probes still run, both path probes fail and the job exits 3. On Helios only, `PYTHON` can be a wrapper such as `apptainer exec <image> python3`. Step 0 read the host `python3` on a compute node of each platform (Helios CPU, Helios GH200, Athena): 3.9.25 on all three.

## Key and host-key handling (the owner's step, before step 0)

The kit never creates a key, never edits `authorized_keys` and never touches `~/.ssh`. The owner does these steps; the job only reads the files named in its placeholders. The exact commands for each cluster, with the private values filled in, are in a private file outside the repository.

0. **Check that the address the clusters dial reaches the VM's own sshd, before installing anything.** On the VM, take the fingerprint of its own host key: `ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub` (this is `VM_HOSTKEY_FP`). From each cluster's login node, scan what answers there: `ssh-keyscan -t ed25519 -p "$VM_SSH_PORT" "$VM_ADDR" | ssh-keygen -lf -`. The two fingerprints must be equal. An SSH banner proves nothing: on 31 Oct port 22 of that address answered with a gateway's key (lesson L-17.7). If they differ, stop: the forwards would end on another machine. The scan only compares; the expected value always comes from the VM's own file.
1. **One dedicated key per cluster**, ed25519, no passphrase (jobs run unattended), generated on the cluster's login node so the private half never leaves the platform. Helios (CPU and GH200 runs) is one cluster and shares a key; Athena has its own. The key goes where alpha looks for it (HBEP-4, `agentapi.DialHomeDir`): `~/.hpcburst/dial-home/key` and `~/.hpcburst/dial-home/known_hosts`, directory mode 700, files mode 600, on the home filesystem (not purged, readable from compute nodes). The job takes the resolved absolute paths, never `~`. The comment names the cluster: `ssh-keygen -t ed25519 -N '' -C hpcburst-dialhome-<cluster> -f ~/.hpcburst/dial-home/key`.
2. **Authorise each public key on the VM** for the VM account `VM_USER`, one line each, in that account's `authorized_keys`:
   ```
   restrict,port-forwarding,permitopen="127.0.0.1:29223",permitopen="127.0.0.1:29227",permitlisten="127.0.0.1:1",command="/bin/false" ssh-ed25519 <public key> hpcburst-dialhome-<cluster>
   ```
   `restrict` switches every extra off (shell, pty, agent and X11 forwarding, user rc files); `port-forwarding` switches forwarding back on, both directions; the two `permitopen` options allow exactly two `-L` destinations, the agent endpoint (`AGENT_PORT`) and the dial-home listener (`DIALHOME_PORT`); `permitlisten` limits `-R` listeners (permitopen does not touch them) to 127.0.0.1 port 1, a privileged port that a non-root account cannot bind while `net.ipv4.ip_unprivileged_port_start` is 1024 (the default; check `sysctl net.ipv4.ip_unprivileged_port_start` on the VM; at 0 the key could open that one loopback port, which the optional server-side setting below closes), so no usable `-R` listener can open; `command="/bin/false"` leaves no shell even if a client asks for one. `permitlisten` needs OpenSSH 7.8 or newer; this VM runs OpenSSH 9.6p1. No `from=` list is given because the compute-node source addresses are not known. The owner may add one if the Cyfronet ranges are known. **A launcher started with another port base needs new keys**, because the two ports are fixed here.

   What the line does and does not stop was run, not read: a throwaway sshd (a plain user process on a loopback port in 29400–29499, own host key, own temporary `AuthorizedKeysFile`) accepted this line, with its two ports swapped for test ports, and `tests/sshd_key_tests.sh` showed: `-L` to each of the two permitted ports carries data; `-L` to a third port is refused (`administratively prohibited`); `-R` to any TCP port is refused; a shell command runs nothing and prints nothing (exit 1 from /bin/false); `-L` to a remote unix socket gets no answer. **A remote unix-socket listener (`ssh -R /path/sock:...`) is not stopped by the key line**: `permitlisten` covers TCP listeners only, and the same attempt succeeds. To stop it, set the server-side options below. The test also runs the control for each refusal (the same attempt with an unrestricted key succeeds), so a refusal is not a broken test.

   **Optional server-side hardening** for the dedicated account (the owner's call; security is not the main concern here, and the key line is enough for the stated purpose of forwarding to two ports). In the VM's `sshd_config`, after all global settings:
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
   When `VM_SSH_PORT` is not 22, ssh looks the host up as `[VM_ADDR]:VM_SSH_PORT`, so write that form instead: `printf '[%s]:%s %s\n' "$VM_ADDR" "$VM_SSH_PORT" "$(cut -d' ' -f1,2 /etc/ssh/ssh_host_ed25519_key.pub)" > known_hosts`. Copy it to each cluster as `~/.hpcburst/dial-home/known_hosts`, mode 600. The first field must be exactly the `VM_ADDR` string the job uses (in brackets with the port when the port is not 22). The job's host-key check refuses a `known_hosts` that lacks this entry or pins another key (exit 6). The job runs ssh with `StrictHostKeyChecking=yes`, only this file, and `-F /dev/null`, so no user or system ssh config can add a key, a proxy or a host alias. A mismatch ends the forward with `Host key verification failed` and is a stop, not something to click through.
4. **The key is handled by path only.** The job checks that the file is mode 600 or 400, passes `-i <path>` to ssh, and never reads, prints or logs its contents. Do not paste it, commit it, or put it on a command line.

## Run order

Work from a copy of this directory. Runtime files (token, CA, configs, logs, pids) go in a private directory outside the repo. The order is: **A** on this VM (the VM side keeps running through B and C), then **B** step 0 on each platform (three short jobs: `helios-cpu`, `helios-gh200`, `athena`), then **C** the three full runs, one at a time.

**A. On this VM, once**

1. `bash fetch-frp.sh` (both architectures; the output must end with `both architectures verified`), and `bash fetch-envoy.sh verify "$ENVOY_BIN"` for an Envoy binary already on the VM, or `bash fetch-envoy.sh` to download one (then `ENVOY_BIN=dl/envoy/envoy-1.33.2`).
2. `source "$S1_SETTINGS"` and `RUN=$(mktemp -d)`. Runtime files live in `$RUN`; the raw logs and the ssh stderr stay private (see step 6 of C). `VM_HOSTKEY_FP` in the settings must be the VM's own: check it with `[ "$VM_HOSTKEY_FP" = "$(ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub | awk '{print $2}')" ] && echo same`.
3. Create a throwaway token, mode 600, and copy it to each cluster, mode 600: `umask 077; head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$TOKEN_FILE_VM"`.
4. Create the test CA and the server certificate: `bash make-test-ca.sh "$RUN/ca" "$TLS_SERVER_NAME"`. Copy only `$RUN/ca/ca.crt` (public) to each cluster as `CA_FILE_CLUSTER`. The keys never leave `$RUN/ca`.
5. Stage the kit with `dl/` (frp only; Envoy runs on the VM) on a filesystem the compute nodes see, for each cluster (compute nodes may have no internet): `rsync -a --exclude dl/envoy spikes/s1-frp/ <cluster>:"$KIT_DIR_CLUSTER"/`.
6. **Start the VM side**, all on loopback, and record each PID (stop them later by these PIDs only). It answers step 0 and serves the full runs, so it runs from before the first probe job to after the last run:
   ```
   FRPS=$(echo dl/linux_amd64/frp_*/frps)
   python3 render_tmpl.py frps.toml.tmpl BIND_ADDR=127.0.0.1 BIND_PORT="$FRPS_PORT" TCPMUX_PORT="$TCPMUX_PORT" \
     PROXY_BIND_ADDR=127.0.0.1 TOKEN_FILE="$TOKEN_FILE_VM" PLUGIN_ADDR="127.0.0.1:$PLUGIN_PORT" LOG_FILE="$RUN/frps.log" > "$RUN/frps.toml"
   python3 render_tmpl.py envoy.yaml.tmpl LISTEN_PORT="$DIALHOME_PORT" CERT_FILE="$RUN/ca/server.crt" KEY_FILE="$RUN/ca/server.key" \
     FRPS_PORT="$FRPS_PORT" SINK_PORT="$SINK_PORT" > "$RUN/envoy.yaml"
   "$FRPS" verify -c "$RUN/frps.toml" && "$ENVOY_BIN" --mode validate -c "$RUN/envoy.yaml"
   python3 plugin_stub.py --listen "127.0.0.1:$PLUGIN_PORT" --log "$RUN/stub.jsonl" >/dev/null 2>"$RUN/stub.err" & echo $! > "$RUN/stub.pid"
   "$FRPS" -c "$RUN/frps.toml" > "$RUN/frps.out" 2>&1 & echo $! > "$RUN/frps.pid"
   python3 path_rate.py serve --listen "127.0.0.1:$SINK_PORT" --max-seconds 43200 --log "$RUN/sink.jsonl" 2>"$RUN/sink.err" & echo $! > "$RUN/sink.pid"
   python3 path_rate.py serve --listen "127.0.0.1:$AGENT_PORT" --tls-cert "$RUN/ca/server.crt" --tls-key "$RUN/ca/server.key" \
     --max-seconds 43200 --log "$RUN/agent.jsonl" 2>"$RUN/agent.err" & echo $! > "$RUN/agent.pid"
   "$ENVOY_BIN" -c "$RUN/envoy.yaml" --disable-hot-restart --concurrency 2 --log-level warn > "$RUN/envoy.log" 2>&1 & echo $! > "$RUN/envoy.pid"
   ```
   Check both TLS listeners from the VM itself; each must print `"far_end": "token"`:
   ```
   python3 path_rate.py probe --host 127.0.0.1 --port "$AGENT_PORT" --tls --ca "$RUN/ca/ca.crt" --server-name "$TLS_SERVER_NAME"
   python3 path_rate.py probe --host 127.0.0.1 --port "$DIALHOME_PORT" --tls --ca "$RUN/ca/ca.crt" --server-name "$TLS_SERVER_NAME"
   ```
   The agent stand-in holds the agent endpoint's port, so no launcher with port base 29220 may run on the VM during the spike. The `/s1/` route is the spike's; alpha's Envoy has no such route.

**B. Step 0, the reachability probe, on each platform (before any frp step).** Each job runs on a real compute node, takes about a minute and writes one JSON line per probe to `$RUN_ROOT/<jobid>-r0/probe.jsonl`. `RUN_ROOT` is the directory that holds the job directories. It is a submit-time environment variable, like `TRANSPORT`, and defaults to `$SCRATCH/s1-frp`. An operator who works in a subdirectory of their own sets it at submit time (`RUN_ROOT="$SCRATCH/<own-subdirectory>/s1-frp" sbatch ...`) and does not override `SCRATCH`. It must be an absolute path without whitespace; the job creates it if it is missing, refuses a relative or spaced value with exit 1, and still refuses to reuse a job directory.

An ssh forward opens its local listener as soon as the key authenticates, which proves the key and the host key and nothing about the port behind it. So each path probe does a **verified TLS exchange** through its forward: `GET /s1/probe` over TLS, the certificate checked against `CA_FILE` for `TLS_SERVER_NAME`, and the fixed token (`S1-PROBE-TOKEN-OK`) in the reply. Through the control forward the agent stand-in answers; through the tunnel forward Envoy terminates TLS and passes the request to the sink, so the exchange also proves Envoy's routing. A closed port, a silent listener, a listener that does not speak TLS, a certificate from another CA and a port the key may not forward to all fail it.

| `probe` | What it does | Expected | Why it is there |
|---|---|---|---|
| `env` | Records the node's architecture, kernel, host `python3` version, and the ssh and curl versions. | n/a | Python 3.6 compatibility and the arch check. |
| `tcp_vm_22` | TCP connect to `VM_ADDR:VM_SSH_PORT` (6 s limit), reads the banner's first 7 bytes. `ok` needs an `SSH-` banner, so a middlebox that accepts a connection and passes nothing on does not count. | ok, `SSH-2.0` | Informational. **A banner does not show whose sshd answered** (lesson L-17.7): `hostkey` decides. |
| `hostkey` | ssh mode, before any forward: the ed25519 host key that `VM_ADDR:VM_SSH_PORT` presents (`ssh-keyscan` into `ssh-keygen -lf -`) against `VM_HOSTKEY_FP`, then the pinned `known_hosts` entry for the name ssh looks up. `state` is `match`, `mismatch` (another key answers: not the VM's sshd), `no_answer`, `pin_missing` (no entry under that name, for example a port-22 entry for another port), `pin_mismatch` or `tool_missing`. The line names neither the address nor a key. A full run repeats it as `hostkey_preflight` right before its forwards. | `ok: true`, `state: match` | Anything else exits 6 (7 for `no_answer`) before a forward starts. |
| `tcp_vm_agent`, `tcp_vm_dialhome` | TCP connect to `VM_ADDR:AGENT_PORT` and `VM_ADDR:DIALHOME_PORT`. | fail on this VM (the VM side listens on loopback only) | Records whether direct mode would work from a compute node. Informational. |
| `https_github`, `https_pypi` | `curl` to `https://github.com/` and `https://pypi.org/simple/` on 443. "Reached" means curl got an HTTP status, any status. The pypi request asks for one byte (`--range 0-0`). | an HTTP status | Whether compute nodes reach the internet (HBEP-6). `proxy_env` says whether a proxy variable is set (never its value). Informational. |
| `path_ctl`, `path_tun` | ssh mode: a real `ssh -N -L` for that forward alone (`LPORT_CTL` to `AGENT_PORT`, `LPORT_TUN` to `DIALHOME_PORT`), then the verified exchange through its local end; the ssh is stopped by its recorded PID and start time afterwards. Direct mode: the exchange with the VM's port. `far_end` is `token` (the only pass), `tls_failed` (the certificate does not verify, or the far end does not speak TLS), `closed` (nothing behind the forward, or the key may not forward there), `timeout` (accepted, silent), `refused`, `data` or `http_error`. | `ok: true`, `far_end: token` for both | The transport. Both decide the exit code. |
| `summary` | One line with the results above and `transport_path_ok` (`true` only when both path probes passed). | n/a | Fills the results table. |
| `path_rate` | Full runs only, one line per forward and direction: see "Path rate". | `ok: true` | The head-of-line scale and the timeout budget. |
| `frpc_login` | Full runs only. `ok: true` when frpc's own log shows `login to server success` within `FRPC_LOGIN_WAIT_S` (90 s): a Login reached frps through the forward and Envoy, over verified wss, and the plugin accepted it. | ok | The gate for a full run. No login in time ends the job with exit 3 and cleans up. |
| `watcher_attach` | Full runs only. `ok: true` when a request of `stream_watch.py` reached the fake engine through the tunnel within `WATCHER_WAIT_S` (900 s, 15 min) of the login. | ok | A paid allocation without a watcher is waste. No watcher in time ends the job with exit 5 and cleans up. |

1. Render the job. The values for `PLATFORM`, `PARTITION` and the account come from the table above:
   ```
   python3 render_tmpl.py job.sbatch.tmpl PLATFORM=helios-cpu ACCOUNT="$ACCOUNT_HELIOS_CPU" PARTITION=plgrid \
     TIME=01:25:00 MEM="$MEM" VM_ADDR="$VM_ADDR" VM_SSH_PORT="$VM_SSH_PORT" VM_HOSTKEY_FP="$VM_HOSTKEY_FP" \
     AGENT_PORT="$AGENT_PORT" DIALHOME_PORT="$DIALHOME_PORT" VM_USER="$VM_USER" \
     SSH_KEY="$SSH_KEY_CLUSTER" SSH_KNOWN_HOSTS="$KNOWN_HOSTS_CLUSTER" LPORT_CTL="$LPORT_CTL" LPORT_TUN="$LPORT_TUN" \
     CA_FILE="$CA_FILE_CLUSTER" TLS_SERVER_NAME="$TLS_SERVER_NAME" TOKEN_FILE="$TOKEN_FILE_CLUSTER" \
     INSTANCE_PREFIX=s1-helios-cpu INSTANCE_UID="$INSTANCE_UID" KIT_DIR="$KIT_DIR_CLUSTER" PYTHON=python3 > job-helios-cpu.sbatch
   ```
   `TRANSPORT` is not a placeholder: it is an environment variable read at submit time, default `ssh`. So are `RUN_ROOT` and the limits `OUTAGE_DEADLINE_S` (300), `FWD_STARTUP_WAIT_S` (20), `PATHRATE_S` (30), `PATHRATE_DIRECTIONS` (`up down`), `PATHRATE_MAX_BPS` (0, no cap; for local tests only), `FRPC_LOGIN_WAIT_S` (90), `WATCHER_WAIT_S` (900), `ENGINE_DOWN_S` (20), `FREEZE_SECONDS` (60) and `ENGINE_PORT` (0). `VM_SSH_PORT` is rendered from the settings; a submit-time value overrides it. For `TRANSPORT=direct` (only if the owner opens the two ports) the five ssh values and `VM_HOSTKEY_FP` are passed as `none`.
2. Submit the probe from a scratch directory on the cluster, with the VM side running. It needs no token and no frp binary, only `ssh`, Python, the key, the known_hosts file and the CA file: `PROBE_ONLY=1 sbatch --time=00:10:00 job-helios-cpu.sbatch`. On Athena the rendered job already names `PARTITION=plgrid-gpu-a100`; do not pass another partition. The job script has `#SBATCH --no-requeue`.
3. Read `$RUN_ROOT/<jobid>-r0/probe.jsonl` and `export/job.log`. The probe lines hold no address, node name or user (the VM is `vm`, and ssh's messages are scrubbed). When a path probe fails, its `stage` says where: `precondition` (key file, mode, known_hosts, CA file, or a listener already on the local port), `ssh_exited` (ssh's last stderr line is in `detail`: `Permission denied (publickey)` is an unauthorised key or wrong user, `Host key verification failed` a known_hosts mismatch, `Connection timed out` port 22 not reachable), `no_listener_in_15s`, or `ok` with `far_end` other than `token`.
4. **Exit codes.** 6: the host key does not match: the sshd at `VM_ADDR:VM_SSH_PORT` is not the VM's, or `known_hosts` lacks or contradicts the pin (`hostkey` says which); stop and ask the owner. 7: no host key came back from `VM_ADDR:VM_SSH_PORT`. Neither starts a forward. 0: the transport works (a `PROBE_ONLY=1` job: both exchanges passed; a full run: it ran until TERM or the time limit). 3: it does not (step 0, or frpc's login did not reach frps in time), and the spike records "not reachable" for that platform and stops. 4: a forward failed for good (authentication, host key or forwarding error, or no established forward for `OUTAGE_DEADLINE_S`) and the job cleaned up; `transport.failed` names the forward (`ctl:` or `tun:`). 5: no watcher attached in time. 1: the job is misconfigured, or this is a restart of the allocation (refused). The informational TCP probes never decide the exit code by themselves. Opening a firewall port, or any other access change, is the owner's decision.

**C. Full run for each platform that passed step 0** (one at a time: Helios CPU, Helios GH200, Athena)

1. The VM side from A6 is running. Check that both TLS listeners still answer (the two probes of A6).
2. **Start the watcher before submitting the job.** The tcpmux domain is the Instance UID, known before `sbatch` runs; the frp user is `INSTANCE_PREFIX`: use one prefix per run (`s1-helios-cpu`, `s1-helios-gh200`, `s1-athena`) and never have two jobs with the same UID pending or running. `--wait-up` makes the run clock start at the first good probe, so queue time and the job's 2-minute path-rate step do not matter. `--streams 4` keeps four streams open at once (see "Four streams at once"). The job gives the watcher `WATCHER_WAIT_S` (15 min) to show up after frpc's login and ends itself with exit 5 if none does.
   ```
   python3 stream_watch.py --tcpmux "127.0.0.1:$TCPMUX_PORT" --domain "$INSTANCE_UID" \
     --wait-up 7200 --duration 4260 --stream-seconds 3660 --streams 4 --stub-log "$RUN/stub.jsonl" --stub-user <INSTANCE_PREFIX> \
     --out "$RUN/summary-<run>.json" > "$RUN/watch-<run>.out" 2>&1 & echo $! > "$RUN/watch.pid"
   ```
   Then submit from the cluster scratch directory: `sbatch job-<run>.sbatch`, and note the job id. **If the watcher expires while the job is still pending** (its summary says `never_up`, exit 3), **the operator cancels the job** (`scancel <jobid>`): a job that starts later has nobody attached and would burn an allocation unobserved. A job that starts and sees no watcher request within 15 min of its login cancels itself, but a pending job is the operator's to cancel. `watch-<run>.out` prints the moment the run clock starts. Call it T+0.
3. T+0 to T+61 min: touch nothing. All four streams of 3660 s must run uncut.
4. Fault tests, each with a mark first. **Before each mark, check that the path is up**: `grep 'probe: path' "$RUN/watch-<run>.out" | tail -n 1` must not be a `path DOWN` line, and after a fault the next one waits for `probe: path UP again`. On this VM `kill -USR1 "$(cat $RUN/watch.pid)"`, then at once on the cluster `scancel --batch --signal=<SIG> <jobid>`. A mark taken while the probe is not `up` is an invalid fault measurement: the summary records it as `valid: false` with the reason (`invalid_reason`), `verdict.py` fails its criterion, and it is never counted as a detection. Wait out each one before the next:

   | At | Signal | Fault | What to expect |
   |---|---|---|---|
   | T+62 min | `HUP` | frpc stopped for 60 s, socket open, no FIN (silent loss) | probe down, `CloseProxy` within the silent-loss budget, then a new `Login` after the thaw, then `path UP again` |
   | T+65 min | `USR2` | fake engine killed, frpc alive; **the job restarts the engine on its original port after 20 s** (`ENGINE_DOWN_S`) | probe fails as `no_response`, `Ping` lines keep coming, no `CloseProxy`, then `path UP again` once the engine is back |
   | T+68 min | `USR1` | frpc killed (only after `path UP again` from the engine fault) | `CloseProxy` within seconds, probe `path_down` |

   The engine is restarted so that the frpc fault starts from an up path. Without the restart the path is down at T+68, the watcher has nothing new to declare, and the measurement is empty.
5. At T+71 min the watcher exits and writes the summary. `scancel <jobid>` stops the job cleanly (TERM: the job stops frpc, the engine, then both forward loops and their ssh, each by PID and start time, and writes `export/`).
6. **Collect only `export/`.** `$RUN_ROOT/<jobid>-r0/export/` holds redacted copies of `job.log`, `probe.jsonl` (step 0 and the path-rate lines), `ssh-ctl-exits.jsonl`, `ssh-ctl-counters.json`, `ssh-tun-exits.jsonl`, `ssh-tun-counters.json`, `frpc.log` and `direct-baseline.json`: no IPv4 or IPv6 address (loopback excepted), no VM account or local account, no key, known_hosts, CA, token or scratch path, no VM address, node name or job id. **The raw files must never be copied out of the job directory**: `job.log` (it holds the node name and paths), `ssh-ctl-raw.err`, `ssh-tun-raw.err`, `ssh-ctl-cur.err` and `ssh-tun-cur.err` (ssh's own stderr, which names the VM address, the account and the key paths), `ssh-probe-ctl.err`, `ssh-probe-tun.err`, `.redact.sed`, `frpc.stdout`, `frpc.toml`, `streamer.log`, `path-rate.err`, `pids`. From the VM, collect the summary and `$RUN/stub.jsonl`.
   - Each `ssh-<fwd>-exits.jsonl` has one line per attempt of that forward with only these fields: UTC time, attempt number, exit code, class (`auth`, `hostkey`, `forwarding` or `transient`), whether the forward was established, seconds it lived, restart delay. Each `ssh-<fwd>-counters.json` keeps four counts apart: **attempts** (every start of ssh), **established** (sessions whose forward came up), **disconnects** (established sessions that ended on their own) and **recoveries** (sessions established after a disconnect). An attempt that fails to authenticate is an attempt and nothing else; it is not a reconnect.
   - Run the verdict on this VM: `python3 verdict.py --summary "$RUN/summary-<run>.json" --job-log export/job.log --probe export/probe.jsonl --ssh-counters export/ssh-ctl-counters.json --ssh-counters export/ssh-tun-counters.json`. It prints PASS or FAIL per criterion, FLAG lines, and `OVERALL: PASS` or `OVERALL: FAIL`.
7. After the last run, stop the VM side by the recorded PIDs: `kill "$(cat $RUN/envoy.pid)" "$(cat $RUN/agent.pid)" "$(cat $RUN/sink.pid)" "$(cat $RUN/frps.pid)" "$(cat $RUN/stub.pid)"`, and check that nothing listens on `AGENT_PORT`, `DIALHOME_PORT`, `FRPS_PORT`, `TCPMUX_PORT`, `PLUGIN_PORT` or `SINK_PORT`.

**D. Cleanup, when the spike ends.** The dial-home keys are not the spike's alone: the same keys serve alpha (owner, 31 Oct 2026), so they stay authorised and stay in `~/.hpcburst/dial-home/` on each cluster. What goes:

1. On the VM, the token file and the run directory (with the test CA's keys) once the logs are collected, and the VM side's processes if any still runs.
2. On each cluster, the token file, the test CA's certificate (`CA_FILE_CLUSTER`; alpha's agent endpoint uses the launcher's certificate), the staged kit and the job directories. Deleting staged files needs the owner's go.
3. If the owner ever withdraws a key, remove its line from the VM account's `authorized_keys` (the comment `hpcburst-dialhome-<cluster>` finds it) and delete `~/.hpcburst/dial-home/key` and its `.pub` on that cluster.

Before anything goes into the PR, read the files of `export/` once more for addresses, host names, node names, user and account names and job ids. The job redacts them at exit, but a pattern list is not a proof; the raw files are never an input to the PR.

## Path rate (C6)

**Why.** frp gives each yamux stream a 6 MiB window, and all streams share one connection (spike S1c). One large reply can therefore put up to 6 MiB on the tunnel ahead of everything else on it, frp's own heartbeats included. Draining it takes about 6 MiB divided by the path rate: S1c's review established this as an **observed scale, not a bound** (S1c saw a tight heartbeat timeout drop a healthy session behind one large reply). What the heartbeat timeout must cover is the **timeout budget**: the heartbeat interval, plus the worst control delay the run observes, plus the round trip, plus a margin. yamux's keepalive ping has a fixed 10 s write timeout, so on a path slower than 6 MiB / 10 s = **629,146 B/s** (about 630 kB/s) a full window takes longer to drain than that, and such a path is a risk whatever the heartbeat setting.

**Method.** After step 0 and with both forwards up, **before frpc starts**, the job runs one bulk transfer at a time, the tunnel first:

- **up** (the node sends to the VM, the direction of the tokens): `POST /s1/sink`, chunked, 64 KiB chunks of random bytes for `PATHRATE_S` (30) s. The VM's receiver times it from the first body byte to the last and returns the byte count, so the rate is not overstated by what sits in the ssh and TCP buffers when the sender stops.
- **down** (the VM sends): `GET /s1/source?seconds=30`, timed at the job's receiver. It is cheap, so the job measures it too.
- **Through the tunnel forward** the transfer crosses the path frpc's wss uses: ssh connection 2 and Envoy's TLS; only the route differs (`/s1/` to the sink instead of `/~!frp` to frps). **Through the control forward** it crosses ssh connection 1 to the agent stand-in's TLS.
- It runs before frpc on purpose: forward 2 is the ssh connection that will carry frpc's channel, and a bulk transfer on another channel of it would hold frp's pings and tokens back.

Each result is a `path_rate` line in `probe.jsonl` (exported), with `forward` (`tun` or `ctl`), `direction`, `ok`, `requested_s`, `max_bps` (the cap, null for none), `bytes`, `duration_s`, `bps`, `timed_by` (`receiver-vm` or `receiver-job`) and `sender_s`. That file is the run's record of the path rate; `verdict.py` reads it.

**Judged.** C6 passes when the tunnel's up measurement is `ok`, asked at least 30 s, was timed at least 29 s at the VM's receiver, and moved bytes. It prints the observed head-of-line scale (6 MiB / rate) and, next to it, the timeout budget: heartbeat interval + worst observed control delay + RTT + margin, against S1's heartbeat timeout. The interval (10 s) and the timeout (25 s, the smaller of frpc's and frps's) are read from `frpc.toml.tmpl` and `frps.toml.tmpl`. The control delay is the longest gap between two of the instance's `Ping` calls at the plugin before the first mark, minus the interval (the watcher records the gaps as `stub.ping_interval_unfaulted_s`). The RTT is the longest kept-alive `/health` request through the tunnel (`latency_baseline.tunnel.kept_alive_request_ms.max`). The margin is 5 s: frps checks each heartbeat once a second, and 5 s covers that check and scheduling slack, as the 25 s setting allowed over two missed pings. FLAG lines, which do not change `OVERALL`: any measured rate under 629,146 B/s; a budget over the timeout, or one the summary cannot supply; a capped measurement (`PATHRATE_MAX_BPS`, used by the local tests to keep the shared VM's load down: a cap is not the link rate).

**What it does not measure.** frp's framing on the wire (S1c: a 222-byte token chunk took 305 bytes over wss); the rate at another time of day or under other users' load (one 30 s sample per forward per run); the rate of several channels at once. An ssh channel has its own flow-control window, so one channel's rate is bounded by that window divided by the round-trip time; frpc's tunnel is one channel, so the measured one-channel rate is the rate frpc gets.

## Four streams at once (`--streams 4`)

The 1 h runs keep four streams open instead of one: `stream_watch.py --streams 4`. All four go through the same frps tcpmux route (the UID domain) and the same frpc (one proxy, four `CONNECT`s, plus the `/health` probe), and all four request the full 3660 s at the fake engine's fixed rate of 1 token per second. That is one long-lived stream at full length plus three more. The fake engine serves them concurrently (`ThreadingHTTPServer`; Python 3.6 gets an equivalent class).

- Each stream is a lane. A stream that is cut is reopened on its own lane, as the single stream always was. Lane 1 runs on the watcher's main thread, so `--streams 1` (the default) behaves as before.
- The summary keeps its top-level keys as overall aggregates (`streams`, `cuts`, `gaps`, `streams_completed`). Every stream, cut and over-threshold gap now carries its `lane`. `streams_requested` is the declared count. `per_stream` has one entry per lane (streams, completed, cuts, `first_stream_completed`, tokens, `max_gap_s`, `max_gap_unfaulted_s`, `gaps_over_threshold_unfaulted`), and `overall` gives the same totals across lanes. The digest prints one line per stream and one overall.
- `verdict.py` applies C2 to every lane: each must have a completed stream of at least 3600 s and no cut before the first mark. A lane with no stream record at all, because it never opened, fails. One cut stream fails C2 whatever the other three did. Cuts after the first mark are the injected faults' effect and do not fail C2.

What this does not test:

- **Throughput at many streams.** Four streams at 1 token per second carry a few hundred bytes per second. That does not load the tunnel, the ssh channel windows or frp's multiplexing (S1c measured those on loopback; the path rate gives the real link's one-channel rate).
- **wss on port 443 straight from a node.** The streams cross frp's wss through Envoy, but inside ssh connection 2 on port 22. Step 0 showed that 443 is reachable from the nodes; a run with `TRANSPORT=direct` against a front on 443 would test that path, and needs a reachable front.

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

- **The forwards and Envoy add layers.** frpc's TCP connection ends at a local ssh process (connection 2), ssh carries it to sshd, which opens Envoy's loopback port; Envoy ends TLS and, after the websocket upgrade, relays bytes to frps. Envoy keeps no heartbeat of its own here (stream idle and route idle timeouts are off), so a frozen frpc is still found by frp's heartbeat. A killed frpc's FIN travels through ssh and Envoy before frps sees EOF; the local runs measure that path. The three injected faults (frpc frozen, engine killed, frpc killed) do not touch ssh, so the rows above hold unchanged. A real network loss between the node and the VM is a different case: frp's heartbeat (25 s), yamux (20 s) and ssh's own keepalive (10 s, three misses, so 30 s) all run, and frpc can reconnect through the forward only after its loop has restarted ssh, which adds the reconnect delay (2 s or more). An `ssh-tun: attempt N exited` or `ssh-ctl: attempt N exited` line in the exported `job.log` (or a line of `ssh-<fwd>-exits.jsonl`) with no injected fault before it is a finding. frps sees every instance as coming from 127.0.0.1, because Envoy opens the connection.
- **The heartbeat timeout and the path rate.** 6 MiB / path rate is the head-of-line scale S1c observed, not a bound. The 25 s timeout is judged against the timeout budget (heartbeat interval + worst observed control delay + RTT + 5 s margin), which `verdict.py` computes from the run's own Ping gaps and tunnel RTT (see "Path rate").

- Marks are taken on this VM before the cluster command is sent, so every measured time includes the command's own latency. They overstate detection time, which is the safe side. All times come from this VM's clock (the watcher's marks and the stub's timestamps), so clock skew with the compute node does not enter. `clock_offset_plus_one_way_s` in the summary is an estimate, for information only.
- Killing a process measures the FIN path. Only the freeze exercises the heartbeat budget, so the two are reported separately.
- A frozen frpc shows as `no_response` at the probe, the same as a dead engine. The `Ping` and `CloseProxy` lines from the stub tell them apart: Pings continuing means the agent is alive.
- A session lost for no injected reason during the hour (a `Login` after the first one with no mark before it) is a finding: yamux gives up after 10 s of silence, which a starved compute node might cause.

## Pass criteria (issue #18)

`python3 verdict.py ...` (see C6 of the run order) prints PASS or FAIL for each of the criteria below (its ids C1 to C6), FLAG lines, and `OVERALL`. `stream_watch.py` exits 0 whenever it collected a summary, even with cuts or an undetected fault; the exit status is not a verdict.

For each platform that passes step 0:

1. (C1) frpc starts inside a Slurm job on a compute node, as a plain user, and registers through the plugin endpoint. Evidence: `export/job.log` (`frpc: login to server success`) and a `Login` plus `NewProxy` line in `stub.jsonl`.
2. (C2) Every stream behind frpc (four, with `--streams 4`) stays reachable through frps, uncut, for at least 1 h. Evidence, per lane in `per_stream`: its first stream has `completed: true`, `requested_s` of 3660 and `duration_s` of at least 3600, and the lane has no cut before the first mark. Two cases fail by design: one cut stream with three that completed, and a lane that never opened.
3. (C3) No stream is cut over that hour, on any lane. Evidence: `cuts` is empty before the first mark, `gaps.over_threshold_unfaulted` is 0, and no entry of `stream_open_failures` or `health.failures_sample` has a time before the first mark.
4. (C4a, C4b, C4c: the frozen frpc, the killed engine, the killed frpc, in the order of the marks) Killing frpc, or the engine, marks the path down within the budget above. Evidence: `marks[]` and the stub lines. A mark taken while the path was already down or failing is `valid: false`: the criterion fails and a rerun from an up path is needed; it is never a detection.
5. (C5, with the informational counts) Recorded for the PR: outbound protocol (frp wss through Envoy, inside ssh connection 2), attempts, established sessions, disconnects and recoveries of each forward (counted apart in `ssh-ctl-counters.json` and `ssh-tun-counters.json`), reconnects of frp, added latency, centre-side blocking, and the step-0 reach of the VM's port 22, its two service ports and of 443 (table below). C5 passes when frpc's login through the transport is recorded and the job did not log a permanent transport failure.
6. (C6) The tunnel's path rate in the token direction was measured: `ok`, at least 30 s asked, at least 29 s timed at the VM's receiver. The verdict prints the observed head-of-line scale 6 MiB / rate and the timeout budget (heartbeat interval + worst observed control delay + RTT + margin) against S1's 25 s heartbeat timeout; a rate under 629,146 B/s, a budget over the timeout or unknown, and a capped number are FLAG lines (see "Path rate").

Verifications that prove nothing, so none of them goes into the results:

- A same-host run, or a run from a login node: neither shows that a compute node can reach out. This includes `local-smoke.sh`.
- A stream shorter than about 30 min: the cuts seen before came from deadlines of 180 s, 900 s and 1,800 s.
- A process kill as the test of the heartbeat budget: it measures the FIN path. Use the freeze.
- A `Ping` line as proof that the engine is up: it shows the agent is alive. The probe through the path shows the engine.
- A sender-timed rate, or a capped one, as the path rate: the first counts bytes still in the buffers, the second measures the cap.
- An ssh forward that merely listens, as proof that a path works: it proves the key. Only the verified exchange through it counts.

## Results tables (to fill in)

**Step 0, one row per platform** (from `probe.jsonl`; the `summary` line holds the five results):

| Platform | VM's ssh port: banner, and `hostkey` state | VM's two service ports (expected to fail) | 443 (github, pypi, `proxy_env`) | `path_ctl`, `path_tun` (ok, ms, `far_end`, or `stage` and reason) | Node arch, kernel, host `python3` |
|---|---|---|---|---|---|
| Helios CPU | | | | | |
| Helios GH200 | | | | | |
| Athena | | | | | |

**The runs, one column per run:**

| | Helios CPU | Helios GH200 | Athena |
|---|---|---|---|
| Outbound protocol used (frp wss through Envoy inside ssh connection 2, or direct) | | | |
| ssh forwards during the hour, control and tunnel: attempts, established, disconnects, recoveries | | | |
| Path rate, tunnel up / down and control up / down (B/s; bytes and seconds) | | | |
| Observed head-of-line scale 6 MiB / tunnel-up rate (s); timeout budget (s) against 25 s; the FLAG lines | | | |
| frpc and frps version, frpc arch | | | |
| Python on the node (from `job.log`) | | | |
| Registered through the stub (`Login`, `NewProxy`) | | | |
| 1 h streams (four): completed, seconds, tokens, per stream | | | |
| Stream cuts before the first mark | | | |
| Token gaps: max, p99, count over 2 s (overall, and the max per stream) | | | |
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

- Python 3.6 itself. No 3.6 interpreter was available (no Docker; `uv` has no 3.6 build). What was done instead: `ast.parse(..., feature_version=(3, 6))` on every script (it rejects walrus and positional-only parameters, but does not look inside f-strings, so the scripts use no f-strings), plus a hand check of the stdlib calls: no `fromisoformat`, `time_ns`, `capture_output`, `nullcontext`, `dataclasses`, type annotations or `from __future__ import annotations`. `fake_streamer.py` imports `ThreadingHTTPServer` (Python 3.7 and newer) and falls back to an equivalent class (`ThreadingMixIn` plus `HTTPServer`) on 3.6; the fallback branch was run with `ThreadingHTTPServer` removed from `http.server` in a child process (`tests/test_engine_and_responder.py`), not on a real 3.6. Step 0 showed the compute nodes' host `python3` is 3.9.25, so the fallback is a margin.
- **wss through Envoy for an hour.** frps accepts websocket only as plain `ws` on its bind port, so Envoy terminates TLS in front of it, as in S1c. The local runs carried frpc's verified wss through Envoy for minutes, and a 35 s transfer through Envoy's `/s1/` route showed that its route timeout does not cut a long stream. No connection through Envoy has lasted an hour; the 1-hour runs are the first. Envoy's connection idle timeout (1 h by default) applies only to connections without an active stream; the upgraded websocket is an active stream.
- The arm64 frpc and `scancel --batch --signal` were not run. Step 0 ran the job template under Slurm on all three platforms, and the GPU variables it logs were present (`SLURM_JOB_GPUS`, `SLURM_GPUS_ON_NODE`, `CUDA_VISIBLE_DEVICES`, one device each), but it needs no frp binary and sends no signal. The full run's template path ran locally as a plain script, on loopback, with the same signals sent by `kill`.
- **The two forwards, against a real sshd, but not the VM's and not from a compute node.** The job's real ssh path ran locally against a throwaway sshd (this user, not root, a loopback port in 29400–29499, its own host key, its own temporary `AuthorizedKeysFile` holding the README's key line, nothing under `~/.ssh`): step 0 with the VM side's stand-ins (verified token through both forwards, exit 0); step 0 with nothing behind the tunnel forward, a silent far end, a far end that does not speak TLS, a certificate from another CA and a port the key may not forward to (all exit 3); two separate ssh processes and two established connections to the sshd; the path rate through both forwards; a full run with the real Envoy, frps, frpc, stub and watcher (`tests/e2e_job.sh`); a revoked key mid-run (real `Permission denied (publickey)`, exit 4) and a changed host key mid-run (real `Host key verification failed`, exit 4). What this does not show: that the VM's own sshd, its `sshd_config` and the Cyfronet network treat the forwards the same way, that a compute node can reach port 22 at all (that is what step 0 is for), and anything about the first real job. The ssh double (`tests/fake_ssh.py`, one plan per forward) stands in only where a case cannot be made to happen on demand with a real sshd (a forwarding failure, an outage, a failure of one forward alone), and its stderr phrases are the ones OpenSSH prints.
- **The kit's own tests are not the spike.** `tests/run-all.sh` passing says the kit does what the review asked on this machine, with a fake engine, a fake frpc where a test needs a job that logs in on demand, and loopback. It says nothing about a compute node.
- **The host-key check from a compute node.** It ran against a throwaway sshd on loopback ports in 29400–29499 (so always the `[host]:port` form): the sshd's own key passes; another key at the address exits 6; a closed port exits 7; a `known_hosts` pinning another key, or holding only a port-22 entry, exits 6; a key that changes between step 0 and the preflight stops the job before its forwards. Port 22 itself cannot be served by a user process here, so its bare-host form is covered by a unit test of the name rule only. Whether `ssh-keyscan` reaches the VM through the gateway's path from a compute node is what step 0 will show.
- **Compute-node tooling.** The probes assume bash 4.4 or newer, GNU `date` (for `%N`), `timeout`, `curl`, `ssh`, `ssh-keyscan`, `ssh-keygen` and Python 3.6 or newer (with its `ssl` module) on the node. Missing `curl` is recorded as `ok: null` (unknown), a missing `ssh` or Python as failed path probes. Step 0 found bash 5.1.8, curl 7.76.1 and OpenSSH (9.9p1 on Helios, 8.7p1 on Athena) on the compute nodes, and its probes ran to the end. It ran in direct mode, so `ssh -N -L` itself was not exercised from a compute node.
- A reading of one run on one machine is not a statistic: the budget numbers above are targets to confirm, not measurements.

## Provenance of what is reused

| What | Origin | Licence |
|---|---|---|
| `frps`, `frpc`, tcpmux, server plugins, token auth, yamux multiplexing | frp v0.71.1, https://github.com/fatedier/frp | Apache-2.0 |
| The keepalive timing used in the budget (10 s write timeout) | `fatedier/yamux` v0.2.0, a fork of `hashicorp/yamux`, built into frp | MPL-2.0 |
| The TLS terminator in front of frps (wss), and its `/~!frp` websocket route | Envoy 1.33.2 release binary, https://github.com/envoyproxy/envoy, sha256 `bd2ff87c…fad741` from the release's checksums.txt.asc (signature not verified); the binary and config shape of spike S1c (`spikes/s1c/s1c_lib.py`, `ENVOY_YAML`, PR #78), first fetched by spike S2 | Apache-2.0 |
| The test CA and server certificate | OpenSSL (`openssl req`, `openssl x509`), as installed on the VM; the certificate shape follows S1c's `make_certs` | Apache-2.0 |
| The ssh forwards, their restrictions (`restrict`, `port-forwarding`, two `permitopen`, `permitlisten`, `command`; optional server-side `AllowTcpForwarding local`, `AllowStreamLocalForwarding no`), host-key pinning (`StrictHostKeyChecking`, `UserKnownHostsFile`) and the keepalives | The OpenSSH project (the ssh client and sshd already on every node and on the VM) | BSD and ISC style |
| The ports (agent endpoint base+3, dial-home listener base+7) and the key directory `.hpcburst/dial-home` | `pkg/agentapi/layout.go` of this repo (PR #79), owner's decisions of 31 Oct 2026 | this repo |
| The 6 MiB yamux stream window and the head-of-line finding behind the path-rate scale (an observed scale, not a bound, per S1c's review) | spike S1c (PR #78), from frp v0.71.1 `client/connector.go` and `server/service.go` | n/a |
| The 443 reach probe | `curl`, as installed on the node | curl licence (MIT style) |
| The stub, streamer, watcher, renderer, path-rate tool, job template (probes, two reconnect loops, path rate, redaction, process records included), smoke test, probe responder, verdict, tests (including the throwaway-sshd helper and the ssh double) | new, Python standard library and bash | this repo |
| The long-stream cut deadlines the watcher guards against (180 s, 900 s, 1,800 s) and the HTTP vhost 60 s trap | www-2027 lessons, as quoted in issue #18 | n/a |

Claims about frp's defaults and behaviour come from the v0.71.1 sources: `doc/server_plugin.md`, `pkg/config/v1/{client,server}.go`, `server/control.go`, `server/service.go`, `pkg/util/vhost/vhost.go` and `pkg/util/tcpmux/httpconnect.go`.
