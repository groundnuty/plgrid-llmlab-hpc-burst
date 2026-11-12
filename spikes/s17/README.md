# Spike S17: the open items of the bulk layout (HBEP-6, open question 12)

Issue #413. S15 (#336) measured one bulk transfer at a time and HBEP-6 adopted layout (c) for bulk paths: a separate frpc with `tcpMux = false`, its own proxy and a second frps. This spike runs the five items S15 left open, as four measurements: a FIFO bottleneck shared by the two frpc's connections, the relay's real `ssh` and `srun` buffers, several bulk transfers at once, and many Instances (the handshake per bulk request, and several Instances behind one link) with a bulk request each per 10 s. The measurements ran on GitHub-hosted runners, three repetitions on fresh runners (111 runs: 96 measured, 15 calibrations), and a second hosted run of 8 (6 measured, 2 calibrations, two repetitions) that logs the relay's ssh rekeys; the development VM ran only builds, smoke runs and the harness's own tests. Every measured run was valid and every run finished: **layout (c) still protects the token streams under all four, with two conditions**: the shared queue's depth, and, in host-dialed mode, a stall of every stream on a relay's ssh connection of about 100–180 ms once per GiB moved, which is OpenSSH's default rekey (confirmed by the second run, which logs it and moves it by changing the limit) and not specific to (c). Several transfers at once cost nothing, and a TLS handshake per bulk request costs no time on the request path while a spare work connection is waiting for it.

## Findings

All figures are the token streams' inter-token gap during a 64 MiB download, p99, pooled over the three repetitions' valid runs (5 downloads per run), against the same runs' reference gap with no transfer (p99 51.4–52.1 ms in every row: the engine double's 50 ms, plus the path). The link is S15's: 48 MB/s each way and a 2.3 ms round trip. "Valid" is S15's rule, which every run met, plus, for concurrent runs, that every batch's transfers overlapped in time.

- **A shared FIFO bottleneck keeps (c)'s advantage while its queue is short, and costs the queue's drain time.** The queue's depth is not given by the issue, and the answer depends on it entirely, so three depths ran, each against the fair link in the same job:

  | link queue | (c) a separate frpc | (a) one frpc |
  |---|---:|---:|
  | fair, per-connection turns (S15's link; the control) | 52.9 ms | 138.6 ms |
  | FIFO, 110 KB (the bandwidth-delay product) | 52.7 ms | 134.4 ms |
  | FIFO, 1 MiB | 72.0 ms | 154.0 ms |
  | FIFO, 6 MiB (frp's stream window) | **180.5 ms** | 139.3 ms |

  - (c)'s gap is the reference plus the queue's drain time: 72.0 − 52.9 = 19 ms at 1 MiB, where 1 MiB over 48 MB/s is 22 ms; 180.5 − 52.9 = 128 ms at 6 MiB, where 6 MiB over 48 MB/s is 131 ms. The three repetitions agree to within 1 ms at 1 MiB (72.3 / 71.9 / 72.1) and 7 ms at 6 MiB (180.5 / 180.8 / 174.0).
  - (c) beats (a) while the queue is below about 4 MiB (where its 52.9 ms plus the drain time reaches (a)'s 139 ms). **At 6 MiB it is worse than (a):** (c)'s bulk connection is plain TCP and fills the queue (full for 6.1 s of about 7 s of transfers), while (a)'s bulk flow is held by frp's 6 MiB stream window to 5.5 MB in the queue (it never filled: 0.0 s).
  - **These are FIFO runs:** the link's statistics show the queue full or nearly so (the largest occupancy 103 KB, 1,049 KB and 6,291 KB for (c)), and a unit test fails a link that serves a small flow as the fair one does (below).
- **The real relay buffers add no head-of-line stall to (c) in a 64 MiB download, over the 320 MiB (5 downloads) that group moved.** Through a real `ssh` client, a real `sshd` and the fake Slurm's `srun` standard streams, (c)'s gap p99 was **54.2 ms**, the same as through S15's stand-in bridge (54.2 ms; the three repetitions 53.7–54.3 against 53.6–54.2), the longest gap 55.9 against 56.2 ms. For (a) it was 180.4 ms against 211.8 ms: the real buffers do not add head-of-line blocking.
  - **The real path is faster and costs more CPU than the stand-in:** one 64 MiB download at 37.7 MB/s in both layouts (36.7–38.6) against the bridge's 31.9 and 34.6 MB/s, and the ssh clients took 0.046–0.048 cores over the transfers' phase against the bridges' 0.029.
  - **A relay stream's rate is bounded by its window and the path's delay,** as S15 derived (256 KiB ÷ round trip): one bulk stream through real ssh got 37–38 MB/s, which is the window over an effective round trip of 7 ms, 4.7 ms more than the link's 2.3 ms. The relay's added delay was not measured apart; this is a derived reading.
  - **Every stream on the relay stalls once per GiB, and the stall is OpenSSH's rekey (confirmed).** In the six runs of four concurrent downloads through real ssh in the first run (both layouts, every repetition), the 8 token streams all stalled at once, 111–154 ms per stream in (c) and 119–179 ms in (a), within the last 0.4 s of the batch whose transfers brought the bytes moved through the relay's one ssh connection to exactly 1.000 GiB (16 downloads of 64 MiB). No other batch of those runs shows it; nor do the ssh runs that moved less (one and two at once, 0.31 and 0.62 GiB; the real-relay group's 0.31 GiB); nor does direct mode, which moved the same 1.25 GiB with no ssh connection (table "Stalls at the end of a batch"). The relay's ssh client sets no `RekeyLimit` and no `Ciphers` (`internal/slurm/sshrun/relay.go:131-145`), so OpenSSH's default applies. A second hosted run (workflow run 43124479105: layout (c), four downloads at once through the real relay, two repetitions) ran every relay client under `ssh -v` and parsed its log (tables "The relay's ssh and its rekeys" and "All-stream stalls against the rekeys"):
    - **The default:** OpenSSH 9.6p1 at both ends, key exchange sntrup761x25519-sha512, host key ssh-ed25519, cipher chacha20-poly1305@openssh.com in both directions (MAC implicit), rekey limit 134,217,728 blocks of 8 B = 1 GiB. The logs show a rekey when the connection had received 1024 MiB (and a second at 2048 MiB in the 2.25 GiB runs): 6 rekeys in the 4 default runs, each started by sshd, and each with an all-stream stall of 121–154 ms per stream at 1013–1016 MiB of payload at the client (2031–2034 MiB for the second). The connection runs 8–17 MiB ahead of the client (4–11 MiB in the 256M runs), the data in frp's and the relay's buffers between them.
    - **With `-o RekeyLimit=256M`** the limit is 33,554,432 blocks (256 MiB) and the connection rekeyed at 257, 514, 770 or 771, 1027 or 1028 and 1284 MiB, 5 times in the 1.40 GiB each run moved, every one started by the client. 9 of the 10 rekeys had an all-stream stall (101–152 ms) at the matching payload; the tenth had one of 98 ms in 6 streams, just under the preset 100 ms (10 of 10 at 80 ms).
    - **No stall without a rekey:** every all-stream stall of the six runs was matched to a rekey, at 100 ms and at 80 ms (0 left over).
    - So the stall is the rekey: it is where the log shows one, and it moves from every 1 GiB to every 256 MiB when the limit is changed. (c) does not protect against it: in host-dialed mode the token frpc and the bulk frpc share the serving relay's one ssh connection, and a rekey stops the whole connection. It is far below the 25 s heartbeat timeout, and no tunnel was lost. The default rekeys were started by sshd, so a client-side `RekeyLimit` cannot remove them; why a key exchange on a 2.3 ms path takes 100–150 ms was not isolated (Unknowns).
- **Concurrent transfers do not hurt (c).** With 1, 2 and 4 downloads at once, (c)'s gap p99 was 53.0, 53.2 and 53.2 ms in direct mode (the longest gap 53.4, 53.6 and 54.9 ms) and 54.1, 55.5 and 57.0 ms through real ssh (longest 55.3, 58.9 and 154.8 ms: the 154.8 ms is the stall at 1 GiB, above). (a)'s was 138.9, 128.6 and 74.2 ms in direct mode and 182.7, 153.4 and 102.7 ms through ssh. Every batch overlapped fully (the least overlap 1.000). The transfers share the link evenly: each takes k times the single transfer's 1.40 s (1.40, 2.79 and 5.60 s in direct mode), and the aggregate rate stays at 47–48 MB/s.
  - **Through real ssh, (c) fills the link with two bulk transfers and (a) does not:** the aggregate rate was 37.3 (one), 47.3 (two) and 47.4 MB/s (four) in (c), against 38.0, 38.8 and 38.4 MB/s in (a). Each of (c)'s bulk connections is its own relay stream with its own 256 KiB window; (a) puts every bulk stream into the one stream of the token frpc's connection, whose window and the relay's delay cap it at 38 MB/s.
- **Many Instances: a TLS handshake per bulk request costs almost nothing here, because a spare work connection is waiting for each request.** With 10 and 50 Instances, each with both frpc and one 256 KiB or 1 MiB download every 10 s:

  | Instances | request | TLS connections/s | ClientHellos/s | front cores (c) | front cores (a), the control | TTFB p50 / p99 / max, ms (c) | TTFB p50 / p99 / max, ms (a) |
  |---:|---|---:|---:|---:|---:|---|---|
  | 10 | 256 KiB | 0.99 | 0.99 | 0.024 | 0.022 | 5.2 / 6.6 / 6.9 | 5.5 / 8.7 / 8.7 |
  | 10 | 1 MiB | 0.99 | 0.99 | 0.032 | 0.026 | 5.2 / 7.0 / 9.0 | 5.0 / 8.5 / 8.6 |
  | 50 | 256 KiB | 5.01 | 5.01 | 0.106 | 0.090 | 5.2 / 7.2 / 12.0 | 5.4 / 8.7 / 10.3 |
  | 50 | 1 MiB | 5.01 | 5.01 | 0.112 | 0.107 | 5.1 / 6.9 / 8.3 | 5.0 / 7.9 / 9.6 |

  - One TLS connection per bulk request, one ClientHello each (5.01 a second at 50 Instances × 1 request per 10 s). At 50 Instances the front's processes took 0.106–0.112 cores with (c) and 0.090–0.107 with (a): at most 0.005–0.016 cores for 5 handshakes a second (an upper bound: the difference includes the second frps's own idle cost). The time to first byte did not grow (p50 5.1–5.2 ms, p99 6.9–7.2 ms in (c); 5.0–5.4 and 7.9–8.7 in (a)), and the front host was at most 10% busy (the median over the repetitions).
  - **The handshake is not on the request path here, which is why the time to first byte did not grow.** Layout (a), with no handshake, has the same p50 (5.0–5.5 ms), and the connection counts show that every request found its work connection already made: the bulk frpc held 2 established connections (its control and one spare; `conns_max_frpc-bulk` 20 at 10 Instances, 100–101 at 50), and the link carried 450 connections at 50 Instances, which is 50 token + 50 bulk control + 50 first spares + 300 replacement spares, one per request (90 = 10 + 10 + 10 + 60 at 10 Instances). frp reads `poolCount = 0` as 1, so frpc opens one spare work connection at login and frps asks for another each time it hands one out (HBEP-6, Heartbeats; frp v0.71.1 `pkg/config/v1/client.go:153`). A request 10 s after the last finds the spare ready. **Requests that arrive together do not all find one:** with 2 and 4 downloads started together, one spare is waiting and the others wait for the replacements, and in (c) their times to first byte rise with their place in the batch (table "Time to first byte of concurrent downloads"): at 4 at once, 6.0, 6.7, 7.2 and 8.4 ms in direct mode against (a)'s 4.9, 5.4, 5.7 and 6.1, and through ssh 9.1, 13.4, 16.0 and 22.3 ms against 8.3, 9.9, 10.1 and 11.5 (the p99 of the last over 15 batches: 28.6 against 17.2 ms, and 52.5 against 39.9). Part of (a)'s rise is the shared link; the difference is consistent with a handshake on the request path, and the run did not isolate it.
  - The token streams' gap p99 was 52.0–52.2 ms in (c) at both sizes; the longest gap 55.8–56.9 ms at 50 Instances, against 262 ms in (a) at 1 MiB (10 Instances: 217 ms), where the 1 MiB reply crosses the token frpc's session.
- **No tunnel was lost** in 119 runs (111 in the first hosted run, 8 in the rekey run): 0 drops, 0 frpc re-logins, 0 relay restarts, 0 heartbeat timeouts, and the ssh node left no process behind.

## Question

HBEP-6's open question 12 (as merged in #352) adopted layout (c) for bulk paths: a separate frpc with `tcpMux = false`, its own proxy, and a second frps behind the dial-home listener (S15, issue #336). S15 measured one bulk transfer at a time, with the link's per-connection queues, and left five items open. They block beta. The issue asks, for each, whether layout (c) still protects the token streams, and what it costs:
- a FIFO bottleneck shared by the two frpc's connections in direct mode (the token streams' inter-token gap at p99 during a 64 MiB download, against S15's 53 ms);
- the relay's real `ssh` and `srun` buffers in host-dialed mode (the same gap during the same download);
- several bulk transfers at once (1, 2 and 4, beside 8 token streams: the gap, and each transfer's time);
- many Instances, each with both frpc, behind one front, with a bulk request each per 10 s (the front host's CPU, the TLS handshakes per second, and the time to first byte of a bulk reply);
- the verdict: whether the open items close, and a proposed text.

## Method

S17 is S15's harness (`spikes/s15`, itself S14's), with four additions, one per item. Everything not named here is S15's: frps and frpc v0.71.1 with HBEP-6's settings, S1c's link emulator (rate shared by every connection, a bounded receive buffer, a one-way delay), the dial-home listener with internal/front's passthrough and the SNI route to the second frps, the engine double (50 ms before each token) with the bulk paths beside it, `hostdial.Owner`, `Job` and `Relay` unchanged, the driver's process records and `/proc` sampler, and the validity rule. Layouts (b) (frp's `bandwidthLimit`) and (d) (a relay of its own) are not run here: S15 closed them.

### The load and the validity rule

- **Token streams:** 8 streams, each one request of 2,000 tokens (100 s at 20 tokens/s, longer than a run), started over 2 s, so none is between requests while a transfer runs. In the many-Instances run, one stream per Instance.
- **Transfers:** after a 10 s reference window with no transfer, 5 batches of 64 MiB non-streamed downloads (a batch is 1, 2 or 4 downloads started together), each after a 4 s pause.
- **Gaps:** a gap belongs to a transfer when its interval overlaps any transfer's span (S1c's attribution), and to the reference when both tokens fall inside the reference window.
- **Valid (S15's rule, issue #413's):** every transfer completed with every byte at the receiver; through every transfer's whole span every one of the 8 streams had a request in flight that delivered tokens before and after it; in the reference window every stream received a token in every second. **A concurrent run is also valid only if every batch had all its transfers and they overlapped in time**: the share of the shortest transfer's span that all of a batch's transfers had in common is at least 80% (`analysis.batch_overlap`, with its tests); the runs report their least overlap. A run whose transfers did not overlap proves nothing about concurrency, and the rule refuses it.

### Item 1: the shared FIFO queue (direct mode)

S15's link gives every connection its own queue: a chunk waits for the link behind at most one chunk of each other connection. That is per-flow fairness by construction, and it flatters a layout that puts the bulk bytes on a connection of its own, as S15 noted. `s17link -queue fifo -queue-bytes N` is the other discipline: one byte queue per direction that every connection's chunks join in arrival order, with no per-flow fairness.
- **The queue holds at most N bytes,** a chunk in transmission included. A reader whose chunk does not fit waits for room, in arrival order among the waiting; that backpressure stands for a sender's loss and slowdown on loopback. A small flow's chunk then waits behind everything queued before it, about N over the rate when a bulk flow keeps the queue full.
- **N is not given by the issue, and the answer depends on it entirely,** so it is a parameter of the result: the bandwidth-delay product (48 MB/s × 2.3 ms = 110,400 B), 1 MiB, and frp's 6 MiB stream window, where the queue holds as much as the window and (c) should lose its advantage if the mechanism is right. Layouts (a) and (c) run on each queue and on the fair link as the control, in the same job, so every comparison is on one runner.
- **That the run was a FIFO** is shown twice: a unit test puts a saturating flow and a one-byte flow on one link and requires the small flow's delay to be about the queue's size over the rate in FIFO mode (132 ms against 131 ms expected) and about a chunk's time in fair mode (0.1–3 ms); and every run records the queue's largest occupancy and the time it was full (`link-queue.json`), which the tables show.

### Item 2: the relay through a real ssh and a real sshd (host-dialed mode)

S15's host-dialed path carried the relay's session over a TCP stand-in for ssh, `s15bridge`, and a node-side stand-in for "the login node's sshd, then srun, then the relay step". S17's `ssh` mode replaces both with the real ones, as `internal/hostdial/harness_test.go` has them:
- the relay owner's ssh client is the system `ssh` as the owner starts it (`sshrun.Relay`: one process per relay, `BatchMode`, `ServerAliveInterval` 10), to OpenSSH's `sshd` run as the user by `internal/rehearsal`'s login node, in inetd mode, with the client key forcing a command;
- the forced command hands the line to a run-mode fake Slurm (`internal/slurm/fakeslurm`), whose `srun --overlap` step runs the relay (`hostdial.Relay`) in a running job, and carries the session over the step's standard input and output in frames of at most 64 KiB (`internal/rehearsal/relay.go`): that is the stand-in for `srun`'s stdio pipe;
- each Allocation is a job on the fake whose batch process is the agent's job side (`hostdial.StartJob`); the link sits between the ssh client and the sshd.

```
direct       client --CONNECT--> frps (tcpmux) ==yamux in frpc's wss== link ==> listener (TLS) ==> frps
             (c) frps2 (tcpmux, mux off) == one wss per request == link ==> listener (SNI) ==> frps2
bridge       frpc --> job side --yamux stream--> relay step --TCP--> link --> bridge --pipes--> owner --> listener --> frps
ssh          frpc --> job side --yamux stream--> relay step --srun stdio frames--> forced command --> sshd
                 --ssh channel--> link --> ssh client --pipes--> owner --> listener --> frps
```

The bridge is run in the same job as the control, so the effect of the real buffers is a difference on one runner. Only the node binary links `internal/rehearsal` (`go list -deps -test` counts it in `sshnode` and in no other package of the spike); on the development VM this mode was a smoke run under a memory cap, from a tree that contains the rehearsal's sshd guard (#309).

### Item 3: concurrent transfers

The load client starts the batch's downloads together, each on a connection of its own (it dials a tunnel per request), and records each. The transfer's time is from its first to its last byte at the client; the batch's from the first start to the last end; the aggregate rate is the batch's bytes over its time. 1, 2 and 4 downloads, layouts (a) and (c), in direct mode (fair link) and through the real ssh. Each transfer's time to first byte is recorded too, and, per batch, the token streams' stalls near the batch's end with the bytes moved through the relay by then (the two tables added after the review of PR 416, computed from the committed `transfers.csv` and `streams.csv.xz`).

### Item 4: many Instances

N Instances (10 and 50), each with an engine double, a token frpc and, in layout (c), a bulk frpc with `tcpMux = false`, behind one frps, one second frps and one dial-home listener, in direct mode on the fair link. Layout (a), which carries the bulk request on the token frpc's session, is the control. Each Instance has one token stream and makes one 256 KiB or 1 MiB download every 10 s, the Instances' requests evenly staggered over the interval, for 60 s after the streams' ramp and a 4 s warm-up. (The issue gives the request's size nowhere: 64 MiB every 10 s from 50 Instances would be 320 MB/s through a 48 MB/s link. These sizes keep the link at most 5.3 MB/s busy, so the cost measured is the handshake's.)
- **TLS handshakes per second** come from counters in the dial-home listener: connections accepted and ClientHellos received, every 500 ms, over the window. With `tcpMux = false` every bulk request uses one work connection and frps has the frpc open the next, so one new TLS connection per request.
- **Time to first byte** is from before the request's dial to the first body byte at the client, so it includes the CONNECT through frps's tcpmux port, the request for a work connection and the engine's reply. In layout (c) the work connection is normally the spare that the second frpc opened after the previous request (frp reads `poolCount = 0` as 1 and frps replaces each spare it hands out), so a request made well after the last finds it ready and its time does not include a TLS handshake; requests made together do not (Item 3 and the concurrent table).
- **The front host's CPU** is frps, the second frps and the listener (and the relay owner in host-dialed runs), from `/proc` every 0.5 s over the window.
- **Valid** when every request completed in full and ended before its Instance's next was due, and every stream was streaming through all of them (`analysis.validity_many`).

### Item 5: the relay's ssh rekey (added after the review of PR 416)

The first run left an all-stream stall at 1 GiB through real ssh as "consistent with OpenSSH's default rekey, not confirmed". The rekey group tests it. The ssh-mode owner passes extra options to every relay client (`-ssh-opt`, as `ssh -o`) and can run each under `ssh -v` with its log in a private directory (`-ssh-debug-dir`; `-v` is put first so that it takes the log level before the relay's own `-o LogLevel=ERROR`); the driver parses each log (`analysis.parse_ssh_debug`) into the negotiated algorithms, the rekey limits, who started each key exchange and the bytes the connection had received at each rekey, and keeps nothing else of it. Three runs, layout (c), four downloads at once, through the real relay: the default with 40 MiB downloads (160 MiB a batch, so the 1 GiB point falls inside a batch), the same with `RekeyLimit 256M`, and the default with 64 MiB downloads for 2.25 GiB. A stall and a rekey are paired by the bytes moved (`analysis.all_stream_stalls`, `bytes_moved_at`, `match_rekeys`): the stall's from the transfers' 100 ms steps, the rekey's from the log.

### Ports and processes

Every process the harness starts binds a kernel-chosen port on 127.0.0.1 and reports it in a file (the link, the listener, the node's relay acceptor, the job sides); frps's configuration needs its three ports before it starts, so they are taken from the kernel by a bind and close right before the start, and frps is started again on fresh ports if it does not come up listening (the gap issue #384 describes; tolerable in a spike, and not hit). Every process is recorded by PID and start time and stopped by that PID; nothing is signalled by name.

## Results

All tables come from `results/<group>/rep{1,2,3}/`, rendered by `python3 summarize.py tables`. Every run finished (`status ok`); the 102 measured runs (96 of the first hosted run and 6 of the rekey run) were all valid (17 more are calibrations).

**How to read the tables:** gaps are pooled over the repetitions' valid runs (each row pools three runs, one per repetition); a "p99" rests on 3,300 or more gaps during transfers in every row of the first three groups. The reference gap's p99 is 51.4–52.1 ms in every row. Ranges in brackets are the minimum and maximum over the three repetitions. `python3 summarize.py verify` recomputes from the committed files every value the tables take from `runs.csv`: 1,788 CPU figures from the samples (within 0.00005 cores), 95 transfer summaries, 102 validity verdicts, 78 gap summaries and 24 many-Instances summaries, 24 TLS rates, 119 link connection counts, 36 queue statistics, 173 connection maxima and 6 ssh key-exchange counts; it finds no mismatch, and it exits 1 on one.

### Path rate (calibration)

One uncapped 64 MiB download in layout (a), with no stream, on the fair link, per mode and repetition; the median of its transfers at the receiver.

| mode | path rate, MB/s (reps) |
|---|---|
| direct | 47.2 / 47.0 / 47.0 / 47.0 / 47.1 / 47.2 |
| host-dialed | 31.4 / 31.6 / 34.3 |
| ssh | 37.6 / 38.0 / 38.4 / 38.0 / 37.0 / 36.9 / 37.9 / 36.7 |

### A shared FIFO bottleneck (direct mode)

The two frpc's connections through one emulated link of 48 MB/s and 2.3 ms, per queue discipline, during a 64 MiB download beside 8 token streams (5 downloads per run). Gaps pool the repetitions' valid runs: during a transfer (a gap overlapping a transfer's span) against the same run's reference window with no transfer. The queue columns are the link's statistics for the upstream direction, the one the downloads cross (node to front): the largest occupancy over the runs and the time the queue was full (less room than one chunk) over the run.

| layout | link queue | runs valid | reference gap p50 / p99 / max, ms | gap during the download p50 / p99 / max, ms | gaps during | rate, MB/s | time, s | queue max, KB | queue full, s |
|---|---|---:|---|---|---:|---:|---:|---:|---:|
| (a) one frpc | fair (per-connection turns) | 3/3 | 50.7 / 51.7 / 52.3 | 49.3 / 138.6 / 300.0 | 3576 | 47.2 (47.0–47.2) | 1.42 (1.42–1.43) | - | - |
| (a) one frpc | FIFO 110 KB (bandwidth-delay product) | 3/3 | 50.5 / 51.8 / 52.1 | 52.2 / 134.4 / 147.8 | 3493 | 47.3 (47.1–47.4) | 1.42 | 109 | 6.7 |
| (a) one frpc | FIFO 1 MiB | 3/3 | 50.6 / 51.9 / 52.2 | 51.5 / 154.0 / 307.0 | 3556 | 47.2 (47.2–47.5) | 1.42 (1.41–1.42) | 1049 | 5.7 (5.6–5.8) |
| (a) one frpc | FIFO 6 MiB (frp's window) | 3/3 | 50.6 / 52.0 / 52.8 | 65.2 / 139.3 / 152.6 | 3470 | 47.7 (47.7–47.8) | 1.41 | 5555 (5490–5584) | 0.0 |
| (c) a separate frpc | fair (per-connection turns) | 3/3 | 50.5 / 51.9 / 52.2 | 50.6 / 52.9 / 53.4 | 3463 | 47.8 (47.8–47.9) | 1.40 | - | - |
| (c) a separate frpc | FIFO 110 KB (bandwidth-delay product) | 3/3 | 50.9 / 52.0 / 52.2 | 50.6 / 52.7 / 54.3 | 3448 | 47.9 | 1.40 | 103 | 6.8 |
| (c) a separate frpc | FIFO 1 MiB | 3/3 | 50.5 / 52.0 / 52.2 | 50.6 / 72.0 / 74.5 | 3455 | 47.9 | 1.40 | 1049 | 6.7 (6.6–6.7) |
| (c) a separate frpc | FIFO 6 MiB (frp's window) | 3/3 | 50.7 / 52.0 / 52.4 | 50.5 / 180.5 / 182.3 | 3388 | 47.9 | 1.40 | 6291 | 6.1 (6.0–6.1) |

### Real relay buffers (host-dialed mode)

The relay through S15's bridge (a TCP stand-in for ssh, the control) and through a real `ssh` client, a real sshd and the fake Slurm's srun standard streams, during a 64 MiB download beside 8 token streams. Front CPU is frps, the second frps, the listener, the relay owner and the ssh clients (or the bridges), in cores, over the transfers' phase; `ssh` is the ssh clients alone.

| layout | relay | runs valid | reference gap p50 / p99 / max, ms | gap during the download p50 / p99 / max, ms | gaps during | rate, MB/s | time, s | front cores, transfers' phase | ssh or bridge cores |
|---|---|---:|---|---|---:|---:|---:|---:|---:|
| (a) one frpc | S15's bridge | 3/3 | 50.7 / 51.9 / 53.2 | 23.5 / 211.8 / 230.9 | 5072 | 31.9 (30.8–34.1) | 2.11 (1.97–2.18) | 0.205 (0.107–0.208) | 0.029 (0.015–0.029) |
| (a) one frpc | real ssh, sshd and srun streams | 3/3 | 50.5 / 51.9 / 53.3 | 53.5 / 180.4 / 197.7 | 4349 | 37.7 (37.6–38.4) | 1.78 (1.75–1.79) | 0.233 (0.129–0.240) | 0.048 (0.024–0.049) |
| (c) a separate frpc | S15's bridge | 3/3 | 50.7 / 52.0 / 53.0 | 50.6 / 54.2 / 56.2 | 4844 | 34.6 (33.3–36.7) | 1.94 (1.83–2.02) | 0.198 (0.099–0.199) | 0.029 (0.015–0.030) |
| (c) a separate frpc | real ssh, sshd and srun streams | 3/3 | 50.5 / 51.9 / 53.1 | 50.5 / 54.2 / 55.9 | 4520 | 37.7 (36.7–38.6) | 1.78 (1.74–1.83) | 0.223 (0.126–0.230) | 0.046 (0.025–0.047) |

### Concurrent bulk transfers

1, 2 and 4 downloads of 64 MiB started together beside 8 token streams (5 batches per run). Every batch's transfers overlapped in time: the valid runs' least overlap, the share of the shortest transfer's span that all of a batch's transfers had in common, is in the last column. Each transfer's time is from its first to its last byte at the client; the batch's is from the first start to the last end.

| mode | layout | transfers at once | runs valid | reference gap p50 / p99 / max, ms | gap during the transfers p50 / p99 / max, ms | gaps during | each transfer's time, s | batch time, s | aggregate rate, MB/s | least overlap |
|---|---|---:|---:|---|---|---:|---:|---:|---:|---:|
| direct | (a) one frpc | 1 | 3/3 | 50.4 / 51.6 / 52.2 | 45.5 / 138.9 / 300.6 | 3592 | 1.43 (1.42–1.43) | 1.43 (1.42–1.43) | 46.9 (46.8–47.1) | 1.000 |
| direct | (a) one frpc | 2 | 3/3 | 50.4 / 51.4 / 51.7 | 58.6 / 128.6 / 325.5 | 6809 | 2.81 (2.80–2.81) | 2.82 | 47.6 | 1.000 |
| direct | (a) one frpc | 4 | 3/3 | 50.5 / 52.0 / 52.6 | 59.0 / 74.2 / 145.4 | 13428 | 5.61 (5.60–5.62) | 5.63 | 47.7 | 1.000 |
| direct | (c) a separate frpc | 1 | 3/3 | 50.5 / 51.6 / 52.1 | 50.6 / 53.0 / 53.4 | 3458 | 1.40 | 1.41 | 47.7 | 1.000 |
| direct | (c) a separate frpc | 2 | 3/3 | 50.7 / 51.7 / 52.1 | 50.6 / 53.2 / 53.6 | 6796 | 2.79 (2.79–2.80) | 2.81 | 47.7 | 1.000 |
| direct | (c) a separate frpc | 4 | 3/3 | 50.4 / 51.6 / 53.1 | 50.6 / 53.2 / 54.9 | 13444 | 5.60 | 5.62 | 47.8 | 1.000 |
| ssh | (a) one frpc | 1 | 3/3 | 50.6 / 51.8 / 54.9 | 48.5 / 182.7 / 210.2 | 4327 | 1.76 (1.73–1.79) | 1.77 (1.74–1.79) | 38.0 (37.4–38.6) | 1.000 |
| ssh | (a) one frpc | 2 | 3/3 | 50.6 / 51.7 / 53.5 | 44.1 / 153.4 / 212.9 | 8282 | 3.44 (3.42–3.44) | 3.46 (3.45–3.46) | 38.8 (38.7–38.9) | 1.000 |
| ssh | (a) one frpc | 4 | 3/3 | 50.5 / 51.5 / 52.6 | 44.9 / 102.7 / 273.2 | 16693 | 6.96 (6.94–6.98) | 6.98 (6.98–7.05) | 38.4 (38.1–38.5) | 1.000 |
| ssh | (c) a separate frpc | 1 | 3/3 | 50.6 / 52.1 / 53.3 | 50.5 / 54.1 / 55.3 | 4456 | 1.79 (1.74–1.80) | 1.80 (1.75–1.81) | 37.3 (37.1–38.3) | 1.000 |
| ssh | (c) a separate frpc | 2 | 3/3 | 50.6 / 51.9 / 53.0 | 50.3 / 55.5 / 58.9 | 6916 | 2.81 | 2.84 (2.83–2.90) | 47.3 (46.3–47.5) | 1.000 |
| ssh | (c) a separate frpc | 4 | 3/3 | 50.5 / 51.8 / 53.0 | 50.3 / 57.0 / 154.8 | 13648 | 5.59 (5.58–5.60) | 5.67 (5.65–5.67) | 47.4 (47.3–47.6) | 1.000 |

#### Stalls at the end of a batch

A stall of the token streams near a batch's end: at least 6 of the 8 streams with a gap of more than 100 ms inside the batch whose end falls in the batch's last 400 ms. Per row, over the three repetitions: the batches with such a stall of the 15 (5 per run), which batches they were (0 is the first of 5), the bytes the transfers had moved through the relay's one connection by the end of those batches, the stall's gap per stream (smallest–largest) and how long before the batch's end the stall ended. Layout (a)'s own head-of-line stalls (S15: its 6 MiB window) recur through a transfer, so its rows at one transfer at a time, in either mode, trip the rule at any batch and are not the event; the rows that carry the finding are (c)'s, which has no such stall, and (a)'s at four at once, where direct mode shows none.

| mode | layout | transfers at once | batches with a stall | which batches | bytes moved by then, GiB | gap per stream, ms | ended before the batch's end, ms |
|---|---|---:|---:|---|---|---|---|
| direct | (a) one frpc | 1 | 13 of 15 | 0, 1, 2, 3, 4 | 0.062, 0.125, 0.188, 0.250, 0.312 | 100–257 | 21–360 |
| direct | (a) one frpc | 2 | 0 of 15 | - | - (all five batches moved 0.62 GiB in all) | - | - |
| direct | (a) one frpc | 4 | 0 of 15 | - | - (all five batches moved 1.25 GiB in all) | - | - |
| direct | (c) a separate frpc | 1 | 0 of 15 | - | - (all five batches moved 0.31 GiB in all) | - | - |
| direct | (c) a separate frpc | 2 | 0 of 15 | - | - (all five batches moved 0.62 GiB in all) | - | - |
| direct | (c) a separate frpc | 4 | 0 of 15 | - | - (all five batches moved 1.25 GiB in all) | - | - |
| ssh | (a) one frpc | 1 | 5 of 15 | 0, 1, 3, 4 | 0.062, 0.125, 0.250, 0.312 | 114–209 | 20–187 |
| ssh | (a) one frpc | 2 | 0 of 15 | - | - (all five batches moved 0.62 GiB in all) | - | - |
| ssh | (a) one frpc | 4 | 3 of 15 | 3 | 1.000 | 119–179 | 182–230 |
| ssh | (c) a separate frpc | 1 | 0 of 15 | - | - (all five batches moved 0.31 GiB in all) | - | - |
| ssh | (c) a separate frpc | 2 | 0 of 15 | - | - (all five batches moved 0.62 GiB in all) | - | - |
| ssh | (c) a separate frpc | 4 | 3 of 15 | 3 | 1.000 | 111–154 | 128–158 |

#### Time to first byte of concurrent downloads

A download's time to first byte (from before its dial to its first body byte at the client), by its place in its batch: the median over the three repetitions' batches of the first to answer, the second, and so on, and the p99 of the last to answer. Layout (c)'s transfers each need a work connection from the second frpc: one spare is waiting for the first, and the others wait for a spare that frps asks for after it hands one out.

| mode | layout | transfers at once | first-byte time of each, in the order they answered, p50, ms | p99 of the last, ms |
|---|---|---:|---|---:|
| direct | (a) one frpc | 2 | 4.9 / 5.4 | 13.4 |
| direct | (a) one frpc | 4 | 4.9 / 5.4 / 5.7 / 6.1 | 17.2 |
| direct | (c) a separate frpc | 2 | 5.6 / 6.3 | 23.6 |
| direct | (c) a separate frpc | 4 | 6.0 / 6.7 / 7.2 / 8.4 | 28.6 |
| ssh | (a) one frpc | 2 | 6.9 / 9.2 | 28.8 |
| ssh | (a) one frpc | 4 | 8.3 / 9.9 / 10.1 / 11.5 | 39.9 |
| ssh | (c) a separate frpc | 2 | 8.0 / 10.1 | 28.6 |
| ssh | (c) a separate frpc | 4 | 9.1 / 13.4 / 16.0 / 22.3 | 52.5 |

### Many Instances behind one front (direct mode)

N Instances, each with a token stream and one bulk download of the size shown every 10 s (the Instances' requests staggered evenly over the interval), over a 60 s window. Layout (c) gives each Instance a second frpc with `tcpMux = false` (a new TLS connection through the dial-home listener for every bulk request); layout (a), the control, carries the bulk request on the token frpc's session. Front CPU is frps, the second frps and the listener, in cores, over the window (the host's busy share in the last column). TLS rates are from the listener's counters: connections accepted and ClientHellos received per second over the window. The time to first byte runs from before the request's dial to the first body byte.

| layout | Instances | request | runs valid | front cores | TLS connections/s | ClientHellos/s | TTFB p50 / p99 / max, ms | request time p50 / p99, ms | token gap p50 / p99 / max, ms | host busy |
|---|---:|---:|---:|---:|---:|---:|---|---|---|---:|
| (a) one frpc | 10 | 256 KiB | 3/3 | 0.022 (0.022–0.048) | 0.00 | 0.00 | 5.5 / 8.7 / 8.7 | 9.3 / 10.7 | 50.6 / 51.9 / 54.6 | 0.02 (0.02–0.04) |
| (a) one frpc | 10 | 1 MiB | 3/3 | 0.026 (0.025–0.059) | 0.00 | 0.00 | 5.0 / 8.5 / 8.6 | 25.7 / 27.5 | 50.7 / 52.0 / 217.3 | 0.03 (0.02–0.04) |
| (a) one frpc | 50 | 256 KiB | 3/3 | 0.090 (0.079–0.153) | 0.00 | 0.00 | 5.4 / 8.7 / 10.3 | 9.2 / 11.1 | 50.6 / 52.3 / 57.0 | 0.09 (0.08–0.14) |
| (a) one frpc | 50 | 1 MiB | 3/3 | 0.107 (0.106–0.212) | 0.00 | 0.00 | 5.0 / 7.9 / 9.6 | 25.7 / 31.1 | 50.6 / 52.3 / 262.0 | 0.09 (0.09–0.17) |
| (c) a second frpc each | 10 | 256 KiB | 3/3 | 0.024 (0.022–0.050) | 0.99 (0.99–1.01) | 0.99 (0.99–1.01) | 5.2 / 6.6 / 6.9 | 9.0 / 10.4 | 50.8 / 52.0 / 53.3 | 0.03 (0.02–0.04) |
| (c) a second frpc each | 10 | 1 MiB | 3/3 | 0.032 (0.028–0.058) | 0.99 (0.99–1.01) | 0.99 (0.99–1.01) | 5.2 / 7.0 / 9.0 | 25.7 / 27.1 | 50.6 / 52.0 / 53.8 | 0.03 (0.03–0.05) |
| (c) a second frpc each | 50 | 256 KiB | 3/3 | 0.106 (0.089–0.163) | 5.01 | 5.01 | 5.2 / 7.2 / 12.0 | 9.0 / 11.3 | 50.7 / 52.1 / 56.9 | 0.10 (0.09–0.15) |
| (c) a second frpc each | 50 | 1 MiB | 3/3 | 0.112 (0.107–0.209) | 5.01 | 5.01 | 5.1 / 6.9 / 8.3 | 25.7 / 28.1 | 50.6 / 52.2 / 55.8 | 0.10 (0.10–0.17) |

### The relay's ssh and its rekeys (layout (c), four downloads at once, through real ssh)

The review of PR 416 found an all-stream stall at 1 GiB in the ssh runs and read it as OpenSSH's default rekey. These runs log it: each relay client ran with `ssh -v`, and its debug log's key exchanges and negotiated algorithms are parsed (nothing else of the log is kept: it names the host and the user). *default* is the relay's ssh as `sshrun` starts it; *RekeyLimit 256M* adds `-o RekeyLimit=256M`. Downloads of 40 MiB (160 MiB a batch, so the rekey points fall inside batches, not at their ends) or 64 MiB, nine batches of four.

| run | repetition | OpenSSH (client / server) | key exchange | host key | cipher, MAC (receive / send) | rekey limit | key exchanges on the serving relay | started by | bytes the connection had received at each rekey, MiB |
|---|---:|---|---|---|---|---|---:|---|---|
| default-40MiB | 1 | OpenSSH_9.6p1 / OpenSSH_9.6p1 | sntrup761x25519-sha512@openssh.com | ssh-ed25519 | chacha20-poly1305@openssh.com, <implicit> / chacha20-poly1305@openssh.com, <implicit> | 134217728 blocks of 8 B = 1 GiB | 2 | client 1, server 1 | 1024 |
| default-40MiB | 2 | OpenSSH_9.6p1 / OpenSSH_9.6p1 | sntrup761x25519-sha512@openssh.com | ssh-ed25519 | chacha20-poly1305@openssh.com, <implicit> / chacha20-poly1305@openssh.com, <implicit> | 134217728 blocks of 8 B = 1 GiB | 2 | client 1, server 1 | 1024 |
| default-64MiB | 1 | OpenSSH_9.6p1 / OpenSSH_9.6p1 | sntrup761x25519-sha512@openssh.com | ssh-ed25519 | chacha20-poly1305@openssh.com, <implicit> / chacha20-poly1305@openssh.com, <implicit> | 134217728 blocks of 8 B = 1 GiB | 3 | client 1, server 2 | 1024, 2048 |
| default-64MiB | 2 | OpenSSH_9.6p1 / OpenSSH_9.6p1 | sntrup761x25519-sha512@openssh.com | ssh-ed25519 | chacha20-poly1305@openssh.com, <implicit> / chacha20-poly1305@openssh.com, <implicit> | 134217728 blocks of 8 B = 1 GiB | 3 | client 1, server 2 | 1024, 2048 |
| rekey256M-40MiB | 1 | OpenSSH_9.6p1 / OpenSSH_9.6p1 | sntrup761x25519-sha512@openssh.com | ssh-ed25519 | chacha20-poly1305@openssh.com, <implicit> / chacha20-poly1305@openssh.com, <implicit> | 33554432 blocks of 8 B = 256 MiB | 6 | client 6 | 257, 514, 770, 1027, 1284 |
| rekey256M-40MiB | 2 | OpenSSH_9.6p1 / OpenSSH_9.6p1 | sntrup761x25519-sha512@openssh.com | ssh-ed25519 | chacha20-poly1305@openssh.com, <implicit> / chacha20-poly1305@openssh.com, <implicit> | 33554432 blocks of 8 B = 256 MiB | 6 | client 6 | 257, 514, 771, 1028, 1284 |

### All-stream stalls against the rekeys

An all-stream stall is a gap of more than 100 ms in at least 6 of the 8 token streams, starting within 0.3 s of one another, while the transfers ran. Its bytes are the transfers' payload at the client when it began (the steps give a range, shown as the midpoint); the rekey's are what the ssh connection had received when its new keys took effect, which runs ahead of the client by what sits in the buffers between (frp's 6 MiB window, the relay's, the sockets'). A stall matches a rekey when its bytes lie from 16 MiB below the rekey's to 2 MiB above (`analysis.match_rekeys`).

The first columns use the rule as set before the runs (a gap of more than 100 ms); the last two repeat the match with a gap of more than 80 ms, to show how near the threshold the stalls are and that a looser one finds no stall that is not a rekey.

| run | repetition | valid | rekeys | all-stream stalls | rekeys with a stall | stalls with no rekey | rekey at MiB (connection) → stall at MiB (payload), gap per stream ms | rekeys with a stall, over 80 ms | stalls with no rekey, over 80 ms |
|---|---:|---|---:|---:|---:|---:|---|---:|---:|
| default-40MiB | 1 | True | 1 | 1 | 1 | 0 | 1024 → 1013, 122–135 | 1 | 0 |
| default-40MiB | 2 | True | 1 | 1 | 1 | 0 | 1024 → 1013, 130–154 | 1 | 0 |
| default-64MiB | 1 | True | 2 | 2 | 2 | 0 | 1024 → 1015, 121–122; 2048 → 2031, 134–134 | 2 | 0 |
| default-64MiB | 2 | True | 2 | 2 | 2 | 0 | 1024 → 1016, 133–154; 2048 → 2034, 136–153 | 2 | 0 |
| rekey256M-40MiB | 1 | True | 5 | 4 | 4 | 0 | 257 → 253, 116–133; 770 → 761, 131–132; 1027 → 1019, 104–104; 1284 → 1273, 101–101 | 5 | 0 |
| rekey256M-40MiB | 2 | True | 5 | 5 | 5 | 0 | 257 → 253, 107–121; 514 → 508, 115–140; 771 → 764, 122–149; 1028 → 1017, 109–152; 1284 → 1273, 115–143 | 5 | 0 |

### Tunnel losses

Summed over all 119 runs: frps's lists (a proxy not online or registered again), frpc logins beyond the first (every frpc), relay sessions beyond the first, and heartbeat-timeout lines.

| drops | frpc re-logins | relay restarts | heartbeat-timeout lines | unreadable lists | runs not ok | processes the ssh node left |
|---:|---:|---:|---:|---:|---:|---:|
| 0 | 0 | 0 | 0 | 0 | 0 | 0 |

### Runners

- rep 1: AMD EPYC 7763 64-Core Processor, 4 CPUs, 15983 MB, Linux 7.0.0-1017-azure
- rep 1: AMD EPYC 9V45 96-Core Processor, 4 CPUs, 15983 MB, Linux 7.0.0-1017-azure
- rep 1: AMD EPYC 9V74 80-Core Processor, 4 CPUs, 15983 MB, Linux 7.0.0-1017-azure
- rep 2: AMD EPYC 7763 64-Core Processor, 4 CPUs, 15983 MB, Linux 7.0.0-1017-azure
- rep 2: AMD EPYC 9V74 80-Core Processor, 4 CPUs, 15983 MB, Linux 7.0.0-1017-azure
- rep 3: AMD EPYC 7763 64-Core Processor, 4 CPUs, 15983 MB, Linux 7.0.0-1017-azure
- rep 3: AMD EPYC 9V45 96-Core Processor, 4 CPUs, 15983 MB, Linux 7.0.0-1017-azure
- rep 3: Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz, 4 CPUs, 15982 MB, Linux 7.0.0-1017-azure

## Verdict

**The open items close.** Layout (c) still protects the token streams when its connections share a FIFO bottleneck, through the real ssh and srun buffers, with several transfers at once, and with 50 Instances making bulk requests every 10 s; what each costs is below. There are two conditions: the bottleneck's queue depth, which no spike here can say, and, in host-dialed mode, a stall of every stream on a relay's ssh connection about once per GiB moved.

- **Adopt (c) as HBEP-6 has it, and record two conditions. First, the shared bottleneck's buffer.** Behind a FIFO queue every flow pays the queue's drain time, so (c)'s stall is about the queue's size over the path rate, and it stays below (a)'s while the queue is under about 4 MiB at 48 MB/s (about 85 ms of buffering). The real path's buffer is not known. It can be read from a loaded round trip: the delay a saturating download adds to the path's idle round trip is the buffer's drain time. If it is near or above 85 ms, (c) is no better than (a). Then no remedy is measured: frp's `bandwidthLimit` is not one (S15: its first second goes at line rate, so it cannot keep a queue of several MiB from filling), and a limiter without that burst, holding the bulk frpc below the path's rate so that it cannot fill the queue, is what would be tested.
- **Second condition: every stream on a relay stalls for about 100–180 ms once per GiB moved through its ssh connection, and the cause is OpenSSH's rekey (confirmed).** The first run saw it in all six runs that moved 1 GiB through the real ssh path, in (c) as in (a); the second run's logs show the rekey at 1024 MiB (and 2048 MiB), each with a stall, and `RekeyLimit 256M` moves the stalls to every 256 MiB, with no stall that is not a rekey. It stops the whole connection, the token streams and the bulk streams on it, so (c) cannot protect against it, since the token and bulk frpc share the serving relay's ssh connection in host-dialed mode. It is one event per GiB moved, far below the 25 s heartbeat timeout, and it caused no tunnel loss. The default rekeys were started by sshd (its own limit for chacha20-poly1305, 1 GiB), so setting `RekeyLimit` on the relay's ssh client cannot remove them; what would (a cipher with a larger default limit, ssh_config(5) says up to 4G, or a longer limit on the login node's sshd, which is not ours) was not tested and is for a separate issue about the relay's ssh arguments.
- **Real ssh and srun buffers need no change to the design,** beyond that second condition. In the runs of that group, which moved 320 MiB through each relay (below the 1 GiB point of the second condition), they add no stall to (c) (54.2 ms through both) and take less of a bulk stream's rate than the stand-in did. They keep S15's constraint that the relay's per-stream window stays at yamux's 256 KiB, and show its price on this path: 37–38 MB/s for one bulk stream. Two bulk transfers fill a 48 MB/s link, which (c) allows and (a) does not.
- **Concurrent bulk transfers need no limit on their number** to protect the token streams: 4 at once left (c)'s gap p99 at 53 ms in direct mode and 57 ms through ssh (the stall at 1 GiB through ssh is the second condition, and comes from the bytes moved, not from the transfers' number). In direct mode each transfer's time grows in proportion, since they share the link; through ssh, the batches of two and four took 2.81 and 5.59 s against one transfer's 1.79 s, because one stream is window-bound at 38 MB/s and several fill the link.
- **A handshake per bulk request is cheap at this scale:** at most 0.005–0.016 cores (an upper bound that includes the second frps's own idle cost) and no added time to first byte for requests that find a spare work connection waiting, which one request per Instance every 10 s does. Requests from one Instance that arrive together do not all find one: the later downloads of a batch started together took 0.9–2.3 ms (direct) and 0.9–10.7 ms (through ssh) longer to first byte in (c) than in (a), at the median (through ssh 0.9 ms for the second of two, and 3.5, 5.9 and 10.7 ms for the second to the fourth of four). It does not bound the number of Instances. The second frps was not a bottleneck either (the front's processes took about 0.11 cores in all at 50 Instances). What this does not cover is a path with a longer round trip, where a handshake on the request path costs several round trips before the first byte (Unknowns).

## Proposed HBEP-6 text for open question 12

For the science agent to amend. It replaces the item's "Still open" bullet and answers it.

> 12. **Bulk endpoints off the token tunnel.** **Answered** (spikes S15, issue #336, for one transfer, and S17, issue #413, for the rest): bulk paths go to a separate frpc with `transport.tcpMux = false`, with its own tcpmux proxy for the Instance, a second frps with `tcpMux = false`, the dial-home listener routing that frpc's websocket to it by TLS server name, and a second `CONNECT` target in the front's transport (S15). S17 measured the items S15 left open, on loopback at 48 MB/s and a 2.3 ms round trip, with 8 token streams of 20 tokens/s beside a 64 MiB download; the gap p99 below is the streams' inter-token gap, against 52 ms with no transfer:
>     - **A FIFO bottleneck shared by the two frpc's connections** adds the queue's drain time to the gap: 52.7 ms at a 110 KB queue (the bandwidth-delay product), 72.0 ms at 1 MiB and 180.5 ms at 6 MiB, against one frpc's 134–154 ms. So (c) protects the token streams while the bottleneck's queue is under about 4 MiB (about 85 ms), and not beyond; at 6 MiB it is worse than one frpc, whose bulk flow frp's 6 MiB stream window holds out of the queue. The path's real buffer is to be read from a loaded round trip before beta; if it is deep, no remedy is measured (S15 measured frp's `bandwidthLimit` out: its first second goes at line rate).
>     - **The relay's real ssh and srun buffers** add no head-of-line stall in a run of 320 MiB (the second caveat, below, appears at 1 GiB): 54.2 ms through a real ssh client, sshd and srun's standard streams, as through the TCP stand-in. A bulk stream through the relay is bounded by its 256 KiB window over the path's delay: 37–38 MB/s on loopback through real ssh (an effective round trip of 7 ms). The window must stay at yamux's default (S15), and the path's delay then sets the bulk rate.
>     - **Concurrent bulk transfers** (1, 2 and 4 at once) leave the gap at 53 ms in direct mode and at most 57 ms through ssh, and share the link evenly. Through ssh, two bulk work connections fill the link and one frpc's session does not: each is its own relay stream.
>     - **Many Instances:** with 10 and 50 Instances, each with both frpc, a bulk request each per 10 s costs one TLS connection and one ClientHello (5 a second at 50) and at most 0.005–0.016 cores of the front's processes, and **adds no time to first byte when the request finds a spare work connection waiting** (p99 about 7 ms): frp reads `poolCount = 0` as 1, so frpc keeps one spare and frps replaces each it hands out. Requests from one Instance that arrive together do not all find one: the handshake is then on the request path, and the later requests of a batch started together took 0.9–2.3 ms (direct) and 0.9–10.7 ms (through ssh) longer to first byte than on one frpc, at the median, at this path's 2.3 ms round trip (through ssh 0.9 ms for the second of two; 3.5, 5.9 and 10.7 ms for the second to the fourth of four). Each such request costs the handshake's round trips, so a longer path costs more.
>     - **A second caveat, host-dialed mode: a relay's ssh connection stalls all its streams at every rekey, by default once per GiB** (confirmed, S17's rekey run). OpenSSH 9.6p1's default for chacha20-poly1305 is a rekey after 134,217,728 blocks of 8 B (1 GiB); the relay's ssh sets no `RekeyLimit`. Each rekey (sshd started all six default ones) stalled every token stream for 121–154 ms at the 1 GiB and 2 GiB points, in (c) as in (a), because the token and bulk frpc share the serving relay's ssh connection; with `RekeyLimit 256M` the stalls came every 256 MiB, and no stall occurred without a rekey. A client-side `RekeyLimit` cannot remove the sshd-started one; a cipher or sshd setting that would is untested.
>     - **Still open:** a path with a longer round trip (a handshake on the request path costs several round trips; a relay stream's rate is 256 KiB over the delay), loss on the path, srun's own stdio buffering (S17 used the fake Slurm's), a way to remove or shorten the relay ssh's rekey stall (a cipher with a larger default limit, or sshd's limit), and many Instances in host-dialed mode.

## What the results mean for the other HBEPs

- **HBEP-6:** the text above, with its two caveats (the shared queue's depth, and the relay ssh's stall about once per GiB in host-dialed mode). The second frps and the listener's routing stay the cost of (c); S17 found no further cost at 50 Instances, one request per Instance at a time.
- **HBEP-4 and HBEP-7:** none. The relay count per Allocation (2) is unchanged by the verdict.

## Unit tests of the harness

`go test ./...` and `python3 -m unittest discover -s tests` in this directory; the hosted jobs ran them before measuring, except the tests of the two rules added after the review of PR 416 (the late-batch stall and the per-batch time to first byte, and for the rekey group the ssh log parser, the bytes moved at a time, the all-stream stall detector and the rekey pairing), which read the committed results.
- **The link's FIFO discipline** (`cmd/s17link`): a saturating flow and a one-byte flow share the link, and the small flow's delay is about the queue's size over the rate in FIFO mode (132, 134 and 134 ms against 131 ms expected) and a chunk's time in fair mode (0.1–3.3 ms), which is the test that a "FIFO" run is not per-flow fair; two flows pass intact within the queue's size at no more than the link's rate, with the queue's statistics showing it full; the flags refuse a queue that cannot work.
- **The result rules** (`analysis.py`, `tests/test_analysis.py`): S15's, and the overlap rule (a batch of transfers that do not overlap, overlap below 80% of the shortest span, a batch with a missing transfer), the many-Instances validity (a failed or short request, a request longer than the interval, a stream that ended before the last request), the time to first byte and the TLS rates; and, added after the review, the stalls near a batch's end (all streams, a gap over 100 ms, in the batch's last 400 ms, with the bytes moved by then) and each batch's times to first byte in order.
- **Mutants of S17's own rules,** 33 applied against passing baselines: the FIFO queue admitting past its size, falling back to per-connection turns, not waiting out a chunk's transmission, not accounting its time full or its largest occupancy, passing a flow's end on before its chunks; and the overlap threshold, the batch-size check, the interval check, the stream check, the handshake and first-byte computations, and, after the review, the late-stall window, its minimum of streams, its gap threshold, its byte count and the first-byte order, and, for the rekey group, the ssh log parser (the two directions' algorithms swapped, the rekey's received and sent bytes swapped, the exchange's initiator, the exchange count, the channel read from any line), the bytes moved at a time (the upper bound, the first byte), the all-stream stall detector (the minimum of streams, the transfers' window, the separation of events) and the rekey-to-stall pairing (the window's two sides, a stall paired twice, the nearest stall). 32 were killed, each by the test that names its rule. One survives: releasing a chunk from the queue's count when its transmission starts instead of when it ends changes the queue by one 16 KiB chunk, which the discriminator's tolerance (half the queue's drain time) does not see. At the smallest depth, 110 KB, that is 15% of the queue.

## Environment

- **The runs:** workflow run 43112185203 on draft PR #414 (a throwaway whose head merges this spike's commit `27c2055c`, the harness that ran, with a one-off workflow; never merged), on 11 Nov 2026, 22:29–22:44 UTC. Twelve jobs, one per group and repetition, 6–15 minutes each. Commits after `27c2055c` change only this README and `results/`. The raw files stay in the run's artifacts until 25 Nov 2026; the committed `streams.csv.xz`, `transfers.csv`, `bulk.csv`, `links.csv`, `queues.csv`, `listener.csv.xz`, `conns.csv.xz`, `samples.csv.xz` and `host.csv.xz` keep every measured value, and `python3 summarize.py verify` recomputes the tables from them.
- **The rekey run:** workflow run 43124479105 on #414 (its head merges `da126aaa`, the harness that ran, and copies `s17-rekey.yml` into `.github/workflows`), 12 Nov 2026, 00:03–00:11 UTC: two jobs on fresh runners (AMD EPYC 9V74 and 7763), each of three runs and a calibration. The artifacts (14 days of retention, until 26 Nov 2026) hold the results directory, which keeps the parsed ssh facts (`ssh.csv`) and no debug log: the logs name the host and the user, stayed on the runner and were not uploaded. The relay's ssh was OpenSSH 9.6p1 (Ubuntu 3ubuntu13.20), client and sshd, from the runner image.
- **Runners:** fourteen fresh GitHub-hosted `ubuntu-24.04` runners (twelve for the first run, two for the rekey run), each with 4 CPUs, 15,983 MB and Linux 7.0.0-1017-azure, on AMD EPYC 7763, 9V45 and 9V74 and one Intel Xeon Platinum 8370C (the table of runners follows the results). They had Go from `go.mod`, Python 3.12.3, open files raised to the hard limit, and OpenSSH's `sshd` (installed only if missing, never started as a service, as `ci.yml` does).
- **The link:** 48,000,000 B/s each way, 1.15 ms each way, a 256 KiB receive buffer, 16 KiB chunks and 2 ms of catch-up slack. Direct mode measured 47.0–47.2 MB/s at the receiver in calibration; host-dialed through the bridge 31.4–34.3 MB/s; through real ssh 36.7–38.4 MB/s.
- **Priority:** as S15: frps, the second frps, the listener, the owner (and the ssh clients it starts), the link and the client at nice 0; frpc, the node and, in `ssh` mode, the fake cluster's processes at nice 10.
- **The development VM** ran only builds, the harness's tests, mutants and smoke runs, at nice 10 under `systemd-run --user --scope -p MemoryMax=4G` (8G once, for a 50-Instance smoke run of 100 frpc), with a 1-minute load of at most 3 in the smoke runs of the first harness and up to 12 in some of the rekey group's (smoke runs, never results). Real sshd ran there only through `internal/rehearsal`'s guarded login node, from a tree that contains #309; `go list -deps -test` counts `internal/rehearsal` in `sshnode` and in no other package of the spike. Nothing from the VM is in `results/`.

## Provenance

| Component | Version and pin | Project and licence | Use |
|---|---|---|---|
| frp (frps, frpc) | v0.71.1 linux_amd64; tarball sha256 `1d987521…f803` (S1c's pin); frps `23227629…6ab3` and frpc `35c6054d…c28b` from `internal/launcher/frp-pins.txt` | [fatedier/frp](https://github.com/fatedier/frp), Apache-2.0 | the tunnels |
| fatedier/yamux | v0.2.0, through `internal/hostdial` | [fatedier/yamux](https://github.com/fatedier/yamux), MPL-2.0 | the relay sessions |
| OpenSSH (`ssh`, `sshd`, `ssh-keygen`) | the runner image's package, 9.6p1 Ubuntu-3ubuntu13.20, `/usr/sbin/sshd`, or the VM's | [OpenSSH](https://www.openssh.com/), BSD | the real relay path |
| Spike S15's harness | `spikes/s15` at `eaa225ed` (itself S14's, `3f863bad`): the link, the listener, the node, the load client, the driver, `analysis.py`, `summarize.py` | this project | adapted in `cmd/s17*`, `node/`, `s17.py`, `analysis.py`, `summarize.py` |
| `internal/hostdial/harness_test.go` | this repository at `eaa225ed` | this project | the pattern of `sshnode/ssh_test.go` |
| `internal/rehearsal`, `internal/slurm/fakeslurm`, `internal/slurm/sshrun`, `internal/hostdial`, `internal/agent/enginedouble` | this project, unchanged | this project | the login node and sshd, the fake Slurm, the relay's ssh client, the relays, the engine double |
| GitHub Actions | `actions/checkout` v7.0.1, `actions/setup-go` v7.0.0, `actions/upload-artifact` v7.0.2 (as `ci.yml` and S15's workflow pin them) | GitHub, MIT | the hosted runs |

- **New:** the FIFO discipline and queue statistics of the link, the batch of concurrent transfers and the overlap rule, `s17 many` and its rules, the listener's TLS counters, the ssh node and the owner's ssh mode, the kernel-chosen ports, and the tables.
- **No third-party code is copied.**

## Deviations from the issue

- **The FIFO queue's depth** (the issue gives none) is a parameter: three depths and the fair link, so the answer is a function of it. The headline against "S15's 53 ms" is the 110 KB queue (52.7 ms) and the fair link (52.9 ms); at 1 MiB it is 72.0 ms.
- **Many Instances** ran in direct mode only, with one token stream per Instance (S14 ran 1 to 8), and bulk requests of 256 KiB and 1 MiB, which the issue does not size. A 64 MiB request per Instance every 10 s would be 320 MB/s at 50 Instances, through a 48 MB/s link. Each Instance's requests are 10 s apart, so each finds the spare work connection opened after the previous one; a burst of bulk requests from one Instance was measured only in the concurrent group. Host-dialed mode with many Instances was not run; its handshake lands on the same listener.
- **Concurrent transfers** ran in direct mode and through real ssh, on the fair link; not on the FIFO queue. The overlap rule's 80% threshold is this spike's.
- **The real relay's `srun` is the fake Slurm's:** the step's standard streams cross a unix socket to the test process in frames of at most 64 KiB, then the forced command and sshd. `ssh` and `sshd` are OpenSSH's, with its encryption and channel windows; real `srun`'s own buffering is not modelled. The login node and the compute node are one host, and the link sits between the ssh client and the sshd: the srun leg is not delayed.
- **The rekey group was added after the review of PR 416** (the first run left the 1 GiB stall as "consistent with, not confirmed"): three runs of layout (c), four downloads at once through the real relay, with the relay clients' debug logs. Its downloads are 40 MiB (160 MiB a batch, nine batches), so that the 1 GiB point falls inside a batch and not at its end, except the 64 MiB run for 2.25 GiB. The stall detector's thresholds (a gap above 100 ms in 6 of the 8 streams, within 0.3 s) and the match window (16 MiB below the rekey's bytes to 2 MiB above) were set before the run; one rekey missed the 100 ms threshold by 2 ms, and the table repeats the match at 80 ms.
- **S15's layouts (b) and (d)** are not run.
- **One link rate and round trip** (48 MB/s, 2.3 ms), as S15.

## Unknowns

- **The real path's queue depth.** The first result is a function of it; the loaded round trip of the real path reads it.
- **A longer round trip and loss.** A handshake on the request path costs several round trips before the first byte (requests that arrive together: 1–11 ms above one frpc's at this path's 2.3 ms round trip; requests that find a spare waiting pay none, so a longer path matters mainly for bursts), and a relay stream's rate is 256 KiB over the delay, so a longer path lowers the bulk rate; neither was run. The link drops nothing: its backpressure stands for loss.
- **`srun`'s own standard-stream buffering** on a real cluster.
- **The rekey's duration and its remedies.** A rekey stalls every stream 100–150 ms on a 2.3 ms path; why a key exchange takes that long here was not isolated (the data queued in the relay's buffers ahead of it is the candidate; the logs carry no timestamps). The default rekeys were started by sshd, so a client-side `RekeyLimit` does not remove them; whether a cipher with a larger default limit (ssh_config(5): up to 4G) or a longer limit on sshd would is untested, and belongs to a separate issue on the relay's ssh arguments. No run moved more than 2.25 GiB, and the ssh runs of the first hosted run were 1.25 GiB at most.
- **Many Instances through the relay** (host-dialed mode), and more than 50.
- **The admission plugin,** not configured for either frps (as S14 and S15).
- **Why the 4-transfer gap in (a) is lower than the single transfer's** (74 ms against 139 ms in direct mode, 103 against 183 through ssh): it was measured, in every repetition, and its mechanism was not isolated.

## Files and how to run

| File | What |
|---|---|
| `fetch.sh` | frp, sha256-checked, into `$S17_BIN`; builds `s17`, `s17link`, `s17bridge`, `s17node.test` and `s17ssh.test` |
| `cmd/s17/` | `certs`, `listener` (with the SNI route and the TLS counters), `owner` (host-dialed through the bridge, and `ssh` mode), `load` (streams and batches of transfers), `many` (many Instances) |
| `cmd/s17link/` | the link emulator: fair queues or one FIFO, with unit tests |
| `cmd/s17bridge/` | the ssh stand-in (S15's, the control) |
| `node/` | the node side (engine doubles, bulk paths, job sides, relay steps), a test binary |
| `sshnode/` | the real-ssh node: fake Slurm, login node and sshd, jobs, engine doubles; a test binary that links `internal/rehearsal` |
| `s17.py` | the driver: `run --group fifo\|ssh\|concurrent\|many\|rekey`, `host`, `leftovers` |
| `analysis.py`, `tests/` | the result rules and their tests |
| `summarize.py` | `extract`, `compress`, `tables`, `verify` |
| `s17-measure.yml`, `s17-rekey.yml` | the one-off workflows (the first run's, and the rekey run's), copied to `.github/workflows` on a throwaway PR |
| `results/<group>/rep{1,2,3}/` (`rekey` has 1 and 2) | `runs.csv`, `meta.csv`, `ssh.csv` (rekey group: the relay clients' parsed ssh facts), `samples.csv.xz`, `host.csv.xz` (the driver's), `streams.csv.xz` (every token's arrival), `transfers.csv` or `bulk.csv`, `links.csv`, `queues.csv`, `conns.csv.xz`, `listener.csv.xz` |

```sh
export S17_BIN=<dir> S17_WORK=<short scratch dir> S17_RESULTS=<dir>
bash fetch.sh
(go vet ./... && go test ./... && python3 -m unittest discover -s tests)
python3 s17.py run --group fifo --rep 1                  # about 8 minutes on a 4-CPU runner
python3 s17.py run --group ssh --rep 1                   # real ssh: needs sshd; on a development machine, run it only through internal/rehearsal's guard
python3 s17.py run --group concurrent --rep 1 --modes direct,ssh --layouts a,c --concurrent 1,2,4
python3 s17.py run --group many --rep 1 --ready-timeout 180
python3 s17.py run --group rekey --rep 1 --ready-timeout 90   # the relay clients under ssh -v; the logs stay in the run's private directory
python3 s17.py leftovers
python3 summarize.py extract $S17_RESULTS/raw results/<group>/rep1 && cp $S17_RESULTS/{runs,samples,host}.csv results/<group>/rep1/
python3 summarize.py compress; python3 summarize.py tables; python3 summarize.py verify
```

Every listener is on 127.0.0.1 on a kernel-chosen port; each run checks that every listener its processes hold is on 127.0.0.1, and afterwards that no recorded process is alive (`runs.csv`: `leftovers`, `leftover_listeners`, and for the ssh node `node_leaked`, the rehearsal's own count of processes left outside the test's tree: 0 in every run).
