# Spike S1c: frp tunnel capacity, the wss path and heartbeats (HBEP-6)

Issue #66. Both ends of the tunnel ran on one VM, on loopback, with a user-level link emulator
between frpc and frps. No cluster, keys or network exposure were involved.

## Findings

The numbers below cover both passes. Every matrix cell counted ran with the VM's 1-minute load at or below 4.0 at its start and its end (Environment). After the PR review, results were re-derived from the runs' raw files with corrected rules (`reanalyze.py`); nothing was re-measured.

- **The tunnel adds little delay, and the transport hardly matters.**
  - With no impairment, at 1 to 256 concurrent streams of 20 tokens/s, frp added 1–4 ms to the median time to first token (TTFT) over tcpmux, wss and ssh.
  - The 95th percentile rose by at most 10 ms over tcpmux and wss. Over ssh it rose by 5–47 ms at 1–64 streams, from a few slow samples.
  - Token rate stayed at 19.7–19.8 tokens/s per stream, and no stream failed.
  - With 20 ms added to the round trip, the tunnel still added only 1–6 ms. frps keeps a spare work connection, so a new stream rarely waits a round trip for one.
- **The front's CPU cost.** At 256 streams, with CPU and tokens counted over the same whole load run, the front used:
  - over tcpmux: 6.6–6.9% of a core per 1,000 tokens/s (frps);
  - over wss: 7.9–8.7% (frps plus Envoy);
  - over ssh: 7.5–7.8% (frps plus sshd).

  The cost per token was higher at 64 streams (8.8–16%), so it does not scale linearly with load. Other figures at 256 streams:
  - frpc used 40–47% of a core on the instance side;
  - RSS was about 50 MB for frps, 52 MB for Envoy and 70 MB for frpc;
  - each 222-byte token chunk took 290 bytes of TCP payload over tcpmux, 305 over wss and 316–335 over ssh.
- **On a capped link, throughput is the cap divided by the payload bytes per token.** At 512 kB/s of TCP payload, 256 streams got:
  - tcpmux: 1,763–1,766 tokens/s;
  - wss: 1,675–1,677 tokens/s;
  - ssh: 1,623–1,639 tokens/s.

  Every stream slowed evenly to 7–8 tokens/s, TTFT p50 grew to 1.3–3.5 s, and no stream failed. Up to 64 streams still fit.
- **Head-of-line blocking is severe whenever streams share a connection.** On a 1 MB/s link, one 4 MiB reply on the same tunnel held every other stream's tokens back for 4.5 s, the whole transfer. That happened in every run: 4 over tcpmux, 2 over wss and 4 over ssh.
  - It comes from yamux multiplexing every stream over one TCP connection, with up to 6 MiB of send credit per stream (frp's window).
  - A slow reader is no help. With the client reading at 64 KiB/s, the other streams were still held for 4.9–7.4 s.
  - With separate connections behind the same shared cap (frp with `tcpMux = false`, or no tunnel), the delay stayed under 0.15 s.
- **frps notices a silent path at the yamux keepalive, or at the application heartbeat if that is shorter.** These figures are min–max over 3 runs each, for a frozen path and for a node crash:
  - frp's defaults: 28–34 s. The failures fell 3–13 s into a 30 s keepalive cycle; the source bound over the whole cycle is 10–40 s.
  - S1's settings (keepalive 10 s, heartbeat 10/25 s): 12–20 s.
  - tight (keepalive 5 s, heartbeat every 2 s, 6 s timeout): 4–6 s.
  - A SIGKILL'd frpc is noticed in 0.07 s at every setting, through the kernel's FIN. That case says nothing about heartbeats.
- **frps does not bound how long a request waits.** Until frps notices, new requests into a dead tunnel hang.
  - Most waited frps's `userConnTimeout` of 10 s, which bounds only the wait for a work connection.
  - One request per run waited longer: most likely the one that drew frps's spare work connection, for which nothing times out until the session is torn down. It hit the probe's own 15 s deadline in every default run and in 3 of 6 runs at S1's settings.
  - The front must enforce its own first-byte deadline.
- **A short heartbeat timeout plus head-of-line blocking drops healthy tunnels.** With the tight setting, the slow-reader run queued more than 6 s of data on the tunnel in both passes.
  - A heartbeat timed out: frpc's in pass 1, frps's in pass 2. The healthy session dropped, and all 16 streams were cut.
  - S1's setting survived the same run.
- **Recommendation for HBEP-6** (details under "Recommendation for HBEP-6"):
  - transport: wss through the front's TLS terminator on 443, with tcpmux routing;
  - heartbeats: S1's settings, as a tested starting point;
  - capacity: an extrapolated 580–630 wss streams per front core, with the link the more likely limit;
  - bulk replies kept off the token tunnel.

## Question

HBEP-6 has the instance dial home to the front through frp. Three of its questions are open:

- **Tunnel protocol (open question 1).** frp's tcp transport with tcpmux routing, or wss through a TLS front on 443?
- **Capacity (operational readiness, item 5).** What do many concurrent token streams cost through one tunnel, and how do they degrade on a slow link? Does one slow or large reply delay the others on the same connection (head-of-line blocking)?
- **Heartbeats (open question 6).** How long does frps take to notice a dead tunnel, at frp's defaults and at tighter settings?

The issue names open questions "1, 5 and 6". In HBEP-6, open question 5 is multi-node instances and capacity is operational-readiness item 5; this spike answers the capacity item.

## Method

### Path and arms

```
load client --(loopback)--> frps tcpmux :29411 ==[link]== frpc --> engine :29400
                                                    ^
                         toxiproxy (matrix, heartbeats) or the shaper (head-of-line)
```

| Arm | frpc transport | Link carries | Front side |
|---|---|---|---|
| direct | none: the client dials the engine through the link | one TCP connection per stream | none |
| tcpmux | `tcp`, frp's default TLS inside (auto-generated certificate, not verified) | one TCP connection; every stream is a yamux stream in it | frps |
| wss | `wss`; frpc verifies the front's certificate against a throwaway CA | one TLS connection carrying a websocket, with yamux inside | Envoy 1.33.2 terminates TLS and passes the `/~!frp` websocket upgrade to frps |
| ssh | `tcp` to a local `ssh -L` forward (S1's option B) | one ssh connection; frp's connection is one ssh channel | a private sshd run as the user, with a key restricted to one forward |

- **Routing.** Every frp arm uses the same proxy type, `tcpmux` with the `httpconnect` multiplexer, and the instance name as its domain. Only the transport differs between arms. The client sends `CONNECT inst:80` to frps's tcpmux port, then a normal HTTP request.
- **wss needs a TLS front.** frps v0.71.1 does not serve wss itself, so the TLS terminator is needed rather than a choice. Its websocket listener matches only a plaintext `GET /~!frp` (`server/service.go:296-301`). A TLS ClientHello goes to frp's own TLS listener (`:361-364`), which expects frp messages after the handshake, not a websocket upgrade. The self-test confirms this: frpc in wss mode, pointed straight at an frps that has a certificate, never registers (frpc logs `connect to server error: unexpected EOF`). A front that serves the LLMLab API on 443 would terminate TLS anyway, so the wss arm puts Envoy in that place.
- **Engine.** `llm-d-inference-sim` v1.0.0, the build spike S2 patched to bind 127.0.0.1. It was chosen over S1's `fake_streamer.py` because it is Go rather than Python threads, so 256 streams keep precise 50 ms pacing (the self-test measures a gap p50 of 50.6 ms). It also speaks the OpenAI SSE format of a real engine (about 222 body bytes per token chunk), and it is the engine double the framework already uses. Flags: `--inter-token-latency 50ms --time-to-first-token 50ms --max-num-seqs 1024 --max-model-len 4096`. The default of 5 sequences would have queued every arm. Each request asks for `max_tokens = min_tokens = 60` with `ignore_eos`, so each reply is exactly 60 tokens, about 3 s.
- **frp settings in the matrix and head-of-line runs.** These are S1's candidate configuration: tcpMux on, poolCount 0, yamux keepalive 10 s, application heartbeat 10 s with a 25 s timeout, TCP keepalive 30 s. Even with poolCount 0, frps keeps one spare work connection: it asks frpc for a replacement every time it takes one (`server/control.go:634-635`).

### Load

- `s1cbench load` runs a closed loop: N workers, each starting a new streamed request when its last one ends, with every request on a new connection, as a front opening one tcpmux connection per stream would.
- Worker i starts at 3 s × i/N. Statistics cover the requests started in the following 24 s window.
- TTFT runs from before the client's TCP connect, including the CONNECT exchange, to the first content token.
- The token rate per stream is (tokens − 1) / (last token − first token).
- Total throughput counts every token that arrived inside the window.
- **CPU.** CPU is sampled per process from `/proc` over the whole load run: ramp, window and the drain of the last requests (about 30 s).
  - CPU per 1,000 tokens/s divides it by the tokens that arrived in that same run (about 4,540 tokens/s at 256 streams, against about 5,045 in the steady window).
  - The first version divided by the steady-window rate and understated the cost by about 10%. The run time and token count are now kept in `matrix.csv` (`run_s`, `run_tokens`, `meter_s`).
- **Link bytes.** Bytes on the link are TCP payload (`bytes_received` from the kernel's `tcp_info`), without IP and link headers, ACKs or retransmissions.
- A request counts as complete only with status 200, a `finish_reason` chunk, `[DONE]`, and exactly 60 content tokens.

### Impairment (toxiproxy 2.13.0, between frpc and frps)

Every arm goes through toxiproxy in every condition, so the emulator's own cost is common to all of them.

- **clean:** no toxic.
- **lat20:** a latency toxic of 10 ms on each direction, which adds 20 ms to the round trip.
- **bw:** a bandwidth toxic of 512 kB/s (toxiproxy's KB is 1,000 bytes) on each direction.
  - About 64 streams fit under this cap, and 256 streams need about 3 times it.
  - toxiproxy applies a toxic **per connection** (each link runs its own toxic stub; `link.go`, `toxics/bandwidth.go`). So in the direct arm every stream gets its own 512 kB/s and the cap never binds. The direct arm is therefore also run through the shaper below (`bw-shared`).

### Head-of-line (`s1cbench hol`)

- **Load.** 16 token streams of 30 s each (600 tokens) start over 1 s. At 8 s, one bulk request starts on the same path to a second frpc proxy (`bulk`). It goes through the same frpc, so it shares the yamux session.
  - `bulk`: a 4 MiB reply read as fast as it arrives (a large response).
  - `slow`: a 64 MiB reply read at 64 KiB/s, abandoned after 12 s (a slow reader).
  - `nocap`: the bulk variant with no cap.
- **Link.** `s1cbench shaper` with **one** rate of 1,000,000 B/s shared by all of its connections, in both directions. A chunk of up to 16 KiB occupies the link for size/rate, and each connection has at most one chunk waiting. The bottleneck is therefore shared, with per-connection queues, like a fair-queueing router. This gives a fair control where streams use separate connections. With toxiproxy's per-connection cap, that control would win by construction. One run per variant also goes through toxiproxy, as a cross-check on the shared-connection result.
- **Paths compared:**
  - tcpmux, wss and ssh, each sharing one connection;
  - `tcp-separate`: frp with `tcpMux = false`, so every work connection is its own TCP connection through the shaper (the control the issue asks for);
  - `direct-separate`: no tunnel, every stream on its own connection through the shaper;
  - `tcpmux-shared-tight`: the tcpmux path with the tight heartbeat setting, to see whether queueing behind a bulk reply trips a short heartbeat timeout.
- **Measures.** The fast streams' lateness and inter-token gaps before, during and after the bulk transfer.
  - **Lateness** of token k is its arrival minus its stream's own schedule (first token plus k times the stream's median gap): the token's queueing delay.
  - **Attribution.** A token belongs to the phase in which it was due. A gap belongs to the transfer when it overlaps it. A stall that ends only after the transfer still counts as the transfer's (`holPhases`, with a unit test for that crossing).
  - **Headline.** The largest lateness of a token arriving from the bulk start to 5 s after the bulk ends (`stall_late_max`).
  - **Recovery of the first runs.** Those runs assigned tokens by arrival, which hid a stall that ended just after the transfer (ssh pass 2). Their stall was recovered from the CSV's after-phase maxima and the raw per-250 ms lateness series.
- **Validity.** A run counts only if its bulk injection did what the variant needs; invalid runs are excluded from the tables (`hol_validity`).
  - A large reply must arrive whole (status 200 and all 4 MiB) with no error.
  - A slow reader must be abandoned by design after 12 s, having read about 64 KiB/s × 12 s.
  - All 46 counted runs were valid. A run whose raw file a later retry had overwritten was judged from the bytes it read.

### Heartbeats (`s1c.py heartbeat`)

- **Path.** frpc → toxiproxy → frps, with frps and frpc logging at `trace`, because yamux's keepalive error reaches frp's log only at trace level.
- **Before the failure.** Each run waits a random time after registration, so that the keepalive timers' phase differs between repetitions. The committed runs drew 0–10 s, which covers a whole cycle at S1's 10 s and the tight 5 s, but only 3–13 s of the default 30 s cycle. The harness now draws over each setting's whole keepalive interval. `s1cbench probe` then starts: a new connection through frps every 200 ms (CONNECT, then `GET /health` on the engine, 15 s timeout), plus frps's admin API polled every 100 ms.
- **Scenarios.**
  - **kill:** SIGKILL frpc with the path intact. On a live host the kernel closes frpc's socket and the FIN reaches frps at once, so this case cannot depend on heartbeats. It is measured because the issue asks for it.
  - **freeze:** the path goes silent with every socket open, and frpc lives on (a partition). After frps notices, the path heals.
  - **crash:** freeze, then SIGKILL frpc 0.5 s later (a node crash: no FIN ever reaches frps).
  - **disable:** toxiproxy's "disable proxy", which closes both sockets. This is evidence only, at the default setting.
- **How the path is frozen.** toxiproxy has no silent-path toxic:
  - its `timeout` toxic drops data but passes a close on (`toxics/timeout.go`: an input of `nil` calls `stub.Close`);
  - a bandwidth rate of 0 means unlimited (`toxics/bandwidth.go`);
  - disabling a proxy closes its connections.

  The self-test shows all three. So the path is frozen by stopping the toxiproxy process with SIGSTOP. The kernel keeps both TCP connections open and buffers what arrives, nothing is forwarded, and a killed frpc's FIN stays unread. The self-test checks, in 10 tries, that a client's close does not reach the server while the path is frozen, and that it does after SIGCONT.
- **Waiting for the freeze.** The harness now waits until every toxiproxy thread is stopped, which takes about 2 ms.
  - A self-test run during the review showed why: a Go process keeps running briefly after SIGSTOP, and a close slipped through in 2 of 8 tries.
  - The committed heartbeat runs did not wait, but none was affected. Their kills came 0.5 s after the stop, and every freeze and crash run logged a keepalive or heartbeat timeout at frps, never an EOF.
- **Measures.**
  - when the admin API reports the proxy offline or absent. Only these explicit answers count; an API error or a malformed reply is a failed measurement, counted, and a run without an explicit answer is censored. No run had either: every raw file holds only `online` and `offline`;
  - frps's `proxy closing` log line, and the cause it logs;
  - the first probe that frps refuses (404: no route);
  - how many probes hung meanwhile, for how long, and how many were cut by the probe's own 15 s deadline (censored waits);
  - for freeze, when frpc noticed, and how long after the heal routing returned.
- **Settings.** The exact keys are in `s1c_lib.py`, `HB`.

| Setting | frps | frpc | Bound from source |
|---|---|---|---|
| default | nothing set: tcpMux on, `tcpMuxKeepaliveInterval` 30 s, `heartbeatTimeout` −1 (off, because tcpMux is on), `tcpKeepalive` 7200 s | nothing set: keepalive 30 s, `heartbeatInterval` −1 (off), `dialServerKeepalive` 7200 s, `poolCount` 1 | yamux pings every 30 s; an unanswered ping fails after yamux's fixed 10 s `ConnectionWriteTimeout`: 10–40 s |
| s1 | `tcpMuxKeepaliveInterval` 10, `heartbeatTimeout` 25, `tcpKeepalive` 30 | `tcpMuxKeepaliveInterval` 10, `heartbeatInterval` 10, `heartbeatTimeout` 25, `dialServerKeepalive` 30 | yamux 10–20 s; the application heartbeat 15–26 s |
| tight | `tcpMuxKeepaliveInterval` 5, `heartbeatTimeout` 6, `tcpKeepalive` 30 | `tcpMuxKeepaliveInterval` 5, `heartbeatInterval` 2, `heartbeatTimeout` 6, `dialServerKeepalive` 30 | the application heartbeat 4–7 s, before yamux's 10–15 s |

Source for the defaults and bounds: frp v0.71.1 `pkg/config/v1/client.go:150-158`, `server.go:182-191`; `client/connector.go:150-155` and `server/service.go:747-751` set only the yamux keepalive interval and a 6 MiB stream window; `server/control.go:639-647` runs the heartbeat check. In fatedier/yamux v0.2.0, `mux.go` (`DefaultConfig`: `ConnectionWriteTimeout` 10 s) and `session.go:366-382` (`keepalive`) apply.

## Results

All numbers come from `results/*.csv`, rendered by `summarize.py`.
- **Rows.** Each matrix cell and each head-of-line run ran twice (pass 1 / pass 2), and each heartbeat case three times. Each CSV keeps every attempt in measurement order.
- **Attempts.** The tables use, per cell and pass, the last attempt whose 1-minute load was at most 4.0 at its start and end and, for head-of-line runs, whose bulk injection was valid (Environment, Method).
- **Re-derived columns.** `reanalyze.py` computed the run-window CPU, the head-of-line stall and validity, and the heartbeat detection rules from the raw files of these runs. Every counted row was reproduced by its raw file before its new columns were filled in.

### Capacity and impairment

#### No impairment

Values are pass 1 / pass 2. Added TTFT is against the direct arm in the same condition, concurrency and pass. CPU is percent of one core; path = frpc + frps (+ Envoy for wss, + ssh and sshd for ssh), averaged over the whole load run. Front CPU per 1k tok/s is frps (+ Envoy for wss, + sshd for ssh) per 1,000 tokens/s, with CPU and tokens over the same interval (the whole run); total tok/s is the steady 24 s window. RSS is the peak during the cell, shown for the clean cells only: the processes stay up across cells, and Go keeps the memory it grew to, so later cells show the earlier peak.

| arm | streams | TTFT p50 ms | added p50 ms | added p95 ms | gap p99 ms | tok/s per stream p50 (p5) | total tok/s | failed | path CPU % | front CPU % per 1k tok/s | frpc / frps RSS MB | TCP payload B/token |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| direct | 1 | 53 / 53 | - | - | 57 / 52 | 19.7 / 19.8 (19.6 / 19.7) | 20 / 20 | 0 / 0 | - | - | - | - |
| direct | 16 | 53 / 53 | - | - | 57 / 52 | 19.7 / 19.8 (19.6 / 19.7) | 314 / 316 | 0 / 0 | - | - | - | - |
| direct | 64 | 52 / 52 | - | - | 56 / 53 | 19.7 / 19.8 (19.6 / 19.7) | 1261 / 1264 | 0 / 0 | - | - | - | - |
| direct | 256 | 52 / 52 | - | - | 58 / 54 | 19.7 / 19.8 (19.6 / 19.7) | 5040 / 5054 | 0 / 0 | - | - | - | - |
| tcpmux | 1 | 55 / 56 | +2 / +3 | +2 / +4 | 52 / 60 | 19.8 / 19.7 (19.7 / 19.7) | 20 / 20 | 0 / 0 | 2 / 1 | 45.6 / 30.5 | 16.9 / 27.2 | 291.6 |
| tcpmux | 16 | 54 / 55 | +2 / +2 | -2 / +10 | 52 / 60 | 19.8 / 19.7 (19.7 / 19.5) | 316 / 314 | 0 / 0 | 14 / 8 | 21.0 / 12.7 | 21.6 / 29.4 | 290.7 |
| tcpmux | 64 | 54 / 53 | +1 / +1 | -0 / +2 | 53 / 54 | 19.8 / 19.8 (19.7 / 19.7) | 1264 / 1262 | 0 / 0 | 32 / 23 | 11.5 / 8.8 | 31.8 / 35.3 | 290.5 |
| tcpmux | 256 | 54 / 54 | +2 / +2 | +3 / +2 | 57 / 56 | 19.8 / 19.8 (19.7 / 19.7) | 5043 / 5050 | 0 / 0 | 74 / 78 | 6.6 / 6.9 | 67.9 / 50.2 | 290.3 |
| wss | 1 | 55 / 55 | +2 / +2 | +3 / +1 | 56 / 53 | 19.7 / 19.8 (19.7 / 19.8) | 20 / 20 | 0 / 0 | 2 / 2 | 71.0 / 76.0 | 17.0 / 26.8 | 305.9 |
| wss | 16 | 54 / 55 | +2 / +2 | -2 / +4 | 53 / 54 | 19.8 / 19.8 (19.7 / 19.7) | 316 / 316 | 0 / 0 | 17 / 16 | 32.2 / 28.7 | 21.9 / 30.1 | 305.2 |
| wss | 64 | 54 / 54 | +2 / +2 | +0 / +4 | 54 / 55 | 19.8 / 19.8 (19.7 / 19.7) | 1263 / 1262 | 0 / 0 | 37 / 36 | 15.9 / 15.5 | 32.3 / 36.1 | 305.1 |
| wss | 256 | 54 / 54 | +2 / +2 | +2 / +5 | 61 / 62 | 19.8 / 19.7 (19.6 / 19.5) | 5047 / 5041 | 0 / 0 | 84 / 76 | 8.7 / 7.9 | 71.4 / 53.2 | 304.9 |
| ssh | 1 | 56 / 56 | +3 / +3 | +41 / +47 | 59 / 56 | 19.7 / 19.8 (19.7 / 19.7) | 20 / 20 | 0 / 0 | 2 / 2 | 51.1 / 50.9 | 16.5 / 26.8 | 334.6 |
| ssh | 16 | 57 / 57 | +4 / +4 | +30 / +13 | 56 / 54 | 19.7 / 19.8 (19.7 / 19.7) | 315 / 315 | 0 / 0 | 14 / 14 | 21.4 / 19.9 | 21.5 / 29.1 | 326.1 |
| ssh | 64 | 56 / 56 | +3 / +3 | +5 / +20 | 57 / 54 | 19.7 / 19.8 (19.7 / 19.7) | 1261 / 1262 | 0 / 0 | 36 / 36 | 12.8 / 12.7 | 31.8 / 35.3 | 319.2 |
| ssh | 256 | 55 / 56 | +3 / +4 | +1 / +8 | 55 / 63 | 19.8 / 19.7 (19.7 / 19.6) | 5054 / 5037 | 0 / 0 | 91 / 89 | 7.8 / 7.5 | 69.8 / 52.2 | 315.6 |

#### 20 ms added round trip (10 ms each way)

Values are pass 1 / pass 2. Added TTFT is against the direct arm in the same condition, concurrency and pass. CPU is percent of one core; path = frpc + frps (+ Envoy for wss, + ssh and sshd for ssh), averaged over the whole load run. Front CPU per 1k tok/s is frps (+ Envoy for wss, + sshd for ssh) per 1,000 tokens/s, with CPU and tokens over the same interval (the whole run); total tok/s is the steady 24 s window. RSS is the peak during the cell, shown for the clean cells only: the processes stay up across cells, and Go keeps the memory it grew to, so later cells show the earlier peak.

| arm | streams | TTFT p50 ms | added p50 ms | added p95 ms | gap p99 ms | tok/s per stream p50 (p5) | total tok/s | failed | path CPU % | front CPU % per 1k tok/s | frpc / frps RSS MB | TCP payload B/token |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| direct | 1 | 74 / 74 | - | - | 52 / 53 | 19.8 / 19.8 (19.8 / 19.7) | 20 / 20 | 0 / 0 | - | - | - | - |
| direct | 16 | 74 / 74 | - | - | 52 / 57 | 19.8 / 19.7 (19.7 / 19.6) | 314 / 313 | 0 / 0 | - | - | - | - |
| direct | 64 | 73 / 74 | - | - | 53 / 57 | 19.8 / 19.7 (19.7 / 19.6) | 1255 / 1250 | 0 / 0 | - | - | - | - |
| direct | 256 | 73 / 73 | - | - | 53 / 54 | 19.8 / 19.8 (19.7 / 19.7) | 5027 / 5022 | 0 / 0 | - | - | - | - |
| tcpmux | 1 | 76 / 75 | +2 / +2 | +4 / +7 | 57 / 58 | 19.8 / 19.7 (19.7 / 19.6) | 20 / 20 | 0 / 0 | 2 / 1 | 46.0 / 30.7 | - | 291.3 |
| tcpmux | 16 | 75 / 75 | +1 / +1 | +2 / -0 | 54 / 54 | 19.7 / 19.7 (19.7 / 19.7) | 313 / 313 | 0 / 0 | 13 / 11 | 19.7 / 18.0 | - | 291.0 |
| tcpmux | 64 | 75 / 74 | +1 / +1 | +4 / +0 | 56 / 57 | 19.8 / 19.7 (19.7 / 19.6) | 1253 / 1253 | 0 / 0 | 26 / 23 | 9.6 / 8.7 | - | 290.7 |
| tcpmux | 256 | 74 / 74 | +1 / +1 | +9 / +2 | 80 / 59 | 19.7 / 19.8 (19.3 / 19.7) | 4986 / 5019 | 0 / 0 | 64 / 66 | 5.7 / 5.9 | - | 290.3 |
| wss | 1 | 76 / 76 | +2 / +2 | +4 / +15 | 53 / 56 | 19.8 / 19.8 (19.8 / 19.7) | 20 / 20 | 0 / 0 | 3 / 2 | 91.9 / 71.5 | - | 306.4 |
| wss | 16 | 75 / 75 | +1 / +1 | +2 / +1 | 53 / 54 | 19.8 / 19.8 (19.7 / 19.7) | 314 / 313 | 0 / 0 | 17 / 15 | 32.0 / 28.2 | - | 305.5 |
| wss | 64 | 75 / 75 | +1 / +1 | +2 / -0 | 55 / 56 | 19.8 / 19.8 (19.7 / 19.7) | 1254 / 1254 | 0 / 0 | 35 / 32 | 15.0 / 14.0 | - | 305.3 |
| wss | 256 | 75 / 74 | +2 / +1 | +6 / +6 | 60 / 65 | 19.8 / 19.8 (19.6 / 19.5) | 5014 / 5008 | 0 / 0 | 73 / 72 | 7.3 / 7.3 | - | 304.9 |
| ssh | 1 | 76 / 76 | +3 / +2 | +40 / +40 | 53 / 55 | 19.8 / 19.8 (19.7 / 19.7) | 20 / 20 | 0 / 0 | 3 / 2 | 61.4 / 61.4 | - | 334.1 |
| ssh | 16 | 80 / 79 | +6 / +5 | +10 / +14 | 53 / 53 | 19.8 / 19.8 (19.7 / 19.7) | 313 / 313 | 0 / 0 | 18 / 16 | 27.5 / 24.7 | - | 331.8 |
| ssh | 64 | 77 / 77 | +4 / +3 | +8 / +5 | 53 / 55 | 19.8 / 19.8 (19.7 / 19.7) | 1254 / 1253 | 0 / 0 | 38 / 35 | 13.5 / 12.6 | - | 322.8 |
| ssh | 256 | 76 / 76 | +3 / +3 | +6 / +6 | 55 / 59 | 19.8 / 19.8 (19.7 / 19.7) | 5017 / 5014 | 0 / 0 | 82 / 78 | 6.9 / 6.4 | - | 316.9 |

#### Bandwidth cap 512,000 B/s per connection, both directions

Values are pass 1 / pass 2. Added TTFT is against the direct arm in the same condition, concurrency and pass. CPU is percent of one core; path = frpc + frps (+ Envoy for wss, + ssh and sshd for ssh), averaged over the whole load run. Front CPU per 1k tok/s is frps (+ Envoy for wss, + sshd for ssh) per 1,000 tokens/s, with CPU and tokens over the same interval (the whole run); total tok/s is the steady 24 s window. RSS is the peak during the cell, shown for the clean cells only: the processes stay up across cells, and Go keeps the memory it grew to, so later cells show the earlier peak.

| arm | streams | TTFT p50 ms | added p50 ms | added p95 ms | gap p99 ms | tok/s per stream p50 (p5) | total tok/s | failed | path CPU % | front CPU % per 1k tok/s | frpc / frps RSS MB | TCP payload B/token |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| direct | 1 | 56 / 55 | - | - | 52 / 54 | 19.8 / 19.8 (19.7 / 19.7) | 20 / 20 | 0 / 0 | - | - | - | - |
| direct | 16 | 55 / 56 | - | - | 53 / 57 | 19.8 / 19.7 (19.7 / 19.6) | 315 / 315 | 0 / 0 | - | - | - | - |
| direct | 64 | 55 / 55 | - | - | 54 / 54 | 19.8 / 19.8 (19.7 / 19.7) | 1263 / 1263 | 0 / 0 | - | - | - | - |
| direct | 256 | 55 / 55 | - | - | 55 / 57 | 19.8 / 19.7 (19.7 / 19.6) | 5049 / 5036 | 0 / 0 | - | - | - | - |
| tcpmux | 1 | 57 / 56 | +1 / +1 | +28 / +0 | 59 / 60 | 19.7 / 19.7 (19.6 / 19.7) | 20 / 20 | 0 / 0 | 2 / 1 | 40.7 / 30.5 | - | 291.3 |
| tcpmux | 16 | 57 / 57 | +2 / +1 | +7 / +2 | 60 / 59 | 19.7 / 19.7 (19.6 / 19.6) | 314 / 314 | 0 / 0 | 13 / 10 | 20.7 / 15.1 | - | 290.6 |
| tcpmux | 64 | 58 / 58 | +3 / +3 | +12 / +9 | 63 / 63 | 19.7 / 19.7 (19.6 / 19.7) | 1258 / 1259 | 0 / 0 | 31 / 28 | 11.5 / 10.1 | - | 290.6 |
| tcpmux | 256 | 1398 / 1281 | +1343 / +1226 | +3657 / +3672 | 206 / 195 | 7.5 / 7.5 (7.3 / 7.3) | 1766 / 1763 | 0 / 0 | 27 / 26 | 5.5 / 5.3 | - | 291.9 |
| wss | 1 | 57 / 58 | +1 / +4 | +2 / +6 | 53 / 62 | 19.8 / 19.7 (19.7 / 19.6) | 20 / 20 | 0 / 0 | 3 / 2 | 81.2 / 61.0 | - | 306.1 |
| wss | 16 | 56 / 57 | +1 / +1 | +3 / -1 | 56 / 57 | 19.7 / 19.7 (19.7 / 19.7) | 315 / 315 | 0 / 0 | 16 / 15 | 31.5 / 28.8 | - | 305.5 |
| wss | 64 | 57 / 58 | +2 / +3 | +6 / +7 | 58 / 59 | 19.8 / 19.7 (19.7 / 19.6) | 1261 / 1259 | 0 / 0 | 40 / 34 | 17.9 / 15.4 | - | 305.2 |
| wss | 256 | 2239 / 1535 | +2184 / +1480 | +4040 / +4038 | 194 / 196 | 7.4 / 7.1 (6.9 / 6.9) | 1675 / 1677 | 0 / 0 | 35 / 31 | 7.7 / 7.0 | - | 307.1 |
| ssh | 1 | 59 / 58 | +3 / +4 | +47 / +39 | 56 / 55 | 19.8 / 19.8 (19.7 / 19.7) | 20 / 20 | 0 / 0 | 2 / 2 | 51.0 / 61.2 | - | 335.9 |
| ssh | 16 | 60 / 60 | +4 / +5 | +14 / +12 | 56 / 58 | 19.7 / 19.7 (19.7 / 19.7) | 314 / 314 | 0 / 0 | 14 / 13 | 20.4 / 20.0 | - | 326.5 |
| ssh | 64 | 60 / 62 | +6 / +7 | +11 / +19 | 59 / 66 | 19.7 / 19.7 (19.7 / 19.5) | 1259 / 1254 | 0 / 0 | 38 / 32 | 14.3 / 11.4 | - | 322.8 |
| ssh | 256 | 3261 / 3521 | +3206 / +3466 | +4503 / +4676 | 196 / 194 | 8.2 / 8.2 (7.0 / 6.9) | 1639 / 1623 | 0 / 0 | 25 / 33 | 4.5 / 6.1 | - | 310.8 |

#### Direct-arm controls

| condition | streams | TTFT p50 ms | gap p99 ms | tok/s per stream p50 | total tok/s | failed |
|---|---:|---:|---:|---:|---:|---:|
| direct, no toxiproxy | 1 | 53 / 52 | 54 / 53 | 19.7 / 19.8 | 20 / 20 | 0 / 0 |
| direct, no toxiproxy | 16 | 52 / 52 | 53 / 53 | 19.8 / 19.7 | 316 / 316 | 0 / 0 |
| direct, no toxiproxy | 64 | 52 / 52 | 52 / 55 | 19.8 / 19.7 | 1263 / 1261 | 0 / 0 |
| direct, no toxiproxy | 256 | 52 / 52 | 54 / 58 | 19.8 / 19.7 | 5053 / 5029 | 0 / 0 |
| direct through the shared-bottleneck shaper at 512,000 B/s | 1 | 56 / 56 | 52 / 54 | 19.8 / 19.8 | 20 / 20 | 0 / 0 |
| direct through the shared-bottleneck shaper at 512,000 B/s | 16 | 56 / 56 | 54 / 54 | 19.8 / 19.8 | 315 / 315 | 0 / 0 |
| direct through the shared-bottleneck shaper at 512,000 B/s | 64 | 56 / 57 | 56 / 61 | 19.8 / 19.8 | 1261 / 1258 | 0 / 0 |
| direct through the shared-bottleneck shaper at 512,000 B/s | 256 | 3202 / 3055 | 2474 / 2517 | 21.7 / 20.0 | 2222 / 2222 | 0 / 0 |

Measured only above a 1-minute load of 4.0 (every attempt): none

**What the matrix shows:**
- **No impairment.** All three transports add 1–4 ms to the median TTFT and keep every stream at the engine's pace (gap p99 within about 8 ms of the direct arm).
  - The ssh arm's p95 at 1–64 streams is noisier (+5 to +47 ms), from a few slow samples among 8 to 500 requests. At 256 streams it is +1 to +8 ms.
- **Per-connection setup.** HBEP-6 expected a work-connection round trip per new stream, but there is none in the common case.
  - With 20 ms added to the round trip, the direct arm's TTFT rises by the expected two one-way delays (21 ms: request and first token), and the tunnels add 1–6 ms on top.
  - frps keeps one spare work connection and asks for a replacement each time it hands one out (`server/control.go:634-635`), so a new stream finds one waiting.
  - At 256 streams (about 85 new streams/s) some streams arrive before the replacement does, and the tunnel's p95 rises by 2–9 ms. `poolCount` would raise the number of spares.
- **CPU.** Averaged over the load run at 256 streams: frps 25–31% of a core, frpc 40–47%, Envoy 11–12%, ssh plus sshd 20–21%.
  - Per 1,000 tokens/s over the same run, the front needed 6.6–6.9% (tcpmux), 7.9–8.7% (wss) and 7.5–7.8% (ssh). At 64 streams it needed 8.8–11.5%, 15–16% and 12.6–13.5% (the clean and lat20 cells): part of the cost does not grow with the token rate.
- **Bandwidth cap.** With a 512 kB/s cap and up to 64 streams (about 370 kB/s of demand), nothing degrades except a few ms of TTFT.
  - At 256 streams the demand is about 1.5 MB/s, three times the cap. Every transport delivers almost exactly the cap divided by its TCP payload per token (512,000 / 290 = 1,766; / 305 = 1,679; / 316 = 1,620), so framing overhead turns directly into lost capacity.
  - The direct arm through the same shared cap (the shaper) gets 2,222 tokens/s.
  - Streams slow evenly to 7–8 tokens/s, with TTFT p50 1.3–3.5 s and p95 3.7–4.7 s, and none fails. frp's yamux keepalive and S1's heartbeat survived the seconds of queueing.
  - The direct arm under toxiproxy's cap does not degrade, because toxiproxy caps each connection separately (Method).

### Head-of-line blocking

Lateness is the queueing delay of a token: its arrival minus its stream's own schedule. "Heartbeat timeouts frpc-frps" counts `heartbeat timeout` lines in each endpoint's log; "?" means that run's frps log was not kept (pass 1, before per-run logs were added).

Values are run 1 / run 2 (/ more runs where listed). The stall is the largest lateness of a fast-stream token arriving from the bulk start to 5 s after the bulk ends.

| path | variant | bulk bytes read | bulk B/s | bulk s | fast gap p99 before the bulk, ms | stall: fast lateness max, ms | fast streams complete | heartbeat timeouts frpc / frps |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| tcpmux-shared | bulk | 4194304 / 4194304 / 4194304 / 4194304 | 953531 / 956271 / 957026 / 961469 | 4.5 / 4.5 / 4.5 / 4.5 | 55 / 53 / 53 / 58 | 4539 / 4513 / 4493 / 4538 | 16/16 / 16/16 / 16/16 / 16/16 | 0-? / 0-0 / 0-0 / 0-0 |
| tcpmux-shared | slow | 811699 / 786291 | 67686 / 66212 | 12.0 / 12.0 | 55 / 56 | 5793 / 4872 | 16/16 / 16/16 | 0-? / 0-0 |
| tcpmux-shared | nocap | 4194304 / 4194304 | 165979285 / 144702875 | 0.0 / 0.0 | 54 / 57 | 27 / 54 | 16/16 / 16/16 | 0-? / 0-0 |
| wss-shared | bulk | 4194304 / 4194304 | 961729 / 962016 | 4.5 / 4.5 | 54 / 56 | 4492 / 4544 | 16/16 / 16/16 | 0-? / 0-0 |
| wss-shared | slow | 807603 / 786291 | 67823 / 66240 | 12.0 / 12.0 | 58 / 60 | 5458 / 5204 | 16/16 / 16/16 | 0-? / 0-0 |
| wss-shared | nocap | 4194304 / 4194304 | 132550187 / 97959008 | 0.0 / 0.1 | 52 / 56 | 29 / 56 | 16/16 / 16/16 | 0-? / 0-0 |
| ssh-shared | bulk | 4194304 / 4194304 / 4194304 / 4194304 | 946308 / 943821 / 948786 / 970370 | 4.5 / 4.5 / 4.5 / 4.5 | 54 / 59 / 54 / 55 | 4466 / 4481 / 4522 / 4476 | 16/16 / 16/16 / 16/16 / 16/16 | 0-? / 0-0 / 0-0 / 0-0 |
| ssh-shared | slow | 795123 / 791219 | 66682 / 66863 | 12.0 / 12.0 | 73 / 64 | 7239 / 7401 | 16/16 / 16/16 | 0-? / 0-0 |
| ssh-shared | nocap | 4194304 / 4194304 | 159850165 / 169697899 | 0.1 / 0.0 | 65 / 61 | 74 / 60 | 16/16 / 16/16 | 0-? / 0-0 |
| tcp-separate | bulk | 4194304 / 4194304 | 914817 / 905743 | 4.6 / 4.6 | 52 / 59 | 80 / 65 | 16/16 / 16/16 | 0-? / 0-0 |
| tcp-separate | slow | 809728 / 798208 | 67525 / 66627 | 12.0 / 12.0 | 58 / 55 | 143 / 134 | 16/16 / 16/16 | 0-? / 0-0 |
| tcp-separate | nocap | 4194304 / 4194304 | 265400632 / 296913814 | 0.0 / 0.0 | 52 / 56 | 22 / 40 | 16/16 / 16/16 | 0-? / 0-0 |
| direct-separate | bulk | 4194304 / 4194304 | 925560 / 921559 | 4.5 / 4.6 | 53 / 55 | 60 / 42 | 16/16 / 16/16 | 0-? / 0-0 |
| direct-separate | slow | 795315 / 795315 | 66313 / 66322 | 12.0 / 12.0 | 53 / 56 | 64 / 48 | 16/16 / 16/16 | 0-? / 0-0 |
| direct-separate | nocap | 4194304 / 4194304 | 428086786 / 365180277 | 0.0 / 0.0 | 53 / 55 | 17 / 23 | 16/16 / 16/16 | 0-? / 0-0 |
| tcpmux-shared-toxiproxy | bulk | 4194304 / 4194304 | 992114 / 1015181 | 4.2 / 4.2 | 53 / 56 | 4254 / 4251 | 16/16 / 16/16 | 0-? / 0-0 |
| tcpmux-shared-toxiproxy | slow | 795315 / 791219 | 66320 / 66508 | 12.0 / 12.0 | 54 / 54 | 6746 / 6741 | 16/16 / 16/16 | 0-? / 0-0 |
| tcpmux-shared-toxiproxy | nocap | 4194304 / 4194304 | 151782340 / 189394779 | 0.0 / 0.0 | 56 / 56 | 22 / 47 | 16/16 / 16/16 | 0-? / 0-0 |
| tcpmux-shared-tight | bulk | 4194304 / 4194304 | 956304 / 963150 | 4.5 / 4.5 | 53 / 57 | 4501 / 4570 | 16/16 / 16/16 | 0-? / 0-0 |
| tcpmux-shared-tight | slow | 795315 / 807603 | 66325 / 67817 | 12.0 / 12.0 | 53 / 53 | 4702 / 6698 | 0/16 / 0/16 | 1-? / 0-1 |
| tcpmux-shared-tight | nocap | 4194304 / 4194304 | 144170800 / 101634700 | 0.0 / 0.1 | 56 / 58 | 48 / 48 | 16/16 / 16/16 | 0-0 / 0-0 |

**What the head-of-line runs show:**
- **A large reply on the shared connection.** 16 token streams plus one 4 MiB reply ran on a 1 MB/s link. Over tcpmux (4 runs), wss (2) and ssh (4), the token streams stalled for 4.47–4.54 s, the length of the transfer: their tokens waited behind the whole reply. toxiproxy's cap gave the same result (4.2 s).
- **The ssh pass-2 run is no exception.** The first version reported that run as showing no blocking (a 154 ms stall). Its stalled tokens arrived just after the transfer ended and were counted in the "after" phase, whose maximum lateness was 4,481 ms. Counted by when they were due, it stalled like the others.
- **Mechanism.** yamux writes every stream's frames into one send queue and one TCP socket. frp lets each stream send up to 6 MiB ahead of its reader (`client/connector.go:154`, `server/service.go:751`), and the credit returns as frps reads.
  - On a slow link the socket's buffer and the link fill with the large reply, and every token frame queues behind it in order.
  - For this fixture, the stall came close to the reply's size divided by the link rate (4 MiB at 1 MB/s: 4.2–4.5 s).
  - It is not a general bound. Queued bytes add up across streams, socket buffers, proxies and ssh's buffers; see the slow reader next.
- **A slow reader does not help.** The front's client read a 64 MiB reply at 64 KiB/s, but frps keeps reading the yamux stream into its own buffer while it has credit. So the link still carried megabytes at full rate, and the other streams' tokens were held for:
  - 4.9–5.8 s over tcpmux and wss;
  - 6.7 s through toxiproxy;
  - 7.2–7.4 s over ssh, more than 6 MiB / 1 MB/s (6.3 s), because ssh adds its channel and socket buffers.
- **Separate connections avoid it.** The control is frp with `tcpMux = false` (every work connection its own TCP connection) through the same shared 1 MB/s bottleneck. There the token streams' lateness stayed at 60–80 ms for the large reply and 134–143 ms for the slow reader. With no tunnel it was 42–64 ms. Without a cap no path showed any blocking (at most 74 ms).
  - The shaper is a per-connection fair queue. Behind a FIFO bottleneck, separate connections would share one queue and gain less.
- **No transport avoids it.** tcpmux, wss and ssh all multiplex every stream over one connection, so the choice between them cannot rest on head-of-line behaviour.
- **A short heartbeat timeout cuts a healthy tunnel.** With the tight setting (2 s interval, 6 s timeout), the slow-reader run queued more than 6 s of data ahead of the heartbeats in both passes.
  - In pass 1 frpc logged `heartbeat timeout`. In pass 2 frps did (`server/control.go:647`) and frpc logged in again.
  - Either way the session dropped and all 16 token streams were cut (0/16 complete).
  - The 4 MiB run (4.5 s of queue) survived. S1's setting (25 s, with yamux's fixed 10 s ping timeout) survived both runs.
- **An engine artifact, worked around.** In pass 1, the tight no-cap run (`tcpmux-shared-tight`, nocap) followed the run that cut 16 streams, and 9 of its 16 streams got `404 Not Found` from the engine.
  - llm-d-inference-sim v1.0.0 does this: after streams are cut mid-reply, it answers some later requests with a fasthttp 404. This was reproduced without frp, by cutting 16 direct streams and sending new requests.
  - Every head-of-line run now starts with a fresh engine. That run was repeated (16/16), and pass 2 ran with the fix.

### Heartbeats and dead-tunnel detection

| setting | scenario | reps | frps marks proxy gone, s (min / median / max) | first refused probe, s | probes hung meanwhile: median count, median wait s, longest wait s (cut at 15 s: runs) | API errors | censored | frps cause (rep 1) | frpc noticed, s | routing back after heal, s |
|---|---|---:|---:|---:|---:|---:|---:|---|---:|---:|
| default | kill | 3 | 0.1 / 0.1 / 0.1 | 0.2 / 0.2 / 0.2 | 0 | 0 | 0 | accept new mux stream error: EOF | - | - |
| default | freeze | 3 | 28.4 / 32.5 / 34.4 | 28.4 / 32.6 / 34.4 | 162, 10.0, 15.0 (3) | 0 | 0 | [ERR] yamux: keepalive failed: i/o deadline reached / accept new mux stream error: keepalive timeout | 28.3 / 32.4 / 34.3 | 0.1 / 0.1 / 0.2 |
| default | crash | 3 | 27.6 / 29.4 / 31.3 | 27.8 / 29.6 / 31.4 | 147, 10.0, 15.0 (3) | 0 | 0 | [ERR] yamux: keepalive failed: i/o deadline reached / accept new mux stream error: keepalive timeout | - | - |
| default | disable | 1 | 0.1 / 0.1 / 0.1 | 0.2 / 0.2 / 0.2 | 0 | 0 | 0 | accept new mux stream error: EOF | - | 0.1 / 0.1 / 0.1 |
| s1 | kill | 3 | 0.1 / 0.1 / 0.1 | 0.2 / 0.2 / 0.2 | 0 | 0 | 0 | accept new mux stream error: EOF | - | - |
| s1 | freeze | 3 | 13.3 / 15.3 / 16.3 | 13.4 / 15.4 / 16.4 | 76, 7.7, 15.0 (2) | 0 | 0 | [ERR] yamux: keepalive failed: i/o deadline reached / accept new mux stream error: keepalive timeout | 13.2 / 15.2 / 16.2 | 0.0 / 0.1 / 0.1 |
| s1 | crash | 3 | 11.8 / 12.4 / 19.7 | 12.0 / 12.4 / 19.8 | 61, 6.1, 15.0 (1) | 0 | 0 | [ERR] yamux: keepalive failed: i/o deadline reached / accept new mux stream error: keepalive timeout | - | - |
| tight | kill | 3 | 0.1 / 0.1 / 0.1 | 0.2 / 0.2 / 0.2 | 0 | 0 | 0 | accept new mux stream error: EOF | - | - |
| tight | freeze | 3 | 4.2 / 5.2 / 5.8 | 4.2 / 5.3 / 6.0 | 26, 2.6, 7.7 (0) | 0 | 0 | heartbeat timeout | 4.1 / 5.2 / 5.8 | 0.1 / 0.1 / 0.1 |
| tight | crash | 3 | 4.9 / 5.2 / 6.0 | 5.0 / 5.2 / 6.0 | 25, 2.6, 7.8 (0) | 0 | 0 | heartbeat timeout | - | - |

**What the heartbeat runs show:**
- **kill and disable.** frps learns of a SIGKILL'd frpc, or of a proxy that closes its sockets, within 0.07 s at every setting, through the FIN. Heartbeats only matter when the path dies silently: a node crash, a network partition, or a NAT that drops state.
- **Silent path: frp's defaults.** frps noticed after 28–34 s.
  - frps's yamux pings every 30 s and gives up 10 s after an unanswered ping (yamux's fixed `ConnectionWriteTimeout`). The application heartbeat is off because tcpMux is on.
  - frps logged `yamux: keepalive failed: i/o deadline reached`.
  - The failures fell 3–13 s into the 30 s cycle (Method). Over the whole cycle the source bound is 10–40 s.
- **Silent path: S1's settings.** 12–20 s, again through the yamux keepalive (10 s interval plus the 10 s timeout), which fires before the 25 s application heartbeat.
- **Silent path: the tight setting.** 4–6 s, through frps's application heartbeat check (`heartbeat timeout`, 6 s after the last ping; pings come every 2 s).
- **What the front sees meanwhile.** Every new connection is accepted (CONNECT answers 200) and then hangs.
  - **Most probes** waited frps's `userConnTimeout` of 10 s, which bounds only the wait for a work connection, or until frps closed the session.
  - **One probe per run waited longer.** Most likely it drew frps's spare work connection: it already had a path into the dead tunnel, so nothing timed it out. It waited until the session was torn down or the probe's own 15 s deadline, whichever came first. Those 15 s waits are censored: one probe in every default run, and in 3 of 6 runs at S1's settings.
  - **After frps removed the proxy,** the next probe was refused at once (404). The admin API, the log and the first refusal agreed within 0.2 s in every run.
- **Detection by frpc.** In the freeze runs frpc noticed within 0.1 s of frps, because both sides use the same timers.
- **After the heal.** Routing returned 0.1 s after the path healed. frpc's reconnect attempts were already waiting in the frozen proxy's accept queue, so this number does not show frpc's reconnect backoff on a real partition.

## Recommendation for HBEP-6

1. **tcpmux or wss for alpha (open question 1): wss.** Use frp's `wss` transport to the front's TLS terminator on 443, keeping `tcpmux` (httpconnect) routing by Instance name.
   - **What wss costs.**
     - Latency: the same as frp's plain tcp transport (TTFT +1–2 ms).
     - Head-of-line behaviour: the same, since both run yamux over one connection.
     - Failure detection: the same frp timers.
     - About 15–30% more front CPU per token: 7.9–8.7% of a core per 1,000 tokens/s for frps plus Envoy, against 6.6–6.9% for frps alone over tcp.
     - About 5% more TCP payload.
   - **What wss buys.** It is ordinary HTTPS on 443. S1's step 0 found that compute nodes reach 443, and a front that serves the LLMLab API on 443 terminates TLS anyway.
   - **Front configuration.** frps v0.71.1 cannot terminate wss itself, so the front's TLS terminator (Envoy here) must pass the `/~!frp` websocket upgrade on to frps's bind port. frpc should verify the front's certificate (`transport.tls.trustedCaFile`, `transport.tls.serverName`).
   - **Development path.** frp's `tcp` transport inside an ssh forward remains HBEP-11's development path (S1's option B). It behaves the same, at +3 ms TTFT, with a front cost of 7.5–7.8% per 1,000 tokens/s including sshd.
2. **Heartbeat settings (open question 6): S1's set, on both ends, as the tested starting point.**
   - frps: `transport.tcpMuxKeepaliveInterval = 10`, `transport.heartbeatTimeout = 25`, `transport.tcpKeepalive = 30`.
   - frpc: `transport.tcpMuxKeepaliveInterval = 10`, `transport.heartbeatInterval = 10`, `transport.heartbeatTimeout = 25`, `transport.dialServerKeepalive = 30`.

   frps then noticed a silent path in 12–20 s, against 28–34 s at frp's defaults, and the set survived every head-of-line run. Do not go tighter for alpha:
   - **Budget each timeout.** A timeout must cover:
     - the heartbeat interval;
     - the full delay of a control message on a loaded tunnel, which includes head-of-line queueing;
     - the round trip;
     - a margin.
   - **What the queueing delay was here.** On this 1 MB/s fixture it reached 4.5 s behind one 4 MiB reply and 7.4 s behind one slow reader. It scales with the bytes ahead in the queue and inversely with the path's rate, but no fixed bound was found.
   - **The tight set failed this budget.** Its 6 s timeout dropped a healthy tunnel and cut every stream on it, in both passes.
   - **The fixed yamux timeout.** The yamux ping timeout is fixed at 10 s, so a queue of more than 10 s, possible on a slow path, trips it at any setting.
   - **The undetected window.** Requests sent before frps notices hang: most for up to `userConnTimeout` (10 s), and one holding a spare work connection until the session is torn down (more than 15 s at the defaults). So the front must enforce its own first-byte deadline and retry elsewhere, and HBEP-6's end-to-end probe through the path should decide routing rather than frps's view alone.
3. **Capacity per front (operational readiness, item 5).**
   - **CPU.** At 256 streams the front needed 7.9–8.7% of a core per 1,000 tokens/s over wss, and 6.6–6.9% over tcp.
     - Extrapolated linearly, one front core would carry about **11,500–12,600 tokens/s, 580–630 streams at 20 tokens/s over wss**, or 14,500–15,200 tokens/s (720–760 streams) over tcp.
     - This is an extrapolation from 36–39% of a core (wss). The cost per token was higher at 64 streams, and the front's behaviour near full CPU was not measured.
   - **Memory.** About 50 MB each for frps and Envoy at 256 streams.
   - **The link is the more likely limit.** Each token takes 290–335 bytes of TCP payload (body 222), so a path carries about payload bytes/s ÷ (300 × tokens/s per stream) streams before they slow. That is about 85 streams at 512 kB/s.
     - Divided the same way, the rule gives about 2,000 streams at 100 Mbit/s and 20,000 at 1 Gbit/s of payload. IP and TCP headers and ACKs take a few percent more of a physical link.
   - **Overload.** Past that point, every stream on the tunnel slows evenly and TTFT grows to seconds; nothing fails.
   - **Not measured.** Many tunnels on one front were not measured (Unknowns).
4. **Head-of-line blocking (Risks: "The tunnel slows streaming"): confirmed.** One large or slowly read reply stalled every token stream on its tunnel, whichever transport carried it. Here that meant 4.5 s behind a 4 MiB reply and up to 7.4 s behind a slow reader, at 1 MB/s. Mitigations, none tested here:
   - keep bulk endpoints (embeddings, file transfers, large non-streamed replies) off the token tunnel;
   - give bulk endpoints their own frpc proxy with frp's per-proxy `transport.bandwidthLimit`;
   - or carry them on separate connections (`tcpMux = false`), at the cost of a TCP and TLS handshake per stream. Behind a FIFO bottleneck that gains less than behind this fair-queueing shaper.

## Unknowns

- **Loss.** TCP head-of-line blocking caused by packet loss was not measured. toxiproxy is a userspace proxy and cannot drop packets, and `tc`/netem needs root. All the blocking above comes from a bandwidth cap.
- **Real bottlenecks.** On a real path the queue lives in the sender's socket buffer and the bottleneck router, whose size and discipline (FIFO or fair queueing) set how large the delay is. Here, loopback TCP never sees loss, so the socket buffers grow to their maximum (4 MiB send, 6 MiB receive), and the measured delays are close to the 6 MiB-window bound.
- **The front near full CPU.** The per-core capacity is extrapolated from at most 39% of a core.
- **The emulated path.** The latency toxic delays application data only, not TCP handshakes or ACKs, and the shaper's fair queueing is kinder to separate connections than a FIFO bottleneck.
- **Many tunnels per front.** One frpc was measured. frps's CPU and memory with hundreds of tunnels, and many tunnels sharing one link, were not.
- **CPU on a shared host.** The figures come from a shared VM (1-minute load 0.66–3.95 for the cells counted, 3–3.3 even with the other builders paused) with the client, engine and both tunnel ends on one host. The ratios between arms are more reliable than the absolute values.
- **The real front.** wss through HBEP-11's actual front on 443, and through HTTP proxies on a center's network, were not measured. Here Envoy terminated TLS on loopback.
- **frpc reconnect.** After a real partition, frpc's reconnect timing (its backoff) was not measured; see the heal note above.
- **The path for routing decisions.** The front may learn that a path is gone from the frps server plugin's `CloseProxy` call; that path's latency was not measured, only the admin API and the log.
- **ssh liveness.** The ssh arm's own liveness (`ServerAliveInterval` 10 × 3) was not tested. Nor was a dead engine behind a live tunnel (HBEP-6's end-to-end probe).

## Self-test and repeatability

`bash test.sh` (with `S1C_SCRATCH`, `S1C_SIM` and `S1C_ENVOY` set) runs, in order:
- the s1cbench unit tests;
- a rebuild of the s1cbench binary from this source, so the integration checks never run a stale binary;
- the result rules' unit tests;
- a check of the engine double's identity;
- the harness self-test;
- the leftover check. Expected values come from the inputs each check sets or from component source, not from the harness.

Output of the final run, after the review fixes:

```
$ bash test.sh
ok  	github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s1c/cmd/s1cbench	4.549s
rebuilt s1cbench from this source
.........
----------------------------------------------------------------------
Ran 9 tests in 0.003s

OK
engine: sha256 59d19f9d8f496d9d8228447c9f514d0bc96ffba4439b649db6e3e3d54b37709c
  mod	github.com/llm-d/llm-d-inference-sim	v1.0.0+dirty	
  build	vcs.revision=7bda03f9fef85139617cb4f724dccafe1cf8cbbf
  build	vcs.modified=true
PASS  engine paces one token per 50 ms (sim flag): rate p50 19.73 tok/s, gap p50 50.6 ms, 47/47 complete
PASS  latency toxic adds two one-way delays of 10 ms to a direct TTFT: clean 52.9 ms, lat20 73.9 ms, added 21.0 ms (want about 20)
PASS  bandwidth toxic caps a bulk transfer at 512,000 B/s: 1536000 B at 511124 B/s
PASS  a frozen toxiproxy (SIGSTOP, all threads stopped) is silent: a client close does not reach the server: 10 tries; failures (try, silent while frozen, EOF after SIGCONT): none
PASS  toxiproxy's timeout toxic (0) is NOT silent: it passes a close on (toxics/timeout.go): server saw EOF: True
PASS  toxiproxy bandwidth rate 0 does not block (toxics/bandwidth.go: rate <= 0 is unlimited): 1 MiB through rate 0 in 0.004 s
PASS  frpc wss straight to frps (TLS cert on frps) does not register: proxy status None; last frpc warning: ['[W] [client/service.go:346] connect to server error: unexpected EOF']
PASS  probe: a registered instance answers ok, an unknown one is refused (frps 404): inst ['ok'], nobody ['refused']
PASS  every listener in 29400-29499 is on 127.0.0.1: 11 listeners: ['127.0.0.1:29400', '127.0.0.1:29401', '127.0.0.1:29410', '127.0.0.1:29411', '127.0.0.1:29412', '127.0.0.1:29420', '127.0.0.1:29421', '127.0.0.1:29422', '127.0.0.1:29423', '127.0.0.1:29424', '127.0.0.1:29425']
PASS  nothing left listening on 29400-29499 after the checks: []
10 checks, 0 failed
$ ss -ltnH, ports 29400-29499: none
recorded processes still alive: none
```

**Repeatability.** Each matrix cell and head-of-line run was measured twice; the tables show both passes side by side.
- **Matrix.** Medians agree within a few ms. Throughput agrees within 1%, and under the cap within 1% too. Two rows vary more between passes:
  - the 256-stream TTFT under the cap (wss 2.2 s against 1.5 s);
  - small-sample p95 values (8 requests at 1 stream).
- **Head-of-line.** The runs reproduce. The ssh run that the first version reported as an exception was an attribution error (Results).
- **Heartbeats.** Each case's three runs fall inside the bound read from frp's source.

## Environment

- **VM.** Shared, 6 cores, 31 GB RAM, Linux 6.8. Other agents' sessions and spikes ran at the same time.
- **Load.** The VM was shared with other agents' builds and tests. Each matrix row records the 1-minute load at its start and end; head-of-line and heartbeat rows record it at the end.
  - **Gate.** Every cell first waited for a 1-minute load of at most 4.0 and at least 8 GB available.
  - **Retries.** A cell whose load was above 4.0 at its end (others' work plus the cell's own) was repeated, up to three times per try. Every attempt stays in the CSV.
  - **Re-runs.** Seventeen cells measured before a session restart at about 12:45 had ended above 4.0, and were re-run.
  - **Last cells.** The other builders were paused from 14:22 to 15:02 for the remaining 35 cells. Even then the base load stayed at 3 to 3.3, from other sessions and the root k3s.
  - **Cells counted.** The 112 matrix cells shown ran at a load of 0.66–3.92 at their start and 0.66–3.95 at their end. The 46 head-of-line runs shown ended at 0.77–4.00, and the 28 heartbeat runs at 0.53–1.67.
  - **Accepted limitation.** A cell's own processes add up to about 1.5–2 to the load at 256 streams, so most 256-stream cells ran at 2.5–3.9. The CPU figures come from a machine that was never idle.
- **Priority.** Every process ran at `nice -n 10`.
- **Socket buffers.** `tcp_wmem` max 4 MiB, `tcp_rmem` max 6 MiB.
- **Network.** Loopback only. Every listener was on 127.0.0.1 in ports 29400–29499, checked with `ss` after each start.

| Component | Version and pin | Project and licence | Use |
|---|---|---|---|
| frp (frps, frpc) | v0.71.1 linux_amd64, sha256 `1d987521…f803` (S1's pin) | [fatedier/frp](https://github.com/fatedier/frp), Apache-2.0 | the tunnel |
| toxiproxy | v2.13.0 linux_amd64, sha256 `08fd7b3d…6b3b4d`, from the release's checksums.txt, which agrees with GitHub's asset digest | [Shopify/toxiproxy](https://github.com/Shopify/toxiproxy), MIT | latency and bandwidth toxics |
| Envoy | 1.33.2 release binary, sha256 `bd2ff87c…fad741`, from the release's checksums.txt.asc (signature not verified); S2's binary | [envoyproxy/envoy](https://github.com/envoyproxy/envoy), Apache-2.0 | TLS front for wss |
| llm-d-inference-sim | v1.0.0 (commit 7bda03f) with S2's loopback patch (`third_party/forked/llm-d-inference-sim/`). S2's local build: sha256 `59d19f9d…709c`, Go-embedded `vcs.revision=7bda03f9…`, `vcs.modified=true`, checked by `test.sh` and `fetch.sh` | [llm-d/llm-d-inference-sim](https://github.com/llm-d/llm-d-inference-sim), Apache-2.0 | engine, 50 ms per token |
| OpenSSH | 9.6p1 (the VM's ssh and sshd binaries, run as the user) | [OpenSSH](https://www.openssh.com), BSD-style | ssh arm |
| fatedier/yamux | v0.2.0, inside frp (read only) | [fatedier/yamux](https://github.com/fatedier/yamux), MPL-2.0 | source for the keepalive bounds |
| Go, Python | 1.27.3; 3.15.0 | | s1cbench; the driver |

- **Reused from this project.** S1's frp pin and its candidate frps/frpc settings (branch `spike/s1-frp-dial-home`, `spikes/s1-frp/fetch-frp.sh` and `*.toml.tmpl`), and the shape of S1's throwaway sshd (`tests/throwaway-sshd.sh`): an unprivileged sshd with its own host key and authorized_keys. Also S2's simulator build.
- **New.** `s1cbench` and the Python driver.
- **Copied.** No third-party code is copied into this directory.

## Deviations from the issue

- **wss needs a TLS front.** The wss arm runs through Envoy 1.33.2, because frps cannot serve wss itself (Method). The issue asked for "wss with TLS on frps". The self-test shows that frps with its own certificate refuses frpc's wss connection.
- **The silent path is a frozen toxiproxy.** The issue suggested toxiproxy's bandwidth 0 or disabling the proxy. A rate of 0 means unlimited, and disabling closes both sockets (a FIN, like a clean exit); the self-test shows both. So a silent path is made by stopping the toxiproxy process with SIGSTOP. The "disable" case is kept as evidence.
- **The head-of-line control uses the shaper.** toxiproxy's cap applies per connection, which would hand the control with separate connections a win by construction. The control therefore runs through `s1cbench shaper`, whose one rate is shared by all connections. The shared-connection result was cross-checked through toxiproxy.
- **"20 ms latency" is read as the round trip:** 10 ms in each direction.
- **No packet loss.** It needs `tc`/netem, which needs root. Head-of-line blocking under loss is not measured (Unknowns).
- **Open-question numbers.** The issue calls capacity open question 5. In HBEP-6, open question 5 is multi-node instances and capacity is operational-readiness item 5, which this spike answers.
- **Extra runs.** These were added to the briefed set:
  - S1's settings as a third heartbeat setting;
  - a `tight` head-of-line run, to test the interplay between heartbeats and head-of-line blocking;
  - two more runs of the shared-connection bulk case on ssh and tcpmux, to check ssh's one run without blocking.
- **Re-derived after the review.** After the cross-model review of PR #78, these were re-derived from the runs' raw files, with no new measurements (`reanalyze.py`):
  - the CPU per token;
  - the head-of-line stall attribution and validity;
  - the heartbeat detection rule.
- **Pushed, though SPIKE-COMMON says not to push.** The branch is pushed because this spike's brief asked for it.

## Files and how to run

| File | What |
|---|---|
| `fetch.sh` | puts frp and toxiproxy (sha256-checked) into `$S1C_SCRATCH/bin`, checks Envoy, builds `s1cbench` |
| `cmd/s1cbench/` | Go, standard library only: `load`, `hol`, `bulk`, `probe`, `shaper`, with unit tests |
| `s1c.py` | driver: `matrix`, `hol`, `heartbeat`, `leftovers` |
| `s1c_lib.py`, `s1c_stack.py`, `s1c_heartbeat.py` | ports, processes (started at nice 10, stopped by recorded PID and start time), configs, toxiproxy, accounting |
| `s1c_selftest.py`, `test.sh` | the harness checks (below) |
| `s1c_analysis.py` | the result rules: CPU per token over one interval, head-of-line validity, heartbeat detection |
| `reanalyze.py` | re-derives the review's corrected columns from the raw files and rewrites `results/*.csv` |
| `tests/test_analysis.py` | unit tests of the result rules (run by `test.sh`) |
| `summarize.py` | the tables in this README, from `results/*.csv` |
| `results/` | `matrix.csv`, `hol.csv`, `heartbeat.csv`: one row per attempt, in measurement order, with its load. Re-runs supersede earlier attempts (Results) |

```sh
export S1C_SCRATCH=<scratch dir> S1C_SIM=<patched llm-d-inference-sim> S1C_ENVOY=<envoy 1.33.2>
bash fetch.sh
bash test.sh                                   # unit tests, harness self-test, leftover check
python3 s1c.py matrix --pass 1; python3 s1c.py matrix --pass 2
python3 s1c.py hol --pass 1;    python3 s1c.py hol --pass 2
python3 s1c.py heartbeat --reps 3
python3 reanalyze.py                           # only to re-derive columns from the raw files
python3 summarize.py
python3 s1c.py leftovers
```
