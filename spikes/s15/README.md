# Spike S15: bulk transfers beside token streams in one tunnel (HBEP-6, open question 12)

Issue #336. One Instance's tunnel carries 8 token streams at 20 tokens/s while one bulk transfer runs: a 64 MiB upload, a 64 MiB non-streamed download, or a 2,000-input embeddings reply. Four layouts are compared: (a) one proxy, (b) a second proxy with frp's `bandwidthLimit` at 25, 50 and 75% of the path rate, (c) a separate frpc with `tcpMux = false`, and, in host-dialed mode only, (d) a relay of its own for that frpc. They run in direct and host-dialed mode, on loopback through S1c's link emulator at S1's probed path rate and round trip. The measurements ran on GitHub-hosted runners, three repetitions on three fresh runners.

## Findings

The answer to open question 12: carry bulk paths on a separate frpc with `tcpMux = false`, layout (c). That frpc needs a second frps behind the same dial-home listener; the verdict prices that. Layout (b) does not protect the token streams. Layout (d) gains 3 ms over (c) and costs a relay, so it is not worth that. All 99 configurations were valid in all three repetitions, on three AMD EPYC 9V45 runners with 4 CPUs. "Valid" means every one of the 8 streams was streaming through every transfer's span, and received a token in every second of the reference window.

- **A download or a large reply on the token streams' own frp session stalls them for about frp's stream window over the path rate.**
  - In layout (a), the gap p99 during a 64 MiB download was 137 ms in direct mode and 206 ms in host-dialed mode, against 52 ms with no transfer; the longest gap was 286 and 235 ms.
  - The 2,000-input embeddings reply (about 25 MB) did the same: 138 and 192 ms.
  - frp gives every stream of its session 6 MiB of send credit (frp facts). 6 MiB over the measured path rate is 133 ms at 47.4 MB/s and 185 ms at 34 MB/s: the stall is that queue.
- **Uploads do not stall the token streams.** An upload flows front to node, the token streams node to front. The gap p99 during a 64 MiB upload was 52 ms in every layout and mode.
- **frp's `bandwidthLimit` (layout (b)) does not prevent the stall.**
  - Its limiter starts with a full burst of one second's bytes, so the first `cap` bytes go at line rate. A transfer smaller than one second's cap never meets the cap at all: at 50% and 75% of the path rate, the embeddings reply went at full rate, and at 75% so did the 64 MiB transfers.
  - When the cap binds, the rate after the first second was the cap exactly (11.8 and 23.7 MB/s in direct mode). The burst's stall stayed (p99 110–142 ms in direct mode, 87–203 ms in host-dialed mode), and the transfer took 1.3–3.5 times as long.
  - One limiter serves both directions and every connection of the proxy.
- **A separate frpc with `tcpMux = false` (layout (c)) removes the stall in both modes, at the full path rate.**
  - Gap p99 during a download was 53 ms in direct mode and 55 ms in host-dialed mode (max 54 and 60 ms); during an embeddings reply, 53 and 55 ms.
  - In direct mode its transfers took their own connections on the link: 9 connections over a run, 6 of them carrying more than 1 MiB, against 2 and 1 in layout (a).
  - In host-dialed mode its connections are streams of the one serving relay, whose yamux window is 256 KiB, not 6 MiB. That bounds the bulk stream's queue to 256 KiB over 34 MB/s, about 7.7 ms. The link carried only the 2 relays.
- **A relay of its own (layout (d), host-dialed only) gains about 3 ms over (c):** gap p99 52 ms, max 53. It costs a third relay per Allocation.
- **The front host's CPU is not what decides.** During the transfers the front used at most 0.13 cores in any layout, against 0.01–0.03 with no transfer. Layout (c)'s second frps adds a process, not CPU.
- **No tunnel was lost** in 105 runs: 0 drops, 0 frpc re-logins, 0 relay restarts, 0 heartbeat timeouts.
- **For #273:** the node read an upload's last byte 0.14–0.34 s after the client's last write without a cap, and up to 0.92 s with one. A front-side timing that stops at the front's last write misses that tail.

## Question

HBEP-6's open question 12, which blocks beta: "embeddings, file transfers and large non-streamed replies get their own frpc proxy with `transport.bandwidthLimit`, or a separate frpc with `transport.tcpMux = false`. S1c tested neither." S1c measured head-of-line blocking between token streams that share frp's one multiplexed connection, with up to 6 MiB of send credit per stream. The issue asks, per layout and transfer:
- the token streams' inter-token gap at p50, p99 and max during the transfer, against the same streams with no transfer;
- the transfer's time and rate at the receiver (for uploads the job side, the number #273 lacks);
- the front host's CPU;
- the layout to adopt and its cost, a proposed text for open question 12, and a note on #273.

## Method

### The layouts

| Layout | Token streams | Bulk transfer | Front side |
|---|---|---|---|
| (a) one proxy | frpc's tcpmux proxy `<uid>` | the same proxy | frps |
| (b) a capped bulk proxy | `<uid>` | a second proxy `<uid>-bulk` on the same frpc, with `transport.bandwidthLimit` at 25, 50 or 75% of the calibrated path rate, `bandwidthLimitMode = "client"` | frps |
| (c) a separate frpc | `<uid>` on the first frpc | a second frpc with `transport.tcpMux = false` and its own proxy `<uid>-bulk` | a second frps with `tcpMux = false`, reached through the same dial-home listener by TLS server name |
| (d) a relay of its own (host-dialed only) | `<uid>` | (c)'s second frpc, through a relay of its own | (c)'s second frps |

- **Why (c) needs a second frps.** frps with `tcpMux` on, HBEP-6's frps and frp's default, wraps every connection to its bind port in a multiplexed server session, the websocket listener's included. So an frpc with `tcpMux = false` cannot log in to it (frp facts below).
  - The second frps runs with `tcpMux = false`, and S1's heartbeat set is explicit, since with mux off frp's heartbeat timeout defaults to 90 s.
  - The listener sends a websocket whose TLS server name is `bulk.dial-home.hpcburst.invalid` (a second name on the certificate) to it. The front has no such routing today.
  - The front's transport needs a second `CONNECT` target, the second frps's tcpmux port.
- **Why (d).** In host-dialed mode, (c)'s second frpc dials the job side's serving listener like the first, so its connections become streams of the one serving relay's session, over the one pipe. (d) gives it a relay of its own, to (c)'s second frps.
  - `internal/hostdial` has two channels, and its `Serve` dials only base+7, so (d) is built inside the spike with hostdial unchanged.
  - It is a second job side with no engine (`relay_only`), plus a second Target of the same `Owner`, whose serving relay carries only the bulk frpc.
  - Each relay is its own `s15bridge` process, its own link connection and its own yamux session with hostdial's session config.
  - What differs from a real third channel: this is a second Allocation's pair, so it runs 4 relays per Instance (the second pair's control relay idle), where a real `bulk` channel would make 3.

### The path

```
direct       client --CONNECT--> frps (tcpmux)            ==yamux in frpc's wss==   link ==> listener (TLS) ==> frps
                                 (c) frps2 (tcpmux, mux off) == one wss per request ==  link ==> listener (SNI) ==> frps2
host-dialed  frpc --> job side --yamux stream--> relay step --TCP--> link --> ssh stand-in --pipes--> owner --> listener --> frps
                                                                                (d): a second job side and relay for the bulk frpc
```

- **The link** is S1c's shared-rate shaper (`spikes/s1c/cmd/s1cbench/shaper.go`) with three additions, in `cmd/s15link`:
  - a one-way delay per direction (1.15 ms: a 2.3 ms round trip, S1's probe);
  - a bounded receive buffer of its own (`SO_RCVBUF` 256 KiB), so a sender's queue builds in its own socket buffer, as before a real bottleneck;
  - per-connection counts.

  Its rate is 48 MB/s each way, the low end of S1's 48–85 MB/s probes, where head-of-line blocking is worst. It is shared by every connection, with per-connection queues, as in S1c. Behind a FIFO bottleneck, separate connections would gain less (Unknowns).
- **The engine and the bulk paths** are on the Instance's one socket: `internal/agent/enginedouble` (50 ms before each token) for the streams; `POST /bulk/upload`, `GET /bulk/download?bytes=N` and `POST /v1/embeddings` beside it (`node/bulk.go`).
  - The embeddings reply is 2,000 embeddings of 1,024 float32 values as JSON (25,258,379 bytes), in the OpenAI-compatible shape vLLM serves, sent at once with a `Content-Length`. The double computes nothing, so the reply's cost is its transfer.
- **Everything else is spike S14's harness**: frps and frpc v0.71.1 with HBEP-6's settings; the dial-home listener with internal/front's passthrough (copied, with its parity test); `hostdial.Owner`, `Job` and `Relay`, unchanged; the ssh stand-in; the driver's process records and `/proc` sampler.

### The load and the measures

- **Token streams:** 8 streams, each one request of 2,000 tokens (100 s at 20 tokens/s, longer than a run), started over 2 s. No stream is between requests while a transfer runs.
- **Transfers:** after a 10 s reference window with no transfer, 5 transfers of one kind, one at a time, each after a 4 s pause.
- **Calibration:** per mode and repetition, one uncapped 64 MiB download in layout (a) with no stream, measured at the receiver. Layout (b)'s caps are 25, 50 and 75% of it, written in frp's MB of 1,048,576 bytes.
- **Gaps:** a gap belongs to a transfer when its interval overlaps the transfer's span (S1c's attribution), and to the reference when both tokens fall inside the reference window.
- **The receiver's view:** a download's or an embeddings reply's first and last byte at the client, an upload's at the node, with the bytes received by each 100 ms after the first. Those steps show a cap's burst and whether the cap binds.
- **For #273:** for an upload, how long after the client's last write the node read the last byte.
- **The front host's CPU:** frps (+ the second frps) + listener (+ owner + ssh stand-ins), from `/proc` every 0.5 s, over the reference window and over the transfers' phase.
- **Connections:** the link's per-connection bytes, and each frpc's established connections (`ss`) every 0.5 s. They show whether a layout put a second connection on the link.
- **Tunnel losses:** frps's proxy lists every 10 s, frpc logins beyond the first, and relay sessions beyond the first.
- **Validity** (the science agent's ruling on #336, 10 Nov 2026):
  - every transfer completed, with every byte at the receiver;
  - through every transfer's whole span, every one of the 8 streams had a request in flight that delivered tokens before and after it;
  - in the reference window, every stream received a token in every second.

  Seconds with no token inside a transfer are the stall being measured, not a pause. Streams that pause during the transfer, or a transfer that ends before the streams start, would prove nothing; the rule refuses both (`analysis.validity`, with its tests).

## Results

All tables come from `results/rep{1,2,3}/`, rendered by `python3 summarize.py tables`. The gaps are recomputed from every token's committed arrival (`streams.csv`), the transfers from `transfers.csv`, and the CPU from `samples.csv`. Every run finished (status `ok`), and on every runner nothing was left running and no listener stayed in 29980–29999 (`s15.py leftovers`, in each job's log).

### Path rate (calibration)

One uncapped 64 MiB download in layout (a), with no stream, per mode and repetition; the median of its transfers at the receiver. Layout (b)'s caps are 25, 50 and 75% of it, in frp's MB of 1,048,576 bytes.

| mode | path rate, MB/s (reps) |
|---|---|
| direct | 47.2 / 47.3 / 47.5 |
| host-dialed | 31.7 / 33.3 / 33.9 |

### The token streams during a transfer

Inter-token gaps of the 8 streams, pooled over the repetitions' valid runs (5 transfers each): during a transfer (a gap overlapping a transfer's span) against the same run's reference window with no transfer. The engine double sends a token every 50 ms.

| mode | transfer | layout | runs valid | reference gap p50 / p99 / max, ms | gap during the transfer p50 / p99 / max, ms | gaps during |
|---|---|---|---:|---|---|---:|
| direct | download | a | 3/3 | 50.9 / 51.6 / 53.1 | 51.2 / 136.8 / 285.6 | 3484 |
| direct | download | b25 | 3/3 | 50.8 / 51.7 / 54.1 | 50.6 / 122.1 / 145.1 | 11217 |
| direct | download | b50 | 3/3 | 50.8 / 51.6 / 52.8 | 50.5 / 134.5 / 147.0 | 4504 |
| direct | download | b75 | 3/3 | 50.9 / 51.9 / 52.2 | 49.8 / 138.2 / 294.5 | 3524 |
| direct | download | c | 3/3 | 50.8 / 51.9 / 52.3 | 50.5 / 53.0 / 54.3 | 3459 |
| direct | embeddings | a | 3/3 | 50.9 / 51.8 / 52.5 | 50.3 / 138.1 / 145.3 | 1497 |
| direct | embeddings | b25 | 3/3 | 50.9 / 51.9 / 52.6 | 50.5 / 139.4 / 300.2 | 2986 |
| direct | embeddings | b50 | 3/3 | 50.9 / 51.7 / 52.7 | 49.2 / 230.7 / 334.5 | 1545 |
| direct | embeddings | b75 | 3/3 | 50.8 / 51.8 / 52.7 | 50.4 / 139.3 / 147.6 | 1497 |
| direct | embeddings | c | 3/3 | 50.9 / 51.9 / 52.4 | 50.7 / 53.0 / 53.4 | 1484 |
| direct | upload | a | 3/3 | 50.9 / 52.0 / 52.3 | 50.4 / 52.2 / 54.3 | 3467 |
| direct | upload | b25 | 3/3 | 50.8 / 51.9 / 52.5 | 50.8 / 52.2 / 53.6 | 11227 |
| direct | upload | b50 | 3/3 | 50.6 / 52.0 / 55.8 | 50.5 / 52.5 / 56.5 | 4498 |
| direct | upload | b75 | 3/3 | 50.9 / 51.9 / 52.1 | 50.4 / 52.1 / 53.7 | 3470 |
| direct | upload | c | 3/3 | 50.9 / 51.9 / 52.4 | 50.4 / 52.1 / 55.8 | 3464 |
| host-dialed | download | a | 3/3 | 50.6 / 51.8 / 53.9 | 25.1 / 206.1 / 234.6 | 4806 |
| host-dialed | download | b25 | 3/3 | 50.7 / 51.9 / 52.8 | 50.4 / 93.1 / 220.6 | 17101 |
| host-dialed | download | b50 | 3/3 | 50.6 / 51.8 / 53.1 | 50.4 / 189.8 / 251.4 | 7427 |
| host-dialed | download | b75 | 3/3 | 50.6 / 51.7 / 52.6 | 3.8 / 218.0 / 247.9 | 4884 |
| host-dialed | download | c | 3/3 | 50.6 / 51.7 / 56.1 | 50.6 / 55.0 / 59.6 | 4787 |
| host-dialed | download | d | 3/3 | 50.5 / 51.7 / 53.4 | 50.6 / 52.3 / 53.2 | 4822 |
| host-dialed | embeddings | a | 3/3 | 50.5 / 51.7 / 52.8 | 31.8 / 192.1 / 221.0 | 1979 |
| host-dialed | embeddings | b25 | 3/3 | 50.6 / 51.7 / 52.3 | 50.3 / 173.4 / 220.4 | 5256 |
| host-dialed | embeddings | b50 | 3/3 | 50.6 / 51.7 / 53.0 | 35.7 / 189.1 / 216.8 | 2034 |
| host-dialed | embeddings | b75 | 3/3 | 50.6 / 51.8 / 53.9 | 31.7 / 204.0 / 223.6 | 2008 |
| host-dialed | embeddings | c | 3/3 | 50.7 / 51.8 / 58.7 | 50.5 / 54.7 / 55.9 | 1980 |
| host-dialed | embeddings | d | 3/3 | 50.5 / 51.8 / 53.1 | 50.5 / 52.3 / 53.2 | 2030 |
| host-dialed | upload | a | 3/3 | 50.7 / 51.7 / 52.4 | 50.5 / 52.3 / 54.7 | 4686 |
| host-dialed | upload | b25 | 3/3 | 50.6 / 51.7 / 52.5 | 50.6 / 52.0 / 54.9 | 17084 |
| host-dialed | upload | b50 | 3/3 | 50.7 / 51.8 / 52.5 | 50.4 / 52.1 / 53.6 | 7484 |
| host-dialed | upload | b75 | 3/3 | 50.6 / 51.6 / 54.0 | 50.5 / 52.2 / 54.8 | 4676 |
| host-dialed | upload | c | 3/3 | 50.6 / 51.2 / 52.9 | 50.5 / 52.0 / 53.1 | 4860 |
| host-dialed | upload | d | 3/3 | 50.6 / 51.9 / 57.6 | 50.5 / 52.0 / 53.5 | 4816 |

### The bulk transfer at the receiver

Median over the repetitions (range in brackets). The rate is bytes over the receiver's first-to-last byte; the first second's bytes and the rate after it show a cap's burst and whether it binds. For an upload the receiver is the node.

| mode | transfer | layout | cap, MB/s | rate, MB/s | time, s | first second, MB | rate after the first second, MB/s | binds |
|---|---|---|---:|---:|---:|---:|---:|---|
| direct | download | a | - | 47.4 (47.3–47.6) | 1.42 (1.41–1.42) | 46.7 (46.4–47.2) | 48.9 (48.8–49.5) | - |
| direct | download | b25 | 11.8 (11.8–11.9) | 14.4 (14.3–14.4) | 4.67 (4.65–4.68) | 23.6 (23.6–23.7) | 11.8 (11.8–11.9) | yes |
| direct | download | b50 | 23.7 (23.6–23.8) | 36.6 (36.4–36.8) | 1.84 (1.83–1.84) | 47.3 (47.2–47.5) | 23.7 (23.6–23.8) | yes |
| direct | download | b75 | 35.5 (35.4–35.6) | 47.4 (47.3–47.5) | 1.42 (1.41–1.42) | 46.7 (45.9–46.8) | 49.6 (49.2–50.9) | no |
| direct | download | c | - | 47.9 | 1.40 | 47.9 | 47.9 (47.8–47.9) | - |
| direct | embeddings | a | - | 47.6 (47.4–47.7) | 0.53 | - | - | - |
| direct | embeddings | b25 | 11.8 (11.8–11.9) | 21.6 (21.6–21.8) | 1.17 (1.16–1.17) | 23.3 (23.2–23.4) | 11.9 (11.9–12.0) | yes |
| direct | embeddings | b50 | 23.7 (23.6–23.8) | 47.5 (47.2–47.5) | 0.53 | - | - | no (done in the first second) |
| direct | embeddings | b75 | 35.5 (35.4–35.6) | 47.6 (47.6–47.7) | 0.53 | - | - | no (done in the first second) |
| direct | embeddings | c | - | 47.9 | 0.53 | - | - | - |
| direct | upload | a | - | 48.0 (47.9–48.0) | 1.40 | 47.9 (47.8–48.0) | 48.2 (48.2–48.3) | - |
| direct | upload | b25 | 11.8 (11.8–11.9) | 14.4 (14.3–14.4) | 4.67 (4.65–4.68) | 23.6 (23.6–23.7) | 11.8 (11.8–11.9) | yes |
| direct | upload | b50 | 23.7 (23.6–23.8) | 36.6 (36.4–36.7) | 1.84 (1.83–1.84) | 47.3 (47.1–47.5) | 23.7 (23.7–23.8) | yes |
| direct | upload | b75 | 35.5 (35.4–35.6) | 48.0 (47.9–48.0) | 1.40 | 47.9 (47.6–47.9) | 48.3 (48.1–48.7) | no |
| direct | upload | c | - | 48.0 | 1.40 | 48.0 | 47.9 | - |
| host-dialed | download | a | - | 33.8 (33.6–34.6) | 1.99 (1.94–1.99) | 32.1 (32.1–32.8) | 34.8 (33.9–36.8) | - |
| host-dialed | download | b25 | 8.3 (7.9–8.5) | 9.5 (9.0–9.7) | 7.05 (6.91–7.45) | 16.7 (15.9–17.0) | 8.3 (7.9–8.5) | yes |
| host-dialed | download | b50 | 16.7 (15.9–17.0) | 22.2 (20.8–22.7) | 3.02 (2.95–3.23) | 33.3 (31.8–33.9) | 16.7 (15.9–17.0) | yes |
| host-dialed | download | b75 | 25.0 (23.8–25.4) | 33.2 (33.0–34.9) | 2.02 (1.92–2.03) | 31.1 (29.8–33.7) | 36.2 (35.0–36.6) | no |
| host-dialed | download | c | - | 35.1 (31.6–35.7) | 1.91 (1.88–2.12) | 34.3 (31.8–35.3) | 35.7 (31.4–35.8) | - |
| host-dialed | download | d | - | 33.8 (33.0–35.4) | 1.98 (1.89–2.03) | 32.9 (32.7–34.5) | 34.8 (33.3–36.1) | - |
| host-dialed | embeddings | a | - | 34.4 (33.7–34.9) | 0.73 (0.72–0.75) | - | - | - |
| host-dialed | embeddings | b25 | 8.3 (7.9–8.5) | 12.2 (11.3–12.5) | 2.07 (2.02–2.23) | 16.3 (15.5–16.6) | 8.3 (7.9–8.5) | yes |
| host-dialed | embeddings | b50 | 16.7 (15.9–17.0) | 33.3 (33.2–34.1) | 0.76 (0.74–0.76) | - | - | no (done in the first second) |
| host-dialed | embeddings | b75 | 25.0 (23.8–25.4) | 33.9 (33.5–35.1) | 0.74 (0.72–0.75) | - | - | no (done in the first second) |
| host-dialed | embeddings | c | - | 35.3 (34.5–35.4) | 0.72 (0.71–0.73) | - | - | - |
| host-dialed | embeddings | d | - | 32.6 (32.0–34.2) | 0.78 (0.74–0.79) | - | - | - |
| host-dialed | upload | a | - | 35.1 (34.7–35.7) | 1.91 (1.88–1.94) | 34.7 (33.3–35.3) | 35.5 (35.3–35.8) | - |
| host-dialed | upload | b25 | 8.3 (7.9–8.5) | 9.5 (9.0–9.7) | 7.05 (6.91–7.45) | 16.6 (15.9–16.9) | 8.3 (7.9–8.5) | yes |
| host-dialed | upload | b50 | 16.7 (15.9–17.0) | 22.2 (20.8–22.7) | 3.03 (2.96–3.23) | 33.0 (31.6–33.6) | 16.8 (15.9–17.0) | yes |
| host-dialed | upload | b75 | 25.0 (23.8–25.4) | 35.4 (34.8–36.2) | 1.90 (1.85–1.93) | 34.3 (33.9–34.7) | 36.2 (35.8–37.2) | no |
| host-dialed | upload | c | - | 34.1 (34.0–34.5) | 1.97 (1.95–1.97) | 33.8 (33.1–34.2) | 34.8 (34.0–35.0) | - |
| host-dialed | upload | d | - | 34.2 (34.0–35.5) | 1.96 (1.89–1.98) | 34.1 (33.8–35.7) | 34.6 (34.0–35.3) | - |

### Uploads: when the receiver had the last byte

How long after the client's last write the node read the upload's last byte: the part of an upload a front-side timing does not see (#273).

| mode | layout | receiver's last byte after the client's last write, ms |
|---|---|---:|
| direct | a | 143 (142–144) |
| direct | b25 | 618 (613–878) |
| direct | b50 | 301 (301–304) |
| direct | b75 | 144 (138–148) |
| direct | c | 225 (221–256) |
| host-dialed | a | 194 (170–199) |
| host-dialed | b25 | 921 (859–1247) |
| host-dialed | b50 | 455 (434–610) |
| host-dialed | b75 | 193 (189–195) |
| host-dialed | c | 342 (340–359) |
| host-dialed | d | 344 (320–388) |

### Connections and the front's CPU

The link's connections over the run (the emulator's per-connection counts), those that carried more than 1 MiB, and the bulk frpc's established connections at their most. Front CPU is frps (+ the second frps) + listener (+ owner + ssh stand-ins), in cores, over the reference window and over the transfers' phase.

| mode | transfer | layout | link connections | over 1 MiB | bulk frpc connections | front cores, reference | front cores, transfers | host busy, transfers |
|---|---|---|---:|---:|---:|---:|---:|---:|
| direct | download | a | 2 | 1 | - | 0.015 (0.013–0.027) | 0.094 (0.090–0.117) | 0.05 (0.05–0.07) |
| direct | download | b25 | 2 | 1 | - | 0.013 (0.013–0.026) | 0.075 (0.075–0.095) | 0.06 (0.05–0.07) |
| direct | download | b50 | 2 | 1 | - | 0.015 (0.013–0.027) | 0.092 (0.089–0.100) | 0.06 (0.05–0.06) |
| direct | download | b75 | 2 | 1 | - | 0.014 (0.013–0.015) | 0.092 (0.091–0.093) | 0.05 |
| direct | download | c | 9 | 6 | 3 | 0.015 (0.012–0.017) | 0.080 (0.077–0.085) | 0.05 |
| direct | embeddings | a | 2 | 1 | - | 0.015 (0.013–0.025) | 0.050 (0.049–0.067) | 0.04 (0.03–0.05) |
| direct | embeddings | b25 | 2 | 1 | - | 0.015 (0.013–0.023) | 0.054 (0.050–0.060) | 0.04 (0.03–0.04) |
| direct | embeddings | b50 | 2 | 1 | - | 0.013 | 0.047 (0.044–0.049) | 0.03 |
| direct | embeddings | b75 | 2 | 1 | - | 0.015 (0.012–0.017) | 0.049 (0.049–0.050) | 0.03 |
| direct | embeddings | c | 9 | 6 | 3 | 0.015 (0.014–0.015) | 0.042 (0.041–0.047) | 0.03 |
| direct | upload | a | 2 | 1 | - | 0.013 (0.013–0.014) | 0.060 (0.059–0.061) | 0.04 |
| direct | upload | b25 | 2 | 1 | - | 0.014 (0.013–0.029) | 0.037 (0.037–0.047) | 0.04 (0.04–0.05) |
| direct | upload | b50 | 2 | 1 | - | 0.015 (0.013–0.021) | 0.059 (0.056–0.065) | 0.04 (0.04–0.05) |
| direct | upload | b75 | 2 | 1 | - | 0.012 (0.012–0.013) | 0.055 (0.055–0.058) | 0.04 |
| direct | upload | c | 9 | 6 | 3 | 0.013 (0.012–0.017) | 0.029 (0.027–0.032) | 0.04 |
| host-dialed | download | a | 2 | 1 | - | 0.028 (0.025–0.031) | 0.110 (0.103–0.112) | 0.06 |
| host-dialed | download | b25 | 2 | 1 | - | 0.027 (0.025–0.031) | 0.109 (0.105–0.111) | 0.07 (0.06–0.07) |
| host-dialed | download | b50 | 2 | 1 | - | 0.030 (0.025–0.032) | 0.127 (0.125–0.132) | 0.08 (0.07–0.08) |
| host-dialed | download | b75 | 2 | 1 | - | 0.029 (0.025–0.031) | 0.108 (0.107–0.119) | 0.06 (0.06–0.07) |
| host-dialed | download | c | 2 | 1 | 3 | 0.027 (0.026–0.028) | 0.103 (0.101–0.104) | 0.06 |
| host-dialed | download | d | 4 | 2 | 3 | 0.026 (0.024–0.028) | 0.099 (0.097–0.101) | 0.06 |
| host-dialed | embeddings | a | 2 | 1 | - | 0.028 (0.022–0.029) | 0.068 (0.061–0.071) | 0.04 |
| host-dialed | embeddings | b25 | 2 | 1 | - | 0.028 (0.026–0.028) | 0.075 (0.074–0.083) | 0.05 |
| host-dialed | embeddings | b50 | 2 | 1 | - | 0.028 (0.025–0.029) | 0.066 (0.065–0.067) | 0.04 |
| host-dialed | embeddings | b75 | 2 | 1 | - | 0.028 (0.028–0.029) | 0.070 (0.068–0.073) | 0.04 (0.04–0.05) |
| host-dialed | embeddings | c | 2 | 1 | 3 | 0.029 (0.027–0.029) | 0.064 (0.056–0.066) | 0.04 (0.03–0.04) |
| host-dialed | embeddings | d | 4 | 2 | 3 | 0.025 (0.023–0.025) | 0.056 (0.056–0.063) | 0.04 |
| host-dialed | upload | a | 2 | 1 | - | 0.029 (0.025–0.031) | 0.086 (0.086–0.096) | 0.06 (0.05–0.06) |
| host-dialed | upload | b25 | 2 | 1 | - | 0.026 (0.022–0.027) | 0.053 (0.049–0.053) | 0.04 |
| host-dialed | upload | b50 | 2 | 1 | - | 0.028 (0.025–0.029) | 0.080 (0.074–0.082) | 0.05 (0.05–0.06) |
| host-dialed | upload | b75 | 2 | 1 | - | 0.027 (0.025–0.030) | 0.092 (0.085–0.097) | 0.06 (0.05–0.06) |
| host-dialed | upload | c | 2 | 1 | 3 | 0.025 (0.023–0.030) | 0.064 (0.058–0.067) | 0.05 (0.04–0.05) |
| host-dialed | upload | d | 4 | 2 | 3 | 0.028 (0.026–0.028) | 0.059 (0.059–0.061) | 0.04 (0.04–0.05) |

### Tunnel losses

Summed over all 105 runs: frps's lists (a proxy not online or registered again), frpc logins beyond the first (both frpc), relay sessions beyond the first, and heartbeat-timeout lines.

| drops | frpc re-logins | relay restarts | heartbeat-timeout lines | unreadable lists | runs not ok |
|---:|---:|---:|---:|---:|---:|
| 0 | 0 | 0 | 0 | 0 | 0 |

### Runners

- rep 1: AMD EPYC 9V45 96-Core Processor, 4 CPUs, 15983 MB, Linux 7.0.0-1017-azure
- rep 2: AMD EPYC 9V45 96-Core Processor, 4 CPUs, 15983 MB, Linux 7.0.0-1017-azure
- rep 3: AMD EPYC 9V45 96-Core Processor, 4 CPUs, 15983 MB, Linux 7.0.0-1017-azure

**How to read the tables:**
- **Gaps.** The reference gap p99 is 51–52 ms in every row: the engine double's 50 ms, plus the path. A row's "during" figures are the transfers' effect. Each configuration pooled 1,500–17,000 gaps during transfers over its three repetitions, so p99 rests on 15 or more gaps.
- **Per repetition,** the download's gap p99 ranged as follows:
  - direct mode: 136–141 ms in (a), 53 ms in (c);
  - host-dialed mode: 186–222 ms in (a), 55 ms in (c), 52 ms in (d).

  The repetitions agree.
- **The gap p50 below 50 ms** in host-dialed (a) and (b75) is tokens released together after a stall, not faster tokens.
- **Caps** are in frp's MB (1,048,576 bytes) and shown in MB/s of 10^6 bytes. "Binds" compares the rate after the first second with the cap. "Done in the first second" means the transfer was smaller than the burst.
- **Host-dialed mode's path rate** was 31.7–33.9 MB/s against the link's 48 MB/s: the relay chain (the stand-in, the owner and the relay step, each a process copying bytes) on a 4-CPU runner. Its caps were set from that rate.

## Verdict

**Adopt layout (c) for bulk paths, in both modes:** a separate frpc with `tcpMux = false`, its own proxy for the bulk paths, and a second frps behind the dial-home listener.
- **Why:** a bulk transfer stalls the token streams for its stream window over the path rate. (a) and (b) share frp's session, whose window is a fixed 6 MiB (`server/service.go:752`, `client/connector.go:154`; not configurable at v0.71.1). (c) moves the bulk bytes off it:
  - in direct mode onto connections of their own;
  - in host-dialed mode onto streams of the serving relay, whose window is 256 KiB.

  The stall falls from 137–206 ms to under 10 ms at p99, at the full path rate.
- **What it costs:**
  - **a second frps** with `tcpMux = false` and S1's heartbeat set explicit (with mux off frp's timeout default is 90 s). frps with `tcpMux` on cannot serve a mux-off frpc, so it can't be the same frps. One second frps serves every Instance; it held 25–29 MB here;
  - **the dial-home listener routing** that frpc's websocket by TLS server name (`bulk.dial-home.hpcburst.invalid`, a second name on the launcher's certificate), a change to `internal/front`;
  - **a second CONNECT target** in the front's transport, chosen by path: embeddings, file uploads and downloads, and other non-streamed replies that may be large;
  - **a second frpc per Instance** in the job (16–21 MB here), with its own proxy, staged and supervised by the agent like the first;
  - **a new TLS connection per bulk request** through the listener (mux off: one work connection per request). That is one handshake per request, a few round trips on the path;
  - **in host-dialed mode, nothing more:** the second frpc reaches the job side's serving listener like the first.
- **One constraint follows:** the host-dialed relay's per-stream window must stay small. `internal/hostdial` uses yamux's default of 256 KiB. Raised to frp's 6 MiB, the serving relay would stall the token streams behind (c)'s bulk streams again.
  - **Its price:** a 256 KiB window caps every relay stream's rate at 256 KiB ÷ round trip: about 114 MB/s at 2.3 ms, and about 26 MB/s at 10 ms.
  - In host-dialed mode that bounds the bulk path's own rate on a longer round trip, since (c) rides the same relay.
  - Only 2.3 ms was measured here; a longer path would trade a lower bulk rate for the same low token latency, or need the window sized to the path.
- **Not layout (b).** frp's limiter starts with a full one-second burst, so it never meets a transfer smaller than one second's cap. When it binds, the stall at the start remains (p99 87–203 ms), and every transfer is 1.3–3.5 times slower. One limiter also caps both directions of the proxy.
  - **Why no cap could help much.** On frp's session the bulk stream can queue at most the smaller of the burst and frp's 6 MiB window, so the stall is about min(burst, 6 MiB) ÷ path rate.
  - Every cap #336 named sets a burst above 6 MiB: at 25% of 47 MB/s the burst is 11.8 MB. So all of them leave (a)'s stall, as measured.
  - A cap shrinks the stall below (a)'s only when one second of it is under 6 MiB: below about 6.3 MB/s, 13% of the direct path and 19% of host-dialed. That cap was not run. It would stretch a 64 MiB transfer to about 11 s, and the stall would still be the burst over the path rate.
- **Not layout (d).** A relay of its own gains about 3 ms at p99 over (c) in host-dialed mode. Its cost is 3 relays per Allocation (4 in this stand-in, Method). Under HBEP-4's count of 4 relays per login host, only one Allocation then fits, which breaks the handover pair unless the count is raised from a measurement.
- **Uploads need nothing.** They flow against the token streams and stalled nothing in any layout; on the separate frpc they are equally fine.
- **What the verdict rests on, and what it does not:**
  - one bulk transfer at a time, on loopback at 48 MB/s with a 2.3 ms round trip, and a link that queues per connection;
  - behind a FIFO bottleneck, direct mode's (c) would keep the bulk bytes off frp's 6 MiB window but share the bottleneck's queue. Host-dialed mode's (c) does not depend on that, since the relay is one connection either way (Unknowns).

## Proposed HBEP-6 text for open question 12

For the science agent to amend. It replaces the item's current text.

> 12. **Bulk endpoints off the token tunnel.** **Answered for one transfer at a time** (spike S15, issue #336): on loopback at 48 MB/s with a 2.3 ms round trip, 8 token streams of 20 tokens/s ran beside a 64 MiB upload, a 64 MiB download or a 2,000-input embeddings reply, in direct and host-dialed mode.
>     - **On one frp session a download or a large reply stalls the token streams** for about frp's stream window, a fixed 6 MiB, over the path rate. Gap p99 was 137 ms in direct mode and 206 ms in host-dialed mode, against 52 ms. Uploads, which flow the other way, stall nothing.
>     - **`transport.bandwidthLimit` is not adopted.** frp's limiter starts with a full burst of one second's bytes, so transfers below one second's cap never meet it. When it binds, the burst's stall stays (p99 87–203 ms) and the transfer slows 1.3–3.5 times. The stall is about min(burst, 6 MiB) ÷ path rate, so a cap helps only below about 6 MB/s, where a 64 MiB transfer takes about 11 s; the 25–75% caps tested were all above that.
>     - **Bulk paths go to a separate frpc with `transport.tcpMux = false`,** with its own tcpmux proxy for the Instance. This removed the stall at the full path rate: p99 53 ms in direct mode, 55 ms in host-dialed mode. It needs:
>       - a second frps with `tcpMux = false`, since frps with tcpMux on wraps every connection in a multiplexed session (frp v0.71.1, `server/service.go:746-772`), with S1's heartbeat set explicit;
>       - the dial-home listener routing that frpc's websocket to it by TLS server name;
>       - a second `CONNECT` target in the front's transport, for embeddings, file transfers and large non-streamed replies;
>       - a second frpc per Instance, staged and supervised like the first;
>       - a TLS connection per bulk request.
>     - **In host-dialed mode the separate frpc shares the serving relay.** Its connections are streams of the relay's yamux session, whose 256 KiB per-stream window bounds the stall to a few ms. That window must stay at yamux's default. Its price is a per-stream rate ceiling of 256 KiB ÷ round trip: about 114 MB/s at the 2.3 ms measured, about 26 MB/s at 10 ms, which bounds the bulk path's rate on a longer path.
>     - **A third relay channel for bulk paths is not adopted:** it gained about 3 ms and costs a third relay per Allocation, which under HBEP-4's 4 per login host breaks the handover pair.
>     - **Still open:** a FIFO bottleneck shared by the two frpc's connections in direct mode; real ssh and srun buffers on the relay; several bulk transfers at once.

## A note on #273

**Front-side per-request upload timing is not worth building.**
- The node read an upload's last byte 0.14–0.34 s after the client's last write, without a cap. That is 10–18% of a 64 MiB upload's 1.4–2 s, and up to 0.92 s under a cap. The tail is the bytes buffered between the front and the receiver: frp's 6 MiB window, the socket buffers and, in host-dialed mode, the relay.
- So a timing that stops at the front's last write overstates the rate by that much, and for an upload below about 6 MiB it times only the front's loopback write.
- The front already sees a better bound: a non-streamed upload's response headers follow the receiver's last byte, so the time to the response covers the whole upload.
- If an upload rate is wanted per request, it should come from the receiver (the agent or the engine), as this spike measured it.

## frp facts this rests on

Read at frp v0.71.1 (`fatedier/frp`, tag `v0.71.1`, commit `d8cf180`). Paths are relative to the repository root.

| Fact | Source |
|---|---|
| frps with `tcpMux` on wraps every connection to its bind port in a yamux server session, with a 6 MiB stream window, before reading any frp message; the websocket listener is handled the same way | `server/service.go:746-772` (the 6 MiB window at `:752`), `:412` |
| `tcpMux` defaults to true on both ends; with it off, frps's `heartbeatTimeout` defaults to 90 s | `pkg/config/v1/server.go:183-191`; `pkg/config/v1/client.go:154-158` |
| frpc with `tcpMux` off opens a new connection to frps for each work connection | `client/connector.go:99`, `:141`, `:164` |
| frpc's multiplexed session also uses a 6 MiB stream window | `client/connector.go:150-155` |
| `bandwidthLimit` makes one `rate.Limiter` per proxy, in `client` mode at frpc (the default) | `client/proxy/proxy.go:66-69`; `pkg/config/v1/proxy.go:36-42`, `:156` |
| That limiter wraps both the reader and the writer of every work connection of the proxy: uploads and downloads share it | `client/proxy/proxy.go:134-139` |
| Its burst is one second of the limit, and it starts full | `pkg/util/limit/limiter.go:25-33` (`rate.NewLimiter(limit, burst = limit)`) |
| A write or read larger than the burst is split into burst-sized pieces, each waiting for its tokens | `pkg/util/limit/writer.go`, `reader.go` |
| `MB` is 1,048,576 bytes and `KB` 1,024 | `pkg/config/types/types.go:26-27`, `:73-77` |
| The `unix_domain_socket` plugin is handed the limited connection | `client/proxy/proxy.go:197-222`; `pkg/plugin/client/unix_domain_socket.go:52-66` |

The relay's per-stream window in host-dialed mode is yamux's default of 256 KiB (`fatedier/yamux` v0.2.0 `const.go:128-129`, `mux.go:65-76`), since `internal/hostdial` uses `yamux.DefaultConfig()` with only the keepalive and write timeout changed.

## Environment

- **The run:** workflow run 42836566161 on draft PR #341, whose head merges the spike's commit `099c983c` with a one-off workflow. It took 10 Nov 2026, 14:41–15:10 UTC, 29 min per job.
  - The commits after `099c983c` change only `README.md` and `results/`, so the harness in this directory is the one that ran.
  - The raw files stay in the run's artifacts until 24 Nov 2026; the committed `streams.csv`, `transfers.csv`, `links.csv`, `samples.csv` and `host.csv` keep every measured value.
- **Runners:** three fresh GitHub-hosted `ubuntu-24.04` runners, each an AMD EPYC 9V45 96-Core Processor with 4 CPUs, 15,983 MB and Linux 7.0.0-1017-azure. They had Go from `go.mod`, Python 3.12.3, and open files raised to the hard limit.
- **The link:** 48,000,000 B/s each way, 1.15 ms each way, a 256 KiB receive buffer, 16 KiB chunks and 2 ms of catch-up slack (Method).
  - Direct mode measured 47.2–47.5 MB/s at the receiver.
  - Host-dialed mode measured 31.7–33.9 MB/s, held by the relay chain's copying on 4 CPUs.
- **Priority:** frps, the second frps, the listener, the owner (and the stand-ins it starts), the link and the client ran at nice 0; frpc and the node at nice 10. During transfers the runner was at most 8% busy.
- **The development VM** ran only builds and smoke runs at nice 19 under `systemd-run --user --scope -p MemoryMax=4G`, with transfers of 8 and 32 MiB and the 1-minute load at most 3.04. Every smoke configuration was valid, with 0 drops and nothing left running. Nothing that links `internal/rehearsal` was built into this spike (`go list -deps -test` counts 0 for all four packages).

## Provenance

| Component | Version and pin | Project and licence | Use |
|---|---|---|---|
| frp (frps, frpc) | v0.71.1 linux_amd64; tarball sha256 `1d987521…f803` (S1c's pin); frps `23227629…6ab3` and frpc `35c6054d…c28b` from `internal/launcher/frp-pins.txt` | [fatedier/frp](https://github.com/fatedier/frp), Apache-2.0 | the tunnels; its source, for the facts above |
| fatedier/yamux | v0.2.0, linked through `internal/hostdial` (the version frp v0.71.1 pins) | [fatedier/yamux](https://github.com/fatedier/yamux), MPL-2.0 | the relay sessions |
| S1c's shaper | `spikes/s1c/cmd/s1cbench/shaper.go` at `3f863bad`, with a delay line, a bounded receive buffer, catch-up slack and per-connection counts | this project | `cmd/s15link` |
| Spike S14's harness | `spikes/s14` at `3f863bad`: the listener, the owner, the ssh stand-in, the node, the dial, the driver's process records and sampler, the rules for CPU and drops | this project | adapted in `cmd/s15`, `cmd/s15bridge`, `node/`, `s15.py`, `analysis.py` |
| `cmd/s15/frppass.go` | a copy of `internal/front/frppass.go` at `14f9a318`; `cmd/s15/parity_test.go` fails if the two diverge | this project | the dial-home listener's passthrough |
| `internal/hostdial`, `internal/agent/enginedouble` | this project, unchanged | this project | the relays; the engine double |
| GitHub Actions | `actions/checkout` v7.0.1, `actions/setup-go` v7.0.0 (as in `ci.yml`), `actions/upload-artifact` v7.0.2 (`cf430e03…`) | GitHub, MIT | the hosted runs (a one-off workflow on a draft PR, never merged) |

- **New:** the link's additions, the node's bulk paths, the client's transfers, layout (c)'s SNI routing, layout (d)'s stand-in, and the S15 rules.
- **No third-party code is copied.**

## Deviations from the issue

- **Validity (accepted by the science agent, 10 Nov 2026).** The brief asked for S14's every-second rule through each transfer's window. A head-of-line stall is exactly a second with no token, so that rule would have invalidated layout (a) whenever it showed the effect. So:
  - the rule holds over the reference window;
  - through each transfer, every stream must have a request in flight that delivers tokens before and after it;
  - each stream is one long request, so none is between requests while a transfer runs.
- **Layout (c) needs a second frps (accepted, 10 Nov 2026).** At the pinned frp, an frpc with `tcpMux = false` cannot use HBEP-6's frps (frp facts). (c) therefore runs:
  - a second frps with `tcpMux = false` and S1's heartbeats explicit;
  - SNI routing in the spike's listener (`cmd/s15/listener.go`; the passthrough copy is unchanged);
  - a second `CONNECT` target.

  The front has none of these today; the Verdict prices them.
- **Layout (d) was added by the science agent** (host-dialed mode only), and is built as a stand-in (Method): 4 relays where a real third channel makes 3.
- **The link runs at one rate,** 48 MB/s each way, the low end of S1's probes, with a 2.3 ms round trip. 85 MB/s was not run: at a higher rate the same transfer queues for less time, so 48 MB/s is the case where blocking is worst.
- **Fair queueing.** The link shares its rate with per-connection queues, as S1c's shaper does. That flatters layouts with separate connections against a FIFO bottleneck (Unknowns).
- **The embeddings reply** is 2,000 embeddings of 1,024 floats as JSON. The double computes nothing, so the reply goes at once; a real engine would compute first, which changes when the transfer starts, not what it does to the streams.
- **New connection per transfer,** as in S1c and S14.
- **The frps admission plugin is not configured,** for either frps.

## Unknowns

- **A FIFO bottleneck.** With one shared queue instead of per-connection queues, a separate connection's bulk bytes would still sit ahead of the token connection's in the queue; layouts (c) and (d) would gain less than measured here.
- **Real ssh and srun.** The stand-in copies bytes in the clear over one TCP hop. ssh's and srun's own buffers, which S1c found add to head-of-line queues, are not modelled; nor is ssh's encryption cost.
- **Higher path rates and other round trips.** At 85 MB/s a transfer queues for less time; at a longer round trip, the relay's 256 KiB yamux window would limit a relay stream's rate (256 KiB ÷ round trip: about 114 MB/s at 2.3 ms).
- **Several bulk transfers at once,** and bulk transfers on several Instances behind one link.
- **The front's own HTTP proxy,** which would carry the bulk bytes once more, and its upload path (#273).
- **The cost of (c)'s connections at scale.** With `tcpMux = false`, the bulk frpc opens a new TLS connection through the listener for every work connection: a handshake per bulk request, for the listener and the second frps. The admission plugin's operations (Login, NewProxy, CloseProxy) are not called per work connection. This spike ran one Instance.

## Files and how to run

| File | What |
|---|---|
| `fetch.sh` | frp, sha256-checked, into `$S15_BIN`; builds `s15`, `s15link`, `s15bridge` and `s15node.test` |
| `cmd/s15/` | `certs`, `listener` (with the SNI route), `owner`, `load` (the streams and the transfers), with tests and the passthrough's parity test |
| `cmd/s15link/` | the link emulator, with tests |
| `cmd/s15bridge/` | the ssh stand-in |
| `node/` | the node side (engine doubles, bulk paths, job sides, relay steps), a test binary, with tests |
| `s15.py` | the driver: `run`, `host`, `leftovers` |
| `analysis.py`, `tests/` | the result rules and the sample writer, with their tests |
| `summarize.py` | `extract` (raw files to `streams.csv`, `transfers.csv`, `links.csv`), `tables` |
| `results/rep{1,2,3}/` | `runs.csv`, `samples.csv`, `host.csv` (the driver's), `streams.csv` (every token's arrival), `transfers.csv`, `links.csv` |

```sh
export S15_BIN=<dir> S15_WORK=<scratch> S15_RESULTS=<dir>
bash fetch.sh
(go vet ./... && go test ./... && python3 -m unittest discover -s tests)
python3 s15.py run --rep 1            # every configuration, about 40 min on a 4-CPU runner
python3 s15.py leftovers
python3 summarize.py extract $S15_RESULTS/raw results/rep1 && cp $S15_RESULTS/{runs,samples,host}.csv results/rep1/
python3 summarize.py tables
```

Ports, all on 127.0.0.1, in #336's block: 29980–29982 frps (bind, tcpmux, web), 29983 the dial-home listener, 29984 the link, 29985 the node's relay acceptor, 29986 the control channel's port (nothing listens), 29987–29989 the second frps. The job sides' listeners are kernel-chosen. Every run checks with `ss` that every listener its processes hold is on 127.0.0.1, and that nothing is left in 29980–29999 afterwards.
