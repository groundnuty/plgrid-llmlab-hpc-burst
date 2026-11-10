# Spike S15: bulk transfers beside token streams in one tunnel (HBEP-6, open question 12)

Issue #336. One Instance's tunnel carries 8 token streams at 20 tokens/s while one bulk transfer runs: a 64 MiB upload, a 64 MiB non-streamed download, or a 2,000-input embeddings reply. Four layouts are compared: (a) one proxy, (b) a second proxy with frp's `bandwidthLimit` at 25, 50 and 75% of the path rate, (c) a separate frpc with `tcpMux = false`, and, in host-dialed mode only, (d) a relay of its own for that frpc. They run in direct and host-dialed mode, on loopback through S1c's link emulator at S1's probed path rate and round trip. The measurements ran on GitHub-hosted runners, three repetitions on three fresh runners.

## Findings

FINDINGS

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
  - The embeddings reply is 2,000 embeddings of 1,024 float32 values as JSON, in the OpenAI-compatible shape vLLM serves, sent at once with a `Content-Length`. The double computes nothing, so the reply's cost is its transfer.
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

RESULTS

## Verdict

VERDICT

## Proposed HBEP-6 text for open question 12

OQ12TEXT

## A note on #273

NOTE273

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

ENVIRONMENT

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

DEVIATIONS

## Unknowns

UNKNOWNS

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
