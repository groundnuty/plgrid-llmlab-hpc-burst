# S8: JetStream durability for events and usage

Spike S8 (issue #25) for HBEP-8 (events, usage) and HBEP-10 (agent primitives, "guaranteed delivery").

## Question

Can embedded NATS JetStream with CloudEvents give: at-least-once delivery, resumption from a cursor, per-resource ordering, and an explicit gap marker whenever retention evicts a message? Which settings, and which thin layer of our own, does that take? What does `sync_interval: always` cost against the default?

## Short answer

- **Yes, with a thin layer.** JetStream alone does not give the gap marker or the per-resource order after a redelivery. Both were shown missing with controls, then supplied by about 700 lines of Go in `evlayer/`.
- **Settings:** one file-backed replica, limits retention (`DiscardOld`), `sync_interval: always`, a two-minute duplicate window with the event id as `Nats-Msg-Id`, direct get on, one subject per resource (`hb.ev.<kind>.<name>`).
- **Recommended subscriber: stored cursor** (`evlayer.Tail`): an ordered consumer started at `cursor+1`, the cursor kept by the subscriber (in the framework: a control plane object, HBEP-10/11) and saved after the handler succeeds. It is in stream order by construction, marks every gap, and detects lost acknowledged writes. A server-side durable consumer (`evlayer.Durable`) also works but needs a re-sequencer and has a hole (see Findings 3 and 4).
- **A SIGKILL cannot show the fsync policy.** With the default policy, no acknowledged message was lost in 30 SIGKILL runs. The kernel keeps what the process wrote. The policies differ only on power loss or a kernel crash, which this VM cannot emulate without root (see Unknown). The fsync difference is shown instead by counting syscalls and by the cost.
- **Cost of `always`:** about 900 acknowledged publishes per second for one publisher and about 1,200 per second at any higher concurrency, against 6,600 and 35,000 for the default. There is no batching of fsyncs across publishers.

## Method

Everything runs on the agents VM, loopback only. `jsnode` starts nats-server in process from a config text, so the setting under test is the literal `sync_interval: always`. The parent reads back the effective option the server parsed (a misspelt option would fall back to the default silently) and the `strace` test checks the syscalls.

| Acceptance criterion | Tests (in `tests/`) | What makes them valid |
|---|---|---|
| 1. SIGKILL after publish acks loses nothing acknowledged | `TestKill_SyncAlways_...`, `TestKill_DefaultSync_...`, `TestKill_Control_AsyncPersist_CanLose` | The server runs in one child process, the publisher (8 workers, 5 resources) in another. The parent records every `ACK <seq> <event id>` line the publisher prints after the broker's acknowledgement. At a random moment it SIGKILLs the server process, restarts the server on the same store directory, and looks up every recorded sequence: it must exist and hold the same event id. 30 iterations per mode. The publisher's lines, not the server, define what was acknowledged. A positive control (stream `persist_mode: async`, which acknowledges before the write) must lose messages, or the harness proves nothing. A new publish after the restart must get a sequence above every acknowledged one. |
| 1b. cause, not only outcome | `TestFsyncCallsPerAcknowledgedPublish` | The host runs under `strace -f -e trace=fsync,fdatasync`. Counts the calls for 0 and 200 acknowledged publishes. |
| 2. Eviction produces a gap marker the subscriber sees | `TestGap_*` | Streams with `MaxMsgs` 50 (publishing 310 to 600) and with `MaxAge` 1 s. Each test asserts that the limit was reached (stream holds only its limit, first sequence moved) before it asserts anything else. Expected values come from the publisher's acknowledgements: every acknowledged sequence is either handled once or inside exactly one gap, in order, with no hole or overlap between consecutive records. Slow live subscriber, stopped-then-evicted subscriber, and an idle subscriber whose messages all expired (marker with no new event). Controls: a plain consumer in the same scenario sees only a silent jump; with no eviction there is no marker (no false positive). |
| 3. Per-resource order after a redelivery | `TestOrder_*` | Redelivery by NAK, and by ack-wait expiry of events a "crashed" worker took. The same scenario runs first with a plain consumer, which must be out of order (control), then with the layer, which must hand each resource's events over exactly once in publish order. The expected order is the order of the publisher's acknowledgements. |
| 4. Resume from a stored cursor after a restart | `TestCursor_*` | Subscriber restarts and a SIGKILL of the server in between, for the durable consumer and for the file cursor. Every acknowledged event must arrive at least once; repeats must stay inside the in-flight window; first deliveries must be in per-resource order. |
| Cost | `cmd/s8bench` | Runs interleaved: always, default, always, default, always, default. 1, 8 and 32 publishers, 8 s per phase, first second dropped. The VM load is sampled each second. |

The lost-tail detection test emulates a power failure by restoring an older copy of the store directory, since a power failure cannot be caused here. It tests the detection, not the cause.

### Design of the layer (`evlayer/`)

- **Envelope:** CloudEvents from the sdk-go v2 event type, structured JSON mode, `specversion` "1.0" (the attribute value of spec 1.0.2). The CloudEvents subject is `kind/name`; the NATS subject is `hb.ev.kind.name`. Names with `.`, `*`, `>`, `/` or spaces are refused before publishing.
- **Publish:** event id is `Nats-Msg-Id`. `PublishRetry` retries on timeout; the stream de-duplicates, so a retry stores one copy.
- **Gap marker:** a subscriber tracks the next stream sequence it expects. A first delivery above it yields `Gap{From, To}` before that event; when nothing is left to deliver, the stream's oldest sequence above the expected one yields the same marker with no new event needed. A fresh subscription is owed no history. The marker is a `Record` in the handler's sequence (and can be turned into a `io.hpcburst.gap.v1` CloudEvent with `GapEvent`).
- **Re-sequencer (durable consumer only):** before handing over a message of resource R, the layer asks the stream (direct get with the subject and a start sequence) for earlier messages of R that it has not handled, and hands those over first. A redelivered copy that arrives later is dropped as a duplicate. A failed earlier message makes the later one wait (NAK).
- **Stored cursor:** `Cursor{Seq, EventID}`. On resume it checks that the stream still holds the same event at `Seq` and ends at or after it, else `ErrHistoryRewritten`.

## Environment

- One VM with 6 cores and 31 GB, no swap, ext4 on a virtio disk (`/`), kernel 6.8. Other agents' sessions and other spikes ran at the same time; builds and runs used `nice -n 10`, and every test and benchmark waited until the 1-minute load was at most 4.0 and at least 8 GB were free.
- **Load during the benchmark:** 1-minute load 1.4 to 3.4; system-wide CPU busy averaged 16 to 59 percent per phase, peak second 65 (the default policy's runs are the busier ones, because they do more work); at least 21 GB available. The load rose slowly from run to run (other spikes), which is why the runs are interleaved. The full suite waited several times for the load to fall below 4.0 (see `waiting for capacity` lines in the output).
- Go 1.27.3, linux/amd64.

## Results

### 1. SIGKILL (30 iterations per mode)

| Policy | Iterations | Acknowledged | Lost acknowledged | Wrong event at an acknowledged sequence |
|---|---|---|---|---|
| `sync_interval: always` | 30 | 5,183 | **0** | 0 |
| default (about 2 minutes) | 30 | 52,955 | **0** | 0 |
| control: `persist_mode: async` | 30 | 50,261 | **1,249** (30 of 30 iterations lose some, up to 137) | 0 |

In the control, the restarted server also handed out sequences that had been acknowledged to the publisher before. Without the control, the zero for the default policy would say nothing about the harness.

**The default does not lose acknowledged writes to a process crash, and `always` does not protect against anything more here.** The default policy writes each message to the OS before acknowledging it; a kill leaves the kernel's copy intact. Only power loss or a kernel crash separate the two policies (Jepsen found about 30 s of acknowledged writes lost after a power failure with the default, in 5-node clusters of nats-server 2.12.1). Not testable here.

**fsync calls** (`strace`, per 200 sequential acknowledged publishes): `always` made 200 `fsync` calls (plus 2 at start-up), exactly one per acknowledged publish; the default made 0.

### 2. Gap marker

| Scenario | Limit reached | Result |
|---|---|---|
| Slow durable subscriber, `MaxMsgs` 50, 600 published | 600 published, stream holds 50, first sequence 551 | 80 handled, 520 sequences in 6 markers; every acknowledged sequence covered once |
| Control: plain consumer, same run | same | 8 to 9 silent jumps in sequence, 90 of 600 delivered, no marker |
| Subscriber stopped after 10, `MaxMsgs` 50, 310 published (durable and cursor) | first sequence 261 | one marker for 11 to 260 (250 sequences), then the 50 retained |
| Subscriber stopped, `MaxAge` 1 s, all expired (durable and cursor) | stream empty | marker for the 40 expired sequences before any new event; the next event follows it contiguously |
| No eviction, slow subscriber (durable and cursor) | n/a | no marker |

### 3. Per-resource order

| Scenario | Plain consumer (control) | With the layer |
|---|---|---|
| NAK of one event, other events overtake it | one resource handled out of order (`o-6`, the NAKed event, handled after `o-9`) | in order; 1 event re-sequenced; each of 60 events once |
| Worker took 6 events and never acknowledged (ack wait 1 s) | 3 of 3 resources out of order | in order; 6 re-sequenced, the 6 redelivered copies dropped; each of 36 events once |
| Stored cursor (ordered consumer) | n/a | stream order, each of 60 events once |

### 4. Resume from a cursor

- **Durable consumer:** subscriber restarted twice, then the server SIGKILLed and restarted: all 200 acknowledged events delivered. If the SIGKILL comes within about 100 ms of the last handled event, the server's consumer state is older (it writes consumer state at most every 100 ms, `flushLoop` in nats-server `filestore.go`), so tens of events were delivered a second time (42 in the recorded run; replay, no loss). After a pause of 600 ms: 0 repeats.
- **Stored cursor:** the same sequence of restarts: all 200 delivered, 0 repeats (the cursor file is saved after each handled event, with fsync).
- **Lost acknowledged tail** (power failure emulated by restoring an older store copy): cursor at 50, stream ends at 30 gives `ErrHistoryRewritten`; the stream refilled with different events up to 70 gives `ErrHistoryRewritten` too (sequence 50 holds another event id). A server-side consumer cannot see this: its state is restored together with the stream.

### 5. Cost of `sync_interval: always`

Median of 3 interleaved runs per cell (individual runs in brackets), publish to acknowledgement, loopback TCP, event of about 400 bytes:

| Publishers | always: per second | p50 / p99 | default: per second | p50 / p99 | ratio |
|---|---|---|---|---|---|
| 1 | 899 (883, 1025, 899) | 1.0 ms / 2.9 ms | 6,606 (6857, 6606, 6041) | 0.12 ms / 0.44 ms | 7x |
| 8 | 1,204 (1139, 1285, 1204) | 6.4 ms / 13.9 ms | 20,561 (20561, 20991, 17978) | 0.34 ms / 1.2 ms | 17x |
| 32 | 1,207 (1207, 1067, 1482) | 25.9 ms / 48.0 ms | 34,743 (34743, 37174, 32784) | 0.79 ms / 3.3 ms | 29x |

- `always` throughput stays near 1,200 per second from 8 to 32 publishers, and latency grows with the queue: one fsync per message, not batched across publishers. The system disk saw about 2.3 flush requests per acknowledged publish under `always` and none under the default (the whole VM's disk, so a rough figure).
- Worst single publish per phase: 12 to 71 ms (`always`), 9 to 26 ms (default).
- One-time consume benchmark (20,000-event backlog, no-op handler, default policy, one run): plain pull and ack 87,000 per second; stored-cursor tail 26,000 per second (cursor saved every 100); durable layer 5,500 per second (one direct get per message, handled in sequence). The smoke run before it gave 95,000, 33,000 and 6,600.
- These numbers are for this VM's disk. A different disk changes the `always` figures.

## Findings

1. **Default policy, SIGKILL:** no acknowledged loss in 52,955 acknowledged messages. See Results 1.
2. **JetStream gives no gap signal.** After eviction the consumer silently continues at the oldest retained message. The marker has to be derived from the sequence jump (and from the stream's first sequence when idle). It works for `MaxMsgs` and `MaxAge`; a consumer filtered to some subjects would also see jumps for other subjects' messages (by construction, not tested), so the subscriber uses the whole subject space.
3. **A delivered but unacknowledged message that is then evicted vanishes from the consumer's ack state** (nats-server 2.15.2): the ack floor jumps past it, pending goes to zero, nothing is redelivered (`TestFinding_...`). A durable subscriber that restarts afterwards cannot mark that range; the stored-cursor subscriber can (its own cursor is the truth). The test pins this behaviour, so a change in a later release shows up.
4. **A durable consumer's ack state lags by up to about 100 ms,** so a server SIGKILL can replay recently handled events. At-least-once is kept; handlers must de-duplicate by event id.
5. **Redelivery does reorder** with more than one message pending (NAK or ack-wait expiry); the controls show it. A consumer with `MaxAckPending` 1 would serialise delivery and avoid it, at the cost of parallelism; this spike did not test that setting.
6. **Unacknowledged leftovers pin the ack floor.** Events the layer hands over ahead of their own delivery, and events fetched but not yet handled at shutdown, stay unacknowledged at the server. A restart then replays everything after the oldest of them (101 repeats in the first version of the cursor test). The layer now NAKs the unhandled rest of a batch on shutdown and acknowledges a re-sequenced event when it still holds its delivery. After a crash (no shutdown) the replay window is still everything since the oldest unacknowledged event, so a handler must de-duplicate by event id.
7. **A cursor must carry the event id** to detect a stream whose tail was lost and refilled (`ErrHistoryRewritten`).
8. **A setting that is not applied looks like the default.** A mistyped fsync option and the default behave the same until power fails, so the spike asserts the option the server parsed (the READY line) and counts the fsync calls.

## What this means for the HBEPs

- **HBEP-10 "Guaranteed delivery" (proposed meaning)** is implementable as written: at least once, ordered per resource, durable across restarts, resumable from a cursor, each event with a unique ID, an explicit gap marker. Suggested wording: subscribers keep their own cursor (sequence and last event id) in the control plane; the log is embedded JetStream with `sync_interval: always`; a gap is the range `From..To` of sequences evicted unread; a cursor that no longer matches the log is an error ("history rewritten"), and the subscriber re-reads status.
- **Open question "push or pull delivery":** this spike favours a stored cursor over an ordered consumer (push, flow-controlled, in order). Callback with acknowledgement (durable consumer) needs the re-sequencer and cannot mark pending-then-evicted ranges. If the HBEP wants server-side backlog visibility ("unacknowledged backlog visible to its owner"), the backlog of a cursor subscriber is the stream's last sequence minus the cursor, which needs no consumer state.
- **HBEP-8:** the "gap" event for an observer that restarts (open question) and the gap marker for a late subscriber are the same mechanism. Retention per event class can be `MaxMsgs`, `MaxAge` or `MaxBytes`; gap detection does not depend on which.
- **Usage records:** `always` caps one node at about 1,200 acknowledged publishes per second on this disk. A front that meters every request must publish usage records in batches (one message of many records) or accept the loss window of the default policy for that stream. Per-request records below a few hundred per second are fine.
- **Single node:** no cluster was tested, so Jepsen's split-brain and `.blk`-corruption findings are outside this spike; they are not evidence either way.

## Unknown

- **Power loss or a kernel crash:** whether `always` really keeps acknowledged writes and the default really loses them is shown by Jepsen and by the syscall counts here, not by a test of ours. A direct test needs a disk that drops unsynced writes (dm-flakey, a VM power cut; root needed) or a write-dropping FUSE passthrough. `/dev/fuse` is writable by the user on this VM, so a FUSE test might be possible without root. Not attempted.
- Bit flips, truncation of `.blk` files, snapshot files (Jepsen findings 3 and 4): not tested.
- Disks other than this VM's: the `always` ceiling will differ on NVMe, Ceph or Lustre.
- Several instances of one subscriber (competing consumers): per-resource order needs partitioning (one instance per resource key range); not tested. The re-sequencer assumes one active instance per consumer.
- A very slow live subscriber on the stored-cursor path: the ordered consumer buffers ahead in the client, so eviction may not be seen until the buffer drains. The restart case is tested; the live case is not.
- Recovery time of a large store after SIGKILL, and growth of the store directory over weeks.
- `MaxAckPending` 1 as a simpler alternative to the re-sequencer (Finding 5).

## How to run

```
. /tmp/hpc-burst-tools/env.sh      # on the VM: Go 1.27.3, shared caches. Elsewhere: Go 1.27 or newer
cd spikes/s8
nice -n 10 go test -count=1 -v -timeout 45m ./...      # about 12 minutes, most of it the SIGKILL loops
nice -n 10 go run ./cmd/s8bench -rounds 3 -dur 8s -workers 1,8,32 -out /tmp/results.json
```

Variables: `S8_KILL_ITERS` (default 30), `S8_SEED` (kill timing), `S8_SCRATCH` (store directories, default under `/tmp/hpc-burst-tools/spikes/s8/data`; never under `/home`), `S8_KEEP=1` (keep the store directories), `S8_VERBOSE=1`. `strace` must be installed for the fsync test (it skips, reporting "unknown", otherwise). Tests and benchmark wait while the 1-minute load is above 4.0.

## Provenance (what is reused)

| Component | Project and URL | Licence | Version | Use |
|---|---|---|---|---|
| nats-server | https://github.com/nats-io/nats-server | Apache-2.0 | v2.15.2 | the embedded JetStream node (the `server` package) |
| nats.go | https://github.com/nats-io/nats.go | Apache-2.0 | v1.55.0 | client and `jetstream` API |
| CloudEvents Go SDK | https://github.com/cloudevents/sdk-go | Apache-2.0 | v2.16.2 | the event envelope |
| nats-io jwt, nkeys, nuid | https://github.com/nats-io | Apache-2.0 | v2.8.2, v0.4.16, v1.0.1 | nats-server dependencies |
| klauspost/compress, minio/highwayhash | https://github.com/klauspost/compress, https://github.com/minio/highwayhash | Apache-2.0 (compress also carries BSD and MIT parts) | v1.20.3, v1.0.4 | nats-server dependencies |
| google/uuid, golang.org/x/{crypto,sys,time} | https://github.com/google/uuid, https://go.googlesource.com/ | BSD-3-Clause | v1.6.0, v0.58.0, v0.49.0, v0.16.0 | dependencies |
| json-iterator/go, go.uber.org/zap, go.uber.org/multierr, antithesishq/antithesis-sdk-go | https://github.com/json-iterator/go, https://github.com/uber-go/zap, https://github.com/uber-go/multierr, https://github.com/antithesishq/antithesis-sdk-go | MIT | v1.1.12, v1.27.0, v1.11.0, v0.8.0-default-no-op | dependencies of the SDK and the server |
| modern-go/concurrent, modern-go/reflect2, google/go-tpm | https://github.com/modern-go, https://github.com/google/go-tpm | Apache-2.0 | see `go.mod` | dependencies |
| strace 6.8 | https://strace.io | LGPL-2.1 or later (per the Debian copyright file) | system package, not vendored | counting fsync calls in one test |
| `jsnode/`, `evlayer/`, `harness/`, `tests/`, `cmd/s8bench/` | this repository | the repository's licence | new | the spike's own code |

The licences above were read from each module's LICENSE file. The idea of an embedded NATS JetStream with CloudEvents comes from the state-of-the-art evaluation; no code was copied from another project, so there is no `third_party/forked/` entry.
