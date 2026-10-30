# S8: JetStream durability for events and usage

Spike S8 (issue #25) for HBEP-8 (events, usage) and HBEP-10 (agent primitives, "guaranteed delivery").

## Question

Can embedded NATS JetStream with CloudEvents give: at-least-once delivery, resumption from a cursor, per-resource ordering, and an explicit gap marker whenever retention evicts a message? Which settings, and which thin layer of our own, does that take? What does `sync_interval: always` cost against the default?

## Short answer

- **Yes, with a thin layer.** JetStream alone does not give the gap marker or the per-resource order after a redelivery. Both were shown missing with controls, then supplied by about 800 lines of Go in `evlayer/`.
- **Settings:** one file-backed replica, limits retention (`DiscardOld`), `sync_interval: always`, a two-minute duplicate window with the event id as `Nats-Msg-Id`, direct get on, one subject per resource (`hb.ev.<kind>.<name>`).
- **Recommended subscriber: stored cursor** (`evlayer.Tail`): an ordered consumer started at `cursor+1`, the cursor kept by the subscriber (in the framework: a control plane object, HBEP-10/11) and saved after the handler succeeds. It is in stream order by construction, marks every gap, and detects lost acknowledged writes. A server-side durable consumer (`evlayer.Durable`) also works but needs a re-sequencer and has holes (see Findings 3, 4 and 10).
- **A SIGKILL cannot show the fsync policy.** With the default policy, no acknowledged message was lost in 30 SIGKILL runs. The kernel keeps what the process wrote. The policies differ only on power loss or a kernel crash, which this VM cannot emulate without root (see Unknown). Counting syscalls shows that `always` asks the OS to flush once per acknowledged publish and the default does not; it does not show that acknowledged writes survive a power loss. That stays unverified here.
- **Cost of `always`:** about 860 acknowledged publishes per second for one publisher and about 950 to 1,000 per second (runs between 950 and 1,360) at 8 or 32 publishers, against about 6,300 and 20,000 to 34,000 for the default. There is no batching of fsyncs across publishers.

## Method

Everything runs on the agents VM, loopback only. `jsnode` starts nats-server in process from a config text, so the setting under test is the literal `sync_interval: always`. The parent reads back the effective option the server parsed (a misspelt option would fall back to the default silently) and the `strace` test checks the syscalls.

| Acceptance criterion | Tests (in `tests/`) | What makes them valid |
|---|---|---|
| 1. SIGKILL after publish acks loses nothing acknowledged | `TestKill_SyncAlways_...`, `TestKill_DefaultSync_...`, `TestKill_Control_AsyncPersist_CanLose` | The server runs in one child process, the publisher (8 workers, 5 resources) in another. The parent records every `ACK <seq> <event id>` line the publisher prints after the broker's acknowledgement, reading the publisher's output to end of file through a pipe it owns, and cross-checks the count against the publisher's own `DONE <n>` line. At a random moment it SIGKILLs the server process (the wait status must say SIGKILL, so a host that died of something else fails the test), restarts the server on the same store directory, and looks up every recorded sequence: it must exist and hold the same event id. Only the stream's own "message not found" answer counts a message as lost; any other read error fails the iteration, and a missing stream (all acknowledged messages lost) differs from an unreachable server (an error). The acknowledgements read before the kill are kept apart and reported separately. 30 iterations per mode. The publisher's lines, not the server, define what was acknowledged. A positive control (stream `persist_mode: async`, which acknowledges before the write) must lose messages, or the harness proves nothing. A new publish after the restart must get a sequence above every acknowledged one. |
| 1b. cause, not only outcome | `TestFsyncCallsPerAcknowledgedPublish` | The host runs under `strace -f -e trace=fsync,fdatasync`. Counts the calls for 0 and 200 acknowledged publishes. This shows what the server asks of the OS, not what survives a power loss. |
| 2. Eviction produces a gap marker the subscriber sees | `TestGap_*` | Streams with `MaxMsgs` 50 (publishing 310 to 600) and with `MaxAge` 1 s. Each test asserts that the limit was reached (stream holds only its limit, first sequence moved) before it asserts anything else. Expected values come from the publisher's acknowledgements: every acknowledged sequence is either handled once or inside exactly one gap, in order, with no hole or overlap between consecutive records. Coverage alone cannot tell a gap from a delivery, so two more assertions pin the outcome. In the stopped-subscriber scenario the gap must be exactly 11 to 260 (10 events handled, 310 published, 50 kept) and the 50 retained events 261 to 310 are all delivered once; in the live `MaxMsgs` cases, once publishing stops, the final retained 50 sequences are cross-checked against the stream and must all be delivered once. Slow live subscriber (durable and stored cursor), stopped-then-evicted subscriber, and an idle subscriber whose messages all expired (marker with no new event). Controls: a plain consumer in the same scenario sees only a silent jump; with no eviction there is no marker (no false positive), and a subscription that starts after history was already evicted is owed none of it. The starting boundary is tested on its own: a stored-cursor subscriber whose first handler call fails, or that is stopped before any event arrived, then loses its first events to retention and restarts, must report the gap from sequence 1; so must a durable consumer that retention overtakes before its first fetch. |
| 3. Per-resource order after a redelivery | `TestOrder_*` | Redelivery by NAK, and by ack-wait expiry of events a "crashed" worker took. A NAKed event that the layer hands over early is acknowledged at the server at once: the test reads the server's pending count before the NAK delay ends. The same scenario runs first with a plain consumer, which must be out of order (control), then with the layer, which must hand each resource's events over exactly once in publish order. The expected order is the order of the publisher's acknowledgements. |
| 4. Resume from a stored cursor after a restart | `TestCursor_*` | Subscriber restarts and a SIGKILL of the server in between, for the durable consumer and for the file cursor. Every acknowledged event must arrive at least once; repeats must stay inside the in-flight window; first deliveries must be in per-resource order. A separate test isolates the durable consumer's ack-state lag: every message is acknowledged with a confirmed acknowledgement (the server's reply is awaited), the consumer's state is read from the server, the server is SIGKILLed at once and restarted, and the recovered state and the deliveries are compared with the state before the kill. A control kills a second later. |
| Cost | `cmd/s8bench` | Runs interleaved: always, default, always, default, always, default. 1, 8 and 32 publishers, 8 s per phase, first second dropped: the acknowledgements, the elapsed time, the device flush requests and the load samples all cover the same window after the warm-up. A phase with any publish error is marked invalid, and the run exits non-zero. The VM load is sampled each second; the samples and their extrema are kept in the JSON. |

The lost-tail detection test emulates a power failure by restoring an older copy of the store directory, since a power failure cannot be caused here. It tests the detection, not the cause.

### Design of the layer (`evlayer/`)

- **Envelope:** CloudEvents from the sdk-go v2 event type, structured JSON mode, `specversion` "1.0" (the attribute value of spec 1.0.2). The CloudEvents subject is `kind/name`; the NATS subject is `hb.ev.kind.name`. Names with `.`, `*`, `>`, `/` or spaces are refused before publishing.
- **Publish:** event id is `Nats-Msg-Id`. `PublishRetry` retries on timeout; the stream de-duplicates inside its duplicate window (two minutes here), so a retry within the window stores one copy and a retry after it would store a second. The test loses the acknowledgement after the event was stored and checks that the retry ends with one copy.
- **Gap marker:** a subscriber tracks the next stream sequence it expects. A first delivery above it yields `Gap{From, To}` before that event; when nothing is left to deliver, the stream's oldest sequence above the expected one yields the same marker with no new event needed. A subscription is owed everything the stream holds when it first starts, and nothing that was evicted before. That boundary is fixed and stored at the start, not at the first success: the cursor carries a `Started` flag (the tail stores `FirstSeq-1` at once), and the durable consumer's start is held by the server. The marker is a `Record` in the handler's sequence (and can be turned into a `io.hpcburst.gap.v1` CloudEvent with `GapEvent`).
- **Re-sequencer (durable consumer only):** before handing over a message of resource R, the layer asks the stream (direct get with the subject and a start sequence) for earlier messages of R that it has not handled, and hands those over first. A redelivered copy that arrives later is dropped as a duplicate. A failed earlier message makes the later one wait (NAK). When a later delivery hands over a message the layer had NAKed, the layer acknowledges it by its reply subject and checks the server's confirmation, because after a NAK the client's message object refuses every further acknowledgement.
- **Stored cursor:** `Cursor{Seq, EventID, Started}`. On resume it checks that the stream still holds the same event at `Seq` and ends at or after it, else `ErrHistoryRewritten`.

## Environment

- One VM with 6 cores and 31 GB, no swap, ext4 on a virtio disk (`/`), kernel 6.8. Other agents' sessions and other spikes ran at the same time; builds and runs used `nice -n 10`, and every test and benchmark run waited before starting until the 1-minute load was at most 4.0 and at least 8 GB were free.
- **Load during the benchmark:** 1-minute load 3.0 to 4.4 (lowest and highest sample of any phase); system-wide CPU busy averaged 19 to 67 percent per phase, peak second 71 (the default policy's runs are the busier ones, because they do more work); at least 20 GiB available. Other spikes ran at the same time and the load drifted from run to run, which is why the runs are interleaved; the third `always` round is visibly faster than the first two (1,288 and 1,362 per second at 8 and 32 publishers against about 950), a spread to keep in mind. Each run of three phases started only after the load had fallen to 4.0 or below, and some phases ended above it. The full suite also waited several times for the load to fall below 4.0 (see `waiting for capacity` lines in the output).
- Go 1.27.3, linux/amd64.

## Results

### 1. SIGKILL (30 iterations per mode)

| Policy | Iterations | Acknowledged | Lost acknowledged | Wrong event at an acknowledged sequence |
|---|---|---|---|---|
| `sync_interval: always` | 30 | 5,278 | **0** | 0 |
| default (about 2 minutes) | 30 | 51,626 | **0** | 0 |
| control: `persist_mode: async` | 30 | 59,591 | **1,222** (30 of 30 iterations lose some, up to 115) | 0 |

These are the final run, on the committed code. Of the acknowledgements the parent had read before it sent the SIGKILL (5,250, 51,438 and 59,439), 0, 0 and 1,074 were lost; the rest were read after the kill from the publisher's pipe and were acknowledged by the broker before it died. Each publisher's `DONE` line matched the number of acknowledgement lines read, in every iteration where the publisher exited by itself. Earlier full runs, before the review fixes, gave 4,921 and 5,183 (`always`) and 53,675 and 52,955 (default) acknowledged with 0 lost, and 1,091 and 1,249 lost in the control (28 and 30 of 30 iterations).

In the control, the restarted server also handed out sequences that had been acknowledged to the publisher before. Without the control, the zero for the default policy would say nothing about the harness.

**The default does not lose acknowledged writes to a process crash, and `always` does not protect against anything more here.** The default policy writes each message to the OS before acknowledging it; a kill leaves the kernel's copy intact. The policies can differ only on power loss or a kernel crash, which is not testable here, so that difference is unverified. (Jepsen reported about 30 s of acknowledged writes lost after a power failure with the default, but for nats-server 2.12.1 in 5-node clusters, not this version or setup.)

**fsync calls** (`strace`, per 200 sequential acknowledged publishes): `always` made 200 `fsync` calls (plus 2 at start-up), exactly one per acknowledged publish; the default made 0. This is what the server asks of the OS; it does not show that acknowledged writes survive a power loss.

### 2. Gap marker

| Scenario | Limit reached | Result |
|---|---|---|
| Slow durable subscriber (5 per fetch), `MaxMsgs` 50, 600 published | 600 published, stream holds 50, first sequence 551 | 80 handled, 520 sequences in 6 markers; every acknowledged sequence covered once; the final 50 retained sequences (551 to 600) all delivered once |
| Slow stored-cursor subscriber (5 held ahead), same run | same | 79 handled, 521 sequences in 9 markers; every acknowledged sequence covered once; the final 50 delivered once |
| Slow stored-cursor subscriber with the default client buffer (500 ahead), same run | same | 550 handled, 50 sequences in 1 marker; coverage exact, the final 50 delivered once. The subscriber had already pulled most of the stream into memory, so retention reached it late (Finding 9) |
| Control: plain consumer, same run | same | 6 to 9 silent jumps in sequence, 80 to 95 of 600 delivered, no marker |
| Subscriber stopped after 10, `MaxMsgs` 50, 310 published (durable and cursor) | first sequence 261 | exactly one marker, for 11 to 260 (250 sequences), then the 50 retained (261 to 310), each delivered once |
| Subscriber stopped, `MaxAge` 1 s, all expired (durable and cursor) | stream empty | marker for the 40 expired sequences before any new event; the next event follows it contiguously |
| No eviction, slow subscriber (durable and cursor) | n/a | no marker |
| Starting boundary, stored cursor: handler fails on the first of 5 events (or the subscriber is stopped before any event arrived), then 200 are published, `MaxMsgs` 50, and it restarts | 205 (200) published, first sequence 156 (151) | one marker for 1 to 155 (1 to 150), then the 50 retained once each. Without the stored boundary the restart is a new subscription and the first events vanish silently |
| Starting boundary, durable consumer created on an empty stream and overtaken before its first fetch (200 published, `MaxMsgs` 50) | first sequence 151 | one marker for 1 to 150, then the 50 retained |
| Fresh subscriber (durable or cursor) on a stream that had already evicted 150 of 200 | first sequence 151 | no marker; the 50 retained delivered once |
| Durable subscriber whose handler fails on one event until retention evicts it (Finding 10) | 205 published, first sequence 156 | sequence 1 is neither handled nor inside a marker; 2 to 205 each covered once |

### 3. Per-resource order

| Scenario | Plain consumer (control) | With the layer |
|---|---|---|
| NAK of one event, other events overtake it | one resource handled out of order (`o-6`, the NAKed event, handled after `o-9`) | in order; 1 event re-sequenced; each of 60 events once |
| Worker took 6 events and never acknowledged (ack wait 1 s) | 3 of 3 resources out of order | in order; 6 re-sequenced, the 6 redelivered copies dropped; each of 36 events once |
| Stored cursor (ordered consumer) | n/a | stream order, each of 60 events once |
| A NAKed event handed over early (NAK delay 4 s) | n/a | the server shows 0 pending and ack floor 2 within 11 ms of the NAK, before the delay ends; no redelivered copy afterwards, each event once |

### 4. Resume from a cursor

- **Durable consumer:** subscriber restarted twice, then the server SIGKILLed a second after the last handled event and restarted: all 200 acknowledged events delivered, 0 repeats.
- **Ack-state lag, isolated** (`TestCursor_AckStateLagsBehindConfirmedAcks`): 60 messages were fetched and acknowledged one by one with confirmed acknowledgements (the call returns after the server's reply), and the consumer's state read from the server showed ack floor 60 and 0 pending. SIGKILL at once, restart, read the recovered state and pull: in **8 of 8 trials the recovered ack floor was 0**, and all 60 already-acknowledged messages were delivered again (480 repeats in total), each exactly once, nothing above the floor missing. Control: killed one second after the acknowledgements, **3 of 3 trials recovered floor 60 and delivered nothing**. So the lag is real with confirmed acknowledgements, not an effect of acknowledgements still in transit. The burst fell inside one write interval of the server's 100 ms minimum between state writes; the test does not measure how long the delay can last. A first version of this check (one-way `Ack()` calls and a repeat count with no lower bound) could not tell this lag from acknowledgements lost in transit.
- **Stored cursor:** the same sequence of restarts and a SIGKILL of the server: all 200 delivered, 0 repeats (the cursor file is saved after each handled event, with fsync).
- **Lost acknowledged tail** (power failure emulated by restoring an older store copy): cursor at 50, stream ends at 30 gives `ErrHistoryRewritten`; the stream refilled with different events up to 70 gives `ErrHistoryRewritten` too (sequence 50 holds another event id). A server-side consumer cannot see this: its state is restored together with the stream.

### 5. Cost of `sync_interval: always`

Median of 3 interleaved runs per cell (individual runs in round order in brackets), publish to acknowledgement, loopback TCP, event of about 400 bytes; latencies are those of the median run:

| Publishers | always: per second | p50 / p99 | default: per second | p50 / p99 | ratio |
|---|---|---|---|---|---|
| 1 | 857 (857, 830, 905) | 1.0 ms / 3.5 ms | 6,255 (6255, 5949, 6753) | 0.13 ms / 0.55 ms | 7x |
| 8 | 956 (951, 956, 1288) | 7.8 ms / 21.7 ms | 19,885 (18939, 19885, 20334) | 0.34 ms / 1.5 ms | 21x |
| 32 | 988 (946, 988, 1362) | 32.5 ms / 54.7 ms | 33,897 (33409, 33897, 37883) | 0.84 ms / 3.1 ms | 34x |

- `always` throughput stays near 950 to 1,000 per second from 8 to 32 publishers (1,300 to 1,400 in the third round), and latency grows with the queue: one fsync per message, not batched across publishers. The system disk saw 2.0 flush requests per acknowledged publish under `always` (2.01 to 2.04 over the nine phases, counted over the same window as the acknowledgements) and 6 to 620 per phase under the default (the whole VM's disk, so a rough figure). An earlier version counted the warm-up second in the flushes but not in the acknowledgements and reported about 2.3.
- Worst single publish per phase: 17 to 74 ms (`always`), 8 to 21 ms (default). No phase had a publish error.
- One-time consume benchmark (20,000-event backlog, no-op handler, default policy, one run): plain pull and ack 102,000 per second; stored-cursor tail 38,000 per second (cursor saved every 100); durable layer 6,900 per second (one direct get per message, handled in sequence). An earlier run gave 87,000, 26,000 and 5,500.
- These numbers are for this VM's disk. A different disk changes the `always` figures.

## Findings

1. **Default policy, SIGKILL:** no acknowledged loss in 51,626 acknowledged messages in the final run (30 SIGKILLs), and none in the two earlier runs (about 106,000 more). See Results 1.
2. **JetStream gives no gap signal.** After eviction the consumer silently continues at the oldest retained message. The marker has to be derived from the sequence jump (and from the stream's first sequence when idle). It works for `MaxMsgs` and `MaxAge`; a consumer filtered to some subjects would also see jumps for other subjects' messages (by construction, not tested), so the subscriber uses the whole subject space.
3. **A delivered but unacknowledged message that is then evicted vanishes from the consumer's ack state** (nats-server 2.15.2): the ack floor jumps past it, pending goes to zero, nothing is redelivered (`TestFinding_...`). A durable subscriber that restarts afterwards cannot mark that range; the stored-cursor subscriber can (its own cursor is the truth). The test pins this behaviour, so a change in a later release shows up.
4. **A durable consumer's ack state lags behind its acknowledgements,** so a server SIGKILL shortly after them replays events that were already acknowledged. The server writes a consumer's state to disk with a minimum interval of 100 ms between writes (`flushLoop` in nats-server v2.15.2 `server/filestore.go`, `minTime`); this limits how often it writes, it is not a maximum delay, and the write itself and scheduling add more. In `TestCursor_AckStateLagsBehindConfirmedAcks` every message is acknowledged with a confirmed acknowledgement and the state is read from the server before the kill: the floor was 60 before the kill and 0 after it in 8 of 8 immediate trials (all 60 delivered again), and unchanged in 3 of 3 trials killed a second later (Results 4). At-least-once is kept; handlers must de-duplicate by event id.
5. **Redelivery does reorder** with more than one message pending (NAK or ack-wait expiry); the controls show it. A consumer with `MaxAckPending` 1 would serialise delivery and avoid it, at the cost of parallelism; this spike did not test that setting.
6. **Unacknowledged leftovers pin the ack floor.** Events the layer hands over ahead of their own delivery, and events fetched but not yet handled at shutdown, stay unacknowledged at the server. A restart then replays everything after the oldest of them (101 repeats in the first version of the cursor test). The layer now NAKs the unhandled rest of a batch on shutdown and acknowledges a re-sequenced event when it still holds its delivery. That acknowledgement first called `Ack` on the NAKed message object, which the client refuses (`ErrMsgAlreadyAckd`, the error was discarded), so the event stayed pending at the server until its redelivery; it now goes to the message's reply subject as a request and the confirmation is checked, and a test reads the server's pending count before the NAK delay ends. After a crash (no shutdown) the replay window is still everything since the oldest unacknowledged event, so a handler must de-duplicate by event id.
7. **A cursor must carry the event id** to detect a stream whose tail was lost and refilled (`ErrHistoryRewritten`).
8. **A setting that is not applied looks like the default.** A mistyped fsync option and the default behave the same until power fails, so the spike asserts the option the server parsed (the READY line) and counts the fsync calls.
9. **A slow stored-cursor subscriber holds up to `PullMaxMessages` events in client memory** (500 by default in nats.go), so retention reaches it only after it has worked through them. `TailConfig.PullMaxMessages` sets the bound; the slow-subscriber test uses 5. Coverage is exact either way, and nothing is lost that the client already holds.
10. **A failed event that retention evicts before its retry vanishes from a running durable subscriber, without a marker.** The gap tracker advances when a message is delivered, not when its handler succeeds, and the re-sequencer reads only retained predecessors. So an event whose handler keeps failing until it is evicted is neither handled nor inside any gap, while the subscriber stays up (`TestFinding_Durable_FailedThenEvictedEventLeavesNoMarkerWhileRunning`). This is the live counterpart of Finding 3, which is the restart case. The stored-cursor subscriber does not have it: it stops at the failing event and its cursor stays before it, so the later gap covers the event.
11. **A subscription needs its starting boundary stored when it starts.** A cursor that is only written after the first successful handler call cannot tell a new subscription from one that started and lost its first events, so the second would restart as new and skip them without a marker. The first version did exactly that (the durable consumer had the same hole when retention overtook it before its first fetch). Now the cursor carries `Started`, the tail stores `FirstSeq-1` before delivering, and the durable layer takes the next owed sequence from the consumer state the server created at the stream's first sequence. What was evicted before a subscription ever started is still not owed to it.

## What this means for the HBEPs

- **HBEP-10 "Guaranteed delivery" (proposed meaning)** is implementable as written: at least once, ordered per resource, durable across restarts, resumable from a cursor, each event with a unique ID, an explicit gap marker. Suggested wording: subscribers keep their own cursor (sequence and last event id) in the control plane; the log is embedded JetStream with `sync_interval: always`; a gap is the range `From..To` of sequences evicted unread; a cursor that no longer matches the log is an error ("history rewritten"), and the subscriber re-reads status.
- **Open question "push or pull delivery":** this spike favours a stored cursor over an ordered consumer (the implemented one uses the pull API of nats.go and is in stream order by construction). Callback with acknowledgement (durable consumer) needs the re-sequencer and cannot mark pending-then-evicted ranges. If the HBEP wants server-side backlog visibility ("unacknowledged backlog visible to its owner"), the backlog of a cursor subscriber is the stream's last sequence minus the cursor, which needs no consumer state.
- **HBEP-8:** the "gap" event for an observer that restarts (open question) and the gap marker for a late subscriber are the same mechanism. Retention per event class can be `MaxMsgs`, `MaxAge` or `MaxBytes`; gap detection does not depend on which.
- **Usage records:** `always` caps one node at about 1,000 acknowledged publishes per second on this disk (950 to 1,360 in these runs), so a front that meters every request should publish usage records in batches (one message of many records); below a few hundred records per second one record per message is fine. The default policy is a risk for usage records as HBEP-8 is written ("a lost record must show as a gap in the usage record, never as silence"), if a power loss does lose the tail under it (not verified here): a lost tail that no subscriber has read yet would be silent, because the restarted server hands the lost sequences out again (the async control did) and `ErrHistoryRewritten` fires only for a cursor that had gone past the lost range. It would meet HBEP-8 only if the front also put its own monotonic counter in each usage record, so that a consumer sees the missing range.
- **Single node:** no cluster was tested, so Jepsen's split-brain and `.blk`-corruption findings are outside this spike; they are not evidence either way.

## Unknown

- **Power loss or a kernel crash:** nothing here verifies that `always` keeps acknowledged writes through one, or that the default loses them. The syscall counts show what the server asks of the OS, and the Jepsen study cited above concerns other versions (nats-server 2.12.1) and clustered configurations. A direct test needs a disk that drops unsynced writes (dm-flakey, a VM power cut; root needed) or a write-dropping FUSE passthrough. `/dev/fuse` is writable by the user on this VM, so a FUSE test might be possible without root. Not attempted.
- Bit flips, truncation of `.blk` files, snapshot files (Jepsen findings 3 and 4): not tested.
- Disks other than this VM's: the `always` ceiling will differ on NVMe, Ceph or Lustre.
- Several instances of one subscriber (competing consumers): per-resource order needs partitioning (one instance per resource key range); not tested. The re-sequencer assumes one active instance per consumer.
- A bound for the consumer-state lag: the isolated test shows that a kill within the write throttle replays acknowledged events, and how many in a burst, not the longest delay (the write may take longer than the 100 ms interval under load).
- Recovery time of a large store after SIGKILL, and growth of the store directory over weeks.
- `MaxAckPending` 1 as a simpler alternative to the re-sequencer (Finding 5).

## How to run

```
# Go 1.27.x on PATH, then, from the repository root:
cd spikes/s8
nice -n 10 go test -count=1 -v -timeout 60m ./...      # most of the time is the SIGKILL loops
nice -n 10 go run ./cmd/s8bench -rounds 3 -dur 8s -workers 1,8,32 -out results.json
```

Variables: `S8_KILL_ITERS` (default 30), `S8_LAG_IMMEDIATE` and `S8_LAG_SETTLED` (trials of the ack-state test, default 8 and 3), `S8_SEED` (kill timing), `S8_SCRATCH` (store directories; default `s8-data` under the system temp directory, put it on a large disk if that is small), `S8_KEEP=1` (keep the store directories), `S8_VERBOSE=1`. `strace` must be installed for the fsync test (it skips, reporting "unknown", otherwise). Tests and benchmark wait while the 1-minute load is above 4.0.

## Provenance (what is reused)

| Component | Project and URL | Licence | Version | Use |
|---|---|---|---|---|
| nats-server | https://github.com/nats-io/nats-server | Apache-2.0 | v2.15.2 | the embedded JetStream node (the `server` package) |
| nats.go | https://github.com/nats-io/nats.go | Apache-2.0 | v1.55.0 | client and `jetstream` API |
| CloudEvents Go SDK | https://github.com/cloudevents/sdk-go | Apache-2.0 | v2.16.2 | the event envelope |
| nats-io jwt, nkeys, nuid | https://github.com/nats-io | Apache-2.0 | v2.8.2, v0.4.16, v1.0.1 | nats-server dependencies |
| klauspost/compress | https://github.com/klauspost/compress | BSD-3-Clause (the root LICENSE file is BSD-3-Clause; it also carries Apache-2.0 and MIT texts for separately identified packages) | v1.20.3 | nats-server dependency |
| minio/highwayhash | https://github.com/minio/highwayhash | Apache-2.0 | v1.0.4 | nats-server dependency |
| google/uuid, golang.org/x/{crypto,sys,time} | https://github.com/google/uuid, https://go.googlesource.com/ | BSD-3-Clause | v1.6.0, v0.58.0, v0.49.0, v0.16.0 | dependencies |
| json-iterator/go, go.uber.org/zap, go.uber.org/multierr, antithesishq/antithesis-sdk-go | https://github.com/json-iterator/go, https://github.com/uber-go/zap, https://github.com/uber-go/multierr, https://github.com/antithesishq/antithesis-sdk-go | MIT | v1.1.12, v1.27.0, v1.11.0, v0.8.0-default-no-op | dependencies of the SDK and the server |
| modern-go/concurrent, modern-go/reflect2, google/go-tpm | https://github.com/modern-go, https://github.com/google/go-tpm | Apache-2.0 | see `go.mod` | dependencies |
| strace 6.8 | https://strace.io | LGPL-2.1 or later (per the Debian copyright file) | system package, not vendored | counting fsync calls in one test |
| `jsnode/`, `evlayer/`, `harness/`, `tests/`, `cmd/s8bench/` | this repository | none declared: the repository has no LICENSE file at its root (checked on origin/main, 30 Oct 2026) | new | the spike's own code |

The licences above were read from each module's LICENSE file. The idea of an embedded NATS JetStream with CloudEvents comes from the state-of-the-art evaluation; no code was copied from another project, so there is no `third_party/forked/` entry.
