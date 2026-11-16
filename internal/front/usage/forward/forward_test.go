package forward

// The forwarder against the durable log as a REAL child process (HBEP-8, Test plan, Beta: "Usage
// while the log is down", "A lost tail", "Full streams", "The caps fit the disk"; issue #625). The
// log is stopped or SIGKILLed through a pidfd opened at its start (internal/eventlog/node/nodetest),
// and the stream is read back through a FRESH client. The expectation is always the FILES: each
// counter once, each message's bytes equal to its lines in the files, never what the forwarder held
// in memory and never its acknowledgements.
//
// Ports: 28510 and 28511, the forwarder's pair of #625's block (IMPL-COMMON). The tests start one
// log at a time and do not run in parallel.

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog/node/nodetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage/usagetest"
)

// HBEP-8, Test plan, Beta, "Usage while the log is down". Records 1 to 1,000 are written and
// acknowledged by the log, and the gauge reads 0. The log is stopped, and records 1,001 to 1,500
// are written: every Write returns nil and the gauge reads 500. The log restarts; the forwarder
// reads the stream's last counter, 1,000, and publishes 1,001 to 1,500. The stream then holds 1 to
// 1,500, each counter once, each record's bytes equal to its line in the files, and the gauge reads
// 0. The queue is lowered to 8 batches, so the 500 records written while the log is down cannot all
// wait in it: the forwarder must read them back from the files, which Dropped and FromFiles show.
func TestUsageWhileTheLogIsDown(t *testing.T) {
	d := nodetest.NewDir(t, logPorts, bigCaps)
	p := d.Start(t)
	p.Ready()
	clk := &clock{}
	clk.set(time.Date(2026, 11, 16, 8, 0, 0, 0, time.UTC))
	h := startHost(t, d, t.TempDir(), func(o *Options) { o.QueueBatches, o.Now = 8, clk.now }, nil)
	h.awaitConnected()

	h.write(1000, 8)
	h.awaitCaughtUp(1000)
	if s := h.fwd.Stats(); !s.BehindSince.IsZero() {
		t.Fatalf("1,000 records logged, gauge 0, but BehindSince is %v", s.BehindSince)
	}
	usagetest.CheckStream(t, d, h.usageDir, usagetest.Range(1, 1000))

	p.Stop()
	h.awaitDown()
	down := time.Date(2026, 11, 16, 9, 30, 0, 0, time.UTC)
	clk.set(down)
	first, last := h.write(500, 8)
	if first != 1001 || last != 1500 {
		t.Fatalf("the records written while the log is down have counters %d to %d, want 1001 to 1500", first, last)
	}
	s := h.fwd.Stats()
	if s.Connected || s.Logged != 1000 || s.Synced != 1500 || s.Unlogged != 500 {
		t.Errorf("log down: connected %v, logged %d, synced %d, unlogged %d; want false, 1000, 1500, 500", s.Connected, s.Logged, s.Synced, s.Unlogged)
	}
	if !s.BehindSince.Equal(down) {
		t.Errorf("BehindSince = %v, want %v, the time of the first record written after the log went down", s.BehindSince, down)
	}
	if s.Dropped == 0 {
		t.Error("no batch was dropped although 500 records were written into a queue of 8: the test does not exercise the files")
	}
	clk.set(down.Add(time.Hour)) // the log has been down for an hour: still behind since the first record
	if got := h.fwd.Stats().BehindSince; !got.Equal(down) {
		t.Errorf("BehindSince moved to %v while the log stayed down, want %v", got, down)
	}

	d.Start(t).Ready()
	h.awaitCaughtUp(1500)
	usagetest.CheckStream(t, d, h.usageDir, usagetest.Range(1, 1500))
	s = h.fwd.Stats()
	if s.FromFiles == 0 {
		t.Error("nothing was published from the files, yet records 1,001 to 1,500 reached the log")
	}
	if !s.BehindSince.IsZero() {
		t.Errorf("BehindSince = %v with the gauge at 0, want zero", s.BehindSince)
	}
}

// Killed mid-batch, the log still ends with each counter once (HBEP-8, Test plan, Beta, "Usage while
// the log is down"). The records 1 to 1,500 are written before the log starts, and the forwarder
// publishes them in 60 messages of 25 lines. The log is SIGKILLed, through its pidfd, from a hook
// that runs just before the k-th publish, k chosen from a seeded generator (the seed is logged and
// can be set with HB_TEST_SEED): the publish then races the kill, so across runs the message in
// flight is stored and lost its acknowledgement, or never stored. Either way the restarted log must
// hold each counter once: the forwarder reads the stream's last message again before it goes on.
func TestKilledMidBatchTheLogStillEndsWithEachCounterOnce(t *testing.T) {
	seed := time.Now().UnixNano()
	if s := os.Getenv("HB_TEST_SEED"); s != "" {
		var err error
		if seed, err = strconv.ParseInt(s, 10, 64); err != nil {
			t.Fatal(err)
		}
	}
	k := 3 + rand.New(rand.NewSource(seed)).Intn(48) // the 3rd to the 50th of 60 messages
	t.Logf("seed %d: the log is killed before publish number %d of 60", seed, k)

	d := nodetest.NewDir(t, logPorts, bigCaps)
	var victim atomic.Pointer[nodetest.Proc]
	var published atomic.Int64
	h := startHost(t, d, t.TempDir(), func(o *Options) { o.MaxLines = 25 }, func(f *Forwarder) {
		f.beforePublish = func(first, last uint64) {
			switch published.Add(1) {
			case int64(k):
				// The kill races this publish: the message may be stored and acknowledged, stored and
				// not acknowledged, or not stored.
				if err := victim.Load().SendKill(); err != nil {
					t.Errorf("SIGKILL through the pidfd: %v", err)
				}
			case int64(k) + 1:
				// Reached only if the k-th publish got its acknowledgement first: the next message is
				// published to a log that is dead, so the kill always lands before the 60 are done.
				<-victim.Load().Exited()
			}
		}
	})
	// The forwarder runs with the log not started yet: the records are in the files alone.
	if first, last := h.write(1500, 8); first != 1 || last != 1500 {
		t.Fatalf("counters %d to %d, want 1 to 1500", first, last)
	}
	p := d.Start(t)
	victim.Store(p)
	p.Ready()
	p.WaitKilled() // the hook killed it, mid-catch-up
	if got := published.Load(); got < int64(k) || got > int64(k)+1 {
		t.Fatalf("%d publishes were attempted when the log was killed, want %d or %d of 60: the kill did not land mid-batch", got, k, k+1)
	}
	h.awaitDown()

	d.Start(t).Ready()
	h.awaitCaughtUp(1500)
	usagetest.CheckStream(t, d, h.usageDir, usagetest.Range(1, 1500))
	if s := h.fwd.Stats(); s.Failures == 0 {
		t.Error("the kill never made a publish or a connection fail")
	}
}

// HBEP-8, Test plan, Beta, "A lost tail". A power loss is emulated, as S8 did, by restoring an older
// copy of the store: the stream ends at counter 400 while the files hold 1 to 1,500. The forwarder
// publishes 401 to 1,500, and the stream then matches the files. The copy is made with the log
// stopped (a copy of a live store is not consistent) when the stream ends at 400.
func TestALostTailIsPublishedFromTheFiles(t *testing.T) {
	d := nodetest.NewDir(t, logPorts, bigCaps)
	p := d.Start(t)
	p.Ready()
	h := startHost(t, d, t.TempDir(), nil, nil)
	h.awaitConnected()
	h.write(400, 4)
	h.awaitCaughtUp(400)
	p.Stop()
	h.awaitDown()
	backup := d.CopyStore(t)

	p = d.Start(t)
	p.Ready()
	h.awaitConnected()
	h.write(1100, 8)
	h.awaitCaughtUp(1500)
	usagetest.CheckStream(t, d, h.usageDir, usagetest.Range(1, 1500))

	p.Stop()
	h.awaitDown()
	d.RestoreStore(t, backup)
	d.Start(t).Ready()
	// The forwarder's last counter was 1,500, and the restored stream's is 400: it holds 1,500 again
	// only after it has read the stream and published 401 to 1,500.
	h.awaitCaughtUp(1500)
	usagetest.CheckStream(t, d, h.usageDir, usagetest.Range(1, 1500))
	if s := h.fwd.Stats(); s.FromFiles == 0 {
		t.Error("nothing was published from the files, yet the restored stream lacked 401 to 1,500")
	}
}

// padded makes a usage record whose line is exactly `size` bytes with its newline, as the sink writes
// it, when it gets counter n: the record's own encoding, with a request ID padded to fit.
func padded(t *testing.T, n uint64, size int) usage.Record {
	t.Helper()
	enc := func(pad int) int {
		var b bytes.Buffer
		if err := json.NewEncoder(&b).Encode(usage.Record{Counter: n, RequestID: strings.Repeat("p", pad), Path: "/v1/chat/completions", Status: 200}); err != nil {
			t.Fatal(err)
		}
		return b.Len()
	}
	pad := size - enc(0)
	if pad < 0 || enc(pad) != size {
		t.Fatalf("a record cannot be made %d bytes with counter %d (its bare line is %d)", size, n, enc(0))
	}
	return usage.Record{RequestID: strings.Repeat("p", pad), Path: "/v1/chat/completions", Status: 200}
}

// HBEP-8, Test plan, Beta, "Full streams" (the usage half): with a usage cap of 10 one-record
// messages and records 1 to 30 written, HB_USAGE holds counters 21 to 30, no publish fails, the
// gauge returns to 0, and the files hold 1 to 30. The cap is exactly 10 messages' logical size,
// worked out by hand from nats-server v2.15.2's per-message formula (filestore.go,
// fileStoreMsgSizeRaw: 22 + subject + 4 + header + payload + 8):
//
//	subject "hb.usage"                                  8
//	header "NATS/1.0\r\nNats-Msg-Id: <id>\r\n\r\n"      10 + 13 + len(id) + 2 + 2, the id usage-N-N being 7 + 2d for a d-digit N
//	message of one line of L bytes: 22 + 8 + 4 + (34 + 2d) + L + 8 = 76 + 2d + L
//
// A line of 622 bytes for a one-digit counter and of 620 for a two-digit one (a record's bare line is
// 406 bytes, so a padded request ID makes up the rest) makes every message 700 bytes, so the cap is
// 7,000. The stream's own byte count is checked against the formula.
func TestTheUsageCapKeepsTheLatestTenRecordsAndNothingFails(t *testing.T) {
	const msgSize, capBytes = 700, 10 * 700
	d := nodetest.NewDir(t, logPorts, eventlog.Caps{Events: 1 << 20, Usage: capBytes})
	d.Start(t).Ready()
	// The safety-net poll is set far off: a poll that fell between two Writes would read the files and
	// could publish two records as one message.
	h := startHost(t, d, t.TempDir(), func(o *Options) { o.PollEvery = time.Hour }, nil)
	h.awaitConnected()
	h.awaitServing(1) // from here each synced record is its own message, from the channel

	for n := uint64(1); n <= 30; n++ {
		line := 624 - 2*len(strconv.FormatUint(n, 10)) // 622 for one digit, 620 for two
		if got, err := h.sink.Write(padded(t, n, line)); err != nil || got != n {
			t.Fatalf("Write %d = %d, %v", n, got, err)
		}
	}
	h.awaitCaughtUp(30)

	_, js := d.Connect(t)
	st := nodetest.StreamInfo(t, js, eventlog.StreamUsage).State
	if st.Msgs != 10 || st.FirstSeq != 21 || st.LastSeq != 30 || st.Bytes != capBytes {
		t.Fatalf("HB_USAGE: %d messages, sequences %d to %d, %d bytes; want 10 messages, 21 to 30, %d bytes (10 x %d, worked by hand)",
			st.Msgs, st.FirstSeq, st.LastSeq, st.Bytes, capBytes, msgSize)
	}
	msgs := usagetest.CheckStream(t, d, h.usageDir, usagetest.Range(21, 30))
	for _, m := range msgs {
		if bytes.Count(m.Payload, []byte("\n")) != 1 {
			t.Errorf("message %s holds more than one record", m.ID)
		}
	}
	if s := h.fwd.Stats(); s.Failures != 0 || s.Unlogged != 0 || s.LastError != "" {
		t.Errorf("failures %d, unlogged %d, last error %q: no publish may fail at the cap, and the gauge returns to 0", s.Failures, s.Unlogged, s.LastError)
	}
	if files := usagetest.FileLines(t, h.usageDir); len(files) != 30 {
		t.Errorf("the files hold %d records, want 1 to 30", len(files))
	}
}

// R4 (the science agent, 16 Nov 2026): on an EMPTY stream the forwarder starts from the oldest daily
// file of the newest set of files whose sizes fit within the usage cap. Four daily files of 100-byte
// lines, by hand: 600 bytes (counters 1-6), 500 (7-11), 400 (12-15) and 300 (16-18).
//
//   - cap 1,000: 300 + 400 = 700 fit, and the third file would make 1,200: the set is the two
//     newest, and the first counter published is 12;
//   - cap 3,000: all four, 1,800 bytes, fit: the first counter is 1;
//   - cap 250: the newest file alone, 300, is over the cap and is the set anyway: the first
//     counter is 16, and the stream, whose own limit decides what it keeps, ends with 18.
//
// The first two read the stream: its messages are the 700 and the 1,800 bytes of lines, and fit their
// caps with their headers. The third reads the forwarder's first publish through the test hook, since
// a cap of 250 keeps one record of the three.
func TestAnEmptyStreamStartsAtTheOldestFileOfTheNewestSetThatFits(t *testing.T) {
	files := map[string]string{
		"usage-2026-11-10.jsonl": lines(t, 1, 6, 100),
		"usage-2026-11-11.jsonl": lines(t, 7, 11, 100),
		"usage-2026-11-12.jsonl": lines(t, 12, 15, 100),
		"usage-2026-11-13.jsonl": lines(t, 16, 18, 100),
	}
	for _, c := range []struct {
		name     string
		cap      int64
		maxLines int
		want     []uint64
		firstPub uint64
	}{
		{"cap 1000", 1000, 0, usagetest.Range(12, 18), 12},
		{"cap 3000", 3000, 0, usagetest.Range(1, 18), 1},
		{"the newest file alone is over the cap", 250, 1, usagetest.Range(18, 18), 16},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, text := range files {
				put(t, dir, name, text)
			}
			d := nodetest.NewDir(t, logPorts, eventlog.Caps{Events: 1 << 20, Usage: c.cap})
			d.Start(t).Ready()
			var firstPub atomic.Uint64
			r := startForwarder(t, d, dir, func() uint64 { return 18 }, func(o *Options) { o.MaxLines = c.maxLines }, func(f *Forwarder) {
				f.beforePublish = func(first, _ uint64) { firstPub.CompareAndSwap(0, first) }
			})
			r.awaitCaughtUp(18)
			if got := firstPub.Load(); got != c.firstPub {
				t.Errorf("the first message begins at counter %d, want %d", got, c.firstPub)
			}
			usagetest.CheckStream(t, d, dir, c.want)
		})
	}
}

// Counters have gaps (a crash's reserved block), and the forwarder takes the next counter PRESENT
// after the stream's last, never stalling on a missing one. Day one holds 1 to 5 and is forwarded.
// A crash leaves a gap, and day two holds 1,001 to 1,003. A new forwarder reads the stream's last
// counter, 5, and publishes 1,001 to 1,003: the stream holds 1-5 and 1,001-1,003, each once.
func TestACounterGapIsCrossedNotWaitedFor(t *testing.T) {
	dir := t.TempDir()
	put(t, dir, "usage-2026-11-10.jsonl", lines(t, 1, 5, 100))
	d := nodetest.NewDir(t, logPorts, bigCaps)
	d.Start(t).Ready()
	a := startForwarder(t, d, dir, func() uint64 { return 5 }, nil, nil)
	a.awaitCaughtUp(5)
	usagetest.CheckStream(t, d, dir, usagetest.Range(1, 5))
	a.stop()

	put(t, dir, "usage-2026-11-11.jsonl", lines(t, 1001, 1003, 100))
	b := startForwarder(t, d, dir, func() uint64 { return 1003 }, nil, nil)
	b.awaitCaughtUp(1003)
	want := append(usagetest.Range(1, 5), usagetest.Range(1001, 1003)...)
	usagetest.CheckStream(t, d, dir, want)
	if s := b.fwd.Stats(); s.Failures != 0 {
		t.Errorf("%d failures while crossing the gap", s.Failures)
	}
}

// The forwarder takes only what the sink has synced. The file holds 10 lines but the sink reports
// 6 synced: lines 7 to 10 are written and not yet synced, which HBEP-8 forbids publishing (the sink
// may still cut them back). Only 1 to 6 reach the log. When the sink offers 7 to 10 they are
// published from the channel, and a batch the log already holds, offered again, or only partly
// new, publishes only what is new: each counter once.
func TestTheFilesAreReadOnlyUpToWhatTheSinkSyncedAndOffersNeverDuplicate(t *testing.T) {
	dir := t.TempDir()
	all := lines(t, 1, 10, 100)
	put(t, dir, "usage-2026-11-10.jsonl", all)
	d := nodetest.NewDir(t, logPorts, bigCaps)
	d.Start(t).Ready()
	r := startForwarder(t, d, dir, func() uint64 { return 6 }, nil, nil)
	r.awaitCaughtUp(6)
	usagetest.CheckStream(t, d, dir, usagetest.Range(1, 6)) // the file also holds 7 to 10, not yet synced

	offer := func(from, to int) { r.fwd.Offer(uint64(from), uint64(to), []byte(lines(t, from, to, 100))) }
	offer(7, 10)
	r.awaitCaughtUp(10)
	// The sink writes 11 and 12; the same lines are offered again (a drop and a refill), 3 to 5 whole,
	// 10 to 12 partly. The file must hold what the log is expected to.
	put(t, dir, "usage-2026-11-10.jsonl", all+lines(t, 11, 12, 100))
	offer(3, 5)
	offer(10, 12)
	r.awaitCaughtUp(12)
	msgs := usagetest.CheckStream(t, d, dir, usagetest.Range(1, 12))
	if last := msgs[len(msgs)-1].ID; last != "usage-11-12" {
		t.Errorf("the last message is %s, want usage-11-12: the partly new batch must publish only its new lines", last)
	}
}

// Many writers while the log is up and the queue is tiny: batches are dropped and the forwarder
// moves between the channel and the files again and again. Each counter must still reach the log
// once, with the files' own bytes, whichever path took it. The safety-net poll is set far off, so a
// dropped batch comes back only by the behind mark: without it the test would wait for a poll that
// never comes. The drops are asserted, so a run that dropped nothing cannot pass.
func TestTheChannelAndTheFilesNeverDuplicateOrLoseARecord(t *testing.T) {
	d := nodetest.NewDir(t, logPorts, bigCaps)
	d.Start(t).Ready()
	h := startHost(t, d, t.TempDir(), func(o *Options) { o.QueueBatches, o.MaxLines, o.PollEvery = 2, 40, time.Hour }, nil)
	h.awaitConnected()
	h.write(1200, 16)
	h.awaitCaughtUp(1200)
	usagetest.CheckStream(t, d, h.usageDir, usagetest.Range(1, 1200))
	if s := h.fwd.Stats(); s.Failures != 0 {
		t.Errorf("%d failures with the log up", s.Failures)
	} else if s.Dropped == 0 || s.FromFiles == 0 || s.FromChannel == 0 {
		t.Errorf("dropped %d batches, %d messages from the files, %d from the channel: the test did not move between the two", s.Dropped, s.FromFiles, s.FromChannel)
	}
}

// A front started when the log is not there (HBEP-8, Test plan, "The caps fit the disk": at 23.5 GB
// the control plane starts without it) forwards nothing and loses nothing: Writes return nil, the
// forwarder is not connected and its logged counter is unknown, so a gauge built from it has no
// value (unknown is never 0). When the log starts, the forwarder connects by itself and publishes
// every record.
func TestALogThatIsNotThereAtStartIsReachedLater(t *testing.T) {
	d := nodetest.NewDir(t, logPorts, bigCaps)
	nodetest.NothingListens(t, logPorts.Client)
	h := startHost(t, d, t.TempDir(), nil, nil)
	h.write(50, 4)
	eventually(t, "the forwarder to have tried the log", func() bool { return h.fwd.Stats().Failures > 0 })
	s := h.fwd.Stats()
	if s.Connected || s.LoggedKnown || s.Synced != 50 || s.Unlogged != 0 || s.LastError == "" {
		t.Errorf("no log: connected %v, logged known %v, synced %d, unlogged %d, last error %q; want false, false, 50, 0 (no reading), an error", s.Connected, s.LoggedKnown, s.Synced, s.Unlogged, s.LastError)
	}
	d.Start(t).Ready()
	h.awaitCaughtUp(50)
	usagetest.CheckStream(t, d, h.usageDir, usagetest.Range(1, 50))
}

// A forwarder with the wrong token never connects, and the log holds nothing from it.
func TestAWrongTokenNeverConnects(t *testing.T) {
	d := nodetest.NewDir(t, logPorts, bigCaps)
	d.Start(t).Ready()
	h := startHost(t, d, t.TempDir(), func(o *Options) { o.Token = strings.Repeat("0", 64) }, nil)
	h.write(5, 1)
	eventually(t, "two refused connections", func() bool { return h.fwd.Stats().Failures >= 2 })
	s := h.fwd.Stats()
	if s.Connected || s.LoggedKnown || !strings.Contains(strings.ToLower(s.LastError), "authorization") {
		t.Errorf("wrong token: connected %v, logged known %v, last error %q; want false, false, an authorization error", s.Connected, s.LoggedKnown, s.LastError)
	}
	if msgs := d.ReadAll(t, eventlog.StreamUsage); len(msgs) != 0 {
		t.Errorf("the log holds %d messages from a client that was refused", len(msgs))
	}
}

// The poll is a safety net beside the channel: records the sink synced that the stream lacks, with
// nothing in the channel to carry them, are read from the files. A hole of that kind is made by hand
// here, since the sink's own hook never leaves one: the sink's synced counter is moved past the log's
// without a batch being offered.
func TestAnIdlePollReadsTheFilesWhenTheChannelHasNothingToCarryWhatWasSynced(t *testing.T) {
	dir := t.TempDir()
	put(t, dir, "usage-2026-11-10.jsonl", lines(t, 1, 10, 100))
	d := nodetest.NewDir(t, logPorts, bigCaps)
	d.Start(t).Ready()
	r := startForwarder(t, d, dir, func() uint64 { return 6 }, func(o *Options) { o.PollEvery = 20 * time.Millisecond }, nil)
	r.awaitCaughtUp(6)
	usagetest.CheckStream(t, d, dir, usagetest.Range(1, 6))

	r.fwd.mu.Lock()
	r.fwd.synced = 10
	r.fwd.recomputeLocked()
	r.fwd.mu.Unlock()
	r.awaitCaughtUp(10)
	usagetest.CheckStream(t, d, dir, usagetest.Range(1, 10))
}

// HBEP-8, The durable log, Streams: a message holds up to 256 records and stays under nats-server's
// maximum payload of 1 MiB. 150 records of 8,000 bytes are 1.2 MB, so they cannot go as one message
// whatever their count: the forwarder cuts them by size. A record whose line alone is longer than a
// message (a client's model name can be as long as a request body) is passed over, never stalling the
// ones after it: the files hold it, the forwarder counts it, and the gauge returns to 0.
func TestMessagesStayUnderAMiBAndALineLongerThanOneIsSkipped(t *testing.T) {
	dir := t.TempDir()
	put(t, dir, "usage-2026-11-10.jsonl", lines(t, 1, 150, 8000)+line(t, 151, 1<<20+1000)+lines(t, 152, 153, 100))
	d := nodetest.NewDir(t, logPorts, bigCaps)
	d.Start(t).Ready()
	r := startForwarder(t, d, dir, func() uint64 { return 153 }, nil, nil)
	r.awaitCaughtUp(153)
	want := append(usagetest.Range(1, 150), 152, 153)
	msgs := usagetest.CheckStream(t, d, dir, want) // also fails on a message of 1 MiB or more
	if len(msgs) < 2 {
		t.Errorf("%d messages for 1.2 MB of records: they must be cut by size", len(msgs))
	}
	if s := r.fwd.Stats(); s.Skipped != 1 || s.Failures != 0 {
		t.Errorf("skipped %d records and %d failures, want 1 and 0", s.Skipped, s.Failures)
	}
}
