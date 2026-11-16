package node

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
)

// The per-message size nats-server counts against a cap, worked out by hand from filestore.go's
// fileStoreMsgSizeRaw: 22 + subject + 4 + header + payload + 8, where the header is the
// "NATS/1.0\r\nNats-Msg-Id: <id>\r\n\r\n" block, 10 + 13 + len(id) + 2 + 2 bytes. The tests check
// the figure against the bytes the stream reports, so a wrong formula fails there.
//
//	events: subject "hb.ev.instance.u1" (17), id "e0001" (5), header 32, payload 100: 22+17+4+32+100+8 = 183
//	usage:  subject "hb.usage" (8), id "u001" (4), header 31, payload 50:              22+8+4+31+50+8   = 123
const (
	eventSubject = "hb.ev.instance.u1"
	eventMsgSize = 183
	usageSubject = "hb.usage"
	usageMsgSize = 123
)

func eventID(n int) string { return fmt.Sprintf("e%04d", n) }
func usageID(n int) string { return fmt.Sprintf("u%03d", n) }

var (
	eventPayload = bytes.Repeat([]byte("x"), 100)
	usagePayload = bytes.Repeat([]byte("y"), 50)
)

func ctx20(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// The log serves with the settings HBEP-8 says: what the READY line reports is what nats-server
// parsed, and the streams read back from a client are the two of HBEP-8, with their caps.
func TestTheLogServesTheSettingsHBEP8Says(t *testing.T) {
	d := newLogDir(t, eventlog.Caps{Events: 3 * eventMsgSize, Usage: 10 * usageMsgSize})
	p := startLog(t, d)
	f := p.ready()
	for k, want := range map[string]string{
		"client": "127.0.0.1:28500", "monitor": "127.0.0.1:28501", "sync_always": "true", "jetstream": "true",
		"store_dir": d.dir, "token": "present", "streams": "HB_EVENTS,HB_USAGE", "events_cap": "549", "usage_cap": "1230",
	} {
		if f[k] != want {
			t.Errorf("READY %s=%q, want %q (the line: %v)", k, f[k], want, f)
		}
	}
	if strings.Contains(p.out.String(), d.token) {
		t.Error("the token is in the log's output")
	}

	_, js := connect(t, d.token)
	ev, us := streamInfo(t, js, "HB_EVENTS").Config, streamInfo(t, js, "HB_USAGE").Config
	// Hand-written from HBEP-8, one literal per setting.
	if !slices.Equal(ev.Subjects, []string{"hb.ev.>"}) || ev.Discard != jetstream.DiscardNew || ev.MaxBytes != 549 {
		t.Errorf("HB_EVENTS: subjects %v, discard %v, max bytes %d; want hb.ev.>, new, 549", ev.Subjects, ev.Discard, ev.MaxBytes)
	}
	if !slices.Equal(us.Subjects, []string{"hb.usage"}) || us.Discard != jetstream.DiscardOld || us.MaxBytes != 1230 {
		t.Errorf("HB_USAGE: subjects %v, discard %v, max bytes %d; want hb.usage, old, 1230", us.Subjects, us.Discard, us.MaxBytes)
	}
	for _, c := range []jetstream.StreamConfig{ev, us} {
		if c.Storage != jetstream.FileStorage || c.Replicas != 1 || c.Retention != jetstream.LimitsPolicy || c.Duplicates != 2*time.Minute ||
			!c.AllowDirect || c.Compression != jetstream.S2Compression || c.MaxAge != 0 || c.MaxMsgs > 0 {
			t.Errorf("%s: %+v does not have HBEP-8's settings (file, 1 replica, limits, 2 min, direct get, s2, no max age, no max messages)", c.Name, c)
		}
	}
	// Health: nats-server's /healthz on the monitoring port, without js-enabled-only.
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/healthz", monitorPort))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("/healthz answered %d: %s", resp.StatusCode, body)
	}
	if fi, err := os.Stat(filepath.Join(d.dir, eventlog.RefusalFile)); err == nil {
		t.Errorf("a refusal is on record while the log serves: %v", fi.Name())
	}
}

func TestAClientWithoutTheTokenIsRefused(t *testing.T) {
	d := newLogDir(t, eventlog.Caps{Events: 1 << 20, Usage: 1 << 20})
	p := startLog(t, d)
	p.ready()
	if nc, err := nats.Connect(clientURL(), nats.MaxReconnects(0)); err == nil {
		nc.Close()
		t.Fatal("a client with no token connected")
	} else if !strings.Contains(err.Error(), "uthorization") {
		t.Errorf("no token: %v, want an authorization violation", err)
	}
	if nc, err := nats.Connect(clientURL(), nats.Token(strings.Repeat("0", 64)), nats.MaxReconnects(0)); err == nil {
		nc.Close()
		t.Fatal("a client with another token connected")
	}
	// Control: the right token connects, so the refusals above are about the token.
	nc, _ := connect(t, d.token)
	if !nc.IsConnected() {
		t.Fatal("the right token did not connect")
	}
}

// A misspelt setting is refused, and a setting nats-server parses but not as HBEP-8 says is too.
// Reading the configuration file would prove nothing (S8, Finding 8), so each row below is judged by
// what the process says after nats-server read the file, and by the process's exit status.
func TestTheLogRefusesASettingThatIsNotAsParsed(t *testing.T) {
	for name, c := range map[string]struct{ from, to, want string }{
		"a misspelt sync_interval":  {"sync_interval: always", "sync_intervall: always", "sync_interval"},
		"sync_interval as a time":   {"sync_interval: always", "sync_interval: 30s", "sync_interval"},
		"no sync_interval at all":   {"  sync_interval: always\n", "", "sync_interval"},
		"a misspelt store_dir":      {"store_dir:", "store_dirr:", "store_dir"},
		"a store elsewhere":         {`store_dir: "`, `store_dir: "/tmp/not-the-launchers`, "store_dir"},
		"a client listener on any":  {"listen: 127.0.0.1:28500", "listen: 0.0.0.0:28500", "client listener"},
		"a misspelt authorization":  {"authorization {", "authorisation {", "authorisation"},
		"monitoring on any address": {"http: 127.0.0.1:28501", "http: 0.0.0.0:28501", "monitoring listener"},
	} {
		t.Run(name, func(t *testing.T) {
			d := newLogDir(t, eventlog.Caps{Events: 1 << 20, Usage: 1 << 20})
			text, _ := os.ReadFile(d.confPath())
			if !strings.Contains(string(text), c.from) {
				t.Fatalf("the configuration has no %q:\n%s", c.from, text)
			}
			d.writeConf(t, strings.Replace(string(text), c.from, c.to, 1))
			p := startLog(t, d)
			out := p.refused()
			if !strings.Contains(out, c.want) {
				t.Errorf("the output does not name %q:\n%s", c.want, out)
			}
			// The refusal is on record for status, and nothing was served.
			rec, err := os.ReadFile(filepath.Join(d.dir, eventlog.RefusalFile))
			if err != nil || !strings.Contains(string(rec), c.want) {
				t.Errorf("%s: %q, %v; want it to name %q", eventlog.RefusalFile, rec, err, c.want)
			}
			nothingListens(t, clientPort)
		})
	}
	// Control: the unmodified file serves, so each refusal above is about its one change.
	d := newLogDir(t, eventlog.Caps{Events: 1 << 20, Usage: 1 << 20})
	startLog(t, d).ready()
}

// preCreate makes a stream in the store as another nats-server would have left it, with the settings
// cfg says, and shuts that server down: the next start finds it there.
func preCreate(t *testing.T, d *logDir, cfg jetstream.StreamConfig) {
	t.Helper()
	srv, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: d.dir, NoLog: true, NoSigs: true, Authorization: d.token})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(30 * time.Second) {
		t.Fatal("the pre-creating server is not ready")
	}
	nc, err := nats.Connect("", nats.InProcessServer(srv), nats.Token(d.token))
	if err != nil {
		t.Fatal(err)
	}
	js, _ := jetstream.New(nc)
	if _, err := js.CreateStream(ctx20(t), cfg); err != nil {
		t.Fatal(err)
	}
	nc.Close()
	srv.Shutdown()
	srv.WaitForShutdown()
}

// A stream that exists with another setting is reported, never corrected: nats-server would accept
// the correction, and the check would then prove nothing.
func TestAnExistingStreamWithAnotherSettingIsRefusedByName(t *testing.T) {
	for name, c := range map[string]struct {
		cfg  jetstream.StreamConfig
		want []string
	}{
		"HB_EVENTS discard old":                       {jetstream.StreamConfig{Name: "HB_EVENTS", Subjects: []string{"hb.ev.>"}, Discard: jetstream.DiscardOld, MaxBytes: 1 << 20, Storage: jetstream.FileStorage}, []string{"HB_EVENTS", "discard"}},
		"HB_USAGE discard new":                        {jetstream.StreamConfig{Name: "HB_USAGE", Subjects: []string{"hb.usage"}, Discard: jetstream.DiscardNew, MaxBytes: 1 << 20, Storage: jetstream.FileStorage}, []string{"HB_USAGE", "discard"}},
		"HB_EVENTS with a max age and no compression": {jetstream.StreamConfig{Name: "HB_EVENTS", Subjects: []string{"hb.ev.>"}, Discard: jetstream.DiscardNew, MaxBytes: 1 << 20, MaxAge: time.Hour, Storage: jetstream.FileStorage}, []string{"max_age", "compression"}},
		"HB_EVENTS on other subjects":                 {jetstream.StreamConfig{Name: "HB_EVENTS", Subjects: []string{"hb.other.>"}, Discard: jetstream.DiscardNew, MaxBytes: 1 << 20, Storage: jetstream.FileStorage, Compression: jetstream.S2Compression}, []string{"subjects"}},
	} {
		t.Run(name, func(t *testing.T) {
			d := newLogDir(t, eventlog.Caps{Events: 1 << 20, Usage: 1 << 20})
			preCreate(t, d, c.cfg)
			out := startLog(t, d).refused()
			for _, w := range c.want {
				if !strings.Contains(out, w) {
					t.Errorf("the refusal does not name %q:\n%s", w, out)
				}
			}
			nothingListens(t, clientPort)
		})
	}
}

// With an events cap of three messages of one fixed size, the fourth publish is refused with
// nats-server's own words, and the three stay.
func TestTheEventsCapRefusesTheFourthMessageAndKeepsTheThree(t *testing.T) {
	d := newLogDir(t, eventlog.Caps{Events: 3 * eventMsgSize, Usage: 1 << 20})
	startLog(t, d).ready()
	_, js := connect(t, d.token)
	ctx := ctx20(t)
	for n := 1; n <= 3; n++ {
		if err := publish(ctx, js, eventSubject, eventID(n), eventPayload); err != nil {
			t.Fatalf("publish %d: %v", n, err)
		}
	}
	// The stream's own count of the bytes agrees with the hand-worked per-message size.
	if st := streamInfo(t, js, "HB_EVENTS").State; st.Msgs != 3 || st.Bytes != 3*eventMsgSize {
		t.Fatalf("three messages stored: %d messages, %d bytes, want 3 and %d (the hand-worked size)", st.Msgs, st.Bytes, 3*eventMsgSize)
	}
	err := publish(ctx, js, eventSubject, eventID(4), eventPayload)
	if err == nil || !strings.Contains(err.Error(), "maximum bytes exceeded") {
		t.Fatalf("the fourth publish: %v, want nats-server's maximum bytes exceeded", err)
	}
	if st := streamInfo(t, js, "HB_EVENTS").State; st.Msgs != 3 || st.FirstSeq != 1 || st.LastSeq != 3 {
		t.Fatalf("after the refusal: %d messages, sequences %d to %d, want all three kept, 1 to 3", st.Msgs, st.FirstSeq, st.LastSeq)
	}
}

// After the cap is raised and the log restarted, publishing resumes. Lowering the cap below the
// stream's bytes and restarting leaves its first sequence unchanged, and every message: a cap tested
// with discard old would prove nothing for events, since it deletes while its publishes succeed.
func TestRaisingTheCapResumesPublishingAndLoweringItEvictsNothing(t *testing.T) {
	d := newLogDir(t, eventlog.Caps{Events: 3 * eventMsgSize, Usage: 1 << 20})
	p := startLog(t, d)
	p.ready()
	_, js := connect(t, d.token)
	ctx := ctx20(t)
	for n := 1; n <= 3; n++ {
		if err := publish(ctx, js, eventSubject, eventID(n), eventPayload); err != nil {
			t.Fatal(err)
		}
	}
	if err := publish(ctx, js, eventSubject, eventID(4), eventPayload); err == nil {
		t.Fatal("the fourth publish was accepted at a cap of three")
	}
	p.stop()

	// Raised to four messages: the same publish now succeeds.
	d.setCaps(t, eventlog.Caps{Events: 4 * eventMsgSize, Usage: 1 << 20})
	p = startLog(t, d)
	p.ready()
	_, js = connect(t, d.token)
	if got := streamInfo(t, js, "HB_EVENTS").Config.MaxBytes; got != 4*eventMsgSize {
		t.Fatalf("the cap after the raise is %d, want %d", got, 4*eventMsgSize)
	}
	if err := publish(ctx, js, eventSubject, eventID(4), eventPayload); err != nil {
		t.Fatalf("publishing did not resume after the cap was raised: %v", err)
	}
	p.stop()

	// Lowered to two messages, below the four the stream holds.
	d.setCaps(t, eventlog.Caps{Events: 2 * eventMsgSize, Usage: 1 << 20})
	p = startLog(t, d)
	p.ready()
	_, js = connect(t, d.token)
	info := streamInfo(t, js, "HB_EVENTS")
	if info.Config.MaxBytes != 2*eventMsgSize || info.State.Msgs != 4 || info.State.FirstSeq != 1 || info.State.LastSeq != 4 || info.State.Bytes != 4*eventMsgSize {
		t.Fatalf("after the lowering: cap %d, %d messages, sequences %d to %d, %d bytes; want cap %d, 4 messages, 1 to 4, %d bytes: nothing evicted",
			info.Config.MaxBytes, info.State.Msgs, info.State.FirstSeq, info.State.LastSeq, info.State.Bytes, 2*eventMsgSize, 4*eventMsgSize)
	}
	// The stream is over its cap, so it refuses new events until it is under it.
	if err := publish(ctx, js, eventSubject, eventID(5), eventPayload); err == nil || !strings.Contains(err.Error(), "maximum bytes exceeded") {
		t.Fatalf("a publish to a stream over its lowered cap: %v, want maximum bytes exceeded", err)
	}
}

// Lowering HB_USAGE's cap drops its oldest messages at once, with no publish: it is a rolling copy.
// And the log keeps accepting new records at the cap, dropping the oldest, so no publish fails.
func TestLoweringTheUsageCapDropsTheOldestAtOnce(t *testing.T) {
	d := newLogDir(t, eventlog.Caps{Events: 1 << 20, Usage: 10 * usageMsgSize})
	p := startLog(t, d)
	p.ready()
	_, js := connect(t, d.token)
	ctx := ctx20(t)
	for n := 1; n <= 10; n++ {
		if err := publish(ctx, js, usageSubject, usageID(n), usagePayload); err != nil {
			t.Fatalf("publish %d: %v", n, err)
		}
	}
	if st := streamInfo(t, js, "HB_USAGE").State; st.Msgs != 10 || st.Bytes != 10*usageMsgSize {
		t.Fatalf("ten records: %d messages, %d bytes, want 10 and %d (the hand-worked size)", st.Msgs, st.Bytes, 10*usageMsgSize)
	}
	// At the cap, the eleventh record is accepted and the oldest goes.
	if err := publish(ctx, js, usageSubject, usageID(11), usagePayload); err != nil {
		t.Fatalf("a publish at the usage cap failed: %v", err)
	}
	if st := streamInfo(t, js, "HB_USAGE").State; st.Msgs != 10 || st.FirstSeq != 2 || st.LastSeq != 11 {
		t.Fatalf("at the cap: %d messages, sequences %d to %d, want 10 messages, 2 to 11", st.Msgs, st.FirstSeq, st.LastSeq)
	}
	p.stop()

	d.setCaps(t, eventlog.Caps{Events: 1 << 20, Usage: 5 * usageMsgSize})
	p = startLog(t, d)
	p.ready()
	_, js = connect(t, d.token)
	st := streamInfo(t, js, "HB_USAGE").State
	if st.Msgs != 5 || st.FirstSeq != 7 || st.LastSeq != 11 || st.Bytes != 5*usageMsgSize {
		t.Fatalf("right after the restart at a lower cap: %d messages, sequences %d to %d, %d bytes; want the newest five, 7 to 11, %d bytes",
			st.Msgs, st.FirstSeq, st.LastSeq, st.Bytes, 5*usageMsgSize)
	}
}

// Retention, the half that needs no publisher (HBEP-8, Test plan, Beta): the streams have no max
// age, and a closed block is compressed. nats-server sizes a block from the cap (dynBlkSize): under
// about 128 kB it is 32,000 bytes, so a cap of 120,000 gives blocks that close after 29 messages of
// 1,083 bytes (22 + 17 + 4 + 32 + 1000 + 8). A compressed block starts with "cmp", the algorithm
// byte (1 is S2) and the original size, then S2's stream framing, which opens with the stream
// identifier chunk 0xff 0x06 0x00 0x00 "S2sTwO" (klauspost/compress's S2 format; Snappy's is
// "sNaPpY"), read off a real block of this version.
func TestAClosedBlockIsCompressedAndTheOpenOneIsNot(t *testing.T) {
	d := newLogDir(t, eventlog.Caps{Events: 120000, Usage: 1 << 20})
	startLog(t, d).ready()
	_, js := connect(t, d.token)
	ctx := ctx20(t)
	payload := bytes.Repeat([]byte("a"), 1000)
	const sent = 70
	for n := 1; n <= sent; n++ {
		if err := publish(ctx, js, eventSubject, eventID(n), payload); err != nil {
			t.Fatalf("publish %d: %v", n, err)
		}
	}
	info := streamInfo(t, js, "HB_EVENTS")
	if info.Config.MaxAge != 0 || info.Config.Compression != jetstream.S2Compression {
		t.Fatalf("max age %v, compression %v, want none and s2", info.Config.MaxAge, info.Config.Compression)
	}
	if info.State.Msgs != sent || info.State.Bytes != sent*1083 {
		t.Fatalf("%d messages, %d bytes, want %d and %d: the hand-worked size of a message is wrong", info.State.Msgs, info.State.Bytes, sent, sent*1083)
	}
	blocks, _ := filepath.Glob(filepath.Join(d.dir, "jetstream", "*", "streams", "HB_EVENTS", "msgs", "*.blk"))
	if len(blocks) != 3 {
		t.Fatalf("blocks %v: %d messages of 1,083 bytes in blocks of 32,000 should make 3", blocks, sent)
	}
	slices.Sort(blocks)
	closedOK := func() bool {
		b, err := os.ReadFile(blocks[0])
		return err == nil && len(b) > 14 && string(b[:3]) == "cmp" && b[3] == 1 && bytes.HasPrefix(b[headerLen(b):], []byte("\xff\x06\x00\x00S2sTwO")) && len(b) < 29*1083/2
	}
	deadline := time.Now().Add(waitFor)
	for !closedOK() && time.Now().Before(deadline) {
		time.Sleep(25 * time.Millisecond)
	}
	if !closedOK() {
		b, _ := os.ReadFile(blocks[0])
		t.Fatalf("the first, closed block %s is not S2-compressed: %d bytes, starts % x", blocks[0], len(b), b[:min(len(b), 24)])
	}
	// The block being written stays plain: no "cmp" header.
	open, err := os.ReadFile(blocks[len(blocks)-1])
	if err != nil {
		t.Fatal(err)
	}
	if len(open) > 4 && string(open[:3]) == "cmp" && open[3] == 1 {
		t.Errorf("the open block %s is compressed already", blocks[len(blocks)-1])
	}
}

// headerLen is the length of a compressed block's metadata: "cmp", the algorithm byte, and the
// original size as a uvarint.
func headerLen(b []byte) int {
	n := 4
	for n < len(b) && b[n]&0x80 != 0 {
		n++
	}
	return n + 1
}

// The kill and restart row (HBEP-8, Test plan, Beta; S8): the log runs as a real child, is killed
// with SIGKILL through its pidfd at a random point, and restarts on the same store and the same
// ports with every acknowledged message present, each once. The publisher keeps a list of what the
// log acknowledged; the stream is then read through a new client, never through the publisher.
func TestKilledWithSIGKILLTheLogRestartsWithEveryAcknowledgedMessage(t *testing.T) {
	seed := time.Now().UnixNano()
	if s := os.Getenv("HB_KILL_SEED"); s != "" {
		fmt.Sscan(s, &seed)
	}
	t.Logf("kill timing seed %d (HB_KILL_SEED)", seed)
	rng := rand.New(rand.NewSource(seed))
	d := newLogDir(t, eventlog.Caps{Events: 64 << 20, Usage: 64 << 20})
	acked := map[string]bool{}
	next := 1
	for round := 1; round <= 5; round++ {
		p := startLog(t, d)
		p.ready()
		nc, js := connect(t, d.token)

		var mu sync.Mutex
		var ackedThisRound int
		stopPub := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopPub:
					return
				default:
				}
				id := fmt.Sprintf("k%d-%05d", round, next)
				next++
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := publish(ctx, js, eventSubject, id, eventPayload)
				cancel()
				if err != nil {
					return // the log is gone: this publish is in doubt, and is not counted as acknowledged
				}
				mu.Lock()
				acked[id] = true
				ackedThisRound++
				mu.Unlock()
			}
		}()
		// Wait on state (some acknowledgements), then kill after a random extra time: the randomness
		// is the point, the seed is logged, and nothing here waits for a fixed window to pass.
		deadline := time.Now().Add(waitFor)
		for {
			mu.Lock()
			n := ackedThisRound
			mu.Unlock()
			if n >= 5 || time.Now().After(deadline) {
				break
			}
			time.Sleep(time.Millisecond)
		}
		time.Sleep(time.Duration(rng.Intn(150)) * time.Millisecond)
		p.kill()
		close(stopPub)
		wg.Wait()
		nc.Close()
		mu.Lock()
		if ackedThisRound < 5 {
			t.Fatalf("round %d: only %d acknowledgements before the kill", round, ackedThisRound)
		}
		mu.Unlock()
	}

	// Restart once more on the same store and read everything back through a new client.
	p := startLog(t, d)
	f := p.ready()
	if f["client"] != "127.0.0.1:28500" {
		t.Fatalf("the restarted log is on %s, want the same port 127.0.0.1:28500", f["client"])
	}
	_, js := connect(t, d.token)
	ctx := ctx20(t)
	s, err := js.Stream(ctx, "HB_EVENTS")
	if err != nil {
		t.Fatal(err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq; seq++ {
		m, err := s.GetMsg(ctx, seq)
		if err != nil {
			continue // a sequence the kill left unused, or a deleted one
		}
		seen[m.Header.Get("Nats-Msg-Id")]++
	}
	for id := range acked {
		if seen[id] != 1 {
			t.Errorf("acknowledged message %s is in the stream %d times, want 1", id, seen[id])
		}
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("message %s is in the stream %d times", id, n)
		}
	}
	if len(acked) < 25 {
		t.Fatalf("only %d acknowledged messages over five rounds: the test did not exercise the log", len(acked))
	}
	t.Logf("%d acknowledged messages over 5 kills, all present once; the stream holds %d", len(acked), info.State.Msgs)
}
