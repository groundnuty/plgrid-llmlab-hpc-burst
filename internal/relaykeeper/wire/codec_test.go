package wire_test

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
)

// readLine reads the one message of line.
func readLine(line string) (wire.Envelope, error) {
	return wire.Read(wire.NewReader(strings.NewReader(line)))
}

func TestReplyKind(t *testing.T) {
	requests := map[wire.Kind]wire.Kind{
		wire.KindPing:   wire.KindPong,
		wire.KindSet:    wire.KindReport,
		wire.KindAck:    wire.KindAcked,
		wire.KindStatus: wire.KindStatusReport,
	}
	for req, want := range requests {
		if got, ok := wire.ReplyKind(req); !ok || got != want {
			t.Errorf("ReplyKind(%s) = %q, %v; want %q, true", req, got, ok, want)
		}
	}
	for _, k := range []wire.Kind{wire.KindPong, wire.KindReport, wire.KindAcked, wire.KindStatusReport, wire.KindRefused, "", "bogus"} {
		if got, ok := wire.ReplyKind(k); ok {
			t.Errorf("ReplyKind(%q) = %q, true; %q is not a request", k, got, k)
		}
	}
}

func TestEveryMessageRoundTrips(t *testing.T) {
	for _, m := range everyMessage() {
		t.Run(string(m.kind), func(t *testing.T) {
			var buf bytes.Buffer
			if err := wire.Write(&buf, m.kind, m.body); err != nil {
				t.Fatalf("Write: %v", err)
			}
			line := buf.String()
			if !strings.HasSuffix(line, "\n") || strings.Count(line, "\n") != 1 {
				t.Fatalf("Write made %q; want exactly one line, ended by its newline", line)
			}
			env, err := wire.Read(wire.NewReader(&buf))
			if err != nil {
				t.Fatalf("Read: %v", err)
			}
			if env.V != wire.Version || env.Kind != m.kind {
				t.Errorf("envelope v=%d kind=%q; want v=%d kind=%q", env.V, env.Kind, wire.Version, m.kind)
			}
			got := m.fresh()
			if err := env.Decode(got); err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if g := reflect.ValueOf(got).Elem().Interface(); !reflect.DeepEqual(g, m.body) {
				t.Errorf("round trip gave %+v; want %+v", g, m.body)
			}
		})
	}
}

// The literal bytes are written by hand, so they check the field names and
// their order against the protocol, not against Write's own reading.
func TestWriteMakesTheHandWrittenBytes(t *testing.T) {
	setLine := `{"v":1,"kind":"set","body":{"provider":"provider-a","providerUID":"6f1d2c3b-aaaa-4bbb-8ccc-000000000001",` +
		`"providerCreated":"2026-11-10T12:00:00Z","leaseEpoch":3,"complete":true,"listedAt":"2026-11-10T12:00:01Z",` +
		`"listingAgeMs":1200,"validForMs":40000,` +
		`"ssh":{"hostName":"login.example","user":"example-user","port":22,"identityFile":"/home/example-user/.ssh/id-ed25519","knownHostsFile":"/home/example-user/.ssh/known-hosts"},` +
		`"allocations":[{"uid":"u-1","tag":"hb-job-1-0000abcdef01","job":"4001","allocDir":"/scratch/example/hb/alloc-1","agentPath":"/scratch/example/hb/agent","gen":"0123456789abcdef","endedForGood":false}]}}` + "\n"
	set := validSet()
	set.ProviderCreated = ts.Truncate(time.Second)
	set.ListedAt = set.ProviderCreated.Add(time.Second)
	set.Allocations = set.Allocations[:1]
	set.Allocations[0].UID = "u-1"

	cases := []struct {
		name string
		kind wire.Kind
		body any
		want string
	}{
		{"pong", wire.KindPong, wire.Pong{KeeperEpoch: 7}, `{"v":1,"kind":"pong","body":{"keeperEpoch":7}}` + "\n"},
		{"acked", wire.KindAcked, wire.Acked{Provider: "p1", Decided: true}, `{"v":1,"kind":"acked","body":{"provider":"p1","decided":true}}` + "\n"},
		{"ack", wire.KindAck, validAck(),
			`{"v":1,"kind":"ack","body":{"provider":"provider-a","leaseEpoch":3,"uid":"u-1","job":"4001","channel":"control","epoch":5,"seq":2,"outcome":"marked","reason":""}}` + "\n"},
		{"refused leaves out what it does not use", wire.KindRefused,
			wire.Refused{Reason: wire.RefusedVersion, Supported: []int{1}, Message: "m"},
			`{"v":1,"kind":"refused","body":{"reason":"version","supported":[1],"message":"m"}}` + "\n"},
		{"set", wire.KindSet, set, setLine},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := wire.Write(&buf, c.kind, c.body); err != nil {
				t.Fatal(err)
			}
			if buf.String() != c.want {
				t.Errorf("Write made\n%s\nwant\n%s", buf.String(), c.want)
			}
		})
	}

	// And the other way: the hand-written line reads back as the set.
	env, err := readLine(setLine)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	var got wire.DesiredSet
	if err := env.Decode(&got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !reflect.DeepEqual(got, set) {
		t.Errorf("the hand-written set read as %+v; want %+v", got, set)
	}
	if err := got.Validate(); err != nil {
		t.Errorf("the hand-written set does not validate: %v", err)
	}
}

// A newline inside a string is escaped, so a message is always one line.
func TestANewlineInAValueDoesNotSplitTheLine(t *testing.T) {
	msg := "first\nsecond\r\nthird fourth"
	var buf bytes.Buffer
	if err := wire.Write(&buf, wire.KindRefused, wire.Refused{Reason: wire.RefusedMalformed, Message: msg}); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(buf.String(), "\n"); n != 1 {
		t.Fatalf("%d newlines in %q; want exactly the terminator", n, buf.String())
	}
	env, err := wire.Read(wire.NewReader(&buf))
	if err != nil {
		t.Fatal(err)
	}
	var r wire.Refused
	if err := env.Decode(&r); err != nil {
		t.Fatal(err)
	}
	if r.Message != msg {
		t.Errorf("message read as %q; want %q", r.Message, msg)
	}
}

func TestReadReadsMessagesInOrder(t *testing.T) {
	var buf bytes.Buffer
	for _, m := range everyMessage() {
		if err := wire.Write(&buf, m.kind, m.body); err != nil {
			t.Fatal(err)
		}
	}
	r := wire.NewReader(&buf)
	for _, m := range everyMessage() {
		env, err := wire.Read(r)
		if err != nil || env.Kind != m.kind {
			t.Fatalf("Read = kind %q, %v; want %q", env.Kind, err, m.kind)
		}
	}
	if _, err := wire.Read(r); !errors.Is(err, io.EOF) {
		t.Errorf("Read past the last message: %v; want io.EOF", err)
	}
}

func TestReadAcceptsAnEnvelopeWithNoBody(t *testing.T) {
	env, err := readLine(`{"v":1,"kind":"ping"}` + "\n")
	if err != nil {
		t.Fatal(err)
	}
	if env.Kind != wire.KindPing {
		t.Errorf("kind %q; want ping", env.Kind)
	}
	var p struct{}
	if err := env.Decode(&p); err != nil {
		t.Errorf("Decode of an absent body: %v", err)
	}
}

func TestReadRefusesAMessageWithNoNewline(t *testing.T) {
	if env, err := readLine(`{"v":1,"kind":"ping"}`); err == nil {
		t.Errorf("a line with no newline read as %+v; it is not a whole message", env)
	}
}

func TestReadRefusesWhatIsNotOneEnvelopeOfAKnownKind(t *testing.T) {
	cases := []struct{ name, line string }{
		{"an empty line", "\n"},
		{"not JSON", "not json\n"},
		{"an array", "[]\n"},
		{"a number", "42\n"},
		{"a string", `"x"` + "\n"},
		{"a truncated object", `{"v":1,"kind":"ping"` + "\n"},
		{"an unknown envelope field", `{"v":1,"kind":"ping","extra":true}` + "\n"},
		{"an unknown envelope field with a body", `{"v":1,"kind":"ping","body":{},"trace":"x"}` + "\n"},
		{"an unknown kind", `{"v":1,"kind":"frobnicate"}` + "\n"},
		{"no kind", `{"v":1}` + "\n"},
		{"a second value", `{"v":1,"kind":"ping"} {"v":1,"kind":"ping"}` + "\n"},
		{"a second value without a space", `{"v":1,"kind":"ping"}{"v":1,"kind":"ping"}` + "\n"},
		{"trailing text", `{"v":1,"kind":"ping"} x` + "\n"},
		{"a version that is a string", `{"v":"1","kind":"ping"}` + "\n"},
		{"a kind that is a number", `{"v":1,"kind":7}` + "\n"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env, err := readLine(c.line)
			if !errors.Is(err, wire.ErrMalformed) {
				t.Errorf("Read(%q) = %+v, %v; want ErrMalformed", c.line, env, err)
			}
			var ve *wire.VersionError
			if errors.As(err, &ve) || errors.Is(err, wire.ErrTooLong) {
				t.Errorf("Read(%q): %v is also another error", c.line, err)
			}
		})
	}
}

// strict's own comment: it refuses "anything after the value".
func TestReadRefusesAClosingBracketAfterTheValue(t *testing.T) {
	for _, tail := range []string{"}", "]", "}}", "}]"} {
		line := `{"v":1,"kind":"ping"}` + tail + "\n"
		if env, err := readLine(line); !errors.Is(err, wire.ErrMalformed) {
			t.Errorf("Read(%q) = %+v, %v; want ErrMalformed", line, env, err)
		}
	}
}

func TestReadNamesTheVersionReceived(t *testing.T) {
	for _, v := range []string{"2", "7", "99"} {
		env, err := readLine(`{"v":` + v + `,"kind":"ping"}` + "\n")
		var ve *wire.VersionError
		if !errors.As(err, &ve) {
			t.Fatalf("v=%s: Read error %v; want a *VersionError", v, err)
		}
		if got := strconv.Itoa(ve.Got); got != v {
			t.Errorf("v=%s: VersionError.Got = %d", v, ve.Got)
		}
		if !strings.Contains(err.Error(), v) {
			t.Errorf("v=%s: the error %q does not name it", v, err)
		}
		if errors.Is(err, wire.ErrMalformed) {
			t.Errorf("v=%s: a version error is also ErrMalformed", v)
		}
		if env.Kind != wire.KindPing {
			t.Errorf("v=%s: the envelope returned with the error has kind %q; want ping", v, env.Kind)
		}
	}
}

// padToLine returns a ping line of exactly total bytes before its newline,
// its body a string of padding.
func padToLine(total int) (line string) {
	const prefix, suffix = `{"v":1,"kind":"ping","body":"`, `"}`
	return prefix + strings.Repeat("a", total-len(prefix)-len(suffix)) + suffix
}

func TestReadBoundsTheLineAtMaxMessage(t *testing.T) {
	if env, err := readLine(padToLine(wire.MaxMessage) + "\n"); err != nil || env.Kind != wire.KindPing {
		t.Errorf("a line of exactly MaxMessage bytes: kind %q, %v; want it read", env.Kind, err)
	}
	if env, err := readLine(padToLine(wire.MaxMessage+1) + "\n"); !errors.Is(err, wire.ErrTooLong) {
		t.Errorf("a line of MaxMessage+1 bytes: %+v, %v; want ErrTooLong", env, err)
	}
	if env, err := readLine(strings.Repeat("a", 3*wire.MaxMessage) + "\n"); !errors.Is(err, wire.ErrTooLong) {
		t.Errorf("a line of 3*MaxMessage bytes: %+v, %v; want ErrTooLong", env, err)
	}
	if env, err := readLine(strings.Repeat("a", 3*wire.MaxMessage)); !errors.Is(err, wire.ErrTooLong) {
		t.Errorf("3*MaxMessage bytes with no newline: %+v, %v; want ErrTooLong", env, err)
	}
	if errors.Is(wire.ErrTooLong, wire.ErrMalformed) {
		t.Error("ErrTooLong is also ErrMalformed; a sender must tell them apart")
	}
}

func TestWriteBoundsTheLineAtMaxMessage(t *testing.T) {
	// A string body of n bytes makes a ping line of len(prefix)+n+2+1 bytes.
	const overhead = len(`{"v":1,"kind":"ping","body":"`) + len(`"}`)
	var buf bytes.Buffer
	if err := wire.Write(&buf, wire.KindPing, strings.Repeat("a", wire.MaxMessage-overhead)); err != nil {
		t.Fatalf("Write of a line of exactly MaxMessage bytes: %v", err)
	}
	if got := buf.Len(); got != wire.MaxMessage+1 {
		t.Errorf("Write wrote %d bytes; want MaxMessage and a newline", got)
	}
	if env, err := wire.Read(wire.NewReader(&buf)); err != nil || env.Kind != wire.KindPing {
		t.Errorf("Read of that line: kind %q, %v", env.Kind, err)
	}

	for _, size := range []int{wire.MaxMessage - overhead + 1, 2 * wire.MaxMessage} {
		var out bytes.Buffer
		err := wire.Write(&out, wire.KindPing, strings.Repeat("a", size))
		if !errors.Is(err, wire.ErrTooLong) {
			t.Errorf("Write of a %d-byte body: %v; want ErrTooLong", size, err)
		}
		if out.Len() != 0 {
			t.Errorf("Write of a %d-byte body wrote %d bytes before it refused", size, out.Len())
		}
	}
}

func TestWriteReportsABodyItCannotEncode(t *testing.T) {
	var buf bytes.Buffer
	if err := wire.Write(&buf, wire.KindPing, make(chan int)); err == nil {
		t.Error("Write of a channel body succeeded")
	}
	if buf.Len() != 0 {
		t.Errorf("Write wrote %q for a body it could not encode", buf.String())
	}
}

func TestDecodeRefusesWhatIsNotTheBodyOfItsType(t *testing.T) {
	cases := []struct {
		name string
		body string
		into any
	}{
		{"an unknown body field", `{"keeperEpoch":1,"extra":2}`, new(wire.Pong)},
		{"an unknown field in a nested object", `{"ssh":{"hostName":"login.example","extra":1}}`, new(wire.DesiredSet)},
		{"an unknown field in a list element", `{"allocations":[{"uid":"u-1","extra":1}]}`, new(wire.DesiredSet)},
		{"a value of the wrong type", `{"leaseEpoch":"three"}`, new(wire.DesiredSet)},
		{"a fraction for an integer", `{"leaseEpoch":1.5}`, new(wire.DesiredSet)},
		{"a time that is not a time", `{"providerCreated":"yesterday"}`, new(wire.DesiredSet)},
		{"a second value", `{"keeperEpoch":1} {"keeperEpoch":2}`, new(wire.Pong)},
		{"trailing text", `{"keeperEpoch":1} x`, new(wire.Pong)},
		{"an array for an object", `[]`, new(wire.Pong)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := wire.Envelope{V: wire.Version, Kind: wire.KindPong, Body: []byte(c.body)}
			if err := env.Decode(c.into); !errors.Is(err, wire.ErrMalformed) {
				t.Errorf("Decode(%s) = %v; want ErrMalformed", c.body, err)
			}
		})
	}
}

// Decode's refusals within a whole line: the unknown field is found after the
// envelope was read.
func TestAnUnknownBodyFieldIsRefusedAfterRead(t *testing.T) {
	env, err := readLine(`{"v":1,"kind":"pong","body":{"keeperEpoch":1,"extra":2}}` + "\n")
	if err != nil {
		t.Fatalf("Read: %v (the envelope itself is well formed)", err)
	}
	var p wire.Pong
	if err := env.Decode(&p); !errors.Is(err, wire.ErrMalformed) {
		t.Errorf("Decode = %v; want ErrMalformed", err)
	}
}

// Decode of a closing bracket after the body, like Read's.
func TestDecodeRefusesAClosingBracketAfterTheBody(t *testing.T) {
	env := wire.Envelope{V: wire.Version, Kind: wire.KindPong, Body: []byte(`{"keeperEpoch":1}}`)}
	var p wire.Pong
	if err := env.Decode(&p); !errors.Is(err, wire.ErrMalformed) {
		t.Errorf("Decode = %v; want ErrMalformed", err)
	}
}

// An envelope that names another version is a *VersionError whatever else it
// holds, a field this version does not know included, so its sender is told
// which versions are served. One that names no version is malformed, not a
// version error.
func TestReadTellsAnotherVersionWhateverItsEnvelopeHolds(t *testing.T) {
	for _, line := range []string{
		`{"v":2,"kind":"ping","trace":"x"}` + "\n",
		`{"v":2,"kind":"frobnicate","body":{"new":1}}` + "\n",
	} {
		var ve *wire.VersionError
		if _, err := readLine(line); !errors.As(err, &ve) || ve.Got != 2 {
			t.Errorf("Read(%q): %v, want a *VersionError naming 2", line, err)
		}
	}
	for _, line := range []string{"null\n", "{}\n", `{"kind":"ping"}` + "\n"} {
		_, err := readLine(line)
		var ve *wire.VersionError
		if !errors.Is(err, wire.ErrMalformed) || errors.As(err, &ve) {
			t.Errorf("Read(%q): %v, want ErrMalformed and no version error", line, err)
		}
	}
}
