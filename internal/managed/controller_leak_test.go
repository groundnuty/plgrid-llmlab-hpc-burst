package managed

import (
	"context"
	"encoding/json"
	"fmt"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
	"k8s.io/client-go/tools/events"
	clocktesting "k8s.io/utils/clock/testing"
)

// The key never leaks: it is in a mode-600 file, it goes out only as the
// Authorization header, and it reaches no error, log line, event or object.
// The double's EchoKey makes its error bodies carry the Authorization header
// it received, so a controller that copied a body anywhere would copy the key.

const secretKey = "example-secret-key-7f3a9c"

// lockedBuffer collects log lines; Start and the test write from different
// goroutines.
type lockedBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *lockedBuffer) line(prefix, args string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.b.WriteString(prefix + args + "\n")
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// rawGet is a request of the test's own to the double, to show what its error
// body holds.
func rawGet(t *testing.T, url, key string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(b)
}

func TestKeyReachesNoErrorLogEventOrObject(t *testing.T) {
	var logs lockedBuffer
	log := funcr.New(logs.line, funcr.Options{Verbosity: 100}) // every level
	rec := events.NewFakeRecorder(100)
	var syncErrs []error
	var seenEvents []string

	drain := func() {
		for {
			select {
			case ev := <-rec.Events:
				seenEvents = append(seenEvents, ev)
			default:
				return
			}
		}
	}
	leakCheck := func(step string, envs ...*env) {
		t.Helper()
		drain()
		if out := logs.String(); strings.Contains(out, secretKey) {
			t.Fatalf("%s: the key is in the log:\n%s", step, out)
		}
		for _, ev := range seenEvents {
			if strings.Contains(ev, secretKey) {
				t.Fatalf("%s: the key is in an event: %s", step, ev)
			}
		}
		for _, err := range syncErrs {
			if err != nil && strings.Contains(err.Error(), secretKey) {
				t.Fatalf("%s: the key is in an error Sync returned: %v", step, err)
			}
		}
		for _, e := range envs {
			b, err := json.Marshal(e.provider())
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(b), secretKey) {
				t.Fatalf("%s: the key is in the Provider object: %s", step, b)
			}
		}
	}

	// Passes by Sync.
	a := newEnv(t, withKey(secretKey), withModels(model("example-a")), withRecorder(rec), withLog(log))
	a.gw.EchoKey(true)
	pass := func(when time.Time) {
		t.Helper()
		_, err := a.trySyncAt(when)
		syncErrs = append(syncErrs, err)
		leakCheck("Sync at "+when.Format("15:04:05"), a)
	}
	pass(at(12, 0, 0)) // a healthy read

	a.gw.RejectKey(secretKey, 401)
	// The control: this gateway's 401 body holds the key.
	if status, body := rawGet(t, a.gw.URL()+"/v1/models", secretKey); status != 401 || !strings.Contains(body, secretKey) {
		t.Fatalf("control: the double answered %d with %q, want a 401 body that holds the key", status, body)
	}
	pass(at(12, 1, 0))
	a.gw.RejectKey(secretKey, 403)
	pass(at(12, 2, 0))

	a.gw.RejectKey(secretKey, 0)
	undo := failReads(a.gw)
	if status, body := rawGet(t, a.gw.URL()+"/v1/models", secretKey); status != 504 || !strings.Contains(body, secretKey) {
		t.Fatalf("control: the double answered %d with %q, want a 504 body that holds the key", status, body)
	}
	pass(at(12, 3, 0)) // a 504 with the key in its body: an unknown error, logged
	undo()

	a.gw.SetModels(model(strings.Repeat("n", 254)))
	pass(at(12, 4, 0)) // a listing the status cannot hold: Sync returns an error

	// Credentials files that hold the key and are refused: their errors and
	// the events they raise must not repeat it.
	writeFile(t, a.path, fmt.Sprintf(`{"key":%q,"baseURL":%q,"extra":true}`, secretKey, a.gw.BaseURL()))
	pass(at(12, 5, 0))
	writeFile(t, a.path, fmt.Sprintf(`{"key":"%s\u00e9","baseURL":%q}`, secretKey, a.gw.BaseURL()))
	pass(at(12, 6, 0))

	// Passes by Start.
	fc := clocktesting.NewFakeClock(at(12, 0, 0))
	b := newEnv(t, withKey(secretKey), withClock(fc), withModels(model("example-a")), withRecorder(rec), withLog(log))
	b.gw.EchoKey(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- b.ctl.Start(ctx) }()
	waitFor(t, "Start's first pass", fc.HasWaiters)
	leakCheck("Start, first pass", a, b)
	next := func(what string) {
		t.Helper()
		fc.Step(time.Minute)
		waitFor(t, what, fc.HasWaiters)
		leakCheck(what, a, b)
	}
	b.gw.RejectKey(secretKey, 401)
	next("Start, a rejected key")
	b.gw.RejectKey(secretKey, 0)
	undo = failReads(b.gw)
	next("Start, a 504 with the key in its body")
	undo()
	b.gw.SetModels(model("")) // an entry with no name: the pass fails, and Start logs it
	next("Start, a pass that fails")
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return")
	}

	// The checks above must have looked at something.
	out := logs.String()
	for _, want := range []string{"catalog read failed", "managed catalog pass failed"} {
		if !strings.Contains(out, want) {
			t.Fatalf("the log has no %q line, so the log check saw no output of that kind:\n%s", want, out)
		}
	}
	joined := strings.Join(seenEvents, "\n")
	for _, want := range []string{"KeyRejected", "FileRefused"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("no event with %q was emitted, so the event check saw nothing of that kind: %q", want, seenEvents)
		}
	}
	failed := 0
	for _, err := range syncErrs {
		if err != nil {
			failed++
		}
	}
	if failed == 0 {
		t.Fatal("no Sync returned an error, so the error check saw nothing")
	}
	// The positive control: the key really went out, as the double logged it.
	for name, e := range map[string]*env{"Sync": a, "Start": b} {
		sent := 0
		for _, r := range e.gw.Requests() {
			if r.Method == http.MethodGet && r.Path == "/v1/models" {
				if got := r.Header.Get("Authorization"); got != "Bearer "+secretKey {
					t.Fatalf("%s: a read sent Authorization %q, want %q", name, got, "Bearer "+secretKey)
				}
				sent++
			}
		}
		if sent == 0 {
			t.Fatalf("%s: the double logged no read", name)
		}
	}
}

// The key cannot reach a command line: the package's non-test files import no
// os/exec.
func TestNonTestFilesImportNoOsExec(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	parsed := map[string]bool{}
	sawNetHTTP := false
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		af, err := parser.ParseFile(fset, f, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		parsed[f] = true
		for _, imp := range af.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if path == "os/exec" {
				t.Errorf("%s imports os/exec", f)
			}
			sawNetHTTP = sawNetHTTP || path == "net/http"
		}
	}
	for _, f := range []string{"controller.go", "client.go", "catalog.go"} {
		if !parsed[f] {
			t.Fatalf("%s was not parsed (parsed %v): the check looked at nothing", f, parsed)
		}
	}
	if !sawNetHTTP {
		t.Fatal("no file imports net/http: the import scan is not reading imports")
	}
}
