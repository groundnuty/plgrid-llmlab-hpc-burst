package e2e

// Reading the API server's audit log FILES: the plain file being written and the rotated, gzipped ones beside it
// (the API server's log writer, lumberjack, names a rotated file audit-<timestamp>.log and gzips it in the
// background, leaving the plain one in place until the gzip file is whole). A reader sees the log the way a person
// would, whatever moment the rotation is at.

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// auditEvent is the part of an audit.k8s.io/v1 event these tests read.
type auditEvent struct {
	AuditID    string `json:"auditID"`
	Level      string `json:"level"`
	Stage      string `json:"stage"`
	Verb       string `json:"verb"`
	RequestURI string `json:"requestURI"`
	UserAgent  string `json:"userAgent"`
	User       struct {
		Username string `json:"username"`
	} `json:"user"`
	ObjectRef struct {
		APIGroup    string `json:"apiGroup"`
		Resource    string `json:"resource"`
		Subresource string `json:"subresource"`
		Namespace   string `json:"namespace"`
		Name        string `json:"name"`
		UID         string `json:"uid"`
	} `json:"objectRef"`
	ResponseStatus struct {
		Code int `json:"code"`
	} `json:"responseStatus"`
	RequestObject            json.RawMessage `json:"requestObject"`
	ResponseObject           json.RawMessage `json:"responseObject"`
	RequestReceivedTimestamp time.Time       `json:"requestReceivedTimestamp"`

	// size is the line's length in bytes, and file the log file it came from.
	size int
	file string
}

func (e auditEvent) String() string {
	r := e.ObjectRef.Resource
	if e.ObjectRef.Subresource != "" {
		r += "/" + e.ObjectRef.Subresource
	}
	return fmt.Sprintf("%s %s %s/%s by %s -> %d at %s", e.Level, e.Verb, r, e.ObjectRef.Name, e.User.Username, e.ResponseStatus.Code, e.Stage)
}

// operation and approval decode the request or the response body of an Operation or an Approval event.
func (e auditEvent) requestOperation(t testing.TB) *v1alpha1.Operation {
	t.Helper()
	var op v1alpha1.Operation
	if len(e.RequestObject) == 0 || json.Unmarshal(e.RequestObject, &op) != nil {
		t.Fatalf("event %s has no request body that is an Operation (%q)", e, e.RequestObject)
	}
	return &op
}

func (e auditEvent) requestApproval(t testing.TB) *v1alpha1.Approval {
	t.Helper()
	var ap v1alpha1.Approval
	if len(e.RequestObject) == 0 || json.Unmarshal(e.RequestObject, &ap) != nil {
		t.Fatalf("event %s has no request body that is an Approval (%q)", e, e.RequestObject)
	}
	return &ap
}

// logFile is one file of the log: its name, whether it is a gzip file, and its text, decompressed.
type logFile struct {
	name string
	gz   bool
	text []byte
	mode fs.FileMode
}

// logSet is the whole log at one moment: every file in the order the API server wrote them, oldest first, and the
// events of all of them.
type logSet struct {
	files  []logFile
	events []auditEvent
}

// backupBase is the name of a rotated file without the .gz suffix, and whether the file is a rotated one at all.
func backupBase(name string) (string, bool) {
	base := strings.TrimSuffix(name, ".gz")
	return base, strings.HasPrefix(base, "audit-") && strings.HasSuffix(base, ".log")
}

// reader reads a log directory and remembers the text of the gzip files that are whole, which never change.
type reader struct {
	logPath string
	gunzip  map[string][]byte // by name and size of a finished .gz file
}

func newReader(logPath string) *reader { return &reader{logPath: logPath, gunzip: map[string][]byte{}} }

// read lists the directory once and reads every file. A file that vanishes between the listing and the read (the
// active file renamed by a rotation, a plain file removed once its gzip is whole) or a gzip file that is not whole
// yet makes it start again, a bounded number of times. The active file's last line may be half written: it is left
// out.
func (r *reader) read(t testing.TB) *logSet {
	t.Helper()
	var last error
	for attempt := 0; attempt < 50; attempt++ {
		set, err := r.readOnce()
		if err == nil {
			return set
		}
		last = err
		time.Sleep(40 * time.Millisecond)
	}
	t.Fatalf("the audit log could not be read whole in 50 tries: %v", last)
	return nil
}

func (r *reader) readOnce() (*logSet, error) {
	dir := filepath.Dir(r.logPath)
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	plain := map[string]bool{}
	var names []string
	for _, e := range ents {
		if base, ok := backupBase(e.Name()); ok && !strings.HasSuffix(e.Name(), ".gz") {
			plain[base] = true
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)
	set := &logSet{}
	var rotated, active []logFile
	for _, name := range names {
		base, isBackup := backupBase(name)
		isGz := strings.HasSuffix(name, ".gz")
		switch {
		case name == filepath.Base(r.logPath):
		case isBackup && isGz && plain[base]:
			continue // being compressed: the plain file is whole, and the gzip file is not
		case isBackup:
		default:
			continue
		}
		path := filepath.Join(dir, name)
		fi, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		f := logFile{name: name, gz: isGz, mode: fi.Mode()}
		switch {
		case isGz:
			key := fmt.Sprintf("%s/%d", name, fi.Size())
			text, ok := r.gunzip[key]
			if !ok {
				if text, err = gunzipFile(path); err != nil {
					return nil, err
				}
				r.gunzip[key] = text
			}
			f.text = text
		default:
			if f.text, err = os.ReadFile(path); err != nil {
				return nil, err
			}
		}
		if name == filepath.Base(r.logPath) {
			f.text = f.text[:bytes.LastIndexByte(f.text, '\n')+1]
			active = append(active, f)
		} else {
			rotated = append(rotated, f)
		}
	}
	set.files = append(rotated, active...)
	for _, f := range set.files {
		evs, err := parseEvents(f)
		if err != nil {
			return nil, err
		}
		set.events = append(set.events, evs...)
	}
	return set, nil
}

// gunzipFile reads a gzip file whole: a file that is cut off fails here (the checksum is read to the end).
func gunzipFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", filepath.Base(path), err)
	}
	b, err := io.ReadAll(zr)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", filepath.Base(path), err)
	}
	return b, nil
}

func parseEvents(f logFile) ([]auditEvent, error) {
	var out []auditEvent
	for n, line := range bytes.Split(bytes.TrimSuffix(f.text, []byte("\n")), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var e auditEvent
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, fmt.Errorf("%s line %d is not a JSON event: %w", f.name, n+1, err)
		}
		e.size, e.file = len(line)+1, f.name
		out = append(out, e)
	}
	return out, nil
}

// rotatedNames are the names of the rotated files of the set, without a .gz suffix.
func (s *logSet) rotatedNames() []string {
	var out []string
	for _, f := range s.files {
		if base, ok := backupBase(f.name); ok {
			out = append(out, base)
		}
	}
	return out
}

// find returns the events of verb on the resource's object name by user, in the order logged. An empty
// subresource matches the main resource only.
func (s *logSet) find(verb, resource, subresource, name, user string) []auditEvent {
	var out []auditEvent
	for _, e := range s.events {
		if e.Verb == verb && e.ObjectRef.Resource == resource && e.ObjectRef.Subresource == subresource &&
			(name == "" || e.ObjectRef.Name == name) && (user == "" || e.User.Username == user) {
			out = append(out, e)
		}
	}
	return out
}

// tokenChar reports whether b can be part of a longer token: a letter, a digit, and the characters of base64url and
// of names. An equals sign is not one: it stands before a value in a query string, and after an unpadded base64 form
// as its padding, and a value found there is a leak all the same.
func tokenChar(b byte) bool {
	return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '+' || b == '/' || b == '_' || b == '-'
}

// containsToken reports whether text holds token as a WHOLE token: not followed or preceded by a character that
// would make it part of a longer one. Random strings (hashes, UIDs, temporary names) contain any short string now and
// then; a leak is the value standing alone, between quotes, commas or spaces (IMPL-COMMON, 11 Nov 2026).
func containsToken(text []byte, token string) bool {
	if token == "" {
		return false
	}
	for from := 0; ; {
		i := bytes.Index(text[from:], []byte(token))
		if i < 0 {
			return false
		}
		i += from
		end := i + len(token)
		if (i == 0 || !tokenChar(text[i-1])) && (end == len(text) || !tokenChar(text[end])) {
			return true
		}
		from = i + 1
	}
}

// anyFileHasToken returns the names of the files, plain or gunzipped, that hold token.
func (s *logSet) filesWithToken(token string) []string {
	var out []string
	for _, f := range s.files {
		if containsToken(f.text, token) {
			out = append(out, f.name)
		}
	}
	return out
}

// waitLog reads the log until ok holds, for a bounded time. On a timeout it fails with the events' summary and
// every request a controller identity made that the API server refused (403): a role that lacks a verb shows there.
func (w *world) waitLog(r *reader, what string, ok func(*logSet) bool) *logSet {
	t := w.t
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		set := r.read(t)
		if ok(set) {
			return set
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: not in the audit log within 90 s (%d events in %d files)\n%s", what, len(set.events), len(set.files), refusals(set))
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// refusals lists, for a failing wait, the refused requests of the controllers' and agents' identities.
func refusals(s *logSet) string {
	var b strings.Builder
	n := 0
	for _, e := range s.events {
		if e.ResponseStatus.Code == 403 && strings.HasPrefix(e.User.Username, "system:serviceaccount:") {
			if n++; n <= 20 {
				fmt.Fprintf(&b, "  refused: %s\n", e)
			}
		}
	}
	if n == 0 {
		return "  no request of a ServiceAccount was refused"
	}
	return b.String()
}

var errNotAGzip = errors.New("not a gzip file")

// isGzip reports whether the file starts with gzip's magic bytes, read from the file itself.
func isGzip(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	var magic [2]byte
	if _, err := io.ReadFull(f, magic[:]); err != nil {
		return err
	}
	if magic != [2]byte{0x1f, 0x8b} {
		return fmt.Errorf("%s starts with % x: %w", filepath.Base(path), magic, errNotAGzip)
	}
	return nil
}
