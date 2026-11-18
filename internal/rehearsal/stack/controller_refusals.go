package stack

// A run under the controllers' own identities (HBEP-10, Audit; issue #660) is proof of their roles only if a request an
// identity was refused fails it: a controller that is answered 403 logs the error, joins it to its pass's and carries on, so a
// stack that has finished its scenario may have been refused a verb on the way. The API server's audit file is the one account
// of who asked what and how it was answered, and it is the API server's own, not the client's. ControllerAudit reads it.
//
// "No refusal" proves nothing from a log that holds no request of the identity: a stack whose controller never reached the
// API server, or whose audit file was never written, has no refusal either. So the check passes only if the log is there, is
// not empty, and holds at least one request each identity had answered 2xx, besides no 403.

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
)

// controllerUserPrefix is the user-name prefix of the controllers' identities: the ServiceAccounts of hpcburst-system.
const controllerUserPrefix = "system:serviceaccount:hpcburst-system:"

// ControllerAuditReport is what the stack's audit log holds of the controller identities.
type ControllerAuditReport struct {
	// Refusals has one line for each distinct request of a controller identity that the API server answered 403, sorted:
	// "<n> x <identity's name> <verb> <resource>[/<subresource>] (<group>)". It names no path, host or object.
	Refusals []string
	// Answered counts the requests of each controller identity (by its name) that the API server answered 2xx.
	Answered map[string]int
	// Seen counts the requests of controller identities, whatever the answer.
	Seen int
}

// ControllerAudit reads the stack's audit log under state (state/logs/audit.log, and the rotated files beside it, whether gzip
// or plain). A file that is not there is an error, since the log is the run's only evidence, and so is a line that is not a
// JSON object, not skipped. A log with no event at all, or none of a controller identity, is not an error here: Problems
// reports it, since what is missing depends on which identities were to run.
func ControllerAudit(state string) (ControllerAuditReport, error) {
	logs := filepath.Join(state, "logs")
	if _, err := os.Stat(filepath.Join(logs, "audit.log")); err != nil {
		return ControllerAuditReport{}, fmt.Errorf("the stack's audit log: %w", err)
	}
	files, err := filepath.Glob(filepath.Join(logs, "audit*.log*"))
	if err != nil {
		return ControllerAuditReport{}, err
	}
	sort.Strings(files)
	counts := map[string]int{}
	rep := ControllerAuditReport{Answered: map[string]int{}}
	for _, f := range files {
		if err := countAudit(f, counts, &rep); err != nil {
			return ControllerAuditReport{}, fmt.Errorf("the audit log %s: %w", filepath.Base(f), err)
		}
	}
	for k, n := range counts {
		rep.Refusals = append(rep.Refusals, fmt.Sprintf("%d x %s", n, k))
	}
	sort.Strings(rep.Refusals)
	return rep, nil
}

// Problems is what makes the log fail a run, one line each, with no path, host or object in any: a refusal, and each of want,
// the identities that were to run, that has no request answered 2xx (which an empty log gives for all of them).
func (r ControllerAuditReport) Problems(want []string) []string {
	var out []string
	for _, name := range want {
		if r.Answered[name] == 0 {
			out = append(out, fmt.Sprintf("the audit log holds no request that %s%s had answered 2xx (%d requests of controller identities in all): the check read nothing of it", controllerUserPrefix, name, r.Seen))
		}
	}
	for _, line := range r.Refusals {
		out = append(out, "a controller identity was REFUSED by the API server (403): its role lacks what the code calls: "+line)
	}
	return out
}

// Counts is the log's tally for want, "name: n answered 2xx", in the order given, and the number of 403s: all a run may print.
func (r ControllerAuditReport) Counts(want []string) string {
	parts := make([]string, 0, len(want)+1)
	for _, name := range want {
		parts = append(parts, fmt.Sprintf("%s: %d answered 2xx", name, r.Answered[name]))
	}
	refused := 0
	for _, line := range r.Refusals {
		var n int
		fmt.Sscanf(line, "%d x ", &n)
		refused += n
	}
	parts = append(parts, fmt.Sprintf("%d refused 403", refused))
	return strings.Join(parts, ", ")
}

// countAudit adds the file's 403s of controller identities to counts, and its requests of controller identities to rep.
func countAudit(path string, counts map[string]int, rep *ControllerAuditReport) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	var r io.Reader = f
	if strings.HasSuffix(path, ".gz") {
		zr, err := gzip.NewReader(f)
		if err != nil {
			return err
		}
		defer zr.Close()
		r = zr
	}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	for n := 1; sc.Scan(); n++ {
		var e struct {
			Verb string `json:"verb"`
			User struct {
				Username string `json:"username"`
			} `json:"user"`
			ObjectRef struct {
				APIGroup    string `json:"apiGroup"`
				Resource    string `json:"resource"`
				Subresource string `json:"subresource"`
			} `json:"objectRef"`
			ResponseStatus struct {
				Code int `json:"code"`
			} `json:"responseStatus"`
		}
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			// The API server may be writing the last line while the stack runs: a cut-short last line is not a refusal.
			if !sc.Scan() {
				return nil
			}
			return fmt.Errorf("line %d is not an audit event: %w", n, err)
		}
		if !strings.HasPrefix(e.User.Username, controllerUserPrefix) {
			continue
		}
		name := strings.TrimPrefix(e.User.Username, controllerUserPrefix)
		rep.Seen++
		switch code := e.ResponseStatus.Code; {
		case code >= 200 && code < 300:
			rep.Answered[name]++
		case code == 403:
			res := e.ObjectRef.Resource
			if e.ObjectRef.Subresource != "" {
				res += "/" + e.ObjectRef.Subresource
			}
			counts[fmt.Sprintf("%s %s %s (%s)", name, e.Verb, res, e.ObjectRef.APIGroup)]++
		}
	}
	return sc.Err()
}

// AssertControllerAudit fails the test for every request of a controller identity that the stack's API server refused (403),
// and for each of the controller identities that has no request answered 2xx in the log, or for a log that is missing or
// empty (the check would pass on nothing). It prints the tally and, for each refusal, the count and its verb and resource,
// and nothing else: the audit log holds paths and logins, and the output may be posted. It reads the log while the stack is
// still up. Every stack runs all of launcher.ControllerIdentityNames(): hpcburst up starts the controller (two identities)
// and the front.
func (h *Host) AssertControllerAudit(t T) {
	t.Helper()
	want := launcher.ControllerIdentityNames()
	rep, err := ControllerAudit(h.State)
	if err != nil {
		t.Errorf("read the stack's audit log for the controller identities' requests: %v", err)
		return
	}
	t.Logf("the controller identities in the API server's audit log: %s", rep.Counts(want))
	for _, p := range rep.Problems(want) {
		t.Errorf("%s", p)
	}
}
