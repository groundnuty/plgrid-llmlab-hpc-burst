package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s13/cases"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s13/rawjson"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s13/recorder"
)

// Verdicts beyond rawjson's, for a body that never reached a worker.
const (
	Refused      rawjson.Verdict = "refused"       // the router answered an error and forwarded nothing
	NotDelivered rawjson.Verdict = "not delivered" // no error, and still nothing reached a worker
	Unparseable  rawjson.Verdict = "unparseable"   // what reached the worker is not JSON
)

// Run is one pass of every case through one path.
type Run struct {
	Label      string   `json:"label"`
	Router     string   `json:"router,omitempty"`
	Policy     string   `json:"policy,omitempty"`
	Started    string   `json:"started"`
	Listeners  []string `json:"listeners,omitempty"`
	RouterExit string   `json:"router_exit,omitempty"`
	Probes     []string `json:"worker_probes"`
	Results    []Result `json:"results"`
}

// Result is one variant: what was sent, what arrived, and each covered field's verdict.
type Result struct {
	Key               string `json:"key"`
	Case              string `json:"case"`
	Endpoint          string `json:"endpoint"`
	Stream            bool   `json:"stream"`
	Note              string `json:"note,omitempty"`
	Invalid           string `json:"invalid_for_vllm,omitempty"`
	Status            int    `json:"status"`
	StreamDone        bool   `json:"stream_done,omitempty"`
	Response          string `json:"router_response,omitempty"`
	Error             string `json:"error,omitempty"`
	Arrivals          int    `json:"arrivals"`
	Worker            string `json:"worker,omitempty"`
	WorkerContentType string `json:"worker_content_type,omitempty"`
	SessionHeader     bool   `json:"session_header_arrived"`
	Identical         bool   `json:"identical_bytes"`
	// Diffs and Fields are recomputed from Sent and Received by rawjson.Compare, so the record
	// keeps only the bytes; fields.md holds the tables.
	Diffs    []rawjson.Diff `json:"-"`
	Fields   []FieldResult  `json:"-"`
	Sent     string         `json:"sent"`
	Received string         `json:"received,omitempty"`
}

// FieldResult is one cover's outcome in one variant.
type FieldResult struct {
	Field    string          `json:"field"`
	At       string          `json:"at"`
	Verdict  rawjson.Verdict `json:"verdict"`
	Sent     string          `json:"sent"`
	Received string          `json:"received,omitempty"`
}

func (r *Result) compare(v cases.Variant) {
	sent, err := rawjson.Parse(v.Body)
	if err != nil {
		r.Error = "the case is not JSON: " + err.Error()
		return
	}
	verdictAll := func(vd rawjson.Verdict) {
		for _, cv := range v.Case.Covers {
			r.Fields = append(r.Fields, FieldResult{Field: cv.Field, At: cv.At, Verdict: vd, Sent: raw(rawjson.Lookup(sent, cv.At))})
		}
	}
	if r.Arrivals == 0 {
		if r.Status >= 400 || r.Error != "" {
			verdictAll(Refused)
		} else {
			verdictAll(NotDelivered)
		}
		return
	}
	r.Identical = r.Received == string(v.Body)
	recv, err := rawjson.Parse([]byte(r.Received))
	if err != nil {
		verdictAll(Unparseable)
		return
	}
	r.Diffs = rawjson.Compare(sent, recv)
	for _, cv := range v.Case.Covers {
		vd, hits := rawjson.At(r.Diffs, cv.At)
		got := rawjson.Lookup(recv, cv.At)
		for _, h := range hits {
			if h.Verdict == rawjson.Renamed && h.Path == cv.At {
				got = rawjson.Lookup(recv, h.To)
			}
		}
		fr := FieldResult{Field: cv.Field, At: cv.At, Verdict: vd, Sent: raw(rawjson.Lookup(sent, cv.At)), Received: raw(got)}
		if vd == rawjson.Renamed {
			fr.Received = hits[0].To + ": " + fr.Received
		}
		r.Fields = append(r.Fields, fr)
	}
}

func raw(n *rawjson.Node) string {
	if n == nil {
		return ""
	}
	return string(n.Raw)
}

// probes lists the requests a router made to its workers other than the cases, with counts.
func probes(rec *recorder.Recorder) []string {
	count := map[string]int{}
	for _, a := range rec.All() {
		if a.Case == "" || a.Case == "warm-up" {
			count[a.Method+" "+a.Path+" (case "+orNone(a.Case)+")"]++
		}
	}
	var out []string
	for k, n := range count {
		out = append(out, fmt.Sprintf("%s x%d", k, n))
	}
	sort.Strings(out)
	return out
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

type tally struct{ variants, identical, noDiff, reencodedOnly, lossy, refused int }

func count(run *Run) tally {
	var t tally
	for _, r := range run.Results {
		t.variants++
		switch {
		case r.Arrivals == 0:
			t.refused++
		case r.Identical:
			t.identical++
		case len(r.Diffs) == 0:
			t.noDiff++
		default:
			lossy := false
			for _, d := range r.Diffs {
				if d.Verdict != rawjson.Reencoded && d.Verdict != rawjson.KeyReencoded {
					lossy = true
				}
			}
			if lossy {
				t.lossy++
			} else {
				t.reencodedOnly++
			}
		}
	}
	return t
}

func summaryLine(run *Run) string {
	t := count(run)
	return fmt.Sprintf("%s: %d variants; %d arrived as their exact bytes; %d differ only in member order and insignificant whitespace; "+
		"%d differ only in re-encoded values or keys; %d lost or changed a value; %d did not reach a worker",
		run.Label, t.variants, t.identical, t.noDiff, t.reencodedOnly, t.lossy, t.refused)
}

// writeResults writes results.jsonl, the run's header on its first line and then one line per
// variant with the bytes sent and received (received is left out when it is the bytes sent), and
// fields.md, the tables.
func writeResults(dir string, run *Run) error {
	head := *run
	head.Results = nil
	var buf []byte
	line, err := json.Marshal(head)
	if err != nil {
		return err
	}
	buf = append(append(buf, line...), '\n')
	for _, r := range run.Results {
		if r.Identical {
			r.Received = "" // the same bytes as Sent
		}
		line, err := json.Marshal(r)
		if err != nil {
			return err
		}
		buf = append(append(buf, line...), '\n')
	}
	if err := os.WriteFile(filepath.Join(dir, "results.jsonl"), buf, 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "fields.md"), []byte(markdown(run)), 0o644)
}

func cell(s string, n int) string {
	if s == "" {
		return "—"
	}
	s = strings.ReplaceAll(truncate(s, n), "|", `\|`)
	return "`" + strings.ReplaceAll(s, "`", "'") + "`"
}

func markdown(run *Run) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# %s\n\n", run.Label)
	if run.Router != "" {
		if run.Policy != "" {
			fmt.Fprintf(&b, "- Router: `%s`, `--policy %s`, two workers.\n", run.Router, run.Policy)
		} else {
			fmt.Fprintf(&b, "- Router: `%s`, two workers.\n", run.Router)
		}
		fmt.Fprintf(&b, "- Router listeners: %s. Stop: %s.\n", strings.Join(run.Listeners, ", "), run.RouterExit)
	}
	fmt.Fprintf(&b, "- Started %s.\n- %s.\n", run.Started, summaryLine(run))
	if run.Router != "" {
		fmt.Fprintf(&b, "- Requests the router made to the workers besides the cases: %s.\n", strings.Join(run.Probes, "; "))
	}
	b.WriteString("\n")

	type key struct{ c, field, at string }
	var order []key
	rows := map[key]map[bool]FieldResult{}
	invalid := map[string]bool{}
	for _, r := range run.Results {
		if r.Invalid != "" {
			invalid[r.Case] = true
		}
		for _, f := range r.Fields {
			k := key{r.Case, f.Field, f.At}
			if rows[k] == nil {
				rows[k] = map[bool]FieldResult{}
				order = append(order, k)
			}
			rows[k][r.Stream] = f
		}
	}
	b.WriteString("## Fields\n\nOne row per field a case covers; `plain` and `stream` are the verdicts without and with `\"stream\":true`.\n\n")
	b.WriteString("| case | field | at | sent | received | plain | stream |\n|---|---|---|---|---|---|---|\n")
	for _, k := range order {
		p, hasP := rows[k][false]
		s, hasS := rows[k][true]
		show := p
		if !hasP {
			show = s
		}
		pv, sv := "—", "—"
		if hasP {
			pv = string(p.Verdict)
		}
		if hasS {
			sv = string(s.Verdict)
		}
		name := k.c
		if invalid[k.c] {
			name += " (invalid for vLLM)"
		}
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %s |\n", name, k.field, cell(k.at, 60), cell(show.Sent, 70), cell(show.Received, 70), pv, sv)
	}

	b.WriteString("\n## Injected values\n\nValues present at the worker and absent from the body sent, by path and value.\n\n| path | value | variants | first case |\n|---|---|---|---|\n")
	type inj struct{ path, value string }
	injCount := map[inj]int{}
	injFirst := map[inj]string{}
	var injOrder []inj
	for _, r := range run.Results {
		for _, d := range r.Diffs {
			if d.Verdict != rawjson.Injected {
				continue
			}
			k := inj{d.Path, d.Received}
			if injCount[k] == 0 {
				injOrder = append(injOrder, k)
				injFirst[k] = r.Key
			}
			injCount[k]++
		}
	}
	for _, k := range injOrder {
		fmt.Fprintf(&b, "| %s | %s | %d | %s |\n", cell(k.path, 60), cell(k.value, 60), injCount[k], injFirst[k])
	}

	b.WriteString("\n## Other differences\n\nDiffs outside every covered field and not injected.\n\n| variant | path | verdict | sent | received |\n|---|---|---|---|---|\n")
	for _, r := range run.Results {
		for _, d := range r.Diffs {
			if d.Verdict == rawjson.Injected {
				continue
			}
			covered := false
			for _, f := range r.Fields {
				if v, _ := rawjson.At([]rawjson.Diff{d}, f.At); v != rawjson.Kept {
					covered = true
				}
			}
			if !covered {
				fmt.Fprintf(&b, "| %s | %s | %s | %s | %s |\n", r.Key, cell(d.Path, 60), d.Verdict, cell(d.Sent, 60), cell(d.Received, 60))
			}
		}
	}

	b.WriteString("\n## Not delivered\n\n| variant | status | the router's reply |\n|---|---|---|\n")
	for _, r := range run.Results {
		if r.Arrivals == 0 {
			fmt.Fprintf(&b, "| %s | %d | %s |\n", r.Key, r.Status, cell(r.Response+r.Error, 200))
		}
	}
	return b.String()
}
