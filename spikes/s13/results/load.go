// Package results reads a committed run (results.jsonl) and recomputes its verdicts from the bytes
// it recorded, with package rawjson and the covers of package cases. Nothing is taken from the
// run's own analysis: the record holds only the bytes sent and received.
package results

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s13/cases"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s13/rawjson"
)

// Variant is one line of results.jsonl.
type Variant struct {
	Key      string `json:"key"`
	Case     string `json:"case"`
	Endpoint string `json:"endpoint"`
	Stream   bool   `json:"stream"`
	Status   int    `json:"status"`
	Arrivals int    `json:"arrivals"`
	Worker   string `json:"worker"`
	// WorkerContentType is the Content-Type the worker received.
	WorkerContentType string `json:"worker_content_type"`
	Invalid           string `json:"invalid_for_vllm"`
	Response          string `json:"router_response"`
	Sent              string `json:"sent"`
	Received          string `json:"received"`
	// Identical is recomputed by Load from Sent and Received; the record's own flag must agree.
	Identical bool `json:"identical_bytes"`
	Diffs     []rawjson.Diff
}

// FieldKey names one cover of one case.
type FieldKey struct{ Case, Field, At string }

// Field is a cover's verdict in one variant.
type Field struct {
	Verdict  rawjson.Verdict
	Sent     string
	Received string
}

// Injection is a value present at a worker and absent from the body sent.
type Injection struct{ Endpoint, Path, Value string }

// Transport is one transport check of a run (cmd/s13fields/transport.go). For a chunked check,
// Identical is recomputed from Sent and Received.
type Transport struct {
	Name                string `json:"name"`
	Key                 string `json:"key"`
	Status              int    `json:"status"`
	Arrivals            int    `json:"arrivals"`
	Identical           bool   `json:"identical_bytes"`
	WorkerContentLength int64  `json:"worker_content_length"`
	WorkerContentType   string `json:"worker_content_type"`
	Sent                string `json:"sent"`
	Received            string `json:"received"`
	StreamedThrough     bool   `json:"streamed_through"`
	Held                bool   `json:"held"`
	TimedOut            bool   `json:"timed_out"`
	Error               string `json:"error"`
}

// Run is a loaded run with its recomputed verdicts.
type Run struct {
	Label      string
	Transport  []Transport
	Variants   map[string]*Variant
	Fields     map[FieldKey]map[bool]Field // by stream
	FieldOrder []FieldKey
	Injected   map[Injection]int
	Delivered  map[string]int // variants that reached a worker, by endpoint
}

// Refused and NotDelivered are the verdicts of a variant that reached no worker.
const (
	Refused      rawjson.Verdict = "refused"
	NotDelivered rawjson.Verdict = "not delivered"
)

// Load reads path and recomputes the verdicts.
func Load(path string) (*Run, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	if !sc.Scan() {
		return nil, fmt.Errorf("%s: empty", path)
	}
	var head struct {
		Label     string      `json:"label"`
		Transport []Transport `json:"transport"`
	}
	if err := json.Unmarshal(sc.Bytes(), &head); err != nil {
		return nil, fmt.Errorf("%s: header: %v", path, err)
	}
	for i, t := range head.Transport {
		if t.Name == "chunked request body" {
			got := t.Arrivals > 0 && t.Received == t.Sent
			if got != t.Identical {
				return nil, fmt.Errorf("%s: %s: the record says identical_bytes %v, its bytes say %v", path, t.Key, t.Identical, got)
			}
			head.Transport[i].Identical = got
		}
	}
	run := &Run{Label: head.Label, Transport: head.Transport, Variants: map[string]*Variant{}, Fields: map[FieldKey]map[bool]Field{},
		Injected: map[Injection]int{}, Delivered: map[string]int{}}
	for sc.Scan() {
		v := &Variant{}
		if err := json.Unmarshal(sc.Bytes(), v); err != nil {
			return nil, fmt.Errorf("%s: %v", path, err)
		}
		run.Variants[v.Key] = v
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	for _, c := range cases.All() {
		for _, cv := range c.Variants() {
			v := run.Variants[cv.Key()]
			if v == nil {
				return nil, fmt.Errorf("%s: no record of %s", path, cv.Key())
			}
			if v.Sent != string(cv.Body) {
				return nil, fmt.Errorf("%s: %s was recorded with other bytes than the case sends", path, cv.Key())
			}
			if err := run.add(c, cv, v); err != nil {
				return nil, fmt.Errorf("%s: %s: %v", path, v.Key, err)
			}
		}
	}
	return run, nil
}

func (run *Run) add(c *cases.Case, cv cases.Variant, v *Variant) error {
	sent, err := rawjson.Parse([]byte(v.Sent))
	if err != nil {
		return err
	}
	var recv *rawjson.Node
	recorded := v.Identical
	v.Identical = v.Arrivals > 0 && v.Received == v.Sent
	if recorded != v.Identical {
		return fmt.Errorf("the record says identical_bytes %v, its bytes say %v", recorded, v.Identical)
	}
	if v.Arrivals > 0 {
		run.Delivered[v.Endpoint]++
		if recv, err = rawjson.Parse([]byte(v.Received)); err != nil {
			return fmt.Errorf("received bytes: %v", err)
		}
		v.Diffs = rawjson.Compare(sent, recv)
		for _, d := range v.Diffs {
			if d.Verdict == rawjson.Injected {
				run.Injected[Injection{v.Endpoint, d.Path, d.Received}]++
			}
		}
	}
	for _, cover := range c.Covers {
		k := FieldKey{c.Key(), cover.Field, cover.At}
		if run.Fields[k] == nil {
			run.Fields[k] = map[bool]Field{}
			run.FieldOrder = append(run.FieldOrder, k)
		}
		f := Field{Sent: raw(rawjson.Lookup(sent, cover.At))}
		switch {
		case v.Arrivals == 0 && v.Status >= 400:
			f.Verdict = Refused
			f.Received = fmt.Sprintf("HTTP %d", v.Status)
		case v.Arrivals == 0:
			f.Verdict = NotDelivered
		default:
			vd, hits := rawjson.At(v.Diffs, cover.At)
			f.Verdict = vd
			f.Received = raw(rawjson.Lookup(recv, cover.At))
			for _, h := range hits {
				if h.Verdict == rawjson.Renamed && h.Path == cover.At {
					f.Received = h.To + ": " + h.Received
				}
			}
		}
		run.Fields[k][cv.Stream] = f
	}
	return nil
}

func raw(n *rawjson.Node) string {
	if n == nil {
		return ""
	}
	return string(n.Raw)
}
