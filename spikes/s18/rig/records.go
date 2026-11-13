package rig

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

// EngineEvent is one line of an engine double's log (HB_ENGINE_DOUBLE_LOG), the fields the spike reads.
// It mirrors internal/agent/enginedouble's LogEvent: "start" (PID), "admitted" (Tail: the request has a
// slot; written at once) and "request" (Tail, Outcome: written when the request ends).
type EngineEvent struct {
	Event   string `json:"event"`
	PID     int    `json:"pid"`
	Tail    string `json:"tail"`
	Outcome string `json:"outcome"`
	Stream  bool   `json:"stream"`
}

// ReadEngineLog reads an engine log; a missing file is no events.
func ReadEngineLog(path string) ([]EngineEvent, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []EngineEvent
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var ev EngineEvent
		if json.Unmarshal(sc.Bytes(), &ev) == nil {
			out = append(out, ev)
		}
	}
	return out, sc.Err()
}

// logWatch answers whether an engine log holds an admitted record of an ID, reading only what was
// appended since its last look.
type logWatch struct {
	path string

	mu       sync.Mutex
	off      int64
	admitted map[string]bool
}

func newLogWatch(path string) *logWatch { return &logWatch{path: path, admitted: map[string]bool{}} }

// Admitted reports whether the log holds id's admitted record.
func (w *logWatch) Admitted(id string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.admitted[id] {
		return true
	}
	f, err := os.Open(w.path)
	if err != nil {
		return false
	}
	defer f.Close()
	if _, err := f.Seek(w.off, 0); err != nil {
		return false
	}
	r := bufio.NewReader(f)
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			break // a partial last line is read again next time
		}
		w.off += int64(len(line))
		var ev EngineEvent
		if json.Unmarshal(line, &ev) == nil && ev.Event == "admitted" {
			if got := IDIn([]byte(ev.Tail)); got != "" {
				w.admitted[got] = true
			}
		}
	}
	return w.admitted[id]
}
