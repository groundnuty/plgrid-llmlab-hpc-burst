package rig

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
)

// Reanalyze computes a cell's summary again from the records its run wrote in dir (attempts.jsonl,
// layer.jsonl, exits.jsonl, engines.json, and summary.json for its configuration and restarts), so a
// change to the analysis is applied to the same measurements, never to a new run.
func Reanalyze(dir string) (Summary, error) {
	var old Summary
	if err := readJSON(filepath.Join(dir, "summary.json"), &old); err != nil {
		return Summary{}, err
	}
	results, err := readJSONL[Result](filepath.Join(dir, "attempts.jsonl"))
	if err != nil {
		return Summary{}, err
	}
	attempts, err := readJSONL[Attempt](filepath.Join(dir, "layer.jsonl"))
	if err != nil {
		return Summary{}, err
	}
	exits, err := readJSONL[Exit](filepath.Join(dir, "exits.jsonl"))
	if err != nil {
		return Summary{}, err
	}
	var begins map[string][]Begin
	if err := readJSON(filepath.Join(dir, "engines.json"), &begins); err != nil {
		return Summary{}, err
	}
	s := Analyze(old.Cell, results, begins, exits, attempts, old.Restarts)
	bases := map[string][]Result{}
	var order []string
	for _, r := range results {
		if r.Marker != MarkPoison {
			continue
		}
		if _, ok := bases[r.Base]; !ok {
			order = append(order, r.Base)
		}
		bases[r.Base] = append(bases[r.Base], r)
	}
	for _, b := range order {
		s.Poison = append(s.Poison, PoisonCost(b, bases[b], begins, attempts, exits))
	}
	return s, nil
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func readJSONL[T any](path string) ([]T, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []T
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var v T
		if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, sc.Err()
}
