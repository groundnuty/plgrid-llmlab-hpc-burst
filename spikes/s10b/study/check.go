package study

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// s10Source says which of S10's committed result files holds the same
// configuration as one of ours, and under which scenario and policy name.
type s10Source struct {
	file, scenario string
	kv             int
	release        bool // the policy name carries +release-on-drain
}

var s10Sources = map[string]s10Source{
	"tight":        {"steady.csv", "steady", 1_500_000, false},
	"scarce":       {"steady.csv", "steady", 750_000, false},
	"half-prefill": {"sens-prefill-12500.csv", "steady", 1_500_000, false},
	"drain":        {"drainfix-1500000.csv", "drain", 1_500_000, true},
}

// CheckAgainstS10 compares every round-robin, c = 1.25 and c = 2.0 cell
// with the row S10 committed for the same regime, policy, key and seed: the
// requests, prompt tokens, hit tokens and completed output must match
// exactly. Those options do not use anything S10b added (the cap, the
// adaptive bound, the front queue), so a mismatch means the additions
// changed S10's behaviour. The expected values are S10's committed results,
// produced before the additions. It returns the number of cells compared and
// one message per mismatch or missing row.
func CheckAgainstS10(cells []Cell, s10Results string) (compared int, problems []string, err error) {
	tables := map[string][]map[string]string{}
	load := func(file string) ([]map[string]string, error) {
		if t, ok := tables[file]; ok {
			return t, nil
		}
		f, err := os.Open(filepath.Join(s10Results, file))
		if err != nil {
			return nil, err
		}
		defer f.Close()
		recs, err := csv.NewReader(f).ReadAll()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", file, err)
		}
		var rows []map[string]string
		for _, r := range recs[1:] {
			row := map[string]string{}
			for i, h := range recs[0] {
				row[h] = r[i]
			}
			rows = append(rows, row)
		}
		tables[file] = rows
		return rows, nil
	}
	for _, c := range cells {
		var policy string
		switch c.Option {
		case RoundRobin.Name:
			policy = "round-robin"
		case "c=1.25":
			policy = "chwbl-1.25"
		case "c=2.0":
			policy = "chwbl-2.00"
		default:
			continue
		}
		src := s10Sources[c.Regime]
		file, key := src.file, c.Key
		if c.Option == RoundRobin.Name {
			key = ""
			if src.release { // S10 ran round robin around the drain in the events sweep
				file = "events-1500000.csv"
			}
		} else if src.release {
			policy += "+release-on-drain"
		}
		rows, err := load(file)
		if err != nil {
			return compared, problems, err
		}
		var found map[string]string
		for _, r := range rows {
			if r["scenario"] == src.scenario && r["kv_tokens"] == strconv.Itoa(src.kv) && r["seed"] == fmt.Sprint(c.Seed) && r["policy"] == policy && r["key"] == key {
				found = r
				break
			}
		}
		if found == nil {
			problems = append(problems, fmt.Sprintf("%s %s %s seed %d: no row in %s", c.Regime, c.Option, c.Key, c.Seed, file))
			continue
		}
		compared++
		for _, f := range []struct {
			col  string
			ours int64
		}{{"requests", int64(c.Requests)}, {"prompt_tokens", c.PromptTokens}, {"hit_tokens", c.HitTokens}, {"done_output_tokens", c.DoneOutput}} {
			if found[f.col] != fmt.Sprint(f.ours) {
				problems = append(problems, fmt.Sprintf("%s %s %s seed %d: %s is %d, S10 committed %s", c.Regime, c.Option, c.Key, c.Seed, f.col, f.ours, found[f.col]))
			}
		}
	}
	return compared, problems, nil
}
