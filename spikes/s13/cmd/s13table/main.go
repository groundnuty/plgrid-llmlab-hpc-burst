// Command s13table reads the committed runs (results/*/results.jsonl), recomputes every verdict
// from the bytes sent and received, and prints the README's tables: each covered field that any
// run did not deliver as sent, side by side per run, and the values the routers injected.
//
//	s13table results/vllm-router-0.1.16-consistent_hash results/vllm-router-0.1.17-consistent_hash ...
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s13/rawjson"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s13/results"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: s13table <results dir>...")
		os.Exit(2)
	}
	var runs []*results.Run
	for _, d := range os.Args[1:] {
		r, err := results.Load(filepath.Join(d, "results.jsonl"))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		runs = append(runs, r)
	}
	fmt.Print(countTable(runs))
	fmt.Print(fieldTable(runs))
	fmt.Print(injectedTable(runs))
}

// countTable counts, per run, the covers of enumerated fields and of extensions by their worst
// verdict over the plain and streamed variants.
func countTable(runs []*results.Run) string {
	var b strings.Builder
	b.WriteString("| run | covers | kept | kept, re-encoded | key re-escaped | duplicate removed | dropped | retyped | changed | renamed | refused |\n|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, r := range runs {
		for _, ext := range []bool{false, true} {
			n := map[rawjson.Verdict]int{}
			total := 0
			for k, m := range r.Fields {
				if strings.HasPrefix(k.Field, "ext:") != ext {
					continue
				}
				total++
				worst := rawjson.Kept
				for _, f := range m {
					if f.Verdict == results.Refused || rawjson.Worse(f.Verdict, worst) {
						worst = f.Verdict
					}
				}
				n[worst]++
			}
			kind := "enumerated fields"
			if ext {
				kind = "extensions and encodings"
			}
			fmt.Fprintf(&b, "| %s, %s | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d |\n", r.Label, kind, total, n[rawjson.Kept], n[rawjson.Reencoded],
				n[rawjson.KeyReencoded], n[rawjson.DuplicateRemoved], n[rawjson.Dropped], n[rawjson.Retyped], n[rawjson.Changed], n[rawjson.Renamed], n[results.Refused])
		}
	}
	return b.String() + "\n"
}

func cell(s string) string {
	if s == "" {
		return "—"
	}
	if len(s) > 60 {
		s = s[:60] + "…"
	}
	return "`" + strings.ReplaceAll(s, "|", `\|`) + "`"
}

// verdict joins a field's plain and streamed verdicts; one word when they agree.
func verdict(f map[bool]results.Field) string {
	p, hp := f[false]
	s, hs := f[true]
	switch {
	case hp && hs && p.Verdict == s.Verdict:
		return string(p.Verdict)
	case hp && hs:
		return string(p.Verdict) + " / stream: " + string(s.Verdict)
	case hs:
		return string(s.Verdict) + " (stream only)"
	}
	return string(p.Verdict)
}

func fieldTable(runs []*results.Run) string {
	var b strings.Builder
	b.WriteString("| case | field | sent |")
	for _, r := range runs {
		fmt.Fprintf(&b, " %s: received | verdict |", r.Label)
	}
	b.WriteString("\n|---|---|---|" + strings.Repeat("---|---|", len(runs)) + "\n")
	for _, k := range runs[0].FieldOrder {
		allKept := true
		for _, r := range runs {
			for _, f := range r.Fields[k] {
				if f.Verdict != rawjson.Kept {
					allKept = false
				}
			}
		}
		if allKept {
			continue
		}
		first := runs[0].Fields[k]
		show, ok := first[false]
		if !ok {
			show = first[true]
		}
		fmt.Fprintf(&b, "| %s | %s | %s |", k.Case, k.Field, cell(show.Sent))
		for _, r := range runs {
			f := r.Fields[k]
			got, ok := f[false]
			if !ok {
				got = f[true]
			}
			fmt.Fprintf(&b, " %s | %s |", cell(got.Received), verdict(f))
		}
		b.WriteString("\n")
	}
	return b.String()
}

func injectedTable(runs []*results.Run) string {
	var b strings.Builder
	b.WriteString("\n| endpoint | injected path | value |")
	for _, r := range runs {
		fmt.Fprintf(&b, " %s: variants |", r.Label)
	}
	b.WriteString("\n|---|---|---|" + strings.Repeat("---|", len(runs)) + "\n")
	keys := map[results.Injection]bool{}
	for _, r := range runs {
		for k := range r.Injected {
			keys[k] = true
		}
	}
	var order []results.Injection
	for k := range keys {
		order = append(order, k)
	}
	sort.Slice(order, func(i, j int) bool {
		if order[i].Endpoint != order[j].Endpoint {
			return order[i].Endpoint < order[j].Endpoint
		}
		return order[i].Path < order[j].Path
	})
	for _, k := range order {
		fmt.Fprintf(&b, "| %s | %s | %s |", k.Endpoint, cell(k.Path), cell(k.Value))
		for _, r := range runs {
			fmt.Fprintf(&b, " %d of %d |", r.Injected[k], r.Delivered[k.Endpoint])
		}
		b.WriteString("\n")
	}
	return b.String()
}
