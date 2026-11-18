// Command studyload is a load generator for an artificial study (HBEP-764, issue #764, test item 3): it runs N concurrent multi-turn chat
// sessions for each declared workload, with a fixed shape (context, turn and output sizes in words, turns and think time), against one
// OpenAI-compatible endpoint. Every request carries a per-session header (default x-session-id) and the workload's static headers; every
// response's attribution headers (HBEP-5) and timings are recorded, one JSON line per request. See README.md.
//
//	go run ./hack/studyload -config study.json -out records.jsonl
//
// It prints a summary per workload and per route on stdout. Exit 0: every request succeeded; 1: some request failed (the records and
// the summary say which); 2: a usage, config or file error, before or during the run.
//
// This is a test tool. It holds no HBEP-764 mechanism: it routes nothing and budgets nothing.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

// run is main with its streams and context passed in.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("studyload", flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfgPath := fs.String("config", "", "the JSON config file (required)")
	outPath := fs.String("out", "", "the records file, one JSON line per request; it must not exist yet (required)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *cfgPath == "" || *outPath == "" || fs.NArg() != 0 {
		fmt.Fprintln(stderr, "usage: studyload -config <config.json> -out <records.jsonl>")
		return 2
	}
	cfg, err := LoadConfig(*cfgPath)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	key, err := readKey(cfg.APIKeyFile)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	out, err := os.OpenFile(*outPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		fmt.Fprintf(stderr, "records file: %v\n", err)
		return 2
	}
	rw := newRecordWriter(out)
	recs, runErr := Run(ctx, cfg, key, rw.write)
	closeErr := out.Close()
	writeSummary(stdout, cfg, recs)
	switch {
	case runErr != nil:
		fmt.Fprintln(stderr, runErr)
		return 2
	case closeErr != nil:
		fmt.Fprintf(stderr, "records file: %v\n", closeErr)
		return 2
	}
	for _, r := range recs {
		if r.Error != "" {
			return 1
		}
	}
	return 0
}

// readKey reads the API key from its file when one is named. The error never carries the file's content.
func readKey(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		var pe *fs.PathError
		if errors.As(err, &pe) {
			err = pe.Err // the reason, without the path
		}
		return "", fmt.Errorf("apiKeyFile: %w", err)
	}
	key := strings.TrimSpace(string(b))
	if key == "" || strings.ContainsAny(key, "\r\n") {
		return "", errors.New("apiKeyFile: the file must hold one non-empty line")
	}
	return key, nil
}
