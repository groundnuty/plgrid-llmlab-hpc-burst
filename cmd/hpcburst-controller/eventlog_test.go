package main

import (
	"bytes"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
)

func TestEventlogSubcommandIsDispatchedBeforeTheControllersFlags(t *testing.T) {
	var stderr bytes.Buffer
	// A usage error of the subcommand's own proves it ran: the controller's flag set would say
	// "flag provided but not defined" for --caps-file.
	code, ok := runSubcommand([]string{"eventlog", "--caps-file", "x"}, &stderr)
	if !ok || code != 2 || !strings.Contains(stderr.String(), "--config is required") {
		t.Fatalf("runSubcommand(eventlog ...) = %d, %v, stderr %q; want 2, true and the subcommand's own message", code, ok, stderr.String())
	}
	if _, ok := runSubcommand([]string{"--health-addr", "127.0.0.1:1"}, &stderr); ok {
		t.Fatal("the controller's own flags were taken for a subcommand")
	}
}

func TestParseEventlog(t *testing.T) {
	f, err := parseEventlog([]string{"--config", "/s/eventlog/nats.conf", "--caps-file", "/s/eventlog/caps.json"}, &bytes.Buffer{})
	if err != nil || f.config != "/s/eventlog/nats.conf" || f.caps != "/s/eventlog/caps.json" {
		t.Fatalf("parse: %+v, %v", f, err)
	}
	for name, args := range map[string][]string{
		"no config":        {"--caps-file", "c"},
		"no caps":          {"--config", "c"},
		"an extra word":    {"--config", "a", "--caps-file", "b", "token"},
		"an unknown flag":  {"--config", "a", "--caps-file", "b", "--token", "x"},
		"a flag with none": {"--config"},
	} {
		if _, err := parseEventlog(args, &bytes.Buffer{}); err == nil {
			t.Errorf("%s: parse accepted %v", name, args)
		}
	}
	var help bytes.Buffer
	if _, err := parseEventlog([]string{"-h"}, &help); !errors.Is(err, flag.ErrHelp) || !strings.Contains(help.String(), "usage: hpcburst-controller eventlog") {
		t.Fatalf("-h: %v, %q", err, help.String())
	}
	// The token is in the configuration file only: no flag carries one.
	if strings.Contains(strings.ToLower(help.String()), "token") {
		t.Errorf("the usage names a token flag:\n%s", help.String())
	}
}

// A configuration the process refuses ends with exit status 3, naming the setting on stderr, and
// without starting a server (the process tests of internal/eventlog/node start one for real).
func TestEventlogExitsRefusedWithTheSettingNamed(t *testing.T) {
	dir := t.TempDir()
	conf := filepath.Join(dir, "nats.conf")
	caps := filepath.Join(dir, "caps.json")
	if err := os.WriteFile(conf, []byte("listen: 127.0.0.1:1\njetstream {\n  sync_intervall: always\n}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := eventlog.WriteCaps(caps, eventlog.Caps{Events: 1000, Usage: 1000}); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := eventlogMain([]string{"--config", conf, "--caps-file", caps}, &stdout, &stderr); code != eventlog.ExitRefused {
		t.Fatalf("exit %d, want %d; stderr:\n%s", code, eventlog.ExitRefused, stderr.String())
	}
	if !strings.Contains(stderr.String(), "sync_intervall") {
		t.Errorf("stderr does not name the setting:\n%s", stderr.String())
	}
	if stdout.Len() != 0 {
		t.Errorf("a refused log printed on stdout: %q", stdout.String())
	}
	// A missing caps file is a failure, not a refusal of a setting.
	if code := eventlogMain([]string{"--config", conf, "--caps-file", filepath.Join(dir, "nope.json")}, &stdout, &stderr); code != 1 {
		t.Errorf("a missing caps file: exit %d, want 1", code)
	}
}
