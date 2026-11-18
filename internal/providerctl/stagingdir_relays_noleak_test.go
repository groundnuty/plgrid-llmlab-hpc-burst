package providerctl

// The host-dialed path of the Provider's stagingDir (HBEP-4, Provider settings; #757, #735): Sano lists only host-dialed, and
// there the Allocation's directory is in the relays' command line (--chdir and --dir), and the login node's standard error
// comes back, where srun prints the directory it could not enter. The relays' ssh here is a script that records its arguments
// and ends at once with such an srun text on its standard error, naming the --chdir argument it was given. Nothing the
// controller keeps may then hold the directory or its unique middle segment: not the relays' states, the Allocation, the
// events, nor any line the controller logged. The recorded command line shows the path is in the system, so the scan looks
// for something that is really there. The sibling rows (stagingdir_provider_test.go) show the same directory in the command line.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/go-logr/logr/funcr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

const (
	relayLeakDir     = "/invented-stage/zq7f3akx/home/someone"
	relayLeakSegment = "zq7f3akx"
)

// relayLogSink keeps every line the controller logs up to the controller-runtime levels (the same cap as the e2e no-leak rows).
type relayLogSink struct {
	mu    sync.Mutex
	lines []string
}

func (s *relayLogSink) add(prefix, args string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lines = append(s.lines, strings.TrimSpace(prefix+" "+args))
}

func (s *relayLogSink) text() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Join(s.lines, "\n")
}

func TestARelayEndingWithTheDirectoryOnStderrNeverLeaksIt(t *testing.T) {
	rec := filepath.Join(t.TempDir(), "ssh-args")
	script := filepath.Join(t.TempDir(), "ssh")
	body := "#!/bin/sh\n" +
		"printf '%s\\n' \"$*\" >> " + rec + "\n" +
		"d=$(printf '%s' \"$*\" | sed -n 's/.*--chdir=\\([^ ]*\\).*/\\1/p')\n" +
		"echo \"srun: error: couldn't chdir to \\`$d': No such file or directory\" >&2\n" +
		"echo \"slurmstepd: error: execve(): $d/agent: No such file or directory\" >&2\n" +
		"exit 1\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	sink := &relayLogSink{}
	logger := logging.NewLogrLogger(funcr.New(sink.add, funcr.Options{Verbosity: 5}))
	e := newEnv(t, hostDialed, withResolvedStaging(relayLeakDir), func(e *env) {
		e.opts.Relays.SSHBinary = script
		e.opts.Log = logger
	})
	runRelays(e)
	e.newAlloc("alloc-a")
	e.run("alloc-a", 4)
	a := e.get("alloc-a")
	tag := tagOf(a)

	// Positive controls: the directory is in the command line the relays ran, srun's text reached the controller as the end of a
	// relay, and the controller logged something.
	waitRelays(t, "a relay ended on srun's text, after a recorded start", func() bool {
		b, _ := os.ReadFile(rec)
		if !strings.Contains(string(b), "--chdir="+relayLeakDir+"/alloc/"+tag) {
			return false
		}
		for _, r := range relaysOf(e, tag) {
			if r.Starts < 2 || r.LastEnd == "" {
				return false
			}
		}
		return len(relaysOf(e, tag)) == 2
	})
	if sink.text() == "" {
		t.Fatal("the controller logged nothing: the log capture is not the real stream")
	}

	surfaces := map[string]string{
		"the relays' states":   fmt.Sprintf("%+v", e.ctl.Relays()),
		"the controller's log": sink.text(),
	}
	ab, err := yaml.Marshal(e.get("alloc-a"))
	if err != nil {
		t.Fatal(err)
	}
	surfaces["the Allocation"] = string(ab)
	e.rec.mu.Lock()
	surfaces["the legacy events"] = fmt.Sprintf("%+v", e.rec.events)
	e.rec.mu.Unlock()
	e.typed.mu.Lock()
	for _, ev := range e.typed.events {
		surfaces["the typed event "+ev.reason] += ev.id + " " + ev.action + " " + ev.note + "\n"
	}
	e.typed.mu.Unlock()
	if err := e.ctl.SyncStatus(e.ctx); err != nil {
		t.Fatal(err)
	}
	var prov v1alpha1.Provider
	if err := e.c.Get(e.ctx, client.ObjectKey{Name: testProvider}, &prov); err != nil {
		t.Fatal(err)
	}
	pb, err := yaml.Marshal(prov.Status)
	if err != nil {
		t.Fatal(err)
	}
	surfaces["the Provider's status"] = string(pb)
	for name, text := range surfaces {
		if strings.Contains(text, relayLeakDir) || strings.Contains(text, relayLeakSegment) {
			var hits []string
			for _, line := range strings.Split(text, "\n") {
				if (strings.Contains(line, relayLeakDir) || strings.Contains(line, relayLeakSegment)) && len(hits) < 3 {
					hits = append(hits, line[:min(len(line), 300)])
				}
			}
			t.Errorf("%s holds the Provider's stagingDir or its unique segment, in: %q", name, hits)
		}
	}
}
