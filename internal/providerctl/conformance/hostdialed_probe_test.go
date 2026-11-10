package conformance

// The serving probe's report (testdata/serving-probe.sh): what the job's step wrote, read line by line in its own form,
// never guessed at.

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// probeReport is what the serving probe wrote, line by line (testdata/serving-probe.sh's header).
type probeReport struct {
	TmpDirSet     bool
	TmpDir        string // private
	Socket        string // private
	SocketDirMode string
	Listening     int
	Fallback      int
	PortFound     bool
	Connected     bool
	GotBytes      int
	GotSHA256     string // "none", or 64 hex digits
}

var (
	hex64     = regexp.MustCompile(`^[0-9a-f]{64}$`)
	octalMode = regexp.MustCompile(`^[0-7]{3,4}$`)
)

// parseProbe reads the probe's output: every line of the header's forms, in its order, each once, nothing else. Anything
// else is an unreadable report, named, never a guess. The paths are kept as they came; they are private.
func parseProbe(out []byte) (probeReport, error) {
	var r probeReport
	lines := strings.Split(strings.TrimSuffix(string(out), "\n"), "\n")
	i := 0
	next := func(key string) (string, error) {
		if i >= len(lines) {
			return "", fmt.Errorf("the probe's report ends before %s", key)
		}
		k, v, ok := strings.Cut(lines[i], "=")
		if !ok || k != key {
			return "", fmt.Errorf("the probe's line %d is not %s", i+1, key)
		}
		i++
		return v, nil
	}
	count := func(key string) (int, error) {
		v, err := next(key)
		if err != nil {
			return 0, err
		}
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || strconv.Itoa(n) != v {
			return 0, fmt.Errorf("the probe's %s is not a count", key)
		}
		return n, nil
	}
	oneOf := func(key string, values ...string) (string, error) {
		v, err := next(key)
		if err != nil {
			return "", err
		}
		for _, w := range values {
			if v == w {
				return v, nil
			}
		}
		return "", fmt.Errorf("the probe's %s is not one of %s", key, strings.Join(values, ", "))
	}
	if _, err := oneOf("probe", "1"); err != nil {
		return r, err
	}
	set, err := oneOf("tmpdir-step", "set", "unset")
	if err != nil {
		return r, err
	}
	if r.TmpDirSet = set == "set"; r.TmpDirSet {
		if r.TmpDir, err = next("tmpdir"); err != nil {
			return r, err
		}
	}
	if r.Socket, err = next("socket"); err != nil {
		return r, err
	}
	if r.SocketDirMode, err = next("socket-dir-mode"); err != nil {
		return r, err
	}
	if r.SocketDirMode != "unknown" && !octalMode.MatchString(r.SocketDirMode) {
		return r, fmt.Errorf("the probe's socket-dir-mode is not a mode")
	}
	if r.Listening, err = count("listening"); err != nil {
		return r, err
	}
	if r.Fallback, err = count("fallback"); err != nil {
		return r, err
	}
	port, err := oneOf("serving-port", "found", "missing")
	if err != nil {
		return r, err
	}
	if r.PortFound = port == "found"; r.PortFound {
		c, err := oneOf("connect", "ok", "failed")
		if err != nil {
			return r, err
		}
		r.Connected = c == "ok"
		if r.GotBytes, err = count("got-bytes"); err != nil {
			return r, err
		}
		if r.GotSHA256, err = next("got-sha256"); err != nil {
			return r, err
		}
		if r.GotSHA256 != "none" && !hex64.MatchString(r.GotSHA256) {
			return r, fmt.Errorf("the probe's got-sha256 is not a sha256")
		}
	}
	if _, err := oneOf("done", "1"); err != nil {
		return r, err
	}
	if i != len(lines) {
		return r, fmt.Errorf("the probe's report has %d line(s) after its end", len(lines)-i)
	}
	return r, nil
}
