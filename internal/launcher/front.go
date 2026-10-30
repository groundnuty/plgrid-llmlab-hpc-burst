package launcher

import (
	"fmt"
	"net"
	"os"
	"strconv"
)

// Front is the operator's statement of how compute nodes reach the front (HBEP-11, open
// question 8): directly on the dial-home port, or over ssh on port 22 with a restricted key.
// The launcher records it. The live check by the first Instance comes later, with the front.
type Front struct {
	Mode    string `json:"mode"`               // "direct", "ssh", or "" when not given
	Addr    string `json:"addr"`               // host:port; for ssh the port is always 22
	KeyPath string `json:"key_path,omitempty"` // ssh only: the path of the restricted key, never its content
}

// Set reports whether the operator gave a front at all.
func (f Front) Set() bool { return f.Mode != "" || f.Addr != "" || f.KeyPath != "" }

// Normalize validates the front and returns it in recorded form (for ssh the address gets :22).
func (f Front) Normalize() (Front, error) {
	if !f.Set() {
		return f, nil
	}
	if f.Mode != "direct" && f.Mode != "ssh" {
		return f, fmt.Errorf("front mode %q: want direct or ssh (give --front-mode together with --front-addr)", f.Mode)
	}
	if f.Addr == "" {
		return f, fmt.Errorf("front mode %s needs --front-addr", f.Mode)
	}
	host, port, err := net.SplitHostPort(f.Addr)
	if err != nil {
		if f.Mode != "ssh" {
			return f, fmt.Errorf("front address %q: want host:port (the dial-home port): %w", f.Addr, err)
		}
		host, port = f.Addr, "22" // ssh without a port means 22
	}
	if host == "" {
		return f, fmt.Errorf("front address %q has no host", f.Addr)
	}
	if p, err := strconv.Atoi(port); err != nil || p < 1 || p > 65535 {
		return f, fmt.Errorf("front address %q: %q is not a port", f.Addr, port)
	}
	out := Front{Mode: f.Mode, Addr: net.JoinHostPort(host, port), KeyPath: f.KeyPath}
	switch f.Mode {
	case "ssh":
		if port != "22" {
			return f, fmt.Errorf("front address %q: the ssh dial-home mode uses port 22 (HBEP-11, open question 8)", f.Addr)
		}
		if f.KeyPath == "" {
			return f, fmt.Errorf("front mode ssh needs --front-key, the path of the restricted key")
		}
		fi, err := os.Stat(f.KeyPath)
		if err != nil {
			return f, fmt.Errorf("front key: %w", err)
		}
		if !fi.Mode().IsRegular() {
			return f, fmt.Errorf("front key %s is not a regular file", f.KeyPath)
		}
	case "direct":
		if f.KeyPath != "" {
			return f, fmt.Errorf("front mode direct takes no key; --front-key is for ssh")
		}
	}
	return out, nil
}

// frontWarning is printed when the operator named no front. The host starts anyway (decision of
// HBEP-11's owner, 30 Oct 2026): it is the lifecycle controller that refuses to submit jobs while
// no front is configured.
const frontWarning = "warning: no front recorded (--front-mode, --front-addr): no jobs will be submitted until a front is configured. " +
	"Alpha needs a front that compute nodes reach, directly on the dial-home port or over ssh on port 22 with a restricted key " +
	"(HBEP-11, open question 8). Record it with --front-mode direct|ssh and --front-addr."
