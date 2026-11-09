package launcher

import (
	"fmt"
	"net"
	"os"
	"strconv"
)

// Front is the operator's statement of how the jobs and the control-plane host reach each other
// (HBEP-11, open question 8; HBEP-6, Dial-home modes): directly on the dial-home port, over ssh
// with a restricted key (port 22 by default, or any other port the operator routes to the
// control-plane host's sshd; `@hpc-burst-science-agent`, 31 Oct 2026), or host-dialed: no address
// and no key, because the host dials the jobs through the login node (HBEP-6, Dial-home modes;
// HBEP-11, open question 8, amended 9 Nov 2026).
// The launcher records it. The live check by the first Instance comes later, with the front.
type Front struct {
	Mode    string `json:"mode"`               // "direct", "ssh", "host-dialed", or "" when not given
	Addr    string `json:"addr"`               // host:port; for ssh a missing port means 22; empty for host-dialed
	KeyPath string `json:"key_path,omitempty"` // ssh only: the path of the restricted key, never its content
}

// frontModeHostDialed is the mode that has no front address and no key (agentapi.DialHomeHostDialed;
// the launcher does not import pkg/agentapi, so it names the recorded string itself).
const frontModeHostDialed = "host-dialed"

// Set reports whether the operator gave a front at all.
func (f Front) Set() bool { return f.Mode != "" || f.Addr != "" || f.KeyPath != "" }

// Normalize validates the front and returns it in recorded form (for ssh the address gets :22;
// host-dialed records the mode alone).
func (f Front) Normalize() (Front, error) {
	if !f.Set() {
		return f, nil
	}
	if f.Mode != "direct" && f.Mode != "ssh" && f.Mode != frontModeHostDialed {
		return f, fmt.Errorf("front mode %q: want direct, ssh or %s (give --front-mode; direct and ssh also need --front-addr)", f.Mode, frontModeHostDialed)
	}
	if f.Mode == frontModeHostDialed {
		// Nothing in the job dials the host, so there is no address to record and no key to hold.
		if f.Addr != "" {
			return f, fmt.Errorf("front mode %s takes no --front-addr: the host dials the jobs", f.Mode)
		}
		if f.KeyPath != "" {
			return f, fmt.Errorf("front mode %s takes no key; --front-key is for ssh", f.Mode)
		}
		return Front{Mode: f.Mode}, nil
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
	"Alpha needs a front that compute nodes reach, directly on the dial-home port or over ssh (port 22 by default) with a restricted key, " +
	"or host-dialed mode, where the host dials the jobs and there is no address (HBEP-11, open question 8). " +
	"Record it with --front-mode direct|ssh|host-dialed, and --front-addr for direct and ssh."
