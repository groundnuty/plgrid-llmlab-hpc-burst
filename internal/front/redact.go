package front

import (
	"bytes"
	"net/http"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// redacted replaces a target credential in a passed-through error.
const redacted = "[redacted]"

// minSecret is the shortest credential redacted: a shorter one would match
// ordinary bytes. Every key the front holds is far longer.
const minSecret = 8

// targetRefs lists where the credentials of every target in the current
// view live: the managed Providers' credentials files and the served
// Instances' UIDs.
func (r *Router) targetRefs() (credFiles []string, uids []types.UID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.view == nil {
		return nil, nil
	}
	seen := map[string]bool{}
	for _, m := range r.view.Models {
		for _, t := range m.Managed {
			if !seen[t.CredentialsFile] {
				seen[t.CredentialsFile] = true
				credFiles = append(credFiles, t.CredentialsFile)
			}
		}
		for _, d := range m.Served {
			for _, i := range d.Members {
				uids = append(uids, i.UID)
			}
		}
	}
	return credFiles, uids
}

// secrets are every target credential the front holds: the managed keys
// and every Instance key, not only the key of the request's target
// (@hpc-burst-science-agent, 31 Oct 2026). They are read only when an error
// passes through. The longest go first, so one that contains another is
// replaced whole.
func (p *Proxy) secrets(extra ...string) []string {
	files, uids := p.router.targetRefs()
	out := append([]string(nil), extra...)
	for _, f := range files {
		if c, err := provider.ReadManagedCredentials(f); err == nil {
			out = append(out, c.Key)
		}
	}
	for _, uid := range uids {
		if k, err := agentapi.ReadKeyFile(agentapi.InstanceKeyPath(p.host.CredentialsDir(), string(uid))); err == nil {
			out = append(out, k)
		}
	}
	keep := out[:0]
	for _, s := range out {
		if len(s) >= minSecret {
			keep = append(keep, s)
		}
	}
	sort.Slice(keep, func(i, j int) bool { return len(keep[i]) > len(keep[j]) })
	return keep
}

// redact replaces every copy of each secret in b; all other bytes pass
// unchanged.
func redact(b []byte, secrets []string) []byte {
	for _, s := range secrets {
		if bytes.Contains(b, []byte(s)) {
			b = bytes.ReplaceAll(b, []byte(s), []byte(redacted))
		}
	}
	return b
}

// redactHeader replaces every copy of each secret in the header values.
func redactHeader(h http.Header, secrets []string) {
	for k, vs := range h {
		for i, v := range vs {
			for _, s := range secrets {
				v = strings.ReplaceAll(v, s, redacted)
			}
			h[k][i] = v
		}
	}
}

// targetKey is the key the front sent the request's target, which may have
// left the view since.
func (p *Proxy) targetKey(t *target) string {
	if t.managed != nil {
		if c, err := provider.ReadManagedCredentials(t.managed.CredentialsFile); err == nil {
			return c.Key
		}
		return ""
	}
	k, _ := agentapi.ReadKeyFile(agentapi.InstanceKeyPath(p.host.CredentialsDir(), string(t.inst.UID)))
	return k
}
