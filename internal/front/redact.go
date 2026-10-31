package front

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"path/filepath"
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

// credentialFiles lists every Provider's credentials file in the snapshot,
// attached or not: a key the front has held stays redacted after a Provider
// is detached.
func (r *Router) credentialFiles() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	seen := map[string]bool{}
	var out []string
	for i := range r.snap.Providers {
		if ref := r.snap.Providers[i].Spec.CredentialsRef; ref != nil && ref.File != "" && !seen[ref.File] {
			seen[ref.File] = true
			out = append(out, ref.File)
		}
	}
	return out
}

// secrets are every target credential the front holds: every Provider's
// managed key and every Instance key on the host, not only the key of the
// request's target
// (@hpc-burst-science-agent, 31 Oct 2026). They are read only when an error
// passes through. The longest go first, so one that contains another is
// replaced whole.
func (p *Proxy) secrets(extra ...string) []string {
	out := append([]string(nil), extra...)
	for _, f := range p.router.credentialFiles() {
		if c, err := provider.ReadManagedCredentials(f); err == nil {
			out = append(out, c.Key)
		}
	}
	// Every Instance key on the host, a draining Instance's too, not only
	// the members of the current view.
	dir := filepath.Dir(agentapi.InstanceKeyPath(p.host.CredentialsDir(), "x"))
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		uid := types.UID(e.Name())
		if k, ok := p.instKeys.Load(uid); ok { // an Instance's key never changes
			out = append(out, k.(string))
			continue
		}
		if k, err := agentapi.ReadKeyFile(filepath.Join(dir, e.Name())); err == nil {
			p.instKeys.Store(uid, k)
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

// redactReader redacts a target's body byte by byte as it streams, with no
// parsing (@hpc-burst-science-agent, 31 Oct 2026). A read's tail that could
// begin a secret is held until the next read completes or refutes it, so a
// key split between two reads is still caught; a tail that cannot, which is
// every SSE event's end in practice, passes at once, so streams are not
// delayed.
type redactReader struct {
	r       io.Reader
	secrets []string
	buf     []byte // read, not yet returned: out first, then held
	out     int    // bytes of buf ready to return
	err     error
	chunk   []byte
}

func newRedactReader(r io.Reader, secrets []string) io.Reader {
	if len(secrets) == 0 {
		return r
	}
	return &redactReader{r: r, secrets: secrets}
}

func (z *redactReader) Read(p []byte) (int, error) {
	for z.out == 0 {
		if z.err != nil {
			if len(z.buf) > 0 { // the end: what is held cannot become a secret
				z.buf = redact(z.buf, z.secrets)
				z.out = len(z.buf)
				break
			}
			return 0, z.err
		}
		if z.chunk == nil {
			z.chunk = make([]byte, 32<<10)
		}
		n, err := z.r.Read(z.chunk)
		z.err = err
		if n == 0 && err == nil {
			continue
		}
		z.buf = redact(append(z.buf, z.chunk[:n]...), z.secrets)
		z.out = len(z.buf) - z.held()
	}
	n := copy(p, z.buf[:z.out])
	z.buf, z.out = z.buf[n:], z.out-n
	return n, nil
}

// held is the length of buf's longest tail that is a proper prefix of a
// secret: the bytes that must wait for the next read.
func (z *redactReader) held() int {
	if z.err != nil {
		return 0
	}
	most := 0
	for _, s := range z.secrets {
		for k := min(len(s)-1, len(z.buf)); k > most; k-- {
			if bytes.HasSuffix(z.buf, []byte(s[:k])) {
				most = k
				break
			}
		}
	}
	return most
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
