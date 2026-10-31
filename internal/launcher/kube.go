package launcher

import (
	"context"
	"crypto/tls"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// wrongServerError is restConfig's refusal of a kubeconfig that is whole and names another
// server. A file cut short never gets this far: restConfig refuses it earlier, with a plain error,
// until it resolves completely (see resolved). A whole file that names the wrong server will not
// change, so waitReady fails on it at once instead of waiting.
type wrongServerError struct{ msg string }

func (e *wrongServerError) Error() string { return e.msg }

// resolved reports why a kubeconfig does not yet resolve to a cluster and a way to authenticate
// to it, or nil when it does: a non-empty current-context names a context in the file, that
// context names a cluster that is in the file, and a user that is in the file and carries a
// complete credential (a token, or a client certificate and key that both parse as PEM).
//
// k3s writes the file with clientcmd's serialisation, which puts a cluster's body before its name,
// the contexts and current-context after all the clusters, and the users last. So a file cut
// inside its first cluster still parses, as a cluster with no name and a cut-short server address,
// and client-go would use that cluster even though no context selects it. A file cut inside the
// users resolves to a config that cannot authenticate, and /readyz answers an anonymous request
// (system:public-info-viewer), so the wait would go on with a config that fails on everything
// else. Only a file that resolves completely can have its server address trusted, to use or to
// refuse. The credentials are held as data, as k3s writes them; a kubeconfig that names files
// instead is not one k3s wrote and counts as incomplete.
func resolved(c *clientcmdapi.Config) error {
	if c.CurrentContext == "" {
		return errors.New("it has no current-context (yet)")
	}
	cx := c.Contexts[c.CurrentContext]
	if cx == nil {
		return fmt.Errorf("its current-context %q names no context (yet)", c.CurrentContext)
	}
	if cx.Cluster == "" {
		return fmt.Errorf("its context %q names no cluster (yet)", c.CurrentContext)
	}
	if c.Clusters[cx.Cluster] == nil {
		return fmt.Errorf("its context %q names the cluster %q, which the file lacks (yet)", c.CurrentContext, cx.Cluster)
	}
	if cx.AuthInfo == "" {
		return fmt.Errorf("its context %q names no user (yet)", c.CurrentContext)
	}
	u := c.AuthInfos[cx.AuthInfo]
	if u == nil {
		return fmt.Errorf("its context %q names the user %q, which the file lacks (yet)", c.CurrentContext, cx.AuthInfo)
	}
	if u.Token == "" && !(isPEM(u.ClientCertificateData) && isPEM(u.ClientKeyData)) {
		return fmt.Errorf("its user %q has no complete credential (yet): a token, or a client certificate and key as PEM", cx.AuthInfo)
	}
	return nil
}

// isPEM reports whether b holds a PEM block.
func isPEM(b []byte) bool {
	blk, _ := pem.Decode(b)
	return blk != nil
}

// restConfig reads the launcher's kubeconfig and checks that it points at this host: loopback,
// and the HTTPS port the launcher gave k3s. A kubeconfig that names anything else (the system
// k3s on 6443, say) must never be used to judge or change this host. The file is read once, so
// the check that it is complete and the address it gives come from the same bytes. A file that
// is not complete is an ordinary error, never a wrongServerError, because its address may be cut
// short, and so is one that cannot authenticate yet.
func restConfig(path string, ports Ports, timeout time.Duration) (*rest.Config, error) {
	raw, err := (&clientcmd.ClientConfigLoadingRules{ExplicitPath: path}).Load()
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	if err := resolved(raw); err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	cfg, err := clientcmd.NewDefaultClientConfig(*raw, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	u, err := url.Parse(cfg.Host)
	if err != nil {
		return nil, fmt.Errorf("%s: bad server address %q: %w", path, cfg.Host, err)
	}
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil || port != strconv.Itoa(ports.HTTPS) || (host != "127.0.0.1" && host != "localhost" && host != "::1") {
		return nil, &wrongServerError{fmt.Sprintf("%s points at %s, not at this launcher's 127.0.0.1:%d", path, cfg.Host, ports.HTTPS)}
	}
	cfg.Timeout = timeout
	cfg.QPS, cfg.Burst = 50, 100
	cfg.WarningHandler = rest.NoWarnings{} // API deprecation notices are not the launcher's to print
	return cfg, nil
}

// getStatus does a GET, bound to ctx, and returns the status code; the body is discarded.
func getStatus(ctx context.Context, hc *http.Client, rawURL string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return 0, err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}

// readyz asks the apiserver for /readyz with the launcher's credentials. The request ends with ctx
// or with cfg.Timeout, whichever comes first.
func readyz(ctx context.Context, cfg *rest.Config) error {
	hc, err := rest.HTTPClientFor(cfg)
	if err != nil {
		return err
	}
	code, err := getStatus(ctx, hc, cfg.Host+"/readyz")
	if err != nil {
		return err
	}
	if code != http.StatusOK {
		return fmt.Errorf("/readyz answered %d", code)
	}
	return nil
}

// kcmHealthz asks k3s's embedded controller manager for /healthz on its own secure port. The
// controller manager serves a self-signed certificate on loopback, so the certificate is not
// verified; what ties the answer to this host is that the caller checks the port is held by the
// launched process. The request ends with ctx or after timeout, whichever comes first.
func kcmHealthz(ctx context.Context, port int, timeout time.Duration) error {
	hc := &http.Client{Timeout: timeout, Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}} //nolint:gosec // loopback, self-signed; see above
	code, err := getStatus(ctx, hc, fmt.Sprintf("https://127.0.0.1:%d/healthz", port))
	if err != nil {
		return err
	}
	if code != http.StatusOK {
		return fmt.Errorf("/healthz answered %d", code)
	}
	return nil
}

const (
	// probeTimeout is the longest one probe (one HTTP request) may take; near the deadline it is
	// cut to the time left, so no probe runs, or answers, past it.
	probeTimeout = 1500 * time.Millisecond
	// probePause is how long waitReady waits between rounds.
	probePause = 20 * time.Millisecond
)

// waitReadyHook, when set (by tests only), runs on waitReady's own goroutine after each round
// that did not finish the wait, with the reason that round gave (nil when the round got a step
// further, or had nothing to report yet). A test uses it to change the host between rounds in a
// fixed order: the rounds are the only events it needs to count.
var waitReadyHook func(last error)

// waitReady waits until the host the record names is ready: /readyz answers 200 on the
// apiserver, and /healthz answers 200 on the embedded controller manager's port. It is tied to
// the launched process at every step through h, the pidfd taken at launch. The process must still
// be the recorded one at every probe, and the probed port must be held by that process, before
// and after the answer. So a 200 from some other server on the port (a system k3s, a leftover)
// counts for nothing. Before it returns ready, confirmHost checks the whole host once more.
//
// timeout is a deadline for the whole wait, probes included: a probe's own timeout is the shorter
// of probeTimeout and the time left when its round began, its request also ends at the deadline
// and with ctx, and the pause between rounds is cut to the time left. A probe that starts just
// before the deadline therefore cannot answer after it.
func waitReady(ctx context.Context, l layout, st *State, h *procHandle, timeout time.Duration) (readyzAt, readyAt time.Duration, err error) {
	t0 := st.StartedAt
	deadline := time.Now().Add(timeout)
	// The requests end at the deadline whatever else a round does before it sends them (scanning
	// /proc for the port's owner, say). The loop below tests ctx, not dctx, so that running out of
	// time still gives the "not ready after" error and not a context error.
	dctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	stage := "apiserver /readyz"
	var last error
	readyzDone := false
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return 0, 0, ctx.Err()
		}
		if v, d := h.verify(); v != Ours {
			return 0, 0, fmt.Errorf("k3s is no longer running before it was ready (%s); see %s", d, l.logFile())
		}
		ptimeout := max(min(probeTimeout, time.Until(deadline)), time.Millisecond)
		if !readyzDone {
			// k3s creates the admin kubeconfig early in its start and fills it in afterwards,
			// not atomically, so a read can find it empty or cut short (a slow filesystem makes
			// that common). The file is read again at every probe until /readyz has answered,
			// because even a cut that parses (inside a base64 blob, on a 4-character boundary)
			// can give a config that only fails when it is used. A file that does not parse yet,
			// or does not yet resolve to a cluster (restConfig), is not ready yet: remember why
			// and try again. Only a file that resolves completely can be refused for its server
			// address, because in a cut one the address may be cut short. cfg comes only from a
			// whole, checked read, so a kubeconfig that names another server is never used.
			var cfg *rest.Config
			if _, serr := os.Stat(l.adminKubeconfig()); serr == nil {
				c, cerr := restConfig(l.adminKubeconfig(), st.Ports, ptimeout)
				var wrong *wrongServerError
				if errors.As(cerr, &wrong) {
					return 0, 0, cerr
				}
				if cerr != nil {
					last = cerr
				} else {
					cfg = c
				}
			}
			if cfg != nil {
				if own, oerr := portOwned(h, st.Ports.HTTPS); oerr != nil {
					last = oerr
				} else if !own {
					last = fmt.Errorf("port %d is not held by the launched k3s yet", st.Ports.HTTPS)
				} else if rerr := readyz(dctx, cfg); rerr != nil {
					last = rerr
				} else if own2, _ := portOwned(h, st.Ports.HTTPS); !own2 {
					last = fmt.Errorf("/readyz answered, but port %d is not held by the launched k3s", st.Ports.HTTPS)
				} else {
					readyzDone, readyzAt, stage, last = true, time.Since(t0), "controller manager /healthz", nil
				}
			}
		} else {
			if own, oerr := portOwned(h, st.Ports.KCM); oerr != nil {
				last = oerr
			} else if !own {
				last = fmt.Errorf("port %d is not held by the launched k3s yet", st.Ports.KCM)
			} else if herr := kcmHealthz(dctx, st.Ports.KCM, ptimeout); herr != nil {
				last = herr
			} else if own2, _ := portOwned(h, st.Ports.KCM); !own2 {
				last = fmt.Errorf("/healthz answered, but port %d is not held by the launched k3s", st.Ports.KCM)
			} else {
				readyAt = time.Since(t0)
				if err := confirmHost(l, st, h); err != nil {
					return 0, 0, err
				}
				return readyzAt, readyAt, nil
			}
		}
		if waitReadyHook != nil {
			waitReadyHook(last)
		}
		if pause := min(probePause, time.Until(deadline)); pause > 0 {
			select {
			case <-time.After(pause):
			case <-ctx.Done():
			}
		}
	}
	return 0, 0, fmt.Errorf("%s not ready after %s: %v; see %s", stage, timeout, last, l.logFile())
}

// confirmHost is the last check before a host counts as ready or healthy, made after the probes
// have answered: the process tree holds exactly the expected loopback listeners, k3s runs a binary
// it unpacked into its own data directory, and the process is still the recorded one after both
// reads.
func confirmHost(l layout, st *State, h *procHandle) error {
	owned, others, err := treeListeners(h)
	if err != nil {
		return fmt.Errorf("cannot list the listeners: %w", err)
	}
	if probs := verifyListeners(st.Ports.Expected(), owned, others); len(probs) > 0 {
		return fmt.Errorf("k3s answered, but its listeners are wrong: %v", probs)
	}
	if _, err := checkExe(l, h); err != nil {
		return err
	}
	if v, d := h.verify(); v != Ours {
		return fmt.Errorf("k3s changed while it was checked: %s", d)
	}
	return nil
}
