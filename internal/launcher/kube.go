package launcher

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// restConfig reads the launcher's kubeconfig and checks that it points at this host: loopback,
// and the HTTPS port the launcher gave k3s. A kubeconfig that names anything else (the system
// k3s on 6443, say) must never be used to judge or change this host.
func restConfig(path string, ports Ports, timeout time.Duration) (*rest.Config, error) {
	cfg, err := clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	u, err := url.Parse(cfg.Host)
	if err != nil {
		return nil, fmt.Errorf("%s: bad server address %q: %w", path, cfg.Host, err)
	}
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil || port != strconv.Itoa(ports.HTTPS) || (host != "127.0.0.1" && host != "localhost" && host != "::1") {
		return nil, fmt.Errorf("%s points at %s, not at this launcher's 127.0.0.1:%d", path, cfg.Host, ports.HTTPS)
	}
	cfg.Timeout = timeout
	cfg.QPS, cfg.Burst = 50, 100
	cfg.WarningHandler = rest.NoWarnings{} // API deprecation notices are not the launcher's to print
	return cfg, nil
}

// getStatus does a GET and returns the status code; the body is discarded.
func getStatus(hc *http.Client, rawURL string) (int, error) {
	resp, err := hc.Get(rawURL)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}

// readyz asks the apiserver for /readyz with the launcher's credentials.
func readyz(cfg *rest.Config) error {
	hc, err := rest.HTTPClientFor(cfg)
	if err != nil {
		return err
	}
	code, err := getStatus(hc, cfg.Host+"/readyz")
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
// launched process.
func kcmHealthz(port int, timeout time.Duration) error {
	hc := &http.Client{Timeout: timeout, Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}} //nolint:gosec // loopback, self-signed; see above
	code, err := getStatus(hc, fmt.Sprintf("https://127.0.0.1:%d/healthz", port))
	if err != nil {
		return err
	}
	if code != http.StatusOK {
		return fmt.Errorf("/healthz answered %d", code)
	}
	return nil
}

// waitReady waits until the host the record names is ready: /readyz answers 200 on the
// apiserver, and /healthz answers 200 on the embedded controller manager's port. It is tied to
// the launched process at every step through h, the pidfd taken at launch. The process must still
// be the recorded one at every probe, and the probed port must be held by that process, before
// and after the answer. So a 200 from some other server on the port (a system k3s, a leftover)
// counts for nothing. Before it returns ready, confirmHost checks the whole host once more.
func waitReady(ctx context.Context, l layout, st *State, h *procHandle, timeout time.Duration) (readyzAt, readyAt time.Duration, err error) {
	t0 := st.StartedAt
	deadline := time.Now().Add(timeout)
	stage := "apiserver /readyz"
	var last error
	var cfg *rest.Config
	readyzDone := false
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return 0, 0, ctx.Err()
		}
		if v, d := h.verify(); v != Ours {
			return 0, 0, fmt.Errorf("k3s is no longer running before it was ready (%s); see %s", d, l.logFile())
		}
		if !readyzDone {
			if cfg == nil {
				if _, serr := os.Stat(l.adminKubeconfig()); serr == nil {
					// k3s creates this file early in its start and fills it in afterwards, not
					// atomically, so a read can find it empty or cut short (a slow filesystem makes
					// that common). A file that does not parse or check out yet is not ready yet:
					// remember why, and read it again. cfg is set only from a whole, checked file,
					// so a kubeconfig that names another server is never used, only waited out.
					if c, cerr := restConfig(l.adminKubeconfig(), st.Ports, 1500*time.Millisecond); cerr != nil {
						last = cerr
					} else {
						cfg = c
					}
				}
			}
			if cfg != nil {
				if own, oerr := portOwned(h, st.Ports.HTTPS); oerr != nil {
					last = oerr
				} else if !own {
					last = fmt.Errorf("port %d is not held by the launched k3s yet", st.Ports.HTTPS)
				} else if rerr := readyz(cfg); rerr != nil {
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
			} else if herr := kcmHealthz(st.Ports.KCM, 1500*time.Millisecond); herr != nil {
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
		time.Sleep(20 * time.Millisecond)
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
