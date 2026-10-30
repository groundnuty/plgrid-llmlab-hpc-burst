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

// portOwned reports whether a LISTEN socket on port is held by the process tree of pid.
func portOwned(pid, port int) (bool, error) {
	owned, _, err := ownedListeners([]int{pid})
	if err != nil {
		return false, err
	}
	for _, s := range owned {
		if s.Port == port {
			return true, nil
		}
	}
	return false, nil
}

// waitReady waits until the host the record names is ready: /readyz answers 200 on the
// apiserver, and /healthz answers 200 on the embedded controller manager's port. It is tied to
// the launched process at every step. The process must still be the recorded one at every
// probe, and the probed port must be held by that process, before and after the answer. So a
// 200 from some other server on the port (a system k3s, a leftover) counts for nothing.
func waitReady(ctx context.Context, l layout, st *State, timeout time.Duration) (readyzAt, readyAt time.Duration, err error) {
	t0 := st.StartedAt
	rec := st.Procs[0]
	deadline := time.Now().Add(timeout)
	stage := "apiserver /readyz"
	var last error
	var cfg *rest.Config
	readyzDone := false
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return 0, 0, ctx.Err()
		}
		if v, d := identify(rec); v != Ours {
			return 0, 0, fmt.Errorf("k3s is no longer running before it was ready (%s); see %s", d, l.logFile())
		}
		if !readyzDone {
			if cfg == nil {
				if _, serr := os.Stat(l.adminKubeconfig()); serr == nil {
					c, cerr := restConfig(l.adminKubeconfig(), st.Ports, 1500*time.Millisecond)
					if cerr != nil {
						return 0, 0, cerr
					}
					cfg = c
				}
			}
			if cfg != nil {
				if own, oerr := portOwned(rec.PID, st.Ports.HTTPS); oerr != nil {
					last = oerr
				} else if !own {
					last = fmt.Errorf("port %d is not held by the launched k3s yet", st.Ports.HTTPS)
				} else if rerr := readyz(cfg); rerr != nil {
					last = rerr
				} else if own2, _ := portOwned(rec.PID, st.Ports.HTTPS); !own2 {
					last = fmt.Errorf("/readyz answered, but port %d is not held by the launched k3s", st.Ports.HTTPS)
				} else {
					readyzDone, readyzAt, stage, last = true, time.Since(t0), "controller manager /healthz", nil
				}
			}
		} else {
			if own, oerr := portOwned(rec.PID, st.Ports.KCM); oerr != nil {
				last = oerr
			} else if !own {
				last = fmt.Errorf("port %d is not held by the launched k3s yet", st.Ports.KCM)
			} else if herr := kcmHealthz(st.Ports.KCM, 1500*time.Millisecond); herr != nil {
				last = herr
			} else if own2, _ := portOwned(rec.PID, st.Ports.KCM); !own2 {
				last = fmt.Errorf("/healthz answered, but port %d is not held by the launched k3s", st.Ports.KCM)
			} else {
				readyAt = time.Since(t0)
				if probs := checkHostListeners(st); len(probs) > 0 {
					return 0, 0, fmt.Errorf("k3s is ready but its listeners are wrong: %v", probs)
				}
				return readyzAt, readyAt, nil
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return 0, 0, fmt.Errorf("%s not ready after %s: %v; see %s", stage, timeout, last, l.logFile())
}

// checkHostListeners checks the recorded process tree holds exactly the expected ports, on loopback.
func checkHostListeners(st *State) []string {
	pids := make([]int, 0, len(st.Procs))
	for _, p := range st.Procs {
		pids = append(pids, p.PID)
	}
	owned, others, err := ownedListeners(pids)
	if err != nil {
		return []string{err.Error()}
	}
	return verifyListeners(st.Ports.Expected(), owned, others)
}
