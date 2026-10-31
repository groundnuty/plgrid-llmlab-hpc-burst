package frpsplugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

const (
	// proxyListPath is frps's list of tcpmux proxies, online and offline (frp v0.71.1,
	// server/api_router.go:27-49, server/http/controller.go:151-161).
	proxyListPath = "/api/proxy/tcpmux"
	// StatusOnline is the status of a proxy frps has registered. Every other value, "offline"
	// included, means the proxy is not connected (HBEP-6: offline entries stay in the list, so only
	// "online" counts).
	StatusOnline = "online"
	// DefaultReadTimeout bounds one read of the list.
	DefaultReadTimeout = 5 * time.Second
	// maxListBytes bounds the list's size: an Instance's entry is a few hundred bytes.
	maxListBytes = 16 << 20
)

// ReaderConfig says where and how to read frps's web API.
type ReaderConfig struct {
	// Addr is "127.0.0.1:<port>" of frps's web server (agentapi.Host.FrpsWebAddr).
	Addr string
	// User is the web server's user name (agentapi.FrpsWebUser).
	User string
	// PasswordFile holds the web server's password (agentapi.Host.FrpsWebPasswordFile). It is read
	// at every read of the list, so a password that was replaced is picked up.
	PasswordFile string
	// Timeout bounds one read; zero means DefaultReadTimeout.
	Timeout time.Duration
}

// Reader reads frps's proxy list.
type Reader struct {
	cfg    ReaderConfig
	client *http.Client
}

// NewReader returns a Reader for the configuration.
func NewReader(cfg ReaderConfig) (*Reader, error) {
	if cfg.Addr == "" || cfg.User == "" || cfg.PasswordFile == "" {
		return nil, errors.New("frps reader: the address, the user and the password file are all required")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = DefaultReadTimeout
	}
	if cfg.Timeout < 0 {
		return nil, fmt.Errorf("frps reader: timeout %s is negative", cfg.Timeout)
	}
	return &Reader{cfg: cfg, client: &http.Client{
		Timeout: cfg.Timeout,
		// No proxy from the environment: frps is on loopback, and a configured http_proxy would
		// carry the web password to it. No redirects, so the credentials go to one place only.
		Transport:     &http.Transport{Proxy: nil, DisableKeepAlives: true},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

// NewHostReader returns the Reader for the launcher's frps on the host: its web address, agentapi.FrpsWebUser
// and the web password file.
func NewHostReader(h agentapi.Host) (*Reader, error) {
	return NewReader(ReaderConfig{Addr: h.FrpsWebAddr(), User: agentapi.FrpsWebUser, PasswordFile: h.FrpsWebPasswordFile()})
}

// Proxy is one entry of frps's list (frp server/http/model/types.go:100-111), the fields the
// inventory uses (HBEP-6, Path state).
type Proxy struct {
	Name          string `json:"name"`
	Status        string `json:"status"`
	LastStartTime string `json:"lastStartTime"`
	LastCloseTime string `json:"lastCloseTime"`
}

// List reads the list of tcpmux proxies. Any failure is an error and no list: the password file
// cannot be read, frps cannot be reached, answers another status than 200, or answers something
// that is not the list. The error never contains the password.
func (r *Reader) List(ctx context.Context) ([]Proxy, error) {
	pw, err := agentapi.ReadKeyFile(r.cfg.PasswordFile)
	if err != nil {
		return nil, fmt.Errorf("the web password: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+r.cfg.Addr+proxyListPath, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(r.cfg.User, pw)
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("frps web API: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("frps web API answered %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxListBytes+1))
	if err != nil {
		return nil, fmt.Errorf("frps web API: %w", err)
	}
	if len(body) > maxListBytes {
		return nil, fmt.Errorf("frps web API: the list is longer than %d bytes", maxListBytes)
	}
	// "proxies" is a pointer so that an answer without the key (another page, another API) is
	// an error, not an empty list that would read as every Instance offline.
	var doc struct {
		Proxies *[]Proxy `json:"proxies"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("frps web API: the answer is not JSON: %w", err)
	}
	if doc.Proxies == nil {
		return nil, errors.New("frps web API: the answer has no proxies list")
	}
	return *doc.Proxies, nil
}

// Report is the path state for a set of Instances.
type Report struct {
	// Conditions holds one TunnelConnected condition per Instance UID asked for. Its type, status,
	// reason and message are set; the caller sets lastTransitionTime and observedGeneration, as
	// meta.SetStatusCondition does.
	Conditions map[string]metav1.Condition
	// Orphans are the names of the online proxies that are no asked-for Instance's UID, sorted
	// (HBEP-6: an online proxy whose name is no current Instance's UID gets the event
	// TunnelOrphaned). It is empty when the list could not be read.
	Orphans []string
	// Err is why the list could not be read, nil when it could.
	Err error
}

// Read reads the list and reports on the Instances with the given UIDs.
func (r *Reader) Read(ctx context.Context, uids []string) Report {
	list, err := r.List(ctx)
	return BuildReport(uids, list, err)
}

// BuildReport turns a read of frps's list into the Instances' TunnelConnected conditions (HBEP-6,
// Path state):
//   - the list could not be read (err): Unknown with ProxyListUnreadable, for every Instance, so the
//     condition is never left True on a list that was not read;
//   - frps lists the Instance's proxy as online: True with ProxyOnline;
//   - it lists it as anything else, or not at all: False with ProxyOffline.
func BuildReport(uids []string, list []Proxy, err error) Report {
	rep := Report{Conditions: make(map[string]metav1.Condition, len(uids)), Err: err}
	if err != nil {
		for _, uid := range uids {
			rep.Conditions[uid] = tunnelCondition(metav1.ConditionUnknown, v1alpha1.ReasonProxyListUnreadable,
				"frps's proxy list could not be read: "+err.Error())
		}
		return rep
	}
	status := make(map[string]string, len(list))
	for _, p := range list {
		status[p.Name] = p.Status
	}
	asked := make(map[string]struct{}, len(uids))
	for _, uid := range uids {
		asked[uid] = struct{}{}
		switch s, listed := status[uid]; {
		case listed && s == StatusOnline:
			rep.Conditions[uid] = tunnelCondition(metav1.ConditionTrue, v1alpha1.ReasonProxyOnline, "frps lists the Instance's proxy as online")
		case listed:
			rep.Conditions[uid] = tunnelCondition(metav1.ConditionFalse, v1alpha1.ReasonProxyOffline, fmt.Sprintf("frps lists the Instance's proxy as %q", s))
		default:
			rep.Conditions[uid] = tunnelCondition(metav1.ConditionFalse, v1alpha1.ReasonProxyOffline, "frps does not list a proxy for the Instance")
		}
	}
	for _, p := range list {
		if _, ok := asked[p.Name]; !ok && p.Status == StatusOnline {
			rep.Orphans = append(rep.Orphans, p.Name)
		}
	}
	sort.Strings(rep.Orphans)
	return rep
}

func tunnelCondition(s metav1.ConditionStatus, reason, msg string) metav1.Condition {
	return metav1.Condition{Type: v1alpha1.ConditionTunnelConnected, Status: s, Reason: reason, Message: msg}
}
