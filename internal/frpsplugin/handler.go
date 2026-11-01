package frpsplugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The operations frps posts to the plugin (frp v0.71.1, pkg/plugin/server/plugin.go:24-29). The
// launcher subscribes to these three only (internal/launcher/frps.go, frpsPluginOps); Ping is left out
// on purpose (`@hpc-burst-science-agent`, 31 Oct 2026, HBEP-6): a failed Ping call makes frpc close
// its session. Anything else that reaches the handler is refused.
const (
	OpLogin      = "Login"
	OpNewProxy   = "NewProxy"
	OpCloseProxy = "CloseProxy"
)

const (
	// DefaultDeadline is how long the handler takes over a call, from the moment it starts, for
	// reading the body and for the Index, before it refuses. HBEP-6 wants every answer within
	// 1 s, because frps calls NewProxy on the session's own read loop with no timeout on its HTTP
	// client. The budget leaves the rest of the second to the HTTP exchange.
	DefaultDeadline = 700 * time.Millisecond
	// DefaultMaxInFlight bounds the calls being handled at once, from the start of the body read.
	// A call that does not return (an Index that blocks) keeps its slot, so a stuck Index fills
	// the slots, and then every call is refused at once instead of piling up goroutines.
	DefaultMaxInFlight = 128
	// maxBody is the most a request body may hold: frps's messages are a few hundred bytes.
	maxBody = 64 << 10
	// proxyTypeTCPMux and multiplexerHTTPConnect are what an Instance's proxy is (HBEP-6, frpc in
	// the job).
	proxyTypeTCPMux        = "tcpmux"
	multiplexerHTTPConnect = "httpconnect"
	// metaToken is the key of frpc's metadatas that holds the Allocation's credential.
	metaToken = "token"
)

// Reject reasons. They name the rule that failed and never contain a token (HBEP-6: they reach
// frpc and frps's log).
const (
	reasonNotSynced   = "the plugin has not loaded the Allocations yet; retry"
	reasonCredential  = "the login metadata token is missing or is not the credential of a live Allocation"
	reasonMalformed   = "the plugin request is malformed"
	reasonProxyType   = "proxy_type must be tcpmux and multiplexer must be httpconnect"
	reasonDomains     = "custom_domains must hold exactly one entry, equal to proxy_name"
	reasonNotPlaced   = "proxy_name is not the UID of an Instance placed on this Allocation"
	reasonExtraFields = "subdomain, group, http_user and route_by_http_user must be empty"
	reasonSlowBody    = "the request body did not arrive in time; retry"
	reasonBusy        = "the plugin is busy; retry"
	reasonInternal    = "the plugin failed internally; retry"
)

// Options configures a Handler.
type Options struct {
	// Deadline is the whole budget of a call, reading its body included; zero means
	// DefaultDeadline. It must be shorter than one second (HBEP-6).
	Deadline time.Duration
	// MaxInFlight bounds the calls being handled at once; zero means DefaultMaxInFlight.
	MaxInFlight int
	// Notifier hears accepted NewProxy and CloseProxy calls; nil means nobody does.
	Notifier Notifier
}

// Handler is frps's HTTP server plugin. It serves agentapi.FrpsPluginPath, and answers 404 anywhere
// else. Mount it on agentapi.Host.FrpsPluginAddr, on a server with ReadHeaderTimeout and
// WriteTimeout set: the handler's deadline starts once the headers are read.
type Handler struct {
	idx      Index
	notify   Notifier
	deadline time.Duration
	slots    chan struct{}
}

var _ http.Handler = (*Handler)(nil)

// NewHandler returns the plugin handler for the Index.
func NewHandler(idx Index, o Options) (*Handler, error) {
	if idx == nil {
		return nil, errors.New("frps plugin: no Index")
	}
	if o.Deadline == 0 {
		o.Deadline = DefaultDeadline
	}
	if o.Deadline < 0 || o.Deadline >= time.Second {
		return nil, fmt.Errorf("frps plugin: deadline %s is not in (0, 1s): HBEP-6 wants every answer within 1 s", o.Deadline)
	}
	if o.MaxInFlight == 0 {
		o.MaxInFlight = DefaultMaxInFlight
	}
	if o.MaxInFlight < 0 {
		return nil, fmt.Errorf("frps plugin: MaxInFlight %d is negative", o.MaxInFlight)
	}
	return &Handler{idx: idx, notify: o.Notifier, deadline: o.Deadline, slots: make(chan struct{}, o.MaxInFlight)}, nil
}

// request is frps's call (frp pkg/plugin/server/types.go:21-25): the operation in the body, and
// again in the query.
type request struct {
	Version string          `json:"version"`
	Op      string          `json:"op"`
	Content json.RawMessage `json:"content"`
}

// response is frps's expected answer (types.go:27-32). unchange is always true: the plugin never
// edits content.
type response struct {
	Reject       bool   `json:"reject"`
	RejectReason string `json:"reject_reason,omitempty"`
	Unchange     bool   `json:"unchange"`
}

// userInfo is the session's login as frps keeps it (types.go:40-44); its metas are frpc's
// metadatas from the Login message, which no later message can change.
type userInfo struct {
	Metas map[string]string `json:"metas"`
}

// loginContent is the part of the Login content the plugin reads (frp pkg/msg/msg.go:76-93).
type loginContent struct {
	Metas map[string]string `json:"metas"`
}

// newProxyContent is the part of the NewProxy content the plugin reads (types.go:46-49 and
// pkg/msg/msg.go:102-134). The proxy's own "metas" key is the proxy's metadata, which frpc
// chooses per proxy, and is deliberately not read: the credential is the session's, in user.metas.
type newProxyContent struct {
	User            userInfo `json:"user"`
	ProxyName       string   `json:"proxy_name"`
	ProxyType       string   `json:"proxy_type"`
	CustomDomains   []string `json:"custom_domains"`
	SubDomain       string   `json:"subdomain"`
	Group           string   `json:"group"`
	HTTPUser        string   `json:"http_user"`
	RouteByHTTPUser string   `json:"route_by_http_user"`
	Multiplexer     string   `json:"multiplexer"`
}

// closeProxyContent is the CloseProxy content (types.go:51-54, msg.go:142-144).
type closeProxyContent struct {
	User      userInfo `json:"user"`
	ProxyName string   `json:"proxy_name"`
}

type verdict struct {
	reject bool
	reason string
}

func accept() verdict           { return verdict{} }
func refuse(why string) verdict { return verdict{reject: true, reason: why} }

// ServeHTTP answers a plugin call. Every call frps can make gets HTTP 200 with a verdict, a refusal
// included, so that the reason reaches frpc; a call that is not one of frps's gets an error status.
//
// One absolute deadline, from the moment the handler starts, covers everything after the request's
// headers: taking a slot, reading the body, and the decision from the Index. The headers are read
// before the handler runs, so the server mounting it sets ReadHeaderTimeout. A call that has not been
// decided by the deadline (a body that stalls, an Index that blocks) is a refusal, and a call that
// finds every slot taken is a refusal at once. The slot is taken before the body is read, so a
// flood of stalled bodies counts against the cap like any other call.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != agentapi.FrpsPluginPath {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	deadline := time.Now().Add(h.deadline)
	// The body comes off the connection, and a client can stall it. This bounds the read at the
	// connection, which also lets the server finish the request afterwards. A ResponseWriter that
	// has no connection (a test recorder) reports ErrNotSupported, and the race in guarded still
	// answers in time.
	_ = http.NewResponseController(w).SetReadDeadline(deadline)
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	defer cancel()
	var op atomic.Value // the call's op, once the body has been read
	v := h.guarded(ctx, func(ctx context.Context) verdict { return h.handle(ctx, r, &op) })
	// CloseProxy is a notification: frps ignores the answer's content and logs a rejection only. It
	// is accepted whatever the Index or the Notifier does, unless it was malformed.
	if got, _ := op.Load().(string); got == OpCloseProxy && v.reject && v.reason != reasonMalformed {
		v = accept()
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response{Reject: v.reject, RejectReason: v.reason, Unchange: true})
}

// guarded runs fn in its own goroutine and answers with its verdict, or with a refusal when fn has
// not returned by the context's deadline, when it panics, or when no slot is free. Because fn runs
// apart from the caller, one that blocks without looking at its context does not hold the answer;
// the goroutine keeps its slot until fn returns.
func (h *Handler) guarded(ctx context.Context, fn func(context.Context) verdict) verdict {
	select {
	case h.slots <- struct{}{}:
	default:
		return refuse(reasonBusy)
	}
	out := make(chan verdict, 1)
	go func() {
		v := func() (v verdict) {
			defer func() {
				if recover() != nil {
					v = refuse(reasonInternal)
				}
			}()
			return fn(ctx)
		}()
		// The slot is free before the answer is handed over, so a caller that has the answer
		// finds it free.
		<-h.slots
		out <- v
	}()
	select {
	case v := <-out:
		return v
	case <-ctx.Done():
		return refuse(fmt.Sprintf("the plugin could not decide within %s; retry", h.deadline))
	}
}

// handle reads the call and decides it. It runs inside guarded.
func (h *Handler) handle(ctx context.Context, r *http.Request, op *atomic.Value) verdict {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody+1))
	switch {
	case errors.Is(err, os.ErrDeadlineExceeded):
		return refuse(reasonSlowBody)
	case err != nil || len(body) > maxBody:
		return refuse(reasonMalformed)
	}
	var req request
	if err := json.Unmarshal(body, &req); err != nil {
		return refuse(reasonMalformed)
	}
	// frps puts the operation in the body and in the query; a call where they differ is not frps's.
	if q := r.URL.Query().Get("op"); q != "" && q != req.Op {
		return refuse(reasonMalformed)
	}
	op.Store(req.Op)
	switch req.Op {
	case OpLogin:
		return h.login(ctx, req.Content)
	case OpNewProxy:
		return h.newProxy(ctx, req.Content)
	case OpCloseProxy:
		return h.closeProxy(req.Content)
	}
	return refuse(fmt.Sprintf("op %q is not served by this plugin", req.Op))
}

// allocation returns the Allocation whose credential the session's metas carry, or the verdict that
// refuses. It runs inside decide, so Synced and the lookup are covered by the deadline.
func (h *Handler) allocation(ctx context.Context, metas map[string]string) (string, verdict) {
	if !h.idx.Synced() {
		return "", refuse(reasonNotSynced)
	}
	tok := metas[metaToken]
	// A token that is not a key's form cannot be any Allocation's credential; saying so up
	// front also keeps garbage out of the Index.
	if agentapi.CheckKey([]byte(tok)) != nil {
		return "", refuse(reasonCredential)
	}
	uid, ok := h.idx.AllocationForToken(ctx, tok)
	if !ok {
		return "", refuse(reasonCredential)
	}
	return uid, accept()
}

// login accepts only a token that is a live Allocation's credential (HBEP-6, Login).
func (h *Handler) login(ctx context.Context, raw json.RawMessage) verdict {
	var c loginContent
	if err := json.Unmarshal(raw, &c); err != nil {
		return refuse(reasonMalformed)
	}
	_, v := h.allocation(ctx, c.Metas)
	return v
}

// newProxy applies HBEP-6's NewProxy rules. It rechecks the session's token on every call and keeps
// no session state.
func (h *Handler) newProxy(ctx context.Context, raw json.RawMessage) verdict {
	var c newProxyContent
	if err := json.Unmarshal(raw, &c); err != nil {
		return refuse(reasonMalformed)
	}
	alloc, v := h.allocation(ctx, c.User.Metas)
	if v.reject {
		return v
	}
	if c.ProxyType != proxyTypeTCPMux || c.Multiplexer != multiplexerHTTPConnect {
		return refuse(reasonProxyType)
	}
	if len(c.CustomDomains) != 1 || c.CustomDomains[0] != c.ProxyName {
		return refuse(reasonDomains)
	}
	if c.ProxyName == "" || !h.idx.InstanceOnAllocation(ctx, alloc, c.ProxyName) {
		return refuse(reasonNotPlaced)
	}
	if c.SubDomain != "" || c.Group != "" || c.HTTPUser != "" || c.RouteByHTTPUser != "" {
		return refuse(reasonExtraFields)
	}
	h.tell(OpNewProxy, c.ProxyName, alloc)
	return accept()
}

// closeProxy takes the call as a reason to re-read frps's list.
func (h *Handler) closeProxy(raw json.RawMessage) verdict {
	var c closeProxyContent
	if err := json.Unmarshal(raw, &c); err != nil {
		return refuse(reasonMalformed)
	}
	h.tell(OpCloseProxy, c.ProxyName, "")
	return accept()
}

func (h *Handler) tell(op, name, allocationUID string) {
	if h.notify != nil {
		h.notify.ProxyChanged(op, name, allocationUID)
	}
}
