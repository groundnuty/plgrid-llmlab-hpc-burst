// Package rig is spike S18's loopback stand: the real front (internal/front's Proxy and Router), N
// Instances of one served ModelDeployment, each an engine double behind the spike's own Transports, a
// controller emulation that takes a dead Instance out of routing and back, and the client. It holds no
// retry policy of its own: arm (a) is the front as it is, and arms (b)-(d) are the retry layer
// (layer.go) at the tunnel's position.
package rig

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/inventory"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Model is the served model's name: the deployment's spec.model and the doubles' served name.
const Model = "s18/model"

// Instance is one of the deployment's Instances.
type Instance struct {
	UID    types.UID
	Name   string
	Key    string // the front's name for it in pauses: namespace/name
	APIKey string // the Instance key: the front sends it, the double checks it
}

// Front is the real front, built as internal/front's own tests build it (harness_test.go): a hand-built
// inventory snapshot fed to the Router as the watch would, and the Proxy served on a kernel-chosen
// loopback port.
type Front struct {
	URL       string
	ClientKey string
	Router    *front.Router
	Instances []Instance

	mu   sync.Mutex
	snap inventory.Snapshot
	srv  *http.Server
	sink *usage.Sink
	// Logs is the front's own log, for a failure's message.
	Logs *syncWriter
}

// NewFront starts the front over n Instances of one deployment, reached through trans. edit, when set,
// changes the front's configuration. Every Instance starts ready; SetReady takes one out and back.
func NewFront(dir string, n int, trans front.Transports, edit func(*front.Config)) (*Front, error) {
	f := &Front{ClientKey: "s18-client-key-0001", Logs: &syncWriter{}}
	keys := filepath.Join(dir, "client-keys.json")
	if err := os.WriteFile(keys, []byte(fmt.Sprintf(`{"keys":[{"id":"s18","key":%q}]}`, f.ClientKey)), 0o600); err != nil {
		return nil, err
	}
	cfg := front.Config{DialHomeListen: "127.0.0.1:0", ClientKeysFile: keys, UsageDir: filepath.Join(dir, "usage")}.WithDefaults()
	if edit != nil {
		edit(&cfg)
	}
	ck, err := front.ReadClientKeys(keys)
	if err != nil {
		return nil, err
	}
	if f.sink, err = usage.OpenOptions(cfg.UsageDir, usage.Options{Now: time.Now}); err != nil {
		return nil, err
	}
	host := agentapi.Host{StateDir: filepath.Join(dir, "state")}
	log := slog.New(slog.NewTextHandler(f.Logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	clk := clock.Real{}
	f.Router = front.NewRouter(cfg, clk, trans, &front.MemoryRecorder{}, log, nil)
	proxy := front.NewProxy(front.ProxyOptions{Config: cfg, Host: host, Clock: clk, Router: f.Router, Transports: trans, Keys: ck, Sink: f.sink, Log: log})

	f.snap.Profiles = []v1alpha1.PlatformProfile{{ObjectMeta: metav1.ObjectMeta{Name: "s18-profile", ResourceVersion: "1"}, Spec: v1alpha1.PlatformProfileSpec{ProviderKind: v1alpha1.PlatformBatch}}}
	f.snap.Providers = []v1alpha1.Provider{{
		ObjectMeta: metav1.ObjectMeta{Name: "s18-provider", Labels: map[string]string{v1alpha1.LabelPlatform: "helios", v1alpha1.LabelOrganization: "cyfronet"}},
		Spec:       v1alpha1.ProviderSpec{PlatformProfileRef: v1alpha1.ObjectReference{Name: "s18-profile"}},
	}}
	mdUID := types.UID("00000000-0000-4000-8000-000000000001")
	f.snap.Deployments = []v1alpha1.ModelDeployment{{
		ObjectMeta: metav1.ObjectMeta{Namespace: "s18", Name: "md", UID: mdUID},
		Spec: v1alpha1.ModelDeploymentSpec{Model: Model, Instances: int32(n), Providers: v1alpha1.ProviderSelection{Names: []string{"s18-provider"}},
			Source: v1alpha1.ModelSource{Served: &v1alpha1.ServingSpec{}}},
	}}
	leaseEnd := metav1.NewTime(time.Now().Add(48 * time.Hour))
	for i := 0; i < n; i++ {
		uid := types.UID(fmt.Sprintf("00000000-0000-4000-8000-%012d", 100+i))
		name := fmt.Sprintf("inst-%d", i)
		apiKey, err := agentapi.NewKey()
		if err != nil {
			return nil, err
		}
		if err := agentapi.WriteKeyFile(agentapi.InstanceKeyPath(host.CredentialsDir(), string(uid)), apiKey); err != nil {
			return nil, err
		}
		f.Instances = append(f.Instances, Instance{UID: uid, Name: name, Key: "s18/" + name, APIKey: apiKey})
		f.snap.Instances = append(f.snap.Instances, v1alpha1.Instance{
			ObjectMeta: metav1.ObjectMeta{Namespace: "s18", Name: name, UID: uid,
				Labels: map[string]string{v1alpha1.LabelProvider: "s18-provider", v1alpha1.LabelModelDeployment: "md"},
				OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "ModelDeployment", Name: "md", UID: mdUID,
					Controller: ptr(true)}}},
			Spec:   v1alpha1.InstanceSpec{DeploymentID: v1alpha1.FullDeploymentID(fmt.Sprintf("%064x", 0x518000+i)), Launch: v1alpha1.Launch{MaxNumSeqs: 256}},
			Status: v1alpha1.InstanceStatus{Phase: v1alpha1.InstanceReady, LeaseEnd: &leaseEnd},
		})
	}
	f.apply()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	f.srv = &http.Server{Handler: proxy, ReadHeaderTimeout: time.Minute, BaseContext: func(net.Listener) context.Context { return context.Background() }}
	go func() { _ = f.srv.Serve(ln) }()
	f.URL = "http://" + ln.Addr().String()
	return f, nil
}

// SetReady sets Instance i's phase ready or not, as the controller does when its worker dies and again
// when it is restarted and verified, and hands the snapshot to the router, as the watch would.
func (f *Front) SetReady(i int, ready bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	phase := v1alpha1.InstanceReady
	if !ready {
		phase = v1alpha1.InstanceVerifying
	}
	f.snap.Instances[i].Status.Phase = phase
	f.Router.Update(f.snap)
}

func (f *Front) apply() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Router.Update(f.snap)
}

// Close stops the front's server and closes its usage sink.
func (f *Front) Close() {
	_ = f.srv.Close()
	_ = f.sink.Close()
}

func ptr[T any](v T) *T { return &v }

// syncWriter is an io.Writer safe for the front's goroutines, keeping what it is given.
type syncWriter struct {
	mu sync.Mutex
	b  []byte
}

func (w *syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.b = append(w.b, p...)
	return len(p), nil
}

func (w *syncWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return string(w.b)
}

var _ io.Writer = (*syncWriter)(nil)
