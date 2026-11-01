package lifecycle

// The readiness check "caches" on a real API server (HBEP-11, Readiness; #147): it passes only when
// the informer of every kind the controller watches has synced, and the list of those kinds is the
// one the controller registers. Both tests use #147's port block, 29540-29549, for the health
// addresses of the copies they start, and #61's envtest block for the control plane, as
// TestManagerOnControlPlane does; none runs in parallel with it.

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

const (
	cachesLeaderAddr  = "127.0.0.1:29540"
	cachesStandbyAddr = "127.0.0.1:29541"
	cachesDriftAddr   = "127.0.0.1:29542"
)

// cachesRig is a control plane with the framework's CRDs and the two namespaces the controller uses,
// and the directories its options need.
type cachesRig struct {
	ctx           context.Context
	cfg           *rest.Config
	cl            client.Client
	creds, builds string
}

func newCachesRig(t *testing.T) *cachesRig {
	t.Helper()
	if os.Getenv("HPCBURST_TESTENV_PORT_BASE") == "" {
		t.Setenv("HPCBURST_TESTENV_PORT_BASE", managerPortBase)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	t.Cleanup(cancel)
	cp, err := testenv.Start(ctx, t.TempDir(), testenv.Options{BeforeControllerManager: installTestCRDs})
	if err != nil {
		t.Fatalf("start the control plane: %v", err)
	}
	t.Cleanup(func() {
		if err := cp.Stop(); err != nil {
			t.Errorf("stop the control plane: %v", err)
		}
		if err := cp.AssertAllDead(); err != nil {
			t.Errorf("leftover process: %v", err)
		}
	})
	s, err := Scheme()
	if err != nil {
		t.Fatal(err)
	}
	cl, err := client.New(cp.Config, client.Options{Scheme: s})
	if err != nil {
		t.Fatal(err)
	}
	for _, ns := range []string{v1alpha1.ControllerNamespace, rigNS} {
		if err := cl.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
			t.Fatal(err)
		}
	}
	return &cachesRig{ctx: ctx, cfg: cp.Config, cl: cl, creds: t.TempDir(), builds: t.TempDir()}
}

// start runs one copy of the controller and returns its stop, which waits once for the manager to
// return and is also run at the test's end. Each copy has a registry of its own: two managers in
// one process cannot register the same collectors twice.
func (g *cachesRig) start(t *testing.T, name string, cfg *rest.Config, o Options) (stop func(), mgr manager.Manager) {
	t.Helper()
	o.Config, o.CredentialsDir, o.AgentBuildsDir = cfg, g.creds, g.builds
	o.FrontConfigured, o.Checkpoints, o.Registry = true, &stubSource{}, prometheus.NewRegistry()
	mgr, _, err := NewManager(o)
	if err != nil {
		t.Fatal(err)
	}
	mctx, cancel := context.WithCancel(g.ctx)
	done := make(chan error, 1)
	go func() { done <- mgr.Start(mctx) }()
	var once sync.Once
	stop = func() { once.Do(func() { stopManager(t, name, cancel, done) }) }
	t.Cleanup(stop)
	return stop, mgr
}

// readyzCodes returns the status codes of a copy's two readiness checks.
func readyzCodes(t *testing.T, addr string) (caches, leading int) {
	t.Helper()
	return getStatus(t, "http://"+addr+"/readyz/"+ReadyzCaches), getStatus(t, "http://"+addr+"/readyz/"+ReadyzLeading)
}

// holdState holds every GET of one resource, a list or a watch, until the gate closes, so that the
// informer of that kind cannot sync, and says when the first such request has come.
type holdState struct {
	suffix  string
	gate    <-chan struct{}
	arrived chan struct{}
	once    sync.Once
}

type holdTransport struct {
	next http.RoundTripper
	*holdState
}

func (h holdTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, h.suffix) {
		h.once.Do(func() { close(h.arrived) })
		select {
		case <-h.gate:
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
	}
	return h.next.RoundTrip(req)
}

// heldConfig is cfg with a round-tripper in front of the API server that holds the initial list of
// one resource, so the informer of that kind has not synced while the rest have.
func heldConfig(cfg *rest.Config, resource string, gate <-chan struct{}) (*rest.Config, <-chan struct{}) {
	h := &holdState{suffix: "/" + resource, gate: gate, arrived: make(chan struct{})}
	c := rest.CopyConfig(cfg)
	prev := c.WrapTransport
	c.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
		if prev != nil {
			rt = prev(rt)
		}
		return holdTransport{next: rt, holdState: h}
	}
	return c, h.arrived
}

// The six kinds whose informers "caches" waits for (HBEP-11): the resource name each is listed by.
var cachesKinds = []struct {
	name, resource string
	obj            client.Object
}{
	{"ModelDeployment", "modeldeployments", &v1alpha1.ModelDeployment{}},
	{"Allocation", "allocations", &v1alpha1.Allocation{}},
	{"Instance", "instances", &v1alpha1.Instance{}},
	{"Provider", "providers", &v1alpha1.Provider{}},
	{"PlatformProfile", "platformprofiles", &v1alpha1.PlatformProfile{}},
	{"ControllerRevision", "controllerrevisions", &appsv1.ControllerRevision{}},
}

// othersSynced says whether the informers of every kind but held have synced, read from the same
// shared cache the check reads (an informer asked for without waiting for its sync).
func othersSynced(ctx context.Context, mgr manager.Manager, held string) (bool, string) {
	for _, k := range cachesKinds {
		if k.name == held {
			continue
		}
		inf, err := mgr.GetCache().GetInformer(ctx, k.obj, cache.BlockUntilSynced(false))
		if err != nil {
			return false, k.name + ": " + err.Error()
		}
		if !inf.HasSynced() {
			return false, k.name + " has not synced"
		}
	}
	return true, ""
}

// cachesPollPeriod is client-go's pause between two looks at the informers' HasSynced
// (tools/cache, syncedPollPeriod): the check can lag an informer's sync by this much.
const cachesPollPeriod = 100 * time.Millisecond

// TestCachesNeedsEveryWatchedInformer is HBEP-11's two-copies case, once for each of the six kinds.
// A leader holds the Lease. A second copy, the standby, starts with the initial list of one kind
// held in front of the API server (a real informer that has not synced, the other five unaffected):
// its caches check fails and `hpcburst status` reads "starting". Once the list is released the
// standby's caches check passes, its leading check keeps failing while the leader holds the Lease,
// and status reads "not leading". The last case takes the leader down: the standby takes the Lease
// and is ready. The holds are lifted by the test, never by a window, so each state lasts as long as
// the test looks at it (five reads each).
func TestCachesNeedsEveryWatchedInformer(t *testing.T) {
	g := newCachesRig(t)
	stopLeader, _ := g.start(t, "leader", g.cfg, Options{HealthAddr: cachesLeaderAddr, LeaderElection: true})
	eventually(t, time.Minute, "the leader copy is ready", func() (bool, string) {
		st, p := launcher.ReadReadiness(cachesLeaderAddr)
		return st == launcher.ReadyzReady, fmt.Sprintf("state %q %v", st, p)
	})
	for _, k := range cachesKinds {
		t.Run(k.name+" held", func(t *testing.T) {
			gate := make(chan struct{})
			release := sync.OnceFunc(func() { close(gate) })
			defer release()
			cfg, arrived := heldConfig(g.cfg, k.resource, gate)
			stop, mgr := g.start(t, "standby, "+k.name+" held", cfg, Options{HealthAddr: cachesStandbyAddr, LeaderElection: true})
			defer stop()
			select {
			case <-arrived:
			case <-time.After(time.Minute):
				t.Fatalf("the standby never asked for %s", k.resource)
			}
			eventually(t, time.Minute, "the standby's health server answers", func() (bool, string) {
				return getStatus(t, "http://"+cachesStandbyAddr+"/healthz") == http.StatusOK, "no /healthz"
			})
			// The other five informers have synced: only the held one keeps the check failing. The
			// check lags the informers by up to one poll period, so the hold is looked at for ten of
			// them, and for at least five reads, after the others are seen synced. The hold lasts until
			// the test lifts it, so a longer look can only find a fault, never make one.
			eventually(t, time.Minute, "the other five informers have synced", func() (bool, string) {
				return othersSynced(g.ctx, mgr, k.name)
			})
			seen := time.Now()
			for i := 0; i < 5 || time.Since(seen) < 10*cachesPollPeriod; i++ {
				caches, leading := readyzCodes(t, cachesStandbyAddr)
				st, problems := launcher.ReadReadiness(cachesStandbyAddr)
				if caches != http.StatusInternalServerError || leading != http.StatusInternalServerError ||
					st != launcher.ReadyzStarting || len(problems) != 1 || !strings.Contains(problems[0], "starting: GET /readyz/caches") {
					t.Fatalf("with the %s informer held and the others synced: caches %d, leading %d, status %q %v; want 500, 500 and starting", k.name, caches, leading, st, problems)
				}
			}
			release()
			eventually(t, time.Minute, "the standby's caches pass once the held informer synced", func() (bool, string) {
				caches, _ := readyzCodes(t, cachesStandbyAddr)
				return caches == http.StatusOK, fmt.Sprintf("/readyz/caches %d", caches)
			})
			for i := 0; i < 5; i++ {
				caches, leading := readyzCodes(t, cachesStandbyAddr)
				st, problems := launcher.ReadReadiness(cachesStandbyAddr)
				if caches != http.StatusOK || leading != http.StatusInternalServerError ||
					st != launcher.ReadyzNotLeading || len(problems) != 1 || !strings.Contains(problems[0], "not leading: GET /readyz/leading") {
					t.Fatalf("standby after the %s informer synced: caches %d, leading %d, status %q %v; want 200, 500 and not leading", k.name, caches, leading, st, problems)
				}
			}
			if st, _ := launcher.ReadReadiness(cachesLeaderAddr); st != launcher.ReadyzReady {
				t.Errorf("the leader reads %q beside the standby, want ready", st)
			}
		})
	}
	t.Run("the standby takes over when the leader goes", func(t *testing.T) {
		stop, _ := g.start(t, "standby", g.cfg, Options{HealthAddr: cachesStandbyAddr, LeaderElection: true})
		defer stop()
		eventually(t, time.Minute, "the standby reads not leading", func() (bool, string) {
			st, p := launcher.ReadReadiness(cachesStandbyAddr)
			return st == launcher.ReadyzNotLeading, fmt.Sprintf("state %q %v", st, p)
		})
		stopLeader()
		eventually(t, 2*time.Minute, "the standby leads and reads ready", func() (bool, string) {
			st, p := launcher.ReadReadiness(cachesStandbyAddr)
			return st == launcher.ReadyzReady, fmt.Sprintf("state %q %v", st, p)
		})
	})
}

// recordingCache records the kinds asked of GetInformer, which is how a controller's sources (For,
// Owns, Watches) and the readiness check each start an informer.
type recordingCache struct {
	cache.Cache
	mu    sync.Mutex
	asked map[string]bool
}

func (c *recordingCache) GetInformer(ctx context.Context, obj client.Object, opts ...cache.InformerGetOption) (cache.Informer, error) {
	c.mu.Lock()
	c.asked[fmt.Sprintf("%T", obj)] = true
	c.mu.Unlock()
	return c.Cache.GetInformer(ctx, obj, opts...)
}

func (c *recordingCache) kinds() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.asked))
	for k := range c.asked {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func recordingNewCache(rec *recordingCache) cache.NewCacheFunc {
	return func(c *rest.Config, o cache.Options) (cache.Cache, error) {
		inner, err := cache.New(c, o)
		if err != nil {
			return nil, err
		}
		rec.Cache = inner
		return rec, nil
	}
}

// TestCachesListIsWhatTheControllerWatches: the kinds "caches" starts early are the kinds the
// controller's builders register (For, Owns, Watches), read from the running controller and not
// from a second list in the test. A first copy runs with another holder on the Lease, so its
// controller never starts and the only informers asked for are the readiness check's. A second runs
// with leader election off, so the controller starts as well (shown by its reconcile of a
// ModelDeployment) and its sources ask for theirs. The two sets must be the same: a kind that the
// controller watches and "caches" does not start fails here by name.
func TestCachesListIsWhatTheControllerWatches(t *testing.T) {
	g := newCachesRig(t)
	holder, seconds, now := "lifecycle-second-copy", int32(3600), metav1.NewMicroTime(time.Now())
	if err := g.cl.Create(g.ctx, &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.LeaseLifecycle, Namespace: v1alpha1.ControllerNamespace},
		Spec:       coordinationv1.LeaseSpec{HolderIdentity: &holder, LeaseDurationSeconds: &seconds, RenewTime: &now, AcquireTime: &now},
	}); err != nil {
		t.Fatal(err)
	}
	readinessOnly := &recordingCache{asked: map[string]bool{}}
	stop, _ := g.start(t, "readiness alone", g.cfg, Options{HealthAddr: cachesDriftAddr, LeaderElection: true, newCache: recordingNewCache(readinessOnly)})
	eventually(t, time.Minute, "the caches check passes with another holder on the Lease", func() (bool, string) {
		caches, leading := readyzCodes(t, cachesDriftAddr)
		return caches == http.StatusOK && leading == http.StatusInternalServerError, fmt.Sprintf("caches %d, leading %d", caches, leading)
	})
	early := readinessOnly.kinds()
	stop()
	if len(early) != len(cachesKinds) {
		t.Fatalf("the caches check starts %d informers %v, want the %d of HBEP-11", len(early), early, len(cachesKinds))
	}

	withController := &recordingCache{asked: map[string]bool{}}
	_, _ = g.start(t, "controller running", g.cfg, Options{HealthAddr: cachesDriftAddr, LeaderElection: false, newCache: recordingNewCache(withController)})
	md := mdSpec("md-drift")
	md.Generation = 0
	if err := g.cl.Create(g.ctx, md); err != nil {
		t.Fatal(err)
	}
	eventually(t, time.Minute, "the controller reconciles the ModelDeployment", func() (bool, string) {
		var got v1alpha1.ModelDeployment
		if err := g.cl.Get(g.ctx, client.ObjectKeyFromObject(md), &got); err != nil {
			return false, err.Error()
		}
		return len(got.Status.Conditions) > 0, "no condition yet"
	})
	registered := withController.kinds()
	var missing []string
	for _, k := range registered {
		if !slices.Contains(early, k) {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("the controller asks for informers of %v, which the caches check does not start (it starts %v)", missing, early)
	}
}
