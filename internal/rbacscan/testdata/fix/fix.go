// Package fix is the fixture of internal/rbacscan's tests: one function for each way a call reaches an API server that the
// scan has to read, with a comment that says which rights it needs and for whom. It is never built into a binary and
// holds no logic. Two managers (newA and newB) stand for two identities, a clientset (newTyped) for a third, and an
// administrator's (newAdmin) for none.
package fix

import (
	"context"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// newA and newB make the managers of the identities "a" and "b".
func newA(cfg *rest.Config) (manager.Manager, error) { return ctrl.NewManager(cfg, ctrl.Options{}) }
func newB(cfg *rest.Config) (manager.Manager, error) { return ctrl.NewManager(cfg, ctrl.Options{}) }

// newTyped makes the clientset of the identity "c", and newAdmin one that is no identity's.
func newTyped(cfg *rest.Config) kubernetes.Interface { return kubernetes.NewForConfigOrDie(cfg) }
func newAdmin(cfg *rest.Config) kubernetes.Interface { return kubernetes.NewForConfigOrDie(cfg) }

// newDynamic makes the dynamic client of the identity "c" too.
func newDynamic(cfg *rest.Config) dynamic.Interface { return dynamic.NewForConfigOrDie(cfg) }

// reconciler holds the cached client and the API reader of one manager, as the controllers' structs do.
type reconciler struct {
	Client client.Client
	Reader client.Reader
}

// wrapped embeds a manager, as the module's own wrapper of a manager does, so that a helper both identities call hands
// each back its own.
type wrapped struct{ manager.Manager }

func wrap(m manager.Manager) manager.Manager { return &wrapped{Manager: m} }

// clients is the one helper both managers go through: each gets back the clients of its own manager, through the wrapper.
func clients(mgr manager.Manager) (client.Client, client.Reader) {
	m := wrap(mgr)
	return m.GetClient(), m.GetAPIReader()
}

// other is b's struct: the scan follows a field of a struct type to everything stored into it anywhere, so two identities
// do not share one.
type other struct {
	Reader client.Reader
}

func buildA(mgr manager.Manager) *reconciler {
	c, r := clients(mgr)
	return &reconciler{Client: c, Reader: r}
}

func buildB(mgr manager.Manager) *other {
	_, r := clients(mgr)
	return &other{Reader: r}
}

// readerGet is b's: a get through the API reader (b: get allocations).
func (o *other) readerGet(ctx context.Context) {
	var a v1alpha1.Allocation
	_ = o.Reader.Get(ctx, key, &a)
}

var (
	key           = client.ObjectKey{Namespace: "example", Name: "example"}
	listOptions   = metav1.ListOptions{}
	updateOptions = metav1.UpdateOptions{}
	createOptions = metav1.CreateOptions{}
	getOptions    = metav1.GetOptions{}
	patchOptions  = metav1.PatchOptions{}
)

// cacheBypass uses Cache.DisableFor, which the scan does not model: a problem.
var cacheBypass = client.CacheOptions{DisableFor: []client.Object{&v1alpha1.Provider{}}}

// cachedGet: a read through the cached client is a list and a watch of the kind (a: list watch providers).
func (r *reconciler) cachedGet(ctx context.Context) {
	var p v1alpha1.Provider
	_ = r.Client.Get(ctx, key, &p)
}

// readerGet: a read through the API reader is a get (a: get allocations).
func (r *reconciler) readerGet(ctx context.Context) {
	var a v1alpha1.Allocation
	_ = r.Reader.Get(ctx, key, &a)
}

// readerList: a list through the API reader is a list (a: list instances).
func (r *reconciler) readerList(ctx context.Context) {
	var l v1alpha1.InstanceList
	_ = r.Reader.List(ctx, &l)
}

// cachedList: a list through the cached client is a list and a watch (a: list watch modeldeployments).
func (r *reconciler) cachedList(ctx context.Context) {
	var l v1alpha1.ModelDeploymentList
	_ = r.Client.List(ctx, &l)
}

// writes: each write is its own verb, on its own resource (a: create allocations, update instances, delete providers,
// deletecollection platformprofiles, patch modeldeployments).
func (r *reconciler) writes(ctx context.Context) {
	_ = r.Client.Create(ctx, &v1alpha1.Allocation{})
	_ = r.Client.Update(ctx, &v1alpha1.Instance{})
	_ = r.Client.Delete(ctx, &v1alpha1.Provider{})
	_ = r.Client.DeleteAllOf(ctx, &v1alpha1.PlatformProfile{})
	_ = r.Client.Patch(ctx, &v1alpha1.ModelDeployment{}, client.MergeFrom(&v1alpha1.ModelDeployment{}))
}

// statusUpdate: through Status() the resource is its status (a: update instances/status), and through SubResource(name)
// the named subresource (a: update providers/scale).
func (r *reconciler) statusUpdate(ctx context.Context) {
	_ = r.Client.Status().Update(ctx, &v1alpha1.Instance{})
	_ = r.Client.SubResource("scale").Update(ctx, &v1alpha1.Provider{})
}

// generic takes any object, from a slice: its resource is not static, so it fails the scan unless the call is annotated
// (the call below is not).
func (r *reconciler) generic(ctx context.Context, o client.Object) {
	_ = r.Client.Update(ctx, o)
}

// rangeLiteral deletes each object of a slice literal: the scan follows the elements (a: delete instances and approvals).
func (r *reconciler) rangeLiteral(ctx context.Context) {
	for _, o := range []client.Object{&v1alpha1.Instance{}, &v1alpha1.Approval{}} {
		_ = r.Client.Delete(ctx, o)
	}
}

// subresourceGet reads a subresource through the cached client: it has no informer, so it is a get (a: get providers/scale).
func (r *reconciler) subresourceGet(ctx context.Context) {
	_ = r.Client.SubResource("scale").Get(ctx, &v1alpha1.Provider{}, &v1alpha1.Provider{})
}

// localCache and localReader are interfaces the module declares for what a library's cache and reader already have: a call
// through one is the library's (a: list watch providers, get platformprofiles).
type localCache interface {
	GetInformer(ctx context.Context, obj client.Object, opts ...cache.InformerGetOption) (cache.Informer, error)
}

type localReader interface {
	Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error
}

func viaLocalInterfaces(ctx context.Context, mgr manager.Manager) {
	var c localCache = mgr.GetCache()
	_, _ = c.GetInformer(ctx, &v1alpha1.Provider{})
	var r localReader = mgr.GetAPIReader()
	_ = r.Get(ctx, key, &v1alpha1.PlatformProfile{})
}

// followed takes any object too, but every caller passes a value of a type the scan can follow to (a: update instances).
func (r *reconciler) followed(ctx context.Context, o client.Object) {
	_ = r.Client.Update(ctx, o)
}

// annotated is generic with the annotation (a: update approvals and approvalpolicies).
func (r *reconciler) annotated(ctx context.Context, o client.Object) {
	//rbac:resources hpcburst.groundnuty.github.io/approvals hpcburst.groundnuty.github.io/approvalpolicies
	_ = r.Client.Update(ctx, o)
}

// converted passes an interface the scan can follow to its types (a: update operations).
func (r *reconciler) converted(ctx context.Context) {
	var o client.Object = &v1alpha1.Operation{}
	_ = r.Client.Update(ctx, o)
}

// staleNote has an annotation its call does not need, because the object's type is static (a: delete allocations).
func (r *reconciler) staleNote(ctx context.Context) {
	//rbac:resources hpcburst.groundnuty.github.io/allocations
	_ = r.Client.Delete(ctx, &v1alpha1.Allocation{})
}

// rawRequest builds a request by hand, whose path the scan cannot read.
func rawRequest(ctx context.Context, c rest.Interface) {
	_ = c.Get().AbsPath("/api").Do(ctx)
}

// embedsClient embeds a whole client.Client and is used as a client.Reader: the Get it is used for is the embedded client's.
type embedsClient struct{ client.Client }

// viaEmbedded reads through the embedding struct (a: list watch approvalpolicies).
func viaEmbedded(ctx context.Context, c client.Client) {
	var r client.Reader = embedsClient{Client: c}
	_ = r.Get(ctx, key, &v1alpha1.ApprovalPolicy{})
}

// methodValue takes a client method as a value, which the scan cannot read.
func (r *reconciler) methodValue(ctx context.Context) {
	get := r.Reader.Get
	_ = get(ctx, key, &v1alpha1.Allocation{})
}

// watched starts informers: For and Owns on the manager's builder are a list and a watch of the kind (b: list watch
// operations and approvals).
func watched(mgr manager.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&v1alpha1.Operation{}).Owns(&v1alpha1.Approval{}).Complete(nil)
}

// fallback is the nil default of a reader that only a test takes; its store is skipped by position (the line with the
// marker "skip-store").
func (r *reconciler) fallback() {
	if r.Reader == nil {
		r.Reader = r.Client // skip-store
	}
}

// typed reads through a clientset (c: list apps/controllerrevisions, update coordination.k8s.io/leases).
func typed(ctx context.Context, cs kubernetes.Interface) {
	_, _ = cs.AppsV1().ControllerRevisions("example").List(ctx, listOptions)
	_, _ = cs.CoordinationV1().Leases("example").Update(ctx, &coordinationv1.Lease{}, updateOptions)
}

// typedPatch patches a lease and its status through a clientset (c: patch coordination.k8s.io/leases and leases/status).
func typedPatch(ctx context.Context, cs kubernetes.Interface) {
	_, _ = cs.CoordinationV1().Leases("example").Patch(ctx, "example", types.MergePatchType, nil, patchOptions)
	_, _ = cs.CoordinationV1().Leases("example").Patch(ctx, "example", types.MergePatchType, nil, patchOptions, "status")
}

// dyn writes and reads through a dynamic client: the status write is the parent's status (c: update operations, update
// operations/status, get operations/scale).
func dyn(ctx context.Context, d dynamic.Interface) {
	gvr := schema.GroupVersionResource{Group: "hpcburst.groundnuty.github.io", Version: "v1alpha1", Resource: "operations"}
	//rbac:resources hpcburst.groundnuty.github.io/operations
	_, _ = d.Resource(gvr).Namespace("example").Update(ctx, &unstructured.Unstructured{}, updateOptions)
	//rbac:resources hpcburst.groundnuty.github.io/operations
	_, _ = d.Resource(gvr).Namespace("example").UpdateStatus(ctx, &unstructured.Unstructured{}, updateOptions)
	//rbac:resources hpcburst.groundnuty.github.io/operations
	_, _ = d.Resource(gvr).Namespace("example").Get(ctx, "example", getOptions, "scale")
}

// admin calls through the exempt clientset: no site.
func admin(ctx context.Context, cs kubernetes.Interface) {
	_, _ = cs.CoreV1().Namespaces().Create(ctx, nil, createOptions)
}

// dead has a client parameter that no call fills: its call is not a live site.
func dead(ctx context.Context, c client.Client) {
	_ = c.Delete(ctx, &v1alpha1.Instance{})
}

// libTrigger stands for the call that brings a library's requests; a stands for it (see the test's entry).
func libTrigger() {}

// hook is called through a function value: the scan matches it to the functions used as values by signature.
type hook func(mgr manager.Manager) *other

var hooks = []hook{buildB}

// wire connects everything: a gets one of each, b gets the shared helper's and the builder's, c the clientset's.
func wire(ctx context.Context, cfg *rest.Config) error {
	ma, err := newA(cfg)
	if err != nil {
		return err
	}
	mb, err := newB(cfg)
	if err != nil {
		return err
	}
	ra := buildA(ma)
	ra.cachedGet(ctx)
	ra.readerGet(ctx)
	ra.readerList(ctx)
	ra.cachedList(ctx)
	ra.writes(ctx)
	ra.statusUpdate(ctx)
	objs := map[string]client.Object{"a": &v1alpha1.Allocation{}}
	ra.generic(ctx, objs["a"])
	ra.followed(ctx, &v1alpha1.Instance{})
	ra.annotated(ctx, &v1alpha1.Approval{})
	ra.converted(ctx)
	ra.fallback()
	ra.staleNote(ctx)
	viaEmbedded(ctx, ra.Client)
	ra.rangeLiteral(ctx)
	ra.subresourceGet(ctx)
	viaLocalInterfaces(ctx, ma)
	ra.methodValue(ctx)
	rawRequest(ctx, nil)
	libTrigger()
	for _, h := range hooks {
		h(mb).readerGet(ctx) // b's, through a function value: its calls are b's, a's are not
	}
	if err := watched(mb); err != nil {
		return err
	}
	typed(ctx, newTyped(cfg))
	typedPatch(ctx, newTyped(cfg))
	dyn(ctx, newDynamic(cfg))
	admin(ctx, newAdmin(cfg))
	return nil
}
