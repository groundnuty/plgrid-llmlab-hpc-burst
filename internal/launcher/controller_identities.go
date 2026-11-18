package launcher

// The controllers' own identities (HBEP-10, Audit; HBEP-11, Single-user host; issue #660, PR A). The
// processes that call the API server on the host, other than the launcher, are three:
//   - hpcburst-lifecycle: hpcburst-controller's lifecycle and verification managers and the agent API,
//     which read --kubeconfig;
//   - hpcburst-provider: its Provider managers, which read --provider-kubeconfig;
//   - hpcburst-front: hpcburst-front, which reads --kubeconfig.
//
// Each is a ServiceAccount of hpcburst-system with a ClusterRole of its own that grants exactly what its
// code calls (config/rbac/controllers.yaml, embedded), so the audit log tells a controller's write from a
// person's and from the launcher's. Every start (up) and every restart of the supervisor (upgrade)
// installs the manifest, resets it, and issues one token per ServiceAccount with the TokenRequest API,
// the one-year life the science agent ruled (S1), into <state>/identities/system/<name>.kubeconfig: mode
// 600, in a directory of mode 700, written atomically, with the code the agents' identities use
// (identity.go: issueToken). There is no rotation: every start issues new ones, and status warns when a
// token has 30 days or fewer left. The relay keeper and the durable log build no API client.
//
// The launcher keeps the admin kubeconfig for itself, and <state>/kubeconfig stays for the person. A
// token is never logged, never put on a command line and never in a status line; the supervised processes
// are told the kubeconfig's PATH only (supervise.go).
//
// down leaves the files in place: the next start issues them again. A supervisor that restarts a crashed
// child reuses them.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/config/rbac"
)

// The three controller identities: their ServiceAccounts' names, which are also their ClusterRoles', their
// ClusterRoleBindings' and their kubeconfigs' (<name>.kubeconfig).
const (
	ControllerLifecycleIdentity = "hpcburst-lifecycle"
	ControllerProviderIdentity  = "hpcburst-provider"
	ControllerFrontIdentity     = "hpcburst-front"
)

// ControllerTokenSeconds is the lifetime asked of a controller's token: one year (the science agent, 17
// Nov 2026: S1). The API server may grant less, and the expiry that up and status report is the one it
// answers with. Every start issues a new token.
const ControllerTokenSeconds int64 = 365 * 24 * 60 * 60

// ControllerTokenWarnWithin is how little time a token may have left before status warns.
const ControllerTokenWarnWithin = 30 * 24 * time.Hour

// systemIdentitiesDirName is the directory of the controllers' kubeconfigs inside the identities directory
// (<state>/identities/system). A loader of kubeconfigs that must refuse a controller's credential, as the
// capacity guard's (#659) does, refuses this directory by path (SystemIdentitiesDir).
const systemIdentitiesDirName = "system"

// ControllerIdentityNames are the controller identities, in the order up issues them and status lists them.
func ControllerIdentityNames() []string {
	return []string{ControllerLifecycleIdentity, ControllerProviderIdentity, ControllerFrontIdentity}
}

// systemIdentitiesDir is where the controllers' kubeconfigs are. It survives down.
func (l layout) systemIdentitiesDir() string {
	return filepath.Join(l.identitiesDir(), systemIdentitiesDirName)
}

// controllerKubeconfig is the kubeconfig of the controller identity called name.
func (l layout) controllerKubeconfig(name string) string {
	return identityFile(l.systemIdentitiesDir(), name)
}

// SystemIdentitiesDir is the directory of the controllers' kubeconfigs under stateDir, for a caller that
// must refuse those files by path.
func SystemIdentitiesDir(stateDir string) string { return layout{stateDir}.systemIdentitiesDir() }

// ControllerKubeconfig is the path of the controller identity name's kubeconfig under stateDir.
func ControllerKubeconfig(stateDir, name string) string {
	return layout{stateDir}.controllerKubeconfig(name)
}

// controllerManifest is the decoded config/rbac/controllers.yaml, in apply order: the ServiceAccounts, the
// ClusterRoles, the Roles of hpcburst-system (the Lease verbs), then the bindings of both.
type controllerManifest struct {
	accounts        []*unstructured.Unstructured
	clusterRoles    []*unstructured.Unstructured
	roles           []*unstructured.Unstructured
	clusterBindings []*unstructured.Unstructured
	bindings        []*unstructured.Unstructured
}

// all returns the objects in apply order.
func (m *controllerManifest) all() []*unstructured.Unstructured {
	var out []*unstructured.Unstructured
	for _, l := range [][]*unstructured.Unstructured{m.accounts, m.clusterRoles, m.roles, m.clusterBindings, m.bindings} {
		out = append(out, l...)
	}
	return out
}

// controllerObjects decodes the embedded manifest and checks its shape (controllerManifestOf).
func controllerObjects() (*controllerManifest, error) {
	objs, err := decodeManifests(bytes.NewReader(rbac.Controllers), "config/rbac/controllers.yaml")
	if err != nil {
		return nil, err
	}
	return controllerManifestOf(objs)
}

// controllerManifestOf sorts decoded objects into a controllerManifest. It refuses anything but the three
// ServiceAccounts of hpcburst-system, the three ClusterRoles and ClusterRoleBindings, and the three Roles and
// RoleBindings of hpcburst-system, each once; a binding that names more than its one ServiceAccount or a
// role of another name, a role with an aggregationRule, a Role that grants anything but the Lease verbs, and
// a rule with a wildcard in its groups, resources or verbs. A change to the file that the launcher does not
// understand stops here and not at a start, and a role cannot grow a wildcard in silence.
func controllerManifestOf(objs []*unstructured.Unstructured) (*controllerManifest, error) {
	m := &controllerManifest{}
	for _, o := range objs {
		inSystem := o.GetNamespace() == v1alpha1.ControllerNamespace
		switch {
		case o.GetKind() == "ServiceAccount" && o.GetAPIVersion() == "v1" && inSystem:
			m.accounts = append(m.accounts, o)
		case o.GetKind() == "ClusterRole" && o.GetAPIVersion() == "rbac.authorization.k8s.io/v1" && o.GetNamespace() == "":
			m.clusterRoles = append(m.clusterRoles, o)
		case o.GetKind() == "ClusterRoleBinding" && o.GetAPIVersion() == "rbac.authorization.k8s.io/v1" && o.GetNamespace() == "":
			m.clusterBindings = append(m.clusterBindings, o)
		case o.GetKind() == "Role" && o.GetAPIVersion() == "rbac.authorization.k8s.io/v1" && inSystem:
			m.roles = append(m.roles, o)
		case o.GetKind() == "RoleBinding" && o.GetAPIVersion() == "rbac.authorization.k8s.io/v1" && inSystem:
			m.bindings = append(m.bindings, o)
		default:
			return nil, fmt.Errorf("the controllers manifest holds an unexpected %s %s, want the ServiceAccounts, ClusterRoles, ClusterRoleBindings, Roles and RoleBindings %v (the namespaced ones in %s)",
				o.GetKind(), o.GetName(), ControllerIdentityNames(), v1alpha1.ControllerNamespace)
		}
	}
	want := sorted(ControllerIdentityNames())
	names := func(os []*unstructured.Unstructured) []string {
		var n []string
		for _, o := range os {
			n = append(n, o.GetName())
		}
		slices.Sort(n)
		return n
	}
	for what, got := range map[string][]string{"ServiceAccounts": names(m.accounts), "ClusterRoles": names(m.clusterRoles), "ClusterRoleBindings": names(m.clusterBindings),
		"Roles": names(m.roles), "RoleBindings": names(m.bindings)} {
		if !slices.Equal(got, want) {
			return nil, fmt.Errorf("the controllers manifest holds the %s %v, want %v, each once", what, got, want)
		}
	}
	for _, r := range append(slices.Clone(m.clusterRoles), m.roles...) {
		if _, ok := r.Object["aggregationRule"]; ok {
			return nil, fmt.Errorf("the %s %s has an aggregationRule, which lets a controller rewrite its rules", r.GetKind(), r.GetName())
		}
		rules, _, err := unstructured.NestedSlice(r.Object, "rules")
		if err != nil || len(rules) == 0 {
			return nil, fmt.Errorf("the %s %s has no rules, or they are not a list (%v)", r.GetKind(), r.GetName(), err)
		}
		for i, rule := range rules {
			rm, _ := rule.(map[string]any)
			for _, key := range []string{"apiGroups", "resources", "verbs", "resourceNames", "nonResourceURLs"} {
				vals, _ := rm[key].([]any)
				for _, v := range vals {
					if s, _ := v.(string); s == "*" {
						return nil, fmt.Errorf("the %s %s has a wildcard in %s of its rule %d: each role grants exactly what its component calls", r.GetKind(), r.GetName(), key, i+1)
					}
				}
			}
			// The namespaced Role is the Lease verbs and nothing else: what a controller does in
			// hpcburst-system alone.
			if r.GetKind() == "Role" {
				if g, _ := rm["apiGroups"].([]any); len(g) != 1 || g[0] != "coordination.k8s.io" {
					return nil, fmt.Errorf("the Role %s grants %v in %s, want the Lease verbs of coordination.k8s.io alone", r.GetName(), rm["apiGroups"], v1alpha1.ControllerNamespace)
				}
				if res, _ := rm["resources"].([]any); len(res) != 1 || res[0] != "leases" {
					return nil, fmt.Errorf("the Role %s grants %v in %s, want leases alone", r.GetName(), rm["resources"], v1alpha1.ControllerNamespace)
				}
			}
		}
	}
	for _, b := range append(slices.Clone(m.clusterBindings), m.bindings...) {
		subjects, _, _ := unstructured.NestedSlice(b.Object, "subjects")
		if len(subjects) != 1 {
			return nil, fmt.Errorf("the %s %s has %d subjects, want only its own ServiceAccount", b.GetKind(), b.GetName(), len(subjects))
		}
		sm, _ := subjects[0].(map[string]any)
		if sm["kind"] != "ServiceAccount" || sm["name"] != b.GetName() || sm["namespace"] != v1alpha1.ControllerNamespace {
			return nil, fmt.Errorf("the %s %s names the subject %v, want the ServiceAccount %s of %s", b.GetKind(), b.GetName(), sm, b.GetName(), v1alpha1.ControllerNamespace)
		}
		ref, _, _ := unstructured.NestedString(b.Object, "roleRef", "name")
		refKind, _, _ := unstructured.NestedString(b.Object, "roleRef", "kind")
		if ref != b.GetName() || refKind+"Binding" != b.GetKind() {
			return nil, fmt.Errorf("the %s %s names the role %s %q, want the %s of its own name", b.GetKind(), b.GetName(), refKind, ref, strings.TrimSuffix(b.GetKind(), "Binding"))
		}
	}
	return m, nil
}

// InstallControllerRBAC makes the three ServiceAccounts, the roles and the bindings exist and be the manifest's,
// with server-side apply, so repeating it is safe. The roles and bindings that exist already are set to
// the manifest's rules and subjects first (resetControllerObjects), so what was added by hand does not survive
// a start. The namespace hpcburst-system must exist (ensureControllerNamespace).
func InstallControllerRBAC(ctx context.Context, cfg *rest.Config, log io.Writer) error {
	m, err := controllerObjects()
	if err != nil {
		return err
	}
	dyn, mapper, err := newApplier(cfg)
	if err != nil {
		return err
	}
	if err := resetControllerObjects(ctx, dyn, m, log); err != nil {
		return err
	}
	for _, o := range m.all() {
		if err := applyOne(ctx, dyn, mapper, o); err != nil {
			return err
		}
		logf(log, "applied %s %s", o.GetKind(), o.GetName())
	}
	return nil
}

var (
	roleGVR        = schemaGVR("rbac.authorization.k8s.io", "v1", "roles")
	roleBindingGVR = schemaGVR("rbac.authorization.k8s.io", "v1", "rolebindings")
)

// resetControllerObjects sets each role and binding that exists to the manifest's, before the apply, as the
// agents' are (resetAgentObjects): a role keeps exactly the manifest's rules and no aggregationRule; a binding
// keeps exactly the manifest's subjects, and one whose roleRef differs is deleted, for roleRef cannot change,
// and the apply makes it again. The Roles and RoleBindings are namespaced; the others are not.
func resetControllerObjects(ctx context.Context, dyn dynamic.Interface, m *controllerManifest, log io.Writer) error {
	for _, want := range m.clusterRoles {
		if err := resetFields(ctx, dyn, clusterRoleGVR, want, "rules", "aggregationRule"); err != nil {
			return err
		}
	}
	for _, want := range m.roles {
		if err := resetFields(ctx, dyn, roleGVR, want, "rules", "aggregationRule"); err != nil {
			return err
		}
	}
	for gvr, wants := range map[schema.GroupVersionResource][]*unstructured.Unstructured{clusterRoleBindingGVR: m.clusterBindings, roleBindingGVR: m.bindings} {
		for _, want := range wants {
			res := resourceOf(dyn, gvr, want)
			cur, err := res.Get(ctx, want.GetName(), metav1.GetOptions{})
			switch {
			case apierrors.IsNotFound(err):
				continue // the apply creates it
			case err != nil:
				return fmt.Errorf("read the %s %s: %w", want.GetKind(), want.GetName(), err)
			}
			if !sameField(cur, want, "roleRef") {
				uid := cur.GetUID()
				err := res.Delete(ctx, want.GetName(), metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
				if err != nil && !apierrors.IsNotFound(err) {
					return fmt.Errorf("delete the %s %s, whose roleRef differs from the manifest's: %w", want.GetKind(), want.GetName(), err)
				}
				logf(log, "deleted %s %s: its roleRef differs from the manifest's, and a roleRef cannot change", want.GetKind(), want.GetName())
				continue
			}
			if err := resetFields(ctx, dyn, gvr, want, "subjects"); err != nil {
				return err
			}
		}
	}
	return nil
}

// ControllerIdentity is one controller identity as up issued it: never the token, which is in the file.
type ControllerIdentity struct {
	Name       string    `json:"name"`
	Username   string    `json:"username"`
	Kubeconfig string    `json:"kubeconfig"`
	Expires    time.Time `json:"expires"`
}

// IssueControllerIdentities requests one token per controller ServiceAccount, with the API server cfg
// reaches as an administrator, and writes each into <state>/identities/system/<name>.kubeconfig. The
// ServiceAccounts must exist (InstallControllerRBAC). Both directories are set to mode 700.
func IssueControllerIdentities(ctx context.Context, cfg *rest.Config, l layout, log io.Writer) ([]ControllerIdentity, error) {
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return issueControllerTokens(ctx, cs, cfg, l, log)
}

// issueControllerTokens is IssueControllerIdentities over the clientset cs, which a test replaces.
func issueControllerTokens(ctx context.Context, cs kubernetes.Interface, cfg *rest.Config, l layout, log io.Writer) ([]ControllerIdentity, error) {
	for _, d := range []string{l.identitiesDir(), l.systemIdentitiesDir()} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, err
		}
		if err := os.Chmod(d, 0o700); err != nil { // set explicitly, so a umask or an older directory cannot loosen it
			return nil, err
		}
	}
	var out []ControllerIdentity
	for _, name := range ControllerIdentityNames() {
		res, err := issueToken(ctx, cs, cfg, tokenTarget{Namespace: v1alpha1.ControllerNamespace, Name: name, Seconds: ControllerTokenSeconds, Dir: l.systemIdentitiesDir()}, true)
		if err != nil {
			return nil, err
		}
		logf(log, "issued the token of %s, which expires %s; wrote %s", name, res.Expires.Format(time.RFC3339), res.Kubeconfig)
		out = append(out, ControllerIdentity{Name: name, Username: res.Username, Kubeconfig: res.Kubeconfig, Expires: res.Expires})
	}
	return out, nil
}

// controllerIdentitySteps are the steps of prepareControllerIdentities, a variable so that a test can
// record their order and fail one.
var controllerIdentitySteps = struct {
	namespace func(ctx context.Context, cfg *rest.Config) error
	install   func(ctx context.Context, cfg *rest.Config, log io.Writer) error
	issue     func(ctx context.Context, cfg *rest.Config, l layout, log io.Writer) ([]ControllerIdentity, error)
}{ensureControllerNamespace, InstallControllerRBAC, IssueControllerIdentities}

// prepareControllerIdentities leaves the controller identities ready: the namespace hpcburst-system, the
// ServiceAccounts, roles and bindings installed and reset to the manifest, and a fresh token and kubeconfig
// for each ServiceAccount. It is the one function that makes them ready, with k3s ready (the launcher's own
// kubeconfig, whose address is checked to be this host's) and before the supervisor starts
// (startControllerSupervisor), so that no path that starts the supervisor can start it without them.
func prepareControllerIdentities(ctx context.Context, l layout, ports Ports, log io.Writer) ([]ControllerIdentity, error) {
	cfg, err := restConfig(l.kubeconfig(), ports, 30*time.Second)
	if err != nil {
		return nil, err
	}
	return prepareControllerIdentitiesWith(ctx, cfg, l, log)
}

// prepareControllerIdentitiesWith is prepareControllerIdentities against the API server cfg reaches as an
// administrator: the namespace, then the roles, then the tokens, and none of the later steps after a failed
// earlier one.
func prepareControllerIdentitiesWith(ctx context.Context, cfg *rest.Config, l layout, log io.Writer) ([]ControllerIdentity, error) {
	if err := controllerIdentitySteps.namespace(ctx, cfg); err != nil {
		return nil, err
	}
	if err := controllerIdentitySteps.install(ctx, cfg, log); err != nil {
		return nil, fmt.Errorf("install the controllers' roles: %w", err)
	}
	ids, err := controllerIdentitySteps.issue(ctx, cfg, l, log)
	if err != nil {
		return nil, fmt.Errorf("issue the controllers' tokens: %w", err)
	}
	return ids, nil
}

// supervisorStartSteps are what startControllerSupervisor runs in order, a variable so that a test records
// the order.
var supervisorStartSteps = struct {
	prepare func(ctx context.Context, l layout, ports Ports, log io.Writer) ([]ControllerIdentity, error)
	start   func(ctx context.Context, l layout, st *State, argv []string, timeout time.Duration, log io.Writer) error
}{prepareControllerIdentities, startSupervisor}

// startControllerSupervisor is the ONE path that starts the controllers' supervisor: up and upgrade both
// call it, and nothing else calls startSupervisor (TestOnlyStartControllerSupervisorStartsTheSupervisor).
// It prepares the controller identities first, so that an upgrade from a host an earlier release started,
// which has neither the roles nor the files, ends with both, and a supervisor never starts a controller
// whose kubeconfig is missing. It returns the identities it issued.
func startControllerSupervisor(ctx context.Context, l layout, st *State, argv []string, timeout time.Duration, log io.Writer) ([]ControllerIdentity, error) {
	ids, err := supervisorStartSteps.prepare(ctx, l, st.Ports, log)
	if err != nil {
		return nil, err
	}
	return ids, supervisorStartSteps.start(ctx, l, st, argv, timeout, log)
}

// PrepareControllerIdentities is the whole of prepareControllerIdentities for a caller that has the API server
// as an administrator and a state directory of its own, and no running launcher: the namespace, the roles
// and a fresh token and kubeconfig for each controller identity under stateDir, as up and upgrade make them
// ready. A test of the controllers (cmd/hpcburst-controller's harness) runs them under these identities with
// the very function the launcher runs, so the roles it proves are the ones that are installed. It does not
// check the length of stateDir, which only k3s's sockets need.
func PrepareControllerIdentities(ctx context.Context, cfg *rest.Config, stateDir string, log io.Writer) ([]ControllerIdentity, error) {
	abs, err := filepath.Abs(stateDir)
	if err != nil {
		return nil, err
	}
	return prepareControllerIdentitiesWith(ctx, cfg, layout{abs}, log)
}

// requiredKubeconfigs are the kubeconfig files the supervised set names, in supervisedSet's order.
func (s *State) requiredKubeconfigs(l layout) []string {
	var out []string
	if s.Controllers != nil {
		out = append(out, l.controllerKubeconfig(ControllerLifecycleIdentity), l.controllerKubeconfig(ControllerProviderIdentity))
	}
	if s.FrontRun != nil {
		out = append(out, l.controllerKubeconfig(ControllerFrontIdentity))
	}
	return out
}

// checkControllerKubeconfigs refuses to start the supervisor when a kubeconfig that a child is told to read
// is missing or is not a regular file of mode 600: the controller would only fail to read it and be
// restarted with backoff. It names the file, never reads it.
func checkControllerKubeconfigs(l layout, st *State) error {
	var errs []error
	for _, p := range st.requiredKubeconfigs(l) {
		fi, err := os.Stat(p)
		switch {
		case err != nil:
			errs = append(errs, fmt.Errorf("the controller kubeconfig %s: %w (hpcburst up issues it)", p, err))
		case !fi.Mode().IsRegular() || fi.Mode().Perm() != 0o600:
			errs = append(errs, fmt.Errorf("the controller kubeconfig %s is not a regular file of mode 600 (%v)", p, fi.Mode()))
		}
	}
	return errors.Join(errs...)
}
