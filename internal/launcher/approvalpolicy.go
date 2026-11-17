package launcher

// The global ApprovalPolicy and the definitions the lifecycle controller needs (HBEP-10, Approval policy;
// issue #589, launcher track). The controller watches Operation, Approval and ApprovalPolicy, so its caches
// never sync on an API server that lacks one of the three definitions: up refuses to start it then
// (checkControllerCRDs). And with no ApprovalPolicy at all a target that needs approval waits, so up
// writes the global one when none exists, naming the identity of the admin kubeconfig as the API server
// reports it (a SelfSubjectReview). It never changes one that exists: a person's edit of the approvers
// survives every restart, and the object keeps its UID. Only a Create is ever sent, never an Update or a
// patch.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// GlobalApprovalPolicyName is the name of the global policy: a policy's name derives from its scope, and
// the global scope's is "global" (api/v1alpha1/approvalpolicy_types.go).
const GlobalApprovalPolicyName = "global"

// What up reports about the global policy (UpReport.GlobalApprovalPolicy).
const (
	GlobalPolicyCreated = "created"
	GlobalPolicyExists  = "exists"
	GlobalPolicyNoCRD   = "no CRD"
)

// The bound on the wait for the API server to serve the kind, and the pause between asks.
const (
	policyServedBound    = 30 * time.Second
	policyServedInterval = 200 * time.Millisecond
)

// noApprovalPolicyCRDWarning is up's warning when the custom resource definition is absent, which is
// possible only on a host that runs no controller.
const noApprovalPolicyCRDWarning = "warning: no global ApprovalPolicy was written: the custom resource definition " + approvalPolicyCRD +
	" does not exist, so no policy says who may approve and a target that needs approval waits; apply the definitions with --manifests config/crd, and a later hpcburst up writes it"

const approvalPolicyCRD = "approvalpolicies." + FrameworkGroup

var approvalPolicyGVR = v1alpha1.GroupVersion.WithResource("approvalpolicies")

// errPolicyKindNotServed is what one attempt reports while the API server does not serve ApprovalPolicy yet.
var errPolicyKindNotServed = errors.New("the API server does not serve the kind ApprovalPolicy yet")

// EnsureGlobalApprovalPolicy makes ApprovalPolicy/global exist, once. The outcome is GlobalPolicyCreated
// when it wrote one (approver: the user name the API server reports for cfg's credential),
// GlobalPolicyExists when one was there (left exactly as it is, whoever wrote it and whatever it says), or
// GlobalPolicyNoCRD when the definition does not exist (nothing was written; the caller warns). A kind that
// the API server does not serve yet is retried for a bound, and a failure then names ApprovalPolicy.
func EnsureGlobalApprovalPolicy(ctx context.Context, cfg *rest.Config, log io.Writer) (string, error) {
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return "", err
	}
	return ensureGlobalApprovalPolicy(ctx, dyn, func(ctx context.Context) (string, error) { return adminUsername(ctx, cfg) },
		policyServedBound, policyServedInterval, log)
}

// adminUsername asks the API server who cfg's credential is, with a SelfSubjectReview.
func adminUsername(ctx context.Context, cfg *rest.Config) (string, error) {
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return "", err
	}
	review, err := cs.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authenticationv1.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("ask the API server who the admin kubeconfig is (SelfSubjectReview): %w", err)
	}
	if review.Status.UserInfo.Username == "" {
		return "", errors.New("the API server reported no user name for the admin kubeconfig (SelfSubjectReview), so there is no approver to name")
	}
	return review.Status.UserInfo.Username, nil
}

// ensureGlobalApprovalPolicy is EnsureGlobalApprovalPolicy over a dynamic client. whoami is asked only when
// a policy is to be written, so an existing policy never depends on it.
func ensureGlobalApprovalPolicy(ctx context.Context, dyn dynamic.Interface, whoami func(context.Context) (string, error), bound, interval time.Duration, log io.Writer) (string, error) {
	switch _, err := dyn.Resource(crdGVR).Get(ctx, approvalPolicyCRD, metav1.GetOptions{}); {
	case apierrors.IsNotFound(err):
		logf(log, "the custom resource definition %s does not exist, so no global ApprovalPolicy is written", approvalPolicyCRD)
		return GlobalPolicyNoCRD, nil
	case err != nil:
		return "", fmt.Errorf("read the custom resource definition %s: %w", approvalPolicyCRD, err)
	}
	policies := dyn.Resource(approvalPolicyGVR)
	user := ""
	deadline := time.Now().Add(bound)
	for {
		outcome, err := globalPolicyAttempt(ctx, policies, &user, whoami)
		switch {
		case err == nil:
			if outcome == GlobalPolicyCreated {
				logf(log, "created ApprovalPolicy %s, with the approver user %s", GlobalApprovalPolicyName, user)
			} else {
				logf(log, "ApprovalPolicy %s exists; it is left as it is", GlobalApprovalPolicyName)
			}
			return outcome, nil
		case !errors.Is(err, errPolicyKindNotServed):
			return "", err
		case time.Now().After(deadline) || ctx.Err() != nil:
			return "", fmt.Errorf("the global ApprovalPolicy was not written: the API server still did not serve the kind ApprovalPolicy (%s) after %s (%v)", approvalPolicyCRD, bound, err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(interval):
		}
	}
}

// globalPolicyAttempt makes one pass: a Get, then a Create when the policy is not there. An error that
// wraps errPolicyKindNotServed says the API server does not serve the kind yet. A 404 on the Get cannot
// tell an absent object from an unserved resource, so it goes on to the Create, whose 404 can only be the
// latter. user caches the admin's name across passes.
func globalPolicyAttempt(ctx context.Context, policies dynamic.ResourceInterface, user *string, whoami func(context.Context) (string, error)) (string, error) {
	_, err := policies.Get(ctx, GlobalApprovalPolicyName, metav1.GetOptions{})
	switch {
	case err == nil:
		return GlobalPolicyExists, nil
	case meta.IsNoMatchError(err):
		return "", fmt.Errorf("%w: %v", errPolicyKindNotServed, err)
	case !apierrors.IsNotFound(err):
		return "", fmt.Errorf("read ApprovalPolicy %s: %w", GlobalApprovalPolicyName, err)
	}
	if *user == "" {
		u, err := whoami(ctx)
		if err != nil {
			return "", err
		}
		*user = u
	}
	obj, err := globalPolicyObject(*user)
	if err != nil {
		return "", err
	}
	_, err = policies.Create(ctx, obj, metav1.CreateOptions{FieldManager: fieldManager})
	switch {
	case err == nil:
		return GlobalPolicyCreated, nil
	case apierrors.IsAlreadyExists(err): // written by someone else between the Get and the Create
		return GlobalPolicyExists, nil
	case isNotServed(err):
		return "", fmt.Errorf("%w: %v", errPolicyKindNotServed, err)
	}
	return "", fmt.Errorf("create ApprovalPolicy %s: %w", GlobalApprovalPolicyName, err)
}

// globalPolicyObject is the global policy with one approver, the user.
func globalPolicyObject(user string) (*unstructured.Unstructured, error) {
	ap := &v1alpha1.ApprovalPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: GlobalApprovalPolicyName},
		Spec: v1alpha1.ApprovalPolicySpec{
			Scope:     v1alpha1.ApprovalScope{Global: ptr(true)},
			Approvers: []v1alpha1.Approver{{User: user}},
		},
	}
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(ap)
	if err != nil {
		return nil, err
	}
	u := &unstructured.Unstructured{Object: m}
	u.SetAPIVersion(v1alpha1.GroupVersion.String())
	u.SetKind("ApprovalPolicy")
	return u, nil
}

// controllerCRDs are the definitions of the kinds the lifecycle controller watches beyond the ones up needs
// for its own steps: its caches sync only when the API server serves all three.
var controllerCRDs = []struct{ kind, crd string }{
	{"Operation", "operations." + FrameworkGroup},
	{"Approval", "approvals." + FrameworkGroup},
	{"ApprovalPolicy", approvalPolicyCRD},
}

// errMissingControllerCRDs is what checkControllerCRDs wraps.
var errMissingControllerCRDs = errors.New("the API server lacks custom resource definitions the controller watches")

// checkControllerCRDs returns nil when no controller is to be started (ctrlBin empty) or when the API server
// holds the three definitions. Otherwise it is an error that names each missing one and says how to apply
// them. Only a definition that is NotFound counts as missing: any other failure to ask is returned as it is.
func checkControllerCRDs(ctx context.Context, dyn dynamic.Interface, ctrlBin string) error {
	if ctrlBin == "" {
		return nil
	}
	var missing []string
	for _, c := range controllerCRDs {
		switch _, err := dyn.Resource(crdGVR).Get(ctx, c.crd, metav1.GetOptions{}); {
		case apierrors.IsNotFound(err):
			missing = append(missing, c.crd)
		case err != nil:
			return fmt.Errorf("read the custom resource definition %s: %w", c.crd, err)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %s do not exist, and the controller's caches for them would never sync; apply them with --manifests config/crd",
		errMissingControllerCRDs, strings.Join(missing, ", "))
}

// CheckControllerCRDs is checkControllerCRDs against cfg's API server.
func CheckControllerCRDs(ctx context.Context, cfg *rest.Config, ctrlBin string) error {
	if ctrlBin == "" {
		return nil
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return err
	}
	return checkControllerCRDs(ctx, dyn, ctrlBin)
}

// GlobalPolicyState is what status says about ApprovalPolicy/global. Absent (Present false and no Detail)
// is a state, not a fault: with no policy at all a target that needs approval waits.
type GlobalPolicyState struct {
	Present bool `json:"present"`
	// Approvers are the policy's approvers, each as "user X", "group X" or "serviceAccount NAMESPACE/NAME".
	Approvers []string `json:"approvers,omitempty"`
	// Detail says why the policy could not be read, when it could not.
	Detail string `json:"detail,omitempty"`
}

// CheckGlobalPolicy asks the API server for ApprovalPolicy/global. A policy or a definition that is not there
// is Present false with no Detail. The error is a failure to ask, never an absent policy.
func CheckGlobalPolicy(ctx context.Context, cfg *rest.Config) (*GlobalPolicyState, error) {
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return checkGlobalPolicy(ctx, dyn)
}

func checkGlobalPolicy(ctx context.Context, dyn dynamic.Interface) (*GlobalPolicyState, error) {
	obj, err := dyn.Resource(approvalPolicyGVR).Get(ctx, GlobalApprovalPolicyName, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err), meta.IsNoMatchError(err):
		return &GlobalPolicyState{}, nil // no policy, or no definition: the same state
	case err != nil:
		return nil, fmt.Errorf("read ApprovalPolicy %s: %w", GlobalApprovalPolicyName, err)
	}
	var ap v1alpha1.ApprovalPolicy
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &ap); err != nil {
		return &GlobalPolicyState{Present: true, Detail: "its spec could not be read: " + err.Error()}, nil
	}
	return &GlobalPolicyState{Present: true, Approvers: approverStrings(ap.Spec.Approvers)}, nil
}

// approverStrings writes approvers as people read them.
func approverStrings(as []v1alpha1.Approver) []string {
	out := make([]string, 0, len(as))
	for _, a := range as {
		switch {
		case a.User != "":
			out = append(out, "user "+a.User)
		case a.Group != "":
			out = append(out, "group "+a.Group)
		case a.ServiceAccount != nil:
			out = append(out, "serviceAccount "+a.ServiceAccount.Namespace+"/"+a.ServiceAccount.Name)
		default:
			out = append(out, "an approver that names nobody")
		}
	}
	return out
}
