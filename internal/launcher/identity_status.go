package launcher

// What status says about the agents (HBEP-10, Identities and roles; issue #589, PR 2): whether their
// namespace, roles and bindings are the manifest's (agents.go), and each identity's expiry and whether
// the API server accepts its token. An expired or rejected identity is a reported state, never a host
// failure: the host is as healthy as the other checks found it, and status exits 0. A failure to ask
// the API server is a failure.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// defaultServiceAccount is the ServiceAccount the control plane makes in every namespace.
const defaultServiceAccount = "default"

// identityAskTimeout bounds each identity's one question to the API server.
const identityAskTimeout = 10 * time.Second

// IdentityState is one agent identity: a ServiceAccount of the agents' namespace, a kubeconfig file in
// the identities directory, or both.
type IdentityState struct {
	Name       string `json:"name"`
	Kubeconfig string `json:"kubeconfig,omitempty"`
	// Expires is the expiry the token itself carries (its exp claim, which is what the API server set
	// from its TokenRequest answer). Nil when there is no file, or none could be read from it.
	Expires *time.Time `json:"expires,omitempty"`
	// Valid is true when the API server accepts the token: a SelfSubjectReview made with the
	// identity's kubeconfig names this identity. Detail says why not otherwise.
	Valid  bool   `json:"valid"`
	Detail string `json:"detail,omitempty"`
}

// CheckIdentities lists the agent identities: the ServiceAccounts of the agents' namespace, which cfg
// reaches as an administrator, joined by name with the kubeconfig files of identitiesDir, sorted. For
// each file it asks the API server one SelfSubjectReview with that kubeconfig, which must name ports'
// loopback server, as restConfig checks. A rejected token (401) is a state, and so is a missing file or
// ServiceAccount. A failure to reach the API server is an error, never a state.
func CheckIdentities(ctx context.Context, cfg *rest.Config, identitiesDir string, ports Ports) ([]IdentityState, error) {
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	list, err := cs.CoreV1().ServiceAccounts(AgentsNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list the ServiceAccounts of %s: %w", AgentsNamespace, err)
	}
	hasSA := map[string]bool{}
	for _, sa := range list.Items {
		hasSA[sa.Name] = true
	}
	hasFile := map[string]bool{}
	ents, err := os.ReadDir(identitiesDir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read %s: %w", identitiesDir, err)
	}
	for _, e := range ents {
		if name, ok := strings.CutSuffix(e.Name(), ".kubeconfig"); ok && e.Type().IsRegular() && ValidIdentityName(name) == nil {
			hasFile[name] = true
		}
	}
	var names []string
	for n := range hasSA {
		// Kubernetes makes a ServiceAccount called default in every namespace. No identity is issued
		// for it, and only the admin could make a token for it: it is listed only when a kubeconfig
		// was written for it.
		if n != defaultServiceAccount || hasFile[n] {
			names = append(names, n)
		}
	}
	for n := range hasFile {
		if !hasSA[n] {
			names = append(names, n)
		}
	}
	slices.Sort(names)

	var out []IdentityState
	for _, name := range names {
		is := IdentityState{Name: name}
		var notes []string
		if !hasSA[name] {
			notes = append(notes, "no ServiceAccount "+name+" in "+AgentsNamespace)
		}
		if !hasFile[name] {
			is.Detail = strings.Join(append(notes, "no kubeconfig file "+identityFile(identitiesDir, name)+"; hpcburst identity renew "+name+" writes one"), "; ")
			out = append(out, is)
			continue
		}
		is.Kubeconfig = identityFile(identitiesDir, name)
		why, err := askIdentity(ctx, &is, ports)
		if err != nil {
			return nil, fmt.Errorf("agent identity %s: %w", name, err)
		}
		if why != "" {
			notes = append(notes, why)
		}
		is.Detail = strings.Join(notes, "; ")
		out = append(out, is)
	}
	return out, nil
}

// askIdentity reads the identity's kubeconfig through restConfig, so one that names another server
// is refused, takes the expiry from its token, and asks the API server who the token belongs to. It
// sets is.Valid and is.Expires and returns why the identity is not valid, or "". A 401 is an answer.
// Any other failure to ask is an error.
func askIdentity(ctx context.Context, is *IdentityState, ports Ports) (why string, err error) {
	cfg, err := restConfig(is.Kubeconfig, ports, identityAskTimeout)
	if err != nil {
		return err.Error(), nil
	}
	if exp, err := tokenExpiry(cfg.BearerToken); err == nil {
		is.Expires = &exp
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return "", err
	}
	review, err := cs.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authenticationv1.SelfSubjectReview{}, metav1.CreateOptions{})
	switch {
	case apierrors.IsUnauthorized(err):
		if is.Expires != nil && !is.Expires.After(time.Now()) {
			return fmt.Sprintf("the token expired at %s and the API server rejected it; hpcburst identity renew %s writes a new one", is.Expires.Format(time.RFC3339), is.Name), nil
		}
		return "the API server rejected the token (401); hpcburst identity renew " + is.Name + " writes a new one", nil
	case err != nil:
		return "", fmt.Errorf("ask the API server who the token is: %w", err)
	}
	if got := review.Status.UserInfo.Username; got != IdentityUsername(is.Name) {
		return fmt.Sprintf("the kubeconfig authenticates as %q, not as %q", got, IdentityUsername(is.Name)), nil
	}
	is.Valid = true
	return "", nil
}

// tokenExpiry reads the exp claim of a service account token, which is a JWT, without verifying it:
// the API server's answer to a SelfSubjectReview is what says whether it is accepted.
func tokenExpiry(token string) (time.Time, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}, errors.New("the token is not a JWT")
	}
	b, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, err
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(b, &claims); err != nil {
		return time.Time{}, err
	}
	if claims.Exp <= 0 {
		return time.Time{}, errors.New("the token has no exp claim")
	}
	return time.Unix(claims.Exp, 0).UTC(), nil
}
