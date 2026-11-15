package launcher

// The agents' identities (HBEP-10, Identities and roles; issue #589, PR 2): hpcburst identity add and
// renew. An identity is a ServiceAccount of the namespace hpcburst-agents, and its credential is a
// 30-day token that the API server issues for it (TokenRequest, no bound object), written into a
// kubeconfig of its own, <state>/identities/<name>.kubeconfig, mode 600 in a directory of mode 700.
// The kubeconfig holds the loopback server and the CA of the launcher's admin kubeconfig, and the
// token alone: never the admin credential. The roles come from the group the namespace makes
// (agents.go), so adding an identity makes no binding.
//
// Both commands need the host running, and neither prints the token.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// IdentityTokenSeconds is the lifetime asked of an agent's token: 30 days. The API server may grant
// less (its --service-account-max-token-expiration), and the expiry the commands report is the one it
// answers with.
const IdentityTokenSeconds int64 = 30 * 24 * 60 * 60

// Errors the command line tells apart. ErrIdentityName is a usage error, the others are refusals.
var (
	ErrIdentityName   = errors.New("an identity name must be a DNS-1123 label")
	ErrIdentityExists = errors.New("identity already exists")
	ErrNoIdentity     = errors.New("no such identity")
)

// identitiesDir is where the agents' kubeconfigs are. It survives down, as the agent builds do:
// the tokens stay valid across a restart.
func (l layout) identitiesDir() string { return filepath.Join(l.dir, "identities") }

func identityFile(dir, name string) string { return filepath.Join(dir, name+".kubeconfig") }

// ValidIdentityName refuses a name that is not a DNS-1123 label: it becomes the ServiceAccount's name,
// the kubeconfig's file name and the end of the user name the API server reports.
func ValidIdentityName(name string) error {
	if errs := validation.IsDNS1123Label(name); len(errs) > 0 {
		return fmt.Errorf("%w: %q (%s)", ErrIdentityName, name, errs[0])
	}
	return nil
}

// IdentityUsername is the user name the API server reports for the identity called name.
func IdentityUsername(name string) string {
	return "system:serviceaccount:" + AgentsNamespace + ":" + name
}

// IdentityOptions are the inputs of AddIdentity and RenewIdentity.
type IdentityOptions struct {
	StateDir string
	Name     string
	Log      io.Writer
}

// IdentityResult is what add and renew report: where the kubeconfig is and when its token expires,
// as the API server's answer says. It never holds the token.
type IdentityResult struct {
	Name       string    `json:"name"`
	Username   string    `json:"username"`
	Kubeconfig string    `json:"kubeconfig"`
	Expires    time.Time `json:"expires"`
	Renewed    bool      `json:"renewed"`
}

// AddIdentity makes the identity: the ServiceAccount, a token and the kubeconfig. It needs the host
// running. A name that exists is refused, and nothing is written for it.
func AddIdentity(ctx context.Context, o IdentityOptions) (*IdentityResult, error) {
	return issueFromState(ctx, o, false)
}

// RenewIdentity requests a new token for an identity that exists and rewrites its kubeconfig. The
// old token stays valid until it expires: an unbound token cannot be revoked other than by deleting
// the ServiceAccount.
func RenewIdentity(ctx context.Context, o IdentityOptions) (*IdentityResult, error) {
	return issueFromState(ctx, o, true)
}

func issueFromState(ctx context.Context, o IdentityOptions, renew bool) (*IdentityResult, error) {
	if err := ValidIdentityName(o.Name); err != nil {
		return nil, err
	}
	l, err := newLayout(o.StateDir)
	if err != nil {
		return nil, err
	}
	st, err := l.loadState()
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, errNotRunning
	}
	// The launcher's own kubeconfig, whose address is checked to be this host's.
	cfg, err := restConfig(l.kubeconfig(), st.Ports, 30*time.Second)
	if err != nil {
		return nil, err
	}
	return IssueIdentity(ctx, cfg, l.identitiesDir(), o.Name, renew, o.Log)
}

// IssueIdentity is the work of add (renew false) and renew (renew true) against the API server cfg
// reaches as an administrator, writing identitiesDir/<name>.kubeconfig. The kubeconfig takes cfg's server
// and CA (or its choice to skip verification, which only a test's control plane makes), and the new
// token; it takes nothing of cfg's credential. add refuses an existing ServiceAccount before it requests
// a token or writes anything. When add fails after it made the ServiceAccount, it deletes it again, so
// a retry is an add.
func IssueIdentity(ctx context.Context, cfg *rest.Config, identitiesDir, name string, renew bool, log io.Writer) (*IdentityResult, error) {
	if err := ValidIdentityName(name); err != nil {
		return nil, err
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	sas := cs.CoreV1().ServiceAccounts(AgentsNamespace)
	created := false
	if renew {
		if _, err := sas.Get(ctx, name, metav1.GetOptions{}); apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("%w (no ServiceAccount %s in %s): hpcburst identity add makes it", ErrNoIdentity, name, AgentsNamespace)
		} else if err != nil {
			return nil, fmt.Errorf("read the ServiceAccount %s: %w", name, err)
		}
	} else {
		sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: AgentsNamespace, Labels: map[string]string{"app.kubernetes.io/managed-by": fieldManager},
		}}
		switch _, err := sas.Create(ctx, sa, metav1.CreateOptions{FieldManager: fieldManager}); {
		case apierrors.IsAlreadyExists(err):
			return nil, fmt.Errorf("%w (the ServiceAccount %s is in %s): use hpcburst identity renew %s for a new token", ErrIdentityExists, name, AgentsNamespace, name)
		case apierrors.IsNotFound(err):
			return nil, fmt.Errorf("the namespace %s is not there: hpcburst up installs it, so start the host with this release first (%w)", AgentsNamespace, err)
		case err != nil:
			return nil, fmt.Errorf("create the ServiceAccount %s: %w", name, err)
		}
		created = true
		logf(log, "created ServiceAccount %s in %s", name, AgentsNamespace)
	}
	res, err := issueToken(ctx, cs, cfg, identitiesDir, name, renew)
	if err != nil && created {
		// With a context of its own: the failure may be that ctx ended.
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if derr := sas.Delete(rctx, name, metav1.DeleteOptions{}); derr != nil && !apierrors.IsNotFound(derr) {
			err = fmt.Errorf("%w (and deleting the new ServiceAccount %s failed: %v; hpcburst identity renew %s completes it)", err, name, derr, name)
		}
	}
	if err != nil {
		return nil, err
	}
	logf(log, "wrote %s", res.Kubeconfig)
	return res, nil
}

// issueToken requests the token and writes the kubeconfig.
func issueToken(ctx context.Context, cs kubernetes.Interface, cfg *rest.Config, dir, name string, renew bool) (*IdentityResult, error) {
	secs := IdentityTokenSeconds
	tr, err := cs.CoreV1().ServiceAccounts(AgentsNamespace).CreateToken(ctx, name, &authenticationv1.TokenRequest{
		Spec: authenticationv1.TokenRequestSpec{ExpirationSeconds: &secs},
	}, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("request a token for the ServiceAccount %s: %w", name, err)
	}
	if tr.Status.Token == "" || tr.Status.ExpirationTimestamp.IsZero() {
		return nil, fmt.Errorf("the API server answered the token request for %s without a token or an expiry", name)
	}
	b, err := identityKubeconfigBytes(cfg, name, tr.Status.Token)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	if err := os.Chmod(dir, 0o700); err != nil { // set explicitly, so a umask or an older directory cannot loosen it
		return nil, err
	}
	path := identityFile(dir, name)
	if err := writeFileAtomic(path, b, 0o600); err != nil {
		return nil, fmt.Errorf("write %s: %w", path, err)
	}
	if fi, err := os.Stat(path); err != nil {
		return nil, err
	} else if fi.Mode().Perm() != 0o600 {
		return nil, fmt.Errorf("%s has mode %v, want 0600", path, fi.Mode().Perm())
	}
	return &IdentityResult{
		Name: name, Username: IdentityUsername(name), Kubeconfig: path, Expires: tr.Status.ExpirationTimestamp.UTC(), Renewed: renew,
	}, nil
}

// identityKubeconfigBytes is the kubeconfig of one identity: the server and CA of the admin
// configuration cfg, and the token. The CA is inline, as k3s writes its own.
func identityKubeconfigBytes(cfg *rest.Config, name, token string) ([]byte, error) {
	cluster := &clientcmdapi.Cluster{Server: cfg.Host, TLSServerName: cfg.TLSClientConfig.ServerName}
	ca := cfg.TLSClientConfig.CAData
	if len(ca) == 0 && cfg.TLSClientConfig.CAFile != "" {
		var err error
		if ca, err = os.ReadFile(cfg.TLSClientConfig.CAFile); err != nil {
			return nil, fmt.Errorf("read the CA of the admin kubeconfig: %w", err)
		}
	}
	switch {
	case len(ca) > 0:
		cluster.CertificateAuthorityData = ca
	case cfg.TLSClientConfig.Insecure:
		cluster.InsecureSkipTLSVerify = true
	default:
		return nil, errors.New("the admin kubeconfig carries no CA, so the identity's could not verify the server")
	}
	kc := clientcmdapi.NewConfig()
	kc.Clusters["hpcburst"] = cluster
	kc.AuthInfos[name] = &clientcmdapi.AuthInfo{Token: token}
	kc.Contexts[name] = &clientcmdapi.Context{Cluster: "hpcburst", AuthInfo: name}
	kc.CurrentContext = name
	return clientcmd.Write(*kc)
}
