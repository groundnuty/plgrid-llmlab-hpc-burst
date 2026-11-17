package agentcli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The groups the API server gives a ServiceAccount's namespace (HBEP-10, Identities and roles).
const (
	agentsGroup      = "system:serviceaccounts:hpcburst-agents"
	controllersGroup = "system:serviceaccounts:hpcburst-system"
	controllersUser  = "system:serviceaccount:hpcburst-system:"
)

// requestTimeout bounds one request of the commands to the API server.
const requestTimeout = 30 * time.Second

// errNoKubeconfig is the usage error of a command with no credential to act with.
var errNoKubeconfig = errors.New("no kubeconfig: give --kubeconfig <file> or set KUBECONFIG; hpcburst acts as the caller and never falls back to another kubeconfig")

// errController is the refusal of a controller's credential.
var errController = errors.New("a controller's credential is outside the capacity guard; use your own identity")

// usageError marks an error the command reports as a usage error (exit 2).
type usageError struct{ error }

func (u usageError) Unwrap() error { return u.error }

// caller is who runs a command: the client that acts as them, and the user name and groups the API server
// reports for them. Every write of the commands goes through c.
type caller struct {
	c         client.Client
	user      string
	groups    []string
	namespace string // the kubeconfig's context namespace; "default" when it names none
}

// loadCaller is the one place a command takes its credential. It reads --kubeconfig, else the KUBECONFIG
// variable the process was given, and nothing else: not ~/.kube/config, not the in-cluster configuration,
// not the launcher's state directory, and not client-go's default loading rules, which reach all three.
// The file is loaded by itself and turned into a client configuration directly, because the deferred
// loader falls back to the in-cluster configuration when a file is empty. One SelfSubjectReview then says
// who the caller is. A controller's ServiceAccount is refused there, not by path; any identity outside
// the agents' group is allowed with a note on stderr, since the person acts with the admin kubeconfig.
func loadCaller(ctx context.Context, flagPath, envPath string, stderr io.Writer) (*caller, error) {
	path := flagPath
	if path == "" {
		path = envPath
	}
	if path == "" {
		return nil, usageError{errNoKubeconfig}
	}
	if strings.ContainsRune(path, os.PathListSeparator) {
		return nil, usageError{errors.New("KUBECONFIG names more than one file: give --kubeconfig <file> with the one to use")}
	}
	cfg, err := clientcmd.LoadFromFile(path)
	if err != nil {
		return nil, kubeconfigFault(path, err)
	}
	if err := resolveCredentialPaths(cfg, path); err != nil {
		return nil, kubeconfigUnusable(path)
	}
	cc := clientcmd.NewDefaultClientConfig(*cfg, &clientcmd.ConfigOverrides{})
	rc, err := cc.ClientConfig()
	if err != nil {
		return nil, kubeconfigUnusable(path)
	}
	ns, _, err := cc.Namespace()
	if err != nil {
		return nil, kubeconfigUnusable(path)
	}
	rc.Timeout, rc.UserAgent = requestTimeout, "hpcburst"
	user, groups, err := whoAmI(ctx, rc)
	if err != nil {
		return nil, err
	}
	if user == "" {
		return nil, errors.New("the API server reported no user name for this kubeconfig (SelfSubjectReview)")
	}
	if isController(user, groups) {
		return nil, errController
	}
	if !slices.Contains(groups, agentsGroup) {
		fmt.Fprintf(stderr, "hpcburst: acting as %s, outside the capacity guard\n", user)
	}
	s, err := scheme()
	if err != nil {
		return nil, err
	}
	c, err := client.New(rc, client.Options{Scheme: s})
	if err != nil {
		return nil, fmt.Errorf("make the API client: %w", err)
	}
	return &caller{c: c, user: user, groups: groups, namespace: ns}, nil
}

// kubeconfigFault is the message for a kubeconfig that cannot be read or parsed. It names the file and the kind
// of fault and never the text of the error: client-go's parse errors can quote the file, and the file holds
// the caller's credentials (two users of one name make it print the whole list of users, bearer tokens
// included).
func kubeconfigFault(path string, err error) error {
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("the kubeconfig %s does not exist", path)
	case errors.Is(err, fs.ErrPermission):
		return fmt.Errorf("the kubeconfig %s cannot be read: permission denied", path)
	case clientcmd.IsEmptyConfig(err):
		return fmt.Errorf("the kubeconfig %s holds no configuration", path)
	}
	return fmt.Errorf("the kubeconfig %s is not a valid kubeconfig (what is wrong with it is not shown, because the file holds credentials)", path)
}

// kubeconfigUnusable is the message for a kubeconfig that parses but names no cluster, user or context that
// fits together. Like kubeconfigFault it never prints the error of client-go.
func kubeconfigUnusable(path string) error {
	return fmt.Errorf("the kubeconfig %s is not usable: its current context, cluster or user is missing or invalid (what is wrong with it is not shown, because the file holds credentials)", path)
}

// whoAmI asks the API server who the configuration's credential is: one SelfSubjectReview.
func whoAmI(ctx context.Context, rc *rest.Config) (string, []string, error) {
	cs, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return "", nil, fmt.Errorf("make the API client: %w", err)
	}
	r, err := cs.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authenticationv1.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil {
		return "", nil, fmt.Errorf("ask the API server who you are (SelfSubjectReview): %w", err)
	}
	return r.Status.UserInfo.Username, slices.Clone(r.Status.UserInfo.Groups), nil
}

// isController reports whether the identity is a ServiceAccount of the controllers' namespace.
func isController(user string, groups []string) bool {
	return strings.HasPrefix(user, controllersUser) || slices.Contains(groups, controllersGroup)
}

func scheme() (*runtime.Scheme, error) {
	s := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(s); err != nil {
		return nil, err
	}
	return s, nil
}

// resolveCredentialPaths makes every file that a kubeconfig names, a CA, a client certificate or key, a token
// file, an absolute path against the kubeconfig's own directory. client-go's loading rules do that, and
// LoadFromFile alone does not: the relative path would then be read from the process's working directory, so a
// stray file there could make the command act as another identity. A reference that is still relative after the
// resolution is refused, so the working directory is never read.
func resolveCredentialPaths(cfg *clientcmdapi.Config, path string) error {
	for _, c := range cfg.Clusters {
		if c.LocationOfOrigin == "" {
			c.LocationOfOrigin = path
		}
	}
	for _, a := range cfg.AuthInfos {
		if a.LocationOfOrigin == "" {
			a.LocationOfOrigin = path
		}
	}
	if err := clientcmd.ResolveLocalPaths(cfg); err != nil {
		return err
	}
	var refs []*string
	for _, c := range cfg.Clusters {
		refs = append(refs, clientcmd.GetClusterFileReferences(c)...)
	}
	for _, a := range cfg.AuthInfos {
		refs = append(refs, clientcmd.GetAuthInfoFileReferences(a)...)
	}
	for _, ref := range refs {
		if *ref != "" && !filepath.IsAbs(*ref) {
			return errors.New("a credential file is named by a path that cannot be resolved")
		}
	}
	return nil
}
