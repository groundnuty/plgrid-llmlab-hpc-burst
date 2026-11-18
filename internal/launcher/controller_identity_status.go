package launcher

// What status says about the controllers' own identities (HBEP-10, Audit; issue #660, PR A): each of the
// three kubeconfigs up issued (controller_identities.go), with its token's expiry, whether the API server
// accepts it, and a WARNING once 30 days or fewer are left (ControllerTokenWarnWithin). There is no
// rotation: a token that runs out is replaced by the next start (down, then up). An identity whose token is
// expiring, expired or rejected, or whose file is missing, is a reported state, never a host failure: the
// host is as healthy as the other checks found it, and status exits 0, as for the agents' identities
// (identity_status.go). Each is asked through its OWN kubeconfig, never the admin's, so the answer is also
// the proof that the file authenticates as that ServiceAccount.

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// ControllerIdentityState is one controller identity: its kubeconfig file, its token's expiry and whether the
// API server accepts it.
type ControllerIdentityState struct {
	Name       string `json:"name"`
	Kubeconfig string `json:"kubeconfig"`
	// Expires is the expiry the token itself carries (its exp claim). Nil when there is no file, or none
	// could be read from it.
	Expires *time.Time `json:"expires,omitempty"`
	// Expiring is true when the token has ControllerTokenWarnWithin or less left, or has expired: status
	// warns, and the next down and up issue a new one.
	Expiring bool `json:"expiring,omitempty"`
	// Warning is the status phrase of an expiring token: the days left, and what to do. Empty otherwise.
	// It is computed at the check's own clock, so a printed line never reads another.
	Warning string `json:"warning,omitempty"`
	// Valid is true when the API server accepts the token: a SelfSubjectReview made with this kubeconfig
	// names this ServiceAccount. Detail says why not otherwise.
	Valid  bool   `json:"valid"`
	Detail string `json:"detail,omitempty"`
}

// tokenWarning is the status phrase of an expiring token, which names what to do: the tokens are issued at
// every start, and nothing rotates them.
func (s ControllerIdentityState) tokenWarning(now time.Time) string {
	if !s.Expiring || s.Expires == nil {
		return ""
	}
	left := s.Expires.Sub(now)
	if left <= 0 {
		return fmt.Sprintf("the token expired at %s: hpcburst down, then up, issues new ones", s.Expires.Format(time.RFC3339))
	}
	remaining := plural(int(left/(24*time.Hour)), "day")
	if left < 24*time.Hour {
		remaining = plural(int(left/time.Hour), "hour")
	}
	return fmt.Sprintf("%s left, within the %d days that status warns at: hpcburst down, then up, issues new tokens", remaining, int(ControllerTokenWarnWithin/(24*time.Hour)))
}

func plural(n int, unit string) string {
	if n == 1 {
		return "1 " + unit
	}
	return fmt.Sprintf("%d %ss", n, unit)
}

// controllerIdentityAsk answers for one controller kubeconfig: the user name the API server reports for its
// token. A test replaces it; production asks the API server (askControllerToken).
type controllerIdentityAsk func(ctx context.Context, kubeconfig string, ports Ports) (user string, err error)

// CheckControllerIdentities reads the controller kubeconfigs under l and asks the API server, through each
// one, who its token belongs to. now is the clock behind the expiry rule: a test injects one, and the
// warning never reads the real clock. A missing or unreadable file, a 401 and a token for another user are
// states with a Detail. Only the context ending is an error, since then no answer says anything.
func CheckControllerIdentities(ctx context.Context, l layout, ports Ports, now time.Time) ([]ControllerIdentityState, error) {
	return checkControllerIdentities(ctx, l, ports, now, askControllerToken)
}

func checkControllerIdentities(ctx context.Context, l layout, ports Ports, now time.Time, ask controllerIdentityAsk) ([]ControllerIdentityState, error) {
	var out []ControllerIdentityState
	for _, name := range ControllerIdentityNames() {
		s := ControllerIdentityState{Name: name, Kubeconfig: l.controllerKubeconfig(name)}
		if _, err := os.Stat(s.Kubeconfig); err != nil {
			s.Detail = fmt.Sprintf("no kubeconfig file (%v): a host that an earlier release started has none; hpcburst down, then up, issues them", err)
			out = append(out, s)
			continue
		}
		cfg, err := restConfig(s.Kubeconfig, ports, identityAskTimeout)
		if err != nil {
			s.Detail = "unreadable: " + err.Error()
			out = append(out, s)
			continue
		}
		if exp, err := tokenExpiry(cfg.BearerToken); err == nil {
			s.Expires = &exp
			s.Expiring = exp.Sub(now) <= ControllerTokenWarnWithin
			s.Warning = s.tokenWarning(now)
		}
		user, err := ask(ctx, s.Kubeconfig, ports)
		switch {
		case apierrors.IsUnauthorized(err):
			s.Detail = "the API server rejected the token (401); hpcburst down, then up, issues a new one"
			if s.Expires != nil && !s.Expires.After(now) {
				s.Detail = fmt.Sprintf("the token expired at %s and the API server rejected it; hpcburst down, then up, issues a new one", s.Expires.Format(time.RFC3339))
			}
		case err != nil && ctx.Err() != nil:
			return nil, fmt.Errorf("ask the API server who the token of %s is: %w", name, err)
		case err != nil:
			s.Detail = "unreadable: " + err.Error()
		case user != serviceAccountUsername(v1alpha1.ControllerNamespace, name):
			s.Detail = fmt.Sprintf("the kubeconfig authenticates as %q, not as %q", user, serviceAccountUsername(v1alpha1.ControllerNamespace, name))
		default:
			s.Valid = true
		}
		out = append(out, s)
	}
	return out, nil
}

// askControllerToken asks the API server one SelfSubjectReview with the kubeconfig at path, which restConfig
// reads, so one that names another server is refused. It never returns the token.
func askControllerToken(ctx context.Context, path string, ports Ports) (string, error) {
	cfg, err := restConfig(path, ports, identityAskTimeout)
	if err != nil {
		return "", err
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return "", err
	}
	review, err := cs.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authenticationv1.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil {
		return "", err
	}
	return review.Status.UserInfo.Username, nil
}

// statusControllerIdentities sets rep.ControllerIdentities, under the same condition as statusAgents: only
// for a host judged healthy by every other check, and only where controllers run. An identity that is
// expiring, expired, rejected or missing changes nothing about the host's health; a failure to ask does.
func statusControllerIdentities(ctx context.Context, l layout, st *State, now time.Time, rep *StatusReport, bad func(string, ...any)) {
	if st.Controllers == nil || rep.Health != Healthy || rep.Readyz == nil || !*rep.Readyz {
		return
	}
	ids, err := CheckControllerIdentities(ctx, l, st.Ports, now)
	if err != nil {
		bad("the controllers' identities could not be checked: %v", err)
		return
	}
	rep.ControllerIdentities = ids
}

// ControllerIdentityLine is the status text of a controller identity: its name, whether the API server
// accepts it, its expiry, and a WARNING line when 30 days or fewer are left. A kubeconfig path is named, never
// a token.
func ControllerIdentityLine(s ControllerIdentityState) string {
	exp := "expiry unknown"
	if s.Expires != nil {
		exp = "expires " + s.Expires.Format(time.RFC3339)
	}
	var b strings.Builder
	if s.Valid {
		fmt.Fprintf(&b, "controller identity %s: valid, %s (%s)", s.Name, exp, s.Kubeconfig)
	} else {
		fmt.Fprintf(&b, "controller identity %s: NOT VALID, %s (%s)", s.Name, exp, s.Detail)
	}
	if s.Warning != "" {
		fmt.Fprintf(&b, "\nWARNING: controller identity %s: %s", s.Name, s.Warning)
	}
	return b.String()
}
