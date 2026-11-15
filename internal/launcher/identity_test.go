package launcher

// The identity commands' parts that need no API server (identity.go, identity_status.go; issue #589,
// PR 2): the name check, what the kubeconfig holds, and the expiry read from a token. What the API
// server then makes of the token is judged where there is one: api/v1alpha1/e2e and cmd/hpcburst.

import (
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func TestValidIdentityName(t *testing.T) {
	for _, ok := range []string{"a", "agent-a", "agent-42", "0", strings.Repeat("a", 63)} {
		if err := ValidIdentityName(ok); err != nil {
			t.Errorf("%q: %v, want it accepted", ok, err)
		}
	}
	for _, bad := range []string{"", "Agent", "agent_a", "agent.a", "-a", "a-", "a b", "../a", "a/b", strings.Repeat("a", 64)} {
		if err := ValidIdentityName(bad); !errors.Is(err, ErrIdentityName) {
			t.Errorf("%q: %v, want ErrIdentityName", bad, err)
		}
	}
}

// The kubeconfig of an identity holds the server and the CA of the admin configuration, and the new
// token, and none of the admin's credential, which k3s's own kubeconfig holds as a client certificate.
func TestIdentityKubeconfigHoldsTheServerTheCAAndTheTokenOnly(t *testing.T) {
	ca := []byte("-----BEGIN CERTIFICATE-----\nZXhhbXBsZSBjYQ==\n-----END CERTIFICATE-----\n")
	admin := &rest.Config{
		Host:            "https://127.0.0.1:28462",
		BearerToken:     "admin-bearer-token",
		TLSClientConfig: rest.TLSClientConfig{CAData: ca, CertData: []byte("admin-cert"), KeyData: []byte("admin-key"), ServerName: "example.test"},
	}
	b, err := identityKubeconfigBytes(admin, "agent-a", "identity-token")
	if err != nil {
		t.Fatal(err)
	}
	for _, leak := range []string{"admin-bearer-token", "admin-cert", "admin-key", "YWRtaW4"} { // the last: base64 of "admin"
		if strings.Contains(string(b), leak) {
			t.Errorf("the identity's kubeconfig holds %q, which is the admin's credential:\n%s", leak, b)
		}
	}
	kc, err := clientcmd.Load(b)
	if err != nil {
		t.Fatal(err)
	}
	cx := kc.Contexts[kc.CurrentContext]
	if cx == nil {
		t.Fatalf("no current context in %s", b)
	}
	cluster, user := kc.Clusters[cx.Cluster], kc.AuthInfos[cx.AuthInfo]
	if cluster.Server != admin.Host || string(cluster.CertificateAuthorityData) != string(ca) || cluster.TLSServerName != "example.test" || cluster.InsecureSkipTLSVerify {
		t.Errorf("the cluster is %+v, want the admin's server and CA", cluster)
	}
	if user.Token != "identity-token" || len(user.ClientCertificateData) != 0 || len(user.ClientKeyData) != 0 || user.ClientCertificate != "" || user.Username != "" {
		t.Errorf("the user is %+v, want the identity's token alone", user)
	}
	// The result is a whole kubeconfig in the launcher's own sense, so restConfig could read it.
	if err := resolved(kc); err != nil {
		t.Errorf("the kubeconfig is not complete: %v", err)
	}
}

func TestIdentityKubeconfigCASources(t *testing.T) {
	// A CA named by a file is read and put inline: the kubeconfig must not depend on the admin's files.
	caFile := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(caFile, []byte("example-ca-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	b, err := identityKubeconfigBytes(&rest.Config{Host: "https://127.0.0.1:28462", TLSClientConfig: rest.TLSClientConfig{CAFile: caFile}}, "agent-a", "t")
	if err != nil {
		t.Fatal(err)
	}
	if kc, err := clientcmd.Load(b); err != nil || string(kc.Clusters["hpcburst"].CertificateAuthorityData) != "example-ca-bytes" || strings.Contains(string(b), caFile) {
		t.Errorf("a CA file was not inlined: %v\n%s", err, b)
	}
	// A control plane that skips verification (a test's) passes that on.
	b, err = identityKubeconfigBytes(&rest.Config{Host: "https://127.0.0.1:28462", TLSClientConfig: rest.TLSClientConfig{Insecure: true}}, "agent-a", "t")
	if err != nil {
		t.Fatal(err)
	}
	if kc, _ := clientcmd.Load(b); !kc.Clusters["hpcburst"].InsecureSkipTLSVerify {
		t.Errorf("a source that skips verification gave a kubeconfig that verifies:\n%s", b)
	}
	// No CA and no choice to skip: refused, since the kubeconfig could not verify the server.
	if _, err := identityKubeconfigBytes(&rest.Config{Host: "https://127.0.0.1:28462"}, "agent-a", "t"); err == nil {
		t.Errorf("a source with no CA gave a kubeconfig")
	}
	if _, err := identityKubeconfigBytes(&rest.Config{Host: "https://127.0.0.1:28462", TLSClientConfig: rest.TLSClientConfig{CAFile: filepath.Join(t.TempDir(), "absent")}}, "agent-a", "t"); err == nil {
		t.Errorf("a source whose CA file is absent gave a kubeconfig")
	}
}

// jwt builds the compact form of a token with the given payload; the header and signature are filler.
func jwt(payload string) string {
	enc := base64.RawURLEncoding.EncodeToString
	return enc([]byte(`{"alg":"RS256"}`)) + "." + enc([]byte(payload)) + "." + enc([]byte("signature"))
}

func TestTokenExpiryReadsTheExpClaim(t *testing.T) {
	// 1798000000 is 2026-12-23T04:26:40Z; the number is written out, not computed.
	got, err := tokenExpiry(jwt(`{"iss":"example","exp":1798000000,"sub":"system:serviceaccount:hpcburst-agents:agent-a"}`))
	if want := time.Date(2026, 12, 23, 4, 26, 40, 0, time.UTC); err != nil || !got.Equal(want) {
		t.Errorf("tokenExpiry = %v, %v, want %v", got, err, want)
	}
	for name, tok := range map[string]string{
		"not a JWT":                    "abc",
		"two parts":                    "a.b",
		"a payload that is not base64": "a.!!!.c",
		"a payload that is not JSON":   jwt("not json"),
		"no exp":                       jwt(`{"iss":"example"}`),
		"a zero exp":                   jwt(`{"exp":0}`),
		"an empty token":               "",
	} {
		if got, err := tokenExpiry(tok); err == nil {
			t.Errorf("%s: tokenExpiry = %v, want an error", name, got)
		}
	}
}
