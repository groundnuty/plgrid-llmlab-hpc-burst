package e2e

import (
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

const providerNameMessage = "a Provider name must be a DNS-1123 label"

// The rule reads the object's own name. The API server's generic name check
// already refuses uppercase letters, '@', '/' and a leading or trailing '-', so
// only a name with '.' or one of 64 characters shows that the rule itself works.
func TestProviderNamesAreDNS1123Labels(t *testing.T) {
	refused := map[string]string{
		"dot":         "helios.a",
		"dots":        "helios.a.b",
		"64 chars":    strings.Repeat("a", 64),
		"64 digits":   strings.Repeat("1", 64),
		"label+dot63": strings.Repeat("a", 30) + "." + strings.Repeat("b", 30),
	}
	for what, name := range refused {
		t.Run("refuse "+what, func(t *testing.T) {
			err := c.Create(bg(), newProvider(name))
			requireRefused(t, err, providerNameMessage)
		})
	}
	// Names with '@', '/' or uppercase letters never reach the rule: the API
	// server refuses them first. They must still be refused.
	for _, name := range []string{"Helios-a", "helios@a", "helios/a", "helios_a", "-helios", "helios-"} {
		t.Run("refuse "+name, func(t *testing.T) {
			if err := c.Create(bg(), newProvider(name)); err == nil {
				t.Fatalf("Provider %q was accepted", name)
			}
		})
	}
	accepted := map[string]string{
		"simple":     uniq("helios-a"),
		"digits":     uniq("h1"),
		"63 chars":   "a" + strings.Repeat("b", 61) + "c",
		"single":     "x",
		"hyphenated": uniq("a-b-c"),
	}
	for what, name := range accepted {
		t.Run("accept "+what, func(t *testing.T) {
			p := newProvider(name)
			must(t, c.Create(bg(), p), "create Provider "+name)
			cleanup(t, p)
		})
	}
}

func TestProviderCredentialsReferenceNamesExactlyOneSource(t *testing.T) {
	const want = "name exactly one of file and secretRef"
	both := newProvider(uniq("prov"))
	both.Spec.CredentialsRef = &v1alpha1.CredentialsReference{File: "/run/x",
		SecretRef: &v1alpha1.SecretKeyReference{Name: "s", Namespace: "n", Key: "k"}}
	requireRefused(t, c.Create(bg(), both), want)

	neither := newProvider(uniq("prov"))
	neither.Spec.CredentialsRef = &v1alpha1.CredentialsReference{}
	requireRefused(t, c.Create(bg(), neither), want)

	file := newProvider(uniq("prov"))
	must(t, c.Create(bg(), file), "a file reference")
	cleanup(t, file)

	secret := newProvider(uniq("prov"))
	secret.Spec.CredentialsRef = &v1alpha1.CredentialsReference{
		SecretRef: &v1alpha1.SecretKeyReference{Name: "s", Namespace: "n", Key: "k"}}
	must(t, c.Create(bg(), secret), "a Secret reference")
	cleanup(t, secret)

	none := newProvider(uniq("prov"))
	none.Spec.CredentialsRef = nil
	must(t, c.Create(bg(), none), "no credentials reference (a static provider needs none)")
	cleanup(t, none)
}
