package slurm

// The content rules of the shipped profiles (HBEP-4, open question 8; profiles/README.md): no host
// name, address, user name, account, grant or job ID is in a profile. Accounts and grants are
// Provider settings, so they never ship in this repository. The scan reads every manifest of
// profiles/, so a profile added there, athena-now.yaml (issue #376) among them, is covered without
// being listed.

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var (
	// An IPv4 address.
	profileIPv4 = regexp.MustCompile(`\b[0-9]{1,3}(?:\.[0-9]{1,3}){3}\b`)
	// An e-mail address (a '@' between names, with a dotted domain: an image digest "name@sha256:..." has none).
	profileEmail = regexp.MustCompile(`[A-Za-z0-9._-]+@[A-Za-z0-9-]+\.[A-Za-z]{2,}`)
	// A login or ssh host name.
	profileLoginHost = regexp.MustCompile(`\b(?:login|ssh)[0-9]*\.[a-z0-9-]+(?:\.[a-z0-9-]+)*\.[a-z]{2,}\b`)
	// A user or grant name of the PLGrid form: "plg" followed by more name, which is not a partition or
	// QOS ("plgrid", "plgrid-now", "plgrid_now_conformance" and so on).
	profilePlgName = regexp.MustCompile(`\bplg[a-z0-9_-]*\b`)
	// A job ID: "job" and a number of five or more digits.
	profileJobID = regexp.MustCompile(`(?i)\bjob(?: id)?[ :=#]+[0-9]{5,}\b`)
)

// identifiersIn returns what a manifest's text holds that the content rules forbid.
func identifiersIn(text string) []string {
	var found []string
	for _, re := range []*regexp.Regexp{profileIPv4, profileEmail, profileLoginHost, profileJobID} {
		found = append(found, re.FindAllString(text, -1)...)
	}
	for _, tok := range profilePlgName.FindAllString(text, -1) {
		if !strings.HasPrefix(tok, "plgrid") {
			found = append(found, tok)
		}
	}
	return found
}

func TestShippedProfilesHoldNoIdentifiers(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("..", "..", "profiles", "*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no shipped profiles found: %v", err) // never a skip
	}
	seen := map[string]bool{}
	for _, f := range files {
		seen[filepath.Base(f)] = true
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if got := identifiersIn(string(raw)); len(got) != 0 {
			t.Errorf("%s holds identifiers the content rules forbid: %q", filepath.Base(f), got)
		}
	}
	for _, want := range []string{"helios.yaml", "athena.yaml", "athena-now.yaml", "llmlab.yaml"} {
		if !seen[want] {
			t.Errorf("the scan did not read profiles/%s", want)
		}
	}
}

// The scan finds what it is there for: a planted identifier of each kind is found, and a
// partition, a QOS and a digest are not taken for one.
func TestTheIdentifierScanFindsEachKindAndSparesTheProfilesNames(t *testing.T) {
	for _, tc := range []struct {
		name, text string
		want       int
	}{
		{"an address", "host: 192.0.2.7", 1},
		{"an e-mail address", "contact: someone@example.org", 1},
		{"a login host", "ssh: login01.example.org", 1},
		{"a user or grant name", "account: plgexampleuser-gpu-a100", 1},
		{"a job ID", "from job 1234567 of the run", 1},
		{"partitions and QOSes of the profiles", "plgrid-gpu-a100 plgrid-now plgrid plgrid_now_conformance plgrid-services", 0},
		{"an image digest", "image docker.io/example/vllm-openai@sha256:0123456789abcdef", 0},
		{"a documentation URL", "https://docs.example.org/pages", 0},
		{"a 4-digit number", "job 1234 is not an ID", 0},
	} {
		if got := identifiersIn(tc.text); len(got) != tc.want {
			t.Errorf("%s: found %q, want %d", tc.name, got, tc.want)
		}
	}
}
