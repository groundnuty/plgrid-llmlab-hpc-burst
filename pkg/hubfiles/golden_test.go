package hubfiles

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
)

// The golden test reads two public checkpoints from the real Hugging Face
// Hub and compares the result with the references of spike S7 (spikes/s7,
// committed in this repository). It needs the network, so it runs only with
// HPCBURST_TEST_HUB=1.
//
// The expected values are S7's, none of them produced by this package:
//   - the repository ids and revisions: spikes/s7/checkpoints.env;
//   - the Hub's listing as S7 fetched it with a Python script
//     (results/run1/hub/*.json): the file list, sizes, git blob ids and, for
//     LFS files, lfs.oid;
//   - the SHA-256 of every staged file as S7's Python hashed it on a cluster
//     node (results/run1/hash-*.jsonl). For a non-LFS file the Hub publishes no
//     SHA-256, so this is the only independent reference.
const goldenGate = "HPCBURST_TEST_HUB"

var s7Dir = filepath.Join("..", "..", "spikes", "s7")

type s7Listing struct {
	Repo     string `json:"repo"`
	Revision string `json:"revision"`
	Files    []struct {
		Path      string  `json:"path"`
		Size      int64   `json:"size"`
		GitOID    string  `json:"git_oid"`
		LFSSHA256 *string `json:"lfs_sha256"`
	} `json:"files"`
}

var envLine = regexp.MustCompile(`^CKPT_([a-z]+)_(REPO|REV)="([^"]+)"$`)

// s7Checkpoint returns the repository and revision of a label from
// checkpoints.env.
func s7Checkpoint(t *testing.T, label string) (repo, rev string) {
	t.Helper()
	f, err := os.Open(filepath.Join(s7Dir, "checkpoints.env"))
	if err != nil {
		t.Fatalf("S7's checkpoint list: %v", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if m := envLine.FindStringSubmatch(sc.Text()); m != nil && m[1] == label {
			if m[2] == "REPO" {
				repo = m[3]
			} else {
				rev = m[3]
			}
		}
	}
	if repo == "" || rev == "" {
		t.Fatalf("checkpoints.env has no repository and revision for %q", label)
	}
	return repo, rev
}

// s7Hashes returns path -> SHA-256 from S7's hash records.
func s7Hashes(t *testing.T, label string) map[string]string {
	t.Helper()
	f, err := os.Open(filepath.Join(s7Dir, "results", "run1", "hash-"+label+".jsonl"))
	if err != nil {
		t.Fatalf("S7's hash records: %v", err)
	}
	defer f.Close()
	out := map[string]string{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var rec struct {
			Kind   string `json:"kind"`
			Path   string `json:"path"`
			SHA256 string `json:"sha256"`
		}
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatalf("S7 hash record: %v", err)
		}
		if rec.Kind == "file" {
			out[rec.Path] = rec.SHA256
		}
	}
	return out
}

// countingTransport counts the requests that reach the Hub.
type countingTransport struct {
	mu        sync.Mutex
	listing   int // tree requests, first pages and follow-up pages
	followUps int // tree requests that carry a cursor
	downloads int
	paths     []string // downloaded paths, decoded
}

func (c *countingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c.mu.Lock()
	switch {
	case strings.Contains(r.URL.Path, "/tree/"):
		c.listing++
		if r.URL.Query().Get("cursor") != "" {
			c.followUps++
		}
	case strings.Contains(r.URL.Path, "/resolve/"):
		c.downloads++
		c.paths = append(c.paths, r.URL.Path[strings.Index(r.URL.Path, "/resolve/")+len("/resolve/")+41:])
	}
	c.mu.Unlock()
	return http.DefaultTransport.RoundTrip(r)
}

func TestGoldenHub(t *testing.T) {
	if os.Getenv(goldenGate) != "1" {
		t.Skipf("SKIPPED: the golden test against the real Hugging Face Hub needs the network; run it with %s=1", goldenGate)
	}
	for _, tc := range []struct {
		label    string
		pageSize int
	}{
		// DeepSeek has 74 files. Pages of 20 entries make the Hub paginate, so
		// the Link chain is followed for real.
		{"deepseek", 20},
		// Gemma has 18 files. Pages of 5 paginate it too; the default page size
		// is the second run below.
		{"gemma", 5},
		{"gemma", 0},
	} {
		name := tc.label
		if tc.pageSize == 0 {
			name += "-default-page-size"
		}
		t.Run(name, func(t *testing.T) { goldenCheckpoint(t, tc.label, tc.pageSize) })
	}
}

func goldenCheckpoint(t *testing.T, label string, pageSize int) {
	repo, rev := s7Checkpoint(t, label)
	raw, err := os.ReadFile(filepath.Join(s7Dir, "results", "run1", "hub", label+".json"))
	if err != nil {
		t.Fatalf("S7's Hub listing: %v", err)
	}
	var ref s7Listing
	if err := json.Unmarshal(raw, &ref); err != nil {
		t.Fatal(err)
	}
	if ref.Repo != repo || ref.Revision != rev {
		t.Fatalf("S7's listing is of %s@%s, checkpoints.env says %s@%s", ref.Repo, ref.Revision, repo, rev)
	}
	hashes := s7Hashes(t, label)
	if len(hashes) != len(ref.Files) {
		t.Fatalf("S7 hashed %d files, its listing has %d", len(hashes), len(ref.Files))
	}
	lfs := 0
	for _, f := range ref.Files {
		if hashes[f.Path] == "" {
			t.Fatalf("S7 has no hash for %q", f.Path)
		}
		if f.LFSSHA256 != nil {
			lfs++
			if *f.LFSSHA256 != hashes[f.Path] {
				t.Fatalf("S7's own data disagrees on %q: Hub lfs.oid %s, staged hash %s", f.Path, *f.LFSSHA256, hashes[f.Path])
			}
		}
	}

	counter := &countingTransport{}
	c, err := New(Config{PageSize: pageSize, HTTPClient: &http.Client{Transport: counter}})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := c.Files(context.Background(), repo, rev)
	if err != nil {
		t.Fatalf("Files(%s, %s): %v", repo, rev, err)
	}

	// The set of files equals S7's, not only its size.
	var wantPaths []string
	for _, f := range ref.Files {
		wantPaths = append(wantPaths, f.Path)
	}
	slices.Sort(wantPaths)
	if got := paths(entries); !slices.Equal(got, wantPaths) {
		t.Fatalf("%s: %d files, S7 listed %d; the sets differ\n got  %q\n want %q", label, len(got), len(wantPaths), got, wantPaths)
	}
	if len(entries) != len(ref.Files) {
		t.Errorf("%s: %d files, want %d", label, len(entries), len(ref.Files))
	}

	byPath := map[string]Entry{}
	for _, e := range entries {
		byPath[e.Path] = e
	}
	for _, f := range ref.Files {
		e := byPath[f.Path]
		if e.SHA256 != hashes[f.Path] {
			t.Errorf("%s: sha256 = %s, S7 hashed %s", f.Path, e.SHA256, hashes[f.Path])
		}
		if e.BlobID != f.GitOID {
			t.Errorf("%s: blob id = %s, S7's listing has %s", f.Path, e.BlobID, f.GitOID)
		}
		if wantLFS := f.LFSSHA256 != nil; e.LFS != wantLFS {
			t.Errorf("%s: LFS = %v, want %v", f.Path, e.LFS, wantLFS)
		}
		wantSize := f.Size
		if e.Size != wantSize {
			t.Errorf("%s: size = %d, S7's listing has %d", f.Path, e.Size, wantSize)
		}
	}

	// Only the non-LFS files were downloaded, one request each.
	counter.mu.Lock()
	defer counter.mu.Unlock()
	if want := len(ref.Files) - lfs; counter.downloads != want {
		t.Errorf("%d downloads, want %d (the non-LFS files)", counter.downloads, want)
	}
	for _, p := range counter.paths {
		if e := byPath[p]; e.LFS {
			t.Errorf("the LFS file %q was downloaded", p)
		}
	}
	if pageSize > 0 {
		// 18 or 74 files, plus directories, in pages of pageSize entries.
		minPages := (len(ref.Files) + pageSize - 1) / pageSize
		if counter.listing < minPages || counter.followUps != counter.listing-1 || counter.followUps == 0 {
			t.Errorf("%d listing requests (%d follow-ups), want at least %d pages", counter.listing, counter.followUps, minPages)
		}
	}
	t.Logf("%s@%s: %d files (%d LFS), %d listing requests (%d followed Link), %d downloads; all equal to S7",
		repo, rev[:12], len(entries), lfs, counter.listing, counter.followUps, counter.downloads)
}
