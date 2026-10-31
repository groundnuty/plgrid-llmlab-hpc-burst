// Package stage stages an Instance's image and checkpoint under the
// platform's staging directory (HBEP-7, What it runs: Staging), for the
// allocation agent.
//
//   - The image is pulled by digest into <stagingDir>/images/ in the
//     partition's format (agentapi.StagedImagePath): a SIF file with
//     "apptainer pull", or a sandbox directory built from the same digest with
//     "apptainer build --sandbox" where compute nodes cannot run a SIF (spike
//     S1, probe 2).
//   - Missing checkpoint files are downloaded from the Hugging Face Hub at the
//     full commit id into <stagingDir>/checkpoints/<repository>/<revision>/,
//     each checked against the identity record's SHA-256 as it streams.
//   - Every listed file present is rehashed (HBEP-9: rehash at every start).
//     On a whole-Instance restart (Restage) a file whose rehash fails is moved
//     aside and downloaded again.
//
// Each image and file goes to a temporary name in its final directory and is
// renamed into place, so two Allocations staging at once never see a partial
// file.
//
// An image is trusted only against an integrity record written when it was
// pulled or built, and pulled or built again when it fails the check
// (image.go, tree.go). A checkpoint file that needs a download or a restage is
// handled under an exclusive flock on the checkpoint directory, after a fresh
// look at it, so concurrent agents neither download a file twice nor move a
// good replacement aside on a stale result (lock.go, checkpoint.go). Staging's
// Apptainer runs in a process group of its own that staging owns until every
// member is gone (run.go).
//
// The rehash follows spike S7's recommendation (spikes/s7/README.md,
// Recommendation): several parallel readers (4 by default), each reading
// whole files through one reused 8 MiB buffer, never a read sized by a cap,
// whose read-ahead over-read Lustre by up to 190 times (L-12.5).
//
// Which listed files are staged depends on the launch's --load-format
// (skip.go): a listed file the engine does not load is not downloaded, and one
// that is present is still rehashed.
//
// The package imports only the standard library, package agentapi, the
// agent's own package and its procgroup package.
package stage

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
)

// DefaultHubEndpoint is the Hugging Face Hub.
const DefaultHubEndpoint = "https://huggingface.co"

// Config configures a Stager.
type Config struct {
	// StagingDir is the profile's resolved, absolute staging directory.
	StagingDir string
	// Apptainer is the apptainer binary.
	Apptainer string
	// TmpDir is where Apptainer builds (APPTAINER_TMPDIR): the job's own
	// temporary space.
	TmpDir string
	// HubEndpoint is the Hub's base URL; DefaultHubEndpoint when empty.
	HubEndpoint string
	// HTTPClient downloads from the Hub; a client with a 30 s response-header
	// time-out when nil.
	HTTPClient *http.Client
	// Readers is how many files are rehashed at once; 4 when 0.
	Readers int
	// BaseEnv is Apptainer's environment.
	BaseEnv []string
	// RetryBackoff is the wait before a failed download's second try, and twice
	// that before its third; 2 s when 0.
	RetryBackoff time.Duration
	// StopGrace is how long a cancelled Apptainer run has between TERM and
	// KILL to its process group; 5 s when 0.
	StopGrace time.Duration
	// Log receives Apptainer's output and the stager's own notices.
	Log io.Writer
}

// Stager implements agent.Stager.
type Stager struct {
	c       Config
	hooks   hooks
	noFlock sync.Once
}

var _ agent.Stager = (*Stager)(nil)

// New returns a Stager.
func New(c Config) (*Stager, error) {
	if !filepath.IsAbs(c.StagingDir) {
		return nil, fmt.Errorf("stage: the staging directory %q is not absolute", c.StagingDir)
	}
	if c.Apptainer == "" {
		return nil, errors.New("stage: no apptainer binary")
	}
	if c.HubEndpoint == "" {
		c.HubEndpoint = DefaultHubEndpoint
	}
	u, err := url.Parse(c.HubEndpoint)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return nil, fmt.Errorf("stage: Hub endpoint %q is not an http(s) URL", c.HubEndpoint)
	}
	c.HubEndpoint = strings.TrimRight(c.HubEndpoint, "/")
	if c.HTTPClient == nil {
		c.HTTPClient = &http.Client{Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			ResponseHeaderTimeout: 30 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			IdleConnTimeout:       90 * time.Second,
		}}
	}
	// A copy, so the caller's client is left as it is.
	hc := *c.HTTPClient
	hc.CheckRedirect = hubRedirectPolicy(u.Host, hc.CheckRedirect)
	c.HTTPClient = &hc
	if c.Readers <= 0 {
		c.Readers = 4
	}
	if c.TmpDir == "" {
		c.TmpDir = os.TempDir()
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = 2 * time.Second
	}
	if c.StopGrace <= 0 {
		c.StopGrace = 5 * time.Second
	}
	if c.Log == nil {
		c.Log = io.Discard
	}
	return &Stager{c: c}, nil
}

var (
	digestForm = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	// imageForm is a repository without a tag or digest, the characters a
	// registry reference uses.
	imageForm = regexp.MustCompile(`^[a-z0-9][a-z0-9._/:-]*[a-z0-9]$`)
	repoForm  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9][A-Za-z0-9._-]*$`)
	revForm   = regexp.MustCompile(`^[0-9a-f]{40}$`)
	sumForm   = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

func randomHex() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("stage: %v", err))
	}
	return hex.EncodeToString(b[:])
}
