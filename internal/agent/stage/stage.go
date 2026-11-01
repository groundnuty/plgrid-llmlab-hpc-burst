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
// Which listed files are staged (skip.go). The identity record lists every
// file of the revision (HBEP-3), but the engine does not load them all, and a
// listed file that was never staged for that reason is not a failure (HBEP-7,
// Instance start). A listed file the engine does not load is not downloaded; one
// that is present is still rehashed. What the engine loads depends on the
// launch's --load-format, so the rule is read from vLLM v0.32.0's default loader
// and applied to that format only. The paths are in the vLLM repository at tag
// v0.32.0, and "default_loader.py" is model_executor/model_loader/default_loader.py.
//
//   - Weights are found by a glob of the checkpoint directory's top level, one
//     pattern after another; the first pattern with a hit wins and the files of
//     the others are not loaded (default_loader.py lines 228-234). Nothing in a
//     subdirectory is globbed, so a nested 2_Dense/model.safetensors does not
//     make a top-level pytorch_model.bin unloaded, and original/ is never loaded
//     from a local directory. The "original/**/*" ignore pattern
//     (config/load.py line 102) applies only to a Hub download
//     (default_loader.py lines 213-221), which a staged checkpoint is not.
//     Only top-level files are therefore classified; original/ is skipped and
//     no other subdirectory is.
//   - auto (config/load.py line 30; also "", when the launch names no format):
//     when a top-level consolidated*.safetensors file is listed, vLLM switches
//     to its mistral format and globs only consolidated*.safetensors
//     (default_loader.py lines 170-182 and 194-196), so the sharded safetensors
//     files and the other weight formats are not loaded. Otherwise the patterns
//     are *.safetensors, then *.bin, then *.pt (lines 185-186 and 207-208). The
//     mistral probe matches by file name at any depth
//     (transformers_utils/repo_utils.py lines 198-205); a consolidated file
//     that is only nested makes vLLM choose mistral and find no weights, which
//     fails whatever is staged, so only a top-level one counts here.
//   - safetensors: only *.safetensors (default_loader.py lines 187-193).
//   - pt: the code globs *.pt only (default_loader.py lines 198-199 and
//     207-208), while the docstring says "the pytorch bin format"
//     (config/load.py line 36). Both .bin and .pt are staged, which is safe
//     whichever is right.
//   - Any other format (gguf, mistral, bitsandbytes, sharded_state, tensorizer,
//     runai_streamer, dummy, npcache, fastsafetensors ...): nothing is skipped.
//   - A weight extension no pattern globs (.pth .ckpt .h5 .msgpack .onnx) is
//     never loaded under auto, safetensors or pt.
//   - Files that are not weights (config, tokenizer, templates) are never
//     skipped. The format is lowercased first (config/load.py lines 136-138).
//
// What the listing cannot show: a model class may replace the patterns
// (allow_patterns_overrides, default_loader.py lines 210-211 and 377), and the
// safetensors index file may name a subset of the shards (default_loader.py
// lines 250-254, model_loader/weight_utils.py lines 585-611). Staging more than
// the engine loads is safe and staging less is not, so neither is mirrored.
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
	// UsedSpace reads the use of TmpDir while an image is built; statfs when
	// nil (runApptainer).
	UsedSpace UsedSpace
	// SampleEvery is how often UsedSpace is read during a build; 250 ms when 0.
	SampleEvery time.Duration
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
	if c.UsedSpace == nil {
		c.UsedSpace = statfsUsed
	}
	if c.SampleEvery <= 0 {
		c.SampleEvery = 250 * time.Millisecond
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
