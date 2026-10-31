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
// file, and the second rename leaves one complete copy.
//
// The rehash follows spike S7's recommendation (spikes/s7/README.md,
// Recommendation): several parallel readers (4 by default), each reading
// whole files through one reused 8 MiB buffer, never a read sized by a cap,
// whose read-ahead over-read Lustre by up to 190 times (L-12.5).
//
// Which listed files are staged. The identity record lists every file of the
// revision (HBEP-3), but the engine does not load them all, and a listed file
// that was never staged for that reason is not a failure (HBEP-7, Instance
// start). vLLM's default loader loads safetensors weights when the checkpoint
// has them and pytorch .bin weights only when it has none, and ignores
// original/ (vllm/config/load.py at v0.32.0, load_format "auto" and
// ignore_patterns ["original/**/*"]). So when the list has a .safetensors
// file, the weight files of the other formats and everything under original/
// are not downloaded; when present they are still rehashed.
//
// The package imports only the standard library, package agentapi and the
// agent's own package.
package stage

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// DefaultHubEndpoint is the Hugging Face Hub.
const DefaultHubEndpoint = "https://huggingface.co"

// readBuffer is each rehash reader's buffer (spike S7: 8 MiB).
const readBuffer = 8 << 20

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
	// Log receives Apptainer's output.
	Log io.Writer
}

// Stager implements agent.Stager.
type Stager struct {
	c Config
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
	if c.Readers <= 0 {
		c.Readers = 4
	}
	if c.TmpDir == "" {
		c.TmpDir = os.TempDir()
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

// Image implements agent.Stager: it pulls or builds the image once, and
// returns its path.
func (s *Stager) Image(ctx context.Context, image, digest string, format agentapi.ImageFormat) (string, error) {
	if !digestForm.MatchString(digest) {
		return "", fmt.Errorf("image digest %q is not sha256:<64 hex>", digest)
	}
	if !imageForm.MatchString(image) || strings.Contains(image, "@") || strings.Contains(image, "..") {
		return "", fmt.Errorf("image %q is not a repository reference", image)
	}
	if format == "" {
		format = agentapi.ImageFormatSIF
	}
	if format != agentapi.ImageFormatSIF && format != agentapi.ImageFormatSandbox {
		return "", fmt.Errorf("image format %q is neither %q nor %q", format, agentapi.ImageFormatSIF, agentapi.ImageFormatSandbox)
	}
	final := filepath.Join(s.c.StagingDir, agentapi.StagedImagePath(digest, format))
	if fi, err := os.Stat(final); err == nil && fi.IsDir() == (format == agentapi.ImageFormatSandbox) {
		return final, nil
	}
	dir := filepath.Dir(final)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", err
	}
	tmp := filepath.Join(dir, ".tmp-"+randomHex()+filepath.Ext(final))
	ref := "docker://" + image + "@" + digest
	var args []string
	if format == agentapi.ImageFormatSandbox {
		args = []string{"build", "--sandbox", tmp, ref}
	} else {
		args = []string{"pull", tmp, ref}
	}
	cmd := exec.CommandContext(ctx, s.c.Apptainer, args...)
	cmd.Env = append(append([]string(nil), s.c.BaseEnv...),
		"APPTAINER_CACHEDIR="+filepath.Join(dir, ".cache"),
		"APPTAINER_TMPDIR="+s.c.TmpDir,
	)
	cmd.Stdout, cmd.Stderr = s.c.Log, s.c.Log
	if err := cmd.Run(); err != nil {
		os.RemoveAll(tmp)
		return "", fmt.Errorf("apptainer %s %s: %w", args[0], ref, err)
	}
	if err := os.Rename(tmp, final); err != nil {
		// Another Allocation staged it first: keep its copy.
		os.RemoveAll(tmp)
		if _, serr := os.Stat(final); serr == nil {
			return final, nil
		}
		return "", err
	}
	return final, nil
}

// Checkpoint implements agent.Stager.
func (s *Stager) Checkpoint(ctx context.Context, req agent.CheckpointRequest) (agent.CheckpointResult, error) {
	var res agent.CheckpointResult
	if !repoForm.MatchString(req.Repository) {
		return res, fmt.Errorf("checkpoint repository %q is not <owner>/<name>", req.Repository)
	}
	if !revForm.MatchString(req.Revision) {
		return res, fmt.Errorf("checkpoint revision %q is not a full commit id", req.Revision)
	}
	dir := filepath.Join(s.c.StagingDir, agentapi.StagedCheckpointDir(req.Repository, req.Revision))
	res.Dir = dir
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return res, err
	}
	for _, f := range req.Files {
		if err := checkFile(f); err != nil {
			return res, err
		}
	}
	skip := skippedFiles(req.Files)

	// Rehash every listed file present.
	present := make([]bool, len(req.Files))
	for i, f := range req.Files {
		fi, err := os.Lstat(filepath.Join(dir, filepath.FromSlash(f.Path)))
		present[i] = err == nil && fi.Mode().IsRegular()
	}
	sums, err := s.rehash(ctx, dir, req.Files, present)
	if err != nil {
		return res, err
	}
	res.Files = make([]agentapi.FileResult, len(req.Files))
	var fetch []int
	for i, f := range req.Files {
		r := agentapi.FileResult{Path: f.Path}
		switch {
		case present[i] && sums[i] == f.SHA256:
			r.Outcome = agentapi.FileMatch
		case present[i] && req.Restage && !skip[i]:
			// Rung 2: move the bad copy aside and download it again.
			p := filepath.Join(dir, filepath.FromSlash(f.Path))
			if err := os.Rename(p, p+".mismatch-"+time.Now().UTC().Format("20060102T150405.000000000Z")); err != nil {
				return res, fmt.Errorf("moving %s aside: %w", f.Path, err)
			}
			fetch = append(fetch, i)
		case present[i]:
			r.Outcome = agentapi.FileMismatch
			r.SHA256 = sums[i]
		case skip[i]:
			r.Outcome = agentapi.FileAbsent
		default:
			fetch = append(fetch, i)
		}
		res.Files[i] = r
	}

	var done, total int64
	if req.Progress != nil {
		req.Progress = throttle(req.Progress, time.Second)
	}
	for _, i := range fetch {
		f := req.Files[i]
		if err := s.download(ctx, dir, req, f, &done, &total); err != nil {
			return res, fmt.Errorf("staging %s: %w", f.Path, err)
		}
		res.Files[i].Outcome = agentapi.FileMatch
	}
	return res, nil
}

// checkFile refuses a listed path that could leave the checkpoint directory,
// and a hash that is not SHA-256 hex.
func checkFile(f agentapi.FileDigest) error {
	p := f.Path
	if p == "" || strings.HasPrefix(p, "/") || strings.Contains(p, "\\") || path.Clean(p) != p || p == "." ||
		p == ".." || strings.HasPrefix(p, "../") || strings.Contains(p, "\x00") {
		return fmt.Errorf("listed file %q is not a clean relative path", p)
	}
	if !sumForm.MatchString(f.SHA256) {
		return fmt.Errorf("listed file %q: sha256 %q is not 64 lowercase hex characters", p, f.SHA256)
	}
	return nil
}

// otherWeights are the weight formats vLLM's default loader does not load
// when the checkpoint has safetensors.
var otherWeights = []string{".bin", ".pt", ".pth", ".ckpt", ".h5", ".msgpack", ".onnx"}

// skippedFiles marks the listed files staging does not download: with any
// .safetensors file listed, the other weight formats and original/.
func skippedFiles(files []agentapi.FileDigest) []bool {
	skip := make([]bool, len(files))
	safetensors := false
	for _, f := range files {
		safetensors = safetensors || strings.HasSuffix(f.Path, ".safetensors")
	}
	if !safetensors {
		return skip
	}
	for i, f := range files {
		if strings.HasPrefix(f.Path, "original/") {
			skip[i] = true
			continue
		}
		for _, ext := range otherWeights {
			if strings.HasSuffix(f.Path, ext) {
				skip[i] = true
			}
		}
	}
	return skip
}

// rehash returns the SHA-256 of every present file, with Readers files read
// at once, each through its reader's one buffer.
func (s *Stager) rehash(ctx context.Context, dir string, files []agentapi.FileDigest, present []bool) ([]string, error) {
	sums := make([]string, len(files))
	jobs := make(chan int)
	errs := make([]error, len(files))
	var wg sync.WaitGroup
	for range s.c.Readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, readBuffer)
			for i := range jobs {
				sums[i], errs[i] = hashFile(ctx, filepath.Join(dir, filepath.FromSlash(files[i].Path)), buf)
			}
		}()
	}
	for i := range files {
		if present[i] {
			select {
			case jobs <- i:
			case <-ctx.Done():
			}
		}
	}
	close(jobs)
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return sums, errors.Join(errs...)
}

// hashFile reads the whole file through buf with plain reads, never with a
// read sized to the file, and returns its SHA-256. io.Copy is not used: an
// *os.File's WriteTo falls back to a 32 KiB buffer of its own.
func hashFile(ctx context.Context, p string, buf []byte) (string, error) {
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		n, err := f.Read(buf)
		h.Write(buf[:n])
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", fmt.Errorf("reading %s: %w", p, err)
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// download fetches one file from the Hub at the full commit id into a
// temporary name beside its place, checks its SHA-256 against the record and
// renames it into place. A token, when given, goes only into the
// Authorization header; the Go client drops it on a redirect to another host,
// as the Hub's redirect of an LFS file to its CDN is.
func (s *Stager) download(ctx context.Context, dir string, req agent.CheckpointRequest, f agentapi.FileDigest, done, total *int64) error {
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * 2 * time.Second):
			}
		}
		retry, err := s.downloadOnce(ctx, dir, req, f, done, total)
		if err == nil {
			return nil
		}
		last = err
		if !retry {
			return err
		}
	}
	return last
}

func (s *Stager) downloadOnce(ctx context.Context, dir string, req agent.CheckpointRequest, f agentapi.FileDigest, done, total *int64) (retry bool, err error) {
	segs := strings.Split(f.Path, "/")
	for i, seg := range segs {
		segs[i] = url.PathEscape(seg)
	}
	u := s.c.HubEndpoint + "/" + req.Repository + "/resolve/" + req.Revision + "/" + strings.Join(segs, "/")
	hreq, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return false, err
	}
	if req.HubToken != "" {
		hreq.Header.Set("Authorization", "Bearer "+req.HubToken)
	}
	resp, err := s.c.HTTPClient.Do(hreq)
	if err != nil {
		return ctx.Err() == nil, fmt.Errorf("GET %s: %w", u, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		retry := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
		return retry, fmt.Errorf("GET %s: HTTP %d", u, resp.StatusCode)
	}
	if resp.ContentLength > 0 {
		*total += resp.ContentLength
		report(req, *done, *total)
	}
	p := filepath.Join(dir, filepath.FromSlash(f.Path))
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		return false, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), "."+filepath.Base(p)+".part-*")
	if err != nil {
		return false, err
	}
	ok := false
	defer func() {
		if !ok {
			tmp.Close()
			os.Remove(tmp.Name())
		}
	}()
	h := sha256.New()
	buf := make([]byte, 1<<20)
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			h.Write(buf[:n])
			if _, werr := tmp.Write(buf[:n]); werr != nil {
				return false, werr
			}
			*done += int64(n)
			report(req, *done, *total)
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return ctx.Err() == nil, fmt.Errorf("GET %s: %w", u, rerr)
		}
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != f.SHA256 {
		return false, fmt.Errorf("the download has SHA-256 %s, not the identity record's %s", got, f.SHA256)
	}
	if err := tmp.Sync(); err != nil {
		return false, err
	}
	if err := tmp.Close(); err != nil {
		return false, err
	}
	if err := os.Rename(tmp.Name(), p); err != nil {
		return false, err
	}
	ok = true
	return false, nil
}

func report(req agent.CheckpointRequest, done, total int64) {
	if req.Progress != nil {
		req.Progress(done, total)
	}
}

// throttle passes on at most one progress report per interval, so a large
// download does not flood the agent's loop; the first report always passes.
func throttle(f func(done, total int64), every time.Duration) func(done, total int64) {
	var last time.Time
	return func(done, total int64) {
		if now := time.Now(); last.IsZero() || now.Sub(last) >= every {
			last = now
			f(done, total)
		}
	}
}

func randomHex() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("stage: %v", err))
	}
	return hex.EncodeToString(b[:])
}
