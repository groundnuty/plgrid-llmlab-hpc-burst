package stage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Image integrity (PR #87 review, Astra B1, Opus B5).
//
// A staged image is trusted only against a record written in the step that
// pulled or built it, beside the image in images/:
//
//   - <hex>.sif.sha256 for a SIF: the image digest it was pulled from, the
//     file's size and its SHA-256, read through S7's reader (one reused 8 MiB
//     buffer, plain reads).
//   - <hex>.sandbox.manifest for a sandbox: a manifest of the tree (tree.go).
//
// Every Image call checks the image against its record: a SIF is rehashed, a
// sandbox is walked. An image that fails the check, or has no record (one
// staged by older code), is moved aside and pulled or built again, and the
// aside copy is removed once the new image is in place. Image reports
// Verified only after a passed check or a fresh pull or build.
//
// The record is renamed into place before the image, so a reader that finds
// the image finds its record. A pull or build, and the move aside before it,
// run under the digest's lock (lock.go), so two Allocations never interleave
// their renames and pair one pull's SIF with the other's record: two pulls of
// one digest differ byte for byte (the SIF header carries a UUID and times).

// sifRecord is the content of a SIF's sidecar.
type sifRecord struct {
	Format string `json:"format"`
	Image  string `json:"image"` // the digest the SIF was pulled from
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

const sifRecordFormat = "hpcburst-sif/1"

// errImageAbsent says there is no image at the staged path: nothing failed.
var errImageAbsent = errors.New("not staged")

// staleTempAge is how old a temporary pull or build output must be to be
// removed as left by a run that died.
const staleTempAge = time.Hour

// Image implements agent.Stager: it checks a staged image against its record,
// pulls or builds it when there is none or the check fails, and returns its
// path.
func (s *Stager) Image(ctx context.Context, image, digest string, format agentapi.ImageFormat) (agent.ImageResult, error) {
	if !digestForm.MatchString(digest) {
		return agent.ImageResult{}, fmt.Errorf("image digest %q is not sha256:<64 hex>", digest)
	}
	if !imageForm.MatchString(image) || strings.Contains(image, "@") || strings.Contains(image, "..") {
		return agent.ImageResult{}, fmt.Errorf("image %q is not a repository reference", image)
	}
	if format == "" {
		format = agentapi.ImageFormatSIF
	}
	if format != agentapi.ImageFormatSIF && format != agentapi.ImageFormatSandbox {
		return agent.ImageResult{}, fmt.Errorf("image format %q is neither %q nor %q", format, agentapi.ImageFormatSIF, agentapi.ImageFormatSandbox)
	}
	final := filepath.Join(s.c.StagingDir, agentapi.StagedImagePath(digest, format))
	dir := filepath.Dir(final)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return agent.ImageResult{}, err
	}
	hex := strings.TrimPrefix(digest, "sha256:")
	lockPath := filepath.Join(dir, "."+hex+".lock")
	s.removeStaleTemps(dir, hex, lockPath)

	ok := agent.ImageResult{Path: final, Verified: true}
	err := s.verifyImage(ctx, final, digest, format)
	if err == nil {
		return ok, nil
	}
	if cerr := ctx.Err(); cerr != nil {
		return agent.ImageResult{}, cerr
	}
	lk, lerr := s.lock(ctx, lockPath)
	if lerr != nil {
		return agent.ImageResult{}, lerr
	}
	defer lk.release()
	// Another Allocation may have repaired it while this one waited.
	if err = s.verifyImage(ctx, final, digest, format); err == nil {
		return ok, nil
	}
	if cerr := ctx.Err(); cerr != nil {
		return agent.ImageResult{}, cerr
	}
	if !errors.Is(err, errImageAbsent) {
		s.logf("staged image %s failed its check (%v): staging it again", final, err)
	}
	var peak int64
	if err := s.stageImage(ctx, image, digest, format, final, hex, &peak); err != nil {
		return agent.ImageResult{}, err
	}
	ok.BuildPeakTmpBytes = peak
	return ok, nil
}

// recordPath is where an image's integrity record lives.
func recordPath(final string, format agentapi.ImageFormat) string {
	if format == agentapi.ImageFormatSandbox {
		return final + ".manifest"
	}
	return final + ".sha256"
}

// verifyImage checks the staged image against its record.
func (s *Stager) verifyImage(ctx context.Context, final, digest string, format agentapi.ImageFormat) error {
	fi, err := os.Lstat(final)
	if errors.Is(err, os.ErrNotExist) {
		return errImageAbsent
	}
	if err != nil {
		return err
	}
	rec := recordPath(final, format)
	if format == agentapi.ImageFormatSandbox {
		if !fi.IsDir() {
			return errors.New("not a directory")
		}
		want, err := readManifest(rec, digest)
		if err != nil {
			return err
		}
		got, err := scanTree(ctx, final, s.c.Readers)
		if err != nil {
			return err
		}
		return compareTree(want, got)
	}
	if !fi.Mode().IsRegular() {
		return errors.New("not a regular file")
	}
	want, err := readSIFRecord(rec, digest)
	if err != nil {
		return err
	}
	if fi.Size() != want.Size {
		return fmt.Errorf("size %d, recorded %d", fi.Size(), want.Size)
	}
	got, err := hashFile(ctx, final, make([]byte, readBuffer))
	if err != nil {
		return err
	}
	if got != want.SHA256 {
		return fmt.Errorf("SHA-256 %s, recorded %s", got, want.SHA256)
	}
	return nil
}

func readSIFRecord(path, digest string) (sifRecord, error) {
	var rec sifRecord
	b, err := os.ReadFile(path)
	if err != nil {
		return rec, err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&rec); err != nil {
		return rec, fmt.Errorf("record %s: %w", path, err)
	}
	if rec.Format != sifRecordFormat || rec.Image != digest || rec.Size < 0 || !sumForm.MatchString(rec.SHA256) {
		return rec, fmt.Errorf("record %s is not a %s record for %s", path, sifRecordFormat, digest)
	}
	return rec, nil
}

// stageImage moves what is at final aside, pulls or builds the image into a
// temporary name, writes its record, and renames the record and then the image
// into place. The caller holds the digest's lock.
func (s *Stager) stageImage(ctx context.Context, image, digest string, format agentapi.ImageFormat, final, hex string, buildPeak *int64) error {
	dir := filepath.Dir(final)
	rec := recordPath(final, format)
	aside := ""
	if _, err := os.Lstat(final); err == nil {
		aside = final + ".mismatch-" + time.Now().UTC().Format("20060102T150405.000000000Z")
		if err := os.Rename(final, aside); err != nil {
			return fmt.Errorf("moving %s aside: %w", final, err)
		}
	}
	if err := os.Remove(rec); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	fail := func(err error) error {
		if aside != "" {
			if rerr := os.Rename(aside, final); rerr != nil {
				s.logf("putting %s back: %v", final, rerr)
			}
		}
		return err
	}
	base := ".tmp-" + hex + "-" + randomHex()
	tmp, tmpRec := filepath.Join(dir, base+filepath.Ext(final)), filepath.Join(dir, base+".record")
	defer removeAll(tmp)
	defer removeAll(tmpRec)
	ref := "docker://" + image + "@" + digest
	args := []string{"pull", tmp, ref}
	if format == agentapi.ImageFormatSandbox {
		args = []string{"build", "--sandbox", tmp, ref}
	}
	if hook := agent.ImageBuildHook(ctx); hook != nil {
		hook(s.c.TmpDir)
	}
	peak, err := s.runApptainer(ctx, args)
	*buildPeak = peak
	if err != nil {
		return fail(fmt.Errorf("%w for %s", err, ref))
	}
	if err := s.writeRecord(ctx, tmp, tmpRec, digest, format); err != nil {
		return fail(fmt.Errorf("recording %s: %w", ref, err))
	}
	if err := os.Rename(tmpRec, rec); err != nil {
		return fail(err)
	}
	if err := os.Rename(tmp, final); err != nil {
		os.Remove(rec)
		return fail(err)
	}
	if aside != "" {
		if err := removeAll(aside); err != nil {
			s.logf("removing %s: %v", aside, err)
		}
	}
	return nil
}

// writeRecord writes the integrity record of the image at tmp to recTmp.
func (s *Stager) writeRecord(ctx context.Context, tmp, recTmp, digest string, format agentapi.ImageFormat) error {
	if format == agentapi.ImageFormatSandbox {
		entries, err := scanTree(ctx, tmp, s.c.Readers)
		if err != nil {
			return err
		}
		return writeManifest(recTmp, digest, entries)
	}
	fi, err := os.Lstat(tmp)
	if err != nil {
		return err
	}
	if !fi.Mode().IsRegular() {
		return errors.New("the pull did not produce a regular file")
	}
	sum, err := hashFile(ctx, tmp, make([]byte, readBuffer))
	if err != nil {
		return err
	}
	b, err := json.Marshal(sifRecord{Format: sifRecordFormat, Image: digest, Size: fi.Size(), SHA256: sum})
	if err != nil {
		return err
	}
	return os.WriteFile(recTmp, append(b, '\n'), 0o640)
}

// removeStaleTemps removes the digest's temporary pull and build outputs older
// than staleTempAge, which a run that died left. It runs only while it holds
// the digest's lock: a sandbox build's top directory keeps an old modification
// time while a deep tree is still being written, so age alone could delete a
// build in progress.
func (s *Stager) removeStaleTemps(dir, hex, lockPath string) {
	lk, ok, err := s.tryLock(lockPath)
	if err != nil || !ok {
		return
	}
	defer lk.release()
	temps, _ := filepath.Glob(filepath.Join(dir, ".tmp-"+hex+"-*"))
	for _, p := range temps {
		if fi, err := os.Lstat(p); err == nil && time.Since(fi.ModTime()) > staleTempAge {
			if err := removeAll(p); err != nil {
				s.logf("removing %s: %v", p, err)
			}
		}
	}
}
