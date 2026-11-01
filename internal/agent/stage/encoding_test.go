package stage

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// progressLog records every Progress report of one Checkpoint call.
type progressLog struct {
	mu      sync.Mutex
	reports [][2]int64 // done, total
}

func (p *progressLog) fn(done, total int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reports = append(p.reports, [2]int64{done, total})
}

func (p *progressLog) all() [][2]int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([][2]int64(nil), p.reports...)
}

func sumOf(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

// The Hub's way with a file that is not in LFS, as #94 found it with curl:
// gzipped, chunked, no Content-Length, unless the request asks for the
// identity encoding, which gets the file itself with its exact length.
func gzipUnlessIdentity(t *testing.T, hub *fakeHub, files map[string]string, accept *[]string, mu *sync.Mutex) {
	t.Helper()
	hub.setIntercept(func(_ int, w http.ResponseWriter, r *http.Request) bool {
		mu.Lock()
		*accept = append(*accept, r.Header.Get("Accept-Encoding"))
		mu.Unlock()
		c, ok := files[r.URL.Path]
		if !ok {
			return false
		}
		if r.Header.Get("Accept-Encoding") != "identity" {
			w.Header().Set("Content-Encoding", "gzip")
			gz := gzip.NewWriter(w)
			io.WriteString(gz, c)
			gz.Close()
			w.(http.Flusher).Flush() // chunked: no Content-Length
			return true
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(c)))
		io.WriteString(w, c)
		return true
	})
}

// Every Hub GET asks for the identity encoding, so the total and the bytes
// done both come from the exact Content-Length, and done never passes total
// (science-agent ruling, 1 Nov 2026, issue #125; #94, finding 2: bytesDone
// 1,448.8 MiB of bytesTotal 1,444.6 MiB). The double gzips any request that
// does not ask for identity, as the Hub does; Go's client then decodes it and
// the length is unknown. Expected total: the two files' lengths, 300 + 1,000
// bytes.
func TestHubDownloadsAskForTheIdentityEncoding(t *testing.T) {
	cfg := strings.Repeat("{\"vocab\": 1}\n", 25)[:300]
	weights := strings.Repeat("w", 1000)
	files := map[string]string{resolvePath("config.json"): cfg, resolvePath("model.safetensors"): weights}
	s, hub, _ := newStager(t, files)
	var mu sync.Mutex
	var accept []string
	gzipUnlessIdentity(t, hub, files, &accept, &mu)
	var log progressLog
	res, err := s.Checkpoint(context.Background(), agent.CheckpointRequest{
		Repository: "org/model", Revision: rev,
		Files: []agentapi.FileDigest{
			{Path: "config.json", SHA256: sumOf(cfg)},
			{Path: "model.safetensors", SHA256: sumOf(weights)},
		},
		Progress: log.fn,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range res.Files {
		if f.Outcome != agentapi.FileMatch {
			t.Errorf("%s: %s, want a match", f.Path, f.Outcome)
		}
	}
	mu.Lock()
	if len(accept) != 2 || accept[0] != "identity" || accept[1] != "identity" {
		t.Errorf("Accept-Encoding of the Hub GETs: %q, want identity on both", accept)
	}
	mu.Unlock()
	reports := log.all()
	if len(reports) == 0 {
		t.Fatal("no progress reported")
	}
	for _, r := range reports {
		if r[0] > r[1] {
			t.Errorf("report done %d of total %d: done passed the total", r[0], r[1])
		}
	}
	if last := reports[len(reports)-1]; last != [2]int64{1300, 1300} {
		t.Errorf("final report %v, want {1300 1300}", last)
	}
}

// A response with no Content-Length, even to a request for the identity
// encoding, leaves that file's length unknown: it counts in neither the total
// nor the bytes done, and no length is guessed. The file is still staged and
// checked. The other file, 1,000 bytes with its length, is the whole total.
func TestHubFileWithNoContentLengthHasAnUnknownTotal(t *testing.T) {
	cfg := strings.Repeat("c", 300)
	weights := strings.Repeat("w", 1000)
	s, hub, _ := newStager(t, map[string]string{resolvePath("model.safetensors"): weights})
	hub.setIntercept(func(_ int, w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path != resolvePath("config.json") {
			return false
		}
		io.WriteString(w, cfg[:100])
		w.(http.Flusher).Flush() // chunked, no Content-Length, no encoding
		io.WriteString(w, cfg[100:])
		return true
	})
	var log progressLog
	res, err := s.Checkpoint(context.Background(), agent.CheckpointRequest{
		Repository: "org/model", Revision: rev,
		Files: []agentapi.FileDigest{
			{Path: "config.json", SHA256: sumOf(cfg)},
			{Path: "model.safetensors", SHA256: sumOf(weights)},
		},
		Progress: log.fn,
	})
	if err != nil {
		t.Fatal(err)
	}
	if o := outcomes(res); o["config.json"].Outcome != agentapi.FileMatch || o["model.safetensors"].Outcome != agentapi.FileMatch {
		t.Fatalf("outcomes %+v, want both matched", o)
	}
	if b, err := os.ReadFile(filepath.Join(ckptDir(s), "config.json")); err != nil || string(b) != cfg {
		t.Fatalf("config.json staged as %d bytes (%v), want the 300-byte file", len(b), err)
	}
	reports := log.all()
	for _, r := range reports {
		if r[0] > r[1] {
			t.Errorf("report done %d of total %d: done passed the total", r[0], r[1])
		}
	}
	if len(reports) == 0 {
		t.Fatal("no progress reported")
	}
	if last := reports[len(reports)-1]; last != [2]int64{1000, 1000} {
		t.Errorf("final report %v, want {1000 1000}: the unknown-length file counts in neither", last)
	}
}

// A body sent encoded anyway, to a request for the identity encoding, is
// refused with a message that names the encoding, not passed on as a
// checksum mismatch.
func TestHubEncodedBodyIsRefused(t *testing.T) {
	cfg := strings.Repeat("c", 300)
	s, hub, _ := newStager(t, nil)
	hub.setIntercept(func(_ int, w http.ResponseWriter, r *http.Request) bool {
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		io.WriteString(gz, cfg)
		gz.Close()
		return true
	})
	_, err := s.Checkpoint(context.Background(), agent.CheckpointRequest{
		Repository: "org/model", Revision: rev,
		Files: []agentapi.FileDigest{{Path: "config.json", SHA256: sumOf(cfg)}},
	})
	if err == nil || !strings.Contains(err.Error(), `Content-Encoding "gzip"`) {
		t.Fatalf("err = %v, want a refusal naming Content-Encoding gzip", err)
	}
}
