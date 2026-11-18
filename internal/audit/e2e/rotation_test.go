package e2e

// (d), the rotation: the log rotates more times than the old maxbackup=3 kept, and every rotated file is gzip and still there.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
)

// rotation sends requests until the log has rotated more times than the old maxbackup=3 kept, then shows that every
// rotated file is gzip, that none was removed, and that the first request's line is still in the oldest file.
func (w *world) rotation(t *testing.T, rd *reader) {
	t.Helper()
	dir := filepath.Dir(w.logPath)
	backups := func() map[string]bool {
		ents, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]bool{}
		for _, e := range ents {
			if base, ok := backupBase(e.Name()); ok {
				out[base] = true
			}
		}
		return out
	}
	seen := backups() // every rotated file name seen at any moment, ever
	before := len(seen)

	cfg := *w.admin.cfg
	cfg.QPS, cfg.Burst = 1000, 2000
	cs, err := kubernetes.NewForConfig(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	get := func(name, pad string) {
		err := cs.CoreV1().RESTClient().Get().AbsPath("/api/v1/namespaces/"+exampleNS+"/configmaps/"+name).Param("pad", pad).Do(w.ctx).Error()
		if err != nil && !apierrors.IsNotFound(err) {
			t.Errorf("GET %s: %v", name, err)
		}
	}
	first := "rotation-first-" + randomHex(t, 8)
	get(first, "")
	pad := strings.Repeat("x", 4000)
	const batch, workers = 200, 8
	sent := 0
	for len(seen)-before < 5 {
		if sent >= 8000 {
			t.Fatalf("%d requests of 4 KB did not make the log rotate 5 times (the size is %d MB): %d new rotated files", sent, rotationMB, len(seen)-before)
		}
		var wg sync.WaitGroup
		for k := 0; k < workers; k++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < batch/workers; i++ {
					get("rotation-bulk-"+randomHex(t, 4), pad)
				}
			}()
		}
		wg.Wait()
		sent += batch
		for name := range backups() {
			seen[name] = true
		}
	}
	last := "rotation-last-" + randomHex(t, 8)
	get(last, "")
	t.Logf("rotation: %d requests of 4 KB made %d new rotated files (log size %d MB)", sent, len(seen)-before, rotationMB)

	// The gzip of a rotated file runs in the background: wait until no plain rotated file is left.
	eventually(t, 90*time.Second, "every rotated file is compressed", func() (bool, string) {
		ents, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		var plain []string
		for _, e := range ents {
			if _, ok := backupBase(e.Name()); ok && !strings.HasSuffix(e.Name(), ".gz") {
				plain = append(plain, e.Name())
			}
		}
		return len(plain) == 0, fmt.Sprintf("plain rotated files still there: %v", plain)
	})
	for name := range backups() {
		seen[name] = true
	}

	// Rotated files exist FIRST: a run that never rotates proves nothing about the rest.
	set := w.waitLog(rd, "the last request after the rotation", func(s *logSet) bool {
		return len(s.find("get", "configmaps", "", last, w.admin.user)) > 0
	})
	rotated := set.rotatedNames()
	if len(rotated) < 5 {
		t.Fatalf("%d rotated files, want at least 5 (more than the old maxbackup=3 keeps): the log did not rotate enough to show what is kept", len(rotated))
	}
	have := map[string]bool{}
	for _, n := range rotated {
		have[n] = true
	}
	for name := range seen {
		if !have[name] {
			t.Errorf("the rotated file %s was seen and is gone: a file was removed", name)
		}
	}
	for _, f := range set.files {
		if base, ok := backupBase(f.name); ok {
			if !f.gz || f.name != base+".gz" {
				t.Errorf("the rotated file %s is not a gzip file by name", f.name)
				continue
			}
			if err := isGzip(filepath.Join(dir, f.name)); err != nil {
				t.Errorf("rotated file %s: %v", f.name, err)
			}
			if len(f.text) == 0 {
				t.Errorf("rotated file %s decompresses to nothing", f.name)
			}
		}
		if f.mode.Perm()&0o077 != 0 {
			t.Errorf("the audit file %s has mode %v: owner only is the rule", f.name, f.mode.Perm())
		}
	}
	if fi, err := os.Stat(dir); err != nil || fi.Mode().Perm()&0o077 != 0 {
		t.Errorf("the audit log's directory: %v, %v, want owner only", fi, err)
	}
	// The first request's line is still in the log: the oldest file was not the one that was removed.
	if len(set.find("get", "configmaps", "", first, w.admin.user)) == 0 {
		t.Errorf("the first request of the rotation (%s) is in no file: the oldest rotated file was removed", first)
	}
	// Every line of every file is a whole JSON event: parseEvents failed the read otherwise.
	var total int
	for _, f := range set.files {
		total += len(f.text)
	}
	t.Logf("rotation: %d files (%d rotated, all gzip), %d bytes of log in all, decompressed", len(set.files), len(rotated), total)
}
