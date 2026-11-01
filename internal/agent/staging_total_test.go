package agent_test

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// After a file of known length, a file whose response carries no
// Content-Length and whose body stalls before its first byte withdraws the
// total at once: while the Hub holds between the headers and the body, the
// report shows the bytes done and no bytesTotal, never the first file's total
// as though it were the whole (PR #138 review, Astra B5). The waits are on
// observable state: the Hub's hold, then the report.
func TestStalledUnknownLengthWithdrawsTheTotal(t *testing.T) {
	held := "held tokenizer content\n"
	sum := sha256.Sum256([]byte(held))
	heldFile := agentapi.FileDigest{Path: "tokenizer.json", SHA256: hex.EncodeToString(sum[:])}
	holding, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	h := newHarness(t, harnessOptions{hubIntercept: func(w http.ResponseWriter, r *http.Request) bool {
		if !strings.HasSuffix(r.URL.Path, "/"+heldFile.Path) {
			return false
		}
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush() // the headers, chunked: no Content-Length
		once.Do(func() { close(holding) })
		select {
		case <-release:
		case <-r.Context().Done():
			return true
		}
		io.WriteString(w, held)
		return true
	}})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	files := []agentapi.FileDigest{defaultFile, heldFile}
	h.set(h.runState("", 1, 0, verifyChecks(1, files)))
	select {
	case <-holding:
	case <-time.After(30 * time.Second):
		t.Fatal("the Hub was never asked for the held file")
	}
	known := int64(len(defaultContent))
	h.waitReport("the total withdrawn during the hold", 20*time.Second, func(r agentapi.Report) bool {
		if r.Instance == nil || r.Instance.Staging == nil {
			return false
		}
		st := r.Instance.Staging
		return st.Phase == agentapi.StagingInProgress && st.BytesDone == known && st.BytesTotal == 0
	})
	close(release)
	r := h.waitReport("staging done", 30*time.Second, func(r agentapi.Report) bool {
		return r.Instance != nil && r.Instance.Staging != nil && r.Instance.Staging.Phase == agentapi.StagingDone
	})
	if st := r.Instance.Staging; st.BytesDone != known+int64(len(held)) || st.BytesTotal != st.BytesDone {
		t.Errorf("after the hold: done %d of %d, want both %d", st.BytesDone, st.BytesTotal, known+int64(len(held)))
	}
}
