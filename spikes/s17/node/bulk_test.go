package node

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func bulkServer(t *testing.T) *httptest.Server {
	t.Helper()
	engine := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "engine") })
	s := httptest.NewServer(withBulk(engine, slog.New(slog.DiscardHandler)))
	t.Cleanup(s.Close)
	return s
}

func TestUploadIsCountedAtTheReceiver(t *testing.T) {
	s := bulkServer(t)
	body := bytes.Repeat([]byte("x"), 3<<20+17)
	resp, err := http.Post(s.URL+"/bulk/upload", "application/octet-stream", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var rc UploadReceipt
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatal(err)
	}
	if rc.Bytes != int64(len(body)) || rc.FirstNs == 0 || rc.LastNs < rc.FirstNs || rc.FirstNs < rc.HeadersNs {
		t.Fatalf("receipt %+v for %d bytes: want every byte, and headers <= first <= last", rc, len(body))
	}
}

func TestDownloadSendsExactlyTheBytesAskedWithALength(t *testing.T) {
	s := bulkServer(t)
	resp, err := http.Get(s.URL + "/bulk/download?bytes=200001")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil || len(got) != 200001 || resp.ContentLength != 200001 {
		t.Fatalf("got %d bytes (Content-Length %d), %v; want 200001 with its length", len(got), resp.ContentLength, err)
	}
	if bytes.Count(got, []byte{0}) > len(got)/2 {
		t.Fatal("the download is mostly zeros")
	}
	if resp, _ := http.Get(s.URL + "/bulk/download?bytes=-1"); resp == nil || resp.StatusCode != http.StatusBadRequest {
		t.Fatal("a negative size was not refused")
	}
}

func TestEmbeddingsReplyHasOneEmbeddingPerInputOfItsDimension(t *testing.T) {
	s := bulkServer(t)
	in := make([]string, 7)
	for i := range in {
		in[i] = "a sentence to embed"
	}
	req, _ := json.Marshal(map[string]any{"model": "embedder", "input": in})
	resp, err := http.Post(s.URL+"/v1/embeddings", "application/json", bytes.NewReader(req))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.ContentLength != int64(len(raw)) {
		t.Fatalf("Content-Length %d for a body of %d", resp.ContentLength, len(raw))
	}
	var out struct {
		Object string `json:"object"`
		Data   []struct {
			Index     int       `json:"index"`
			Object    string    `json:"object"`
			Embedding []float64 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if out.Object != "list" || len(out.Data) != 7 {
		t.Fatalf("object %q with %d embeddings, want list with 7", out.Object, len(out.Data))
	}
	for i, d := range out.Data {
		if d.Index != i || d.Object != "embedding" || len(d.Embedding) != EmbeddingDim {
			t.Fatalf("embedding %d: index %d, object %q, %d floats", i, d.Index, d.Object, len(d.Embedding))
		}
	}
}

func TestOtherPathsGoToTheEngine(t *testing.T) {
	s := bulkServer(t)
	resp, err := http.Post(s.URL+"/v1/chat/completions", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if b, _ := io.ReadAll(resp.Body); string(b) != "engine" {
		t.Fatalf("chat went to %q, want the engine", b)
	}
}
