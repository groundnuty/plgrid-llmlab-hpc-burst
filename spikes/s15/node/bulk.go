package node

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// The bulk paths beside the engine double, on the same socket (HBEP-6, open
// question 12: embeddings, file transfers and large non-streamed replies):
//
//	POST /bulk/upload            a request body of any size, read and counted; the reply names
//	                             when its first and last byte arrived here, the receiver
//	POST /v1/embeddings          {"input": [N strings]}: N embeddings of EmbeddingDim floats, one
//	                             non-streamed JSON reply with a Content-Length
//	GET  /bulk/download?bytes=N  a non-streamed reply of N bytes with a Content-Length
//
// Everything else goes to the engine double.
const (
	// EmbeddingDim is each embedding's length: 1,024 floats, as embedding
	// models of the bge-large and e5-large size return.
	EmbeddingDim = 1024
	// maxDownload bounds a download's size.
	maxDownload = 1 << 30
)

// UploadReceipt is the upload's reply: when it reached the receiver.
type UploadReceipt struct {
	Bytes     int64 `json:"bytes"`
	HeadersNs int64 `json:"headers_unix_ns"`    // the handler started: the request's headers had arrived
	FirstNs   int64 `json:"first_byte_unix_ns"` // the body's first byte was read
	LastNs    int64 `json:"last_byte_unix_ns"`  // its last byte was read
	// Steps are the bytes received by each 100 ms after the first byte.
	Steps []int64 `json:"steps_bytes"`
}

// withBulk serves the bulk paths and passes every other request to engine.
func withBulk(engine http.Handler, log *slog.Logger) http.Handler {
	emb := &embeddings{bodies: map[int][]byte{}}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/bulk/upload" && r.Method == http.MethodPost:
			upload(w, r, log)
		case r.URL.Path == "/bulk/download" && r.Method == http.MethodGet:
			download(w, r, log)
		case r.URL.Path == "/v1/embeddings" && r.Method == http.MethodPost:
			emb.serve(w, r, log)
		default:
			engine.ServeHTTP(w, r)
		}
	})
}

func upload(w http.ResponseWriter, r *http.Request, log *slog.Logger) {
	rc := UploadReceipt{HeadersNs: time.Now().UnixNano()}
	buf := make([]byte, 64*1024)
	for {
		n, err := r.Body.Read(buf)
		if n > 0 {
			t := time.Now()
			now := t.UnixNano()
			if rc.Bytes == 0 {
				rc.FirstNs = now
			}
			rc.Bytes += int64(n)
			rc.LastNs = now
			i := int(t.Sub(time.Unix(0, rc.FirstNs)) / (100 * time.Millisecond))
			for len(rc.Steps) <= i {
				prev := int64(0)
				if len(rc.Steps) > 0 {
					prev = rc.Steps[len(rc.Steps)-1]
				}
				rc.Steps = append(rc.Steps, prev)
			}
			rc.Steps[i] = rc.Bytes
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Warn("an upload was cut", "bytes", rc.Bytes, "err", err)
			http.Error(w, "the upload was cut", http.StatusBadRequest)
			return
		}
	}
	log.Info("upload received", "bytes", rc.Bytes, "ms", float64(rc.LastNs-rc.FirstNs)/1e6)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(rc)
}

// filler is the download's repeated content: not all zeros, not compressible
// to nothing, though nothing on the path compresses.
var filler = func() []byte {
	b := make([]byte, 64*1024)
	x := uint32(2463534242)
	for i := range b {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		b[i] = byte(x)
	}
	return b
}()

func download(w http.ResponseWriter, r *http.Request, log *slog.Logger) {
	n, err := strconv.ParseInt(r.URL.Query().Get("bytes"), 10, 64)
	if err != nil || n < 0 || n > maxDownload {
		http.Error(w, "bytes must be 0 to 1 GiB", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(n, 10))
	w.WriteHeader(http.StatusOK)
	start := time.Now()
	left := n
	for left > 0 {
		k := min(left, int64(len(filler)))
		if _, err := w.Write(filler[:k]); err != nil {
			log.Warn("a download was cut", "sent", n-left, "err", err)
			return
		}
		left -= k
	}
	log.Info("download sent", "bytes", n, "ms", float64(time.Since(start))/1e6)
}

// embeddings answers an embeddings batch at once, as an engine does once the
// whole batch is computed: one JSON body of the OpenAI-compatible shape vLLM
// serves (object "list", data[i] = {object "embedding", index, embedding}),
// with a Content-Length. The double computes nothing; each batch size's body
// is built once and served again, so its cost is the transfer.
type embeddings struct {
	mu     sync.Mutex
	bodies map[int][]byte
}

type embeddingRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

func (e *embeddings) serve(w http.ResponseWriter, r *http.Request, log *slog.Logger) {
	var req embeddingRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 64<<20)).Decode(&req); err != nil || len(req.Input) == 0 || len(req.Input) > 10000 {
		http.Error(w, "want input: 1 to 10,000 strings", http.StatusBadRequest)
		return
	}
	body := e.body(len(req.Input), req.Model)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	start := time.Now()
	if _, err := w.Write(body); err != nil {
		log.Warn("an embeddings reply was cut", "err", err)
		return
	}
	log.Info("embeddings sent", "inputs", len(req.Input), "bytes", len(body), "ms", float64(time.Since(start))/1e6)
}

func (e *embeddings) body(n int, model string) []byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	if b, ok := e.bodies[n]; ok {
		return b
	}
	b := embeddingBody(n, model)
	e.bodies[n] = b
	return b
}

// embeddingBody is n embeddings of EmbeddingDim float32 values, each written
// as Go writes a float32 in its shortest form (about 11 bytes, as Python's
// repr of a float32 widened to a float is).
func embeddingBody(n int, model string) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, `{"id":"embd-s15","object":"list","created":%d,"model":%q,"data":[`, time.Now().Unix(), model)
	num := make([]byte, 0, 32)
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"index":%d,"object":"embedding","embedding":[`, i)
		for j := 0; j < EmbeddingDim; j++ {
			if j > 0 {
				b.WriteByte(',')
			}
			v := float32(0.05 * math.Sin(float64(i*EmbeddingDim+j)+0.5))
			num = strconv.AppendFloat(num[:0], float64(v), 'g', -1, 32)
			b.Write(num)
		}
		b.WriteString("]}")
	}
	fmt.Fprintf(&b, `],"usage":{"prompt_tokens":%d,"total_tokens":%d,"completion_tokens":0}}`, n*8, n*8)
	return b.Bytes()
}
