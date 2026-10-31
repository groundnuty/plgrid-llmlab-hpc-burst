package main

import (
	"flag"
	"net"
	"net/http"
	"strconv"
)

// runBulk serves GET /bulk?bytes=N (N bytes, written in 32 KiB pieces as fast as the connection
// takes them) and GET /health (200 "ok"), on 127.0.0.1 only.
func runBulk(args []string) error {
	fs := flag.NewFlagSet("bulk", flag.ContinueOnError)
	listen := fs.String("listen", "127.0.0.1:29401", "listen address (127.0.0.1 only)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := requireLoopback(*listen); err != nil {
		return err
	}
	ln, err := net.Listen("tcp4", *listen)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("/bulk", bulkHandler)
	return http.Serve(ln, mux)
}

func bulkHandler(w http.ResponseWriter, r *http.Request) {
	n, err := strconv.ParseInt(r.URL.Query().Get("bytes"), 10, 64)
	if err != nil || n < 1 || n > 1<<30 {
		http.Error(w, "bytes must be 1..1073741824", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(n, 10))
	piece := make([]byte, 32*1024)
	for i := range piece {
		piece[i] = byte('a' + i%26)
	}
	fl, _ := w.(http.Flusher)
	for n > 0 {
		k := int64(len(piece))
		if n < k {
			k = n
		}
		if _, err := w.Write(piece[:k]); err != nil {
			return
		}
		n -= k
		if fl != nil {
			fl.Flush()
		}
	}
}
