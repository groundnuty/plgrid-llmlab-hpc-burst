package stage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sync"
)

// readBuffer is each rehash reader's buffer (spike S7: 8 MiB).
const readBuffer = 8 << 20

// hashPaths returns the SHA-256 of every non-empty path, with readers files
// read at once, each reader through its one buffer. The results are in the
// order of paths; an empty path is left alone. The caller checks ctx: files
// not reached after a cancel have neither a sum nor an error.
func hashPaths(ctx context.Context, paths []string, readers int) ([]string, []error) {
	sums := make([]string, len(paths))
	errs := make([]error, len(paths))
	var todo []int
	for i, p := range paths {
		if p != "" {
			todo = append(todo, i)
		}
	}
	readers = min(readers, len(todo))
	jobs := make(chan int)
	var wg sync.WaitGroup
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, readBuffer)
			for i := range jobs {
				sums[i], errs[i] = hashFile(ctx, paths[i], buf)
			}
		}()
	}
	for _, i := range todo {
		select {
		case jobs <- i:
		case <-ctx.Done():
		}
	}
	close(jobs)
	wg.Wait()
	return sums, errs
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
