package harness

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/evlayer"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/jsnode"
)

// RunRole runs a child role if S8_ROLE is set and then exits the process. It
// returns normally (and false) in the parent.
func RunRole() bool {
	switch os.Getenv("S8_ROLE") {
	case "":
		return false
	case "host":
		hostMain()
	case "pub":
		pubMain()
	default:
		fmt.Fprintln(os.Stderr, "s8: unknown role", os.Getenv("S8_ROLE"))
		os.Exit(2)
	}
	os.Exit(0)
	return true
}

// hostMain runs the embedded server. It prints one READY line with the URL and
// the effective fsync settings, then serves until killed or its stdin closes.
func hostMain() {
	n, err := jsnode.Start(jsnode.Config{StoreDir: os.Getenv("S8_STORE"), Sync: jsnode.SyncMode(os.Getenv("S8_SYNC"))})
	if err != nil {
		fmt.Fprintln(os.Stderr, "s8 host:", err)
		os.Exit(2)
	}
	fmt.Printf("READY %s sync_always=%v sync_interval=%s\n", n.URL(), n.SyncAlways, n.SyncInterval)
	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin)
		n.Stop()
		os.Exit(0)
	}()
	select {}
}

// pubMain publishes events from several workers and prints one line per
// acknowledgement ("ACK <stream seq> <event id>") with a single write each, so
// the parent sees an acknowledgement only after the broker sent it.
func pubMain() {
	url, tag := os.Getenv("S8_URL"), os.Getenv("S8_TAG")
	workers, _ := strconv.Atoi(os.Getenv("S8_WORKERS"))
	resources, _ := strconv.Atoi(os.Getenv("S8_RESOURCES"))
	maxMsgs, _ := strconv.Atoi(os.Getenv("S8_MAX"))
	if workers == 0 {
		workers = 4
	}
	if resources == 0 {
		resources = 5
	}
	if maxMsgs == 0 {
		maxMsgs = 1 << 30
	}
	nc, js, err := evlayer.Connect(url)
	if err != nil {
		fmt.Fprintln(os.Stderr, "s8 pub:", err)
		os.Exit(3)
	}
	defer nc.Close()
	go func() { _, _ = io.Copy(io.Discard, os.Stdin); os.Exit(0) }()
	var wg sync.WaitGroup
	var mu sync.Mutex
	sent := 0
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				mu.Lock()
				if sent >= maxMsgs {
					mu.Unlock()
					return
				}
				sent++
				mu.Unlock()
				id := fmt.Sprintf("%s-w%d-%d", tag, w, i)
				e, err := evlayer.NewEvent(id, "allocation", fmt.Sprintf("r%d", (w+i)%resources), "io.hpcburst.test.v1", time.Now(), map[string]int{"w": w, "i": i})
				if err != nil {
					fmt.Fprintln(os.Stderr, "s8 pub:", err)
					os.Exit(4)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				ack, err := evlayer.Publish(ctx, js, e)
				cancel()
				if err != nil {
					return // the server is gone or not answering: stop, never retry here
				}
				_, _ = os.Stdout.WriteString(fmt.Sprintf("ACK %d %s\n", ack.Sequence, id))
			}
		}(w)
	}
	wg.Wait()
}
