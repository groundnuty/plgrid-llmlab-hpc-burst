package scenario_test

// The front's metrics as the steps read them (HBEP-8). The rest of the front's
// client side is stack.Client.

import (
	"bufio"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// frontCounter sums a counter's samples on the front's /metrics (base+4).
func (r *rig) frontCounter(name string) (float64, bool) {
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Get(r.ports.URL(4, "/metrics"))
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	var sum float64
	sc := bufio.NewScanner(io.LimitReader(resp.Body, 8<<20))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		l := sc.Text()
		if !strings.HasPrefix(l, name+"{") && !strings.HasPrefix(l, name+" ") {
			continue
		}
		f := strings.Fields(l)
		v, err := strconv.ParseFloat(f[len(f)-1], 64)
		if err == nil {
			sum += v
		}
	}
	return sum, resp.StatusCode == http.StatusOK
}
