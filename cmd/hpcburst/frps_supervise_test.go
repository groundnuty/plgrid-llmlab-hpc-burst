package main

// frps under the supervisor, against a real k3s and the pinned frps (HBEP-6; HBEP-11, Single-user
// host): up starts it beside the controller; it holds exactly its three loopback ports and answers
// /healthz; a killed frps is started again and answers again; status reports it. Judged from
// /proc, ss and the ports themselves, not from the launcher's report.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
)

type frpsEntryRecord struct {
	Name     string     `json:"name"`
	State    string     `json:"state"`
	Proc     *stateProc `json:"proc"`
	Restarts int        `json:"restarts"`
}

func frpsEntryOf(t *testing.T, dir string) (frpsEntryRecord, bool) {
	var r struct {
		Entries []frpsEntryRecord `json:"entries"`
	}
	if b := fileOf(dir + "/controller/supervisor.json"); b == "" || json.Unmarshal([]byte(b), &r) != nil {
		return frpsEntryRecord{}, false
	}
	for _, e := range r.Entries {
		if e.Name == "frps" {
			return e, true
		}
	}
	return frpsEntryRecord{}, false
}

// frpsHealthz is GET /healthz on frps's web server, which answers before its password check.
func frpsHealthz(addr string) bool {
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Get("http://" + addr + "/healthz")
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// TestFrpsSupervision: frps runs as a child of the supervisor, from the verified descriptor (its
// command name is "3", the descriptor's number, so nothing may find it by name), holds exactly
// base+9, base+10 and base+11 on loopback, and answers /healthz on base+11. Killed, it is started
// again, with a new PID that holds the same ports and answers; status then reports it running,
// with one restart.
func TestFrpsSupervision(t *testing.T) {
	const base = 29220
	h := newHost(t)
	ctl, _ := fakeController(t)
	r := cli(t, h.upArgs(base, append(frpsArgs(t), "--json", "--controller-bin", ctl)...)...)
	if r.code != exitOK {
		t.Fatalf("up exited %d: %s", r.code, r.err)
	}
	var up struct {
		FrpsPorts []int `json:"frps_ports"`
	}
	parseJSON(t, r.out, &up)
	wantPorts := []int{base + 9, base + 10, base + 11}
	if !slices.Equal(up.FrpsPorts, wantPorts) {
		t.Errorf("up reports frps's ports %v, want %v", up.FrpsPorts, wantPorts)
	}
	wantListen := []string{fmt.Sprintf("127.0.0.1:%d", base+9), fmt.Sprintf("127.0.0.1:%d", base+10), fmt.Sprintf("127.0.0.1:%d", base+11)}
	web := fmt.Sprintf("127.0.0.1:%d", base+11)

	// check judges one frps process from /proc and its ports.
	check := func(pid int, sup int) {
		t.Helper()
		if ppidOfPID(t, pid) != sup {
			t.Fatalf("frps (pid %d) is not a child of the supervisor (pid %d)", pid, sup)
		}
		if comm := strings.TrimSpace(fileOf(fmt.Sprintf("/proc/%d/comm", pid))); comm != "3" {
			t.Errorf("frps's command name is %q, want 3: it must run the verified descriptor /proc/self/fd/3", comm)
		}
		waitFor(t, 30*time.Second, "frps to hold its three ports", func() bool {
			return slices.Equal(sortedStrings(listenersOf(t, pid)), wantListen)
		})
		waitFor(t, 30*time.Second, "frps's /healthz", func() bool { return frpsHealthz(web) })
	}

	sup := recordedProcs(t, h.dir)[1]
	var fe frpsEntryRecord
	waitFor(t, 30*time.Second, "frps to run", func() bool {
		var ok bool
		fe, ok = frpsEntryOf(t, h.dir)
		return ok && fe.State == "running" && fe.Proc != nil
	})
	check(fe.Proc.PID, sup.PID)

	// ---- a killed frps is started again ----
	old := *fe.Proc
	verifiedKill(t, old)
	waitFor(t, 60*time.Second, "frps to be started again", func() bool {
		e, ok := frpsEntryOf(t, h.dir)
		if ok && e.State == "running" && e.Proc != nil && e.Proc.PID != old.PID && e.Restarts == 1 {
			fe = e
			return true
		}
		return false
	})
	if running(old.PID) {
		t.Errorf("the killed frps (pid %d) still runs", old.PID)
	}
	check(fe.Proc.PID, sup.PID)

	// ---- status reports it ----
	st, code := statusOf(t, h)
	got := -1
	for i := range st.Controllers {
		if st.Controllers[i].Name == "frps" {
			got = i
		}
	}
	if got < 0 {
		t.Fatalf("status does not report frps (exit %d, reasons %v)", code, st.Reasons)
	}
	c := st.Controllers[got]
	if code != exitOK || !c.Running || c.PID != fe.Proc.PID || c.Restarts != 1 || c.HealthzOK == nil || !*c.HealthzOK {
		t.Errorf("status exited %d and reports frps as %+v (reasons %v)", code, c, st.Reasons)
	}
}
