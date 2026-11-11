package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/inventory"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The harness's own unit tests. The stack runs under the driver (s16.py), never under go test.

// Two plugin calls on one keep-alive connection, as frps's client makes them, are two records,
// each with its op, proxy name and verdict, and a latency that covers the handler's own time.
func TestTimedListenerRecordsEachCallOnAKeptConnection(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "calls.jsonl")
	cl, err := openCallLog(logPath)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	const hold = 30 * time.Millisecond
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		time.Sleep(hold) // the handler's own time, which the record must include
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(string(body), `"op":"NewProxy"`) {
			_, _ = io.WriteString(w, `{"reject":true,"reject_reason":"proxy_name is not the UID of an Instance placed on this Allocation","unchange":true}`)
			return
		}
		_, _ = io.WriteString(w, `{"reject":false,"unchange":true}`)
	})
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(newTimedListener(ln, "admit", cl)) }()
	defer srv.Close()
	tr := &http.Transport{MaxIdleConnsPerHost: 1}
	c := &http.Client{Transport: tr}
	post := func(op, body string) {
		resp, err := c.Post("http://"+ln.Addr().String()+agentapi.FrpsPluginPath+"?version=0.1.0&op="+op, "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
	}
	post("Login", `{"version":"0.1.0","op":"Login","content":{"metas":{"token":"x"}}}`)
	post("NewProxy", `{"version":"0.1.0","op":"NewProxy","content":{"user":{"metas":{"token":"x"}},"proxy_name":"00000000-0000-4000-8000-000000000001"}}`)
	if err := cl.Close(); err != nil {
		t.Fatal(err)
	}
	var recs []callRec
	f, err := os.Open(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var r callRec
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		recs = append(recs, r)
	}
	if len(recs) != 2 {
		t.Fatalf("%d records, want 2: %+v", len(recs), recs)
	}
	if recs[0].Op != "Login" || recs[0].Reject || recs[0].Status != "200" || recs[0].Plugin != "admit" {
		t.Errorf("first record %+v, want an accepted Login answered 200", recs[0])
	}
	if recs[1].Op != "NewProxy" || !recs[1].Reject || recs[1].Proxy != "00000000-0000-4000-8000-000000000001" ||
		!strings.HasPrefix(recs[1].Reason, "proxy_name is not") {
		t.Errorf("second record %+v, want a refused NewProxy for the UID with its reason", recs[1])
	}
	for _, r := range recs {
		if d := time.Duration(r.EndNs - r.StartNs); d < hold {
			t.Errorf("%s took %v, less than the handler's %v", r.Op, d, hold)
		}
	}
}

func TestOpOfReadsTheQueryFirst(t *testing.T) {
	head := []byte("POST /handler?version=0.1.0&op=CloseProxy HTTP/1.1\r\nHost: x\r\n\r\n{\"op\":\"NewProxy\"}")
	if got := opOf(head); got != "CloseProxy" {
		t.Errorf("opOf = %q, want the query's CloseProxy", got)
	}
	if got := opOf([]byte("POST /handler HTTP/1.1\r\n\r\n{\"op\": \"Login\"}")); got != "Login" {
		t.Errorf("opOf without a query = %q, want the body's Login", got)
	}
}

func TestSpecCheckRefusesWhatACommandWouldMisread(t *testing.T) {
	good := Spec{Mode: ModeDirect, Tunnels: []Tunnel{{Index: 0, UID: "00000000-0000-4000-8000-000000000001",
		Allocation: "10000000-0000-4000-8000-000000000001", Token: "k", Model: "s16-m0000", Socket: "/s/0"}}}
	if err := good.check(); err != nil {
		t.Fatalf("a good spec: %v", err)
	}
	bad := map[string]func(*Spec){
		"mode":        func(s *Spec) { s.Mode = "ssh" },
		"no tunnels":  func(s *Spec) { s.Tunnels = nil },
		"index":       func(s *Spec) { s.Tunnels[0].Index = 1 },
		"no token":    func(s *Spec) { s.Tunnels[0].Token = "" },
		"uid form":    func(s *Spec) { s.Tunnels[0].UID = "u-1" },
		"repeat":      func(s *Spec) { s.Tunnels = append(s.Tunnels, s.Tunnels[0]); s.Tunnels[1].Index = 1 },
		"no model":    func(s *Spec) { s.Tunnels[0].Model = "" },
		"no socket":   func(s *Spec) { s.Tunnels[0].Socket = "" },
		"alloc form":  func(s *Spec) { s.Tunnels[0].Allocation = "a" },
		"same uid as": func(s *Spec) { s.Tunnels[0].Allocation = s.Tunnels[0].UID },
	}
	for name, edit := range bad {
		s := good
		s.Tunnels = append([]Tunnel(nil), good.Tunnels...)
		edit(&s)
		if err := s.check(); err == nil {
			t.Errorf("%s: check accepted it", name)
		}
	}
}

// setup writes frps's configuration with both server plugins, in HBEP-6's order: the
// controller's at base+8, then the front's on its health address, base+4 (Admission).
func TestSetupWritesFrpsWithBothPluginsInOrder(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "c")
	if err := runSetup([]string{"-dir", dir, "-n", "2", "-base", "30130"}); err != nil {
		t.Fatal(err)
	}
	sp, err := readSpec(filepath.Join(dir, "spec.json"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(sp.FrpsConfig)
	if err != nil {
		t.Fatal(err)
	}
	cfg := string(b)
	admit := strings.Index(cfg, `addr = "127.0.0.1:30138"`)
	front := strings.Index(cfg, `addr = "127.0.0.1:30134"`)
	if admit < 0 || front < 0 || admit > front {
		t.Fatalf("frps.toml: the controller's plugin at 30138 (at %d) must come before the front's at 30134 (at %d):\n%s", admit, front, cfg)
	}
	if !strings.Contains(cfg, `ops = ["Login", "NewProxy", "CloseProxy"]`) || !strings.Contains(cfg, `ops = ["NewProxy", "CloseProxy"]`) {
		t.Errorf("frps.toml lacks HBEP-6's ops for one of the plugins:\n%s", cfg)
	}
	for _, tun := range sp.Tunnels {
		fc, err := os.ReadFile(tun.Frpc)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(fc), tun.Token) || !strings.Contains(string(fc), tun.UID) || !strings.Contains(string(fc), "30143") {
			t.Errorf("tunnel %d's frpc.toml does not carry its token, its UID and the link's port", tun.Index)
		}
		if _, err := agentapi.ReadKeyFile(agentapi.InstanceKeyPath(filepath.Join(sp.StateDir, agentapi.CredentialsDirName), tun.UID)); err != nil {
			t.Errorf("tunnel %d's Instance key: %v", tun.Index, err)
		}
	}
}

// The front's own watch reads the fixture: one ModelDeployment per tunnel serving its model,
// and one ready Instance per tunnel under the tunnel's UID.
func TestTheFrontsWatchReadsOneDeploymentAndInstancePerTunnel(t *testing.T) {
	sp := Spec{Mode: ModeDirect, DeploymentsNS: "s16", ProviderName: "example-a", ProfileName: "example-batch"}
	for i := 0; i < 3; i++ {
		sp.Tunnels = append(sp.Tunnels, Tunnel{Index: i, UID: "00000000-0000-4000-8000-00000000000" + string(rune('1'+i)),
			Allocation: "10000000-0000-4000-8000-00000000000" + string(rune('1'+i)), Token: "k", Model: "s16-m000" + string(rune('0'+i)), Socket: "/s"})
	}
	objs, err := fixtureObjects(sp, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	w := inventory.NewWatch(fixtureClient(objs), nil, func() {})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	snap, problems := w.Snapshot()
	if len(problems) > 0 {
		t.Fatalf("the watch skipped objects: %v", problems)
	}
	if len(snap.Deployments) != 3 || len(snap.Instances) != 3 {
		t.Fatalf("the watch read %d deployments and %d instances, want 3 and 3", len(snap.Deployments), len(snap.Instances))
	}
	models := map[string]bool{}
	for _, d := range snap.Deployments {
		models[d.Spec.Model] = true
	}
	for _, tun := range sp.Tunnels {
		if !models[tun.Model] {
			t.Errorf("no deployment serves %s", tun.Model)
		}
	}
	uids := map[string]bool{}
	for _, in := range snap.Instances {
		uids[string(in.UID)] = in.Status.Phase == "ready"
	}
	for _, tun := range sp.Tunnels {
		if !uids[tun.UID] {
			t.Errorf("no ready Instance under %s", tun.UID)
		}
	}
}

// An error event (the front's cut) is the request's error, after the tokens before it.
func TestReadEventsKeepsTheTokensAndTheCut(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"content\":\"b\"}}]}\n\n" +
		"data: {\"error\":{\"code\":\"stream_cut\",\"detail\":\"upstream\"}}\n\n"
	var r reqRec
	err := readEvents(bufio.NewReader(strings.NewReader(body)), time.Now(), &r)
	if err == nil || !strings.Contains(err.Error(), "stream_cut") {
		t.Fatalf("readEvents = %v, want the cut's error event", err)
	}
	if len(r.Tokens) != 2 || r.Done {
		t.Fatalf("tokens %d, done %v; want 2 tokens and no [DONE]", len(r.Tokens), r.Done)
	}
	ok := "data: {\"choices\":[{\"delta\":{\"content\":\"a\"},\"finish_reason\":\"length\"}]}\n\ndata: [DONE]\n\n"
	var r2 reqRec
	if err := readEvents(bufio.NewReader(strings.NewReader(ok)), time.Now(), &r2); err != nil || !r2.Done || !r2.Fin || len(r2.Tokens) != 1 {
		t.Fatalf("a whole stream: err %v, %+v", err, r2)
	}
}
