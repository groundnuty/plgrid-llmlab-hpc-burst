package frpsplugin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// These tests check the Reader's rules against an HTTP double. The double shows what the Reader does
// with each answer; whether frps really answers so is the real-frps tests' job (frp_real_test.go),
// which also read the list from a running frps.

const webPassword = "e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5"

// webDouble serves handler on a loopback port, and returns a Reader for it with the password file.
func webDouble(t *testing.T, handler http.HandlerFunc) (*Reader, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	pwFile := filepath.Join(t.TempDir(), "web-password")
	if err := agentapi.WriteKeyFile(pwFile, webPassword); err != nil {
		t.Fatal(err)
	}
	rd, err := NewReader(ReaderConfig{Addr: strings.TrimPrefix(srv.URL, "http://"), User: agentapi.FrpsWebUser, PasswordFile: pwFile, Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return rd, srv
}

func serve(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }
}

func TestListSendsTheWebCredentialsAndParsesTheList(t *testing.T) {
	var gotUser, gotPass, gotPath string
	rd, _ := webDouble(t, func(w http.ResponseWriter, r *http.Request) {
		gotUser, gotPass, _ = r.BasicAuth()
		gotPath = r.URL.Path
		_, _ = w.Write([]byte(`{"proxies":[
			{"name":"u1","conf":{"customDomains":["u1"]},"todayTrafficIn":0,"curConns":0,"lastStartTime":"01-02 03:04:05","lastCloseTime":"","status":"online"},
			{"name":"u2","conf":null,"lastStartTime":"a","lastCloseTime":"b","status":"offline"}]}`))
	})
	list, err := rd.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if gotUser != agentapi.FrpsWebUser || gotPass != webPassword || gotPath != "/api/proxy/tcpmux" {
		t.Errorf("the request had user %q, a password of %d bytes, path %q", gotUser, len(gotPass), gotPath)
	}
	want := []Proxy{{Name: "u1", Status: "online", LastStartTime: "01-02 03:04:05"}, {Name: "u2", Status: "offline", LastStartTime: "a", LastCloseTime: "b"}}
	if !reflect.DeepEqual(list, want) {
		t.Errorf("list = %+v, want %+v", list, want)
	}
}

func TestAnEmptyListIsReadAsEmpty(t *testing.T) {
	rd, _ := webDouble(t, serve(`{"proxies":[]}`))
	list, err := rd.List(context.Background())
	if err != nil || len(list) != 0 {
		t.Fatalf("list = %+v, %v", list, err)
	}
}

// TestListFailuresAreErrorsNeverAList: whatever the answer, a list that was not read is an error, so
// that no caller turns it into "every Instance is offline".
func TestListFailuresAreErrorsNeverAList(t *testing.T) {
	huge := `{"proxies":[` + strings.Repeat(`{"name":"x","status":"online"},`, maxListBytes/30+10) + `{"name":"y"}]}`
	for _, c := range []struct {
		name    string
		handler http.HandlerFunc
	}{
		{"401", func(w http.ResponseWriter, r *http.Request) { http.Error(w, "Unauthorized", 401) }},
		{"500", func(w http.ResponseWriter, r *http.Request) { http.Error(w, "boom", 500) }},
		{"404", func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) }},
		{"a redirect", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/elsewhere", http.StatusFound) }},
		{"a 200 with a page", serve(`<html><body>frp dashboard</body></html>`)},
		{"a 200 with another object", serve(`{"code":200,"msg":"ok"}`)},
		{"a 200 with an empty object", serve(`{}`)},
		{"a 200 with null proxies", serve(`{"proxies":null}`)},
		{"a 200 with proxies of the wrong type", serve(`{"proxies":"x"}`)},
		{"a 200 with an array", serve(`[]`)},
		{"a 200 with nothing", serve(``)},
		{"a null entry", serve(`{"proxies":[null]}`)},
		{"an entry that is not an object", serve(`{"proxies":[1]}`)},
		{"an entry with no fields", serve(`{"proxies":[{}]}`)},
		{"an entry with a name and no status", serve(`{"proxies":[{"name":"u1"}]}`)},
		{"an entry with a status and no name", serve(`{"proxies":[{"status":"online"}]}`)},
		{"an entry with an empty name", serve(`{"proxies":[{"name":"","status":"online"}]}`)},
		{"an unknown status", serve(`{"proxies":[{"name":"u1","status":"starting"}]}`)},
		{"a status in capitals", serve(`{"proxies":[{"name":"u1","status":"Online"}]}`)},
		{"an online entry beside a null one", serve(`{"proxies":[{"name":"u1","status":"online"},null]}`)},
		{"an online entry beside one with an unknown status", serve(`{"proxies":[{"name":"u1","status":"online"},{"name":"u2","status":"gone"}]}`)},
		{"conflicting duplicates", serve(`{"proxies":[{"name":"u1","status":"online"},{"name":"u1","status":"offline"}]}`)},
		{"identical duplicates", serve(`{"proxies":[{"name":"u1","status":"online"},{"name":"u1","status":"online"}]}`)},
		{"a truncated list", serve(`{"proxies":[{"name":"u1","sta`)},
		{"a list longer than the limit", serve(huge)},
		{"an answer that does not come", func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
			case <-time.After(5 * time.Second):
			}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			rd, _ := webDouble(t, c.handler)
			rd.cfg.Timeout = 300 * time.Millisecond
			rd.client.Timeout = 300 * time.Millisecond
			start := time.Now()
			list, err := rd.List(context.Background())
			if err == nil {
				t.Fatalf("got the list %+v", list)
			}
			if list != nil {
				t.Errorf("a list %+v came with the error", list)
			}
			if strings.Contains(err.Error(), webPassword) {
				t.Errorf("the error contains the password: %v", err)
			}
			if took := time.Since(start); took > 2*time.Second {
				t.Errorf("took %s", took)
			}
			rep := rd.Read(context.Background(), []string{"u1"})
			if st, reason := condOf(rep, "u1"); st != metav1.ConditionUnknown || reason != v1alpha1.ReasonProxyListUnreadable {
				t.Errorf("Read: %s/%s, want Unknown/ProxyListUnreadable", st, reason)
			}
		})
	}
}

func TestListWithNobodyListeningIsAnError(t *testing.T) {
	rd, srv := webDouble(t, serve(`{"proxies":[]}`))
	srv.Close()
	if _, err := rd.List(context.Background()); err == nil {
		t.Fatal("the list was read from a server that is gone")
	}
}

func TestListWithAPasswordFileThatCannotBeReadDoesNotContactFrps(t *testing.T) {
	var hits atomic.Int32
	rd, _ := webDouble(t, func(w http.ResponseWriter, r *http.Request) { hits.Add(1); _, _ = w.Write([]byte(`{"proxies":[]}`)) })
	for name, prep := range map[string]func(){
		"missing":            func() { _ = os.Remove(rd.cfg.PasswordFile) },
		"readable by others": func() { _ = os.Chmod(rd.cfg.PasswordFile, 0o644) },
		"not a key's form": func() {
			_ = os.Chmod(rd.cfg.PasswordFile, 0o600)
			_ = os.WriteFile(rd.cfg.PasswordFile, []byte("short"), 0o600)
		},
		"a symbolic link": func() { _ = os.Remove(rd.cfg.PasswordFile); _ = os.Symlink("/etc/hostname", rd.cfg.PasswordFile) },
	} {
		t.Run(name, func(t *testing.T) {
			_ = agentapi.WriteKeyFile(rd.cfg.PasswordFile, webPassword)
			prep()
			if _, err := rd.List(context.Background()); err == nil {
				t.Fatal("the list was read with an unusable password file")
			}
			if hits.Load() != 0 {
				t.Errorf("frps was contacted %d times", hits.Load())
			}
		})
	}
}

// TestListReadsThePasswordFileAtEveryRead: a replaced password is picked up without a restart.
func TestListReadsThePasswordFileAtEveryRead(t *testing.T) {
	const other = "0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f"
	rd, _ := webDouble(t, func(w http.ResponseWriter, r *http.Request) {
		if _, p, _ := r.BasicAuth(); p != other {
			http.Error(w, "no", 401)
			return
		}
		_, _ = w.Write([]byte(`{"proxies":[]}`))
	})
	if _, err := rd.List(context.Background()); err == nil {
		t.Fatal("the old password was accepted")
	}
	if err := agentapi.WriteKeyFile(rd.cfg.PasswordFile, other); err != nil {
		t.Fatal(err)
	}
	if _, err := rd.List(context.Background()); err != nil {
		t.Fatalf("after the password file changed: %v", err)
	}
}

func TestBuildReportMapsTheListToTheConditions(t *testing.T) {
	list := []Proxy{
		{Name: "on", Status: "online"},
		{Name: "off", Status: "offline"},
		{Name: "orphan-b", Status: "online"},
		{Name: "orphan-a", Status: "online"},
		{Name: "orphan-gone", Status: "offline"}, // an offline entry no one owns is no orphan
	}
	rep := BuildReport([]string{"on", "off", "missing"}, list, nil)
	for _, c := range []struct {
		uid    string
		status metav1.ConditionStatus
		reason string
	}{
		{"on", metav1.ConditionTrue, v1alpha1.ReasonProxyOnline},
		{"off", metav1.ConditionFalse, v1alpha1.ReasonProxyOffline},
		{"missing", metav1.ConditionFalse, v1alpha1.ReasonProxyOffline},
	} {
		got, ok := rep.Conditions[c.uid]
		if !ok || got.Type != v1alpha1.ConditionTunnelConnected || got.Status != c.status || got.Reason != c.reason || got.Message == "" {
			t.Errorf("%s: %+v, want %s/%s with a message", c.uid, got, c.status, c.reason)
		}
	}
	if len(rep.Conditions) != 3 {
		t.Errorf("%d conditions for 3 Instances", len(rep.Conditions))
	}
	if want := []string{"orphan-a", "orphan-b"}; !reflect.DeepEqual(rep.Orphans, want) {
		t.Errorf("orphans = %v, want %v", rep.Orphans, want)
	}
	if rep.Err != nil {
		t.Errorf("Err = %v", rep.Err)
	}
}

func TestBuildReportOfAnUnreadableListIsUnknownForEveryInstance(t *testing.T) {
	// A list in hand beside the error must not matter: the error wins, so a stale or partial list is never used.
	rep := BuildReport([]string{"a", "b"}, []Proxy{{Name: "a", Status: "online"}}, context.DeadlineExceeded)
	for _, uid := range []string{"a", "b"} {
		if c := rep.Conditions[uid]; c.Status != metav1.ConditionUnknown || c.Reason != v1alpha1.ReasonProxyListUnreadable || !strings.Contains(c.Message, "deadline") {
			t.Errorf("%s: %+v", uid, c)
		}
	}
	if len(rep.Orphans) != 0 || rep.Err == nil {
		t.Errorf("orphans %v, err %v", rep.Orphans, rep.Err)
	}
}

func TestNewReaderChecksItsConfiguration(t *testing.T) {
	ok := ReaderConfig{Addr: "127.0.0.1:1", User: "u", PasswordFile: "/p"}
	if _, err := NewReader(ok); err != nil {
		t.Fatal(err)
	}
	for name, mod := range map[string]func(*ReaderConfig){
		"no address":         func(c *ReaderConfig) { c.Addr = "" },
		"no user":            func(c *ReaderConfig) { c.User = "" },
		"no password file":   func(c *ReaderConfig) { c.PasswordFile = "" },
		"a negative timeout": func(c *ReaderConfig) { c.Timeout = -1 },
	} {
		c := ok
		mod(&c)
		if _, err := NewReader(c); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestNewHostReaderUsesTheHostsWebAddressUserAndPasswordFile(t *testing.T) {
	h := agentapi.Host{StateDir: "/state", PortBase: 40000}
	rd, err := NewHostReader(h)
	if err != nil {
		t.Fatal(err)
	}
	want := ReaderConfig{Addr: "127.0.0.1:40011", User: agentapi.FrpsWebUser, PasswordFile: "/state/frps/web-password", Timeout: DefaultReadTimeout}
	if rd.cfg != want {
		t.Errorf("config = %+v, want %+v", rd.cfg, want)
	}
}

// TestBuildReportOfAListNoFrpsWritesIsUnknownForEveryInstance: BuildReport takes a list from any
// caller, so it checks the list itself. A guess of "offline" for an entry it cannot read, or an
// "online" from a list it half trusts, would put a wrong value in the condition.
func TestBuildReportOfAListNoFrpsWritesIsUnknownForEveryInstance(t *testing.T) {
	for _, c := range []struct {
		name string
		list []Proxy
	}{
		{"an entry with no fields", []Proxy{{}}},
		{"no name", []Proxy{{Status: "online"}}},
		{"no status", []Proxy{{Name: "u1"}}},
		{"an unknown status", []Proxy{{Name: "u1", Status: "starting"}}},
		{"a bad entry beside an online one", []Proxy{{Name: "u1", Status: "online"}, {Name: "u2"}}},
		{"conflicting duplicates", []Proxy{{Name: "u1", Status: "online"}, {Name: "u1", Status: "offline"}}},
		{"identical duplicates", []Proxy{{Name: "u1", Status: "online"}, {Name: "u1", Status: "online"}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			rep := BuildReport([]string{"u1", "u2"}, c.list, nil)
			if rep.Err == nil {
				t.Fatal("no error for a list no frps writes")
			}
			for _, uid := range []string{"u1", "u2"} {
				if st, reason := condOf(rep, uid); st != metav1.ConditionUnknown || reason != v1alpha1.ReasonProxyListUnreadable {
					t.Errorf("%s: %s/%s, want Unknown/ProxyListUnreadable", uid, st, reason)
				}
			}
			if len(rep.Orphans) != 0 {
				t.Errorf("orphans %v from a list that was not read", rep.Orphans)
			}
		})
	}
}
