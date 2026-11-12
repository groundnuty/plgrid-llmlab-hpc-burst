package frpsplugin

// NewHostBulkReader (beta; HBEP-6, Path state: "With the bulk path on, the second frps's list at
// base+15 is read the same way", with the second frps's own web password; #448 part 3).

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// TestHostBulkReaderReadsPort15WithTheSecondFrpsPassword: the reader of the second frps asks
// 127.0.0.1:base+15 and sends frps-bulk's own web password, never the first frps's. The web double
// answers only the password in <state>/frps-bulk/web-password; the file of the first frps, beside it
// in <state>/frps/, holds another. The base is the double's port minus 15, so the address is the
// one the reader chose and not one the test passed. A reader that took the first's password file
// is answered 401, and the test's control shows that answer is an error.
func TestHostBulkReaderReadsPort15WithTheSecondFrpsPassword(t *testing.T) {
	const bulkPassword = "b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7"
	var gotPath, gotUser string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pw, _ := r.BasicAuth()
		if pw != bulkPassword {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		gotPath, gotUser = r.URL.Path, user
		_, _ = w.Write([]byte(`{"proxies":[{"name":"bulk-uid","status":"online"}]}`))
	}))
	t.Cleanup(srv.Close)
	_, portStr, err := net.SplitHostPort(srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatal(err)
	}
	h := agentapi.Host{StateDir: t.TempDir(), PortBase: port - 15}
	for dir, pw := range map[string]string{"frps": webPassword, "frps-bulk": bulkPassword} {
		if err := os.MkdirAll(filepath.Join(h.StateDir, dir), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := agentapi.WriteKeyFile(filepath.Join(h.StateDir, dir, "web-password"), pw); err != nil {
			t.Fatal(err)
		}
	}

	rd, err := NewHostBulkReader(h)
	if err != nil {
		t.Fatal(err)
	}
	list, err := rd.List(context.Background())
	if err != nil {
		t.Fatalf("the bulk reader at base+15 with frps-bulk's password: %v", err)
	}
	if len(list) != 1 || list[0].Name != "bulk-uid" || list[0].Status != StatusOnline {
		t.Errorf("list = %+v, want the one online proxy bulk-uid", list)
	}
	if gotPath != "/api/proxy/tcpmux" || gotUser != agentapi.FrpsWebUser {
		t.Errorf("the request was for %q as %q, want /api/proxy/tcpmux as %q", gotPath, gotUser, agentapi.FrpsWebUser)
	}

	// The control: a reader that sends the first frps's password is refused by the double, so the
	// pass above is the second file's doing.
	first, err := NewReader(ReaderConfig{Addr: srv.Listener.Addr().String(), User: agentapi.FrpsWebUser,
		PasswordFile: filepath.Join(h.StateDir, "frps", "web-password"), Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.List(context.Background()); err == nil {
		t.Error("the first frps's password was accepted by the double: the test cannot tell the files apart")
	}
}
