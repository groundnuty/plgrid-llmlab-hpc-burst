package launcher

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestParseStat(t *testing.T) {
	// Expected values are read off the line by hand, using proc(5): field 3 is the state,
	// field 4 the parent PID and field 22 the start time. The command name may contain spaces
	// and parentheses, so a parser that splits on spaces from the left reads the wrong fields.
	cases := []struct {
		name, line string
		state      byte
		ppid       int
		start      uint64
	}{
		{"plain", "1234 (k3s-server) S 1 1234 1234 0 -1 4194560 100 0 0 0 5 6 0 0 20 0 30 0 987654 1000 200 18446744073709551615 0 0 0 0 0 0 0 0 0 0 0 0 17 3 0 0 0 0 0", 'S', 1, 987654},
		{"name with spaces and parentheses", "77 (a b) (c) R 42 77 77 0 -1 0 0 0 0 0 0 0 0 0 20 0 1 0 555 1 1 0 0 0 0 0 0 0 0 0 0 0 0 17 0 0 0 0 0 0", 'R', 42, 555},
		{"zombie", "9 (sleep) Z 3 9 9 0 -1 4228100 0 0 0 0 0 0 0 0 20 0 1 0 31337 0 0 18446744073709551615 0 0 0 0 0 0 0 0 0 0 0 0 17 0 0 0 0 0 0", 'Z', 3, 31337},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := parseStat(c.line)
			if err != nil {
				t.Fatal(err)
			}
			if got.State != c.state || got.PPID != c.ppid || got.StartTicks != c.start {
				t.Fatalf("got %+v, want state %c ppid %d start %d", got, c.state, c.ppid, c.start)
			}
		})
	}
	for _, bad := range []string{"", "no parenthesis here", "1 (x) S 1 2", "1 (x) S notanumber 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22"} {
		if _, err := parseStat(bad); err == nil {
			t.Errorf("parseStat(%q) accepted a malformed line", bad)
		}
	}
}

func TestParseHexAddr(t *testing.T) {
	// Values as the kernel writes them in /proc/net/tcp and tcp6: each 32-bit word in host
	// (little-endian) order. 0100007F is 127.0.0.1; the IPv6 loopback ::1 is 0000000000000000
	// 0000000001000000.
	cases := map[string]string{
		"0100007F":                         "127.0.0.1",
		"00000000":                         "0.0.0.0",
		"0F02000A":                         "10.0.2.15",
		"00000000000000000000000001000000": "::1",
		"00000000000000000000000000000000": "::",
		"0000000000000000FFFF00000100007F": "127.0.0.1", // ::ffff:127.0.0.1; Go prints a mapped IPv4 address in dotted form
	}
	for in, want := range cases {
		got, err := parseHexAddr(in)
		if err != nil || got != want {
			t.Errorf("parseHexAddr(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "0100", "ZZ00007F"} {
		if _, err := parseHexAddr(bad); err == nil {
			t.Errorf("parseHexAddr(%q) accepted a malformed address", bad)
		}
	}
}

func TestParseListenTable(t *testing.T) {
	// A trimmed /proc/net/tcp. Only state 0A (LISTEN) rows count; the ESTABLISHED row (01) must not.
	const table = `  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 0100007F:7324 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1001        0 11111 1 0000000000000000 100 0 0 10 0
   1: 00000000:0016 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 22222 1 0000000000000000 100 0 0 10 0
   2: 0100007F:E4C0 0100007F:7324 01 00000000:00000000 00:00000000 00000000  1001        0 33333 1 0000000000000000 100 0 0 10 0
`
	got, err := parseListenTable(table)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d listeners, want 2: %+v", len(got), got)
	}
	// 0x7324 = 29476, 0x0016 = 22.
	if got[0].Addr != "127.0.0.1" || got[0].Port != 29476 || got[0].inode != "11111" {
		t.Errorf("first row: %+v", got[0])
	}
	if got[1].Addr != "0.0.0.0" || got[1].Port != 22 || got[1].inode != "22222" || got[1].Loopback() {
		t.Errorf("second row: %+v", got[1])
	}
	if _, err := parseListenTable("header\nshort row\n"); err == nil {
		t.Error("a short row was accepted")
	}
}

// fakeProc points procRoot at a temporary tree for the duration of the test.
func fakeProc(t *testing.T, boot string) string {
	t.Helper()
	root := t.TempDir()
	old := procRoot
	procRoot = root
	t.Cleanup(func() { procRoot = old })
	if err := os.MkdirAll(filepath.Join(root, "sys/kernel/random"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "sys/kernel/random/boot_id"), []byte(boot+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return root
}

// writeFakeThread adds a thread (task) of a fake process.
func writeFakeThread(t *testing.T, root string, pid, tid int, state string) {
	t.Helper()
	dir := filepath.Join(root, strconv.Itoa(pid), "task", strconv.Itoa(tid))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	line := strconv.Itoa(tid) + " (k3s-server) " + state + " 1 1 1 0 -1 0 0 0 0 0 0 0 0 0 20 0 1 0 1000 1 1 0 0 0 0 0 0 0 0 0 0 0 0 17 0 0 0 0 0 0"
	if err := os.WriteFile(filepath.Join(dir, "stat"), []byte(line), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestDescendants(t *testing.T) {
	root := fakeProc(t, "b")
	mk := func(pid, ppid int) {
		dir := filepath.Join(root, strconv.Itoa(pid))
		_ = os.MkdirAll(dir, 0o755)
		line := strconv.Itoa(pid) + " (x) S " + strconv.Itoa(ppid) + " 1 1 0 -1 0 0 0 0 0 0 0 0 0 20 0 1 0 5 1 1 0 0 0 0 0 0 0 0 0 0 0 0 17 0 0 0 0 0 0"
		_ = os.WriteFile(filepath.Join(dir, "stat"), []byte(line), 0o644)
	}
	mk(10, 1)
	mk(11, 10)
	mk(12, 11)
	mk(13, 1) // unrelated
	got := descendants([]int{10})
	want := []int{10, 11, 12}
	if len(got) != len(want) {
		t.Fatalf("descendants = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("descendants = %v, want %v", got, want)
		}
	}
}

// startBystander starts a process the launcher knows nothing about and reaps it when it exits,
// so that a killed bystander is gone from /proc and not a zombie.
func startBystander(t *testing.T, name string, args ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(name, args...)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	})
	return cmd
}

// procState reads the state letter of a process with its own, independent parser (a regexp-free
// version of proc(5)'s rule), so tests do not judge the launcher with the launcher's own code.
// It returns 0 when the process does not exist.
func procState(pid int) byte {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0
	}
	s := string(b)
	rest := strings.TrimSpace(s[strings.LastIndex(s, ")")+1:])
	return rest[0]
}

// runningNow is true while any thread of the process lives. The leader's state alone is not
// enough: it reads Z while another thread is still running (see isDead).
func runningNow(pid int) bool {
	st := procState(pid)
	if st == 0 {
		return false
	}
	if st != 'Z' && st != 'X' {
		return true
	}
	tasks, _ := os.ReadDir("/proc/" + strconv.Itoa(pid) + "/task")
	for _, tk := range tasks {
		if c := procStateAt("/proc/" + strconv.Itoa(pid) + "/task/" + tk.Name() + "/stat"); c != 0 && c != 'Z' && c != 'X' {
			return true
		}
	}
	return false
}

func procStateAt(path string) byte {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	s := string(b)
	return strings.TrimSpace(s[strings.LastIndex(s, ")")+1:])[0]
}

func TestNewRecAndIdentifyRealProcess(t *testing.T) {
	cmd := startBystander(t, "sleep", "300")
	rec, err := newRec("sleep", cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	if v, d := identify(rec); v != Ours {
		t.Fatalf("a process just recorded is %v (%s)", v, d)
	}
	rec.StartTicks++
	if v, _ := identify(rec); v != Foreign {
		t.Fatalf("a record with another start time is %v, want foreign", v)
	}
	rec.StartTicks--
	_ = cmd.Process.Kill()
	deadline := time.Now().Add(5 * time.Second)
	for runningNow(cmd.Process.Pid) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if v, _ := identify(rec); v != Gone {
		t.Fatalf("a killed process is %v, want gone", v)
	}
}
