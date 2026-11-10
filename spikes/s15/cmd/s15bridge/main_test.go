package main

import "testing"

func TestCheckRefusesWhatIsNotOneLoopbackLine(t *testing.T) {
	ok := "srun -u --overlap --jobid=1000 --ntasks=1 /a relay serving --dir /d --allocation u --epoch 1 --seq 1"
	if err := check("127.0.0.1:29925", ok); err != nil {
		t.Fatalf("a loopback address and one line: %v", err)
	}
	for _, c := range []struct{ addr, line string }{
		{"10.0.0.1:29925", ok},
		{"localhost:29925", ok},
		{"127.0.0.1", ok},
		{"127.0.0.1:29925", ""},
		{"127.0.0.1:29925", ok + "\nmore"},
	} {
		if err := check(c.addr, c.line); err == nil {
			t.Errorf("check(%q, %q) accepted it", c.addr, c.line)
		}
	}
}
