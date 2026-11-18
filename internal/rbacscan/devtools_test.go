package rbacscan

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/tools/go/ssa"
)

// Two aids for the next person who meets a "cannot follow its client" problem. They run only when asked.
//
//	RBACSCAN_SITES=1 go test ./internal/rbacscan -run TestPrintSites -v      every site the scan found, and the call sites in dead code
//	RBACSCAN_TRACE=internal/lifecycle/x.go:123 go test ./internal/rbacscan -run TestTraceReceiver -v
//	                                                                          how the scan follows the receiver of the call at that line

func TestPrintSites(t *testing.T) {
	if os.Getenv("RBACSCAN_SITES") == "" {
		t.Skip("set RBACSCAN_SITES=1")
	}
	res, err := realScan()
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range res.Sites {
		t.Logf("%s\t%s\t%s\t%v\t%s", s.Pos, s.Ident, s.What, s.Need, s.Via)
	}
	for _, d := range res.Dead {
		t.Logf("dead\t%s", d)
	}
}

func TestTraceReceiver(t *testing.T) {
	at := os.Getenv("RBACSCAN_TRACE")
	if at == "" {
		t.Skip("set RBACSCAN_TRACE=file:line")
	}
	if _, err := strconv.Atoi(at[strings.LastIndex(at, ":")+1:]); err != nil {
		t.Fatalf("RBACSCAN_TRACE is file:line: %v", err)
	}
	o := realOptions()
	l, err := load(o.Root, o.Patterns, nil)
	if err != nil {
		t.Fatal(err)
	}
	s := &scanner{o: o, l: l, res: &Result{Entries: map[string][]string{}}, problemSeen: map[string]bool{}}
	s.constructors = map[string]constructor{
		"sigs.k8s.io/controller-runtime.NewManager": {0, oManager},
	}
	s.addClientConstructors()
	s.x = &index{s: s}
	s.x.build()
	for _, fn := range l.fns {
		for _, b := range fn.Blocks {
			for _, in := range b.Instrs {
				ci, ok := in.(ssa.CallInstruction)
				if !ok || s.pos(in.Pos()) != at {
					continue
				}
				depth := 0
				s.x.trace = func(v ssa.Value) {
					depth++
					pos := s.pos(v.Pos())
					if !v.Pos().IsValid() && v.Parent() != nil {
						pos = s.funcName(v.Parent())
					}
					t.Logf("%3d %-16T %-8s %s", depth, v, v.Name(), pos)
				}
				res := s.x.resolve(ci.Common().Value)
				t.Log(fmt.Sprintf("origins=%v whys=%v unreached=%v", res.sorted(), res.whys, res.unreached))
				s.x.trace = nil
			}
		}
	}
}
