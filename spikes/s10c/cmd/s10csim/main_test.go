package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestBadArgumentsExitWithStatus2(t *testing.T) {
	for _, args := range [][]string{
		{"-seeds", "one"},
		{"-no-such-flag"},
		{"-s10b-results", "/nonexistent-dir-for-test"},
	} {
		var out, errb bytes.Buffer
		if got := run(args, &out, &errb); got != 2 {
			t.Errorf("run(%v) = %d, want 2 (stderr: %s)", args, got, strings.TrimSpace(errb.String()))
		}
	}
}

func TestHelpExitsWithStatus0(t *testing.T) {
	var out, errb bytes.Buffer
	if got := run([]string{"-h"}, &out, &errb); got != 0 {
		t.Errorf("run(-h) = %d, want 0", got)
	}
}

func TestOptionRowsPicksOneOption(t *testing.T) {
	summary := "regime,key,option,x\ntight,k,a,1\ntight,k,b,2\ndrain,k,b,3\n"
	got, err := optionRows(summary, "b")
	if err != nil || got != "regime,key,option,x\ntight,k,b,2\ndrain,k,b,3\n" {
		t.Errorf("optionRows = %q, %v", got, err)
	}
	if _, err := optionRows(summary, "c"); err == nil {
		t.Error("no error for an option with no rows")
	}
}
