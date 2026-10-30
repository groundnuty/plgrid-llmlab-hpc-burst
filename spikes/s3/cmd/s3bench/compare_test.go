package main

import "testing"

// The upgrade verdict rests on compareSnapshots; these cases show it can say "not kept".
func TestCompareSnapshots(t *testing.T) {
	before := map[string]objRec{
		"Fleet/a":  {"u1", "t1"},
		"Worker/b": {"u2", "t2"},
		"CRD/c":    {"u3", "t3"},
	}
	t.Run("identical", func(t *testing.T) {
		kept, diffs := compareSnapshots(before, map[string]objRec{"Fleet/a": {"u1", "t1"}, "Worker/b": {"u2", "t2"}, "CRD/c": {"u3", "t3"}})
		if kept != 3 || len(diffs) != 0 {
			t.Fatalf("kept=%d diffs=%v", kept, diffs)
		}
	})
	t.Run("recreated object has a new UID", func(t *testing.T) {
		kept, diffs := compareSnapshots(before, map[string]objRec{"Fleet/a": {"u1", "t1"}, "Worker/b": {"u9", "t2"}, "CRD/c": {"u3", "t3"}})
		if kept != 2 || len(diffs) != 1 {
			t.Fatalf("kept=%d diffs=%v", kept, diffs)
		}
	})
	t.Run("lost and extra objects", func(t *testing.T) {
		kept, diffs := compareSnapshots(before, map[string]objRec{"Fleet/a": {"u1", "t1"}, "Worker/new": {"u5", "t5"}})
		if kept != 1 || len(diffs) != 3 {
			t.Fatalf("kept=%d diffs=%v", kept, diffs)
		}
	})
	t.Run("same UID but other creation time", func(t *testing.T) {
		kept, _ := compareSnapshots(before, map[string]objRec{"Fleet/a": {"u1", "t7"}, "Worker/b": {"u2", "t2"}, "CRD/c": {"u3", "t3"}})
		if kept != 2 {
			t.Fatalf("kept=%d", kept)
		}
	})
}

func TestMedian(t *testing.T) {
	if got := median([]int64{9, 1, 5}); got != 5 {
		t.Fatalf("median = %d", got)
	}
	if got := median(nil); got != 0 {
		t.Fatalf("median(nil) = %d", got)
	}
}
