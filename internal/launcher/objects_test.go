package launcher

import "testing"

func TestCompareSnapshots(t *testing.T) {
	rec := func(uid string) ObjRec { return ObjRec{UID: uid, Created: "2026-10-30T10:00:00Z"} }
	before := Snapshot{"/configmaps/a/x": rec("u1"), "/configmaps/a/y": rec("u2"), "apps/deployments/a/z": rec("u3")}

	t.Run("everything kept, one added", func(t *testing.T) {
		after := Snapshot{"/configmaps/a/x": rec("u1"), "/configmaps/a/y": rec("u2"), "apps/deployments/a/z": rec("u3"), "/configmaps/a/new": rec("u9")}
		v := compareSnapshots(before, after)
		if !v.OK() || v.Kept != 3 || v.Before != 3 || v.After != 4 || v.Added != 1 {
			t.Fatalf("%+v", v)
		}
	})
	t.Run("one missing", func(t *testing.T) {
		after := Snapshot{"/configmaps/a/x": rec("u1"), "apps/deployments/a/z": rec("u3")}
		v := compareSnapshots(before, after)
		if v.OK() || v.Kept != 2 || len(v.Missing) != 1 || v.Missing[0] != "/configmaps/a/y" {
			t.Fatalf("%+v", v)
		}
	})
	t.Run("same name, new UID: recreated, not kept", func(t *testing.T) {
		after := Snapshot{"/configmaps/a/x": rec("u1"), "/configmaps/a/y": rec("OTHER"), "apps/deployments/a/z": rec("u3")}
		v := compareSnapshots(before, after)
		if v.OK() || v.Kept != 2 || len(v.Changed) != 1 {
			t.Fatalf("%+v", v)
		}
	})
	t.Run("same UID, other creation time", func(t *testing.T) {
		after := Snapshot{"/configmaps/a/x": {UID: "u1", Created: "2030-01-01T00:00:00Z"}, "/configmaps/a/y": rec("u2"), "apps/deployments/a/z": rec("u3")}
		if v := compareSnapshots(before, after); v.OK() || len(v.Changed) != 1 {
			t.Fatalf("%+v", v)
		}
	})
	t.Run("an empty before proves nothing", func(t *testing.T) {
		if v := compareSnapshots(Snapshot{}, Snapshot{"/a/b/c": rec("u")}); v.OK() {
			t.Fatalf("%+v", v)
		}
	})
}
