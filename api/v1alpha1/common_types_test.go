package v1alpha1

import "testing"

// An unset mode, an empty policy and no policy at all are Keep, HBEP-7's
// decided default; a mode that is set is taken as it is.
func TestRetentionPolicyEffectiveMode(t *testing.T) {
	var none *RetentionPolicy
	for _, tc := range []struct {
		name string
		p    *RetentionPolicy
		want RetentionMode
	}{
		{"no policy", none, RetentionKeep},
		{"an empty policy", &RetentionPolicy{}, RetentionKeep},
		{"Keep", &RetentionPolicy{Mode: RetentionKeep}, RetentionKeep},
		{"ReleaseWhenIdle", &RetentionPolicy{Mode: RetentionReleaseWhenIdle}, RetentionReleaseWhenIdle},
	} {
		if got := tc.p.EffectiveMode(); got != tc.want {
			t.Errorf("%s: EffectiveMode = %q, want %q", tc.name, got, tc.want)
		}
	}
}
