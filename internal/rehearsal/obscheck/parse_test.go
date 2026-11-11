package obscheck

// Added with the move (issue #376, P2): ParseExposition's error for a label value that never closes was not under a
// test. Another error would follow it anyway (the parser then finds no value for the rest of the line), so the
// message is what shows that the check itself fired: the moved TestParseExposition passed with the check removed.

import (
	"strings"
	"testing"
)

func TestParseExpositionRefusesALabelValueThatNeverCloses(t *testing.T) {
	for _, text := range []string{"x{a=\"1\n", "x{a=\"1\\\"\n", "x{b=\"2\",a=\"1"} {
		ss, err := ParseExposition(text)
		if err == nil || !strings.Contains(err.Error(), "an unterminated label value") {
			t.Errorf("%q parsed as %+v, error %v; want the unterminated label value's error", text, ss, err)
		}
	}
	// The control: the same shapes closed are accepted.
	ss, err := ParseExposition("x{b=\"2\",a=\"1\"} 3\n")
	if err != nil || len(ss) != 1 || ss[0].Labels["a"] != "1" || ss[0].Labels["b"] != "2" || ss[0].Value != 3 {
		t.Errorf("a closed label set: %+v, %v", ss, err)
	}
}
