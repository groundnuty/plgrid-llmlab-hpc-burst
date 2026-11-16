package launcher

import (
	"strings"
	"testing"
)

// The literals are the HBEP-8 sizes, worked out by hand: GiB is 2^30 and GB is 10^9.
func TestParseByteSize(t *testing.T) {
	for in, want := range map[string]int64{
		"2GiB":        2147483648,
		"2 GiB":       2147483648,
		"2gib":        2147483648,
		"15GB":        15000000000,
		"15gb":        15000000000,
		"1GB":         1000000000,
		"64MiB":       67108864,
		"64 MB":       64000000,
		"1TiB":        1099511627776,
		"512KiB":      524288,
		"7516192768":  7516192768,
		"7516192768B": 7516192768,
		" 3 GiB ":     3221225472,
	} {
		if got, err := ParseByteSize(in); err != nil || got != want {
			t.Errorf("ParseByteSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "GiB", "2G", "2M", "1.5GiB", "-1GiB", "0", "0GB", "2 gigs", "2GiBs", "0x10", "99999999999TiB"} {
		if got, err := ParseByteSize(bad); err == nil {
			t.Errorf("ParseByteSize(%q) = %d, want an error", bad, got)
		}
	}
}

func TestFormatByteSizeSaysTheNumberItWasMadeFrom(t *testing.T) {
	for in, want := range map[int64]string{
		2147483648:     "2 GiB",
		15000000000:    "15 GB",
		23500000000:    "23500 MB",
		23589934592:    "23589934592 B",
		22516192768:    "22516192768 B",
		7516192768:     "7 GiB",
		67108864:       "64 MiB",
		1000:           "1 KB",
		1023:           "1023 B",
		0:              "0 B",
		14778774528:    "14432397 KiB",
		4294967296:     "4 GiB",
		1099511627776:  "1 TiB",
		1000000000000:  "1 TB",
		3000000000:     "3 GB",
		1500000000:     "1500 MB",
		100663296:      "96 MiB",
		7516192768 + 1: "7516192769 B",
	} {
		got := FormatByteSize(in)
		if got != want {
			t.Errorf("FormatByteSize(%d) = %q, want %q", in, got, want)
		}
		// Whatever it says reads back as the same number (except the plain-bytes form's own unit).
		if back, err := ParseByteSize(strings.ReplaceAll(got, " ", "")); in > 0 && (err != nil || back != in) {
			t.Errorf("%q reads back as %d, %v; want %d", got, back, err, in)
		}
	}
}

func TestByteSizeIsAFlagValue(t *testing.T) {
	var b ByteSize
	if b.String() != "" {
		t.Errorf("an unset size prints %q, want nothing", b.String())
	}
	if err := b.Set("2GiB"); err != nil || b != 2147483648 || b.String() != "2 GiB" {
		t.Errorf("Set(2GiB): %v, %d, %q", err, b, b.String())
	}
	if err := b.Set("two"); err == nil || b != 2147483648 {
		t.Errorf("a bad size changed the value or was accepted: %v, %d", err, b)
	}
}
