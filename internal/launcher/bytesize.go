package launcher

import (
	"fmt"
	"strconv"
	"strings"
)

// The byte sizes of the event log's flags: a number and a unit, where GiB, MiB, KiB and TiB are
// powers of 1,024 and GB, MB, KB and TB powers of 1,000. HBEP-8 gives the caps in GiB and the free-space
// floor in GB (2 GiB per stream, 15 GB), so both families are accepted, and a bare "G" or "M" is
// refused as ambiguous.
var byteUnits = []struct {
	name string
	size int64
}{
	{"TiB", 1 << 40}, {"TB", 1_000_000_000_000},
	{"GiB", 1 << 30}, {"GB", 1_000_000_000},
	{"MiB", 1 << 20}, {"MB", 1_000_000},
	{"KiB", 1 << 10}, {"KB", 1_000},
	{"B", 1},
}

// ParseByteSize reads "2GiB", "15GB", "64 MiB" or a bare number of bytes. A unit's case is not
// significant ("gib" is GiB, "gb" is GB). It refuses a size that is not positive, a fraction, and a
// unit it does not know.
func ParseByteSize(s string) (int64, error) {
	t := strings.TrimSpace(s)
	i := 0
	for i < len(t) && t[i] >= '0' && t[i] <= '9' {
		i++
	}
	if i == 0 {
		return 0, fmt.Errorf("%q is not a size: want a whole number and a unit, such as 2GiB or 15GB", s)
	}
	n, err := strconv.ParseInt(t[:i], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%q is not a size: %v", s, err)
	}
	unit := strings.TrimSpace(t[i:])
	mult := int64(1)
	if unit != "" {
		mult = 0
		for _, u := range byteUnits {
			if strings.EqualFold(unit, u.name) {
				mult = u.size
			}
		}
		if mult == 0 {
			return 0, fmt.Errorf("%q: unit %q is not one of B, KB, MB, GB, TB (powers of 1,000) or KiB, MiB, GiB, TiB (powers of 1,024)", s, unit)
		}
	}
	if n <= 0 {
		return 0, fmt.Errorf("%q is not a positive size", s)
	}
	if n > (1<<62)/mult {
		return 0, fmt.Errorf("%q is too large", s)
	}
	return n * mult, nil
}

// FormatByteSize writes n in the largest unit that divides it exactly, so the text always says the
// number it was made from: 2147483648 is "2 GiB", 15000000000 is "15 GB", 23500000000 is "23500 MB".
func FormatByteSize(n int64) string {
	if n > 0 {
		for _, u := range byteUnits {
			if u.size > 1 && n >= u.size && n%u.size == 0 {
				return fmt.Sprintf("%d %s", n/u.size, u.name)
			}
		}
	}
	return fmt.Sprintf("%d B", n)
}

// ByteSize is a flag.Value for a size flag. The zero value is "not given".
type ByteSize int64

// Set parses the flag's text.
func (b *ByteSize) Set(s string) error {
	n, err := ParseByteSize(s)
	if err != nil {
		return err
	}
	*b = ByteSize(n)
	return nil
}

func (b ByteSize) String() string {
	if b == 0 {
		return ""
	}
	return FormatByteSize(int64(b))
}
