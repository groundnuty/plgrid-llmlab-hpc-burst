package study

import (
	"fmt"
	"strings"

	b "github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10b/study"
)

// CompareWithS10b renders each re-run cell as S10b's CellsCSV does and
// compares the line, byte for byte, with the line S10b committed for the
// same regime, key, option and seed. The header must match too, so a column
// added since S10b would be caught. It returns the number of rows compared
// and one message per difference or missing row. The expected values are the
// committed file's, written before this study's changes to the simulator.
func CompareWithS10b(cells []b.Cell, committed string) (compared int, problems []string) {
	lines := strings.Split(strings.TrimSuffix(committed, "\n"), "\n")
	if len(lines) == 0 {
		return 0, []string{"S10b's cells.csv is empty"}
	}
	header := strings.SplitN(b.CellsCSV(nil), "\n", 2)[0]
	if lines[0] != header {
		problems = append(problems, "the column header differs from S10b's cells.csv")
	}
	for _, c := range cells {
		ours := strings.TrimSuffix(strings.SplitN(b.CellsCSV([]b.Cell{c}), "\n", 2)[1], "\n")
		prefix := fmt.Sprintf("%s,%s,%s,%d,", c.Regime, c.Key, c.Option, c.Seed)
		var theirs string
		found := false
		for _, l := range lines[1:] {
			if strings.HasPrefix(l, prefix) {
				theirs, found = l, true
				break
			}
		}
		if !found {
			problems = append(problems, fmt.Sprintf("%s %s %s seed %d: no row in S10b's cells.csv", c.Regime, c.Option, c.Key, c.Seed))
			continue
		}
		compared++
		if ours != theirs {
			problems = append(problems, fmt.Sprintf("%s %s %s seed %d differs:\n  ours:   %s\n  S10b:   %s", c.Regime, c.Option, c.Key, c.Seed, ours, theirs))
		}
	}
	return compared, problems
}
