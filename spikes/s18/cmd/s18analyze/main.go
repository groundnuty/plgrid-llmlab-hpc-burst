// Command s18analyze computes every cell's summary again from the records its run wrote (rig.Reanalyze),
// and rewrites each summary.json under the given directory: an analysis change is applied to the same
// measurements.
//
//	s18analyze <results dir>
package main

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s18/rig"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: s18analyze <results dir>")
		os.Exit(2)
	}
	n := 0
	err := filepath.WalkDir(os.Args[1], func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || d.Name() != "summary.json" {
			return err
		}
		s, err := rig.Reanalyze(filepath.Dir(p))
		if err != nil {
			return fmt.Errorf("%s: %w", filepath.Dir(p), err)
		}
		b, err := json.MarshalIndent(s, "", "  ")
		if err != nil {
			return err
		}
		n++
		return os.WriteFile(p, b, 0o644)
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("%d cells analysed again\n", n)
}
