package alpha

// Point 6 of PR 809's review, shared by Row 1 and its Athena twin: a failed LLMLab P0 reports that no job was submitted, in the marker and the text to post (and the
// script's line, hack/livechecks). The dry faults are in dry_baseline_test.go; runOfRow is baseline_review809_test.go's.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Point 6: a failed LLMLab P0 is INVALID before any GPU submission, and the marker, the post (and the console, hack/livechecks) say that no job was submitted: they
// never say a job started, ran and spent the slot. The control, an INVALID run that did run, keeps its words.
func TestAFailedP0ReportsNoSubmissionAndARunThatRanKeepsItsWords(t *testing.T) {
	for _, row := range []Row{BaselineRow(), AthenaBaselineRow()} {
		for _, fault := range []string{"p0-llmlab-fails", "min-tokens-ignored", "p0-empty-200", "p0-cut-200"} {
			t.Run(row.Name+"/"+fault, func(t *testing.T) {
				out, failures, err := runOfRow(t, row, fault)
				if len(failures) != 0 || !BaselineInvalidRun(row, err) || !BaselineInvalidNoJob(err) {
					t.Fatalf("failures %v, err %v: want INVALID with no job submitted", failures, err)
				}
				post, _ := os.ReadFile(filepath.Join(out, PostsFile))
				if !strings.Contains(string(post), invalidNoJobWords) || strings.Contains(string(post), "started, ran and spent the slot") {
					t.Errorf("the text to post:\n%s", post)
				}
				note := BaselineInvalidNoJobNote(row)
				if !strings.Contains(note, baselineInvalidWords) || !strings.Contains(note, baselineInvalidNoJobWords) || strings.Contains(note, "its job started and ran") {
					t.Errorf("the marker's note is %q", note)
				}
				if obsOfRecord(t, out).ServedCreated {
					t.Error("the served ModelDeployment was created although P0 failed")
				}
			})
		}
		t.Run(row.Name+"/a run that ran", func(t *testing.T) {
			out, failures, err := runOfRow(t, row, "cap-never-binds")
			if len(failures) != 0 || !BaselineInvalidRun(row, err) || BaselineInvalidNoJob(err) {
				t.Fatalf("failures %v, err %v: want INVALID that ran", failures, err)
			}
			post, _ := os.ReadFile(filepath.Join(out, PostsFile))
			if !strings.Contains(string(post), "started, ran and spent the slot") || strings.Contains(string(post), "no job was submitted") {
				t.Errorf("the text to post:\n%s", post)
			}
			if note := BaselineInvalidNote(row); !strings.Contains(note, "its job started and ran") {
				t.Errorf("the marker's note is %q", note)
			}
		})
	}
}
