package stage

import (
	"context"
	"slices"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// stageMixed stages the listed files, every one of them served by the Hub with
// the same content, and returns the result and the paths the Hub was asked for.
func stageMixed(t *testing.T, loadFormat string, paths ...string) (map[string]agentapi.FileResult, []string) {
	t.Helper()
	served := map[string]string{}
	var files []agentapi.FileDigest
	for _, p := range paths {
		served[resolvePath(p)] = "hello weights\n"
		files = append(files, agentapi.FileDigest{Path: p, SHA256: weightsSum})
	}
	s, hub, _ := newStager(t, served)
	res, err := s.Checkpoint(context.Background(), agent.CheckpointRequest{
		Repository: "org/model", Revision: rev, Files: files, LoadFormat: loadFormat,
	})
	if err != nil {
		t.Fatal(err)
	}
	gets, _ := hub.requests()
	return outcomes(res), gets
}

// A repository with both formats, launched with --load-format=pt, stages its
// .bin weights and not its safetensors (Astra B5: skipping the .bin files made
// this valid deployment unable to start).
func TestPtLoadFormatStagesBinWeights(t *testing.T) {
	o, gets := stageMixed(t, "pt", "config.json", "model.safetensors", "pytorch_model.bin")
	if o["pytorch_model.bin"].Outcome != agentapi.FileMatch {
		t.Errorf("pytorch_model.bin: %+v", o["pytorch_model.bin"])
	}
	if o["model.safetensors"].Outcome != agentapi.FileAbsent {
		t.Errorf("model.safetensors: %+v", o["model.safetensors"])
	}
	want := []string{resolvePath("config.json"), resolvePath("pytorch_model.bin")}
	if !slices.Equal(gets, want) {
		t.Errorf("downloads %q, want %q", gets, want)
	}
}

// A safetensors file in a subdirectory does not make a top-level .bin file
// unloaded: vLLM globs the top level only (Opus CE-G, default_loader.py
// lines 228-230).
func TestNestedSafetensorsDoNotHideTopLevelBin(t *testing.T) {
	o, gets := stageMixed(t, "", "pytorch_model.bin", "2_Dense/model.safetensors")
	for _, p := range []string{"pytorch_model.bin", "2_Dense/model.safetensors"} {
		if o[p].Outcome != agentapi.FileMatch {
			t.Errorf("%s: %+v", p, o[p])
		}
	}
	if len(gets) != 2 {
		t.Errorf("downloads %q", gets)
	}
}

// A format the stager has no rule for skips nothing, original/ included.
func TestUnknownLoadFormatSkipsNothing(t *testing.T) {
	paths := []string{"model.safetensors", "pytorch_model.bin", "original/consolidated.00.pth", "model.gguf"}
	o, gets := stageMixed(t, "gguf", paths...)
	for _, p := range paths {
		if o[p].Outcome != agentapi.FileMatch {
			t.Errorf("%s: %+v", p, o[p])
		}
	}
	if len(gets) != len(paths) {
		t.Errorf("downloads %q", gets)
	}
}

func TestSkippedFilesByLoadFormat(t *testing.T) {
	for _, tc := range []struct {
		name   string
		format string
		files  []string
		skip   []string // the listed files that are skipped
	}{
		{"auto prefers safetensors", "auto",
			[]string{"model.safetensors", "pytorch_model.bin", "x.pt", "tf_model.h5", "config.json"},
			[]string{"pytorch_model.bin", "x.pt", "tf_model.h5"}},
		{"empty means auto", "",
			[]string{"model.safetensors", "pytorch_model.bin"},
			[]string{"pytorch_model.bin"}},
		{"auto without safetensors loads bin, not pt", "auto",
			[]string{"pytorch_model.bin", "quant.pt", "flax_model.msgpack"},
			[]string{"quant.pt", "flax_model.msgpack"}},
		{"auto falls back to pt", "auto",
			[]string{"quant.pt", "config.json"},
			nil},
		{"auto with a top-level consolidated file loads only it (mistral)", "auto",
			[]string{"consolidated.safetensors", "model-00001-of-00002.safetensors", "pytorch_model.bin", "params.json"},
			[]string{"model-00001-of-00002.safetensors", "pytorch_model.bin"}},
		{"a nested consolidated file does not switch auto to mistral", "auto",
			[]string{"model-00001-of-00002.safetensors", "sub/consolidated.safetensors", "pytorch_model.bin"},
			[]string{"pytorch_model.bin"}},
		{"safetensors stages consolidated and shards", "safetensors",
			[]string{"consolidated.safetensors", "model-00001-of-00002.safetensors", "pytorch_model.bin", "x.pt"},
			[]string{"pytorch_model.bin", "x.pt"}},
		{"pt stages bin and pt", "pt",
			[]string{"model.safetensors", "pytorch_model.bin", "x.pt", "model.onnx"},
			[]string{"model.safetensors", "model.onnx"}},
		{"the format is case-insensitive", "PT",
			[]string{"model.safetensors", "pytorch_model.bin"},
			[]string{"model.safetensors"}},
		{"original/ is skipped under auto", "auto",
			[]string{"model.safetensors", "original/params.json", "original/consolidated.00.pth"},
			[]string{"original/params.json", "original/consolidated.00.pth"}},
		{"original/ is skipped under safetensors", "safetensors",
			[]string{"model.safetensors", "original/tokenizer.model"},
			[]string{"original/tokenizer.model"}},
		{"another subdirectory is not skipped", "auto",
			[]string{"model.safetensors", "2_Dense/pytorch_model.bin", "tok/tokenizer.json"},
			nil},
		{"mistral skips nothing", "mistral",
			[]string{"consolidated.safetensors", "model.safetensors", "pytorch_model.bin", "original/params.json"},
			nil},
		{"npcache skips nothing", "npcache",
			[]string{"model.safetensors", "pytorch_model.bin"},
			nil},
		{"dummy skips nothing", "dummy",
			[]string{"model.safetensors", "pytorch_model.bin", "model.onnx"},
			nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var files []agentapi.FileDigest
			for _, p := range tc.files {
				files = append(files, agentapi.FileDigest{Path: p, SHA256: weightsSum})
			}
			var got []string
			for i, sk := range skippedFiles(files, tc.format) {
				if sk {
					got = append(got, tc.files[i])
				}
			}
			if !slices.Equal(got, tc.skip) {
				t.Errorf("skipped %q, want %q", got, tc.skip)
			}
		})
	}
}
