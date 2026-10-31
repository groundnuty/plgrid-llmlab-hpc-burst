package stage

import (
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Which listed files are staged.
//
// The identity record lists every file of the revision (HBEP-3), but the
// engine does not load them all, and a listed file that was never staged for
// that reason is not a failure (HBEP-7, Instance start). What the engine loads
// is decided by its --load-format, so the rule is read from vLLM v0.32.0's
// default loader and applied only to that format. Paths below are in the vLLM
// repository at tag v0.32.0.
//
//   - Weights are found by a glob of the checkpoint directory's top level, one
//     pattern after another; the first pattern with a hit wins and the files of
//     the others are not loaded (model_executor/model_loader/default_loader.py
//     lines 228-234). Nothing in a subdirectory is globbed, so a nested
//     2_Dense/model.safetensors does not make a top-level pytorch_model.bin
//     unloaded, and original/ is never loaded from a local directory. The
//     "original/**/*" ignore pattern (config/load.py line 102) is used only for
//     a Hub download (default_loader.py lines 213-221), which a staged
//     checkpoint is not. Only top-level files are therefore classified here;
//     original/ is skipped, and no other subdirectory is.
//   - auto (config/load.py line 30, "" when the launch names none): when a
//     top-level consolidated*.safetensors file is listed, vLLM switches to its
//     mistral format and globs only consolidated*.safetensors
//     (default_loader.py lines 170-182, 194-196), so the sharded safetensors
//     files and the other weight formats are not loaded. Otherwise the patterns
//     are *.safetensors, then *.bin, then *.pt (lines 185-186, 207-208).
//   - safetensors: only *.safetensors (lines 187-193).
//   - pt: the code globs *.pt only (lines 198-199, 207-208), while the
//     docstring says the "pytorch bin format" (config/load.py line 36). Both
//     .bin and .pt are staged, which is safe whichever is right.
//   - Any other format (gguf, mistral, bitsandbytes, sharded_state, tensorizer,
//     runai_streamer, dummy, npcache ...): nothing is skipped.
//   - A weight extension no pattern globs (.pth .ckpt .h5 .msgpack .onnx) is
//     never loaded by the default loader under auto, safetensors or pt.
//   - Files that are not weights (config, tokenizer, templates) are never
//     skipped.
//
// What the listing cannot show: a model class may replace the patterns
// (allow_patterns_overrides, default_loader.py lines 210-211, 377), and the
// safetensors index file may name a subset of the shards (lines 250-254,
// weight_utils.py lines 585-611). Staging more than the engine loads is safe,
// and staging less is not, so neither is mirrored.

// weightKind classifies a top-level listed file by the vLLM glob that matches
// it.
type weightKind int

const (
	notWeight weightKind = iota
	kindConsolidated
	kindSafetensors
	kindBin
	kindPt
	kindUnloaded
)

// unloadedExts are weight extensions that no pattern of the default loader
// matches.
var unloadedExts = []string{".pth", ".ckpt", ".h5", ".msgpack", ".onnx"}

// kindOf classifies one listed path; only a top-level file is a weight.
func kindOf(p string) weightKind {
	if strings.Contains(p, "/") {
		return notWeight
	}
	switch {
	case strings.HasPrefix(p, "consolidated") && strings.HasSuffix(p, ".safetensors"):
		return kindConsolidated
	case strings.HasSuffix(p, ".safetensors"):
		return kindSafetensors
	case strings.HasSuffix(p, ".bin"):
		return kindBin
	case strings.HasSuffix(p, ".pt"):
		return kindPt
	}
	for _, ext := range unloadedExts {
		if strings.HasSuffix(p, ext) {
			return kindUnloaded
		}
	}
	return notWeight
}

// skippedFiles marks the listed files the engine does not load when it is
// launched with loadFormat, so staging does not download them.
func skippedFiles(files []agentapi.FileDigest, loadFormat string) []bool {
	skip := make([]bool, len(files))
	format := strings.ToLower(loadFormat)
	if format == "" {
		format = "auto"
	}
	if format != "auto" && format != "safetensors" && format != "pt" {
		return skip
	}
	var has [kindUnloaded + 1]bool
	for _, f := range files {
		has[kindOf(f.Path)] = true
	}
	loaded := func(k weightKind) bool {
		switch format {
		case "safetensors":
			return k == kindConsolidated || k == kindSafetensors
		case "pt":
			return k == kindBin || k == kindPt
		}
		// auto: the first pattern with a hit wins.
		switch {
		case has[kindConsolidated]:
			return k == kindConsolidated
		case has[kindSafetensors]:
			return k == kindSafetensors
		case has[kindBin]:
			return k == kindBin
		}
		return k == kindPt
	}
	for i, f := range files {
		if strings.HasPrefix(f.Path, "original/") {
			skip[i] = true
			continue
		}
		k := kindOf(f.Path)
		skip[i] = k != notWeight && (k == kindUnloaded || !loaded(k))
	}
	return skip
}
