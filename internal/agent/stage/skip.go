package stage

import (
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The rule, with its sources in vLLM v0.32.0, is in the package comment.

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
