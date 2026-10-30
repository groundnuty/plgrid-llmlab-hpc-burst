package deploymentid

import (
	"encoding/json"
	"fmt"
)

// Names of the vLLM flags whose values can name another checkpoint.
const (
	flagTokenizer         = "tokenizer"
	flagTokenizerRevision = "tokenizer-revision"
	flagSpeculativeConfig = "speculative-config"
)

// checkReferences refuses input values that name a checkpoint the ID does not
// pin (HBEP-3, "References to other checkpoints inside input values", alpha).
// Such a reference carries the risk of an unpinned checkpoint (L-8.3): the
// bytes behind a branch or a repository name can change under one ID. values
// maps each input flag, by its resolved name, to its values in the order
// given; repository is the spec's checkpoint repository.
//
//   - A tokenizer other than the checkpoint's needs tokenizer-revision as a
//     full commit id (40 or 64 lowercase hex characters). "Other than the
//     checkpoint's" means a value that is not the checkpoint's repository
//     string; a repeated flag counts if any value is another. Every
//     tokenizer-revision given must then be a full commit id.
//   - A speculative-config that names a draft model is refused. vLLM
//     v0.32.0 reads the draft model, EAGLE head or additional weights from
//     the "model" member of the JSON object (vllm/config/speculative.py,
//     SpeculativeConfig.model), so an object with a "model" member other than
//     null is refused; "method" alone (ngram, MTP) names no checkpoint. A
//     value that is not a JSON object cannot be checked and is refused too.
//
// A pinned form of both is for beta. Errors do not repeat a value.
func checkReferences(repository string, values map[string][]string) error {
	other := false
	for _, v := range values[flagTokenizer] {
		other = other || v != repository
	}
	if other {
		revs := values[flagTokenizerRevision]
		if len(revs) == 0 {
			return fmt.Errorf("%w: flag %q is not the checkpoint's repository, so %q must pin it as a full commit id",
				ErrRefused, flagTokenizer, flagTokenizerRevision)
		}
		for _, r := range revs {
			if !isLowerHex(r, 40) && !isLowerHex(r, 64) {
				return fmt.Errorf("%w: flag %q is not the checkpoint's repository, so %q must be a full commit id "+
					"(40 or 64 lowercase hex characters)", ErrRefused, flagTokenizer, flagTokenizerRevision)
			}
		}
	}
	for _, v := range values[flagSpeculativeConfig] {
		if err := checkSpeculativeConfig(v); err != nil {
			return err
		}
	}
	return nil
}

func checkSpeculativeConfig(value string) error {
	var cfg map[string]json.RawMessage
	if err := json.Unmarshal([]byte(value), &cfg); err != nil || cfg == nil {
		return invalidf("flag %q must be a JSON object, so that the library can see whether it names a draft model", flagSpeculativeConfig)
	}
	if model, ok := cfg["model"]; ok && string(model) != "null" {
		return fmt.Errorf("%w: flag %q names a draft model (its \"model\" member); a pinned form is for beta, "+
			"use \"method\" alone or drop speculative decoding", ErrRefused, flagSpeculativeConfig)
	}
	return nil
}
