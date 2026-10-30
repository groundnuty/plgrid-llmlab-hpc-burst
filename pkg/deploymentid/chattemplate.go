package deploymentid

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"unicode/utf8"
)

// ChatTemplateSHA256 returns the SHA-256, in lowercase hex, of a chat
// template's UTF-8 text, as ServingSpec.ChatTemplateSHA256 holds it. The text
// is hashed as given: no newline is trimmed and nothing is normalised.
func ChatTemplateSHA256(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

// TemplateSources holds the places HBEP-3 takes a chat template from. A nil
// field means the source is absent; a non-nil empty one is present.
type TemplateSources struct {
	// Spec is the serving spec's own chat template.
	Spec *string
	// Jinja is the content of the checkpoint's chat_template.jinja.
	Jinja *string
	// TokenizerConfig is the content of the checkpoint's tokenizer_config.json.
	TokenizerConfig []byte
}

// ResolveChatTemplate returns the chat template text that scheme v1 hashes
// (HBEP-3, Deployment ID), taking the first of these that exists:
//  1. the serving spec's template;
//  2. the checkpoint's chat_template.jinja;
//  3. the chat_template of its tokenizer_config.json, which is a string or a
//     list of {name, template} entries, of which the one named "default" counts;
//  4. the empty string.
//
// A tokenizer_config.json that is not a JSON object, or whose chat_template is
// neither a string nor a list of named templates, is an error rather than a
// silent empty template.
func ResolveChatTemplate(src TemplateSources) (string, error) {
	switch {
	case src.Spec != nil:
		return textOf("serving spec chat template", *src.Spec)
	case src.Jinja != nil:
		return textOf("chat_template.jinja", *src.Jinja)
	case src.TokenizerConfig != nil:
		return templateFromTokenizerConfig(src.TokenizerConfig)
	}
	return "", nil
}

func textOf(where, s string) (string, error) {
	if !utf8.ValidString(s) {
		return "", invalidf("%s is not valid UTF-8", where)
	}
	return s, nil
}

func templateFromTokenizerConfig(data []byte) (string, error) {
	if !utf8.Valid(data) {
		return "", invalidf("tokenizer_config.json is not valid UTF-8")
	}
	var cfg map[string]json.RawMessage
	if err := json.Unmarshal(data, &cfg); err != nil {
		return "", invalidf("tokenizer_config.json is not a JSON object: %v", err)
	}
	raw, ok := cfg["chat_template"]
	if !ok || string(raw) == "null" {
		return "", nil
	}
	var single string
	if json.Unmarshal(raw, &single) == nil {
		return single, nil
	}
	var named []struct {
		Name     string `json:"name"`
		Template string `json:"template"`
	}
	if err := json.Unmarshal(raw, &named); err != nil {
		return "", invalidf("tokenizer_config.json chat_template is neither a string nor a list of {name, template}; "+
			"give the template in the serving spec instead (%v)", err)
	}
	for _, n := range named {
		if n.Name == "default" {
			return n.Template, nil
		}
	}
	return "", nil
}
