package deploymentid

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

// ChatTemplateSHA256 returns the SHA-256, in lowercase hex, of a chat
// template's UTF-8 text, as ServingSpec.ChatTemplateSHA256 holds it. The text
// is hashed as given: no newline is trimmed and nothing is normalised. Text
// that is not valid UTF-8 is an ErrInvalid error, since its bytes are not a
// template any engine reads the way the hash says. No template is the empty
// string, whose hash is e3b0c442...b855: chat_template_sha256 is always a
// hash, never the empty string itself.
func ChatTemplateSHA256(text string) (string, error) {
	if !utf8.ValidString(text) {
		return "", invalidf("chat template is not valid UTF-8")
	}
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:]), nil
}

// TemplateSources holds the places HBEP-3 takes a chat template from. A nil
// field means the source is absent; a non-nil empty one is present.
type TemplateSources struct {
	// Spec is the serving spec's own chat template. A caller maps an unset
	// optional field of the spec to nil, never to a pointer to "": an empty
	// Spec is a template the spec gave, and it disables the fallbacks below.
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
//  4. the empty string. A deployment with no template at all hashes empty
//     content: ChatTemplateSHA256("") is its chat_template_sha256.
//
// Nothing is repaired or guessed. These are errors rather than a silent empty
// or altered template:
//   - text that is not valid UTF-8, in any source;
//   - a tokenizer_config.json that is not a JSON object (top-level null
//     included), or whose chat_template is neither a string nor a list of
//     named templates;
//   - a list-form chat_template without an entry named "default", or with two,
//     or with an entry that lacks a string "name" or "template" (keys are
//     matched exactly, as transformers reads them): the engine would fail to
//     render or pick a template the ID never saw, so the serving spec has to
//     name the template;
//   - an unpaired surrogate escape such as "\ud800" in the chat_template, which
//     Go's JSON decoder would replace with U+FFFD, so the hash would not be of
//     the text the checkpoint holds. Paired escapes and a literal U+FFFD are
//     kept as they are.
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
	if cfg == nil {
		return "", invalidf("tokenizer_config.json is null, not a JSON object")
	}
	raw, ok := cfg["chat_template"]
	if !ok || string(raw) == "null" {
		return "", nil
	}
	if err := checkSurrogateEscapes(raw); err != nil {
		return "", err
	}
	var single string
	if json.Unmarshal(raw, &single) == nil {
		return single, nil
	}
	// Entries are read by their exact keys, as transformers reads them
	// (t["name"], t["template"]). Decoding into a struct would match "Template"
	// or "NAME" too, and a second spelling of a key could change the text.
	var entries []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &entries); err != nil {
		return "", invalidf("tokenizer_config.json chat_template is neither a string nor a list of {name, template}; "+
			"give the template in the serving spec instead (%v)", err)
	}
	found, template := false, ""
	for i, e := range entries {
		name, err := entryString(e, "name")
		if err != nil {
			return "", invalidf("tokenizer_config.json chat_template entry %d: %v", i, err)
		}
		text, err := entryString(e, "template")
		if err != nil {
			return "", invalidf("tokenizer_config.json chat_template entry %d: %v", i, err)
		}
		if name != "default" {
			continue
		}
		if found {
			return "", invalidf("tokenizer_config.json chat_template has two templates named \"default\"; " +
				"name the template in the serving spec instead")
		}
		found, template = true, text
	}
	if !found {
		return "", invalidf("tokenizer_config.json chat_template is a list without a template named \"default\"; " +
			"name the template in the serving spec instead")
	}
	return template, nil
}

// entryString returns the string value of the exact key of a list entry.
func entryString(entry map[string]json.RawMessage, key string) (string, error) {
	raw, ok := entry[key]
	if !ok {
		return "", fmt.Errorf("has no %q key", key)
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", fmt.Errorf("%q is not a string", key)
	}
	return s, nil
}

// checkSurrogateEscapes scans JSON text that json.Unmarshal has already
// accepted and refuses a \uD800-\uDFFF escape that is not half of a pair: a
// high surrogate that no low surrogate escape follows, or a low one with no
// high one before it. encoding/json turns such an escape into U+FFFD without
// an error (RFC 8259 allows the escape, RFC 8785 section 3.2.2.2 leaves its
// meaning to the parser). The scan tracks string state, so a backslash that
// is itself escaped (\\ud800) is text, not an escape.
func checkSurrogateEscapes(raw []byte) error {
	inString := false
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		if !inString {
			inString = c == '"'
			continue
		}
		switch {
		case c == '"':
			inString = false
		case c == '\\' && i+1 < len(raw) && raw[i+1] != 'u':
			i++ // a two-character escape; the next byte is not a delimiter
		case c == '\\':
			u, ok := escapedUnit(raw, i)
			switch {
			case !ok:
				return invalidf("tokenizer_config.json chat_template has a malformed \\u escape")
			case u >= 0xDC00 && u <= 0xDFFF:
				return invalidf("tokenizer_config.json chat_template has an unpaired low surrogate escape; " +
					"write the character itself or a surrogate pair")
			case u >= 0xD800 && u <= 0xDBFF:
				lo, ok := escapedUnit(raw, i+6)
				if !ok || lo < 0xDC00 || lo > 0xDFFF {
					return invalidf("tokenizer_config.json chat_template has an unpaired high surrogate escape; " +
						"write the character itself or a surrogate pair")
				}
				i += 6 // the pair's second escape; the loop steps past its last digit
			}
			i += 5 // the escape's \u and four digits
		}
	}
	return nil
}

// escapedUnit reads the \uXXXX escape that starts at raw[i], hex digits of
// either case. ok is false when raw[i:] is not such an escape.
func escapedUnit(raw []byte, i int) (u rune, ok bool) {
	if i+6 > len(raw) || raw[i] != '\\' || raw[i+1] != 'u' {
		return 0, false
	}
	for _, d := range raw[i+2 : i+6] {
		switch {
		case d >= '0' && d <= '9':
			u = u<<4 | rune(d-'0')
		case d >= 'a' && d <= 'f':
			u = u<<4 | rune(d-'a'+10)
		case d >= 'A' && d <= 'F':
			u = u<<4 | rune(d-'A'+10)
		default:
			return 0, false
		}
	}
	return u, true
}
