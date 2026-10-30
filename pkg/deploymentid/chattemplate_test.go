package deploymentid

import (
	"errors"
	"strings"
	"testing"
)

// Expected hashes were computed with coreutils, for example
// printf '{{ messages }}' | sha256sum, outside this package.
func TestChatTemplateSHA256(t *testing.T) {
	cases := []struct{ text, want string }{
		{"", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		{"{{ messages }}", "f24189f08c85a1eb19a737306c3a13e85462ee33d964f28108d64a2485fa2171"},
		{"{{ messages }}\n", "7f3de14b4a359865a98f84be7a9314754b7e14e8e23524e2553e4effaa9d18e1"},
		{"\xe2\x86\x92 {% if x %}\n", "0057e3951854c71880e33ee7381fc0cc4ef283fe7797da1891c7b3f1d2f3884c"},
		// A literal U+FFFD is valid text and is hashed as it is.
		{"\xef\xbf\xbd", "83d544ccc223c057d2bf80d3f2a32982c32c3c0db8e2674820da5064783fb097"},
		{"\xf0\x9f\x98\x80", "f0443a342c5ef54783a111b51ba56c938e474c32324d90c3a60c9c8e3a37e2d9"},
	}
	for _, c := range cases {
		if got, err := ChatTemplateSHA256(c.text); err != nil || got != c.want {
			t.Errorf("%q: got %s, %v; want %s", c.text, got, err, c.want)
		}
	}
	for _, bad := range []string{"\xff", "ok\xc3", "\xed\xa0\x80" /* an encoded surrogate */} {
		if got, err := ChatTemplateSHA256(bad); !errors.Is(err, ErrInvalid) || got != "" {
			t.Errorf("%q: got %q, %v; want \"\" and ErrInvalid", bad, got, err)
		}
	}
}

// mustHash is ChatTemplateSHA256 for text the test knows is valid.
func mustHash(t *testing.T, text string) string {
	t.Helper()
	h, err := ChatTemplateSHA256(text)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func strp(s string) *string { return &s }

func TestResolveChatTemplate(t *testing.T) {
	str := []byte(`{"chat_template": "single"}`)
	list := []byte(`{"chat_template": [{"name": "tool_use", "template": "tools"}, {"name": "default", "template": "default one"}]}`)
	cases := []struct {
		name string
		src  TemplateSources
		want string
	}{
		{"nothing at all", TemplateSources{}, ""},
		{"spec only", TemplateSources{Spec: strp("spec template")}, "spec template"},
		{"spec beats jinja and config", TemplateSources{Spec: strp("spec template"), Jinja: strp("jinja template\n"), TokenizerConfig: str}, "spec template"},
		{"an empty spec template is still the spec's", TemplateSources{Spec: strp(""), Jinja: strp("jinja template\n"), TokenizerConfig: str}, ""},
		{"jinja beats config", TemplateSources{Jinja: strp("jinja template\n"), TokenizerConfig: str}, "jinja template\n"},
		{"config string", TemplateSources{TokenizerConfig: str}, "single"},
		{"config list takes default", TemplateSources{TokenizerConfig: list}, "default one"},
		// Escaped surrogates that pair are kept, in either case of hex digit, in
		// both forms; a literal U+FFFD stays; an escaped backslash makes text.
		{"config string with a surrogate pair", TemplateSources{TokenizerConfig: []byte(`{"chat_template": "a\ud83d\ude00b"}`)}, "a\U0001F600b"},
		{"config string with a pair in upper case", TemplateSources{TokenizerConfig: []byte(`{"chat_template": "\uD83D\uDE00"}`)}, "\U0001F600"},
		{"config string with a mixed-case pair", TemplateSources{TokenizerConfig: []byte(`{"chat_template": "\uD83d\uDe00"}`)}, "\U0001F600"},
		{"config list with a pair", TemplateSources{TokenizerConfig: []byte(`{"chat_template": [{"name": "default", "template": "\ud83d\ude00"}]}`)}, "\U0001F600"},
		{"config with an escaped U+FFFD", TemplateSources{TokenizerConfig: []byte(`{"chat_template": "x\ufffdy"}`)}, "x\uFFFDy"},
		{"config with a literal U+FFFD", TemplateSources{TokenizerConfig: []byte("{\"chat_template\": \"x\xef\xbf\xbdy\"}")}, "x\uFFFDy"},
		{"config with an escaped backslash before a surrogate-like text", TemplateSources{TokenizerConfig: []byte(`{"chat_template": "\\ud800"}`)}, `\ud800`},
		{"config with an escaped backslash before a pair", TemplateSources{TokenizerConfig: []byte(`{"chat_template": "\\\ud83d\ude00"}`)}, "\\\U0001F600"},
		{"a lone surrogate outside chat_template is not looked at", TemplateSources{TokenizerConfig: []byte(`{"bos_token": "\ud800", "chat_template": "ok"}`)}, "ok"},
		// Keys are matched exactly, as transformers reads t["template"]: another
		// spelling of a key is not the template (Go's struct decoding would
		// have picked "Template" over "template" here).
		{"a second spelling of template is ignored", TemplateSources{TokenizerConfig: []byte(`{"chat_template": [{"name": "default", "template": "a", "Template": "b"}]}`)}, "a"},
		{"a second spelling of template, other order", TemplateSources{TokenizerConfig: []byte(`{"chat_template": [{"name": "default", "Template": "b", "template": "a"}]}`)}, "a"},
		{"a second spelling of name is ignored", TemplateSources{TokenizerConfig: []byte(`{"chat_template": [{"name": "other", "NAME": "default", "template": "a"}, {"name": "default", "template": "d"}]}`)}, "d"},
		{"a second spelling of chat_template is ignored", TemplateSources{TokenizerConfig: []byte(`{"Chat_Template": "x", "chat_template": "y"}`)}, "y"},
		{"config list takes the default after other entries", TemplateSources{TokenizerConfig: []byte(`{"chat_template": [{"name": "default", "template": "d"}, {"name": "rag", "template": "r"}]}`)}, "d"},
		{"config without chat_template", TemplateSources{TokenizerConfig: []byte(`{"model_max_length": 1}`)}, ""},
		{"config with null chat_template", TemplateSources{TokenizerConfig: []byte(`{"chat_template": null}`)}, ""},
		{"empty config object", TemplateSources{TokenizerConfig: []byte(`{}`)}, ""},
	}
	for _, c := range cases {
		got, err := ResolveChatTemplate(c.src)
		if err != nil || got != c.want {
			t.Errorf("%s: got %q, %v; want %q", c.name, got, err, c.want)
		}
	}
	// The hashes in the end-to-end form, from coreutils.
	want := map[string]string{
		"spec template":    "027d2e8450a8cc1e09f8d97a7924d6cb1928690e784893e4e692ca3ac005a193",
		"jinja template\n": "721d58880699e4eefde56861a8bcf714e6083dab4527867a7c65be0e21013caf",
		"single":           "947f187506f7629c81c81879a2cb2256455038e4ac770091d897fa0a8b945e3b",
		"default one":      "650c488ce1703c61ae71c4fb6285b05149eb5ff8b4ec5a10fe51f36a6464a3e1",
	}
	for text, h := range want {
		if got := mustHash(t, text); got != h {
			t.Errorf("%q: got %s, want %s", text, got, h)
		}
	}
}

func TestResolveChatTemplateErrors(t *testing.T) {
	bad := map[string]TemplateSources{
		"not JSON":                    {TokenizerConfig: []byte(`not json`)},
		"JSON array":                  {TokenizerConfig: []byte(`[]`)},
		"top-level null":              {TokenizerConfig: []byte(`null`)},
		"top-level string":            {TokenizerConfig: []byte(`"x"`)},
		"chat_template is a number":   {TokenizerConfig: []byte(`{"chat_template": 3}`)},
		"chat_template is an object":  {TokenizerConfig: []byte(`{"chat_template": {"default": "x"}}`)},
		"list entry is not an object": {TokenizerConfig: []byte(`{"chat_template": ["x"]}`)},
		"config not UTF-8":            {TokenizerConfig: []byte("{\"chat_template\": \"\xff\"}")},
		"spec not UTF-8":              {Spec: strp("\xff")},
		"jinja not UTF-8":             {Jinja: strp("\xff")},
		// A list without "default" is not the empty template (owner decision 3).
		"list without default":                    {TokenizerConfig: []byte(`{"chat_template": [{"name": "tool_use", "template": "tools"}]}`)},
		"empty list":                              {TokenizerConfig: []byte(`{"chat_template": []}`)},
		"list with two defaults":                  {TokenizerConfig: []byte(`{"chat_template": [{"name": "default", "template": "a"}, {"name": "default", "template": "b"}]}`)},
		"entry with a differently cased name":     {TokenizerConfig: []byte(`{"chat_template": [{"Name": "default", "template": "a"}]}`)},
		"entry with a differently cased template": {TokenizerConfig: []byte(`{"chat_template": [{"name": "default", "Template": "a"}]}`)},
		"entry without a template":                {TokenizerConfig: []byte(`{"chat_template": [{"name": "default"}]}`)},
		"other entry without a template":          {TokenizerConfig: []byte(`{"chat_template": [{"name": "default", "template": "a"}, {"name": "rag"}]}`)},
		"entry with a numeric template":           {TokenizerConfig: []byte(`{"chat_template": [{"name": "default", "template": 3}]}`)},
		"entry with a numeric name":               {TokenizerConfig: []byte(`{"chat_template": [{"name": 3, "template": "a"}]}`)},
		"null entry":                              {TokenizerConfig: []byte(`{"chat_template": [null]}`)},
		"chat_template only in another case":      {TokenizerConfig: []byte(`{"chat_template": 7, "Chat_Template": "x"}`)},
		"list with a nameless entry only":         {TokenizerConfig: []byte(`{"chat_template": [{"template": "a"}]}`)},
		// Astra's counterexample: encoding/json would return U+FFFD with no error.
		"lone high surrogate":                  {TokenizerConfig: []byte(`{"chat_template":"\ud800"}`)},
		"lone high surrogate in upper":         {TokenizerConfig: []byte(`{"chat_template":"\uDBFF"}`)},
		"lone low surrogate":                   {TokenizerConfig: []byte(`{"chat_template":"\udc00"}`)},
		"low surrogate after a char":           {TokenizerConfig: []byte(`{"chat_template":"a\udfffb"}`)},
		"high surrogate before text":           {TokenizerConfig: []byte(`{"chat_template":"\ud800abc"}`)},
		"high surrogate before a BMP escape":   {TokenizerConfig: []byte(`{"chat_template":"\ud800\u0041"}`)},
		"two high surrogates":                  {TokenizerConfig: []byte(`{"chat_template":"\ud800\ud800\udc00"}`)},
		"reversed pair":                        {TokenizerConfig: []byte(`{"chat_template":"\ude00\ud83d"}`)},
		"high surrogate at the end":            {TokenizerConfig: []byte(`{"chat_template":"ok\ud83d"}`)},
		"lone surrogate after a pair":          {TokenizerConfig: []byte(`{"chat_template":"\ud83d\ude00\ud83d"}`)},
		"lone surrogate in the list form":      {TokenizerConfig: []byte(`{"chat_template": [{"name": "default", "template": "x\ud800"}]}`)},
		"lone surrogate in a list name":        {TokenizerConfig: []byte(`{"chat_template": [{"name": "default", "template": "x"}, {"name": "\udc00", "template": "y"}]}`)},
		"surrogate after an escaped backslash": {TokenizerConfig: []byte(`{"chat_template":"\\\ud800"}`)},
	}
	for name, src := range bad {
		got, err := ResolveChatTemplate(src)
		if !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: got %q, %v; want ErrInvalid", name, got, err)
		}
		if got != "" {
			t.Errorf("%s: returned the text %q with its error", name, got)
		}
	}
}

// The error for a list without "default" tells the operator what to do.
func TestListWithoutDefaultNamesTheFix(t *testing.T) {
	_, err := ResolveChatTemplate(TemplateSources{TokenizerConfig: []byte(`{"chat_template": [{"name": "x", "template": "y"}]}`)})
	if err == nil || !strings.Contains(err.Error(), "serving spec") || !strings.Contains(err.Error(), `"default"`) {
		t.Errorf("got %v, want an error that names the default entry and says to give the template in the serving spec", err)
	}
}

// A spec, jinja file or tokenizer config that gives a template always wins over
// a later source's problems: the first source that exists is the only one read.
func TestLaterSourcesAreNotReadWhenAnEarlierOneExists(t *testing.T) {
	broken := []byte(`{"chat_template":"\ud800"}`)
	for name, src := range map[string]TemplateSources{
		"spec":  {Spec: strp("s"), Jinja: strp("\xff"), TokenizerConfig: broken},
		"jinja": {Jinja: strp("j"), TokenizerConfig: broken},
	} {
		if _, err := ResolveChatTemplate(src); err != nil {
			t.Errorf("%s: a source that is never read caused %v", name, err)
		}
	}
}

// HBEP-3's lookup chain: the serving spec, chat_template.jinja, the legacy
// chat_template.json, the chat_template of tokenizer_config.json, then empty
// content. Each source beats every later one.
func TestChatTemplateChainPrecedence(t *testing.T) {
	jsonFile := []byte(`{"chat_template": "from chat_template.json"}`)
	cfg := []byte(`{"chat_template": "from tokenizer_config"}`)
	cases := []struct {
		name string
		src  TemplateSources
		want string
	}{
		{"spec beats all", TemplateSources{Spec: strp("spec"), Jinja: strp("jinja"), ChatTemplateJSON: jsonFile, TokenizerConfig: cfg}, "spec"},
		{"jinja beats the json file and the config", TemplateSources{Jinja: strp("jinja"), ChatTemplateJSON: jsonFile, TokenizerConfig: cfg}, "jinja"},
		{"an empty jinja is still jinja", TemplateSources{Jinja: strp(""), ChatTemplateJSON: jsonFile, TokenizerConfig: cfg}, ""},
		{"the json file beats the config", TemplateSources{ChatTemplateJSON: jsonFile, TokenizerConfig: cfg}, "from chat_template.json"},
		{"the json file alone", TemplateSources{ChatTemplateJSON: jsonFile}, "from chat_template.json"},
		{"an empty template in the json file is still the file's", TemplateSources{ChatTemplateJSON: []byte(`{"chat_template": ""}`), TokenizerConfig: cfg}, ""},
		{"the config when there is no json file", TemplateSources{TokenizerConfig: cfg}, "from tokenizer_config"},
		{"the json file in list form takes default", TemplateSources{ChatTemplateJSON: []byte(`{"chat_template": [{"name": "rag", "template": "r"}, {"name": "default", "template": "d"}]}`), TokenizerConfig: cfg}, "d"},
		{"the json file with a surrogate pair", TemplateSources{ChatTemplateJSON: []byte(`{"chat_template": "\ud83d\ude00"}`)}, "\U0001F600"},
		{"empty content when the chain gives nothing", TemplateSources{}, ""},
	}
	for _, c := range cases {
		if got, err := ResolveChatTemplate(c.src); err != nil || got != c.want {
			t.Errorf("%s: got %q, %v; want %q", c.name, got, err, c.want)
		}
	}
}

// An empty chain is no error, and its hash is the SHA-256 of zero bytes
// (HBEP-3): a caller must not fail only because no source gave a template.
func TestEmptyChainIsNotAnErrorAndHashesZeroBytes(t *testing.T) {
	text, err := ResolveChatTemplate(TemplateSources{})
	if err != nil || text != "" {
		t.Fatalf("got %q, %v; want empty and no error", text, err)
	}
	h, err := ChatTemplateSHA256(text)
	if err != nil || h != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
		t.Errorf("got %s, %v; want the SHA-256 of zero bytes (coreutils: printf '' | sha256sum)", h, err)
	}
	// And a tokenizer_config.json that holds no template at all is the same.
	if text, err := ResolveChatTemplate(TemplateSources{TokenizerConfig: []byte(`{"model_max_length": 8}`)}); err != nil || text != "" {
		t.Errorf("a config without chat_template: got %q, %v", text, err)
	}
}

// chat_template.json exists to hold the template, and transformers reads
// json["chat_template"] without a default, so a file without it is an error,
// as the same faults are in tokenizer_config.json, and the message names the file.
func TestChatTemplateJSONErrors(t *testing.T) {
	bad := map[string][]byte{
		"not JSON":                  []byte(`not json`),
		"top-level null":            []byte(`null`),
		"an array":                  []byte(`[]`),
		"no chat_template member":   []byte(`{}`),
		"another member only":       []byte(`{"template": "x"}`),
		"a null chat_template":      []byte(`{"chat_template": null}`),
		"chat_template in a case":   []byte(`{"Chat_Template": "x"}`),
		"a number":                  []byte(`{"chat_template": 3}`),
		"not UTF-8":                 []byte("{\"chat_template\": \"\xff\"}"),
		"a lone high surrogate":     []byte(`{"chat_template": "\ud800"}`),
		"a lone low surrogate":      []byte(`{"chat_template": "x\udc00"}`),
		"list without default":      []byte(`{"chat_template": [{"name": "rag", "template": "r"}]}`),
		"list with two defaults":    []byte(`{"chat_template": [{"name": "default", "template": "a"}, {"name": "default", "template": "b"}]}`),
		"list entry without a name": []byte(`{"chat_template": [{"template": "a"}]}`),
	}
	for name, data := range bad {
		got, err := ResolveChatTemplate(TemplateSources{ChatTemplateJSON: data, TokenizerConfig: []byte(`{"chat_template": "fallback"}`)})
		if !errors.Is(err, ErrInvalid) || got != "" {
			t.Errorf("%s: got %q, %v; want ErrInvalid (and no fall-through to tokenizer_config.json)", name, got, err)
			continue
		}
		if !strings.Contains(err.Error(), "chat_template.json") {
			t.Errorf("%s: the error does not name the file: %v", name, err)
		}
	}
	// A later source is not read when an earlier one exists.
	if got, err := ResolveChatTemplate(TemplateSources{Jinja: strp("j"), ChatTemplateJSON: []byte(`not json`)}); err != nil || got != "j" {
		t.Errorf("got %q, %v; want the jinja file, the json file never read", got, err)
	}
	// And the errors of tokenizer_config.json still name that file.
	if _, err := ResolveChatTemplate(TemplateSources{TokenizerConfig: []byte(`{"chat_template": "\ud800"}`)}); err == nil || !strings.Contains(err.Error(), "tokenizer_config.json") {
		t.Errorf("got %v, want an error naming tokenizer_config.json", err)
	}
}
