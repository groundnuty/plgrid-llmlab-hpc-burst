package deploymentid

import (
	"errors"
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
	}
	for _, c := range cases {
		if got := ChatTemplateSHA256(c.text); got != c.want {
			t.Errorf("%q: got %s, want %s", c.text, got, c.want)
		}
	}
}

func strp(s string) *string { return &s }

func TestResolveChatTemplate(t *testing.T) {
	str := []byte(`{"chat_template": "single"}`)
	list := []byte(`{"chat_template": [{"name": "tool_use", "template": "tools"}, {"name": "default", "template": "default one"}]}`)
	noDefault := []byte(`{"chat_template": [{"name": "tool_use", "template": "tools"}]}`)
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
		{"config list without default", TemplateSources{TokenizerConfig: noDefault}, ""},
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
		if got := ChatTemplateSHA256(text); got != h {
			t.Errorf("%q: got %s, want %s", text, got, h)
		}
	}
}

func TestResolveChatTemplateErrors(t *testing.T) {
	bad := map[string]TemplateSources{
		"not JSON":                    {TokenizerConfig: []byte(`not json`)},
		"JSON array":                  {TokenizerConfig: []byte(`[]`)},
		"chat_template is a number":   {TokenizerConfig: []byte(`{"chat_template": 3}`)},
		"chat_template is an object":  {TokenizerConfig: []byte(`{"chat_template": {"default": "x"}}`)},
		"list entry is not an object": {TokenizerConfig: []byte(`{"chat_template": ["x"]}`)},
		"config not UTF-8":            {TokenizerConfig: []byte("{\"chat_template\": \"\xff\"}")},
		"spec not UTF-8":              {Spec: strp("\xff")},
		"jinja not UTF-8":             {Jinja: strp("\xff")},
	}
	for name, src := range bad {
		if got, err := ResolveChatTemplate(src); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: got %q, %v; want ErrInvalid", name, got, err)
		}
	}
}
