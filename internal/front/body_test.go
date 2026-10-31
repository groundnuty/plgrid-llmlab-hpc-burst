package front

import (
	"testing"
)

// The one change to a request body is the top-level model value's bytes
// (HBEP-5, Contract). Expected outputs are written by hand.

func TestRewriteChangesOnlyTheTopLevelModel(t *testing.T) {
	cases := []struct {
		name, in, served, want string
	}{
		{
			"shorter name, spacing kept",
			`{ "model" :  "m@helios-a/0123456789ab", "messages":[{"role":"user","content":"x"}], "z": 1 }`,
			"m",
			`{ "model" :  "m", "messages":[{"role":"user","content":"x"}], "z": 1 }`,
		},
		{
			"longer name",
			`{"stream":true,"model":"m","extra":{"keep":"me"}}`,
			"org/a-much-longer-catalog-name",
			`{"stream":true,"model":"org/a-much-longer-catalog-name","extra":{"keep":"me"}}`,
		},
		{
			"a model nested in tools and metadata stays",
			`{"tools":[{"type":"function","function":{"name":"f","model":"inner"}}],"metadata":{"model":"meta"},"model":"m@p"}`,
			"m",
			`{"tools":[{"type":"function","function":{"name":"f","model":"inner"}}],"metadata":{"model":"meta"},"model":"m"}`,
		},
		{
			"an escaped member name is the model member",
			`{"model":"m@p","n":2}`,
			"m",
			`{"model":"m","n":2}`,
		},
		{
			"characters JSON must escape, and none it must not",
			`{"model":"x"}`,
			`a"b<c>&`,
			`{"model":"a\"b<c>&"}`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rb, err := parseBody([]byte(c.in))
			if err != nil {
				t.Fatalf("parseBody: %v", err)
			}
			if got := string(rb.rewrite([]byte(c.in), c.served)); got != c.want {
				t.Errorf("rewrite:\n got %s\nwant %s", got, c.want)
			}
		})
	}
}

func TestParseBodyRefusals(t *testing.T) {
	for name, c := range map[string]struct {
		in   string
		want error
	}{
		"duplicate top-level model":       {`{"model":"a","x":1,"model":"b"}`, errDuplicateModel},
		"duplicate through an escape":     {`{"model":"a","model":"b"}`, errDuplicateModel},
		"no model":                        {`{"messages":[]}`, errNoModel},
		"model not a string":              {`{"model":42}`, errNoModel},
		"model null":                      {`{"model":null}`, errNoModel},
		"not an object":                   {`["model"]`, errNotObject},
		"not JSON":                        {`model=m`, errNotObject},
		"trailing data":                   {`{"model":"m"} {}`, errNotObject},
		"truncated":                       {`{"model":"m",`, errNotObject},
		"nested model only does not count": {`{"metadata":{"model":"m"}}`, errNoModel},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseBody([]byte(c.in)); err != c.want {
				t.Errorf("parseBody(%s) = %v, want %v", c.in, err, c.want)
			}
		})
	}
}

func TestParseBodyReadsStreamUsageAndMessages(t *testing.T) {
	rb, err := parseBody([]byte(`{"model":"m","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if !rb.stream || !rb.includeUsage || string(rb.messages) != `[{"role":"user","content":"hi"}]` || rb.model != "m" {
		t.Errorf("got %+v", rb)
	}
}
