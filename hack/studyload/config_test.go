package main

import (
	"strings"
	"testing"
	"time"
)

const goodWorkload = `{"name": "w", "sessions": 2, "turns": 2, "contextTokens": 10, "turnTokens": 5, "outputTokens": 4}`

func cfgWith(extraTop, workloads string) string {
	return `{"baseURL": "http://127.0.0.1:9/v1", "model": "m", ` + extraTop + ` "workloads": [` + workloads + `]}`
}

func TestConfigDefaultsAndDurations(t *testing.T) {
	c, err := ParseConfig([]byte(cfgWith("", `{"name": "w", "sessions": 2, "turns": 2, "contextTokens": 10, "turnTokens": 5, "outputTokens": 4,
	  "thinkTime": "150ms", "startStagger": "2s", "headers": {"x-wl": "w"}}`)))
	if err != nil {
		t.Fatal(err)
	}
	w := c.Workloads[0]
	if c.SessionHeader != "x-session-id" || time.Duration(c.RequestTimeout) != 10*time.Minute ||
		time.Duration(w.ThinkTime) != 150*time.Millisecond || time.Duration(w.StartStagger) != 2*time.Second || w.Headers["x-wl"] != "w" {
		t.Errorf("config %+v: want the default session header, a 10m timeout, think 150ms, stagger 2s, header x-wl", c)
	}
}

func TestConfigRefusesWhatItCannotRun(t *testing.T) {
	bad := []struct{ name, cfg, want string }{
		{"unknown field", cfgWith(`"colour": 1,`, goodWorkload), "colour"},
		{"unknown workload field", cfgWith("", `{"name": "w", "sessions": 1, "turns": 1, "contextTokens": 1, "outputTokens": 1, "think": "1s"}`), "think"},
		{"not a url", strings.Replace(cfgWith("", goodWorkload), "http://127.0.0.1:9/v1", "127.0.0.1:9", 1), "baseURL"},
		{"url with credentials", strings.Replace(cfgWith("", goodWorkload), "http://127.0.0.1:9/v1", "http://u:p@127.0.0.1:9/v1", 1), "credentials"},
		{"no workloads", cfgWith("", ""), "no workloads"},
		{"duplicate names", cfgWith("", goodWorkload+","+goodWorkload), "used twice"},
		{"zero sessions", cfgWith("", strings.Replace(goodWorkload, `"sessions": 2`, `"sessions": 0`, 1)), "sessions"},
		{"zero output", cfgWith("", strings.Replace(goodWorkload, `"outputTokens": 4`, `"outputTokens": 0`, 1)), "outputTokens"},
		{"no turn size for a second turn", cfgWith("", strings.Replace(goodWorkload, `"turnTokens": 5`, `"turnTokens": 0`, 1)), "turnTokens"},
		{"turn size for one turn", cfgWith("", `{"name": "w", "sessions": 1, "turns": 1, "contextTokens": 1, "turnTokens": 3, "outputTokens": 1}`), "turnTokens"},
		{"negative think time", cfgWith("", strings.Replace(goodWorkload, `"outputTokens": 4`, `"outputTokens": 4, "thinkTime": "-1s"`, 1)), "negative"},
		{"duration not a string", cfgWith("", strings.Replace(goodWorkload, `"outputTokens": 4`, `"outputTokens": 4, "thinkTime": 150`, 1)), "duration"},
		{"static header is the session header", cfgWith("", strings.Replace(goodWorkload, `"outputTokens": 4`, `"outputTokens": 4, "headers": {"X-Session-Id": "a"}`, 1)), "static header"},
		{"static Authorization", cfgWith("", strings.Replace(goodWorkload, `"outputTokens": 4`, `"outputTokens": 4, "headers": {"authorization": "a"}`, 1)), "static header"},
		{"header value with a newline", cfgWith("", strings.Replace(goodWorkload, `"outputTokens": 4`, `"outputTokens": 4, "headers": {"x-a": "a\nb"}`, 1)), "control"},
		{"bad session header", cfgWith(`"sessionHeader": "bad header",`, goodWorkload), "sessionHeader"},
		{"bad run id", cfgWith(`"runId": "a b",`, goodWorkload), "runId"},
		{"two values", cfgWith("", goodWorkload) + ` {}`, "more than one"},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseConfig([]byte(tc.cfg))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %v, want one that names %q", err, tc.want)
			}
		})
	}
}
