package engine

import (
	"context"
	"strconv"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Verify reads what verification needs from the engine (HBEP-7, Instance
// start; HBEP-9): its version, its resolved settings, its fingerprint and the
// chat template's overhead. Every value comes from the engine: a setting it
// does not report stays absent, never filled from the declared spec, so an
// engine that reports nothing fails the controller's required fields
// (HBEP-7, Risks: verification passes because it read nothing). Files and
// ImageDigest are the agent's. A read that fails is named in Error; the
// others are still reported.
func (c *Client) Verify(ctx context.Context) agentapi.VerifyResult {
	var res agentapi.VerifyResult
	var failed []string
	fail := func(what string, err error) { failed = append(failed, what+": "+err.Error()) }
	settings := map[string]string{}

	if v, err := c.getVersion(ctx); err != nil {
		fail("version", err)
	} else {
		res.EngineVersion = cut(v)
	}
	if cards, err := c.models(ctx); err != nil {
		fail("models", err)
	} else if len(cards) > 0 && cards[0].MaxModelLen > 0 {
		settings["max-model-len"] = strconv.FormatInt(cards[0].MaxModelLen, 10)
	}
	if m, err := c.metrics(ctx); err != nil {
		fail("metrics", err)
	} else if cfg := m.cacheConfig(); cfg != nil {
		for label, flag := range cacheSettings {
			if v, ok := cfg[label]; ok && v != "" && v != "None" {
				settings[flag] = cut(v)
			}
		}
	}
	if fp, err := c.fingerprint(ctx); err != nil {
		fail("fingerprint", err)
	} else {
		res.Fingerprint = cut(fp)
	}
	if o, err := c.templateOverhead(ctx); err != nil {
		fail("template overhead", err)
	} else {
		res.TemplateOverhead = o
	}
	if len(settings) > 0 {
		res.Settings = settings
	}
	if len(failed) > 0 {
		res.Error = agentapi.TruncateUTF8(strings.Join(failed, "; "), agentapi.MaxMessageBytes)
	}
	return res
}

// fingerprint sends one short greedy chat request and returns the engine
// fingerprint it carries, "" when it carries none.
func (c *Client) fingerprint(ctx context.Context) (string, error) {
	r, err := c.chat(ctx, chatRequest{
		Messages:  []message{{Role: "user", Content: "Reply with one word: ready."}},
		MaxTokens: 4,
	})
	if err != nil {
		return "", err
	}
	return r.SystemFingerprint, nil
}
