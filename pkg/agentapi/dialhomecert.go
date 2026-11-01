package agentapi

import (
	"bytes"
	"crypto/subtle"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"time"
)

// VerifyDialHomeCert checks the launcher certificate's bytes before frpc may
// trust them (HBEP-6, Certificate checks; HBEP-7): exactly one PEM
// certificate, with nothing before, between or after it and no PEM headers;
// its public-key pin equals agent.json's pin; it names DialHomeServerName,
// which frpc checks the front against; and it is valid at now. frpc trusts every
// certificate in its trustedCaFile, and the pin covers one, so a second one is
// refused.
//
// It is the one check the agent runs before it starts frpc
// (internal/agent, FrpcConfig) and the Provider's controller runs before it
// stages the certificate (internal/providerctl, FrontSettings.CheckCertificate),
// so the two can never disagree about a file: a certificate that passes here
// before the claim passes in the job, and one the agent would refuse is never
// staged. It was in internal/agent; it moved here, unchanged, so the provider
// shares it. Every error starts with "the dial-home certificate:" and never
// holds the certificate's bytes.
func VerifyDialHomeCert(b []byte, pin string, now time.Time) error {
	refuse := func(format string, args ...any) error {
		return fmt.Errorf("the dial-home certificate: %s", fmt.Sprintf(format, args...))
	}
	if pin == "" {
		return refuse("no pin (agent.json's certSPKISHA256) to check it against")
	}
	var certs []*x509.Certificate
	rest := b
	for len(bytes.TrimSpace(rest)) != 0 {
		trimmed := bytes.TrimLeft(rest, " \t\r\n")
		if !bytes.HasPrefix(trimmed, []byte("-----BEGIN ")) {
			return refuse("it holds data that is not a PEM block")
		}
		blk, next := pem.Decode(trimmed)
		if blk == nil {
			return refuse("it holds a malformed PEM block")
		}
		rest = next
		if blk.Type != "CERTIFICATE" {
			return refuse("it holds a %q block; want one certificate only", blk.Type)
		}
		if len(blk.Headers) != 0 {
			// frpc's AppendCertsFromPEM skips a block with headers, which
			// would leave it trusting nothing.
			return refuse("its certificate block has PEM headers")
		}
		c, err := x509.ParseCertificate(blk.Bytes)
		if err != nil {
			return refuse("%v", err)
		}
		certs = append(certs, c)
	}
	if len(certs) != 1 {
		return refuse("it holds %d certificates; want exactly one, the pinned one", len(certs))
	}
	c := certs[0]
	if got := PinOf(c); subtle.ConstantTimeCompare([]byte(got), []byte(pin)) != 1 {
		return refuse("its pin %s is not agent.json's %s", got, pin)
	}
	if err := c.VerifyHostname(DialHomeServerName); err != nil {
		return refuse("it does not name %s, which frpc checks: %v", DialHomeServerName, err)
	}
	if now.Before(c.NotBefore) || now.After(c.NotAfter) {
		return refuse("it is valid from %s to %s, not now (%s)", c.NotBefore.UTC().Format(time.RFC3339), c.NotAfter.UTC().Format(time.RFC3339), now.UTC().Format(time.RFC3339))
	}
	return nil
}
