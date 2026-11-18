package agentcli

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// maxName is the longest name Kubernetes gives an object.
const maxName = 253

// nameAlphabet is the characters of a generated name's tail, 36 of them.
const nameAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

// generatedName is "op-" and 10 random characters of [a-z0-9]. A byte of the random source is used only
// below 252, the largest multiple of 36 that fits, so every character is equally likely.
func generatedName() string {
	out := make([]byte, 0, 10)
	buf := make([]byte, 16)
	for len(out) < 10 {
		if _, err := rand.Read(buf); err != nil {
			panic("agentcli: no random source: " + err.Error()) // crypto/rand does not fail on a working host
		}
		for _, b := range buf {
			if b < 252 && len(out) < 10 {
				out = append(out, nameAlphabet[int(b)%len(nameAlphabet)])
			}
		}
	}
	return "op-" + string(out)
}

// shortHash is the first n hex characters of the SHA-256 of s.
func shortHash(s string, n int) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:n]
}

// derivedKey names one Operation of a release over several ModelDeployments: the key, "-" and 8 hex
// characters of the SHA-256 of the ModelDeployment's name, so a retry derives the same names.
func derivedKey(key, md string) string { return key + "-" + shortHash(md, 8) }

// approvalName is the name of the Approval of user for planID of the Operation: the Operation's name,
// cut so that the whole stays within 253 characters, "-", and 10 hex characters of the SHA-256 of the
// user name, a newline and the plan ID. The same approver of the same plan gets the same name, so a retry
// finds the first. A cut that leaves a "." or "-" at its end would make an invalid name, so it is trimmed.
func approvalName(op, user, planID string) string {
	const tail = 10
	if room := maxName - tail - 1; len(op) > room {
		op = op[:room]
	}
	op = strings.TrimRight(op, ".-")
	return op + "-" + shortHash(user+"\n"+planID, tail)
}
