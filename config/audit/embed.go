// Package audit embeds the framework's API-server audit policy and builds the flags that install it, so that
// the launcher, which starts k3s with them, and internal/testenv's audit option, which starts a test API server
// with them, use one source and no copy (HBEP-10, Audit; issue #660, PR B). The policy file is written by hand;
// hack/generate.sh does not touch it.
package audit

import (
	_ "embed"
	"strconv"
)

// Policy is policy.yaml: the four rules of HBEP-10's Audit, in the order they are evaluated.
//
//go:embed policy.yaml
var Policy []byte

// DefaultMaxSizeMB is the size, in megabytes, at which the API server rotates the log file.
const DefaultMaxSizeMB = 100

// Flags are the kube-apiserver audit flags, each as name=value without the leading dashes: the policy file, the
// log's path, its size at rotation, and the rotation that keeps every file for the grant (HBEP-10, Kept for the
// grant, Compressed when cold):
//   - audit-log-maxbackup=0 and audit-log-maxage=0 mean no limit on the number or the age of the rotated files, so
//     the framework removes none (the API server's own defaults delete after 366 days);
//   - audit-log-compress gzips each rotated file; only the file being written stays plain.
//
// maxSizeMB is the one value a caller may choose (a test's rotation uses 1); 0 means DefaultMaxSizeMB.
func Flags(policyFile, logPath string, maxSizeMB int) []string {
	if maxSizeMB <= 0 {
		maxSizeMB = DefaultMaxSizeMB
	}
	return []string{
		"audit-policy-file=" + policyFile,
		"audit-log-path=" + logPath,
		"audit-log-maxsize=" + strconv.Itoa(maxSizeMB),
		"audit-log-maxbackup=0",
		"audit-log-maxage=0",
		"audit-log-compress=true",
	}
}
