package hubfiles

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

// ErrInvalidRevision is wrapped when the revision is not a full commit id.
var ErrInvalidRevision = errors.New("hubfiles: revision must be a full commit id")

// ErrInvalidRepository is wrapped when the repository id is malformed.
var ErrInvalidRepository = errors.New("hubfiles: invalid repository id")

// ErrInvalidListing is wrapped when the Hub's listing is malformed or cannot
// be trusted: an unclean path, a duplicate path, a missing or malformed hash,
// an entry type this package does not know, or an empty list.
var ErrInvalidListing = errors.New("hubfiles: unusable listing")

var (
	// A repository id is "name" (a few old repositories) or "owner/name"; each
	// part starts with a letter or digit, so neither can be "." or "..".
	repositoryPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*(/[A-Za-z0-9][A-Za-z0-9._-]*)?$`)
	commitPattern     = regexp.MustCompile(`^[0-9a-f]{40}$`)
	sha256Pattern     = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// maxRepositoryLength is the longest repository id accepted; the Hub's limits
// are well below it.
const maxRepositoryLength = 200

func checkRepository(repo string) error {
	if len(repo) > maxRepositoryLength || !repositoryPattern.MatchString(repo) {
		return fmt.Errorf("%w: %q must be \"owner/name\"", ErrInvalidRepository, repo)
	}
	return nil
}

// checkRevision accepts only 40 lowercase hex characters, the form of a git
// commit id. A branch, a tag, a short id and uppercase hex are refused: the
// last two name the commit in another spelling, and the deployment ID takes
// the lowercase form (package deploymentid).
func checkRevision(rev string) error {
	if !commitPattern.MatchString(rev) {
		return fmt.Errorf("%w: %q is not 40 lowercase hex characters", ErrInvalidRevision, rev)
	}
	return nil
}

// checkListedPath validates a path the Hub listed, before it is put in a URL
// or handed to the deployment ID: relative, '/'-separated, valid UTF-8, no
// control characters, no empty, "." or ".." segment.
func checkListedPath(p string) error {
	if p == "" || !utf8.ValidString(p) {
		return fmt.Errorf("%w: file path %q is empty or not valid UTF-8", ErrInvalidListing, p)
	}
	for _, r := range p {
		if r < 0x20 || r == 0x7f || r == '\\' {
			return fmt.Errorf("%w: file path %q has a control character or a backslash", ErrInvalidListing, p)
		}
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return fmt.Errorf("%w: file path %q is not a clean relative path", ErrInvalidListing, p)
		}
	}
	return nil
}
