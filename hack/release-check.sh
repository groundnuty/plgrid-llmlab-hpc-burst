#!/usr/bin/env bash
# The release workflow's guard for a pushed tag (HBEP-15, Releases): it fails unless the tag can be
# released at all.
#
# usage: hack/release-check.sh TAG
#
# Run it in a checkout of the tag, with the history of origin/main (actions/checkout with
# fetch-depth: 0). It fails unless:
#   - TAG is a version tag Go reads: vMAJOR.MINOR.PATCH with an optional pre-release (v0.1.0-alpha.1),
#     and no build metadata;
#   - TAG's major version is the one go.mod's module path allows: 0 or 1 for a path with no /vN
#     suffix, N for one with. Go ignores any other tag in a binary's build information and stamps a
#     pseudo-version instead, so a tag of another major could never be reported by what it names;
#   - docs/releases/TAG.md exists and is not empty: the release's summary is committed by a pull
#     request before the tag, so the push scan and a review see it, and `gh release create` publishes
#     it with --notes-file;
#   - the checkout is the commit TAG names;
#   - that commit is on origin/main (git merge-base --is-ancestor), so a tag pushed from a branch, or
#     from a commit that main never had, releases nothing. A tag is never moved or deleted
#     afterwards (HBEP-15, A failed release), so this is the one place to refuse it.
# It changes nothing.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
die() { echo "release-check.sh: $*" >&2; exit 1; }

[ $# -eq 1 ] && [ -n "$1" ] || die "usage: hack/release-check.sh TAG"
tag=$1
cd "$root"

semver='^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$'
[[ $tag =~ $semver ]] || die "the tag '$tag' is not vMAJOR.MINOR.PATCH with an optional pre-release (v0.1.0-alpha.1); build metadata is not allowed"
major=${BASH_REMATCH[1]}
echo "ok       tag $tag is a version tag"

module=$(awk '$1 == "module" { print $2; exit }' go.mod)
[ -n "$module" ] || die "go.mod has no module line"
if [[ $module =~ /v([0-9]+)$ ]]; then
  [ "$major" = "${BASH_REMATCH[1]}" ] || die "the module path $module takes major version ${BASH_REMATCH[1]}, and the tag $tag has $major"
elif [ "$major" != 0 ] && [ "$major" != 1 ]; then
  die "the module path $module has no /vN suffix, so its tags have major version 0 or 1, and $tag has $major (Go would stamp a pseudo-version, not the tag)"
fi
echo "ok       tag $tag has the major version of $module"

notes=docs/releases/$tag.md
[ -f "$notes" ] && [ -s "$notes" ] || die "$notes is missing or empty: the release's summary is committed by a pull request before the tag"
echo "ok       $notes exists"

tagged=$(git rev-parse --verify --quiet "refs/tags/$tag^{commit}") || die "the tag $tag does not exist in this checkout"
head=$(git rev-parse HEAD)
[ "$tagged" = "$head" ] || die "the checkout is $head, and the tag $tag names $tagged"
echo "ok       the checkout is the commit $tag names"

git rev-parse --verify --quiet refs/remotes/origin/main > /dev/null || die "origin/main is not in this checkout (fetch it, with its history)"
git merge-base --is-ancestor "$head" origin/main || die "the commit $head of the tag $tag is not on origin/main"
echo "ok       $head is on origin/main"
