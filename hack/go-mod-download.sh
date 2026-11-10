#!/usr/bin/env bash
# Download the root module's dependencies for a CI job, with a bounded retry, then verify them.
#
# usage: hack/go-mod-download.sh
#
# Why (issue #346): CI caches no Go modules (issue #290: an untidy go.mod must never hide behind a cache),
# so every job's first go command fetched every module from proxy.golang.org, and one transient error
# failed the job before any test ran (an HTTP/2 stream reset on a zip, PR #344's rehearsal part 3). Two
# layers answer that, and neither caches anything between runs:
#   1. The workflow sets GOPROXY=https://proxy.golang.org|direct. In the go command (Go 1.26.0,
#      cmd/go/internal/modfetch/proxy.go, proxyList and TryProxies) a "," lets it fall through to the next
#      entry only on a 404 or 410, and a "|" on ANY error, a 5xx or a reset stream included. A request
#      that the proxy fails is then answered by the module's own origin (git over https), so a total
#      proxy outage still ends in the same bytes, only slower. Every zip is checked against go.sum
#      whichever source served it, so the pipe changes who answers, never what runs. It covers every go
#      command of every job, tidy's go mod tidy -diff included, which this script cannot retry: that
#      command exits 1 for "go.mod is untidy" and for "a download failed" alike.
#   2. This script, a step in each job that builds, downloads the whole build list before the first build
#      and tries again, a bounded number of times, when the download fails. A blip is then answered by
#      the proxy again within seconds, not by a slower walk to each origin, and every later go command
#      reads the module cache of this run. It then runs go mod verify on what was downloaded, which no
#      other step of CI did: a file whose bytes differ from go.sum's pin fails the job here, and is never
#      retried, since a second download cannot make a corrupt file honest.
#
# A failure that is no network error (go.sum lacks an entry, go.mod is unreadable) fails every attempt
# and is retried too, because the go command's exit status cannot tell the two apart: it costs the pauses
# below and nothing else, and the job still fails.
#
# Environment (all optional):
#   GO_MOD_DOWNLOAD_ATTEMPTS  how many times go mod download may run, 1 to 10 (default 3)
#   GO_MOD_DOWNLOAD_PAUSE     seconds to wait after the first failure, doubling after each next one
#                             (default 10: waits of 10 s and 20 s before the second and third attempt)
#   GO_MOD_DIR                for tests: the module to download, instead of this repository's root
set -uo pipefail

die() { echo "go-mod-download.sh: $*" >&2; exit 2; }

attempts=${GO_MOD_DOWNLOAD_ATTEMPTS:-3}
pause=${GO_MOD_DOWNLOAD_PAUSE:-10}
[[ $attempts =~ ^[1-9][0-9]*$ ]] && [ "$attempts" -le 10 ] ||
  die "GO_MOD_DOWNLOAD_ATTEMPTS is '$attempts', want a whole number from 1 to 10"
[[ $pause =~ ^[0-9]+$ ]] && [ "$pause" -le 300 ] ||
  die "GO_MOD_DOWNLOAD_PAUSE is '$pause', want a whole number of seconds, 0 to 300"
command -v go > /dev/null || die "there is no go on PATH"

root=${GO_MOD_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}
cd "$root" || die "cannot enter $root"

attempt=1
delay=$pause
while ! go mod download; do
  if [ "$attempt" -ge "$attempts" ]; then
    echo "go-mod-download.sh: go mod download failed $attempt time(s); giving up" >&2
    exit 1
  fi
  echo "go-mod-download.sh: go mod download failed (attempt $attempt of $attempts); trying again in $delay s" >&2
  sleep "$delay"
  attempt=$((attempt + 1))
  delay=$((delay * 2))
done

if ! go mod verify; then
  echo "go-mod-download.sh: go mod verify failed: a downloaded module differs from go.sum, and a retry cannot fix that" >&2
  exit 1
fi
echo "go-mod-download.sh: downloaded and verified on attempt $attempt of $attempts"
