#!/usr/bin/env bash
# Mutation check of the alpha rows' pass criteria (issue #194): each mutant changes one condition of
# internal/alpha, and `go test ./internal/alpha` must then fail. A mutant the tests let through is a
# check that no test depends on.
#
#   hack/mutate-alpha.sh            # run every mutant; exit 1 if one survives or does not apply
#
# It edits files in the working tree and restores each from a copy it makes first, also on any exit,
# so run it on a checkout with no uncommitted change to internal/alpha. Run it under a memory cap
# (docs/development.md).
set -u -o pipefail
cd "$(dirname "$0")/.."
pkg=internal/alpha
tmp=$(mktemp -d)
restore() { for f in "$tmp"/*; do [ -e "$f" ] && cp "$f" "$pkg/$(basename "$f")"; done; rm -rf "$tmp"; }
trap restore EXIT
cp "$pkg"/verdict.go "$pkg"/observe.go "$pkg"/run.go "$pkg"/config.go "$pkg"/record.go "$tmp"/

# file @@ exact text (must occur once) @@ replacement
mutants=(
  'verdict.go@@if inFlight == 0 {@@if inFlight < 0 {'
  'verdict.go@@if s.CutBy != "" {@@if s.CutBy == "" {'
  'verdict.go@@} else if !s.Complete {@@} else if s.Complete {'
  'verdict.go@@if o.Handovers != 1 {@@if o.Handovers < 1 {'
  'verdict.go@@if o.WorkerRestarts != 1 {@@if o.WorkerRestarts < 1 {'
  'verdict.go@@if o.WorkerRestarts != 1 {@@if o.WorkerRestarts > 1 {'
  'verdict.go@@s.ReadyAt.After(p.EndedAt) {@@s.ReadyAt.After(p.EndedAt.Add(time.Hour)) {'
  'verdict.go@@o.Attributed != o.Responses {@@o.Attributed > o.Responses {'
  'verdict.go@@if o.Gap() <= 0 {@@if o.Gap() < 0 {'
  'verdict.go@@if !o.NoOverlapSlotSeen {@@if o.NoOverlapSlotSeen {'
  'verdict.go@@if !suc.SubmitTime.Before(*pre.EndTime) {@@if suc.SubmitTime.After(*pre.EndTime) {'
  'verdict.go@@if suc.StartTime.Before(*pre.EndTime) {@@if suc.StartTime.After(*pre.EndTime) {'
  'verdict.go@@case s.Ended.Sub(s.Opened) < Hour:@@case s.Ended.Sub(s.Opened) <= Hour:'
  'verdict.go@@if o.TunnelDrops != 0 {@@if o.TunnelDrops > 1 {'
  'verdict.go@@case !r.OK:@@case r.OK:'
  'verdict.go@@case r.Direction != "up":@@case r.Direction == "up":'
  'verdict.go@@r.AskedSeconds < 30@@r.AskedSeconds < 29'
  'verdict.go@@r.TimedSeconds < 29@@r.TimedSeconds < 28'
  'verdict.go@@r.Bytes <= 0:@@r.Bytes < 0:'
  'verdict.go@@j.EndTime.Sub(*j.StartTime) < Hour@@j.EndTime.Sub(*j.StartTime) < 0'
  'verdict.go@@case j.Partition != row.Partition:@@case j.Partition == row.Partition:'
  'verdict.go@@if o.Partition != row.Partition {@@if o.Partition == row.Partition {'
  'verdict.go@@if o.Basis != "allocated" {@@if o.Basis == "allocated" {'
  'verdict.go@@if o.RequestedGPUs <= 0 || o.NodeGPUs <= 0 {@@if o.RequestedGPUs < 0 || o.NodeGPUs < 0 {'
  'verdict.go@@case int(*o.RecordGPUs) != o.NodeGPUs:@@case int(*o.RecordGPUs) == o.NodeGPUs:'
  'verdict.go@@if o.RequestedGPUs != o.NodeGPUs {@@if o.RequestedGPUs == o.NodeGPUs {'
  'verdict.go@@if len(o.Tags) < 2 {@@if len(o.Tags) < 1 {'
  'verdict.go@@if len(o.Tags) != 2 {@@if len(o.Tags) > 2 {'
  'observe.go@@!s.Opened.After(t) && s.Ended.After(t)@@s.Opened.Before(t) && s.Ended.After(t)'
  'observe.go@@!s.Opened.After(t) && s.Ended.After(t)@@!s.Opened.After(t) && !s.Ended.Before(t)'
  'run.go@@if !rec.Tagged(t) {@@if rec.Tagged(t) {'
  'config.go@@if getenv(k) != "" {@@if getenv(k) == "x" {'
  'config.go@@p != row.Partition@@p == row.Partition'
  'config.go@@g != strconv.Itoa(row.GPUs)@@g == strconv.Itoa(row.GPUs)'
  'record.go@@fi.Mode().Perm()&0o077 != 0@@fi.Mode().Perm()&0o077 == 0'
  'record.go@@if !tagRE.MatchString(tag) {@@if tagRE.MatchString(tag) {'
)

killed=0 survived=0 broken=0
for m in "${mutants[@]}"; do
  file=${m%%@@*}; rest=${m#*@@}; from=${rest%%@@*}; to=${rest#*@@}
  cp "$tmp/$file" "$pkg/$file"
  if ! python3 - "$pkg/$file" "$from" "$to" <<'PY'
import sys
p, a, b = sys.argv[1:4]
s = open(p).read()
if s.count(a) != 1:
    sys.exit(1)
open(p, 'w').write(s.replace(a, b, 1))
PY
  then echo "BROKEN   $file: the text '$from' does not occur exactly once"; broken=$((broken+1)); continue; fi
  if go test -count=1 -timeout 120s "./$pkg" >/dev/null 2>&1; then
    echo "SURVIVED $file: $from  ->  $to"; survived=$((survived+1))
  else
    killed=$((killed+1))
  fi
  cp "$tmp/$file" "$pkg/$file"
done
echo "mutants: ${#mutants[@]}, killed $killed, survived $survived, broken $broken"
[ "$survived" -eq 0 ] && [ "$broken" -eq 0 ]
