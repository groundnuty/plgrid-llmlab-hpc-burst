#!/usr/bin/env bash
# Mutation check of the alpha rows (issue #194): each mutant changes one condition, and the tests of what it
# changes must then fail. A mutant the tests let through is a check that no test depends on.
#
#   hack/mutate-alpha.sh              # every mutant; exit 1 if one survives or does not apply
#   hack/mutate-alpha.sh --no-script  # leave out the mutants of hack/live-checks.sh (the slow ones)
#
# A mutant ends as KILLED (a test fails), SURVIVED (every test passes) or BROKEN: its text does not occur exactly once, or
# the Go file it changes no longer compiles. A mutant that does not compile fails to build the tests, and that is not a test
# failing, so it is reported as BROKEN, never as killed; the run fails for a survivor and for a broken mutant alike. The
# compile is its own step, to one fixed test-binary path per package, and the binary then runs: so MUTATE_ALPHA_BUILD_LOCK,
# a lock file, is taken (flock -o) around each compile only, never around a test run.
#
# Three kinds, by the file they change:
#   internal/alpha (a bare file name)           go test ./internal/alpha
#   internal/providerctl/conformance/...        go test of that package and internal/alpha (the accounting row's half)
#   hack/live-checks.sh                         go test of hack/livechecks, the alpha script tests only. Set
#                                               HPCBURST_LIVECHECKS_ALPHA_TEST and HPCBURST_LIVECHECKS_CONF_TEST to prebuilt
#                                               test binaries to spare it two builds per mutant.
#
# It edits files in the working tree and restores each from a copy it makes first, also on any exit, so run it
# on a checkout with no uncommitted change to them. Run it under a memory cap (docs/development.md).
set -u -o pipefail
cd "${MUTATE_ALPHA_ROOT:-$(dirname "$0")/..}" || exit 2   # the tree to mutate; the tests of this script point it at a throwaway module
pkg=internal/alpha
script_too=1; [ "${1:-}" != --no-script ] || script_too=0
tmp=$(mktemp -d)
declare -A saved=()
restore() { for f in "${!saved[@]}"; do cp "${saved[$f]}" "$f"; done; rm -rf "$tmp"; }
trap restore EXIT
path_of() { case $1 in */*) printf '%s' "$1" ;; *) printf '%s/%s' "$pkg" "$1" ;; esac; }
keep() { [ -n "${saved[$1]:-}" ] || { saved[$1]="$tmp/$(printf '%s' "$1" | tr / _)"; cp "$1" "${saved[$1]}"; }; }
build() { if [ -n "${MUTATE_ALPHA_BUILD_LOCK:-}" ]; then flock -o "$MUTATE_ALPHA_BUILD_LOCK" "$@"; else "$@"; fi; }
# compile_tests <package> <binary>: the package's test binary, to one fixed path, under the build lock; on failure its first
# lines are left in $cerr.
compile_tests() {
  build go test -c -o "$2" "./$1" 2>"$tmp/compile.err" && return 0
  cerr=$(head -n 3 "$tmp/compile.err" | tr '\n' ' '); return 1
}

# file @@ exact text (must occur once) @@ replacement
mutants=(
  'verdict.go@@if inFlight == 0 {@@if inFlight < 0 {'
  'verdict.go@@if s.CutBy != "" {@@if s.CutBy == "" {'
  'verdict.go@@} else if !s.Complete {@@} else if s.Complete {'
  'verdict.go@@if o.Handovers != 1 {@@if o.Handovers < 1 {'
  'verdict.go@@if n != 1 {@@if n < 1 {'
  'verdict.go@@if n != 1 {@@if n > 1 {'
  'verdict.go@@if len(tags) != row.MostJobs {@@if len(tags) < row.MostJobs {'
  'verdict.go@@if len(tags) != row.MostJobs {@@if len(tags) > row.MostJobs {'
  'verdict.go@@s.ReadyAt.After(p.EndedAt) {@@s.ReadyAt.After(p.EndedAt.Add(time.Hour)) {'
  'verdict.go@@} else if o.Attributed != o.Responses {@@} else if o.Attributed > o.Responses {'
  'verdict.go@@if o.Responses <= 0 {@@if o.Responses < 0 {'
  'verdict.go@@if o.Gap() <= 0 {@@if o.Gap() < 0 {'
  'verdict.go@@if !o.NoOverlapSlotSeen {@@if o.NoOverlapSlotSeen {'
  'verdict.go@@if !suc.SubmitTime.Before(*pre.EndTime) {@@if suc.SubmitTime.After(*pre.EndTime) {'
  'verdict.go@@if suc.StartTime.Before(*pre.EndTime) {@@if suc.StartTime.After(*pre.EndTime) {'
  'verdict.go@@case j.StartTime == nil || j.EndTime == nil:@@case j.StartTime == nil && j.EndTime == nil:'
  'verdict.go@@case j.EndTime.Sub(*j.StartTime) < Hour:@@case j.EndTime.Sub(*j.StartTime) <= Hour:'
  'verdict.go@@case !r.OK:@@case r.OK:'
  'verdict.go@@if _, dup := by[r.Direction]; dup {@@if false {'
  'verdict.go@@r.AskedSeconds < 30@@r.AskedSeconds < 29'
  'verdict.go@@r.TimedSeconds < 29@@r.TimedSeconds < 28'
  'verdict.go@@r.Bytes <= 0:@@r.Bytes < 0:'
  'verdict.go@@case j.Partition != row.Partition:@@case j.Partition == row.Partition:'
  'verdict.go@@if o.Partition != row.Partition {@@if o.Partition == row.Partition {'
  'verdict.go@@if o.Basis != "allocated" {@@if o.Basis == "allocated" {'
  'verdict.go@@if o.RequestedGPUs <= 0 || o.NodeGPUs <= 0 {@@if o.RequestedGPUs < 0 || o.NodeGPUs < 0 {'
  'verdict.go@@case int(*o.RecordGPUs) != o.NodeGPUs:@@case int(*o.RecordGPUs) == o.NodeGPUs:'
  'verdict.go@@if o.RequestedGPUs != o.NodeGPUs {@@if o.RequestedGPUs == o.NodeGPUs {'
  'verdict.go@@if perTag[tag] > 1 {@@if perTag[tag] > 2 {'
  'verdict.go@@if len(distinct) > row.MostJobs {@@if len(distinct) >= row.MostJobs {'
  'verdict.go@@if peak > row.MostJobs {@@if peak >= row.MostJobs {'
  'verdict.go@@if ids[j.ID] {@@if false {'
  'verdict.go@@return edges[a].delta < edges[b].delta@@return edges[a].delta > edges[b].delta'
  'verdict.go@@end := farFuture@@end := j.SubmitTime'
  'verdict.go@@complete = false@@complete = true'
  'observe.go@@!s.Opened.After(t) && s.Ended.After(t)@@s.Opened.Before(t) && s.Ended.After(t)'
  'observe.go@@!s.Opened.After(t) && s.Ended.After(t)@@!s.Opened.After(t) && !s.Ended.Before(t)'
  'run.go@@if !rec.Tagged(t) {@@if rec.Tagged(t) {'
  'config.go@@if getenv(k) != "" {@@if getenv(k) == "x" {'
  'config.go@@p != row.Partition@@p == row.Partition'
  'config.go@@g != strconv.Itoa(row.GPUs)@@g == strconv.Itoa(row.GPUs)'
  'record.go@@fi.Mode().Perm()&0o077 != 0@@fi.Mode().Perm()&0o077 == 0'
  'record.go@@if !tagRE.MatchString(tag) {@@if tagRE.MatchString(tag) {'
  'verdict.go@@if len(d.Providers) != 1 {@@if len(d.Providers) > 1 {'
  'verdict.go@@} else if d.Providers[0].Cluster != row.Cluster {@@} else if d.Providers[0].Cluster == row.Cluster {'
  'verdict.go@@case d.LeaseLimit <= Lead():@@case d.LeaseLimit < Lead():'
  'verdict.go@@case d.LeaseLimit != row.Lease:@@case d.LeaseLimit < row.Lease:'
  'verdict.go@@if d.MaxRequestDuration != MaxRequestDuration {@@if d.MaxRequestDuration > MaxRequestDuration {'
  'verdict.go@@!d.ReleaseWhenIdle || d.IdleLimit <= 0 || d.IdleLimit > 10*time.Minute@@d.ReleaseWhenIdle || d.IdleLimit <= 0 || d.IdleLimit > 10*time.Minute'
  'verdict.go@@d.IdleLimit <= 0 || d.IdleLimit > 10*time.Minute@@d.IdleLimit <= 0 || d.IdleLimit >= 10*time.Minute'
  'verdict.go@@case j.TimeLimit != row.Lease:@@case j.TimeLimit == row.Lease:'
  'rows.go@@func MinLease() time.Duration { return Lead() + time.Minute }@@func MinLease() time.Duration { return Lead() }'
  'rows.go@@return MaxRequestDuration + DrainMargin + HandoverMargin + StartTimeout@@return MaxRequestDuration + DrainMargin + HandoverMargin'
  'verdict.go@@case s.Ended.Sub(s.Opened) > MaxRequestDuration:@@case s.Ended.Sub(s.Opened) >= MaxRequestDuration:'
  'verdict.go@@if last.Sub(first) < Hour {@@if last.Sub(first) <= Hour {'
  'verdict.go@@!drainStart.IsZero() && last.After(drainStart)@@!drainStart.IsZero() && !last.Before(drainStart)'
  'verdict.go@@if o.DrainStart.IsZero() {@@if false {'
  'verdict.go@@case s.Opened.IsZero() || s.Ended.IsZero():@@case s.Opened.IsZero() && s.Ended.IsZero():'
  'verdict.go@@if len(reqs) == 0 {@@if len(reqs) < 0 {'
  'verdict.go@@case s.CutBy != "" || s.Error == "stream_cut":@@case s.CutBy != "":'
  'verdict.go@@if len(samples) < 2 {@@if len(samples) < 1 {'
  'verdict.go@@case !s.LastStart.Equal(ss[0].LastStart):@@case false:'
  'verdict.go@@if !s.Connected {@@if s.Connected {'
  'verdict.go@@s.At.Sub(ss[i-1].At) > MaxSampleGap@@s.At.Sub(ss[i-1].At) >= MaxSampleGap'
  'verdict.go@@if ss[0].At.After(first) {@@if false {'
  'verdict.go@@if ss[len(ss)-1].At.Before(last) {@@if false && ss[len(ss)-1].At.Before(last) {'
  'verdict.go@@case s.At.IsZero() || s.LastStart.IsZero():@@case s.At.IsZero() && s.LastStart.IsZero():'
  'verdict.go@@if longestChain(reqs) < MinChain {@@if longestChain(reqs) <= MinChain {'
  'verdict.go@@d >= NearDeadline && d <= MaxRequestDuration@@d > NearDeadline && d <= MaxRequestDuration'
  'verdict.go@@if concurrentStreams(reqs) < ConcurrentStreams {@@if concurrentStreams(reqs) <= ConcurrentStreams {'
  'verdict.go@@if longestIdle(reqs) < MinIdleGap {@@if longestIdle(reqs) <= MinIdleGap {'
  'verdict.go@@r.TokensPerSecond() >= StreamRate@@r.TokensPerSecond() > StreamRate'
  'verdict.go@@gap >= 0 && gap <= MaxChainGap@@gap > 0 && gap <= MaxChainGap'
  'verdict.go@@gap >= 0 && gap <= MaxChainGap@@gap >= 0 && gap < MaxChainGap'
  'verdict.go@@case !r.UnderLoad:@@case r.UnderLoad:'
  'verdict.go@@case dir == DirDownload && r.Tokens <= 0:@@case dir == DirDownload && r.Tokens < 0:'
  'verdict.go@@case dir == DirDownload && r.Tokens <= 0:@@case r.Tokens <= 0:'
  'verdict.go@@case !found:@@case false && !found:'
  'verdict.go@@if dir != DirUpload && dir != DirDownload {@@if false && dir != DirUpload && dir != DirDownload {'
  'verdict.go@@if largestBody(reqs) < MinLargeBody {@@if largestBody(reqs) <= MinLargeBody {'
  'verdict.go@@} else if largeAmongStreams(reqs) == 0 {@@} else if false {'
  'verdict.go@@if r.BodyBytes >= MinLargeBody {@@if r.BodyBytes > MinLargeBody {'
  'verdict.go@@if open >= ConcurrentStreams {@@if open > ConcurrentStreams {'
  'verdict.go@@r.Opened.After(l.BodyStart) && r.Opened.Before(l.BodyEnd)@@r.Opened.After(l.Opened) && r.Opened.Before(l.Ended)'
  'verdict.go@@r.Opened.After(l.BodyStart) && r.Opened.Before(l.BodyEnd)@@r.Opened.After(l.BodyStart) && !r.Opened.After(l.BodyEnd)'
  'verdict.go@@at := []time.Time{l.BodyStart}@@at := []time.Time{l.Opened}'
  'verdict.go@@if !l.UploadRecorded() {@@if false {'
  'observe.go@@return !s.BodyStart.IsZero() && s.BodyEnd.After(s.BodyStart)@@return !s.BodyStart.IsZero()'
  'observe.go@@return !s.BodyStart.IsZero() && s.BodyEnd.After(s.BodyStart)@@return s.BodyEnd.After(s.BodyStart)'
  'observe.go@@return !s.BodyStart.IsZero() && s.BodyEnd.After(s.BodyStart)@@return !s.BodyStart.IsZero() && !s.BodyEnd.IsZero()'
  'verdict.go@@isStream(r) && !r.Opened.After(t) && r.Ended.After(t)@@isStream(r) && !r.Opened.After(t) && !r.Ended.Before(t)'
  'verdict.go@@r.Direction == DirDownload && r.OK && bps > 0 && bps < minRate@@r.OK && bps > 0 && bps < minRate'
  'frontlabel.go@@if label == "" {@@if false {'
  'frontlabel.go@@if len(label) > MaxFrontLabel {@@if len(label) >= MaxFrontLabel {'
  'frontlabel.go@@if len(label) > MaxFrontLabel {@@if len(label) > MaxFrontLabel+1 {'
  'frontlabel.go@@if !ok {@@if !ok && false {'
  'frontlabel.go@@strings.CutPrefix(label, r+", ")@@strings.CutPrefix(label, r)'
  'frontlabel.go@@if m == nil {@@if m == nil && false {'
  'frontlabel.go@@if m[1] != "" && !noteRE.MatchString(m[2]) {@@if false {'
  'frontlabel.go@@if m[1] != "" && !noteRE.MatchString(m[2]) {@@if m[1] == "" && !noteRE.MatchString(m[2]) {'
  'frontlabel.go@@positionRE = regexp.MustCompile(`^(?:inside|outside) Cyfronet@@positionRE = regexp.MustCompile(`(?:inside|outside) Cyfronet'
  'frontlabel.go@@Cyfronet(?:'"'"'s network)?( \((.*)\))?$@@Cyfronet(?:'"'"'s network)( \((.*)\))?$'
  'frontlabel.go@@Cyfronet(?:'"'"'s network)?( \((.*)\))?$@@Cyfronet(?:'"'"'s network)?( \((.*)\))$'
  'frontlabel.go@@[A-Za-z]+(?:@@[A-Za-z0-9]+(?:'
  'frontlabel.go@@[A-Za-z]+(?:@@[A-Za-z.]+(?:'
  'frontlabel.go@@[A-Za-z]+(?:@@[A-Za-z:]+(?:'
  'frontlabel.go@@(?:ms|s|MB/s|Mb/s|%)@@(?:ms|s|MB/s|Mb/s|%)?'
  'frontlabel.go@@(?:ms|s|MB/s|Mb/s|%)@@(?:s|MB/s|Mb/s|%)'
  'frontlabel.go@@(?:ms|s|MB/s|Mb/s|%)@@(?:ms|MB/s|Mb/s|%)'
  'frontlabel.go@@(?:ms|s|MB/s|Mb/s|%)@@(?:ms|s|MB/s|%)'
  'frontlabel.go@@(?:ms|s|MB/s|Mb/s|%)@@(?:ms|s|MB/s|Mb/s)'
  'frontlabel.go@@(?:\.[0-9]+)? ?(?:@@(?:\.[0-9]+) ?(?:'
  'frontlabel.go@@(?:[,;]? @@(?:[,;:]? '
  'frontlabel.go@@(?:[,;]? @@(?:[,;]?'
  'frontlabel.go@@return first + "\n" + FrontLine(label) + "\n" + rest@@return first + "\n" + rest'
  'run.go@@if lerr := ValidFrontLabel(cfg.FrontLabel); lerr != nil {@@if lerr := ValidFrontLabel(cfg.FrontLabel); false && lerr != nil {'
  'run.go@@InsertFrontLine(summary(failures, jobs), cfg.FrontLabel)@@summary(failures, jobs)'
  'run.go@@"frontLabel": cfg.FrontLabel, @@'
  'config.go@@bad = append(bad, EnvFrontLabel+": "+err.Error())@@_ = err'
  'config.go@@cfg.FrontLabel = getenv(EnvFrontLabel)@@cfg.FrontLabel = ""'
  'internal/providerctl/conformance/gpuacct_test.go@@w.p.save(alpha.PostsFile, []byte(alpha.InsertFrontLine(posts, frontLabel(t))))@@w.p.save(alpha.PostsFile, []byte(posts))'
  'internal/providerctl/conformance/gpuacct_test.go@@if err := alpha.ValidFrontLabel(label); err != nil {@@if err := alpha.ValidFrontLabel(label); false && err != nil {'
  'hack/live-checks.sh@@[ -z "${FRONTLABEL[$name]:-}" ] || check_front_label || bad=1@@true'
  'hack/live-checks.sh@@if [ -z "$l" ]; then say "live-checks: setting LIVE_FRONT_HOST_LABEL is missing"; return 1; fi@@:'
  'hack/live-checks.sh@@[ "${#l}" -gt 120 ]@@[ "${#l}" -gt 121 ]'
  'hack/live-checks.sh@@if [ -z "$have_role" ]; then@@if false; then'
  'hack/live-checks.sh@@if [ -z "$have_pos" ]; then@@if false; then'
  'hack/live-checks.sh@@case $l in "$role, "*) rest=${l#"$role, "};@@case $l in "$role"*) rest=${l#"$role, "};'
  'hack/live-checks.sh@@case $rest in "$pos ("*")") have_pos=1;@@case $rest in "$pos ("*) have_pos=1;'
  'hack/live-checks.sh@@if [ -n "$has_note" ] && ! [[ $note =~ $note_re ]]; then@@if false; then'
  'hack/live-checks.sh@@if [ -n "$has_note" ] && ! [[ $note =~ $note_re ]]; then@@if [ -z "$has_note" ] && ! [[ $note =~ $note_re ]]; then'
  'hack/live-checks.sh@@word="[A-Za-z]+(@@word="[A-Za-z0-9]+('
  'hack/live-checks.sh@@word="[A-Za-z]+(@@word="[A-Za-z.]+('
  'hack/live-checks.sh@@word="[A-Za-z]+(@@word="[A-Za-z:]+('
  'hack/live-checks.sh@@(ms|s|MB/s|Mb/s|%)@@(ms|s|MB/s|Mb/s|%)?'
  'hack/live-checks.sh@@(ms|s|MB/s|Mb/s|%)@@(s|MB/s|Mb/s|%)'
  'hack/live-checks.sh@@(ms|s|MB/s|Mb/s|%)@@(ms|s|MB/s|%)'
  'hack/live-checks.sh@@(ms|s|MB/s|Mb/s|%)@@(ms|s|MB/s|Mb/s)'
  'hack/live-checks.sh@@note_re="^$item([,;]? $item)*\$"@@note_re="^$item([,;:]? $item)*\$"'
  'hack/live-checks.sh@@note_re="^$item([,;]? $item)*\$"@@note_re="^$item([,;]?$item)*\$"'
  'hack/live-checks.sh@@never takes one.
unset HPCBURST_FRONT_HOST_LABEL@@never takes one.'
  'hack/live-checks.sh@@    FRONT_ROLES+=("$name")@@    :'
  'hack/live-checks.sh@@[ "$n" -gt 0 ] || { table_empty "FRONT ROLES"; return 2; }@@true'
  'hack/live-checks.sh@@[ "$n" -gt 0 ] || { table_empty "FRONT LABEL"; return 2; }@@true'
  'hack/live-checks.sh@@[ -z "${FRONTLABEL[$1]:-}" ] || export HPCBURST_FRONT_HOST_LABEL=${S[LIVE_FRONT_HOST_LABEL]}@@true'
  'hack/live-checks.sh@@still take it.
  export_front_label "$n"@@still take it.'
  'hack/live-checks.sh@@LIVE_PENDING_MAX_SECONDS:-7200}
  export_front_label "$n"@@LIVE_PENDING_MAX_SECONDS:-7200}'
  'hack/live-checks.sh@@gpu-accounting-athena
# END FRONT LABEL@@# END FRONT LABEL'
  'rows.go@@return lease - (MaxRequestDuration + DrainMargin) - TunnelHour@@return lease - (MaxRequestDuration + DrainMargin) + TunnelHour'
  'rows.go@@if measured < ShortLeaseRule {@@if measured <= ShortLeaseRule {'
  'rows.go@@lease < MinLease() || lease > MaxTunnelLease@@lease <= MinLease() || lease > MaxTunnelLease'
  'rows.go@@lease < MinLease() || lease > MaxTunnelLease@@lease < MinLease() || lease >= MaxTunnelLease'
  'rows.go@@lease%time.Minute != 0@@false'
  'rows.go@@r.SlotHold = lease@@r.SlotHold = r.SlotHold'
  'config.go@@(strings.HasPrefix(row.Name, "tunnel-hour-") && getenv(envDry) != "1")@@false'  'hack/live-checks.sh@@gpu-accounting-helios|helios|helios|plgrid-gpu-gh200|now|now|ACCOUNT_HELIOS_GH200|1|72|117Gi|00:05:00|30|-|10@@gpu-accounting-helios|helios|helios|plgrid-gpu-gh200|now|now|ACCOUNT_HELIOS_GH200|1|72|117Gi|00:05:00|30|-'
  'hack/live-checks.sh@@gpu-accounting-helios|helios|helios|plgrid-gpu-gh200|now|now|@@gpu-accounting-helios|helios|helios|plgrid-gpu-gh200|now|-|'
  'hack/live-checks.sh@@      [ "${SWAIT[$n]:--}" = - ] || wait=0@@      true'
  'hack/live-checks.sh@@    gpu-accounting-*|reach-*) [ "${SWAIT[$n]:--}" != - ] || export HPCBURST_CONF_PENDING_MAX_SECONDS@@    gpu-accounting-*|reach-*) export HPCBURST_CONF_PENDING_MAX_SECONDS'
  'hack/live-checks.sh@@reach-helios|helios|helios|plgrid-gpu-gh200|now|now|ACCOUNT_HELIOS_GH200|1|1|1Gi|00:02:00|20|-C memfs|10@@reach-helios|helios|helios|plgrid-gpu-gh200|now|now|ACCOUNT_HELIOS_GH200|1|1|1Gi|00:02:00|20|-C memfs'
  'hack/live-checks.sh@@reach-athena|athena|athena|plgrid-now|-|plgrid-now|@@reach-athena|athena|athena|plgrid-now|-|-|'
  'hack/live-checks.sh@@    reach-*) export HPCBURST_CONF_PROBE_HOST=${S[LIVE_PROBE_HOST]}@@    reach-*) export HPCBURST_CONF_PROBE_HOST=${S[LIVE_PROBE_HOST]%%.*}'
  'hack/live-checks.sh@@  lc=${label,,}; hc=${host,,}; hc=${hc%.}@@  lc=$label; hc=$host'
  'hack/live-checks.sh@@[[ $flat_l == *"$flat_h"* ]]@@false'
  'internal/providerctl/conformance/gpuacct_test.go@@	if w.s.startWait > 0 {@@	if false {'
  'internal/providerctl/conformance/gpuacct_test.go@@			cfg.QOS = append(cfg.QOS, fakeslurm.QOS{Name: q, MaxGPUsPerUser: fakeslurm.Cap(1)})@@			cfg.QOS = nil'
  'internal/providerctl/conformance/gpuacct_test.go@@		vars["HPCBURST_CONF_START_WAIT"] = r.StartWait.String()@@		vars["HPCBURST_CONF_PENDING_MAX_SECONDS"] = "60"'
  'internal/providerctl/conformance/gpuacct_test.go@@	return ph == v1alpha1.AllocationRunning || ph == v1alpha1.AllocationEnded@@	return ph == v1alpha1.AllocationEnded'
  'rows.go@@SlotHold: 15 * time.Minute, StartWait: 10 * time.Minute, MostJobs: 1, PostsTo: "#145 and #93"@@SlotHold: 15 * time.Minute, MostJobs: 1, PostsTo: "#145 and #93"'
  'internal/providerctl/conformance/reach_run_test.go@@	if outcome != "reachable" {@@	if outcome == "never" {'
  'internal/providerctl/conformance/reach_run_test.go@@	line, ok := reachLines()[got[0]]@@	line, ok := reachLines()[sumOf(reachLine("reachable", "ssh"))]'
  'internal/providerctl/conformance/testdata/reach-probe.sh@@[ "$rc" -eq 124 ]@@[ "$rc" -eq 125 ]'
  'internal/providerctl/conformance/testdata/reach-probe.sh@@  reasons=$(printf '"'"'%s\n'"'"' "$err" | sed -n '"'"'s/^.*: //p'"'"')@@  reasons=$err'
  'internal/providerctl/conformance/reach_run_test.go@@	l, h := strings.ToLower(label), strings.TrimSuffix(strings.ToLower(host), ".")@@	l, h := label, host'
  'internal/providerctl/conformance/reach_run_test.go@@	w.timeLimit = reachTimeLimit@@	w.timeLimit = ""'
  'internal/providerctl/conformance/reach_run_test.go@@const reachTimeLimit = "2m"@@const reachTimeLimit = "5m"'
  'internal/providerctl/conformance/reach_run_test.go@@			if !bytes.Equal(b, payload) && holds(b) {@@			if false && holds(b) {'
  'internal/providerctl/conformance/reach_run_test.go@@	if plat != nil {@@	if false {'
  'internal/providerctl/conformance/testdata/reach-probe.sh@@  SSH-) echo ssh; exit 0 ;;@@  SSX-) echo ssh; exit 0 ;;'
)
# The mutants of a test of this script: a file with one mutant per line, in place of the list above.
[ -z "${MUTATE_ALPHA_MUTANTS:-}" ] || mapfile -t mutants < "$MUTATE_ALPHA_MUTANTS"
here=$PWD
killed=0 survived=0 broken=0 total=0
for m in "${mutants[@]}"; do
  file=${m%%@@*}; rest=${m#*@@}; from=${rest%%@@*}; to=${rest#*@@}
  path=$(path_of "$file")
  case $path in hack/live-checks.sh) [ "$script_too" = 1 ] || continue ;; esac
  total=$((total+1))
  keep "$path"
  cp "${saved[$path]}" "$path"
  if ! python3 - "$path" "$from" "$to" <<'PY'
import sys
p, a, b = sys.argv[1:4]
s = open(p).read()
if s.count(a) != 1:
    sys.exit(1)
open(p, 'w').write(s.replace(a, b, 1))
PY
  then echo "BROKEN   $file: the text '$from' does not occur exactly once"; broken=$((broken+1)); continue; fi
  case $path in
    *.go|internal/providerctl/*)
      cerr=""
      case $path in
        internal/providerctl/*) compile_tests internal/providerctl/conformance "$tmp/conf.test" && compile_tests "$pkg" "$tmp/pkg.test" ;;
        *) compile_tests "$pkg" "$tmp/pkg.test" ;;
      esac || { echo "BROKEN   $file: $from  ->  $to: the mutant does not compile: $cerr"; broken=$((broken+1)); cp "${saved[$path]}" "$path"; continue; } ;;
  esac
  case $path in
    hack/live-checks.sh) tests=(go test -count=1 -timeout 9m -parallel 3 -run 'AccountingRow|TimeoutIs|AccountingRowsGet|ScriptsShapes|EveryAlphaRowRuns|GH200|Conformance|LateJobOnEvery|OwnersGo|ReadAgain|SlotHold|MostJobs|TunnelLease|LeaseBounds|BuildsBefore|FrontLabel|FrontRan|LabelReaches|Reach|FrontRoles|ATableThat' ./hack/livechecks) ;;
    internal/providerctl/*) tests=(bash -c 'cd "$1/internal/providerctl/conformance" && "$2" -test.count=1 -test.timeout 300s && cd "$1/$3" && "$4" -test.count=1 -test.timeout 300s' _ "$here" "$tmp/conf.test" "$pkg" "$tmp/pkg.test") ;;
    *) tests=(bash -c 'cd "$1/$2" && "$3" -test.count=1 -test.timeout 120s' _ "$here" "$pkg" "$tmp/pkg.test") ;;
  esac
  if "${tests[@]}" >/dev/null 2>&1; then
    echo "SURVIVED $file: $from  ->  $to"; survived=$((survived+1))
  else
    killed=$((killed+1))
  fi
  cp "${saved[$path]}" "$path"
done
echo "mutants: $total, killed $killed, survived $survived, broken $broken"
[ "$survived" -eq 0 ] && [ "$broken" -eq 0 ]
