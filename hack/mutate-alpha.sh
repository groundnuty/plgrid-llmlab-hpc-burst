#!/usr/bin/env bash
# Mutation check of the alpha rows (issue #194): each mutant changes one condition, and the tests of what it
# changes must then fail. A mutant the tests let through is a check that no test depends on.
#
#   hack/mutate-alpha.sh              # every mutant; exit 1 if one survives or does not apply
#   hack/mutate-alpha.sh --no-script  # leave out the mutants of hack/live-checks.sh (the slow ones)
#   hack/mutate-alpha.sh --only <regexp>  # only the mutants whose line matches (egrep), to try a new one; exit 2, and
#                                         # nothing run, for an expression that is not valid or matches no mutant
#
# Before the first mutant of each kind the tests of that kind run with no mutant, under the same command: if they fail, or do
# not compile, the run stops (exit 2), since a test that already fails would count every mutant as killed (#285).
# A mutant ends as KILLED (a test fails and passes with no mutant, and the line names it; or a run ended with no failing test, by what
# the line names, and the same command passes with no mutant), SURVIVED (every test passes) or BROKEN: its text does not occur exactly
# once, the Go file it changes no longer compiles, or what ended the run also does so with no mutant, which is no kill. A mutant that
# does not compile fails to build the tests, and that is not a test failing, so it is reported as BROKEN, never as killed; the
# run fails for a survivor and for a broken mutant alike. The run ends with how many kills each failing test made, so a shared cause shows. The
# compile is its own step, to one fixed test-binary path per package, and the binary then runs: so MUTATE_ALPHA_BUILD_LOCK,
# a lock file, is taken (flock -o) around each compile only, never around a test run.
#
# Five kinds, by the file they change:
#   internal/alpha (a bare file name)           go test ./internal/alpha
#   internal/alpha/alpha_live_test.go           the row's test as the script runs it: an alpha test binary is built from
#                                               the mutated package, and the dry runs of hack/livechecks run that binary
#   internal/providerctl/conformance/...        go test of that package and internal/alpha (the accounting row's half)
#   hack/live-checks.sh                         go test of hack/livechecks, the alpha script tests only. Set
#                                               HPCBURST_LIVECHECKS_ALPHA_TEST and HPCBURST_LIVECHECKS_CONF_TEST to prebuilt
#                                               test binaries to spare it two builds per mutant.
#   internal/rehearsal/...                      go test of internal/rehearsal, the sshd sweep's guard tests only (issue #308):
#                                               they start no sshd and send no signal, every send going to a sink that records it
#
# It edits files in the working tree and restores each from a copy it makes first, also on any exit, so run it
# on a checkout with no uncommitted change to them. Run it under a memory cap (docs/development.md). Each run has an external bound
# (MUTATE_ALPHA_RUN_BOUND, seconds), and what it leaves in its process group is killed before the next. A TERM stops it at once,
# except during a compile, which it lets finish.
set -u -o pipefail
cd "${MUTATE_ALPHA_ROOT:-$(dirname "$0")/..}" || exit 2   # the tree to mutate; the tests of this script point it at a throwaway module
pkg=internal/alpha
script_too=1; [ "${1:-}" != --no-script ] || script_too=0
only=""; [ "${1:-}" != --only ] || only=${2:?--only needs a pattern}
if [ -n "$only" ]; then
  # grep exits 2 for an expression it cannot read: that must not pass for "matches nothing".
  printf '' | grep -E -e "$only" >/dev/null 2>&1; [ $? -le 1 ] || { echo "mutate-alpha: --only: not a valid regular expression: $only" >&2; exit 2; }
fi
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
  'verdict.go@@case j.EndTime.Sub(*j.StartTime) < tm.Hour:@@case j.EndTime.Sub(*j.StartTime) <= tm.Hour:'
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
  'verdict.go@@case d.LeaseLimit <= tm.Lead():@@case d.LeaseLimit < tm.Lead():'
  'verdict.go@@case d.LeaseLimit != row.Lease:@@case d.LeaseLimit < row.Lease:'
  'verdict.go@@if d.MaxRequestDuration != tm.MaxRequestDuration {@@if d.MaxRequestDuration > tm.MaxRequestDuration {'
  'verdict.go@@!d.ReleaseWhenIdle || d.IdleLimit <= 0 || d.IdleLimit > tm.MaxIdleLimit@@d.ReleaseWhenIdle || d.IdleLimit <= 0 || d.IdleLimit > tm.MaxIdleLimit'
  'verdict.go@@d.IdleLimit <= 0 || d.IdleLimit > tm.MaxIdleLimit@@d.IdleLimit <= 0 || d.IdleLimit >= tm.MaxIdleLimit'
  'verdict.go@@case j.TimeLimit != row.Lease:@@case j.TimeLimit == row.Lease:'
  # issue #245: the live steps' observations, ledger, job cleanup and worker fault (pure; the stand-in proof is gated).
  'liveobs.go@@if y.After(at) {@@if y.Before(at) {'
  'liveobs.go@@y = y.AddDate(-1, 0, 0)@@y = y.AddDate(0, 0, 0)'
  'liveobs.go@@if p.Name == string(in.UID) && p.Status == frpsplugin.StatusOnline {@@if p.Name == string(in.UID) {'
  'liveobs.go@@if drainWindow > 0 && drainWindow < w {@@if drainWindow > 0 && drainWindow > w {'
  'liveobs.go@@if drainWindow > 0 && drainWindow < w {@@if drainWindow < w {'
  'liveobs.go@@return in.Status.LeaseEnd.Add(-window), true@@return in.Status.LeaseEnd.Add(window), true'
  'liveobs.go@@s.Complete = rec.Status == http.StatusOK && @@s.Complete = '
  'liveobs.go@@rec.Finish != nil && rec.CutBy == nil@@rec.CutBy == nil'
  'liveobs.go@@rec.CutBy == nil && end.Done@@end.Done'
  'liveobs.go@@end.Done && end.Finished && @@end.Finished && '
  'liveobs.go@@end.Finished && end.ErrorCode == ""@@end.ErrorCode == ""'
  'liveobs.go@@&& end.ErrorCode == ""@@'
  'liveobs.go@@strings.HasPrefix(r.Part, "worker-")@@strings.HasPrefix(r.Part, "")'
  'liveobs.go@@if h.Get("Hpcburst-Request-Id") == "" {@@if false {'
  'liveobs.go@@deploymentIDRE.MatchString(h.Get("Hpcburst-Deployment-Id"))@@h.Get("Hpcburst-Deployment-Id") != ""'
  'liveobs.go@@h.Get("Hpcburst-Instance") != "" && uids@@uids'
  'liveobs.go@@uids[h.Get("Hpcburst-Instance-Uid")]@@true'
  'liveobs.go@@return h.Get("Hpcburst-Provider") != "" && h.Get("Hpcburst-Deployment-Id") != ""@@return true'
  'liveobs.go@@if md.Spec.Providers.Selector != nil {@@if false {'
  'liveobs.go@@rp.Mode == v1alpha1.RetentionReleaseWhenIdle@@rp.Mode != v1alpha1.RetentionReleaseWhenIdle'
  'liveobs.go@@if e.Reason == "HandedOver" {@@if e.Reason != "" {'
  'liveobs.go@@in.Status.Phase == v1alpha1.InstanceEnded && @@'
  'liveobs.go@@in.Status.EndReason == v1alpha1.ReasonDrained {@@true {'
  'liveobs.go@@if c.Reason == v1alpha1.ReasonNoOverlapSlot {@@if c.Reason != "" {'
  'workerfault.go@@return "srun --overlap --jobid="@@return "srun --jobid="'
  'workerfault.go@@		if ended {@@		if false {'
  'workerfault.go@@if err := sc.Err(); err != nil || !ended {@@if err := sc.Err(); err != nil {'
  'workerfault.go@@(f[4] != "0" && f[4] != "1")@@false'
  'workerfault.go@@err4 != nil || pid <= 0@@err4 != nil || pid < 0'
  # #488: the engine whose Apptainer parent is hidden from the job's user (the listing's parent-hidden word, the rule, the shape's word, condition 4).
  'workerfault.go@@	if len(found) != 1 {@@	if len(found) < 1 {'
  'engineshape.go@@	return !listed && p.ParentHidden@@	return !listed'
  'engineshape.go@@	return !listed && p.ParentHidden@@	return p.ParentHidden || (listed && false)'
  'workerfault.go@@if [ "$ppid" -gt 0 ] && ! cat "$proc/$ppid/stat" > /dev/null 2>&1; then hidden=@@if false; then hidden='
  'workerfault.go@@if [ "$ppid" -gt 0 ] && ! cat "$proc/$ppid/stat" > /dev/null 2>&1; then hidden=@@if [ "$ppid" -gt 0 ]; then hidden='
  'workerfault.go@@if [ "$ppid" -gt 0 ] && ! cat@@if [ "$ppid" -ge 0 ] && ! cat'
  'engineshape.go@@		case p.ParentHidden:@@		case p.ParentHidden && false:'
  'workerfault.go@@		if f[8] != "parent-hidden" {@@		if false {'
  'workerfault.go@@strings.Contains(base, "apptainer")@@strings.Contains(base, "x-apptainer")'
  'workerfault.go@@strings.HasPrefix(base, "starter")@@false'
  'workerfault.go@@ok && parent.InJob && isApptainer(parent.Argv)@@ok && isApptainer(parent.Argv)'
  'workerfault.go@@ok && parent.InJob && isApptainer(parent.Argv)@@ok && parent.InJob'
  'workerfault.go@@if len(found) != 1 {@@if len(found) < 1 {'
  'workerfault.go@@== "killed "+strconv.Itoa(p.PID)@@!= "killed "+strconv.Itoa(p.PID)'
  'workerfault.go@@if tr '"'"'\0'"'"' '"'"'\n'"'"' < "$d/environ" 2>/dev/null | grep -qx@@if tr '"'"'\0'"'"' '"'"'\n'"'"' < "$d/environ" 2>/dev/null | grep -q'
  'ledger.go@@if l == nil || l.Finder == nil {@@if l == nil {'
  'ledger.go@@if live == 0 && len(errs) == 0 {@@if live == 0 {'
  'config.go@@	if c.StandIn {@@	if false {'
  # issue #245: the deployment ends that keep the stack rows to their job counts.
  'jobcount.go@@return drainStart.Add(-time.Minute).Truncate(time.Second)@@return drainStart.Truncate(time.Second)'
  'jobcount.go@@end := succDrainStart.Truncate(time.Second)@@end := succDrainStart'
  'jobcount.go@@if !end.After(predDrainStart) {@@if end.Before(predDrainStart) {'
  # issue #245: a row is judged on its own clock (Row.Clock), and the zero Timing is the live one.
  'verdict.go@@case d.LeaseLimit <= tm.Lead():@@case d.LeaseLimit <= Lead():'
  'verdict.go@@if last.Sub(first) < tm.Hour {@@if last.Sub(first) < Hour {'
  'verdict.go@@case j.EndTime.Sub(*j.StartTime) < tm.Hour:@@case j.EndTime.Sub(*j.StartTime) < Hour:'
  'verdict.go@@s.At.Sub(ss[i-1].At) > tm.MaxSampleGap@@s.At.Sub(ss[i-1].At) > MaxSampleGap'
  'verdict.go@@if longestIdle(reqs) < tm.MinIdleGap {@@if longestIdle(reqs) < MinIdleGap {'
  'verdict.go@@d >= tm.NearDeadline && d <= tm.MaxRequestDuration@@d >= NearDeadline && d <= tm.MaxRequestDuration'
  'verdict.go@@case s.Ended.Sub(s.Opened) > tm.MaxRequestDuration:@@case s.Ended.Sub(s.Opened) > MaxRequestDuration:'
  'rows.go@@if *v == 0 {@@if *v != 0 {'
  'rows.go@@fill(&t.MaxRequestDuration, l.MaxRequestDuration)@@fill(&t.MaxRequestDuration, 0)'
  'rows.go@@fill(&t.DrainMargin, l.DrainMargin)@@fill(&t.DrainMargin, 0)'
  'rows.go@@fill(&t.HandoverMargin, l.HandoverMargin)@@fill(&t.HandoverMargin, 0)'
  'rows.go@@fill(&t.StartTimeout, l.StartTimeout)@@fill(&t.StartTimeout, 0)'
  'rows.go@@fill(&t.IdleLimit, l.IdleLimit)@@fill(&t.IdleLimit, 0)'
  'rows.go@@fill(&t.MaxIdleLimit, l.MaxIdleLimit)@@fill(&t.MaxIdleLimit, 0)'
  'rows.go@@fill(&t.TunnelHour, l.TunnelHour)@@fill(&t.TunnelHour, 0)'
  'rows.go@@fill(&t.Hour, l.Hour)@@fill(&t.Hour, 0)'
  'rows.go@@fill(&t.MaxChainGap, l.MaxChainGap)@@fill(&t.MaxChainGap, 0)'
  'rows.go@@fill(&t.NearDeadline, l.NearDeadline)@@fill(&t.NearDeadline, 0)'
  'rows.go@@fill(&t.MinIdleGap, l.MinIdleGap)@@fill(&t.MinIdleGap, 0)'
  'rows.go@@fill(&t.MaxSampleGap, l.MaxSampleGap)@@fill(&t.MaxSampleGap, 0)'
  'rows.go@@t.MinChain = l.MinChain@@t.MinChain = 0'
  'rows.go@@t.ConcurrentStreams = l.ConcurrentStreams@@t.ConcurrentStreams = 0'
  'rows.go@@t.StreamRate = l.StreamRate@@t.StreamRate = 0'
  'rows.go@@IdleLimit: IdleLimit, MaxIdleLimit: 10 * time.Minute@@IdleLimit: IdleLimit, MaxIdleLimit: 11 * time.Minute'
  'rows.go@@d.ReleaseWhenIdle, d.IdleLimit = true, tm.IdleLimit@@d.ReleaseWhenIdle, d.IdleLimit = true, IdleLimit'
  'rows.go@@func MinLease() time.Duration { return Lead() + time.Minute }@@func MinLease() time.Duration { return Lead() }'
  'rows.go@@return MaxRequestDuration + DrainMargin + HandoverMargin + StartTimeout@@return MaxRequestDuration + DrainMargin + HandoverMargin'
  'verdict.go@@case s.Ended.Sub(s.Opened) > tm.MaxRequestDuration:@@case s.Ended.Sub(s.Opened) >= tm.MaxRequestDuration:'
  'verdict.go@@if last.Sub(first) < tm.Hour {@@if last.Sub(first) <= tm.Hour {'
  'verdict.go@@!drainStart.IsZero() && last.After(drainStart)@@!drainStart.IsZero() && !last.Before(drainStart)'
  'verdict.go@@if o.DrainStart.IsZero() {@@if false {'
  'verdict.go@@case s.Opened.IsZero() || s.Ended.IsZero():@@case s.Opened.IsZero() && s.Ended.IsZero():'
  'verdict.go@@if len(reqs) == 0 {@@if len(reqs) < 0 {'
  'verdict.go@@case s.CutBy != "" || s.Error == "stream_cut":@@case s.CutBy != "":'
  'verdict.go@@if len(samples) < 2 {@@if len(samples) < 1 {'
  'verdict.go@@case !s.LastStart.Equal(ss[0].LastStart):@@case false:'
  'verdict.go@@if !s.Connected {@@if s.Connected {'
  'verdict.go@@s.At.Sub(ss[i-1].At) > tm.MaxSampleGap@@s.At.Sub(ss[i-1].At) >= tm.MaxSampleGap'
  'verdict.go@@if ss[0].At.After(first) {@@if false {'
  'verdict.go@@if ss[len(ss)-1].At.Before(last) {@@if false && ss[len(ss)-1].At.Before(last) {'
  'verdict.go@@case s.At.IsZero() || s.LastStart.IsZero():@@case s.At.IsZero() && s.LastStart.IsZero():'
  'verdict.go@@if longestChain(reqs, tm.MaxChainGap) < tm.MinChain {@@if longestChain(reqs, tm.MaxChainGap) <= tm.MinChain {'
  'verdict.go@@d >= tm.NearDeadline && d <= tm.MaxRequestDuration@@d > tm.NearDeadline && d <= tm.MaxRequestDuration'
  'verdict.go@@if concurrentStreams(reqs, tm.StreamRate) < tm.ConcurrentStreams {@@if concurrentStreams(reqs, tm.StreamRate) <= tm.ConcurrentStreams {'
  'verdict.go@@if longestIdle(reqs) < tm.MinIdleGap {@@if longestIdle(reqs) <= tm.MinIdleGap {'
  'verdict.go@@r.TokensPerSecond() >= rate@@r.TokensPerSecond() > rate'
  'verdict.go@@gap >= 0 && gap <= maxGap@@gap > 0 && gap <= maxGap'
  'verdict.go@@gap >= 0 && gap <= maxGap@@gap >= 0 && gap < maxGap'
  'verdict.go@@case !r.UnderLoad:@@case r.UnderLoad:'
  'verdict.go@@case dir == DirDownload && r.Tokens <= 0:@@case dir == DirDownload && r.Tokens < 0:'
  'verdict.go@@case dir == DirDownload && r.Tokens <= 0:@@case r.Tokens <= 0:'
  'verdict.go@@case !found:@@case false && !found:'
  'verdict.go@@if dir != DirUpload && dir != DirDownload {@@if false && dir != DirUpload && dir != DirDownload {'
  'verdict.go@@case largestBody(reqs) < MinLargeBody:@@case largestBody(reqs) <= MinLargeBody:'
  'verdict.go@@	case largeAmongStreams(reqs, tm) == 0:@@	case false:'
  'verdict.go@@if r.BodyBytes >= MinLargeBody {@@if r.BodyBytes > MinLargeBody {'
  'verdict.go@@if open >= tm.ConcurrentStreams {@@if open > tm.ConcurrentStreams {'
  'verdict.go@@		if streamsOpenDuring(reqs, l.BodyStart, l.BodyEnd, tm) {@@		if streamsOpenDuring(reqs, l.Opened, l.Ended, tm) {'
  'verdict.go@@r.Opened.After(from) && r.Opened.Before(to)@@r.Opened.After(from) && !r.Opened.After(to)'
  'verdict.go@@	at := []time.Time{from}@@	at := []time.Time{}'
  'verdict.go@@		if streamsOpenDuring(reqs, l.SendStart, l.SendEnd, tm) {@@		if streamsOpenDuring(reqs, l.Opened, l.Ended, tm) {'
  'verdict.go@@if !l.UploadRecorded() {@@if false {'
  'observe.go@@return !s.BodyStart.IsZero() && s.BodyEnd.After(s.BodyStart)@@return !s.BodyStart.IsZero()'
  'observe.go@@return !s.BodyStart.IsZero() && s.BodyEnd.After(s.BodyStart)@@return s.BodyEnd.After(s.BodyStart)'
  'observe.go@@return !s.BodyStart.IsZero() && s.BodyEnd.After(s.BodyStart)@@return !s.BodyStart.IsZero() && !s.BodyEnd.IsZero()'
  'verdict.go@@isStream(r, tm.StreamRate) && !r.Opened.After(t) && r.Ended.After(t)@@isStream(r, tm.StreamRate) && !r.Opened.After(t) && !r.Ended.Before(t)'
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
  'run.go@@InsertFrontLine(post, cfg.FrontLabel)@@post'
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
  'hack/live-checks.sh@@HPCBURST_ALPHA_RUN_ID=$RUN_ID
  export_front_label "$n"@@HPCBURST_ALPHA_RUN_ID=$RUN_ID'
  'hack/live-checks.sh@@gpu-accounting-athena
gpu-accounting-athena-now@@gpu-accounting-athena-now'
  'hack/live-checks.sh@@gpu-accounting-athena-now
# END FRONT LABEL@@# END FRONT LABEL'
  'hack/live-checks.sh@@gpu-accounting-athena-now|LIVE_OWNER_GO_ATHENA_NOW|20
# END OWNER GO@@# END OWNER GO'
  'hack/live-checks.sh@@gpu-accounting-athena-now|athena|athena-now|plgrid-now|-|plgrid-now|@@gpu-accounting-athena-now|athena|athena-now|plgrid-now|-|-|'
  'hack/live-checks.sh@@gpu-accounting-athena-now|athena|athena-now|plgrid-now|-|plgrid-now|ACCOUNT_ATHENA_A100|1|16|125Gi|00:05:00|32|-|10@@gpu-accounting-athena-now|athena|athena-now|plgrid-now|-|plgrid-now|ACCOUNT_ATHENA_A100|1|16|125Gi|00:05:00|32|-|15'
  'hack/live-checks.sh@@gpu-accounting-athena-now|athena|athena-now|@@gpu-accounting-athena-now|athena|athena|'
  'rows.go@@return lease - (MaxRequestDuration + DrainMargin) - TunnelHour@@return lease - (MaxRequestDuration + DrainMargin) + TunnelHour'
  'rows.go@@if measured < ShortLeaseRule {@@if measured <= ShortLeaseRule {'
  'rows.go@@lease < MinLease() || lease > MaxTunnelLease@@lease <= MinLease() || lease > MaxTunnelLease'
  'rows.go@@lease < MinLease() || lease > MaxTunnelLease@@lease < MinLease() || lease >= MaxTunnelLease'
  'rows.go@@lease%time.Minute != 0@@false'
  'rows.go@@r.SlotHold = lease@@r.SlotHold = r.SlotHold'
  'config.go@@(strings.HasPrefix(row.Name, "tunnel-hour-") && getenv(envDry) != "1")@@false'  'hack/live-checks.sh@@gpu-accounting-helios|helios|helios|plgrid-gpu-gh200|now|now|ACCOUNT_HELIOS_GH200|1|72|117Gi|00:05:00|32|-|10@@gpu-accounting-helios|helios|helios|plgrid-gpu-gh200|now|now|ACCOUNT_HELIOS_GH200|1|72|117Gi|00:05:00|32|-'
  'hack/live-checks.sh@@gpu-accounting-helios|helios|helios|plgrid-gpu-gh200|now|now|@@gpu-accounting-helios|helios|helios|plgrid-gpu-gh200|now|-|'
  'hack/live-checks.sh@@      [ "${SWAIT[$n]:--}" = - ] || wait=0@@      true'
  'hack/live-checks.sh@@    gpu-accounting-*|reach-*) [ "${SWAIT[$n]:--}" != - ] || export HPCBURST_CONF_PENDING_MAX_SECONDS@@    gpu-accounting-*|reach-*) export HPCBURST_CONF_PENDING_MAX_SECONDS'
  'hack/live-checks.sh@@reach-helios|helios|helios|plgrid-gpu-gh200|now|now|ACCOUNT_HELIOS_GH200|1|1|1Gi|00:02:00|20|-C memfs|10@@reach-helios|helios|helios|plgrid-gpu-gh200|now|now|ACCOUNT_HELIOS_GH200|1|1|1Gi|00:02:00|20|-C memfs'
  'hack/live-checks.sh@@reach-athena|athena|athena|plgrid-now|-|plgrid-now|@@reach-athena|athena|athena|plgrid-now|-|-|'
  'hack/live-checks.sh@@    reach-*) export HPCBURST_CONF_PROBE_HOST=${S[LIVE_PROBE_HOST]}@@    reach-*) export HPCBURST_CONF_PROBE_HOST=${S[LIVE_PROBE_HOST]%%.*}'
  'hack/live-checks.sh@@  lc=${label,,}; hc=${host,,}; hc=${hc%.}@@  lc=$label; hc=$host'
  'hack/live-checks.sh@@[[ $flat_l == *"$flat_h"* ]]@@false'
  'internal/providerctl/conformance/startwait_test.go@@	if w.s.startWait > 0 {@@	if false {'
  'internal/providerctl/conformance/gpuacct_test.go@@			cfg.QOS = append(cfg.QOS, fakeslurm.QOS{Name: q, MaxGPUsPerUser: fakeslurm.Cap(1)})@@			cfg.QOS = nil'
  'internal/providerctl/conformance/gpuacct_test.go@@		vars["HPCBURST_CONF_START_WAIT"] = r.StartWait.String()@@		vars["HPCBURST_CONF_PENDING_MAX_SECONDS"] = "60"'
  'internal/providerctl/conformance/gpuacct_test.go@@	return ph == v1alpha1.AllocationRunning || ph == v1alpha1.AllocationEnded@@	return ph == v1alpha1.AllocationEnded'
  # The review of PR 255 (B1-B8): each blocking fix has a mutant that a test kills.
  # B1: the kill goes through a pidfd, after the process at the PID is checked, and needs one.
  'workerfault.go@@        pidfd_send_signal(fd, signal.SIGKILL)@@        os.kill(pid, signal.SIGKILL)'
  'workerfault.go@@        if not same(pid, start, cmdline):@@        if False:'
  'workerfault.go@@    if got != pid:@@    if False:'
  'workerfault.go@@command -v python3 >/dev/null 2>&1 || { echo no-pidfd; exit 0; }@@true'
  # B2: the engine is the launch's program, directly or as a Python console script, with exactly its arguments.
  'workerfault.go@@	if !isProgram(rest[0], command[0]) {@@	if false {'
  'workerfault.go@@		if len(rest) == 0 || !strings.HasPrefix(rest[0], "/") || !isProgram(rest[0], command[0]) {@@		if len(rest) == 0 {'
  'workerfault.go@@	return arg == program || (strings.HasPrefix(arg, "/") && filepath.Base(arg) == program)@@	return arg == program || filepath.Base(arg) == program'
  'workerfault.go@@	if len(rest) != len(command) {@@	if len(rest) < len(command) {'
  'workerfault.go@@		if rest[i] != command[i] {@@		if false {'
  'workerfault.go@@	pyFlagsRE = regexp.MustCompile(`^-[sSuEIBO]+$`)@@	pyFlagsRE = regexp.MustCompile(`^-`)'
  # nit: a process of the job by its cgroup too.
  'workerfault.go@@  if grep -q "/job_$job/" "$d/cgroup" 2>/dev/null; then injob=1; fi@@  :'
  'workerfault.go@@  if grep -q "/job_$job/" "$d/cgroup"@@  if grep -q "/job_$job" "$d/cgroup"'
  # B3: the run's jobs are its prefix's, whatever the recorder holds.
  'ledger.go@@		if prefix != "" && strings.HasPrefix(name, prefix) {@@		if false {'
  'hack/live-checks.sh@@      { [ -n "$OWN_PREFIX" ] && [ "${name#"$OWN_PREFIX"}" != "$name" ]; }; then echo "$id"; fi@@      false; then echo "$id"; fi'
  'hack/live-checks.sh@@ HPCBURST_ALPHA_RUN_ID=$RUN_ID@@'
  # B4: a job is alive by the scheduler's state, COMPLETING included.
  'ledger.go@@	return j.Completing || !endedBase(j)@@	return !endedBase(j)'
  'ledger.go@@	return len(j.State) > 0 && endedStates[strings.ToUpper(j.State[0])]@@	return len(j.State) > 0'
  'ledger.go@@		if !endedBase(j) {@@		if false {'
  # Issue #224 in the live tunnel hour, alpha's reading (the ruling (4) on PR 255): arrival shown by the refusal,
  # the upload not measured on live rows only, head-of-line by the client's windows.
  'liveobs.go@@	return max((n-fixed+1+len(" hello")-1)/len(" hello"), 1)@@	return max((n-fixed)/len(" hello"), 1)'
  'liveobs.go@@	if w2 <= w1 || n2 <= n1 || (n2-n1)%(w2-w1) != 0 {@@	if w2 <= w1 || n2 <= n1 {'
  'liveobs.go@@	if l.Fixed < 0 {@@	if false {'
  'liveobs.go@@		if p[1] != l.Tokens(p[0]) {@@		if false {'
  'liveobs.go@@	if len(pts) < 3 {@@	if len(pts) < 2 {'
  'liveobs.go@@		if p[0] <= prev {@@		if false {'
  'liveobs.go@@var CalibrationWords = []int{1000, 2000, 4000}@@var CalibrationWords = []int{1000, 2000}'
  'liveobs.go@@	words := min(WordsForBytes(model, bytes, ArrivalMaxTokens), line.MaxWords(maxModelLen))@@	words := WordsForBytes(model, bytes, ArrivalMaxTokens)'
  'liveobs.go@@	if len(body) < MinLargeBody {@@	if false {'
  'liveobs.go@@	return body, line.Tokens(words), nil@@	return body, words, nil'
  'liveobs.go@@		n, err := count(w)
		if err != nil {
			return "", 0, err
		}@@		n, _ := count(w)'
  'liveobs.go@@func (l TokenLine) MaxWords(maxModelLen int) int { return (maxModelLen - 1 - l.Fixed) / l.Per }@@func (l TokenLine) MaxWords(maxModelLen int) int { return (maxModelLen - l.Fixed) / l.Per }'
  'verdict.go@@		if r.BodyBytes < MinLargeBody {@@		if true {'
  # The delta round of PR 255. B1: a large body not answered with a completed response counting its prompt fails the
  # row, on a line of its own; a fourth calibration prompt, half the large prompt's words, must lie on the line.
  'liveobs.go@@func NearScaleWords(words int) int { return (words + 1) / 2 }@@func NearScaleWords(words int) int { return 4001 }'
  'liveobs.go@@	if _, err := Calibrate(append(pts, [2]int{near, n})...); err != nil {@@	if _, err := Calibrate(append(pts, [2]int{near, n})...); err != nil && false {'
  'liveobs.go@@	n, err := count(near)
	if err != nil {
		return "", 0, err
	}@@	n, _ := count(near)'
  'liveobs.go@@	s.ArrivedTokens, _ = PromptTokensOf(reply)@@'
  'observe.go@@func (s Stream) PromptCounted() bool { return s.ArrivedTokens > 0 }@@func (s Stream) PromptCounted() bool { return true }'
  'verdict.go@@		if otherwise {@@		if otherwise && false {'
  'verdict.go@@			case !l.ArrivalVerified():@@			case false:'
  # B2, after the parent's reading of vLLM v0.32.0: an exact count only, from a completed response's usage, every
  # choice finished, the usage complete and adding up; each prompt asks for one token.
  'liveobs.go@@const ArrivalMaxTokens = 1@@const ArrivalMaxTokens = 32768'
  'liveobs.go@@ || r.Object != "chat.completion" || len(r.Choices) == 0 || @@ || len(r.Choices) == 0 || '
  'liveobs.go@@ || len(r.Choices) == 0 || r.Usage == nil {@@ || r.Usage == nil {'
  'liveobs.go@@ || r.Usage == nil {@@ {'
  'liveobs.go@@		if c.FinishReason == nil || (*c.FinishReason != "stop" && *c.FinishReason != "length") {@@		if c.FinishReason == nil {'
  'liveobs.go@@		if c.FinishReason == nil || (*c.FinishReason != "stop" && *c.FinishReason != "length") {@@		if c.FinishReason == nil && false {'
  'liveobs.go@@	if u.PromptTokens == nil || u.CompletionTokens == nil || u.TotalTokens == nil {@@	if u.PromptTokens == nil {'
  'liveobs.go@@	if *u.PromptTokens <= 0 || *u.TotalTokens != *u.PromptTokens+*u.CompletionTokens {@@	if *u.TotalTokens != *u.PromptTokens+*u.CompletionTokens {'
  'liveobs.go@@	if *u.PromptTokens <= 0 || *u.TotalTokens != *u.PromptTokens+*u.CompletionTokens {@@	if *u.PromptTokens <= 0 {'
  # B3: the request near the deadline is a completed stream.
  'verdict.go@@r.Complete && d >= tm.NearDeadline@@d >= tm.NearDeadline'
  'observe.go@@return s.ExpectedTokens > 0 && s.ArrivedTokens == s.ExpectedTokens@@return s.ArrivedTokens == s.ExpectedTokens'
  'observe.go@@return s.ExpectedTokens > 0 && s.ArrivedTokens == s.ExpectedTokens@@return s.ExpectedTokens > 0 && s.ArrivedTokens >= s.ExpectedTokens'
  'liveobs.go@@func overlaps(from, to, a, b time.Time) bool { return from.Before(b) && to.After(a) }@@func overlaps(from, to, a, b time.Time) bool { return from.Before(b) }'
  'liveobs.go@@func overlaps(from, to, a, b time.Time) bool { return from.Before(b) && to.After(a) }@@func overlaps(from, to, a, b time.Time) bool { return to.After(a) }'
  # The science agent's addition: each gap classed by the large requests' send and prefill windows, send first.
  'liveobs.go@@	if w.Sent.IsZero() || w.Sent.Before(w.Start) || w.Sent.After(w.End) {@@	if w.Sent.IsZero() {'
  'liveobs.go@@				prefill = prefill || overlaps(from, to, s, w.End)@@				prefill = prefill'
  'liveobs.go@@			case send:@@			case send && !prefill:'
  'liveobs.go@@	h.InFlight = max(h.Send, h.Prefill)@@	h.InFlight = h.Send'
  'liveobs.go@@const HeadOfLineStall = time.Second@@const HeadOfLineStall = 2 * time.Second'
  'verdict.go@@	live := o.UploadNotMeasured != ""@@	live := false'
  'verdict.go@@		case !found && dir == DirUpload && uploadNotMeasured:@@		case false:'
  'verdict.go@@		if largeInFlightAmongStreams(reqs, tm) == 0 {@@		if false {'
  'verdict.go@@		if l.SendStart.IsZero() || !l.SendEnd.After(l.SendStart) {@@		if false {'
  'verdict.go@@		case !s.Complete && !s.ArrivalVerified():@@		case !s.Complete:'
  'verdict.go@@		if h.Send > HeadOfLineStall {@@		if h.Send >= HeadOfLineStall {'
  'verdict.go@@		if h.Send > HeadOfLineStall {@@		if false {'
  'verdict.go@@		if h.Prefill > HeadOfLineStall {@@		if h.Prefill >= HeadOfLineStall {'
  'verdict.go@@		if h.Prefill > HeadOfLineStall {@@		if false {'
  'verdict.go@@		if h.Elsewhere > HeadOfLineStall {@@		if h.Elsewhere >= HeadOfLineStall {'
  'verdict.go@@		if h.Elsewhere > HeadOfLineStall {@@		if false {'
  'posts.go@@		case r == nil && dir == DirUpload && o.UploadNotMeasured != "":@@		case false:'
  'rows.go@@	if t.UploadBody == 0 {@@	if false {'
  'rows.go@@	if t.UploadStreams == 0 {@@	if false {'
  'rows.go@@	fill(&t.UploadPause, l.UploadPause)@@'
  'rows.go@@UploadBody: UploadBody, UploadStreams: UploadStreams, UploadPause: UploadPause}@@UploadBody: UploadBody, UploadStreams: UploadStreams}'
  # B5: the two-job guard.
  'jobcount.go@@	if e := l.Clock.Add(g.TimeLimit - g.DrainWindow).Truncate(time.Second); e.After(g.end) {@@	if e := l.Clock.Add(g.TimeLimit).Truncate(time.Second); e.After(g.end) {'
  'jobcount.go@@e.After(g.end) {@@true {'
  'jobcount.go@@	case len(others) > 0 && (g.final || len(others) > 1):@@	case len(others) > 0 && g.final:'
  'jobcount.go@@		if j.HardEnd == nil || j.HardEnd.Sub(*j.StartTime) != g.TimeLimit {@@		if j.HardEnd == nil {'
  'jobcount.go@@	return j.StartTime != nil && (j.EndTime == nil || j.EndTime.After(*j.StartTime))@@	return j.StartTime != nil'
  'jobcount.go@@	if g.pred == "" && len(started) > 0 {@@	if len(started) > 0 {'
  'jobcount.go@@	return clusterNow.Add(timeLimit - drainWindow).Truncate(time.Second)@@	return clusterNow.Add(timeLimit).Truncate(time.Second)'
  # B6: the guard's failures fail the step, and the run ends with the successor's drain start observed.
  'guard_live_test.go@@				if fails++; fails >= guardLookFails {@@				if fails++; false {'
  'guard_live_test.go@@				_, err = setEnd(end)@@				_ = end'
  'guard_live_test.go@@	stack.WaitFor(r.t, d, what, guarded)@@	stack.WaitFor(r.t, d, what, cond); _ = guarded'
  'guard_live_test.go@@			if md.Spec.EndTime == nil || !md.Spec.EndTime.Time.Equal(end) {@@			if false {'
  # B7: the first clock, and a tunnel row's NOT RUN at its first end.
  'jobcount.go@@		if c := timeLimit - drainWindow - time.Minute; c < pendingMax {@@		if c := timeLimit - drainWindow; c < pendingMax {'
  'jobcount.go@@		if c := timeLimit - drainWindow - time.Minute; c < pendingMax {@@		if c := timeLimit - drainWindow - time.Minute; c <= pendingMax {'
  'run.go@@		if ns.PastEnd {@@		if false {'
  'hack/live-checks.sh@@    elif first_end_marker; then@@    elif false; then'
  # #272: a job that never started within its bound is NOT RUN on every row. One mutant per bound sends it to FAIL (exit 1);
  # each is killed by a row with no slot as well as by the slot rows, since the no-slot rows are what the ruling changed.
  # The wait in the queue (the first clock), then a tunnel row's deployment end (both the sentinel and the error that
  # carries it), then a start wait (the conformance tests), then the rule by row that #230 had, which comes back:
  'run.go@@	return errors.Is(err, ErrNotStarted) || errors.Is(err, ErrQueueWaitPastEnd)@@	return errors.Is(err, ErrQueueWaitPastEnd)'
  'run.go@@	return errors.Is(err, ErrNotStarted) || errors.Is(err, ErrQueueWaitPastEnd)@@	return errors.Is(err, ErrNotStarted)'
  'run.go@@func NotRun(_ Row, err error) bool { return neverStarted(err) }@@func NotRun(row Row, err error) bool { return row.SlotHold > 0 && neverStarted(err) }'
  'run.go@@func NotRun(_ Row, err error) bool { return neverStarted(err) }@@func NotRun(row Row, err error) bool { return row.MostJobs == 1 && neverStarted(err) }'
  'jobcount.go@@	return target == ErrNotStarted || (e.PastEnd && target == ErrQueueWaitPastEnd)@@	return (target == ErrNotStarted && !e.PastEnd) || (e.PastEnd && target == ErrQueueWaitPastEnd && false)'
  'internal/providerctl/conformance/startwait_test.go@@	w.t.Skipf("%s", skip)@@	if w.s.startWait > 0 { w.t.Fatalf("%s", skip) }; w.t.Skipf("%s", skip)'
  'internal/providerctl/conformance/startwait_test.go@@	w.t.Skipf("%s", skip)@@	if w.s.startWait == 0 { w.t.Fatalf("%s", skip) }; w.t.Skipf("%s", skip)'
  'internal/providerctl/conformance/startwait_test.go@@	return w.s.pendingMax, "the wait in the queue"@@	return maxStartWait, "the wait in the queue"'
  'hack/live-checks.sh@@  [ -n "${HOST[$n]:-}" ] || return 1@@  [ "${SLOT[$n]:--}" != - ] || return 1'
  'hack/live-checks.sh@@  [ -n "${HOST[$n]:-}" ] || return 1@@  [ "${SWAIT[$n]:--}" != - ] || return 1'
  'hack/live-checks.sh@@  [ -n "${HOST[$n]:-}" ] || return 1@@  [ "${SWAIT[$n]:--}" = - ] || [ "${SLOT[$n]:--}" != - ] || return 1'
  'hack/live-checks.sh@@  [ -n "${HOST[$n]:-}" ] || return 1@@  [ "${SWAIT[$n]:--}" = - ] || return 1'
  'hack/live-checks.sh@@  [ -n "${HOST[$n]:-}" ] || return 1@@  true'
  'hack/live-checks.sh@@  ! first_end_marker || case $n in tunnel-hour-*) true ;; *) false ;; esac@@  ! first_end_marker || case $n in tunnel-hour-*) false ;; *) false ;; esac'
  'hack/live-checks.sh@@  ! first_end_marker || case $n in tunnel-hour-*) true ;; *) false ;; esac@@  true'
  'hack/live-checks.sh@@  [ "$CLEAN_LEFT" -eq 0 ] || verdict=1@@  [ "$CLEAN_LEFT" -eq 0 ] || [ "$NOT_RUN" = 1 ] || verdict=1'
  'hack/live-checks.sh@@  [ "$verdict" -ne 0 ] || [ "$NOT_RUN" != 1 ] || verdict=3@@  [ "$verdict" -ne 0 ] || [ "$NOT_RUN" != 1 ] || verdict=1'
  'hack/live-checks.sh@@conformance-helios|helios|helios|plgrid|-|-|ACCOUNT_HELIOS_CPU|0|1|1Gi|00:05:00|97|-|30@@conformance-helios|helios|helios|plgrid|-|-|ACCOUNT_HELIOS_CPU|0|1|1Gi|00:05:00|97|-'
  'hack/live-checks.sh@@conformance-helios|helios|helios|plgrid|-|-|ACCOUNT_HELIOS_CPU|0|1|1Gi|00:05:00|97|-|30@@conformance-helios|helios|helios|plgrid|-|-|ACCOUNT_HELIOS_CPU|0|1|1Gi|00:05:00|97|-|20'
  'hack/live-checks.sh@@conformance-helios|helios|helios|plgrid|-|-|ACCOUNT_HELIOS_CPU|0|1|1Gi|00:05:00|97|-|30@@conformance-helios|helios|helios|plgrid|-|-|ACCOUNT_HELIOS_CPU|0|1|1Gi|00:05:00|90|-|30'
  # The review of PR 284. B1, B2 and B4: a job that started, or that a read cannot tell about, is never NOT RUN.
  'internal/providerctl/conformance/startwait_test.go@@			accountingNotFailed(w.t, last) // no job starts@@			_ = last // no job starts'
  'internal/providerctl/conformance/startwait_test.go@@		if why := w.whyNotWaiting(last, acceptedID); why != "" {@@		if why := ""; why != "" {'
  'internal/providerctl/conformance/startwait_test.go@@	if o.StartTime != nil || o.Phase == v1alpha1.AllocationRunning || o.Phase == v1alpha1.AllocationEnded {@@	if false {'
  'internal/providerctl/conformance/startwait_test.go@@	if o.StartTime != nil || o.Phase == v1alpha1.AllocationRunning || o.Phase == v1alpha1.AllocationEnded {@@	if o.Phase == v1alpha1.AllocationRunning || o.Phase == v1alpha1.AllocationEnded {'
  'internal/providerctl/conformance/startwait_test.go@@	if o.StartTime != nil || o.Phase == v1alpha1.AllocationRunning || o.Phase == v1alpha1.AllocationEnded {@@	if o.StartTime != nil || o.Phase == v1alpha1.AllocationEnded {'
  'internal/providerctl/conformance/startwait_test.go@@	if o.StartTime != nil || o.Phase == v1alpha1.AllocationRunning || o.Phase == v1alpha1.AllocationEnded {@@	if o.StartTime != nil || o.Phase == v1alpha1.AllocationRunning {'
  'internal/providerctl/conformance/startwait_test.go@@		return fmt.Sprintf("its state could not be read: %v", err)@@		return ""'
  'internal/providerctl/conformance/startwait_test.go@@		return "the queue does not list it"@@		return ""'
  'internal/providerctl/conformance/startwait_test.go@@	case !alpha.WaitingToStart(j):@@	case !alpha.WaitingToStart(j) && false:'
  'internal/providerctl/conformance/startwait_test.go@@		return "its Allocation could not be read"@@		return ""'
  'internal/providerctl/conformance/startwait_test.go@@		return "there is no platform to read it on"@@		return ""'
  'internal/providerctl/conformance/startwait_test.go@@	return a.Annotations[v1alpha1.AnnotationJobID]@@	return acceptedID'
  'internal/providerctl/conformance/startwait_test.go@@	if acceptedID != "" {@@	if false {'
  'jobcount.go@@	return j.StartTime == nil && j.EndTime == nil && !j.Requeued && (j.Phase == v1alpha1.AllocationPending || j.Phase == v1alpha1.AllocationHeld)@@	return j.EndTime == nil && !j.Requeued && (j.Phase == v1alpha1.AllocationPending || j.Phase == v1alpha1.AllocationHeld)'
  'jobcount.go@@	return j.StartTime == nil && j.EndTime == nil && !j.Requeued && (j.Phase == v1alpha1.AllocationPending || j.Phase == v1alpha1.AllocationHeld)@@	return j.StartTime == nil && !j.Requeued && (j.Phase == v1alpha1.AllocationPending || j.Phase == v1alpha1.AllocationHeld)'
  'jobcount.go@@	return j.StartTime == nil && j.EndTime == nil && !j.Requeued && (j.Phase == v1alpha1.AllocationPending || j.Phase == v1alpha1.AllocationHeld)@@	return j.StartTime == nil && j.EndTime == nil && (j.Phase == v1alpha1.AllocationPending || j.Phase == v1alpha1.AllocationHeld)'
  'jobcount.go@@	return j.StartTime == nil && j.EndTime == nil && !j.Requeued && (j.Phase == v1alpha1.AllocationPending || j.Phase == v1alpha1.AllocationHeld)@@	return j.StartTime == nil && j.EndTime == nil && !j.Requeued && (j.Phase == v1alpha1.AllocationPending || j.Phase == v1alpha1.AllocationHeld || true)'
  'jobcount.go@@	return j.StartTime == nil && j.EndTime == nil && !j.Requeued && (j.Phase == v1alpha1.AllocationPending || j.Phase == v1alpha1.AllocationHeld)@@	return j.StartTime == nil && j.EndTime == nil && !j.Requeued && j.Phase == v1alpha1.AllocationPending'
  'jobcount.go@@	return j.StartTime == nil && j.EndTime == nil && !j.Requeued && (j.Phase == v1alpha1.AllocationPending || j.Phase == v1alpha1.AllocationHeld)@@	return j.StartTime == nil && j.EndTime == nil && !j.Requeued && j.Phase == v1alpha1.AllocationHeld'
  'steps_live_test.go@@			if st.StartTime != nil {@@			if false {'
  'steps_live_test.go@@			case v1alpha1.AllocationRunning, v1alpha1.AllocationEnded, v1alpha1.AllocationFailed, v1alpha1.AllocationLost, v1alpha1.AllocationRefused:@@			case v1alpha1.AllocationEnded, v1alpha1.AllocationFailed, v1alpha1.AllocationLost, v1alpha1.AllocationRefused:'
  'steps_live_test.go@@			case v1alpha1.AllocationRunning, v1alpha1.AllocationEnded, v1alpha1.AllocationFailed, v1alpha1.AllocationLost, v1alpha1.AllocationRefused:@@			case v1alpha1.AllocationRunning, v1alpha1.AllocationFailed, v1alpha1.AllocationLost, v1alpha1.AllocationRefused:'
  'steps_live_test.go@@			case v1alpha1.AllocationRunning, v1alpha1.AllocationEnded, v1alpha1.AllocationFailed, v1alpha1.AllocationLost, v1alpha1.AllocationRefused:@@			case v1alpha1.AllocationRunning, v1alpha1.AllocationEnded, v1alpha1.AllocationLost, v1alpha1.AllocationRefused:'
  'steps_live_test.go@@			case v1alpha1.AllocationRunning, v1alpha1.AllocationEnded, v1alpha1.AllocationFailed, v1alpha1.AllocationLost, v1alpha1.AllocationRefused:@@			case v1alpha1.AllocationRunning, v1alpha1.AllocationEnded, v1alpha1.AllocationFailed, v1alpha1.AllocationRefused:'
  'steps_live_test.go@@			case v1alpha1.AllocationRunning, v1alpha1.AllocationEnded, v1alpha1.AllocationFailed, v1alpha1.AllocationLost, v1alpha1.AllocationRefused:@@			case v1alpha1.AllocationRunning, v1alpha1.AllocationEnded, v1alpha1.AllocationFailed, v1alpha1.AllocationLost:'
  'steps_live_test.go@@			if err := r.firstJobWaiting(); err != nil {@@			if err := error(nil); err != nil {'
  'steps_live_test.go@@		return fmt.Errorf("the queue could not be read: %w", err)@@		return nil'
  'steps_live_test.go@@		return errors.New("the queue lists no job of the run")@@		return nil'
  'steps_live_test.go@@		if !WaitingToStart(j) {@@		if false {'
  'hack/live-checks.sh@@    if [ "$state" = PENDING ] && [ -z "$run_t" ] && [ $((now - submit_t)) -gt "$pend_max" ]; then@@    if [ -z "$run_t" ] && [ $((now - submit_t)) -gt "$pend_max" ]; then'
  'hack/live-checks.sh@@    if [ "$state" = PENDING ] && [ -z "$run_t" ] && [ $((now - submit_t)) -gt "$pend_max" ]; then@@    if [ "$state" = PENDING ] && [ $((now - submit_t)) -gt "$pend_max" ]; then'
  'hack/live-checks.sh@@  printf '"'"'%s\n'"'"' "${state:-error}"@@  printf '"'"'%s\n'"'"' "$state"'
  # B3: the first job ran and the second never started is NOT RUN unless an earlier failure stands; its own bound.
  'hack/live-checks.sh@@    if second_job_marker; then@@    if false; then'
  'hack/live-checks.sh@@  ! second_job_marker || [ "${MOSTJOBS[$n]:-1}" -ge 2 ] || return 1@@  true'
  'hack/live-checks.sh@@second_job_marker() { grep -q "the first job ran and the second never started"@@second_job_marker() { grep -q "the second job ran and the first never started"'
  'hack/live-checks.sh@@    alpha-helios-nooverlap) printf "within %s of the first'"'"'s lease end" "$(fmt_pending "$(pending_max "$1")")" ;;@@    alpha-helios-nooverlap) printf "within 30 min of the first'"'"'s lease end" ;;'
  'hack/live-checks.sh@@    *) printf "by the first'"'"'s drain start" ;;@@    *) printf "within its bound" ;;'
  'guard_live_test.go@@			return r.secondNeverFinal()@@			return ErrSecondNotStarted'
  'guard_live_test.go@@		return fmt.Errorf("the successor'"'"'s state could not be read: %w", err)@@		return ErrSecondNotStarted'
  'guard_live_test.go@@	_, final, err := r.guard.Next(l)@@	_, final, _ := r.guard.Next(l)'
  'guard_live_test.go@@		return errors.New("the successor has started, but only after the wait for it, so the run'"'"'s deployment never ended at its drain start in time")@@		return ErrSecondNotStarted'
  'guard_live_test.go@@	if r.guard.pred == "" {@@	if false {'
  'guard_live_test.go@@		if !WaitingToStart(j) {@@		if false {'
  'guard_live_test.go@@	if waiting == 0 {@@	if false {'
  'run.go@@	if second != nil && len(failures) == 0 {@@	if second != nil {'
  'run.go@@	if second != nil && len(failures) == 0 {@@	if false {'
  'run.go@@func stopsTheRun(err error) bool { return err != nil && !errors.Is(err, ErrSecondNotStarted) }@@func stopsTheRun(err error) bool { return err != nil }'
  'run.go@@				return handoverFirstJobChecks(cfg.Row, o, j)@@				return VerifyHandover(cfg.Row, o, j)'
  'run.go@@				return noOverlapFirstJobChecks(cfg.Row, o, j)@@				return VerifyNoOverlap(cfg.Row, o, j)'
  'run.go@@ || errors.Is(err, ErrSecondNotStarted)@@'
  'run.go@@	if errors.Is(err, ErrSecondNotStarted) {@@	if false {'
  'liveobs.go@@		if e.Reason == v1alpha1.ReasonReplacementLate && e.Regarding.Name == deployment {@@		if e.Reason == v1alpha1.ReasonReplacementLate {'
  'liveobs.go@@		if e.Reason == v1alpha1.ReasonReplacementLate && e.Regarding.Name == deployment {@@		if e.Regarding.Name == deployment {'
  'guard_live_test.go@@		return LateUnknown@@		return LateNotRaised'
  'hack/live-checks.sh@@      [ -z "$late" ] || say "$n: ReplacementLate for its deployment: $late (information, not a check)"@@      :'
  'steps.go@@func (e *SecondNotStartedError) Is(target error) bool { return target == ErrSecondNotStarted }@@func (e *SecondNotStartedError) Is(target error) bool { return target == ErrSecondNotStarted && e.ReplacementLate != LateRaised }'
  # The delta review of PR 284. L2: the successor's bound, from a fresh read of the scheduler.
  'guard_live_test.go@@		if err != nil || l.Clock.Before(startBy) {@@		if err != nil {'
  'guard_live_test.go@@			if j.Name != predTag && jobStarted(j) {@@			if false && j.Name != predTag && jobStarted(j) {'
  'guard_live_test.go@@			if j.Name != predTag && jobStarted(j) {@@			if jobStarted(j) {'
  'guard_live_test.go@@				started = true@@				_ = started'
  'guard_live_test.go@@		if started || now.Before(startBy) || now.Before(next) {@@		if started || now.Before(next) {'
  'steps_live_test.go@@		if stop != nil && stop() {@@		if false {'
  # L3: a job read as waiting that had started by its cancel is a failure.
  'steps_live_test.go@@			r.claimed = true@@			_ = r.claimed'
  'guard_live_test.go@@	r.claimed, r.claimedPred = true, r.guard.pred@@	_ = r.claimed'
  'guard_live_test.go@@	r.claimed, r.claimedPred = true, r.guard.pred@@	r.claimed = true'
  'steps_live_test.go@@		if j.ID != r.claimedPred && jobStarted(j) {@@		if false && j.ID != r.claimedPred && jobStarted(j) {'
  'steps_live_test.go@@			if r.claimed {@@			if true {'
  'steps_live_test.go@@		return fmt.Errorf("the run read a job as one that never started, and its history could not be read after its cancel: %w", err)@@		return nil'
  'internal/providerctl/conformance/startwait_test.go@@	case alpha.JobStarted(j):@@	case false && alpha.JobStarted(j):'
  'internal/providerctl/conformance/startwait_test.go@@		w.t.Fatalf("%s: %s of %s ran out, and the job'"'"'s record could not be read after its cancel: %v", step, boundName, bound, err)@@		_ = err'
  # L4: ReplacementLate stays information, on a failure too, and no read of what is not judged can fail the run.
  'run.go@@		return s.ReplacementLate@@		return LateUnknown'
  'run.go@@ the first job'"'"'s\n- ReplacementLate for its deployment: " + lateOf(second) + " (information, not a check)\n"@@ the first job'"'"'s\n"'
  'live_flows_test.go@@	if second != nil {@@	if false {'
  # T1: the stack's start is in the sum; a new wait in the live steps fails, even with the old ones in place.
  'budget.go@@	StartupWait = stack.UpTimeout + 2*stack.WaitDeadline@@	StartupWait = stack.UpTimeout + stack.WaitDeadline'
  'budget.go@@	BudgetFactorPercent = 125@@	BudgetFactorPercent = 100'
  'budget.go@@	return b + Scaled(r.WorstCase(pendingMax)-b)@@	return b + r.WorstCase(pendingMax) - b'
  'budget.go@@		first += pendingMax@@		_ = first'
  'hack/live-checks.sh@@alpha-helios-nooverlap|helios|helios|plgrid-gpu-gh200|now|now|ACCOUNT_HELIOS_GH200|1|72|120000M|01:46:00|213|-C memfs@@alpha-helios-nooverlap|helios|helios|plgrid-gpu-gh200|now|now|ACCOUNT_HELIOS_GH200|1|72|120000M|01:46:00|210|-C memfs'
  'hack/live-checks.sh@@gpu-accounting-helios|helios|helios|plgrid-gpu-gh200|now|now|ACCOUNT_HELIOS_GH200|1|72|117Gi|00:05:00|32|-|10@@gpu-accounting-helios|helios|helios|plgrid-gpu-gh200|now|now|ACCOUNT_HELIOS_GH200|1|72|117Gi|00:05:00|30|-|10'
  'budget.go@@	TunnelRunOver = 29 * time.Minute@@	TunnelRunOver = 28 * time.Minute'
  'budget.go@@		return StartupWait + first + (r.Lease - dw) + tm.MaxRequestDuration + dw + EndMargin + lastLooks + 2*stack.CatchUpWorstCase + CloseTimeout@@		return first + (r.Lease - dw) + tm.MaxRequestDuration + dw + EndMargin + lastLooks + 2*stack.CatchUpWorstCase + CloseTimeout'
  'budget.go@@		return StartupWait + first + (r.Lease - dw) + tm.MaxRequestDuration + dw + EndMargin + lastLooks + 2*stack.CatchUpWorstCase + CloseTimeout@@		return StartupWait + first + (r.Lease - dw) + tm.MaxRequestDuration + dw + EndMargin + lastLooks + CloseTimeout'
  'budget.go@@		return StartupWait + first + r.Lease + SuccessorWait(tm, pendingMax) + lastLooks + 2*stack.CatchUpWorstCase + CloseTimeout@@		return first + r.Lease + SuccessorWait(tm, pendingMax) + lastLooks + 2*stack.CatchUpWorstCase + CloseTimeout'
  'budget.go@@		return StartupWait + first + r.Lease + SuccessorWait(tm, pendingMax) + lastLooks + 2*stack.CatchUpWorstCase + CloseTimeout@@		return StartupWait + first + r.Lease + lastLooks + 2*stack.CatchUpWorstCase + CloseTimeout'
  'budget.go@@	return StartupWait + first + (r.Lease - dw) + dw + EndMargin + stack.CatchUpWorstCase + CloseTimeout@@	return first + (r.Lease - dw) + dw + EndMargin + stack.CatchUpWorstCase + CloseTimeout'
  'budget.go@@	return StartupWait + first + (r.Lease - dw) + dw + EndMargin + stack.CatchUpWorstCase + CloseTimeout@@	return StartupWait + first + (r.Lease - dw) + dw + EndMargin + CloseTimeout'
  'budget.go@@		return StartupWait + first + r.Lease + SuccessorWait(tm, pendingMax) + lastLooks + 2*stack.CatchUpWorstCase + CloseTimeout@@		return StartupWait + first + r.Lease + SuccessorWait(tm, pendingMax) + lastLooks + CloseTimeout'
  'live_flows_test.go@@	if err := r.syncWatch(ctx, stack.KindAllocations, "the run'"'"'s Allocations"); err != nil {@@	if err := r.syncWatch(ctx, stack.KindAllocations, "the run'"'"'s Allocations"); err != nil { _ = err; }; if err := r.syncWatch(ctx, stack.KindAllocations, "the run'"'"'s Allocations"); err != nil {'
  'steps_live_test.go@@			r.watch.ResetsDecideNothing() // this run ends@@			// this run ends'
  # PR #317's delta review: the stand-ins' stop check accepts a snapshot of the stack that lacks a component, and its look at what is
  # left after down ignores the time the clean-ups after it keep. TestAStandInWhoseStackDidNotStopFails kills both (a status that omits
  # the controller; no time left before the deadline), and TestTheStopCheckKeepsTheCleanupsReserve the second.
  'standin_stop_test.go@@if missing := stack.MissingComponents(s.procs); len(missing) > 0 {@@if missing := stack.MissingComponents(s.procs); false && len(missing) > 0 {'
  'standin_stop_test.go@@if last := deadline.Add(-stopReserve); hasDeadline && last.Before(by) {@@if last := deadline.Add(-stopReserve); false && hasDeadline && last.Before(by) {'
  'guard_live_test.go@@	r.claimed, r.claimedPred = true, r.guard.pred@@	r.watch.ResetsDecideNothing(); r.claimed, r.claimedPred = true, r.guard.pred'
  'steps.go@@	return fmt.Errorf("%w; %v", err, second)@@	return fmt.Errorf("%w; %w", err, second)'
  'steps.go@@	return fmt.Errorf("%w; %v", err, second)@@	return err'
  'steps.go@@	if second == nil {@@	if second != nil {'
  'live_flows_test.go@@"the handover'"'"'s result"); err != nil {@@"the handover'"'"'s result"); err != nil && second == nil {'
  'live_flows_test.go@@"the no-overlap row'"'"'s result"); err != nil {@@"the no-overlap row'"'"'s result"); err != nil && second == nil {'
  'rows.go@@	r.Budget = lease + TunnelRunOver@@	_ = r.Budget'
  'live_flows_test.go@@	r.untilBefore(ds, lead, "the stream across the drain start")@@	r.waitFor(time.Hour, "a wait no sum holds", func() bool { return true }); r.untilBefore(ds, lead, "the stream across the drain start")'
  'live_flows_test.go@@r.successorOf(pre.Name, time.Until(ds)+LeadTimesFor(lead).SuccessorWaitEnds, nil)@@r.successorOf(pre.Name, time.Until(ds), nil)'
  # #447: the handover streams' lead sized to the deployment's max-model-len (handoverlead.go): the minimum, the floor (dropped, and inclusive), the
  # rounding down, the prompt's reserve, the successor wait's half lead, the NOT RUN, the context the live steps read, and the script's marker.
  'handoverlead.go@@	return min(maxRequest/3, fit), nil@@	_ = fit; return maxRequest / 3, nil'
  'handoverlead.go@@	if secs < LeadFloor.Seconds() {@@	if secs <= LeadFloor.Seconds() {'
  'handoverlead.go@@	if secs < LeadFloor.Seconds() {@@	if secs < 0 {'
  'handoverlead.go@@func wholeSeconds(secs float64) time.Duration { return time.Duration(secs) * time.Second }@@func wholeSeconds(secs float64) time.Duration { return time.Duration(math.Round(secs)) * time.Second }'
  'handoverlead.go@@const PromptReserve = 512@@const PromptReserve = 256'
  'handoverlead.go@@SuccessorWaitEnds: -lead - lead/2@@SuccessorWaitEnds: -lead'
  'run.go@@errors.Is(err, ErrSlotTaken) || errors.Is(err, ErrLeadTooShort)@@errors.Is(err, ErrSlotTaken)'
  'live_flows_test.go@@return FitLead(r.tm.MaxRequestDuration, r.maxModelLenOrDefault(), rate)@@return FitLead(r.tm.MaxRequestDuration, 32768, rate)'
  'hack/live-checks.sh@@  ! lead_marker || case $n in alpha-athena-handover|alpha-helios-nooverlap|alpha-athena-now-nooverlap) true ;; *) false ;; esac || return 1@@  true'
  'hack/live-checks.sh@@    elif lead_marker; then@@    elif false; then'
  'hack/live-checks.sh@@at [0-9]+ tokens/s with max-model-len@@at .+ tokens/s with max-model-len'
  # #451: the tunnel hour's long stream sized to the context (longstream.go): the cap, the floor (dropped, and inclusive), the cap's formula,
  # the share check 2 asks of a shorter plan, the verdict reading the plan, the NOT RUN, the post's line, and the script's marker.
  'longstream.go@@	if c := tm.LongCap(); secs >= c.Seconds() {@@	if c := tm.LongCap(); false && secs >= c.Seconds() {'
  'longstream.go@@	if secs < tm.LongFloor.Seconds() {@@	if secs < 0 {'
  'longstream.go@@	if secs < tm.LongFloor.Seconds() {@@	if secs <= tm.LongFloor.Seconds() {'
  'longstream.go@@	return t.NearDeadline + (t.MaxRequestDuration-t.NearDeadline)/2@@	return t.MaxRequestDuration'
  # #451 (ruling (a)): check 2 is judged on the long requests only, by role; and the lead's floor judges the FINAL lead.
  'verdict.go@@r.Long && r.Complete && d >= tm.NearDeadline@@r.Complete && d >= tm.NearDeadline'
  'handoverlead.go@@	if lead < LeadFloor {@@	if fit < LeadFloor {'
  'longstream.go@@	t.NearDeadline = max(share, t.LongBarFloor())@@	_ = share; t.NearDeadline = p.Length'
  'longstream.go@@	t.NearDeadline = max(share, t.LongBarFloor())@@	t.NearDeadline = share'
  'longstream.go@@Seconds()*t.NearDeadline.Seconds()/t.LongCap().Seconds()*1000))@@Seconds()*1000))'
  'rows.go@@	LongFloor = 135 * time.Second@@	LongFloor = 2 * time.Minute'
  'verdict.go@@trafficChecks(o.Requests, tm.withLongPlan(o.LongPlan), live)@@trafficChecks(o.Requests, tm, live)'
  'run.go@@ || errors.Is(err, ErrLeadTooShort) || errors.Is(err, ErrLongTooShort)@@ || errors.Is(err, ErrLeadTooShort)'
  'posts.go@@	b.WriteString(longPlanLine(o, tm))@@	b.WriteString("")'
  'hack/live-checks.sh@@  ! long_marker || case $n in tunnel-hour-*) true ;; *) false ;; esac || return 1@@  true'
  'hack/live-checks.sh@@    elif long_marker; then@@    elif false; then'
  'hack/live-checks.sh@@the stream that fits is [0-9hms.]+@@the stream that fits is .+'
  'hack/live-checks.sh@@at [0-9]+ tokens/s with max-model-len [0-9]+ the stream that fits is@@at .+ tokens/s with max-model-len [0-9]+ the stream that fits is'
  'hack/live-checks.sh@@        TMIN[$name]=$((v + 29))@@        TMIN[$name]=$((v + 28))'
  'hack/live-checks.sh@@tunnel-hour-helios|helios|helios|plgrid-gpu-gh200|now|now|ACCOUNT_HELIOS_GH200|1|72|120000M|02:17:00|166|-C memfs@@tunnel-hour-helios|helios|helios|plgrid-gpu-gh200|now|now|ACCOUNT_HELIOS_GH200|1|72|120000M|02:17:00|165|-C memfs'
  'hack/live-checks.sh@@tunnel-hour-athena|athena|athena|plgrid-gpu-a100|-|-|ACCOUNT_ATHENA_A100|1|16|125Gi|02:17:00|166|-@@tunnel-hour-athena|athena|athena|plgrid-gpu-a100|-|-|ACCOUNT_ATHENA_A100|1|16|125Gi|02:17:00|165|-'
  # T2: the source-derived sum sees a plain sleep and a repeated call.
  'internal/providerctl/conformance/conformance_test.go@@	name := "conformance-held-submit-and-cancel-on-delete"@@	time.Sleep(10 * time.Minute); name := "conformance-held-submit-and-cancel-on-delete"'
  'internal/providerctl/conformance/conformance_test.go@@	w.jobNameRoundTrip()@@	w.jobNameRoundTrip(); w.jobNameRoundTrip()'
  # B5 and B6: the sums. Each is compared with a figure worked out by hand, so a longer wait fails a test, not a run.
  'internal/providerctl/conformance/jobname_test.go@@w.batch, req, 36, func()@@w.batch, req, 72, func()'
  'internal/providerctl/conformance/conformance_test.go@@w.until(lostName, "the adopted job ends", 8*time.Minute,@@w.until(lostName, "the adopted job ends", 9*time.Minute,'
  'internal/providerctl/conformance/conformance_test.go@@	deadline := time.Now().Add(5 * time.Minute)@@	deadline := time.Now().Add(6 * time.Minute)'
  'internal/providerctl/conformance/conformance_test.go@@	w.endedInAccounting(id2, 3*time.Minute)@@	w.endedInAccounting(id2, 4*time.Minute)'
  'internal/providerctl/conformance/startwait_test.go@@const duplicateWait = 2 * time.Minute@@const duplicateWait = 3 * time.Minute'
  'internal/providerctl/conformance/gpuacct_test.go@@	accountingEndWait    = 10 * time.Minute@@	accountingEndWait    = 20 * time.Minute'
  'internal/providerctl/conformance/gpuacct_test.go@@	accountingDeleteWait = 5 * time.Minute@@	accountingDeleteWait = 15 * time.Minute'
  'internal/providerctl/conformance/reach_run_test.go@@const reachEndWait = 3 * time.Minute@@const reachEndWait = 10 * time.Minute'
  'hack/live-checks.sh@@local submit_t run_t="" now limit_s pend_max margin=${LIVE_RUN_MARGIN_SECONDS:-900}@@local submit_t run_t="" now limit_s pend_max margin=${LIVE_RUN_MARGIN_SECONDS:-1200}'
  'hack/live-checks.sh@@real-engine|helios|-|plgrid-gpu-gh200|now|now|ACCOUNT_HELIOS_GH200|1|72|120000M|00:57:00|79|-C memfs@@real-engine|helios|-|plgrid-gpu-gh200|now|now|ACCOUNT_HELIOS_GH200|1|72|120000M|00:57:00|75|-C memfs'
  'hack/live-checks.sh@@conformance-helios-gh200|helios|helios|plgrid-gpu-gh200|now|now|ACCOUNT_HELIOS_GH200|1|1|1Gi|00:05:00|57|-C memfs|10@@conformance-helios-gh200|helios|helios|plgrid-gpu-gh200|now|now|ACCOUNT_HELIOS_GH200|1|1|1Gi|00:05:00|50|-C memfs|10'
  'hack/live-checks.sh@@conformance-athena|athena|athena|plgrid-now|-|plgrid-now|ACCOUNT_ATHENA_A100|1|4|32Gi|00:05:00|57|-|10@@conformance-athena|athena|athena|plgrid-now|-|plgrid-now|ACCOUNT_ATHENA_A100|1|4|32Gi|00:05:00|50|-|10'
  # #503: row A sends no requeue; its worst case is the default formula with the events read last and the worker restart's and the deployment's
  # end's catch-ups, and its NOT RUN is the warm start alone.
  'budget.go@@(r.Lease - dw) + stack.WaitDeadline + dw@@(r.Lease - dw) + dw'
  'budget.go@@stack.WaitDeadline + dw + EndMargin + 2*stack.CatchUpWorstCase@@stack.WaitDeadline + dw + EndMargin + stack.CatchUpWorstCase'
  'run.go@@		refused = nowServeNotRun(o)@@		refused = nil'
  # #503: the restart ladder's climb is named by its rung in row A's verdict (LadderOf reads it, ladderChecks names it).
  'nowserve.go@@	case l.RestartLoop:@@	case false && l.RestartLoop:'
  'nowserve.go@@	case l.InstanceRestarts > 0:@@	case false && l.InstanceRestarts > 0:'
  'nowserve.go@@	f := ladderChecks(o.Ladder)@@	f := []string(nil)'
  'liveobs.go@@			case r.Part == v1alpha1.PartInstance:@@			case false:'
  'liveobs.go@@		if in.Status.Phase == v1alpha1.InstanceEnded && in.Status.EndReason == v1alpha1.ReasonRestartLoop {@@		if in.Status.EndReason == v1alpha1.ReasonRestartLoop {'
  'now_flows_test.go@@	o.Ladder = LadderOf(r.watch.Last())@@	o.Ladder = LadderOf(nil)'
  'budget.go@@	CloseTimeout = 5 * time.Minute@@	CloseTimeout = 4 * time.Minute'
  'budget.go@@	ReadyMargin = 5 * time.Minute@@	ReadyMargin = 4 * time.Minute'
  'budget.go@@	EndMargin = 5 * time.Minute@@	EndMargin = 4 * time.Minute'
  'budget.go@@	GuardEvery       = 10 * time.Second@@	GuardEvery       = 5 * time.Second'
  'budget.go@@	GuardResultLooks = 6@@	GuardResultLooks = 3'
  'budget.go@@	return pendingMax + tm.StartTimeout + ReadyMargin@@	return tm.StartTimeout + ReadyMargin'
  'budget.go@@	return time.Duration(r.MostJobs)*pendingMax + r.Budget@@	return pendingMax + r.Budget'
  'budget.go@@	first, _ := FirstClock(r, pendingMax, r.Lease, dw)@@	first := pendingMax'
  'dry_test.go@@	d.tick(bound - time.Second)@@	d.tick(bound - time.Minute)'
  'alpha_live_test.go@@		if merr := rec.SaveText(NotRunFile, note); merr != nil {@@		if merr := error(nil); merr != nil {'
  'run.go@@			if neverStarted(err) {@@			if false {'
  'alpha_live_test.go@@	if NotRun(cfg.Row, err) {@@	if false {'
  'hack/live-checks.sh@@PENDING_MAX_NOW=1800@@PENDING_MAX_NOW=7200'
  'hack/live-checks.sh@@  elif [ "${SLOT[$1]:--}" != - ]; then printf '"'"'%s'"'"' "$PENDING_MAX_NOW"@@  elif false; then printf '"'"'%s'"'"' "$PENDING_MAX_NOW"'
  'hack/live-checks.sh@@  if [ -n "${LIVE_PENDING_MAX_SECONDS:-}" ]; then printf '"'"'%s'"'"' "$LIVE_PENDING_MAX_SECONDS"@@  if false; then printf '"'"'%s'"'"' "$LIVE_PENDING_MAX_SECONDS"'
  'hack/live-checks.sh@@  export HPCBURST_ALPHA_PENDING_MAX_SECONDS=$(pending_max "$n")@@  export HPCBURST_ALPHA_PENDING_MAX_SECONDS=${LIVE_PENDING_MAX_SECONDS:-7200}'
  'hack/live-checks.sh@@"$(( ${TMIN[$n]} + ${MOSTJOBS[$n]:-1} * (($(pending_max "$n") + 59) / 60) ))" env_alpha@@"$(( ${TMIN[$n]} + ${MOSTJOBS[$n]:-1} * ${LIVE_PENDING_MAX_SECONDS:-7200} / 60 ))" env_alpha'
  'hack/live-checks.sh@@"$(( ${TMIN[$n]} + ${MOSTJOBS[$n]:-1} * (($(pending_max "$n") + 59) / 60) ))" env_alpha@@"$(( ${TMIN[$n]} + ${MOSTJOBS[$n]:-1} * ($(pending_max "$n") / 60) ))" env_alpha'
  'hack/live-checks.sh@@"$(( ${TMIN[$n]} + ${MOSTJOBS[$n]:-1} * (($(pending_max "$n") + 59) / 60) ))" env_alpha@@"$(( ${TMIN[$n]} + (($(pending_max "$n") + 59) / 60) ))" env_alpha'
  'hack/live-checks.sh@@may pend up to $(fmt_pending "$(pending_max "$1")") in the queue@@may pend up to $((7200 / 60)) min in the queue'
  'hack/live-checks.sh@@was still pending after $(fmt_pending "$(pending_max "$n")") in the queue@@was still pending after 30 min in the queue'
  'hack/live-checks.sh@@  say "$1: may pend up to $(fmt_pending "$(pending_max "$1")") in the queue (then it is cancelled by its tag and the run is NOT RUN), then holds QOS@@  say "$1: holds QOS'
  'hack/live-checks.sh@@elif [ "$1" -lt 60 ]; then printf '"'"'%d s'"'"' "$1"@@elif false; then printf '"'"'%d s'"'"' "$1"'
  'hack/live-checks.sh@@else printf '"'"'%d min %d s'"'"' $(($1 / 60)) $(($1 % 60)); fi@@else printf '"'"'%d min'"'"' $(($1 / 60)); fi'
  'hack/live-checks.sh@@if [ $(($1 % 60)) -eq 0 ]; then printf '"'"'%d min'"'"' $(($1 / 60))@@if [ $(($1 % 60)) -ge 0 ]; then printf '"'"'%d min'"'"' $(($1 / 60))'
  'hack/live-checks.sh@@  acct=${S[${ACCTKEY[$n]}]}; pend_max=$(pending_max "$n")@@  acct=${S[${ACCTKEY[$n]}]}; pend_max=${LIVE_PENDING_MAX_SECONDS:-7200}'
  'hack/live-checks.sh@@[ $((now - submit_t)) -gt "$pend_max" ]@@[ $((now - submit_t)) -gt $((pend_max + 60)) ]'
  'hack/live-checks.sh@@      NOT_RUN=1@@      :'
  # #297: the history of real-engine's job pending at its bound, read after the cleanup's cancel (never started, started, unreadable, and the exits).
  'hack/live-checks.sh@@NO_VAL_START=4294967294@@NO_VAL_START=4294967295'
  'hack/live-checks.sh@@if [ "$start" = "$NO_VAL_START" ]; then@@if false; then'
  'hack/live-checks.sh@@if [ "$state" != CANCELLED ]; then@@if false; then'
  'hack/live-checks.sh@@v=$(real_engine_history_verdict "$host" "$RE_PENDING_JOB")@@v=never'
  'hack/live-checks.sh@@echo "started $((10#$start))"@@echo never'
  'hack/live-checks.sh@@[ "$((10#$end))" -le "$((10#$start))" ]@@[ "$((10#$end))" -lt "$((10#$start))" ]'
  'hack/live-checks.sh@@a started job is never NOT RUN)"; return 1 ;;@@a started job is never NOT RUN)"; return 0 ;;'
  'hack/live-checks.sh@@if [ "$rc" != 0 ]; then v="unclear sacct failed"@@if false; then v="unclear sacct failed"'
  'hack/live-checks.sh@@and j["job_id"] == job]) != 1:@@and j["job_id"] >= 0]) != 1:'
  'hack/live-checks.sh@@if type(errs) is not list or len(errs) != 0:@@if type(errs) is not list or len(errs) < 0:'
  'hack/live-checks.sh@@case $v in never|started\ *) break ;; esac@@case $v in never) break ;; esac'
  'hack/live-checks.sh@@${v#unclear })"; return 1 ;;@@${v#unclear })"; return 0 ;;'
  'hack/live-checks.sh@@if [ "$CLEAN_LEFT" -ne 0 ]; then@@if false; then'
  'hack/live-checks.sh@@settle_real_engine_not_run "$n" "$host" || verdict=1; fi@@settle_real_engine_not_run "$n" "$host" || true; fi'
  'hack/live-checks.sh@@a job was still pending after $(fmt_pending "$RE_PENDING_BOUND") in the queue@@a job was still pending after 30 min in the queue'
  # #300 round (review of PR 300): the restart count (B1), the decode of the reply (B2), python3, and the cleanup that could not read the queue.
  'hack/live-checks.sh@@if [ "$restart" != 0 ]; then@@if false; then'
  'hack/live-checks.sh@@    rc = "-"@@    rc = 0'
  'hack/live-checks.sh@@or p["job_id"] != 0:@@or p["job_id"] < 0:'
  'hack/live-checks.sh@@if len(jobs) != 1:@@if len(jobs) < 1:'
  'hack/live-checks.sh@@object_pairs_hook=keys_once, @@'
  'hack/live-checks.sh@@except Exception:@@except KeyError:'
  'hack/live-checks.sh@@if rec.get("steps", []) != []:@@if rec.get("steps", []) == 0:'
  'hack/live-checks.sh@@|| { echo "unclear the reply could not be decoded (python3 did not run it)"; return; }@@|| :'
  'hack/live-checks.sh@@if [[ $out =~ $HISTORY_UNCLEAR_RE ]]; then echo "$out"; return; fi@@if false; then echo "$out"; return; fi'
  'hack/live-checks.sh@@command -v "${LIVE_PYTHON:-python3}" >/dev/null 2>&1 ||@@true ||'
  'hack/live-checks.sh@@if [ "$CLEAN_SEEN" -ne 0 ]; then@@if true; then'
  'hack/live-checks.sh@@[ "$left" -eq 0 ] || { CLEAN_LEFT=1; CLEAN_SEEN=1; }@@[ "$left" -eq 0 ] || { CLEAN_LEFT=1; }'
  'hack/live-checks.sh@@gpu-accounting-*|reach-*) [ "${SWAIT[$n]:--}" != - ] || export HPCBURST_CONF_PENDING_MAX_SECONDS=$(pending_max "$n")@@gpu-accounting-*|reach-*) [ "${SWAIT[$n]:--}" != - ] || export HPCBURST_CONF_PENDING_MAX_SECONDS=7200'
  'hack/live-checks.sh@@conformance-athena|athena|athena|plgrid-now|-|plgrid-now|ACCOUNT_ATHENA_A100|1|4|32Gi|00:05:00|57|-|10@@conformance-athena|athena|athena|plgrid-now|-|plgrid-now|ACCOUNT_ATHENA_A100|1|4|32Gi|00:05:00|57|-'
  'hack/live-checks.sh@@reach-athena|athena|athena|plgrid-now|-|plgrid-now|ACCOUNT_ATHENA_A100|1|1|1Gi|00:02:00|20|-|10@@reach-athena|athena|athena|plgrid-now|-|plgrid-now|ACCOUNT_ATHENA_A100|1|1|1Gi|00:02:00|20|-|15'
  'config.go@@cfg.PendingMax = time.Minute@@cfg.PendingMax = 2 * time.Minute'
  'config.go@@if v := getenv(envPending); v != "" {@@if v := getenv(envPending); v == "x" {'
  'config.go@@return time.Duration(s) * time.Second, nil@@return time.Duration(s+60) * time.Second, nil'
  'config.go@@if err != nil || s <= 0 {@@if err != nil || s < 0 {'
  'dry_test.go@@d.tick(2 * time.Second)@@d.tick(61 * time.Second)'
  'dry_test.go@@d.out(n), d.out(n), d.gateWrap(n))@@d.out(n), d.out(n), "sleep 5")'
  'rows.go@@SlotHold: 15 * time.Minute, StartWait: 10 * time.Minute, MostJobs: 1, PostsTo: "#145"@@SlotHold: 15 * time.Minute, MostJobs: 1, PostsTo: "#145"'
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
  'config.go@@if getenv(envAddr) != "" {@@if getenv(envAddr) == "x" {'
  'config.go@@case "host-dialed":@@case "host-dialedX":'
  'config.go@@cfg.DialHomeAddr, cfg.DialHomeKey = need(envAddr), need(envKey)@@cfg.DialHomeAddr, cfg.DialHomeKey = "", need(envKey)'
  'config.go@@cfg.DialHomeAddr, cfg.DialHomeKey = need(envAddr), need(envKey)@@cfg.DialHomeAddr, cfg.DialHomeKey = need(envAddr), getenv(envKey)'
  'config.go@@case "", "direct":@@case "direct":'
  'config.go@@cfg.DialHomeAddr = need(envAddr)@@cfg.DialHomeAddr = getenv(envAddr)'
  'config.go@@bad = append(bad, envMode+" must be direct, ssh or host-dialed")@@bad = append(bad, envMode+" must be direct or ssh")'
  'hack/live-checks.sh@@host-dialed) host_dialed=1 ;;@@host-dialed) ;;'
  'hack/live-checks.sh@@LIVE_DIALHOME_MODE must be direct, ssh or host-dialed"; bad=1 ;;@@LIVE_DIALHOME_MODE must be direct or ssh"; bad=1 ;;'
  'hack/live-checks.sh@@LIVE_DIALHOME_MODE must be direct, ssh or host-dialed"; bad=1 ;;@@LIVE_DIALHOME_MODE must be direct, ssh or host-dialed" ;;'
  'hack/live-checks.sh@@[ -z "${S[LIVE_DIALHOME_ADDR]:-}" ] || {@@true || {'
  'hack/live-checks.sh@@so the mode has no address"; bad=1; }@@so the mode has no address"; }'
  'hack/live-checks.sh@@if [ "$host_dialed" = 1 ]; then@@if false; then'
  'hack/live-checks.sh@@if [ "$host_dialed" = 1 ]; then@@if true; then'
  'hack/live-checks.sh@@unset HPCBURST_ALPHA_QOS HPCBURST_ALPHA_DIALHOME_ADDR HPCBURST_ALPHA_DIALHOME_KEY@@unset HPCBURST_ALPHA_QOS HPCBURST_ALPHA_DIALHOME_KEY'
  'hack/live-checks.sh@@[ "${S[LIVE_DIALHOME_MODE]}" = host-dialed ] || export HPCBURST_ALPHA_DIALHOME_ADDR@@[ "${S[LIVE_DIALHOME_MODE]}" = host-dialedX ] || export HPCBURST_ALPHA_DIALHOME_ADDR'
  'hack/live-checks.sh@@export HPCBURST_ALPHA_DIALHOME_MODE=${S[LIVE_DIALHOME_MODE]}@@export HPCBURST_ALPHA_DIALHOME_MODE=direct'
  # #285 (the ruling on PR 292): the live steps report their watch's real resets. A stand-in run has none, and compared the post with the same call.
  'steps_live_test.go@@func (r *liveRun) WatchResets() int { return r.watch.ResetCount() }@@func (r *liveRun) WatchResets() int { return 0 }'
  # #285, PR 302's review: the same method, as a constant that is not 0 and as another run's Recorder (the previous run's), which a test whose cases
  # all expect one count cannot tell from the right one (it holds the previous call's count method, bound to that run's Recorder, not the Recorder
  # itself: a copy of the watch field is a use TestWatchInventory cannot class, and would be the kill instead of the behaviour); and Run's count read at
  # its start, which loses every reset that comes after (resetsAtStart is the type in watch_resets_test.go that holds the count read then).
  'steps_live_test.go@@func (r *liveRun) WatchResets() int { return r.watch.ResetCount() }@@func (r *liveRun) WatchResets() int { return 3 }'
  'steps_live_test.go@@func (r *liveRun) WatchResets() int { return r.watch.ResetCount() }@@var prevCount func() int; func (r *liveRun) WatchResets() int { c := prevCount; prevCount = r.watch.ResetCount; if c == nil { c = r.watch.ResetCount }; return c() }'
  'run.go@@plan := Plan{Row: cfg.Row, Rec: rec, PendingMax: cfg.PendingMax, Logf: logf}@@st = resetsAtStart{st, st.WatchResets()}; plan := Plan{Row: cfg.Row, Rec: rec, PendingMax: cfg.PendingMax, Logf: logf}'
  # #305: the reach-host-dialed rows' rules: the verdict's checks (both sessions, srun -u, the rendezvous, the preamble, the epoch,
  # the 256 byte values each way, a state that cannot be read) and the outcome, the probe and the responder that make the bytes, the
  # body's waits, and the script's rows, their builds and environment, and the reach rows' refusal in host-dialed mode.
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@w[0] != "srun" || w[1] != "-u" ||@@w[0] != "srun" ||'
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@w[2] != "--overlap" || w[3] != "--jobid="+o.JobID ||@@w[2] != "--overlap" ||'
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@		if byChannel[string(ch)] == 0 {@@		if byChannel[string(ch)] < 0 {'
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@regexp.MustCompile(`^hb-relay-[0-9]+$`)@@regexp.MustCompile(`^hb-relay-.*$`)'
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@if ctl.Sessions < 1 || srv.Sessions < 1 {@@if srv.Sessions < 1 {'
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@if lineArg(lineWords(l), "--epoch") != strconv.FormatInt(o.Epoch, 10) {@@if false {'
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@if len(x.Received) == 256 && x.InPlace && sha256Hex(x.Received) == ascendingSHA256 {@@if len(x.Received) == 256 && x.InPlace {'
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@if len(x.Received) == 256 && x.InPlace && sha256Hex(x.Received) == ascendingSHA256 {@@if len(x.Received) == 256 && sha256Hex(x.Received) == ascendingSHA256 {'
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@case !p.Connected || p.GotBytes != 256 || p.GotSHA256 != descendingSHA256:@@case !p.Connected || p.GotBytes != 256:'
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@case o.Registered < 1:@@case o.Registered < 0:'
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@case srv.Sessions < 1 || srv.Joined < 1 || len(o.Exchanges) == 0:@@case srv.Sessions < 1:'
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@case o.ProbeErr != "":@@case false:'
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@			verdict = "FAIL"@@			verdict = "PASS"'
  'internal/providerctl/conformance/hostdialed_run_test.go@@		t.Fatalf("%s failed: %s", row.Name, strings.Join(failed, "; "))@@		t.Logf("%s failed: %s", row.Name, strings.Join(failed, "; "))'
  'internal/providerctl/conformance/hostdialed_run_test.go@@hostDialedEndWait = 2 * time.Minute@@hostDialedEndWait = 9 * time.Minute'
  'internal/providerctl/conformance/hostdialed_run_test.go@@hostDialedSessionWait = 3 * time.Minute@@hostDialedSessionWait = 1 * time.Minute'
  'internal/providerctl/conformance/hostdialed_rig_test.go@@		if int(buf[i]) != i {@@		if int(buf[i]) < 0 {'
  'internal/providerctl/conformance/hostdialed_probe_test.go@@	if i != len(lines) {@@	if false {'
  'internal/providerctl/conformance/testdata/serving-probe.sh@@cut -c1-64@@cut -c1-63'
  'internal/providerctl/conformance/testdata/serving-probe.sh@@echo "socket-dir-mode=${mode:-unknown}"@@echo "socket-dir-mode=700"'
  'internal/providerctl/conformance/testdata/serving-probe.sh@@while [ "$i" -lt 256 ]; do@@while [ "$i" -lt 255 ]; do'
  'hack/live-checks.sh@@          reach-*) reach_is_host_dialed || check_reach_settings || bad=1 ;;@@          reach-*) check_reach_settings || bad=1 ;;'
  'hack/live-checks.sh@@      if reach_is_host_dialed; then@@      if false; then'
  'hack/live-checks.sh@@reach_is_host_dialed() { [ "${S[LIVE_DIALHOME_MODE]:-}" = host-dialed ]; }@@reach_is_host_dialed() { [ "${S[LIVE_DIALHOME_MODE]:-}" = direct ]; }'
  'hack/live-checks.sh@@    reach-host-dialed-*) ;; # the host-dialed relay rows run in that mode whatever the setting says@@    reach-host-dialed-*) reach_is_host_dialed && return 3 ;;'
  'hack/live-checks.sh@@          reach-host-dialed-*) ;;@@          reach-host-dialed-*) check_reach_settings || bad=1 ;;'
  'hack/live-checks.sh@@    reach-host-dialed-*) export HPCBURST_CONF_AGENT_BUILDS=$HD_BUILD_DIR ;;@@    reach-host-dialed-*) ;;'
  'hack/live-checks.sh@@    reach-host-dialed-*) prepare_host_dialed "$1" ;;@@    reach-host-dialed-*) build_test_binary "$1" "$OUT/$1.test" ;;'
  'hack/live-checks.sh@@  unset HPCBURST_CONF_AGENT_BUILDS HPCBURST_ALPHA_AGENT_BIN@@  unset HPCBURST_CONF_AGENT_BUILDS'
  'hack/live-checks.sh@@reach-host-dialed-athena|athena|athena|plgrid-now|-|plgrid-now|ACCOUNT_ATHENA_A100|1|1|1Gi|00:05:00|25|-|10@@reach-host-dialed-athena|athena|athena|plgrid-now|-|plgrid-now|ACCOUNT_ATHENA_A100|1|1|1Gi|00:05:00|25|-'
  'hack/live-checks.sh@@reach-host-dialed-helios|helios|helios|plgrid-gpu-gh200|now|now|ACCOUNT_HELIOS_GH200|1|1|1Gi|00:05:00|25|-C memfs|10@@reach-host-dialed-helios|helios|helios|plgrid-gpu-gh200|now|-|ACCOUNT_HELIOS_GH200|1|1|1Gi|00:05:00|25|-C memfs|10'
  'hack/live-checks.sh@@reach-host-dialed|reach-host-dialed-helios,reach-host-dialed-athena@@reach-host-dialed|reach-host-dialed-athena'
  'hack/live-checks.sh@@reach-host-dialed-athena|cmd/hb-agent@@reach-host-dialed-athena|internal/slurm'
  # #444: every row that starts the stack checks LIVE_BIN_DIR's layout before the slot is read (check_bin_dir): the alpha rows' call (the
  # incident: only the rehearsal row checked), each of the three binaries, the executable bit, the setting named and never its value, every problem at once.
  'hack/live-checks.sh@@  check_bin_dir || bad=1@@  [ -d "${S[LIVE_BIN_DIR]:-/}" ] || bad=1'
  'hack/live-checks.sh@@for f in frps frpc; do@@for f in frps; do'
  'hack/live-checks.sh@@[ -f "$d/$f" ] && [ -x "$d/$f" ] || { say "live-checks: LIVE_BIN_DIR has no executable $f at@@[ -f "$d/$f" ] || { say "live-checks: LIVE_BIN_DIR has no executable $f at'
  'hack/live-checks.sh@@say "live-checks: LIVE_BIN_DIR has no executable $f at its top level@@say "live-checks: $d has no executable $f at its top level'
  'hack/live-checks.sh@@at its top level (hack/fetch-frp.sh puts it there)"; bad=1; }@@at its top level (hack/fetch-frp.sh puts it there)"; return 1; }'
  'hack/live-checks.sh@@[ -f "$d/k3s-1.36.5/k3s" ] && [ -x "$d/k3s-1.36.5/k3s" ] || { say "live-checks: LIVE_BIN_DIR has no executable k3s-1.36.5/k3s (hack/fetch-k3s.sh)"; bad=1; }@@:'
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@{o.JobID, "<the job>"}@@{"", "<the job>"}'
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@	s = pathWord.ReplaceAllString(s, "<a path>")@@	s = pathWord.ReplaceAllString(s, "$0")'
  # PR 306's review: the evidence each line of the post now requires (B1, B2, B4, B5; B3's is hostdial's own lifecycle).
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@case !p.AgentLogOK:@@case false:'
  'internal/providerctl/conformance/testdata/serving-probe.sh@@echo "agent-log=unreadable"@@echo "agent-log=ok"'
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@case st.Seq <= kept.Seq:@@case st.Seq < kept.Seq:'
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@case !st.Final || !strings.HasPrefix(st.FinalCause, "superseded") || st.Sessions != 0:@@case !st.Final:'
  'internal/providerctl/conformance/hostdialed_run_test.go@@Epoch: func(context.Context) (int64, error) { return hostDialedEpoch - 1, nil },@@Epoch: func(context.Context) (int64, error) { return hostDialedEpoch, nil },'
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@if keptCtl := o.Kept[agentapi.RelayControl]; o.Stale != nil && (ctl.PID == 0 || ctl.PID != keptCtl.PID || ctl.Starts != keptCtl.Starts) {@@if keptCtl := o.Kept[agentapi.RelayControl]; o.Stale != nil && ctl.PID == 0 && keptCtl.PID == 0 {'
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@case !o.StopServed:@@case false:'
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@case !o.StopAcked:@@case false:'
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@case o.EndState != "COMPLETED":@@case false:'
  'internal/providerctl/conformance/hostdialed_run_test.go@@	w.failedIsStart = true@@	w.failedIsStart = false'
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@endStateText(o.EndState), postSafe(lastAgentError(o.AgentTail), o))@@endStateText(o.EndState), "none read")'
  # The science agent's ruling on PR 306: the rendezvous line says yes or no and lengths, and quoted texts lose the run's private values.
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@if p.SocketDirMode == "700" {@@if p.SocketDirMode != "" {'
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@	case path.Dir(sockDir) == agentTmp:@@	case path.Dir(sockDir) != "":'
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@readable := p.AgentLogOK && p.AgentTmpDir != "" && path.IsAbs(p.AgentTmpDir)@@readable := p.AgentLogOK'
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@if would := hostdial.SocketPathUnder(agentTmp); would > agentapi.MaxRelaySocketPath {@@if would := hostdial.SocketPathUnder(agentTmp); would >= 0 {'
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@	for _, v := range privateParts(o) {@@	for _, v := range privateParts(hostDialedObs{}) {'
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@		if short, _, ok := strings.Cut(v, "."); ok {@@		if short, _, ok := strings.Cut(v, "."); false && ok {'
  'internal/providerctl/conformance/hostdialed_verdict_test.go@@	return "set, another"@@	return "set, the same"'
  # #308: what stands between the rehearsal's sshd sweep and a process the test did not start, one mutant per branch. Guard 1 (pool.go's
  # spawn) stores no exited, unreadable or timeless sshd; guard 2 (below) walks nothing from PID 1 or less, a timeless or a recycled root;
  # guard 3 (signalProcs) never signals an unrelated process: each target is pinned by a pidfd and verified a descendant of the test at
  # check time, by a start-time-ordered walk; a target that left the tree after the check can still receive the signal, which reaches only
  # that pinned process. A target found outside the tree is not signalled, and is a leak if still alive at the stop's deadline. The forced
  # command (relayguard.go) asks for SIGKILL at its sshd's exit. Each holds for its own class only: guard 1 does not cover a PID recycled
  # between record's check and its walk (guard 2 does), and guard 3 does not tell the tests of one binary apart.
  'internal/rehearsal/pool.go@@if err != nil || !live || self.pid != pid || self.ticks == 0 {@@if false {'
  'internal/rehearsal/procs.go@@if root.pid <= 1 {@@if root.pid < 0 {'
  'internal/rehearsal/procs.go@@if root.ticks == 0 {@@if false {'
  'internal/rehearsal/procs.go@@if !found {@@if !found && false {'
  'internal/rehearsal/signal.go@@case r.pid <= 1:@@case r.pid < 0:'
  'internal/rehearsal/signal.go@@case r.pid == o.self.pid:@@case false:'
  'internal/rehearsal/signal.go@@case r.ticks == 0:@@case false:'
  'internal/rehearsal/signal.go@@case o.ancestors[r.pid]:@@case false:'
  'internal/rehearsal/signal.go@@if !below {@@if !below && false {'
  'internal/rehearsal/signal.go@@case p.ticks > child.ticks:@@case false:'
  'internal/rehearsal/signal.go@@case !plive:@@case false && !plive:'
  'internal/rehearsal/signal.go@@case perr != nil:@@case false:'
  'internal/rehearsal/signal.go@@if o.self.ticks > child.ticks {@@if false {'
  # The leak report: what is found outside the tree is split out, judged at the stop's last deadline (one that has exited by then was
  # dying) and counted; then the stop's order, its kill of its own sshds, its bound and its report of survivors.
  'internal/rehearsal/pool.go@@if errors.As(e, &l) {@@if false && errors.As(e, &l) {'
  'internal/rehearsal/pool.go@@if now, live, err := s.api.row(l.row.pid); err != nil || (live && now.ticks == l.row.ticks) {@@if now, live, err := s.api.row(l.row.pid); true || err != nil || (live && now.ticks == l.row.ticks) {'
  'internal/rehearsal/pool.go@@s.api.leaks.Add(1)@@s.api.leaks.Add(0)'
  'internal/rehearsal/signal.go@@if n > 0 {@@if false {'
  'internal/rehearsal/pool.go@@if c := cmp.Compare(depth[b.pid], depth[a.pid]); c != 0 {@@if c := cmp.Compare(depth[a.pid], depth[b.pid]); c != 0 {'
  'internal/rehearsal/pool.go@@if listed[k] {@@if false {'
  'internal/rehearsal/pool.go@@if err := s.api.kill(p.cmd.Process); err != nil {@@if err := error(nil); err != nil {'
  'internal/rehearsal/pool.go@@case <-time.After(time.Until(deadline)):@@case <-make(chan time.Time):'
  'internal/rehearsal/pool.go@@if now, ok := readProcRow(r.pid); ok && now.ticks == r.ticks {@@if now, ok := readProcRow(r.pid); false && ok && now.ticks == r.ticks {'
  # The forced command's parent-death signal, its parent check, and the role that runs them.
  'internal/rehearsal/relayguard.go@@if err := deathSignal(); err != nil {@@if err := error(nil); err != nil {'
  'internal/rehearsal/relayguard.go@@if c := comm(ppid); !strings.HasPrefix(c, "sshd") {@@if c := comm(ppid); false && !strings.HasPrefix(c, "sshd") {'
  'internal/rehearsal/relayguard.go@@if os.Getenv(EnvRole) != RoleRelay {@@if true {'
  'internal/rehearsal/relayguard.go@@if ppid <= 1 {@@if false {'
  'internal/rehearsal/relayguard.go@@first, second := ask, check@@first, second := check, ask'
  'internal/rehearsal/relayguard.go@@os.O_WRONLY|os.O_APPEND, 0); ferr == nil {@@os.O_WRONLY|os.O_APPEND, 0); false && ferr == nil {'
  'internal/rehearsal/login.go@@return fmt.Sprintf("exec env %s=%s %s=%s %s", EnvRole, RoleRelay, EnvRelay, sock, exe)@@return fmt.Sprintf("%s=%s %s=%s %s", EnvRole, RoleRelay, EnvRelay, sock, exe)'
)
# The mutants of a test of this script: a file with one mutant per line, in place of the list above.
[ -z "${MUTATE_ALPHA_MUTANTS:-}" ] || mapfile -t mutants < "$MUTATE_ALPHA_MUTANTS"
here=$PWD
# compile_of <path>: the test binaries that judge a mutant of path, from the tree as it is now; on failure $cerr says why.
compile_of() {
  cerr=""
  case $1 in
    internal/providerctl/*) compile_tests internal/providerctl/conformance "$tmp/conf.test" && compile_tests "$pkg" "$tmp/pkg.test" ;;
    internal/rehearsal/*) compile_tests internal/rehearsal "$tmp/reh.test" ;;
    *.go) compile_tests "$pkg" "$tmp/pkg.test" ;;
  esac
}
# The tests of the rehearsal kind: the sweep guards' (internal/rehearsal: procs_test.go, signal_test.go, pool_test.go), which start no
# sshd and send no signal; their stand-ins (true, sh, cat, this binary as a forced command) end on their own, the last by the kernel's
# parent-death SIGKILL, which it asks for.
rehearsal_tests='ExitedSSHD|ZeroRowSignalsNothing|BelowWalksNothing|Guard2Alone|SignalProcs|RefusalIsTheErrorOfClose|WithRelayEnvSelectsNothing|ReportLeaks|CloseReturnsWhen|BeforeItsParent|LeftTheTree|StillAliveAtItsDeadline|StillOutsideTheTree|RelayGuard|ForcedCommandDies|ForcedCommandExecs|GuardFailureIsLogged'
# The tests of the script's kind.
script_tests='AccountingRow|TimeoutIs|AccountingRowsGet|ScriptsShapes|EveryAlphaRowRuns|GH200|Conformance|LateJobOnEvery|OwnersGo|ReadAgain|SlotHold|MostJobs|TunnelLease|LeaseBounds|BuildsBefore|PendingBound|PendingPastTheBound|CanBeNotRun|Reach|RealEngineJobPending|RealEngineNotRun|EveryRowInAOneJobSlot|RunsJobsByItsPrefix|NotRunAtItsFirstEnd|DiesMidWait|FrontLabel|FrontRan|LabelReaches|FrontRoles|ATableThat|NeverStarted|EveryRowCanBeNotRun|OnlyAMarkedCleanSkip|AMarkerAnEarlierRow|TimeoutReaches|AnAccountingRowWithNoSlot|HostDialedMode|HandoverRowsLead|LeadMarkersReason|LeadMarkerOnARow|EveryStackRow|TheRowsThatStartNoStack|TunnelRowsLongStream|LongMarkersReason|LongMarkerOnARow'
# tests_of <path> [control|<names>]: the command that judges a mutant of path, in the array tests. With control, the same command with
# no mutant: from the binaries kept at the baseline, the tree being as it was. With names (a regexp), that, for those tests alone, to
# the end. The
# package's run has -test.timeout 300s, not less: internal/rehearsal/stack gives up its waits two minutes before the test's deadline
# (CleanupMargin), so at 120s every wait on state failed at once and the unmutated tests failed, which counted every mutant
# of the package as killed (#285).
tests_of() {
  local names=${2:-} pkgbin=$tmp/pkg.test confbin=$tmp/conf.test rehbin=$tmp/reh.test
  local bin=(-test.count=1 -test.timeout 300s -test.failfast) sel=(-failfast -run "$script_tests") dry=PendingPastTheBound
  local rbin=(-test.count=1 -test.timeout 300s -test.failfast -test.run "$rehearsal_tests")
  if [ -n "$names" ]; then pkgbin=$tmp/base-pkg.test confbin=$tmp/base-conf.test rehbin=$tmp/base-reh.test; fi
  if [ -n "$names" ] && [ "$names" != control ]; then
    dry=$names bin=(-test.count=1 -test.timeout 300s -test.run "$names") sel=(-run "$names")
    rbin=(-test.count=1 -test.timeout 300s -test.run "$names")
  fi
  case $1 in
    hack/live-checks.sh) tests=(go test -count=1 -timeout 9m -parallel 3 "${sel[@]}" ./hack/livechecks) ;;
    # The dry runs of the script run the alpha test binary: the one compiled above, from the mutated file.
    "$pkg"/alpha_live_test.go) tests=(env HPCBURST_LIVECHECKS_ALPHA_TEST="$pkgbin" go test -count=1 -timeout 9m -run "$dry" ./hack/livechecks) ;;
    internal/providerctl/*) tests=(bash -c 'cd "$1/internal/providerctl/conformance" && "$2" "${@:5}" && cd "$1/$3" && "$4" "${@:5}"' _ "$here" "$confbin" "$pkg" "$pkgbin" "${bin[@]}") ;;
    internal/rehearsal/*) tests=(bash -c 'cd "$1/internal/rehearsal" && "$2" "${@:3}"' _ "$here" "$rehbin" "${rbin[@]}") ;;
    *) tests=(bash -c 'cd "$1/$2" && "$3" "${@:4}"' _ "$here" "$pkg" "$pkgbin" "${bin[@]}") ;;
  esac
}
# failing_tests <log>: the top-level tests a run's log says failed, at most three.
failing_tests() { sed -n -E 's/^--- FAIL: ([^ ]+).*/\1/p' "$1" | head -n 3 | paste -s -d ' ' -; }
# no_test_failed <log> <exit status>: what ended a failed run that names no failing test.
no_test_failed() {
  if [ "$2" = 124 ] || [ "$2" = 137 ]; then printf 'the bound of %s s, no test named as failed' "$run_bound"
  elif grep -q '^panic: test timed out' "$1"; then printf 'a timeout, no test named as failed'
  elif grep -q '^panic: ' "$1"; then printf 'a panic, no test named as failed'
  elif grep -q -E 'build failed|setup failed' "$1"; then printf 'a compile error, no test ran'
  else printf 'exit %s, no test named as failed' "$2"; fi
}
# bounded <log>: runs the array tests with an external bound, in a process group of their own (timeout makes one), its output in the
# log, and sets rc. Go's own -test.timeout cannot reach a hang before the test binary's main, and its panic skips the tests' cleanup, so
# what a run leaves in its group is killed before the next one starts. It waits in the background, so that a signal to this script
# is acted on at once (on_signal). The limit: a process that left the group is not seen (#274 ends the fake Slurm's jobs with their host).
run_bound=${MUTATE_ALPHA_RUN_BOUND:-720}   # above every kind's own deadlines, so that those fire first
child=""
bounded() {
  rc=0
  timeout -k 10 "$run_bound" "${tests[@]}" >"$1" 2>&1 &
  child=$!
  wait "$child" || rc=$?
  kill -KILL -- "-$child" 2>/dev/null
  child=""
}
# on_signal: the run stops now; the EXIT trap then restores the file, which no hung command can hold up.
on_signal() {
  if [ -n "$child" ]; then kill -TERM "$child" 2>/dev/null; kill -KILL -- "-$child" 2>/dev/null; fi
  exit 143
}
trap on_signal TERM INT
# now_epoch: the clock of the green cache. MUTATE_ALPHA_CLOCK is a test seam: a file holding the epoch seconds.
now_epoch() { if [ -n "${MUTATE_ALPHA_CLOCK:-}" ]; then cat "$MUTATE_ALPHA_CLOCK"; else date +%s; fi; }
# kind_of <path>: the name of the kind of mutant, by the tests that judge it.
kind_of() {
  case $1 in
    hack/live-checks.sh) printf "the script's" ;;
    "$pkg"/alpha_live_test.go) printf "the dry runs'" ;;
    internal/providerctl/*) printf "the conformance package's" ;;
    internal/rehearsal/*) printf "the rehearsal package's" ;;
    *) printf "the package's" ;;
  esac
}
# baseline <path>: the tests that judge a mutant of path must pass with no mutant, once per kind, under that kind's own command, or a
# failing test would count every mutant as killed and "0 survived" would say nothing. A tree they fail on stops the run (exit 2),
# with no mutant tried, --only or not. The binaries are kept, for red_with_no_mutant.
declare -A based=()
baseline() {
  local kind
  kind=$(kind_of "$1")
  [ -z "${based[$kind]:-}" ] || return 0
  based[$kind]=1
  compile_of "$1" || { echo "mutate-alpha: $kind tests do not compile with no mutant, so nothing can be checked: $cerr" >&2; exit 2; }
  case $1 in
    internal/providerctl/*) cp "$tmp/conf.test" "$tmp/base-conf.test"; cp "$tmp/pkg.test" "$tmp/base-pkg.test" ;;
    internal/rehearsal/*) cp "$tmp/reh.test" "$tmp/base-reh.test" ;;
    *.go) cp "$tmp/pkg.test" "$tmp/base-pkg.test" ;;
  esac
  tests_of "$1"
  bounded "$tmp/baseline.log"
  [ "$rc" -eq 0 ] || {
    by=$(failing_tests "$tmp/baseline.log")
    echo "mutate-alpha: $kind tests fail with no mutant, so every mutant would count as killed: ${by:-$(no_test_failed "$tmp/baseline.log" "$rc")}" >&2; exit 2; }
}
# red_with_no_mutant <path> <names>: sets red to those of the tests that failed under a mutant that fail with no mutant too (in this
# shell, not a subshell, since it keeps green_at): the tree must be as it was.
# A test that goes red after the baseline (a flake, a date, a port) would kill every mutant it meets, so a kill it alone made is
# not one (the science agent, on PR 284): the caller reads that as BROKEN, with the test named. A test seen green in the last
# ten minutes is not run again: the script's tests take minutes, and a kill costs one look, not one run per mutant. Only a test that
# ran gets a new time. The limits that stay: within the ten minutes a cached green is not proof that the test still passes, and a
# flake that passes on its look reads as a kill.
declare -A green_at=()
red_with_no_mutant() {
  local path=$1 kind now name todo="" failed
  red=""
  kind=$(kind_of "$path"); now=$(now_epoch)
  for name in $2; do
    [ $((now - ${green_at[$kind|$name]:-0})) -lt 600 ] || todo="$todo${todo:+|}$name"
  done
  [ -n "$todo" ] || return 0
  tests_of "$path" "^($todo)\$"
  bounded "$tmp/again.log"
  failed=" $(failing_tests "$tmp/again.log") "
  # The failure can leave no test named (a panic, a timeout): every test asked about is then red, by what ended the run.
  [ "$rc" -eq 0 ] || [ "$failed" != "  " ] || failed=" ${todo//|/ } "
  for name in ${todo//|/ }; do
    case "$failed" in *" $name "*) red="$red${red:+ }$name" ;; *) green_at[$kind|$name]=$now ;; esac
  done
}
killed=0 survived=0 broken=0 total=0
for m in "${mutants[@]}"; do
  [ -z "$only" ] || printf '%s\n' "$m" | grep -q -E -e "$only" || continue
  file=${m%%@@*}; rest=${m#*@@}; from=${rest%%@@*}; to=${rest#*@@}
  path=$(path_of "$file")
  case $path in hack/live-checks.sh) [ "$script_too" = 1 ] || continue ;; esac
  total=$((total+1))
  keep "$path"
  cp "${saved[$path]}" "$path"
  baseline "$path"
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
      compile_of "$path" || { echo "BROKEN   $file: $from  ->  $to: the mutant does not compile: $cerr"; broken=$((broken+1)); cp "${saved[$path]}" "$path"; continue; } ;;
  esac
  tests_of "$path"
  bounded "$tmp/mutant.log"
  cp "${saved[$path]}" "$path"
  if [ "$rc" -eq 0 ]; then
    echo "SURVIVED $file: $from  ->  $to"; survived=$((survived+1)); continue
  fi
  by=$(failing_tests "$tmp/mutant.log")
  if [ -z "$by" ]; then
    # Naming what ended the run does not show the mutant did it. A compile error after a green baseline never does; anything else
    # gets a control run, the same command with no mutant (the delta review, H1). The limit: a timeout from load, or a kill from
    # outside, that does not recur in the control reads as a kill, by what it was.
    by=$(no_test_failed "$tmp/mutant.log" "$rc")
    case $by in
      "a compile error"*) echo "BROKEN   $file: $from  ->  $to: $by"; broken=$((broken+1)); continue ;;
    esac
    tests_of "$path" control
    bounded "$tmp/control.log"
    if [ "$rc" -eq 0 ]; then
      killed=$((killed+1)); echo "KILLED   $file: $from  ->  $to: by $by"; printf '%s\n' "$by" >>"$tmp/kills"
    else
      echo "BROKEN   $file: $from  ->  $to: ended by $by, and the same command fails with no mutant too"; broken=$((broken+1))
    fi
    continue
  fi
  # With the tree as it was. A kill needs a test that fails under the mutant and passes with no mutant.
  red_with_no_mutant "$path" "$by"
  real=""
  for name in $by; do case " $red " in *" $name "*) ;; *) real="$real${real:+ }$name" ;; esac; done
  if [ -z "$real" ]; then
    echo "BROKEN   $file: $from  ->  $to: killed only by $red, which also fails with no mutant"; broken=$((broken+1))
  else
    killed=$((killed+1)); echo "KILLED   $file: $from  ->  $to: by $real"; printf '%s\n' "$real" >>"$tmp/kills"
  fi
done
[ -z "$only" ] || [ "$total" -gt 0 ] || { echo "mutate-alpha: --only $only selected no mutant, so nothing was checked" >&2; exit 2; }
[ ! -s "$tmp/kills" ] || { echo "kills by what failed:"; sort "$tmp/kills" | uniq -c | sort -rn | sed 's/^/  /'; }
echo "mutants: $total, killed $killed, survived $survived, broken $broken"
[ "$survived" -eq 0 ] && [ "$broken" -eq 0 ]
