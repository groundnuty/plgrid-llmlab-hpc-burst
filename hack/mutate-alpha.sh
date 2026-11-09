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
# A mutant ends as KILLED (a test fails, and the line names it), SURVIVED (every test passes) or BROKEN: its text does not
# occur exactly once, or the Go file it changes no longer compiles. A mutant that does not compile fails to build the tests,
# and that is not a test failing, so it is reported as BROKEN, never as killed; the run fails for a survivor and for a broken
# mutant alike. The run ends with how many kills each failing test made, so a shared cause shows. The
# compile is its own step, to one fixed test-binary path per package, and the binary then runs: so MUTATE_ALPHA_BUILD_LOCK,
# a lock file, is taken (flock -o) around each compile only, never around a test run.
#
# Four kinds, by the file they change:
#   internal/alpha (a bare file name)           go test ./internal/alpha
#   internal/alpha/alpha_live_test.go           the row's test as the script runs it: an alpha test binary is built from
#                                               the mutated package, and the dry runs of hack/livechecks run that binary
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
  'hack/live-checks.sh@@conformance-helios|helios|helios|plgrid|-|-|ACCOUNT_HELIOS_CPU|0|1|1Gi|00:05:00|85|-|30@@conformance-helios|helios|helios|plgrid|-|-|ACCOUNT_HELIOS_CPU|0|1|1Gi|00:05:00|85|-'
  'hack/live-checks.sh@@conformance-helios|helios|helios|plgrid|-|-|ACCOUNT_HELIOS_CPU|0|1|1Gi|00:05:00|85|-|30@@conformance-helios|helios|helios|plgrid|-|-|ACCOUNT_HELIOS_CPU|0|1|1Gi|00:05:00|85|-|20'
  'hack/live-checks.sh@@conformance-helios|helios|helios|plgrid|-|-|ACCOUNT_HELIOS_CPU|0|1|1Gi|00:05:00|85|-|30@@conformance-helios|helios|helios|plgrid|-|-|ACCOUNT_HELIOS_CPU|0|1|1Gi|00:05:00|80|-|30'
  # The review of PR 284. B1, B2 and B4: a job that started, or that a read cannot tell about, is never NOT RUN.
  'internal/providerctl/conformance/startwait_test.go@@			accountingNotFailed(w.t, last) // no job starts@@			_ = last // no job starts'
  'internal/providerctl/conformance/startwait_test.go@@		if why := w.whyNotWaiting(last, acceptedID); why != "" {@@		if why := ""; why != "" {'
  'internal/providerctl/conformance/startwait_test.go@@	if o.StartTime != nil || o.Phase == v1alpha1.AllocationRunning || o.Phase == v1alpha1.AllocationEnded {@@	if false {'
  'internal/providerctl/conformance/startwait_test.go@@	if o.StartTime != nil || o.Phase == v1alpha1.AllocationRunning || o.Phase == v1alpha1.AllocationEnded {@@	if o.Phase == v1alpha1.AllocationRunning || o.Phase == v1alpha1.AllocationEnded {'
  'internal/providerctl/conformance/startwait_test.go@@	if o.StartTime != nil || o.Phase == v1alpha1.AllocationRunning || o.Phase == v1alpha1.AllocationEnded {@@	if o.StartTime != nil || o.Phase == v1alpha1.AllocationEnded {'
  'internal/providerctl/conformance/startwait_test.go@@	if o.StartTime != nil || o.Phase == v1alpha1.AllocationRunning || o.Phase == v1alpha1.AllocationEnded {@@	if o.StartTime != nil || o.Phase == v1alpha1.AllocationRunning {'
  'internal/providerctl/conformance/startwait_test.go@@		return fmt.Sprintf("its state could not be read: %v", err)@@		return ""'
  'internal/providerctl/conformance/startwait_test.go@@		return "the queue does not list it"@@		return ""'
  'internal/providerctl/conformance/startwait_test.go@@	case !alpha.WaitingToStart(j):@@	case false:'
  'internal/providerctl/conformance/startwait_test.go@@		return "its Allocation could not be read"@@		return ""'
  'internal/providerctl/conformance/startwait_test.go@@		return "there is no platform to read it on"@@		return ""'
  'internal/providerctl/conformance/startwait_test.go@@		id = a.Annotations[v1alpha1.AnnotationJobID]@@		id = acceptedID'
  'internal/providerctl/conformance/startwait_test.go@@	id := acceptedID@@	id := ""'
  'jobcount.go@@	return j.StartTime == nil && j.EndTime == nil && !j.Requeued && (j.Phase == v1alpha1.AllocationPending || j.Phase == v1alpha1.AllocationHeld)@@	return j.EndTime == nil && !j.Requeued && (j.Phase == v1alpha1.AllocationPending || j.Phase == v1alpha1.AllocationHeld)'
  'jobcount.go@@	return j.StartTime == nil && j.EndTime == nil && !j.Requeued && (j.Phase == v1alpha1.AllocationPending || j.Phase == v1alpha1.AllocationHeld)@@	return j.StartTime == nil && !j.Requeued && (j.Phase == v1alpha1.AllocationPending || j.Phase == v1alpha1.AllocationHeld)'
  'jobcount.go@@	return j.StartTime == nil && j.EndTime == nil && !j.Requeued && (j.Phase == v1alpha1.AllocationPending || j.Phase == v1alpha1.AllocationHeld)@@	return j.StartTime == nil && j.EndTime == nil && (j.Phase == v1alpha1.AllocationPending || j.Phase == v1alpha1.AllocationHeld)'
  'jobcount.go@@	return j.StartTime == nil && j.EndTime == nil && !j.Requeued && (j.Phase == v1alpha1.AllocationPending || j.Phase == v1alpha1.AllocationHeld)@@	return j.StartTime == nil && j.EndTime == nil && !j.Requeued'
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
  # B3: the first job ran and the second never started is NOT RUN unless an earlier failure stands; its own bound.
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
  'run.go@@		post += "- " + secondNotStartedWords + "; the failed checks are the first job'"'"'s\n"@@		_ = secondNotStartedWords'
  'steps_live_test.go@@		if !startBy.IsZero() && time.Now().After(startBy.Add(2*r.every())) && !r.secondStarted() {@@		if false {'
  'steps_live_test.go@@		if !startBy.IsZero() && time.Now().After(startBy.Add(2*r.every())) && !r.secondStarted() {@@		if !startBy.IsZero() && !r.secondStarted() {'
  'steps_live_test.go@@		if !startBy.IsZero() && time.Now().After(startBy.Add(2*r.every())) && !r.secondStarted() {@@		if !startBy.IsZero() && time.Now().After(startBy.Add(2*r.every())) {'
  'steps_live_test.go@@		if !startBy.IsZero() && time.Now().After(startBy.Add(2*r.every())) && !r.secondStarted() {@@		if time.Now().After(startBy.Add(2*r.every())) && !r.secondStarted() {'
  'live_flows_test.go@@time.Until(leaseEnd)+SuccessorWait(r.tm, p.PendingMax), leaseEnd.Add(p.PendingMax))@@time.Until(leaseEnd)+SuccessorWait(r.tm, p.PendingMax), time.Time{})'
  # B5 and B6: the sums. Each is compared with a figure worked out by hand, so a longer wait fails a test, not a run.
  'budget.go@@	CloseTimeout = 5 * time.Minute@@	CloseTimeout = 4 * time.Minute'
  'budget.go@@	ReadyMargin = 5 * time.Minute@@	ReadyMargin = 4 * time.Minute'
  'budget.go@@	EndMargin = 5 * time.Minute@@	EndMargin = 4 * time.Minute'
  'budget.go@@	GuardEvery       = 10 * time.Second@@	GuardEvery       = 5 * time.Second'
  'budget.go@@	GuardResultLooks = 6@@	GuardResultLooks = 3'
  'budget.go@@	return pendingMax + tm.StartTimeout + ReadyMargin@@	return tm.StartTimeout + ReadyMargin'
  'budget.go@@	return time.Duration(r.MostJobs)*pendingMax + r.Budget@@	return pendingMax + r.Budget'
  'budget.go@@		return first + (r.Lease - dw) + tm.MaxRequestDuration + dw + EndMargin + lastLooks + CloseTimeout@@		return first + (r.Lease - dw) + dw + EndMargin + lastLooks + CloseTimeout'
  'budget.go@@		return first + r.Lease + SuccessorWait(tm, pendingMax) + lastLooks + CloseTimeout@@		return first + r.Lease + lastLooks + CloseTimeout'
  'budget.go@@	return first + (r.Lease - dw) + dw + EndMargin + CloseTimeout@@	return first + (r.Lease - dw) + EndMargin + CloseTimeout'
  'budget.go@@	first, _ := FirstClock(r, pendingMax, r.Lease, dw)@@	first := pendingMax'
  'rows.go@@	r.Budget = lease + 15*time.Minute@@	_ = r.Budget'
  'dry_test.go@@	d.tick(bound - time.Second)@@	d.tick(bound - time.Minute)'
  'alpha_live_test.go@@		if merr := rec.SaveText(NotRunFile, note); merr != nil {@@		if merr := error(nil); merr != nil {'
  'run.go@@			if neverStarted(err) {@@			if false {'
  'alpha_live_test.go@@	if NotRun(cfg.Row, err) {@@	if false {'
  'hack/live-checks.sh@@PENDING_MAX_NOW=1800@@PENDING_MAX_NOW=7200'
  'hack/live-checks.sh@@  elif [ "${SLOT[$1]:--}" != - ]; then printf '"'"'%s'"'"' "$PENDING_MAX_NOW"@@  elif false; then printf '"'"'%s'"'"' "$PENDING_MAX_NOW"'
  'hack/live-checks.sh@@  if [ -n "${LIVE_PENDING_MAX_SECONDS:-}" ]; then printf '"'"'%s'"'"' "$LIVE_PENDING_MAX_SECONDS"@@  if false; then printf '"'"'%s'"'"' "$LIVE_PENDING_MAX_SECONDS"'
  'hack/live-checks.sh@@  export HPCBURST_ALPHA_PENDING_MAX_SECONDS=$(pending_max "$n")@@  export HPCBURST_ALPHA_PENDING_MAX_SECONDS=${LIVE_PENDING_MAX_SECONDS:-7200}'
  'hack/live-checks.sh@@"$(( ${TMIN[$n]} + ($(pending_max "$n") + 59) / 60 ))" env_alpha@@"$(( ${TMIN[$n]} + ${LIVE_PENDING_MAX_SECONDS:-7200} / 60 ))" env_alpha'
  'hack/live-checks.sh@@"$(( ${TMIN[$n]} + ($(pending_max "$n") + 59) / 60 ))" env_alpha@@"$(( ${TMIN[$n]} + $(pending_max "$n") / 60 ))" env_alpha'
  'hack/live-checks.sh@@may pend up to $(fmt_pending "$(pending_max "$1")") in the queue@@may pend up to $((7200 / 60)) min in the queue'
  'hack/live-checks.sh@@was still pending after $(fmt_pending "$(pending_max "$n")") in the queue@@was still pending after 30 min in the queue'
  'hack/live-checks.sh@@  say "$1: may pend up to $(fmt_pending "$(pending_max "$1")") in the queue (then it is cancelled by its tag and the run is NOT RUN), then holds QOS@@  say "$1: holds QOS'
  'hack/live-checks.sh@@elif [ "$1" -lt 60 ]; then printf '"'"'%d s'"'"' "$1"@@elif false; then printf '"'"'%d s'"'"' "$1"'
  'hack/live-checks.sh@@else printf '"'"'%d min %d s'"'"' $(($1 / 60)) $(($1 % 60)); fi@@else printf '"'"'%d min'"'"' $(($1 / 60)); fi'
  'hack/live-checks.sh@@if [ $(($1 % 60)) -eq 0 ]; then printf '"'"'%d min'"'"' $(($1 / 60))@@if [ $(($1 % 60)) -ge 0 ]; then printf '"'"'%d min'"'"' $(($1 / 60))'
  'hack/live-checks.sh@@  acct=${S[${ACCTKEY[$n]}]}; pend_max=$(pending_max "$n")@@  acct=${S[${ACCTKEY[$n]}]}; pend_max=${LIVE_PENDING_MAX_SECONDS:-7200}'
  'hack/live-checks.sh@@[ $((now - submit_t)) -gt "$pend_max" ]@@[ $((now - submit_t)) -gt $((pend_max + 60)) ]'
  'hack/live-checks.sh@@      NOT_RUN=1@@      :'
  'hack/live-checks.sh@@a job was still pending after $(fmt_pending "$pend_max") in the queue@@a job was still pending after 30 min in the queue'
  'hack/live-checks.sh@@gpu-accounting-*|reach-*) [ "${SWAIT[$n]:--}" != - ] || export HPCBURST_CONF_PENDING_MAX_SECONDS=$(pending_max "$n")@@gpu-accounting-*|reach-*) [ "${SWAIT[$n]:--}" != - ] || export HPCBURST_CONF_PENDING_MAX_SECONDS=7200'
  'hack/live-checks.sh@@conformance-athena|athena|athena|plgrid-now|-|plgrid-now|ACCOUNT_ATHENA_A100|1|4|32Gi|00:05:00|45|-|10@@conformance-athena|athena|athena|plgrid-now|-|plgrid-now|ACCOUNT_ATHENA_A100|1|4|32Gi|00:05:00|45|-'
  'hack/live-checks.sh@@reach-athena|athena|athena|plgrid-now|-|plgrid-now|ACCOUNT_ATHENA_A100|1|1|1Gi|00:02:00|20|-|10@@reach-athena|athena|athena|plgrid-now|-|plgrid-now|ACCOUNT_ATHENA_A100|1|1|1Gi|00:02:00|20|-|15'
  'config.go@@cfg.PendingMax = time.Minute@@cfg.PendingMax = 2 * time.Minute'
  'config.go@@if v := getenv(envPending); v != "" {@@if v := getenv(envPending); v == "x" {'
  'config.go@@return time.Duration(s) * time.Second, nil@@return time.Duration(s+60) * time.Second, nil'
  'config.go@@if err != nil || s <= 0 {@@if err != nil || s < 0 {'
  'dry_test.go@@d.tick(2 * time.Second)@@d.tick(61 * time.Second)'
  'dry_test.go@@d.out(n), d.out(n), d.gateWrap(n))@@d.out(n), d.out(n), "sleep 5")'
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
# compile_of <path>: the test binaries that judge a mutant of path, from the tree as it is now; on failure $cerr says why.
compile_of() {
  cerr=""
  case $1 in
    internal/providerctl/*) compile_tests internal/providerctl/conformance "$tmp/conf.test" && compile_tests "$pkg" "$tmp/pkg.test" ;;
    *.go) compile_tests "$pkg" "$tmp/pkg.test" ;;
  esac
}
# tests_of <path>: the command that judges a mutant of path, in the array tests. The package's run has -test.timeout 300s, not
# less: internal/rehearsal/stack gives up its waits two minutes before the test's deadline (CleanupMargin), so at 120s every
# wait on state failed at once and the unmutated tests failed, which counted every mutant of the package as killed.
tests_of() {
  case $1 in
    hack/live-checks.sh) tests=(go test -count=1 -timeout 9m -parallel 3 -failfast -run 'AccountingRow|TimeoutIs|AccountingRowsGet|ScriptsShapes|EveryAlphaRowRuns|GH200|Conformance|LateJobOnEvery|OwnersGo|ReadAgain|SlotHold|MostJobs|TunnelLease|LeaseBounds|BuildsBefore|PendingBound|PendingPastTheBound|CanBeNotRun|Reach|RealEngineJobPending|RealEngineNotRun|EveryRowInAOneJobSlot|RunsJobsByItsPrefix|NotRunAtItsFirstEnd|DiesMidWait|FrontLabel|FrontRan|LabelReaches|FrontRoles|ATableThat|NeverStarted|EveryRowCanBeNotRun|OnlyAMarkedCleanSkip|AMarkerAnEarlierRow|TimeoutReaches|AnAccountingRowWithNoSlot' ./hack/livechecks) ;;
    "$pkg"/alpha_live_test.go)
      # The dry runs of the script run the alpha test binary: the one compiled above, from the mutated file.
      tests=(env HPCBURST_LIVECHECKS_ALPHA_TEST="$tmp/pkg.test" go test -count=1 -timeout 9m -run PendingPastTheBound ./hack/livechecks) ;;
    internal/providerctl/*) tests=(bash -c 'cd "$1/internal/providerctl/conformance" && "$2" -test.count=1 -test.timeout 300s -test.failfast && cd "$1/$3" && "$4" -test.count=1 -test.timeout 300s -test.failfast' _ "$here" "$tmp/conf.test" "$pkg" "$tmp/pkg.test") ;;
    *) tests=(bash -c 'cd "$1/$2" && "$3" -test.count=1 -test.timeout 300s -test.failfast' _ "$here" "$pkg" "$tmp/pkg.test") ;;
  esac
}
# failing_tests <log>: the top-level tests a run's log says failed, or the panic that ended it.
failing_tests() { sed -n -E 's/^--- FAIL: ([^ ]+).*/\1/p; s/^panic: (test timed out).*/\1/p' "$1" | head -n 3 | paste -s -d ' ' -; }
# baseline <path>: the tests that judge a mutant of path must pass with no mutant, once per kind, or a failing test would count
# every mutant as killed and "0 survived" would say nothing. A tree they fail on stops the run (exit 2), with no mutant tried.
declare -A based=()
baseline() {
  local kind
  case $1 in
    hack/live-checks.sh) kind="the script's" ;;
    "$pkg"/alpha_live_test.go) kind="the dry runs'" ;;
    internal/providerctl/*) kind="the conformance package's" ;;
    *) kind="the package's" ;;
  esac
  [ -z "${based[$kind]:-}" ] || return 0
  based[$kind]=1
  compile_of "$1" || { echo "mutate-alpha: $kind tests do not compile with no mutant, so nothing can be checked: $cerr" >&2; exit 2; }
  tests_of "$1"
  "${tests[@]}" >"$tmp/baseline.log" 2>&1 || {
    echo "mutate-alpha: $kind tests fail with no mutant, so every mutant would count as killed: $(failing_tests "$tmp/baseline.log")" >&2; exit 2; }
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
  if "${tests[@]}" >"$tmp/mutant.log" 2>&1; then
    echo "SURVIVED $file: $from  ->  $to"; survived=$((survived+1))
  else
    killed=$((killed+1))
    # A kill names what failed, so that one test failing for every mutant shows (#285).
    by=$(failing_tests "$tmp/mutant.log")
    echo "KILLED   $file: $from  ->  $to: by ${by:-no test it could name}"
    printf '%s\n' "${by:-no test it could name}" >>"$tmp/kills"
  fi
  cp "${saved[$path]}" "$path"
done
[ -z "$only" ] || [ "$total" -gt 0 ] || { echo "mutate-alpha: --only $only selected no mutant, so nothing was checked" >&2; exit 2; }
[ ! -s "$tmp/kills" ] || { echo "kills by what failed:"; sort "$tmp/kills" | uniq -c | sort -rn | sed 's/^/  /'; }
echo "mutants: $total, killed $killed, survived $survived, broken $broken"
[ "$survived" -eq 0 ] && [ "$broken" -eq 0 ]
