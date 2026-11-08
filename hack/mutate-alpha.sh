#!/usr/bin/env bash
# Mutation check of the alpha rows (issue #194): each mutant changes one condition, and the tests of what it
# changes must then fail. A mutant the tests let through is a check that no test depends on.
#
#   hack/mutate-alpha.sh              # every mutant; exit 1 if one survives or does not apply
#   hack/mutate-alpha.sh --no-script  # leave out the mutants of hack/live-checks.sh (the slow ones)
#   hack/mutate-alpha.sh --only <regexp>  # only the mutants whose line matches (egrep), to try a new one; exit 2, and
#                                         # nothing run, for an expression that is not valid or matches no mutant
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
cd "$(dirname "$0")/.."
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
  'verdict.go@@case r.Direction != "up":@@case r.Direction == "up":'
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
  'verdict.go@@if ss[len(ss)-1].At.Before(last) {@@if false {'
  'verdict.go@@case s.At.IsZero() || s.LastStart.IsZero():@@case s.At.IsZero() && s.LastStart.IsZero():'
  'verdict.go@@if longestChain(reqs, tm.MaxChainGap) < tm.MinChain {@@if longestChain(reqs, tm.MaxChainGap) <= tm.MinChain {'
  'verdict.go@@d >= tm.NearDeadline && d <= tm.MaxRequestDuration@@d > tm.NearDeadline && d <= tm.MaxRequestDuration'
  'verdict.go@@if concurrentStreams(reqs, tm.StreamRate) < tm.ConcurrentStreams {@@if concurrentStreams(reqs, tm.StreamRate) <= tm.ConcurrentStreams {'
  'verdict.go@@if longestIdle(reqs) < tm.MinIdleGap {@@if longestIdle(reqs) <= tm.MinIdleGap {'
  'verdict.go@@r.TokensPerSecond() >= rate@@r.TokensPerSecond() > rate'
  'verdict.go@@gap >= 0 && gap <= maxGap@@gap > 0 && gap <= maxGap'
  'verdict.go@@gap >= 0 && gap <= maxGap@@gap >= 0 && gap < maxGap'
  'verdict.go@@case !r.UnderLoad || r.Tokens <= 0:@@case !r.UnderLoad && r.Tokens <= 0:'
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
  # B5: the two-job guard.
  'jobcount.go@@	if e := l.Clock.Add(g.TimeLimit - g.DrainWindow).Truncate(time.Second); e.After(g.end) {@@	if e := l.Clock.Add(g.TimeLimit).Truncate(time.Second); e.After(g.end) {'
  'jobcount.go@@e.After(g.end) {@@true {'
  'jobcount.go@@	case len(others) > 0 && (g.final || len(others) > 1):@@	case len(others) > 0 && g.final:'
  'jobcount.go@@		if j.HardEnd == nil || j.HardEnd.Sub(*j.StartTime) != g.TimeLimit {@@		if j.HardEnd == nil {'
  'jobcount.go@@	return j.StartTime != nil && (j.EndTime == nil || j.EndTime.After(*j.StartTime))@@	return j.StartTime != nil'
  'jobcount.go@@	if g.pred == "" && len(started) > 0 {@@	if len(started) > 0 {'
  'jobcount.go@@	return clusterNow.Add(timeLimit - drainWindow).Truncate(time.Second)@@	return clusterNow.Add(timeLimit).Truncate(time.Second)'
  # B6: the guard's failures fail the step, and the run ends with the successor's drain start observed.
  'steps_live_test.go@@				if fails++; fails >= guardLookFails {@@				if fails++; false {'
  'steps_live_test.go@@				_, err = setEnd(end)@@				_ = end'
  'steps_live_test.go@@	stack.WaitFor(r.t, d, what, guarded)@@	stack.WaitFor(r.t, d, what, cond); _ = guarded'
  'steps_live_test.go@@			if md.Spec.EndTime == nil || !md.Spec.EndTime.Time.Equal(end) {@@			if false {'
  'steps_live_test.go@@			return errors.New("the successor never started, so the run'"'"'s deployment never ended at its drain start")@@			return nil'
  # B7: the first clock, and a tunnel row's NOT RUN at its first end.
  'jobcount.go@@		if c := timeLimit - drainWindow - time.Minute; c < pendingMax {@@		if c := timeLimit - drainWindow; c < pendingMax {'
  'jobcount.go@@		if c := timeLimit - drainWindow - time.Minute; c < pendingMax {@@		if c := timeLimit - drainWindow - time.Minute; c <= pendingMax {'
  'run.go@@ || (row.MostJobs == 1 && errors.Is(err, ErrQueueWaitPastEnd))@@'
  'run.go@@		if ns.PastEnd {@@		if false {'
  'hack/live-checks.sh@@  [ "${SWAIT[$n]:--}" != - ] || [ -n "${OGSET[$n]:-}" ] || case $n in tunnel-hour-*) true ;; *) false ;; esac || return 1@@  [ "${SWAIT[$n]:--}" != - ] || [ -n "${OGSET[$n]:-}" ] || return 1'
  'hack/live-checks.sh@@  [ "${SWAIT[$n]:--}" != - ] || [ -n "${OGSET[$n]:-}" ] || case $n in tunnel-hour-*) true ;; *) false ;; esac || return 1@@  [ "${SWAIT[$n]:--}" != - ] || case $n in tunnel-hour-*) true ;; *) false ;; esac || return 1'
  'hack/live-checks.sh@@    elif grep -q "a minute before the deployment'"'"'s first end" "$REC_DIR/not-run.txt" 2>/dev/null; then@@    elif false; then'
  'dry_test.go@@	d.tick(bound - time.Second)@@	d.tick(bound - time.Minute)'
  'run.go@@	return (row.SlotHold > 0 && errors.Is(err, ErrNotStarted)) ||@@	return errors.Is(err, ErrNotStarted) ||'
  'run.go@@	return (row.SlotHold > 0 && errors.Is(err, ErrNotStarted)) ||@@	return (row.SlotHold > 0) ||'
  'alpha_live_test.go@@		if merr := rec.SaveText(NotRunFile, note); merr != nil {@@		if merr := error(nil); merr != nil {'
  'run.go@@			if errors.Is(err, ErrNotStarted) {@@			if false {'
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
killed=0 survived=0 broken=0 total=0
for m in "${mutants[@]}"; do
  [ -z "$only" ] || printf '%s\n' "$m" | grep -q -E -e "$only" || continue
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
    hack/live-checks.sh) tests=(go test -count=1 -timeout 9m -parallel 3 -failfast -run 'AccountingRow|TimeoutIs|AccountingRowsGet|ScriptsShapes|EveryAlphaRowRuns|GH200|Conformance|LateJobOnEvery|OwnersGo|ReadAgain|SlotHold|MostJobs|TunnelLease|LeaseBounds|BuildsBefore|PendingBound|PendingPastTheBound|CanBeNotRun|Reach|RealEngineJobPending|RealEngineNotRun|EveryRowInAOneJobSlot|RunsJobsByItsPrefix|NotRunAtItsFirstEnd' ./hack/livechecks) ;;
    "$pkg"/alpha_live_test.go)
      # The dry runs of the script run the alpha test binary: build it from the mutated file, to one fixed path.
      tests=(bash -c 'go test -c -o "$1" ./internal/alpha && HPCBURST_LIVECHECKS_ALPHA_TEST=$1 go test -count=1 -timeout 9m -run PendingPastTheBound ./hack/livechecks' _ "$tmp/alpha-mutant.test") ;;
    internal/providerctl/*) tests=(go test -count=1 -timeout 300s ./internal/providerctl/conformance "./$pkg") ;;
    *) tests=(go test -count=1 -timeout 120s "./$pkg") ;;
  esac
  if "${tests[@]}" >/dev/null 2>&1; then
    echo "SURVIVED $file: $from  ->  $to"; survived=$((survived+1))
  else
    killed=$((killed+1))
  fi
  cp "${saved[$path]}" "$path"
done
[ -z "$only" ] || [ "$total" -gt 0 ] || { echo "mutate-alpha: --only $only selected no mutant, so nothing was checked" >&2; exit 2; }
echo "mutants: $total, killed $killed, survived $survived, broken $broken"
[ "$survived" -eq 0 ] && [ "$broken" -eq 0 ]
