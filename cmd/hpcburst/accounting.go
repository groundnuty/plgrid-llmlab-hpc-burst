package main

// hpcburst accounting: the totals per grant and account (HBEP-8, Provider accounting, Billing, Totals per
// grant; issue #625). The grouping and the sums are internal/accounting; this file is the command line, the
// reads and the report. It reads the API only, with the person's kubeconfig, and writes nothing.

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/accounting"
)

// accountingUsage is the help of hpcburst accounting.
const accountingUsage = `usage: hpcburst accounting [--state-dir DIR | --kubeconfig FILE] [--json]

Totals the billed units of the kept accounting records and the live Allocations per grant and account, and
prints the whole days left to a grant's grantEnd when a Provider of the line's members sets one (the earliest
such grantEnd, as the Providers' specs say now: a record whose own grant differs from its Provider's current
grant still shows that Provider's grant end). A billed unit is the scheduler's
billing count for a job times its elapsed hours, in its partition's billing unit (GPU-hours or CPU-hours): the
units are never added together, so a grant's GPU and CPU accounts are separate lines. Every figure is
allocated, not billed. A line counts the members whose figure is unknown beside the sum of the known ones: a
pending Allocation, a member with its billing count, elapsed time or billing unit unknown, and a record
written before those fields. It reads the API with the host's kubeconfig (<state-dir>/kubeconfig), or the one --kubeconfig
names, and writes nothing. --json prints the same data.`

// accountingHeading is the first line of the text report.
const accountingHeading = "Accounting totals per grant and account: " + accounting.Label + "."

func cmdAccounting(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flags("accounting", stderr)
	fs.Usage = func() {
		fmt.Fprintln(stderr, accountingUsage)
		fs.PrintDefaults()
	}
	var sd, kubeconfig string
	var asJSON bool
	fs.StringVar(&sd, "state-dir", "", stateDirUsage)
	fs.StringVar(&kubeconfig, "kubeconfig", "", "kubeconfig to read with (default <state-dir>/kubeconfig, the host's)")
	fs.BoolVar(&asJSON, "json", false, "print the result as JSON")
	if code, done := parse(fs, args); done {
		return code
	}
	if kubeconfig == "" {
		dir, err := stateDir(sd)
		if err != nil {
			return fail(stderr, err)
		}
		kubeconfig = filepath.Join(dir, "kubeconfig")
	}
	c, err := accountingClient(kubeconfig)
	if err != nil {
		return fail(stderr, err)
	}
	return runAccounting(ctx, c, time.Now(), asJSON, stdout, stderr)
}

// accountingClient is a client of the API the kubeconfig names, with the framework's types and the
// ControllerRevision registered.
func accountingClient(kubeconfig string) (client.Client, error) {
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("read the kubeconfig: %w", err)
	}
	cfg.Timeout = 30 * time.Second
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("connect to the API server: %w", err)
	}
	return c, nil
}

// runAccounting reads the records and the live objects through c, totals them at now and prints the
// report. The clock is the caller's, so a test sets it.
func runAccounting(ctx context.Context, c client.Reader, now time.Time, asJSON bool, stdout, stderr io.Writer) int {
	in, unreadable, err := readAccounting(ctx, c)
	if err != nil {
		return fail(stderr, err)
	}
	report := accounting.Summarize(in, now)
	report.Unreadable = unreadable
	if asJSON {
		emitJSON(stdout, report)
		return exitOK
	}
	printAccounting(stdout, report)
	return exitOK
}

// readAccounting lists every ControllerRevision labelled as an accounting record, in every namespace,
// and the live Allocations, Providers and PlatformProfiles. A record whose data cannot be read is named
// in unreadable and left out. A failed list is an error: a total made without it would be wrong.
func readAccounting(ctx context.Context, c client.Reader) (accounting.Inputs, []string, error) {
	var in accounting.Inputs
	var revs appsv1.ControllerRevisionList
	if err := c.List(ctx, &revs, client.MatchingLabels{v1alpha1.LabelRecord: v1alpha1.RecordAccounting}); err != nil {
		return in, nil, fmt.Errorf("list the accounting records: %w", err)
	}
	var unreadable []string
	for _, cr := range revs.Items {
		rec, err := accounting.ParseRecord(cr)
		if err != nil {
			unreadable = append(unreadable, cr.Namespace+"/"+cr.Name)
			continue
		}
		in.Records = append(in.Records, rec)
	}
	var allocs v1alpha1.AllocationList
	if err := c.List(ctx, &allocs); err != nil {
		return in, nil, fmt.Errorf("list the Allocations: %w", err)
	}
	in.Allocations = allocs.Items
	var provs v1alpha1.ProviderList
	if err := c.List(ctx, &provs); err != nil {
		return in, nil, fmt.Errorf("list the Providers: %w", err)
	}
	in.Providers = provs.Items
	var profiles v1alpha1.PlatformProfileList
	if err := c.List(ctx, &profiles); err != nil {
		return in, nil, fmt.Errorf("list the PlatformProfiles: %w", err)
	}
	in.Profiles = profiles.Items
	return in, unreadable, nil
}

// printAccounting writes the report as text: the heading, then one line per (grant, account, unit). No
// line adds one unit to another.
func printAccounting(w io.Writer, r accounting.Report) {
	fmt.Fprintln(w, accountingHeading)
	fmt.Fprintln(w, "Billed units are the scheduler's billing count times the allocated hours, in each partition's unit; a unit is never added to another. Each line counts its unknown members beside the sum of the known ones.")
	if len(r.Groups) == 0 {
		fmt.Fprintln(w, "no accounting records and no Allocations.")
	}
	for _, g := range r.Groups {
		fmt.Fprintln(w, groupLine(g, r.Now.Time))
	}
	if n := len(r.Unreadable); n > 0 {
		fmt.Fprintf(w, "%d accounting record(s) could not be read and are in no total: %s\n", n, strings.Join(r.Unreadable, ", "))
	}
}

func groupLine(g accounting.Group, now time.Time) string {
	line := fmt.Sprintf("grant %s, account %s: %s, %d unknown of %d member(s)",
		nameOr(g.Grant), nameOr(g.Account), unitTotal(g), g.Unknown, g.Members)
	if g.GrantEnd == nil {
		return line
	}
	end := g.GrantEnd.UTC().Format("2006-01-02")
	if g.GrantEnd.After(now) && g.DaysLeft != nil {
		return fmt.Sprintf("%s; %d day(s) left (grant ends %s)", line, *g.DaysLeft, end)
	}
	return fmt.Sprintf("%s; the grant ended %s", line, end)
}

// nameOr is the name, or "(unknown)" for the empty name of a member that could not be placed.
func nameOr(s string) string {
	if s == "" {
		return "(unknown)"
	}
	return s
}

// unitTotal is a group's sum with its unit: "7.0 GPU-hours".
func unitTotal(g accounting.Group) string {
	switch g.Unit {
	case v1alpha1.BillingUnitGPUHour:
		return hours(g.Billed) + " GPU-hours"
	case v1alpha1.BillingUnitCPUHour:
		return hours(g.Billed) + " CPU-hours"
	}
	return hours(g.Billed) + " (unit unknown)"
}

// hours prints a count of hours to two decimals, without a trailing zero but the first decimal: 7.0,
// 4.5, 0.33.
func hours(v float64) string {
	s := strconv.FormatFloat(v, 'f', 2, 64)
	if strings.HasSuffix(s, "0") {
		s = s[:len(s)-1]
	}
	return s
}
