package agentcli

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
	"unicode/utf8"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// opRequest is one Operation to make: its name, and its spec but for the owner, which is the caller's.
type opRequest struct {
	name string
	spec v1alpha1.OperationSpec
}

// cmdScale is hpcburst scale <md> --instances N.
func (rt *cli) cmdScale(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs, o := newFlags("scale", stderr, true)
	instances := fs.Int("instances", -1, "the number of Instances the ModelDeployment is to have (required; below its count)")
	md, code, done := oneArg(fs, args, "ModelDeployment name")
	if done {
		return code
	}
	if *instances < 0 || *instances > math.MaxInt32 {
		return failf(stderr, exitUsage, "scale: --instances N is required, between 0 and %d", math.MaxInt32)
	}
	if code := checkOptions(o, stderr); code != exitOK {
		return code
	}
	spec := v1alpha1.OperationSpec{Action: v1alpha1.OperationActionScale, ModelDeploymentRef: v1alpha1.ObjectReference{Name: md},
		Scale: &v1alpha1.ScaleParameters{Instances: int32(*instances)}}
	return rt.createFlow(ctx, o, []opRequest{{name: rt.nameFor(o), spec: spec}}, stdin, stdout, stderr)
}

// cmdEnd is hpcburst end <md> [--delete].
func (rt *cli) cmdEnd(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs, o := newFlags("end", stderr, true)
	del := fs.Bool("delete", false, "remove the ModelDeployment, which releases its started Allocations at once")
	md, code, done := oneArg(fs, args, "ModelDeployment name")
	if done {
		return code
	}
	if code := checkOptions(o, stderr); code != exitOK {
		return code
	}
	spec := v1alpha1.OperationSpec{Action: v1alpha1.OperationActionEnd, ModelDeploymentRef: v1alpha1.ObjectReference{Name: md}}
	if *del {
		spec.End = &v1alpha1.EndParameters{Delete: true}
	}
	return rt.createFlow(ctx, o, []opRequest{{name: rt.nameFor(o), spec: spec}}, stdin, stdout, stderr)
}

// nameFor is the Operation's name: the key, else a generated one.
func (rt *cli) nameFor(o *options) string {
	if o.key != "" {
		return o.key
	}
	return rt.newName()
}

// checkOptions refuses a flag that cannot make an Operation, before anything is created.
func checkOptions(o *options, stderr io.Writer) int {
	if n := utf8.RuneCountInString(o.purpose); n > v1alpha1.PurposeMaxLength {
		return failf(stderr, exitUsage, "--purpose holds %d characters; at most %d", n, v1alpha1.PurposeMaxLength)
	}
	if o.key != "" {
		if errs := validation.IsDNS1123Subdomain(o.key); len(errs) > 0 {
			return failf(stderr, exitUsage, "--key %q is not a valid Operation name (at most %d characters of a DNS-1123 subdomain): %s", o.key, maxName, errs[0])
		}
	}
	if o.timeout <= 0 {
		return failf(stderr, exitUsage, "--timeout must be above zero")
	}
	return exitOK
}

// session is one run of scale, end or release: the caller, the namespace and what is printed.
type session struct {
	rt             *cli
	cl             *caller
	ns             string
	o              *options
	in             *bufio.Reader
	stdout, stderr io.Writer
}

// createFlow makes the Operations, then waits for each plan, prints it, and confirms it where the caller
// says so at a terminal. The caller's name goes in each Operation as its owner.
func (rt *cli) createFlow(ctx context.Context, o *options, reqs []opRequest, stdin io.Reader, stdout, stderr io.Writer) int {
	cl, ns, code := rt.open(ctx, o, stderr)
	if code != exitOK {
		return code
	}
	s := &session{rt: rt, cl: cl, ns: ns, o: o, in: bufio.NewReader(stdin), stdout: stdout, stderr: stderr}
	var made []*v1alpha1.Operation
	worst := exitOK
	for _, r := range reqs {
		op, code := s.create(ctx, r)
		if code != exitOK {
			worst = code
			break
		}
		made = append(made, op)
	}
	var views []operationView
	for _, op := range made {
		v, code := s.settle(ctx, op)
		views = append(views, v)
		worst = max(worst, code)
	}
	if o.asJSON && len(views) > 0 {
		worst = max(worst, emitCode(stderr, emitJSON(stdout, map[string]any{"schemaVersion": schemaVersion, "operations": views})))
	}
	return worst
}

// create makes one Operation. The name is printed first, so a caller that is killed mid-call still has
// it. An Operation that exists under the name is the same request or a different one.
func (s *session) create(ctx context.Context, r opRequest) (*v1alpha1.Operation, int) {
	r.spec.Owner = s.cl.user
	op := &v1alpha1.Operation{ObjectMeta: metav1.ObjectMeta{Name: r.name, Namespace: s.ns}, Spec: r.spec}
	if s.o.purpose != "" {
		op.Annotations = map[string]string{v1alpha1.AnnotationPurpose: s.o.purpose}
	}
	if s.o.asJSON {
		fmt.Fprintf(s.stderr, "operation: %s\n", r.name)
	} else {
		fmt.Fprintf(s.stdout, "operation: %s\n", r.name)
	}
	err := s.cl.c.Create(ctx, op)
	switch {
	case err == nil:
		return op, exitOK
	case !apierrors.IsAlreadyExists(err):
		return nil, failf(s.stderr, exitFail, "create Operation %s: %v", r.name, err)
	}
	have, err := getOperation(ctx, s.cl.c, s.ns, r.name)
	if err != nil {
		return nil, failf(s.stderr, exitFail, "%v", err)
	}
	if !sameRequest(&have.Spec, &op.Spec) {
		return nil, failf(s.stderr, exitFail, "Operation %s exists with a different request", r.name)
	}
	fmt.Fprintf(s.stderr, "hpcburst: Operation %s exists with the same request: continuing with it\n", r.name)
	return have, exitOK
}

// requestKey is an Operation's request in a form that compares: the action, the ModelDeployment, the
// parameters and the owner. An absent end block is {delete: false}, the Allocation names are a set, and
// a selector is its canonical string. The purpose and the confirm are not part of the request.
type requestKey struct {
	action, md, owner string
	instances         int32
	hasScale          bool
	del               bool
	allocations       string
	selector          string
}

func keyOf(s *v1alpha1.OperationSpec) requestKey {
	k := requestKey{action: string(s.Action), md: s.ModelDeploymentRef.Name, owner: s.Owner}
	if s.Scale != nil {
		k.hasScale, k.instances = true, s.Scale.Instances
	}
	k.del = s.End != nil && s.End.Delete
	if r := s.Release; r != nil {
		names := append([]string(nil), r.Allocations...)
		slices.Sort(names)
		k.allocations = strings.Join(names, "\x00")
		if r.Selector != nil {
			k.selector = metav1.FormatLabelSelector(r.Selector)
		}
	}
	return k
}

func sameRequest(a, b *v1alpha1.OperationSpec) bool { return keyOf(a) == keyOf(b) }

// settle waits for the plan of an Operation, prints it, and confirms it where the caller answers yes at a
// terminal. Otherwise it prints the line that confirms it, and confirms nothing.
func (s *session) settle(ctx context.Context, op *v1alpha1.Operation) (operationView, int) {
	cur, timedOut, err := s.rt.awaitPlan(ctx, s.cl.c, op, s.o.timeout)
	v := operationViewOf(cur, s.rt.now())
	switch {
	case err != nil:
		return v, failf(s.stderr, exitFail, "%v", err)
	case cur.Status.Phase == v1alpha1.OperationPhaseRefused:
		return v, failf(s.stderr, exitFail, "Operation %s was refused: %s", cur.Name, conditionMessage(cur, v1alpha1.ConditionExecutable))
	case timedOut:
		return v, failf(s.stderr, exitFail, "Operation %s has no plan after %s; the controller may not be running. It exists: hpcburst operation %s%s", cur.Name, s.o.timeout, cur.Name, inNamespace(s.ns))
	}
	if !s.o.asJSON {
		printPlan(s.stdout, *v.Plan)
	}
	planID := cur.Status.Plan.ID
	switch {
	case cur.Spec.Confirm != nil:
		return s.reportConfirmed(cur, v)
	case s.o.asJSON || !s.rt.env.terminal():
		if !s.o.asJSON {
			fmt.Fprintf(s.stdout, "confirm with: %s\n", v.ConfirmWith)
		}
		return v, exitOK
	case !s.askYes(planID):
		fmt.Fprintf(s.stdout, "not confirmed; confirm with: %s\n", v.ConfirmWith)
		return v, exitOK
	}
	return s.confirm(ctx, cur, planID)
}

// reportConfirmed handles an Operation that was already confirmed: the plan is shown, nothing is asked.
func (s *session) reportConfirmed(op *v1alpha1.Operation, v operationView) (operationView, int) {
	if !s.o.asJSON {
		fmt.Fprintf(s.stdout, "plan %s is already confirmed\n", op.Spec.Confirm.PlanID)
		printWaiting(s.stdout, op, v)
	}
	return v, exitOK
}

// askYes asks "confirm plan <id>? [y/N]" and reads one line: only y or yes confirms.
func (s *session) askYes(planID string) bool {
	fmt.Fprintf(s.stdout, "confirm plan %s? [y/N] ", planID)
	line, _ := s.in.ReadString('\n')
	switch strings.ToLower(strings.TrimSpace(line)) {
	case "y", "yes":
		return true
	}
	return false
}

// confirm confirms the plan the caller was shown, by its ID, and prints the phase reached.
func (s *session) confirm(ctx context.Context, op *v1alpha1.Operation, planID string) (operationView, int) {
	got, outcome, err := confirmPlan(ctx, s.cl.c, s.ns, op.Name, planID)
	if err != nil {
		return operationViewOf(op, s.rt.now()), failf(s.stderr, exitFail, "%v", err)
	}
	if outcome == confirmChanged {
		return operationViewOf(got, s.rt.now()), s.rt.reportChanged(s.stdout, s.stderr, got, planID, false)
	}
	got, err = s.rt.awaitPhaseAfterConfirm(ctx, s.cl.c, got, s.o.timeout)
	v := operationViewOf(got, s.rt.now())
	if err != nil {
		return v, failf(s.stderr, exitFail, "%v", err)
	}
	fmt.Fprintf(s.stdout, "confirmed plan %s\n", planID)
	printWaiting(s.stdout, got, v)
	return v, exitOK
}
