package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s3/kit"
)

type e2eResult struct {
	Passed  []string `json:"passed"`
	Failed  []string `json:"failed"`
	Skipped []string `json:"skipped"`
	Exit    int      `json:"exit"`
	Log     string   `json:"log"`
}

var testLine = regexp.MustCompile(`^--- (PASS|FAIL|SKIP): (\S+)`)

func restConfig(kubeconfig, ctxName string) (*rest.Config, error) {
	rules := &clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfig}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{CurrentContext: ctxName}).ClientConfig()
}

func newClient(kubeconfig, ctxName string) (client.Client, error) {
	rc, err := restConfig(kubeconfig, ctxName)
	if err != nil {
		return nil, err
	}
	rc.QPS, rc.Burst = 200, 400
	return client.New(rc, client.Options{Scheme: kit.Scheme()})
}

// runE2E runs the one compiled e2e binary and parses its verbose output. The binary
// is started as a harness-owned process so its RSS can be sampled by the caller.
func (b *bench) runE2E(kubeconfig, tag string, runPattern string, env []string, onStart func(p *proc)) (e2eResult, error) {
	logPath := filepath.Join(b.scratch, "logs", tag+"-e2e.log")
	_ = os.Remove(logPath)
	args := []string{b.e2eBin, "-test.v", "-test.timeout", "25m"}
	if runPattern != "" {
		args = append(args, "-test.run", runPattern)
	}
	p, err := startProc("e2e.test", logPath, append([]string{"S3_KUBECONFIG=" + kubeconfig}, env...), args...)
	if err != nil {
		return e2eResult{}, err
	}
	if onStart != nil {
		onStart(p)
	}
	<-p.done
	res := e2eResult{Log: logPath}
	if p.cmd.ProcessState != nil {
		res.Exit = p.cmd.ProcessState.ExitCode()
	}
	f, err := os.Open(logPath)
	if err != nil {
		return res, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		if m := testLine.FindStringSubmatch(sc.Text()); m != nil {
			switch m[1] {
			case "PASS":
				res.Passed = append(res.Passed, m[2])
			case "FAIL":
				res.Failed = append(res.Failed, m[2])
			case "SKIP":
				res.Skipped = append(res.Skipped, m[2])
			}
		}
	}
	sort.Strings(res.Passed)
	sort.Strings(res.Failed)
	return res, nil
}

// objRec identifies one stored object across an upgrade.
type objRec struct{ UID, Created string }

// snapshot records UID and creation time of every CRD and every custom object of the load population.
func snapshot(ctx context.Context, c client.Client) (map[string]objRec, error) {
	out := map[string]objRec{}
	for _, g := range []struct {
		l    *unstructured.UnstructuredList
		kind string
	}{{kit.NewList(kit.FleetGVK), "Fleet"}, {kit.NewList(kit.WorkerGVK), "Worker"}} {
		if err := c.List(ctx, g.l, client.InNamespace("s3-load")); err != nil {
			return nil, err
		}
		for _, o := range g.l.Items {
			out[g.kind+"/"+o.GetName()] = objRec{string(o.GetUID()), o.GetCreationTimestamp().UTC().Format(time.RFC3339Nano)}
		}
	}
	crds := &apiextv1.CustomResourceDefinitionList{}
	if err := c.List(ctx, crds); err != nil {
		return nil, err
	}
	for _, o := range crds.Items {
		if strings.HasSuffix(o.Name, "."+kit.Group) {
			out["CRD/"+o.Name] = objRec{string(o.UID), o.CreationTimestamp.UTC().Format(time.RFC3339Nano)}
		}
	}
	return out, nil
}

// compareSnapshots returns how many objects are identical, and a description of every difference.
func compareSnapshots(before, after map[string]objRec) (kept int, diffs []string) {
	for k, b := range before {
		a, ok := after[k]
		switch {
		case !ok:
			diffs = append(diffs, k+": missing after upgrade")
		case a != b:
			diffs = append(diffs, fmt.Sprintf("%s: changed (uid %s->%s)", k, b.UID, a.UID))
		default:
			kept++
		}
	}
	for k := range after {
		if _, ok := before[k]; !ok {
			diffs = append(diffs, k+": new after upgrade")
		}
	}
	sort.Strings(diffs)
	return
}

func serverVersion(kubeconfig, ctxName string) string {
	rc, err := restConfig(kubeconfig, ctxName)
	if err != nil {
		return "unknown"
	}
	dc, err := discovery.NewDiscoveryClientForConfig(rc)
	if err != nil {
		return "unknown"
	}
	v, err := dc.ServerVersion()
	if err != nil {
		return "unknown"
	}
	return v.GitVersion
}
