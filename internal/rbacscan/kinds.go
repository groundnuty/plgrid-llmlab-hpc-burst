package rbacscan

import (
	"fmt"
	"go/types"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/tools/go/ssa"
	"sigs.k8s.io/yaml"
)

// kindInfo is where a kind lives in the API: its group and its resource.
type kindInfo struct{ group, resource string }

// builtinKinds are the kinds of the Kubernetes API the controllers use, by the Go type that holds them. A kind of the
// module is read from its CRD (config/crd) instead.
var builtinKinds = map[string]kindInfo{
	"k8s.io/api/apps/v1.ControllerRevision": {"apps", "controllerrevisions"},
	"k8s.io/api/coordination/v1.Lease":      {"coordination.k8s.io", "leases"},
	"k8s.io/api/core/v1.Event":              {"", "events"},
	"k8s.io/api/events/v1.Event":            {"events.k8s.io", "events"},
	"k8s.io/api/core/v1.Secret":             {"", "secrets"},
	"k8s.io/api/core/v1.ConfigMap":          {"", "configmaps"},
}

// loadKinds reads the module's kinds from the CRDs.
func (s *scanner) loadKinds() (map[string]kindInfo, error) {
	out := map[string]kindInfo{}
	for k, v := range builtinKinds {
		out[k] = v
	}
	if s.o.CRDDir == "" {
		return out, nil
	}
	files, err := filepath.Glob(filepath.Join(s.o.CRDDir, "*.yaml"))
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no CRD in %s", s.o.CRDDir)
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		var crd struct {
			Spec struct {
				Group string `json:"group"`
				Names struct {
					Kind   string `json:"kind"`
					Plural string `json:"plural"`
				} `json:"names"`
			} `json:"spec"`
		}
		if err := yaml.Unmarshal(b, &crd); err != nil {
			return nil, fmt.Errorf("%s: %w", filepath.Base(f), err)
		}
		if crd.Spec.Names.Kind == "" || crd.Spec.Names.Plural == "" {
			return nil, fmt.Errorf("%s names no kind or plural", filepath.Base(f))
		}
		out[s.l.mod+"/api/v1alpha1."+crd.Spec.Names.Kind] = kindInfo{crd.Spec.Group, crd.Spec.Names.Plural}
	}
	return out, nil
}

// kindOf returns the group and resource of a named type, or of the item type of its List.
func (s *scanner) kindOf(t types.Type) (kindInfo, bool) {
	n, ok := t.(*types.Named)
	if !ok || n.Obj().Pkg() == nil {
		return kindInfo{}, false
	}
	key := n.Obj().Pkg().Path() + "." + n.Obj().Name()
	if k, ok := s.kinds[key]; ok {
		return k, true
	}
	if base, ok := strings.CutSuffix(key, "List"); ok {
		if k, ok := s.kinds[base]; ok {
			return k, true
		}
	}
	return kindInfo{}, false
}

// callee returns the function or method a call calls, from the declaring interface or the generic function, or nil.
func callee(cc *ssa.CallCommon) *types.Func {
	if cc.IsInvoke() {
		return cc.Method
	}
	fn := cc.StaticCallee()
	if fn == nil || strings.Contains(fn.Synthetic, "bound method wrapper") || strings.Contains(fn.Synthetic, "thunk") {
		return nil // a method taken as a value: methodValues reports it where it is taken
	}
	if o := fn.Origin(); o != nil {
		fn = o
	}
	m, _ := fn.Object().(*types.Func)
	return m
}

// verbOf maps a client method to its verb.
var writeVerbs = map[string]string{
	"Create": "create", "Update": "update", "Patch": "patch", "Apply": "patch", "Delete": "delete", "DeleteAllOf": "deletecollection",
}

// apiMethod says whether a method is one of the calls the scan reads: a controller-runtime client's reader or writer, a
// typed client's resource client, a dynamic client's.
func apiMethod(m *types.Func) bool {
	if m == nil || m.Pkg() == nil {
		return false
	}
	pkg, name, rn := m.Pkg().Path(), m.Name(), recvName(m)
	switch {
	case pkg == pkgClient && (rn == "Reader" || rn == "Writer" || rn == "SubResourceReader" || rn == "SubResourceWriter"):
		return name == "Get" || name == "List" || writeVerbs[name] != ""
	case strings.HasPrefix(pkg, pkgTyped) && strings.HasSuffix(rn, "Interface"):
		return typedVerbs[name] != ""
	case pkg == pkgDynamic && rn == "ResourceInterface":
		return dynVerbs[name] != ""
	}
	return false
}

// typedVerbs are the methods of a typed client's resource interface that make a request.
var typedVerbs = map[string]string{
	"Get": "get", "List": "list", "Watch": "watch", "Create": "create", "Update": "update", "UpdateStatus": "update",
	"Delete": "delete", "DeleteCollection": "deletecollection", "Patch": "patch", "Apply": "patch", "ApplyStatus": "patch",
	"CreateToken": "create",
}

var typedSub = map[string]string{"UpdateStatus": "status", "ApplyStatus": "status", "CreateToken": "token"}

// typedGroups maps the directory of client-go's typed clients to the API group.
var typedGroups = map[string]string{
	"core": "", "apps": "apps", "coordination": "coordination.k8s.io", "events": "events.k8s.io",
	"authentication": "authentication.k8s.io", "authorization": "authorization.k8s.io", "rbac": "rbac.authorization.k8s.io",
	"batch": "batch", "storage": "storage.k8s.io", "networking": "networking.k8s.io", "policy": "policy",
}

func plural(kind string) string {
	l := strings.ToLower(kind)
	switch {
	case strings.HasSuffix(l, "y"):
		return strings.TrimSuffix(l, "y") + "ies"
	case strings.HasSuffix(l, "s"):
		return l + "es"
	}
	return l + "s"
}

var dynVerbs = map[string]string{
	"Get": "get", "List": "list", "Watch": "watch", "Create": "create", "Update": "update", "UpdateStatus": "update",
	"Delete": "delete", "DeleteCollection": "deletecollection", "Patch": "patch", "Apply": "patch", "ApplyStatus": "patch",
}
