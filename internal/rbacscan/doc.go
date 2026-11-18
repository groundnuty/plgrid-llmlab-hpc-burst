// Package rbacscan checks, statically, that every API call a controller identity makes is granted by that identity's role
// in config/rbac/controllers.yaml, and that every verb a role grants is made by some call (HBEP-10, Audit; issues #660
// and #684).
//
// A refused controller call does not always fail a test: a controller that is answered 403 logs the error, joins it to
// its pass's error and goes on, and some paths (the lifecycle controller's ModelDeployment and Operation writes) no
// in-process suite runs under the identity at all. Three such silent seams were found by hand on 17 Nov 2026
// (#664's calls, #707's Operation finalizer, #715's label patch). This package finds them by reading the code.
//
// How it reads the code. go/packages type-checks the closure of the controller commands, from source, and go/ssa
// builds the functions of the module. A call site is an invocation of a controller-runtime client method (Get, List,
// Create, Update, Patch, Delete, DeleteAllOf, Apply, through Status() and SubResource() too), of a typed client-go
// clientset's, of a dynamic informer factory's, or the start of an informer (the builder's For, Owns and Watches, the
// cache's GetInformer, a field index, source.Kind). Its requirement is a set of (apiGroup, resource, verb): a read
// through a manager's cached client (GetClient, GetCache) is a LIST and a WATCH of the kind, since that is the
// informer's; a read through its API reader (GetAPIReader) or a typed clientset is a GET or a LIST on its own; a write
// is its own verb, on the resource or on its status.
//
// Which identity makes a call, and through which kind of client, is not told by the package it is in (a package such as
// eventcount, or providerctl/supervisor.go reading through the lifecycle manager's client, is shared): it is found by
// following the receiver of the call, through fields, parameters, results, phis, interface wrappers and closures, to the
// call that made the client: a manager's GetClient, GetCache or GetAPIReader, or a clientset's constructor. A manager
// belongs to the identity that its ctrl.NewManager call is listed under (Options.Managers), a clientset to the one its
// constructor is listed under (Options.Clients). A receiver that cannot be followed to such a call is reported, never
// skipped.
//
// What the walk cannot see, the package takes from two lists, each entry of which says why:
//   - the calls a LIBRARY makes on the module's behalf (crossplane-runtime's managed reconciler and finalizer, client-go's
//     Lease lock and event broadcasters): an Allowlist entry names the call that triggers it (so the rule goes when the
//     call does), the identity it is for, the rights it stands for and the pinned library source it was read from, which
//     the test checks against go.mod and the module cache;
//   - a call whose resource is not static (a generic helper taking client.Object, a dynamic informer's GVR): an annotation
//     "//rbac:resources group/resource ..." on the call's line or the line above names the resources it serves. A call of
//     this kind with no annotation is a problem.
//
// A call in code that nothing calls (a function whose client parameter no call fills: the Operation removal controller until
// #660 wires it, helpers only tests call) is not live: Result.Dead lists it, and its rights are not counted. When something
// calls the function, its calls are read like any other and the role must grant them; when a live function loses its last
// caller, the rights it needed are rules nothing calls.
//
// What this does not prove. A struct type's client fields are followed across every value of the type, so a type that two
// identities fill would give each the other's calls through it. A fallback that only a test takes ("if reader == nil {
// reader = client }") would give a field two kinds of client, and is left out by position (Options.SkipStores, checked for
// staleness). An annotation is trusted to name every resource its call serves, and a library entry to name every request its
// library makes; the entry's citation is checked against go.mod and the module cache, not against the library's behaviour.
// A Lease's resourceNames are the launcher's drift guard's. A call made by reflection, or by a client the scan does not read
// (a hand-built REST request outside the exempt packages is a problem), is not seen.
//
// The roles stay hand-written. A role generated from the list this package reads would prove nothing: the package
// checks the roles against the code, in both directions, and a mutant of the role (a verb removed, a rule added) fails.
package rbacscan
