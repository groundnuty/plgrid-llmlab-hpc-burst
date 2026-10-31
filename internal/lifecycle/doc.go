// Package lifecycle is the lifecycle controller of HBEP-7: it keeps each
// ModelDeployment's desired state true, through Allocations and Instances.
//
// It never calls a provider. It creates and deletes Allocations, which each
// Provider's controller reconciles (HBEP-4), and reads their status and the
// Providers' status. It implements agentapi.Backend, the controller's side of
// the agent API, which the agent API server (issue #60) calls after it has
// authenticated an agent.
//
// # The work queue
//
// One controller-runtime controller, keyed per ModelDeployment (namespace and
// name), with several workers (four by default); one key is never reconciled
// by two workers at once. Allocations map to their ModelDeployment through
// spec.modelDeploymentRef, Instances through their controller owner reference,
// Providers and PlatformProfiles to every ModelDeployment that allows them, and
// agent calls to the ModelDeployment of the caller's Allocation.
//
// # State
//
// Every decision is made from API objects, re-read at each pass, and from the
// agents' latest reports, which the Backend keeps in memory: a report is a full
// snapshot, so a restarted controller has every agent's state again within one
// report interval. What must survive a restart is written to the API: the
// Instance's phase, conditions, times and registration; the restart
// generations (annotations); the verification records; the ModelDeployment's
// start-to-ready times. Queue waits are the Provider's status (HBEP-4). Plans
// are computed from those values only, in whole seconds as metav1.Time keeps
// them, so a restarted controller plans the same times.
//
// The Reconciler reads its own objects (the ModelDeployment, its Instances and
// Allocations, verification records) through an uncached reader in
// production, so an object it has just created is never missed by the next
// pass and nothing is created twice.
//
// # Time
//
// The package never reads the wall clock directly: every component takes a
// Clock, so tests drive time by hand.
package lifecycle
