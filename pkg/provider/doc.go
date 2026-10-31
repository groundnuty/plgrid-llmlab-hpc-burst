// Package provider is HBEP-4's core interface: the operations a provider
// plugin implements, their outcomes and error kinds, and the one alpha
// capability flag.
//
// Who calls it. Only a Provider's own controller calls a platform (HBEP-4, Two
// controllers). For a Slurm Provider that controller is crossplane-runtime's
// managed reconciler over the Allocations naming the Provider. Its external
// client maps onto these operations: Connect is ReadProfile; Observe is
// FindByTag, adoption, settle (Cancel and LiftHold) and Observe; Create is
// EnsureFile, a claimed attempt and Submit; Delete is Cancel and
// RemoveAllocationFiles. The lifecycle controller (HBEP-7) never imports this
// package's operations: it creates and deletes Allocations and reads their
// status and their Provider's status. Package provider/fake has an in-memory
// Batch for the plugin's reconciler tests, and a stand-in for a Provider's
// controller that drives Allocation and Provider status for the lifecycle
// controller's tests.
//
// Unknown is never none. Every operation reports per-job results. An error from
// a read (FindByTag, Observe) means the plugin cannot tell: it is never read as
// "no job" or "failed", and it never counts towards the profile's lostGrace.
// Observe's found is false only when both the queue listing and accounting
// were read successfully and neither shows the job. Every time is in UTC.
//
// Idempotency. Every operation except Submit is idempotent (HBEP-10). Submit
// runs only under a claimed attempt (HBEP-7, open question 11) and never
// retries by itself.
package provider
