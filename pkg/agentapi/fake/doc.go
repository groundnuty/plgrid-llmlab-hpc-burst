// Package fake holds two test doubles for the agent protocol (package
// agentapi): a Backend, which stands in for the lifecycle controller, and an
// Agent, which drives a Backend as the real allocation agent would.
//
// The lifecycle controller's tests (issue #61) drive its Backend
// implementation with an Agent; the agent API server's tests (issue #60) call
// a Backend through the server. The Agent's own tests run against this
// package's Backend.
//
// The package imports only the standard library and package agentapi, because
// the agent is a static binary and this package must not drag the Kubernetes
// libraries into it.
//
// Neither double has a goroutine or a timer. The Agent works one Step at a
// time and takes its time from a function the test injects, so a test decides
// when a worker starts and when a report is due.
package fake
