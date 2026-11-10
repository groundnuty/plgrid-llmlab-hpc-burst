// Package keeperproc holds the relay keeper's tests that need processes of
// their own (HBEP-6, The relay keeper, Test plan; issue #343, PR 3). It has
// no code outside its tests.
//
// HBEP-6 rules out, as proof of the keeper's beta items, a keeper and a
// controller in one process, a relay test with both ends in one process and
// no srun standard input and output between them, a count of relays with no
// byte crossing, and a restart or hand-over test with no stream running. So
// every test here runs, as processes of their own:
//   - the keeper: this test binary re-run as relaykeeper.Run, with its state
//     directory, its socket and its relays' ssh;
//   - one or more controller doubles: this test binary re-run to send desired
//     sets and acknowledgements over the keeper's socket with an explicit
//     Lease epoch, as the test tells it on its standard input;
//   - the job side: the rehearsal's login node (a real sshd), the fake
//     Slurm's srun -u --overlap step running this binary as the agent's
//     "relay <channel>", and a job whose batch process is this binary as the
//     agent's job side (hostdial.StartJob);
//
// and a serving stream crosses each test's event, its bytes checked whole
// and in order at the control-plane end.
//
// Provenance: new package, for #343. Its harness copies
// internal/hostdial/harness_test.go's and pins_test.go's (the world, the
// relay and job roles, the targets, the pins), which are test files and so
// cannot be imported.
package keeperproc
