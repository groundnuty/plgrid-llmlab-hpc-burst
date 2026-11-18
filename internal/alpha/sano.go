package alpha

// The Sano rows of issue #735 (the science agent's plan, step 4): alpha's handover row and row A's serve flow on Sano's
// partition gpu under host-dialed mode, one A100 each, and the pre-staging row that stages the engine image and the
// 7B's checkpoint before them, all through the real agent and stager (the science agent's ruling of 18 Nov 2026: the rows
// run the agent's own launch unchanged, and a failure is reported, never patched around).
//
// How each differs from its Athena twin, and why no existing row can run on Sano (ConfigFromEnv refuses a script whose
// profile, partition or GPU count differs from the row's, and the live world reads the row's own):
//
//   - The profile is profiles/sano.yaml, whose clock is the framework's default (drainMargin 10m, handoverMargin 30m,
//     health.startTimeout 1h): the lead an Instance needs is Lead() and the shortest lease MinLease(), as the Athena
//     handover row's, not athena-now's. A cold start fits the profile's start timeout, but the serve row starts warm all
//     the same, from the pre-staging row's staging (warmStartShownByTheAgentsOwnLog), so that what it shows is the agent's
//     own staging, not a cold pull that happened to fit.
//   - There is no QOS in the row: the partition's qosByGpus chooses gpu1 for one GPU (HBEP-4, The Slurm dialect, #745),
//     and a Provider on such a partition sets none (ReadProfile refuses both). Row.QOS means the Provider's own qos and,
//     on Helios, the owner's one-job slot; neither is Sano's, so it stays empty. The script's estimate sends the chosen QOS
//     through its extra column (hack/live-checks.sh, cluster_table).
//   - There is no one-job slot, no owner's go and no start wait: Sano's gpu1 has per-job caps and no per-user limit
//     (profiles/sano.yaml, qos), so a handover's two Allocations run at once, and a waiting job holds nobody's slot.
//   - The Provider gives its own spec.stagingDir (Row.ProviderStaging; #757): a Sano path carries a user name, so the
//     profile holds none. The harness takes it from HPCBURST_ALPHA_STAGING_DIR and it never reaches a log.
//   - The deployment is the 7B (Row.Deployment), by its pinned revision.

import "time"

// The Sano rows' names.
const (
	SanoStageName    = "alpha-sano-stage"
	SanoServeName    = "alpha-sano-serve"
	SanoHandoverName = "alpha-sano-handover"
)

// SanoDeployment is the example ModelDeployment the Sano rows serve and the stage row stages the inputs of.
const SanoDeployment = "modeldeployment-sano-qwen25-7b-instruct.yaml"

// SanoStageLease is the pre-staging row's time limit, the Athena stage row's 30 minutes (NowStageLease): the conformance harness's stage body
// bounds its waits by that one limit (stageTimeLimit, stageEndWait). Part 1 of #735 measured a cold pull of the engine image at 849 s with 11
// CPUs, the 7B's weights at 186 s (14 GiB) and 62 s to hash them, 18 minutes in all, so 30 minutes is that and two thirds again, and a staging
// past it is a FAIL that names it (stageVerdict).
const SanoStageLease = NowStageLease

// SanoStageBudget is the stage row's timeout, the Athena stage row's (NowStageBudget): the script adds one wait in the queue on top, as it does
// for every row without a start wait, and the conformance package's budget test adds up the rest from the stage body's source.
const SanoStageBudget = NowStageBudget

// SanoHandoverRow is the handover on Sano: two A100 Allocations at once on gpu, one stream across the drain start, one
// restart at the worker rung. It is alpha-athena-handover's shape (the same lease and clock) on another platform.
func SanoHandoverRow() Row {
	return Row{Name: SanoHandoverName, Cluster: "sano", Partition: "gpu", GPUs: 1, Lease: MinLease(), Budget: 150 * time.Minute, MostJobs: 2,
		PostsTo: "#735", Deployment: SanoDeployment, ProviderStaging: true}
}

// SanoServeRow is row A's flow on Sano: one job of one A100, the long stream with attribution, the deployment ID under its
// tag, HBEP-8's events and gauges, one worker-rung restart, and the deployment's end with its job seen ended. The lease is
// the shortest the profile's clock allows (MinLease): the run ends well before it, by deleting its deployment.
func SanoServeRow() Row {
	return Row{Name: SanoServeName, Cluster: "sano", Partition: "gpu", GPUs: 1, Lease: MinLease(), Budget: 150 * time.Minute, MostJobs: 1,
		PostsTo: "#735", Deployment: SanoDeployment, ProviderStaging: true}
}

// SanoStageRow is the pre-staging row on Sano: one job whose agent build is the stage helper, which calls the real stager,
// so that the engine image and the 7B's checkpoint are staged where the agent stages, under the digest's lock with the
// integrity records it writes, in the Provider's own staging directory (which the harness keeps). The serve row then starts
// warm. It takes one gpu1 job because the framework cannot submit a CPU-only job on Sano's gpu partition: qosByGpus
// gives a count of 0 the first entry's QOS, gpu1, which the plugin refuses without a GPU (api/v1alpha1/qos.go, QOSForGPUs).
func SanoStageRow() Row {
	return Row{Name: SanoStageName, Cluster: "sano", Partition: "gpu", GPUs: 1, Lease: SanoStageLease, Budget: SanoStageBudget, MostJobs: 1,
		PostsTo: "#735", Deployment: SanoDeployment, ProviderStaging: true}
}

// IsSanoServe and IsSanoHandover say which flow a row of the Sano family runs; the dispatch of Run, WorstCase and the dry steps
// takes them with the Athena names.
func IsSanoServe(name string) bool    { return name == SanoServeName }
func IsSanoHandover(name string) bool { return name == SanoHandoverName }
