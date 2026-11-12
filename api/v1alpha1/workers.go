package v1alpha1

// WorkersPerInstanceCount is the deployment's workers per Instance: spec.workersPerInstance, absent being 1
// (HBEP-7, Several workers).
func (s *ModelDeploymentSpec) WorkersPerInstanceCount() int32 {
	if s.WorkersPerInstance == nil {
		return 1
	}
	return *s.WorkersPerInstance
}

// LaunchWorkers is the Instance's number of workers: launch.workers, absent being 1, alpha's one worker
// (HBEP-7, Several workers).
func LaunchWorkers(l Launch) int32 {
	if l.Workers == nil {
		return 1
	}
	return *l.Workers
}
