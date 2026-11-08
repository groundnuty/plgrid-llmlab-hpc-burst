// Package rb stands for the packages that build event recorders (controller-runtime's
// manager, client-go's broadcasters): the scan test names it as the package of the
// builders.
package rb

// Recorder stands for client-go's event recorder.
type Recorder interface{ Emit(string) }

// Logger is a recorder with one more method, as client-go's EventRecorderLogger
// is: a value of it converts into a Recorder.
type Logger interface {
	Recorder
	Name() string
}

// Manager builds a recorder, as controller-runtime's Manager does.
type Manager interface {
	GetEventRecorder(name string) Recorder
}

// Broadcaster builds a recorder, as client-go's event broadcaster does, through
// a method on a pointer.
type Broadcaster struct{}

// NewRecorder returns a recorder that can be converted to a Recorder.
func (*Broadcaster) NewRecorder(name string) Logger { return nil }
