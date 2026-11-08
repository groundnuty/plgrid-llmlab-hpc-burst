// Package recorders builds recorders in each way the scan must accept or refuse.
package recorders

import (
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rb"
)

type stash struct{ r rb.Recorder }

// Wrapped: the value is the first argument of a counter.

func countedOnManager(m rb.Manager) rb.Recorder {
	return eventcount.Events(m.GetEventRecorder("a"), "c")
}

// The recorder is a Logger, converted into the counter's parameter type.
func countedLegacy(b *rb.Broadcaster) rb.Recorder { return eventcount.Legacy(b.NewRecorder("a"), "c") }

func countedThroughVariable(m rb.Manager) rb.Recorder {
	r := m.GetEventRecorder("a")
	return eventcount.Events(r, "c")
}

func countedThroughMethodExpression(m rb.Manager) rb.Recorder {
	return eventcount.Events(rb.Manager.GetEventRecorder(m, "a"), "c")
}

// Not wrapped.

func inField(m rb.Manager) stash { return stash{r: m.GetEventRecorder("a")} }

func returned(m rb.Manager) rb.Recorder { return m.GetEventRecorder("a") }

func unused(m rb.Manager) { m.GetEventRecorder("a") }

func goroutine(m rb.Manager) { go m.GetEventRecorder("a") }

func deferred(m rb.Manager) { defer m.GetEventRecorder("a") }

func identity(r rb.Recorder) rb.Recorder { return r }

func passedOn(m rb.Manager) rb.Recorder { return identity(m.GetEventRecorder("a")) }

func countedAndKept(m rb.Manager) (rb.Recorder, rb.Recorder) {
	r := m.GetEventRecorder("a")
	return r, eventcount.Events(r, "c")
}

func twice(m rb.Manager) rb.Recorder {
	r := m.GetEventRecorder("a")
	return eventcount.Twice(r, r)
}

func behind(m rb.Manager) rb.Recorder { return eventcount.Behind("c", m.GetEventRecorder("a")) }

func captured(m rb.Manager) func() rb.Recorder {
	r := m.GetEventRecorder("a")
	_ = eventcount.Events(r, "c")
	return func() rb.Recorder { return r }
}

func methodValue(m rb.Manager) func(string) rb.Recorder { return m.GetEventRecorder }

func concreteMethodValue(b *rb.Broadcaster) func(string) rb.Logger { return b.NewRecorder }

func methodExpression() func(rb.Manager, string) rb.Recorder { return rb.Manager.GetEventRecorder }

// Through an interface of this package's own, which rb.Manager implements, and
// a struct of this package that embeds a Manager, used only through it: the
// calls are a builder's.

type recorderSource interface {
	GetEventRecorder(name string) rb.Recorder
}

type embedsManager struct{ rb.Manager }

func countedThroughOwnInterface(m recorderSource) rb.Recorder {
	return eventcount.Events(m.GetEventRecorder("a"), "c")
}

func ownInterfaceReturned(m recorderSource) rb.Recorder { return m.GetEventRecorder("a") }

func ownInterfaceMethodValue(m recorderSource) func(string) rb.Recorder { return m.GetEventRecorder }

// Other gives the struct one more method, so embeddedSource is implemented by
// it, through its embedded Manager, and not by rb.Manager.
func (embedsManager) Other() {}

type embeddedSource interface {
	GetEventRecorder(name string) rb.Recorder
	Other()
}

func throughEmbeddingOnly(e embeddedSource) rb.Recorder { return e.GetEventRecorder("a") }

// An interface no builder's type implements (it has one more method): its
// method of a builder's name is no builder.

type closingSource interface {
	GetEventRecorder(name string) rb.Recorder
	Close()
}

func notABuilderInterface(c closingSource) rb.Recorder { return c.GetEventRecorder("a") }

// Not builders: a method of the same name on a type of another package, and a
// function of the same name.

type fakeManager struct{}

func (fakeManager) GetEventRecorder(string) rb.Recorder { return nil }

func notABuilder(f fakeManager) rb.Recorder { return f.GetEventRecorder("a") }

func NewRecorder(dir string) rb.Recorder { return nil }

func notABuilderFunction() rb.Recorder { return NewRecorder("d") }
