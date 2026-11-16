package launcher

import "testing"

// The barrier's bound logs "not healthy within" only when it is what releases the barrier, so
// release must say whether this call closed the channel: the first call does, every later one
// doesn't, and the channel stays closed (Fable's review of #639: the line came on every start).
func TestTheStartBarrierReleasesOnceAndSaysWhichCallDid(t *testing.T) {
	b := newStartBarrier()
	select {
	case <-b.ch:
		t.Fatal("a new barrier is already released")
	default:
	}
	if !b.release() {
		t.Fatal("the first release reported that it did not release the barrier")
	}
	for i := 0; i < 3; i++ {
		if b.release() {
			t.Fatalf("release number %d reported releasing a barrier that was already released", i+2)
		}
	}
	select {
	case <-b.ch:
	default:
		t.Fatal("the barrier's channel is not closed after release")
	}
}
