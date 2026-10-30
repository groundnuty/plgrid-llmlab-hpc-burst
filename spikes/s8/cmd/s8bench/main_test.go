package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// fakeProbes reads like a machine whose flush counter advances one per millisecond and whose
// load rises by one per reading, so the window a counter covers can be read off its value.
func fakeProbes(t0 time.Time) probes {
	var load atomic.Int64
	return probes{
		flushes:  func() int64 { return time.Since(t0).Milliseconds() },
		load1:    func() float64 { return float64(load.Add(1)) },
		memAvail: func() float64 { return 20 },
		cpuTimes: func() (float64, float64) { return 0, 1 },
	}
}

func okPublish(ctx context.Context, e cloudevents.Event) error {
	time.Sleep(time.Millisecond)
	return nil
}

func spec(pub func(context.Context, cloudevents.Event) error) phaseSpec {
	return phaseSpec{Round: 1, Workers: 2, Mode: "test", Dur: 1500 * time.Millisecond, Warmup: time.Second, SampleEvery: 100 * time.Millisecond, Publish: pub, Probes: fakeProbes(time.Now())}
}

// The counters of a phase cover the same window as its acknowledgements: from the end of the
// warm-up to the last publish. The fake flush counter advances 1000 per second, so a window
// that also covered the one-second warm-up would report about 1500; the measured window is half a second.
func TestRunPhase_FlushCountCoversTheMeasuredWindowOnly(t *testing.T) {
	sp := spec(okPublish)
	sp.Probes = fakeProbes(time.Now()) // t0 = start of the phase
	res := runPhase(sp, map[string]string{})
	if !res.Valid || res.Errors != 0 {
		t.Fatalf("a clean phase was invalidated: %+v", res)
	}
	if res.DeviceFlushes < 350 || res.DeviceFlushes > 800 {
		t.Fatalf("flush requests = %d for a measured window of %.2f s (phase 1.5 s, warm-up 1 s): the counter does not cover just the measured window", res.DeviceFlushes, res.Seconds)
	}
	if res.Seconds < 0.4 || res.Seconds > 0.8 {
		t.Fatalf("measured window = %.2f s, want about 0.5 s", res.Seconds)
	}
	if res.Acks == 0 {
		t.Fatal("no acknowledgements counted in the measured window")
	}
}

// A publish error means the phase did not measure the publish path (the failing worker stops).
func TestRunPhase_PublishErrorsInvalidateThePhase(t *testing.T) {
	var calls atomic.Int64
	failing := func(ctx context.Context, e cloudevents.Event) error {
		time.Sleep(time.Millisecond)
		if calls.Add(1) > 100 {
			return errors.New("broker went away")
		}
		return nil
	}
	res := runPhase(spec(failing), map[string]string{})
	if res.Valid || res.Errors == 0 || res.FirstError != "broker went away" {
		t.Fatalf("a phase whose publishes failed was kept: valid=%v errors=%d first=%q", res.Valid, res.Errors, res.FirstError)
	}
}

// The load readings of the window are kept, not only its start and end.
func TestRunPhase_KeepsLoadSamplesAndTheirExtrema(t *testing.T) {
	res := runPhase(spec(okPublish), map[string]string{})
	if len(res.Samples) < 2 {
		t.Fatalf("only %d load samples kept for a 0.5 s window sampled every 0.1 s", len(res.Samples))
	}
	lo, hi := res.Samples[0].Load1, res.Samples[0].Load1
	for _, s := range res.Samples {
		lo, hi = min(lo, s.Load1), max(hi, s.Load1)
	}
	// The fake load rises with every reading: the window start was read first, the end last.
	if res.Load1Min != res.Load1Start || res.Load1Max != res.Load1End || res.Load1Start >= lo || hi >= res.Load1End {
		t.Fatalf("load extrema min=%v max=%v do not span start=%v, samples %v..%v, end=%v", res.Load1Min, res.Load1Max, res.Load1Start, lo, hi, res.Load1End)
	}
}
