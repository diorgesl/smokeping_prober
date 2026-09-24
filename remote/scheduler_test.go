// Copyright 2026 The smokeping_prober Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package remote

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

var nopLogger = slog.New(slog.DiscardHandler)

type fakeRunner struct {
	run      func(cmd string) (string, error)
	delay    time.Duration
	inFlight *atomic.Int32
	closed   atomic.Bool
}

func (f *fakeRunner) Run(cmd string, _ time.Duration) (string, error) {
	if f.inFlight != nil {
		f.inFlight.Add(1)
		defer f.inFlight.Add(-1)
	}
	time.Sleep(f.delay)
	return f.run(cmd)
}

func (f *fakeRunner) Close() error {
	f.closed.Store(true)
	return nil
}

type fakeRecorder struct {
	mu  sync.Mutex
	got map[string]int
}

func (r *fakeRecorder) Record(t *Target, _ Result) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.got == nil {
		r.got = map[string]int{}
	}
	r.got[t.Link]++
}

func (r *fakeRecorder) count(link string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.got[link]
}

func testTargets(interval time.Duration, links ...string) []*Target {
	var ts []*Target
	for _, l := range links {
		ts = append(ts, &Target{Host: "1.1.1.1", Link: l, Interval: interval, Job: testJob})
	}
	return ts
}

func always(out string, err error) func(string) (string, error) {
	return func(string) (string, error) { return out, err }
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestSchedulerRecordsEveryTarget(t *testing.T) {
	rec := &fakeRecorder{}
	dial := func(context.Context) (Runner, error) { return &fakeRunner{run: always(ipv4OK, nil)}, nil }
	targets := testTargets(50*time.Millisecond, "a", "b", "c")
	s := NewScheduler("sched-ok", 2, dial, targets, rec, nopLogger)
	s.Start()
	waitFor(t, "two results per link", func() bool {
		return rec.count("a") >= 2 && rec.count("b") >= 2 && rec.count("c") >= 2
	})
	waitFor(t, "two sessions up", func() bool { return testutil.ToFloat64(sessionsUp.WithLabelValues("sched-ok")) == 2 })
	s.Stop()
	for _, tg := range targets {
		if tg.Sent() < 6 {
			t.Errorf("link %s: sent = %d, want >= 6", tg.Link, tg.Sent())
		}
	}
	if got := testutil.ToFloat64(sessionsUp.WithLabelValues("sched-ok")); got != 0 {
		t.Errorf("sessions_up after Stop = %v, want 0", got)
	}
}

func TestSchedulerSkipsWhilePending(t *testing.T) {
	dial := func(context.Context) (Runner, error) {
		return &fakeRunner{run: always(ipv4OK, nil), delay: 200 * time.Millisecond}, nil
	}
	s := NewScheduler("sched-skip", 1, dial, testTargets(20*time.Millisecond, "a"), &fakeRecorder{}, nopLogger)
	s.Start()
	waitFor(t, "a skipped job", func() bool { return testutil.ToFloat64(jobsSkipped.WithLabelValues("sched-skip")) > 0 })
	s.Stop()
}

func TestSchedulerParseErrorIsNotLoss(t *testing.T) {
	rec := &fakeRecorder{}
	dial := func(context.Context) (Runner, error) { return &fakeRunner{run: always(vrpError, nil)}, nil }
	targets := testTargets(20*time.Millisecond, "a")
	s := NewScheduler("sched-parse", 1, dial, targets, rec, nopLogger)
	s.Start()
	waitFor(t, "a parse error", func() bool {
		return testutil.ToFloat64(remoteErrors.WithLabelValues("sched-parse", reasonParse)) > 0
	})
	s.Stop()
	if rec.count("a") != 0 || targets[0].Sent() != 0 {
		t.Errorf("parse error was recorded: results=%d sent=%d", rec.count("a"), targets[0].Sent())
	}
}

func TestSchedulerTimeoutKeepsSession(t *testing.T) {
	var dials, calls atomic.Int32
	rec := &fakeRecorder{}
	dial := func(context.Context) (Runner, error) {
		dials.Add(1)
		return &fakeRunner{run: func(string) (string, error) {
			if calls.Add(1) == 1 {
				return "", ErrTimeout
			}
			return ipv4OK, nil
		}}, nil
	}
	s := NewScheduler("sched-timeout", 1, dial, testTargets(20*time.Millisecond, "a"), rec, nopLogger)
	s.Start()
	waitFor(t, "a result after the timeout", func() bool { return rec.count("a") > 0 })
	s.Stop()
	if got := testutil.ToFloat64(remoteErrors.WithLabelValues("sched-timeout", reasonTimeout)); got != 1 {
		t.Errorf("timeout errors = %v, want 1", got)
	}
	if dials.Load() != 1 {
		t.Errorf("dials = %d, want 1 (session kept after a recovered timeout)", dials.Load())
	}
}

func TestSchedulerReplacesClosedSession(t *testing.T) {
	var dials atomic.Int32
	rec := &fakeRecorder{}
	dial := func(context.Context) (Runner, error) {
		if dials.Add(1) == 1 {
			return &fakeRunner{run: always("", fmt.Errorf("%w: %w", ErrTimeout, ErrClosed))}, nil
		}
		return &fakeRunner{run: always(ipv4OK, nil)}, nil
	}
	s := NewScheduler("sched-closed", 1, dial, testTargets(20*time.Millisecond, "a"), rec, nopLogger)
	s.Start()
	waitFor(t, "a result from the new session", func() bool { return rec.count("a") > 0 })
	s.Stop()
	if dials.Load() < 2 {
		t.Errorf("dials = %d, want >= 2", dials.Load())
	}
}

func TestSchedulerRetriesDial(t *testing.T) {
	var dials atomic.Int32
	rec := &fakeRecorder{}
	dial := func(context.Context) (Runner, error) {
		if dials.Add(1) <= 2 {
			return nil, errors.New("connection refused")
		}
		return &fakeRunner{run: always(ipv4OK, nil)}, nil
	}
	s := NewScheduler("sched-dial", 1, dial, testTargets(20*time.Millisecond, "a"), rec, nopLogger)
	s.minBackoff = 10 * time.Millisecond
	s.Start()
	waitFor(t, "a result after reconnecting", func() bool { return rec.count("a") > 0 })
	s.Stop()
	if got := testutil.ToFloat64(remoteErrors.WithLabelValues("sched-dial", reasonConnect)); got != 2 {
		t.Errorf("connect errors = %v, want 2", got)
	}
}

func TestSchedulerStopWaitsForRunningJob(t *testing.T) {
	var inFlight atomic.Int32
	runner := &fakeRunner{run: always(ipv4OK, nil), delay: 100 * time.Millisecond, inFlight: &inFlight}
	dial := func(context.Context) (Runner, error) { return runner, nil }
	s := NewScheduler("sched-stop", 1, dial, testTargets(10*time.Millisecond, "a"), &fakeRecorder{}, nopLogger)
	s.Start()
	waitFor(t, "a job in flight", func() bool { return inFlight.Load() == 1 })
	s.Stop()
	if inFlight.Load() != 0 {
		t.Error("Stop returned while a job was still running")
	}
	if !runner.closed.Load() {
		t.Error("Stop did not close the session")
	}
}
