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
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Runner runs one command at a time on a router shell.
type Runner interface {
	Run(cmd string, timeout time.Duration) (string, error)
	Close() error
}

// Dialer opens a new Runner.
type Dialer func(ctx context.Context) (Runner, error)

// Recorder stores the result of one successful ping run.
type Recorder interface {
	Record(t *Target, r Result)
}

// Target is one (host, link) pair pinged through a router.
type Target struct {
	Host     string
	Link     string
	Interval time.Duration
	Labels   map[string]string
	Job      Job

	sent    atomic.Uint64
	pending atomic.Bool
}

// Sent is the number of echo requests the router reported as transmitted.
func (t *Target) Sent() uint64 { return t.sent.Load() }

// AddSent adds n transmitted echo requests.
func (t *Target) AddSent(n int) { t.sent.Add(uint64(n)) }

// Scheduler runs the targets of one router on a pool of sessions.
type Scheduler struct {
	router   string
	sessions int
	dial     Dialer
	targets  []*Target
	rec      Recorder
	logger   *slog.Logger
	queue    chan *Target

	minBackoff time.Duration
	maxBackoff time.Duration

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewScheduler creates a scheduler; nothing runs until Start.
func NewScheduler(router string, sessions int, dial Dialer, targets []*Target, rec Recorder, logger *slog.Logger) *Scheduler {
	return &Scheduler{
		router:     router,
		sessions:   sessions,
		dial:       dial,
		targets:    targets,
		rec:        rec,
		logger:     logger,
		queue:      make(chan *Target, max(len(targets), 1)),
		minBackoff: time.Second,
		maxBackoff: 30 * time.Second,
	}
}

// Targets returns the targets this scheduler runs.
func (s *Scheduler) Targets() []*Target { return s.targets }

// Start opens the sessions and starts the per-target timers, spread over each interval.
func (s *Scheduler) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	sessionsUp.WithLabelValues(s.router).Set(0)
	for _, reason := range []string{reasonConnect, reasonTimeout, reasonParse} {
		remoteErrors.WithLabelValues(s.router, reason)
	}
	jobsSkipped.WithLabelValues(s.router)

	for range s.sessions {
		s.wg.Add(1)
		go s.worker(ctx)
	}
	n := int64(len(s.targets))
	for i, t := range s.targets {
		offset := time.Duration(int64(t.Interval) * int64(i) / n)
		s.wg.Add(1)
		go s.schedule(ctx, t, offset)
	}
}

// Stop cancels the timers, waits for running commands to finish and closes the sessions.
func (s *Scheduler) Stop() {
	if s.cancel == nil {
		return
	}
	s.cancel()
	s.wg.Wait()
}

func (s *Scheduler) schedule(ctx context.Context, t *Target, offset time.Duration) {
	defer s.wg.Done()
	timer := time.NewTimer(offset)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}
	s.enqueue(t)
	ticker := time.NewTicker(t.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.enqueue(t)
		}
	}
}

func (s *Scheduler) enqueue(t *Target) {
	if !t.pending.CompareAndSwap(false, true) {
		jobsSkipped.WithLabelValues(s.router).Inc()
		return
	}
	select {
	case s.queue <- t:
	default:
		t.pending.Store(false)
		jobsSkipped.WithLabelValues(s.router).Inc()
	}
}

func (s *Scheduler) worker(ctx context.Context) {
	defer s.wg.Done()
	var r Runner
	defer func() {
		if r != nil {
			r.Close()
			sessionsUp.WithLabelValues(s.router).Dec()
		}
	}()
	backoff := s.minBackoff
	for {
		if r == nil {
			conn, err := s.dial(ctx)
			if err != nil {
				remoteErrors.WithLabelValues(s.router, reasonConnect).Inc()
				s.logger.Warn("SSH connection to router failed", "router", s.router, "err", err, "retry_in", backoff)
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				backoff = min(backoff*2, s.maxBackoff)
				continue
			}
			r = conn
			backoff = s.minBackoff
			sessionsUp.WithLabelValues(s.router).Inc()
		}
		select {
		case <-ctx.Done():
			return
		case t := <-s.queue:
			if !s.runJob(r, t) {
				r.Close()
				r = nil
				sessionsUp.WithLabelValues(s.router).Dec()
			}
		}
	}
}

// runJob runs one ping and reports whether the session can be reused.
func (s *Scheduler) runJob(r Runner, t *Target) bool {
	defer t.pending.Store(false)
	cmd := BuildCommand(t.Job)
	out, err := r.Run(cmd, CommandDeadline(t.Job))
	s.logger.Debug("Remote ping", "router", s.router, "link", t.Link, "host", t.Host, "command", cmd, "output", out)
	switch {
	case errors.Is(err, ErrTimeout):
		remoteErrors.WithLabelValues(s.router, reasonTimeout).Inc()
		s.logger.Warn("Remote ping timed out", "router", s.router, "link", t.Link, "host", t.Host, "err", err)
		return !errors.Is(err, ErrClosed)
	case err != nil:
		remoteErrors.WithLabelValues(s.router, reasonConnect).Inc()
		s.logger.Warn("Remote ping failed", "router", s.router, "link", t.Link, "host", t.Host, "err", err)
		return false
	}
	res, err := ParseOutput(out)
	if err != nil {
		remoteErrors.WithLabelValues(s.router, reasonParse).Inc()
		s.logger.Warn("Unexpected output from router", "router", s.router, "link", t.Link, "host", t.Host, "err", err)
		return true
	}
	if res.Sent != len(res.Replies)+res.Timeouts {
		s.logger.Debug("Ping counters do not add up", "router", s.router, "link", t.Link, "host", t.Host,
			"sent", res.Sent, "replies", len(res.Replies), "timeouts", res.Timeouts)
	}
	t.AddSent(res.Sent)
	s.rec.Record(t, res)
	return true
}
