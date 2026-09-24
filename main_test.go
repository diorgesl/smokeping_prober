// Copyright The Prometheus Authors
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

package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/SuperQ/smokeping_prober/config"
	"github.com/SuperQ/smokeping_prober/remote"
)

func init() {
	logger = slog.New(slog.DiscardHandler)
}

func withConfig(t *testing.T, c *config.Config) {
	t.Helper()
	old := sc.C
	sc.C = c
	t.Cleanup(func() { sc.C = old })
}

func testRouterConfig(t *testing.T) config.Router {
	t.Helper()
	pass := filepath.Join(t.TempDir(), "ne8k.pass")
	if err := os.WriteFile(pass, []byte("secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return config.Router{
		Name: "ne8k", Address: "127.0.0.1:1", Username: "u", PasswordFile: pass,
		InsecureSkipHostKey: true, Sessions: 1,
		Links: []config.Link{
			{Name: "operadora-a", Source: "201.131.152.1"},
			{Name: "operadora-b", Source: "201.131.152.5"},
			{Name: "operadora-c", Source: "201.131.152.9"},
		},
	}
}

func TestBuildLabelNamesIncludesLink(t *testing.T) {
	withConfig(t, &config.Config{Targets: []config.TargetGroup{
		{Labels: map[string]string{"zone": "a", "link": "dropped", "category": "DNS"}},
	}})
	got := buildLabelNamesFromConfig()
	want := []string{"ip", "host", "source", "tos", "link", "category", "zone"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestPrepareSplitsLocalAndRemote(t *testing.T) {
	withConfig(t, &config.Config{
		Routers: []config.Router{testRouterConfig(t)},
		Targets: []config.TargetGroup{
			{Hosts: []string{"8.8.8.8"}, Router: "ne8k", Network: "ip4", Interval: time.Minute, Count: 10, PacketInterval: 50 * time.Millisecond, Timeout: 500 * time.Millisecond, Size: 56},
			{Hosts: []string{"127.0.0.1"}, Network: "ip", Protocol: "icmp", Interval: time.Second, Size: 56},
		},
	})
	var sp smokePingers
	hosts := []string{}
	interval, privileged, size, tos := time.Second, true, 56, uint8(0)
	hist := initMetrics(testLabelNames, prometheus.DefBuckets, 1.05)
	if err := sp.prepare(&hosts, &interval, &privileged, &size, &tos, newRemoteRecorder(testLabelNames, hist)); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if len(sp.prepared) != 1 {
		t.Errorf("local probes = %d, want 1", len(sp.prepared))
	}
	if len(sp.preparedRemote) != 1 || len(sp.preparedRemote[0].Targets()) != 3 {
		t.Fatalf("remote schedulers = %+v, want 1 with 3 targets", sp.preparedRemote)
	}
	if sp.sizeOfPrepared() != 4 {
		t.Errorf("sizeOfPrepared = %d, want 4", sp.sizeOfPrepared())
	}
	if sp.maxInterval != time.Second {
		t.Errorf("maxInterval = %v, want 1s (remote intervals must not stretch the local splay)", sp.maxInterval)
	}
}

func TestStartRemoteOnly(t *testing.T) {
	hist := initMetrics(testLabelNames, prometheus.DefBuckets, 1.05)
	targets := []*remote.Target{testRemoteTarget(), testRemoteTarget(), testRemoteTarget()}
	for _, tg := range targets {
		tg.Interval = time.Minute
	}
	dial := func(context.Context) (remote.Runner, error) { return nil, errors.New("unreachable") }
	sp := smokePingers{
		preparedRemote: []*remote.Scheduler{
			remote.NewScheduler("main-test", 1, dial, targets, newRemoteRecorder(testLabelNames, hist), logger),
		},
	}
	sp.start()
	if got := len(sp.remoteTargets()); got != 3 {
		t.Errorf("remoteTargets = %d, want 3", got)
	}
	if err := sp.stop(); err != nil {
		t.Errorf("stop: %v", err)
	}
	if sp.startedRemote != nil {
		t.Error("stop left schedulers running")
	}
}
