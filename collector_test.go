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
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	probing "github.com/prometheus-community/pro-bing"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	"github.com/SuperQ/smokeping_prober/remote"
)

var testLabelNames = []string{"ip", "host", "source", "tos", "link", "smokeping_name"}

func testRemoteTarget() *remote.Target {
	return &remote.Target{
		Host:   "8.8.8.8",
		Link:   "operadora-a",
		Labels: map[string]string{"smokeping_name": "Google-1-v4"},
		Job:    remote.Job{Target: "8.8.8.8", Source: "201.131.152.1"},
	}
}

func TestLabelValues(t *testing.T) {
	got := labelValues(testLabelNames,
		map[string]string{"ip": "1.1.1.1", "host": "one", "source": "", "tos": "0", "link": "x"},
		map[string]string{"smokeping_name": "n", "ignored": "y"})
	want := []string{"1.1.1.1", "one", "", "0", "x", "n"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
	if got := labelValues([]string{"ip", "missing"}, map[string]string{"ip": "a"}, nil); !reflect.DeepEqual(got, []string{"a", ""}) {
		t.Errorf("missing label: got %q", got)
	}
}

// TestLocalSeriesHaveEmptyLink covers the base label set for a local
// (non-router) probe: it must carry the "link" label (so its series can be
// unioned with remote ones) but always with an empty value.
func TestLocalSeriesHaveEmptyLink(t *testing.T) {
	pinger := probing.New("127.0.0.1")
	if err := pinger.Resolve(); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	pr := probe{pinger: pinger, labels: map[string]string{}}
	c := &SmokepingCollector{labelNames: testLabelNames}
	vals := c.buildLabelValues(&pr, "")

	linkIdx := slices.Index(testLabelNames, "link")
	ipIdx := slices.Index(testLabelNames, "ip")
	if vals[linkIdx] != "" {
		t.Errorf("link = %q, want empty for a local probe", vals[linkIdx])
	}
	if vals[ipIdx] != "127.0.0.1" {
		t.Errorf("ip = %q, want 127.0.0.1", vals[ipIdx])
	}
}

func histogramSamples(t *testing.T, hist *prometheus.HistogramVec, vals []string) (uint64, float64) {
	t.Helper()
	m := &dto.Metric{}
	if err := hist.WithLabelValues(vals...).(prometheus.Metric).Write(m); err != nil {
		t.Fatal(err)
	}
	return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
}

func TestRemoteRecorderObserves(t *testing.T) {
	hist := initMetrics(testLabelNames, prometheus.DefBuckets, 1.05)
	rec := newRemoteRecorder(testLabelNames, hist)
	tg := testRemoteTarget()
	rec.Record(tg, remote.Result{Sent: 3, Replies: []remote.Reply{
		{Seq: 1, RTT: 21 * time.Millisecond, TTL: 61},
		{Seq: 3, RTT: 24 * time.Millisecond, TTL: 59},
	}, Timeouts: 1})

	vals := remoteLabelValues(testLabelNames, tg)
	count, sum := histogramSamples(t, hist, vals)
	if count != 2 || sum < 0.0449 || sum > 0.0451 {
		t.Errorf("histogram count=%d sum=%v, want 2 and 0.045", count, sum)
	}
	if got := testutil.ToFloat64(pingResponseTTL.WithLabelValues(vals...)); got != 59 {
		t.Errorf("ttl = %v, want 59", got)
	}
}

func TestRemoteRecorderKeepsItsOwnVectors(t *testing.T) {
	oldNames := []string{"ip", "host", "source", "tos", "link"}
	// Built directly rather than through initMetrics: Prometheus keeps a
	// metric name's label dimension fixed for the life of the process even
	// after Unregister (see registry.go's dimHashesByName), so registering
	// "smokeping_response_ttl" here with a different label count than
	// testLabelNames (used by the other tests in this file) would panic
	// regardless of test order. The recorder only needs vectors shaped like
	// oldNames to prove it keeps using them after the globals are swapped.
	oldHist := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:                   namespace,
		Name:                        "response_duration_seconds",
		Buckets:                     prometheus.DefBuckets,
		NativeHistogramBucketFactor: 1.05,
	}, oldNames)
	pingResponseTTL = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "response_ttl",
		Help:      "The last response Time To Live (TTL).",
	}, oldNames)
	rec := newRemoteRecorder(oldNames, oldHist)

	// A reload swaps the globals for vectors with more labels.
	initMetrics(testLabelNames, prometheus.DefBuckets, 1.05)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("recorder panicked after metrics were re-initialized: %v", r)
		}
	}()
	rec.Record(testRemoteTarget(), remote.Result{Sent: 1, Replies: []remote.Reply{{Seq: 1, RTT: time.Millisecond, TTL: 60}}})
}

func TestCollectorRemoteRequestsTotal(t *testing.T) {
	hist := initMetrics(testLabelNames, prometheus.DefBuckets, 1.05)
	tg := testRemoteTarget()
	tg.AddSent(10)
	c := NewSmokepingCollector(nil, []*remote.Target{tg}, testLabelNames, *hist)
	want := `
# HELP smokeping_requests_total Number of ping requests sent
# TYPE smokeping_requests_total counter
smokeping_requests_total{host="8.8.8.8",ip="8.8.8.8",link="operadora-a",smokeping_name="Google-1-v4",source="201.131.152.1",tos="0"} 10
`
	if err := testutil.CollectAndCompare(c, strings.NewReader(want), "smokeping_requests_total"); err != nil {
		t.Error(err)
	}
}
