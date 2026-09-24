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

package remote

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	reasonConnect = "connect"
	reasonTimeout = "timeout"
	reasonParse   = "parse"
)

var (
	sessionsUp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "smokeping",
		Subsystem: "remote",
		Name:      "sessions_up",
		Help:      "Number of SSH sessions connected and ready per router.",
	}, []string{"router"})

	remoteErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "smokeping",
		Subsystem: "remote",
		Name:      "errors_total",
		Help:      "Remote ping runs that produced no result, by reason.",
	}, []string{"router", "reason"})

	jobsSkipped = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "smokeping",
		Subsystem: "remote",
		Name:      "jobs_skipped_total",
		Help:      "Remote ping runs dropped because the previous run for the same target and link was still pending.",
	}, []string{"router"})
)
