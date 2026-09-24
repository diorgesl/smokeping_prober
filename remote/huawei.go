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

// Package remote runs pings on a Huawei VRP router over SSH.
package remote

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Job is one ping run on the router.
type Job struct {
	Target         string
	IPv6           bool
	Source         string
	Count          int
	PacketInterval time.Duration
	Timeout        time.Duration
	Size           int
	ToS            uint8
}

// Reply is one echo reply reported by the router.
type Reply struct {
	Seq int
	RTT time.Duration
	TTL int
}

// Result is the parsed output of one ping run.
type Result struct {
	Sent     int
	Replies  []Reply
	Timeouts int
}

// ErrParse means the router output was not a ping result.
var ErrParse = errors.New("unexpected ping output")

var (
	// IPv4 prints "ttl=", IPv6 prints "hop limit=" on the line after "Reply from".
	replyRe    = regexp.MustCompile(`Sequence=(\d+)\s+(?:ttl|hop limit)=(\d+)\s+time\s*=\s*(\d+)\s*ms`)
	sentRe     = regexp.MustCompile(`(\d+)\s+packet\(s\)\s+transmitted`)
	receivedRe = regexp.MustCompile(`(\d+)\s+packet\(s\)\s+received`)
	timeoutRe  = regexp.MustCompile(`Request time out`)
)

// BuildCommand returns the VRP ping command for j.
func BuildCommand(j Job) string {
	family, tosFlag := "ping", "-tos"
	if j.IPv6 {
		family, tosFlag = "ping ipv6", "-tc"
	}
	return fmt.Sprintf("%s -c %d -m %d -t %d -s %d %s %d -a %s %s",
		family, j.Count, j.PacketInterval.Milliseconds(), j.Timeout.Milliseconds(),
		j.Size, tosFlag, j.ToS, j.Source, j.Target)
}

// CommandDeadline is how long a run of j may take before it is cancelled.
func CommandDeadline(j Job) time.Duration {
	return time.Duration(j.Count)*(j.Timeout+j.PacketInterval) + 5*time.Second
}

// ParseOutput extracts replies and the number of sent packets from VRP ping output.
func ParseOutput(out string) (Result, error) {
	m := sentRe.FindStringSubmatch(out)
	if m == nil {
		return Result{}, fmt.Errorf("%w: %s", ErrParse, firstLine(out))
	}
	sent, _ := strconv.Atoi(m[1])
	res := Result{Sent: sent}
	for _, r := range replyRe.FindAllStringSubmatch(out, -1) {
		seq, _ := strconv.Atoi(r[1])
		ttl, _ := strconv.Atoi(r[2])
		rtt, _ := strconv.Atoi(r[3])
		res.Replies = append(res.Replies, Reply{Seq: seq, RTT: time.Duration(rtt) * time.Millisecond, TTL: ttl})
	}
	res.Timeouts = len(timeoutRe.FindAllStringIndex(out, -1))

	rm := receivedRe.FindStringSubmatch(out)
	if rm == nil {
		return Result{}, fmt.Errorf("%w: no received count: %s", ErrParse, firstLine(out))
	}
	received, _ := strconv.Atoi(rm[1])
	if received != len(res.Replies) {
		return Result{}, fmt.Errorf("%w: router reported %d replies, parsed %d", ErrParse, received, len(res.Replies))
	}
	return res, nil
}

func firstLine(s string) string {
	for _, line := range strings.Split(s, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			return line
		}
	}
	return "(empty output)"
}
