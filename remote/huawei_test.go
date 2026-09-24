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
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Real output from the NE8000 F1A.
const ipv4OK = `  PING 1.1.1.1: 56  data bytes, press CTRL_C to break
    Reply from 1.1.1.1: bytes=56 Sequence=1 ttl=59 time=24 ms
    Reply from 1.1.1.1: bytes=56 Sequence=2 ttl=59 time=24 ms
    Reply from 1.1.1.1: bytes=56 Sequence=3 ttl=59 time=24 ms

  --- 1.1.1.1 ping statistics ---
    3 packet(s) transmitted
    3 packet(s) received
    0.00% packet loss
    round-trip min/avg/max = 24/24/24 ms`

// Real output from the NE8000 F1A with one lost packet.
const ipv4Loss = `  PING 8.8.8.8: 56  data bytes, press CTRL_C to break
    Reply from 8.8.8.8: bytes=56 Sequence=1 ttl=119 time=55 ms
    Request time out
    Reply from 8.8.8.8: bytes=56 Sequence=3 ttl=119 time=57 ms

  --- 8.8.8.8 ping statistics ---
    3 packet(s) transmitted
    2 packet(s) received
    33.33% packet loss
    round-trip min/avg/max = 55/56/57 ms`

// Real IPv6 output from the NE8000 F1A: address and data on separate lines.
const ipv6OK = `  PING 2001:4860:4860::8888 : 56  data bytes, press CTRL_C to break
    Reply from 2001:4860:4860::8888
    bytes=56 Sequence=1 hop limit=116 time=18 ms
    Reply from 2001:4860:4860::8888
    bytes=56 Sequence=2 hop limit=116 time=18 ms
    Reply from 2001:4860:4860::8888
    bytes=56 Sequence=3 hop limit=116 time=17 ms
    Reply from 2001:4860:4860::8888
    bytes=56 Sequence=4 hop limit=116 time=18 ms
    Reply from 2001:4860:4860::8888
    bytes=56 Sequence=5 hop limit=116 time=17 ms

  --- 2001:4860:4860::8888 ping statistics---
    5 packet(s) transmitted
    5 packet(s) received
    0.00% packet loss
    round-trip min/avg/max=17/17/18 ms`

// Synthetic VRP error: the router refuses the command instead of pinging.
const vrpError = `Error: The source address is invalid.`

func ms(n int) time.Duration { return time.Duration(n) * time.Millisecond }

func TestParseOutput(t *testing.T) {
	tests := []struct {
		name string
		out  string
		want Result
	}{
		{
			name: "ipv4 all replies",
			out:  ipv4OK,
			want: Result{Sent: 3, Replies: []Reply{{1, ms(24), 59}, {2, ms(24), 59}, {3, ms(24), 59}}},
		},
		{
			name: "ipv4 with request time out",
			out:  ipv4Loss,
			want: Result{Sent: 3, Replies: []Reply{{1, ms(55), 119}, {3, ms(57), 119}}, Timeouts: 1},
		},
		{
			name: "ipv6 two-line replies",
			out:  ipv6OK,
			want: Result{Sent: 5, Replies: []Reply{{1, ms(18), 116}, {2, ms(18), 116}, {3, ms(17), 116}, {4, ms(18), 116}, {5, ms(17), 116}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseOutput(tt.out)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestParseOutputCRLF(t *testing.T) {
	got, err := ParseOutput(strings.ReplaceAll(ipv4Loss, "\n", "\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Sent != 3 || len(got.Replies) != 2 || got.Timeouts != 1 {
		t.Errorf("got %+v", got)
	}
}

func TestParseOutputRouterError(t *testing.T) {
	_, err := ParseOutput(vrpError)
	if !errors.Is(err, ErrParse) {
		t.Fatalf("error = %v, want ErrParse", err)
	}
	if !strings.Contains(err.Error(), "The source address is invalid.") {
		t.Errorf("error %q does not carry the router message", err)
	}
}

func TestBuildCommand(t *testing.T) {
	base := Job{Count: 10, PacketInterval: ms(50), Timeout: ms(500), Size: 56}

	v4 := base
	v4.Target, v4.Source = "8.8.8.8", "201.131.152.1"
	if got, want := BuildCommand(v4), "ping -c 10 -m 50 -t 500 -s 56 -tos 0 -a 201.131.152.1 8.8.8.8"; got != want {
		t.Errorf("ipv4: got %q, want %q", got, want)
	}

	v6 := base
	v6.Target, v6.Source, v6.IPv6, v6.ToS = "2001:4860:4860::8888", "2804:194c:1000::155:f0ca:a", true, 32
	if got, want := BuildCommand(v6), "ping ipv6 -c 10 -m 50 -t 500 -s 56 -tc 32 -a 2804:194c:1000::155:f0ca:a 2001:4860:4860::8888"; got != want {
		t.Errorf("ipv6: got %q, want %q", got, want)
	}
}

func TestCommandDeadline(t *testing.T) {
	j := Job{Count: 10, PacketInterval: ms(50), Timeout: ms(500)}
	if got, want := CommandDeadline(j), 10500*time.Millisecond; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
