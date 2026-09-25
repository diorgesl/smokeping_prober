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

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func load(t *testing.T, yaml string) (*SafeConfig, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	sc := &SafeConfig{C: &Config{}}
	return sc, sc.ReloadConfig(path)
}

// The config the user runs today, without any remote field.
const currentUserConfig = `
targets:
  - host: "8.8.8.8"
    interval: 1s
    network: ip4
    protocol: icmp
    size: 56
    tos: 0x00
    labels:
      category: "DNS"
      menu: "Google 1"
      title: "Google 1 - 8.8.8.8"
      smokeping_name: "Google-1-v4"
      alerts_enabled: "true"
  - host: "1.1.1.1"
    interval: 1s
    network: ip4
    protocol: icmp
    size: 56
    tos: 0x00
    labels:
      category: "DNS"
      smokeping_name: "Cloudflare-1-v4"
`

func TestCurrentConfigStillLoads(t *testing.T) {
	sc, err := load(t, currentUserConfig)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sc.C.Targets) != 2 {
		t.Fatalf("got %d target groups, want 2", len(sc.C.Targets))
	}
	tg := sc.C.Targets[0]
	if tg.Router != "" || tg.Count != 0 || tg.PacketInterval != 0 || tg.Timeout != 0 {
		t.Errorf("local group got remote fields: %+v", tg)
	}
	if tg.Interval != time.Second || tg.Network != "ip4" || tg.Size != 56 {
		t.Errorf("local group fields changed: %+v", tg)
	}
	if got := tg.Hosts; len(got) != 1 || got[0] != "8.8.8.8" {
		t.Errorf("hosts = %v, want [8.8.8.8]", got)
	}
	if tg.Labels["smokeping_name"] != "Google-1-v4" {
		t.Errorf("labels = %v", tg.Labels)
	}
}

const routerBlock = `
routers:
- name: ne8k
  address: 10.0.0.1:22
  username: smokeping
  password_file: /etc/smokeping_prober/ne8k.pass
  known_hosts: /etc/smokeping_prober/known_hosts
  links:
  - name: operadora-a
    source: 201.131.152.1
    source6: 2804:194c:1000::155:f0ca:a
  - name: operadora-b
    source: 201.131.152.5
  - name: operadora-c
    source: 201.131.152.9
`

func TestRemoteGroupDefaults(t *testing.T) {
	sc, err := load(t, routerBlock+`
targets:
  - host: "8.8.8.8"
    router: ne8k
    network: ip4
    labels:
      smokeping_name: "Google-1-v4"
`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r, ok := sc.C.Router("ne8k")
	if !ok {
		t.Fatal("router ne8k not found")
	}
	if r.Sessions != DefaultRouterSessions {
		t.Errorf("sessions = %d, want %d", r.Sessions, DefaultRouterSessions)
	}
	tg := sc.C.Targets[0]
	if tg.Interval != DefaultRemoteInterval {
		t.Errorf("interval = %v, want %v", tg.Interval, DefaultRemoteInterval)
	}
	if tg.Count != DefaultRemoteCount || tg.PacketInterval != DefaultRemotePacketInterval || tg.Timeout != DefaultRemoteTimeout {
		t.Errorf("remote defaults not applied: %+v", tg)
	}
}

func TestRemoteGroupExplicitValues(t *testing.T) {
	sc, err := load(t, routerBlock+`
targets:
  - host: "8.8.8.8"
    router: ne8k
    interval: 30s
    count: 5
    packet_interval: 100ms
    timeout: 1s
    links: [operadora-c, operadora-a]
`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	tg := sc.C.Targets[0]
	if tg.Interval != 30*time.Second || tg.Count != 5 || tg.PacketInterval != 100*time.Millisecond || tg.Timeout != time.Second {
		t.Errorf("explicit values not kept: %+v", tg)
	}
	r, _ := sc.C.Router("ne8k")
	links := r.SelectLinks(tg.Links)
	if len(links) != 2 || links[0].Name != "operadora-a" || links[1].Name != "operadora-c" {
		t.Errorf("SelectLinks = %+v, want operadora-a, operadora-c in router order", links)
	}
	if all := r.SelectLinks(nil); len(all) != 3 {
		t.Errorf("SelectLinks(nil) returned %d links, want 3", len(all))
	}
}

func TestValidationErrors(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "unknown router",
			yaml: routerBlock + "targets:\n- host: 8.8.8.8\n  router: nope\n",
			want: `unknown router "nope"`,
		},
		{
			name: "unknown link",
			yaml: routerBlock + "targets:\n- host: 8.8.8.8\n  router: ne8k\n  links: [operadora-z]\n",
			want: `has no link "operadora-z"`,
		},
		{
			name: "source in remote group",
			yaml: routerBlock + "targets:\n- host: 8.8.8.8\n  router: ne8k\n  source: 10.0.0.1\n",
			want: "source cannot be used with router",
		},
		{
			name: "remote fields in local group",
			yaml: "targets:\n- host: 8.8.8.8\n  count: 5\n",
			want: "require router",
		},
		{
			name: "ip6 without any source6",
			yaml: routerBlock + "targets:\n- host: 2001:4860:4860::8888\n  router: ne8k\n  network: ip6\n  links: [operadora-b]\n",
			want: "has no source6",
		},
		{
			name: "network ip with an ipv6 host but no selected link has source6",
			yaml: routerBlock + "targets:\n- host: 2001:4860:4860::8888\n  router: ne8k\n  links: [operadora-b]\n",
			want: "is IPv6 but no selected link",
		},
		{
			name: "invalid network",
			yaml: routerBlock + "targets:\n- host: 8.8.8.8\n  router: ne8k\n  network: tcp\n",
			want: "network must be one of ip, ip4, ip6",
		},
		{
			name: "duplicate router",
			yaml: "routers:\n- {name: a, address: x:22, username: u, password_file: p, known_hosts: k, links: [{name: l, source: 1.1.1.1}]}\n- {name: a, address: y:22, username: u, password_file: p, known_hosts: k, links: [{name: l, source: 1.1.1.1}]}\ntargets: []\n",
			want: `router "a": duplicate name`,
		},
		{
			name: "duplicate link",
			yaml: "routers:\n- {name: a, address: x:22, username: u, password_file: p, known_hosts: k, links: [{name: l, source: 1.1.1.1}, {name: l, source: 1.1.1.2}]}\ntargets: []\n",
			want: `link "l": duplicate name`,
		},
		{
			name: "source is not ipv4",
			yaml: "routers:\n- {name: a, address: x:22, username: u, password_file: p, known_hosts: k, links: [{name: l, source: '2001:db8::1'}]}\ntargets: []\n",
			want: "is not an IPv4 address",
		},
		{
			name: "source6 is not ipv6",
			yaml: "routers:\n- {name: a, address: x:22, username: u, password_file: p, known_hosts: k, links: [{name: l, source6: 1.1.1.1}]}\ntargets: []\n",
			want: "is not an IPv6 address",
		},
		{
			name: "no credentials",
			yaml: "routers:\n- {name: a, address: x:22, username: u, known_hosts: k, links: [{name: l, source: 1.1.1.1}]}\ntargets: []\n",
			want: "password_file or private_key_file is required",
		},
		{
			name: "no host key verification",
			yaml: "routers:\n- {name: a, address: x:22, username: u, password_file: p, links: [{name: l, source: 1.1.1.1}]}\ntargets: []\n",
			want: "known_hosts is required",
		},
		{
			name: "zero sessions",
			yaml: "routers:\n- {name: a, address: x:22, username: u, password_file: p, known_hosts: k, sessions: 0, links: [{name: l, source: 1.1.1.1}]}\ntargets: []\n",
			want: "sessions must be at least 1",
		},
		{
			name: "no links",
			yaml: "routers:\n- {name: a, address: x:22, username: u, password_file: p, known_hosts: k}\ntargets: []\n",
			want: "at least one link is required",
		},
		{
			name: "vpn instance with spaces",
			yaml: "routers:\n- {name: a, address: x:22, username: u, password_file: p, known_hosts: k, links: [{name: l, source: 1.1.1.1, vpn_instance: 'up 1; reboot'}]}\ntargets: []\n",
			want: `vpn_instance "up 1; reboot" may only contain`,
		},
		{
			name: "zero interval",
			yaml: routerBlock + "targets:\n- host: 8.8.8.8\n  router: ne8k\n  interval: 0s\n",
			want: "interval must be positive",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := load(t, tt.yaml)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q, want it to contain %q", err, tt.want)
			}
		})
	}
}

func TestInvalidReloadKeepsPreviousConfig(t *testing.T) {
	sc, err := load(t, currentUserConfig)
	if err != nil {
		t.Fatal(err)
	}
	before := sc.C
	path := filepath.Join(t.TempDir(), "bad.yml")
	if err := os.WriteFile(path, []byte("targets:\n- host: 8.8.8.8\n  router: nope\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := sc.ReloadConfig(path); err == nil {
		t.Fatal("expected error")
	}
	if sc.C != before {
		t.Error("invalid config replaced the running one")
	}
}
