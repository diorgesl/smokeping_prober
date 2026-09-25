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
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/SuperQ/smokeping_prober/config"
)

var testRouter = config.Router{
	Name: "ne8k",
	Links: []config.Link{
		{Name: "operadora-a", Source: "201.131.152.1", Source6: "2804:194c:1000::155:f0ca:a"},
		{Name: "operadora-b", Source: "201.131.152.5"},
		{Name: "operadora-c", Source: "201.131.152.9"},
	},
}

func remoteGroup(host, network string) config.TargetGroup {
	return config.TargetGroup{
		Hosts:          []string{host},
		Router:         "ne8k",
		Network:        network,
		Interval:       time.Minute,
		Count:          10,
		PacketInterval: ms(50),
		Timeout:        ms(500),
		Size:           56,
		Labels:         map[string]string{"smokeping_name": "Google-1-v4"},
	}
}

func TestBuildTargetsOnePerLink(t *testing.T) {
	targets, err := BuildTargets(remoteGroup("8.8.8.8", "ip4"), testRouter, Resolve, nopLogger)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 3 {
		t.Fatalf("got %d targets, want 3", len(targets))
	}
	want := Job{Target: "8.8.8.8", Source: "201.131.152.5", Count: 10, PacketInterval: ms(50), Timeout: ms(500), Size: 56}
	b := targets[1]
	if b.Link != "operadora-b" || b.Host != "8.8.8.8" || b.Interval != time.Minute || b.Job != want {
		t.Errorf("second target = %+v", b)
	}
	if b.Labels["smokeping_name"] != "Google-1-v4" {
		t.Errorf("labels not copied: %v", b.Labels)
	}
}

func TestBuildTargetsCopiesVPNInstance(t *testing.T) {
	r := testRouter
	r.Links = []config.Link{{Name: "viams", VPNInstance: "upstream-1", Source: "45.174.220.14", Source6: "2804:5bc8:f000::4"}}
	for _, host := range []string{"8.8.8.8", "2001:4860:4860::8888"} {
		targets, err := BuildTargets(remoteGroup(host, "ip"), r, Resolve, nopLogger)
		if err != nil {
			t.Fatal(err)
		}
		if len(targets) != 1 || targets[0].Job.VPNInstance != "upstream-1" {
			t.Errorf("%s: targets = %+v, want one job with VPN instance upstream-1", host, targets)
		}
	}
}

func TestBuildTargetsIPv6SkipsLinksWithoutSource6(t *testing.T) {
	targets, err := BuildTargets(remoteGroup("2001:4860:4860::8888", "ip6"), testRouter, Resolve, nopLogger)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 || targets[0].Link != "operadora-a" {
		t.Fatalf("targets = %+v, want only operadora-a", targets)
	}
	if !targets[0].Job.IPv6 || targets[0].Job.Source != "2804:194c:1000::155:f0ca:a" {
		t.Errorf("job = %+v", targets[0].Job)
	}
}

func TestBuildTargetsLinkSubset(t *testing.T) {
	tg := remoteGroup("8.8.8.8", "ip4")
	tg.Links = []string{"operadora-c"}
	targets, err := BuildTargets(tg, testRouter, Resolve, nopLogger)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 || targets[0].Link != "operadora-c" {
		t.Fatalf("targets = %+v, want only operadora-c", targets)
	}
}

func TestBuildTargetsHostnamePrefersIPv4(t *testing.T) {
	resolve := func(host, network string) (net.IP, error) {
		if host != "dns.google" || network != "ip" {
			t.Errorf("resolve(%q, %q)", host, network)
		}
		return net.ParseIP("8.8.4.4"), nil
	}
	targets, err := BuildTargets(remoteGroup("dns.google", "ip"), testRouter, resolve, nopLogger)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 3 || targets[0].Job.Target != "8.8.4.4" || targets[0].Host != "dns.google" {
		t.Fatalf("targets = %+v", targets)
	}
}

func TestResolveLiteralFamilyMismatch(t *testing.T) {
	if _, err := Resolve("8.8.8.8", "ip6"); err == nil {
		t.Error("IPv4 literal accepted for ip6")
	}
	if _, err := Resolve("2001:4860:4860::8888", "ip4"); err == nil {
		t.Error("IPv6 literal accepted for ip4")
	}
	if ip, err := Resolve("2001:4860:4860::8888", "ip"); err != nil || ip.To4() != nil {
		t.Errorf("Resolve ipv6 literal = %v, %v", ip, err)
	}
}

// TestBuildTargetsErrorsWhenNoLinkFitsFamily covers an IPv6 host whose
// router has no link with a source6: every link is skipped and BuildTargets
// must error instead of silently returning zero targets for that host.
func TestBuildTargetsErrorsWhenNoLinkFitsFamily(t *testing.T) {
	r := testRouter
	r.Links = []config.Link{
		{Name: "operadora-b", Source: "201.131.152.5"},
		{Name: "operadora-c", Source: "201.131.152.9"},
	}
	_, err := BuildTargets(remoteGroup("2001:4860:4860::8888", "ip6"), r, Resolve, nopLogger)
	if err == nil {
		t.Fatal("expected an error when no link has a source address for the host's family")
	}
}

func TestNewSSHDialerReadsPasswordFile(t *testing.T) {
	r := testRouter
	r.PasswordFile = filepath.Join(t.TempDir(), "missing.pass")
	if _, err := NewSSHDialer(r, nopLogger); err == nil {
		t.Fatal("expected error for a missing password file")
	}
	if err := os.WriteFile(r.PasswordFile, []byte("secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewSSHDialer(r, nopLogger); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
