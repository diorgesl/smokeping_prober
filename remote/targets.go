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
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"

	"github.com/SuperQ/smokeping_prober/config"
)

// Resolve returns the address to ping for host. A literal decides the
// family; a name is resolved locally, preferring IPv4 when network is "ip".
func Resolve(host, network string) (net.IP, error) {
	if ip := net.ParseIP(host); ip != nil {
		switch {
		case network == "ip4" && ip.To4() == nil:
			return nil, fmt.Errorf("%s is not an IPv4 address", host)
		case network == "ip6" && ip.To4() != nil:
			return nil, fmt.Errorf("%s is not an IPv6 address", host)
		}
		return ip, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ips, err := net.DefaultResolver.LookupIP(ctx, network, host)
	if err != nil {
		return nil, err
	}
	if network == "ip" {
		for _, ip := range ips {
			if ip.To4() != nil {
				return ip, nil
			}
		}
	}
	return ips[0], nil
}

// BuildTargets expands a remote target group into one Target per host and link.
// Links without a source address for the host's family are skipped.
func BuildTargets(tg config.TargetGroup, r config.Router, resolve func(host, network string) (net.IP, error), logger *slog.Logger) ([]*Target, error) {
	var targets []*Target
	for _, host := range tg.Hosts {
		ip, err := resolve(host, tg.Network)
		if err != nil {
			return nil, fmt.Errorf("resolve %q: %w", host, err)
		}
		v6 := ip.To4() == nil
		before := len(targets)
		for _, l := range r.SelectLinks(tg.Links) {
			src := l.Source
			if v6 {
				src = l.Source6
			}
			if src == "" {
				logger.Warn("Link has no source address for this address family, skipping",
					"router", r.Name, "link", l.Name, "host", host)
				continue
			}
			targets = append(targets, &Target{
				Host:     host,
				Link:     l.Name,
				Interval: tg.Interval,
				Labels:   tg.Labels,
				Job: Job{
					Target:         ip.String(),
					IPv6:           v6,
					Source:         src,
					Count:          tg.Count,
					PacketInterval: tg.PacketInterval,
					Timeout:        tg.Timeout,
					Size:           tg.Size,
					ToS:            tg.ToS,
					VPNInstance:    l.VPNInstance,
				},
			})
		}
		if len(targets) == before {
			return nil, fmt.Errorf("host %q: no link of router %q has a source address for its address family", host, r.Name)
		}
	}
	return targets, nil
}

// NewSSHDialer reads the router credentials and returns a Dialer for its sessions.
func NewSSHDialer(r config.Router, logger *slog.Logger) (Dialer, error) {
	cfg := SSHConfig{
		Address:             r.Address,
		Username:            r.Username,
		KnownHostsFile:      r.KnownHosts,
		InsecureSkipHostKey: r.InsecureSkipHostKey,
		DialTimeout:         10 * time.Second,
	}
	if r.PasswordFile != "" {
		b, err := os.ReadFile(r.PasswordFile)
		if err != nil {
			return nil, fmt.Errorf("read password_file: %w", err)
		}
		cfg.Password = strings.TrimRight(string(b), "\r\n")
	}
	if r.PrivateKeyFile != "" {
		b, err := os.ReadFile(r.PrivateKeyFile)
		if err != nil {
			return nil, fmt.Errorf("read private_key_file: %w", err)
		}
		cfg.PrivateKey = b
	}
	return func(ctx context.Context) (Runner, error) {
		if cfg.InsecureSkipHostKey {
			logger.Warn("SSH host key verification is disabled", "router", r.Name)
		}
		s, err := Dial(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return s, nil
	}, nil
}
