// Copyright 2021 Ben Kochie <superq@gmail.com>
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
	"fmt"
	"net"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.yaml.in/yaml/v2"
)

const namespace = "smokeping_prober"

var (
	configReloadSuccess = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "config_last_reload_successful",
		Help:      "smokeping_prober config loaded successfully.",
	})

	configReloadSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "config_last_reload_success_timestamp_seconds",
		Help:      "Timestamp of the last successful configuration reload.",
	})

	// DefaultTargetGroup sets the default configuration for the TargetGroup
	DefaultTargetGroup = TargetGroup{
		Interval: time.Second,
		Network:  "ip",
		Protocol: "icmp",
		Size:     56,
	}
)

// Defaults for target groups that ping through a router.
const (
	DefaultRemoteInterval       = time.Minute
	DefaultRemoteCount          = 10
	DefaultRemotePacketInterval = 50 * time.Millisecond
	DefaultRemoteTimeout        = 500 * time.Millisecond
	DefaultRouterSessions       = 5
)

type Config struct {
	Routers []Router      `yaml:"routers,omitempty"`
	Targets []TargetGroup `yaml:"targets"`
}

// Router is a device that runs pings on behalf of the prober over SSH.
type Router struct {
	Name                string `yaml:"name"`
	Address             string `yaml:"address"`
	Username            string `yaml:"username"`
	PasswordFile        string `yaml:"password_file,omitempty"`
	PrivateKeyFile      string `yaml:"private_key_file,omitempty"`
	KnownHosts          string `yaml:"known_hosts,omitempty"`
	InsecureSkipHostKey bool   `yaml:"insecure_skip_host_key,omitempty"`
	Sessions            int    `yaml:"sessions,omitempty"`
	Links               []Link `yaml:"links"`
}

// Link is one uplink of a router, identified by the source address used to ping through it.
type Link struct {
	Name    string `yaml:"name"`
	Source  string `yaml:"source,omitempty"`
	Source6 string `yaml:"source6,omitempty"`
}

type SafeConfig struct {
	sync.RWMutex
	C *Config
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (r *Router) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*r = Router{Sessions: DefaultRouterSessions}
	type plain Router
	return unmarshal((*plain)(r))
}

// Router returns the router with the given name.
func (c *Config) Router(name string) (Router, bool) {
	for _, r := range c.Routers {
		if r.Name == name {
			return r, true
		}
	}
	return Router{}, false
}

// SelectLinks returns the links named in names, in router order, or every link when names is empty.
func (r Router) SelectLinks(names []string) []Link {
	if len(names) == 0 {
		return r.Links
	}
	var links []Link
	for _, l := range r.Links {
		if slices.Contains(names, l.Name) {
			links = append(links, l)
		}
	}
	return links
}

func (sc *SafeConfig) ReloadConfig(confFile string) (err error) {
	c := &Config{}
	defer func() {
		if err != nil {
			configReloadSuccess.Set(0)
		} else {
			configReloadSuccess.Set(1)
			configReloadSeconds.SetToCurrentTime()
		}
	}()

	yamlReader, err := os.Open(confFile)
	if err != nil {
		return fmt.Errorf("error reading config file: %w", err)
	}
	defer yamlReader.Close()
	decoder := yaml.NewDecoder(yamlReader)

	if err = decoder.Decode(c); err != nil {
		return fmt.Errorf("error parsing config file: %w", err)
	}

	if err = c.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	sc.Lock()
	sc.C = c
	sc.Unlock()

	return nil
}

// TargetGroup supports both the original list of hosts and the new single host format.
// If `host` is set, it is folded into `hosts`.
type TargetGroup struct {
	Hosts          []string          `yaml:"hosts"`
	Host           string            `yaml:"host,omitempty"`
	Interval       time.Duration     `yaml:"interval,omitempty"`
	Network        string            `yaml:"network,omitempty"`
	Protocol       string            `yaml:"protocol,omitempty"`
	Size           int               `yaml:"size,omitempty"`
	Source         string            `yaml:"source,omitempty"`
	ToS            uint8             `yaml:"tos,omitempty"`
	Labels         map[string]string `yaml:"labels,omitempty"`
	Router         string            `yaml:"router,omitempty"`
	Links          []string          `yaml:"links,omitempty"`
	Count          int               `yaml:"count,omitempty"`
	PacketInterval time.Duration     `yaml:"packet_interval,omitempty"`
	Timeout        time.Duration     `yaml:"timeout,omitempty"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain Config
	return unmarshal((*plain)(c))
}

// Validate checks routers and the target groups that reference them.
func (c *Config) Validate() error {
	routers := make(map[string]Router, len(c.Routers))
	for i, r := range c.Routers {
		if r.Name == "" {
			return fmt.Errorf("routers[%d]: name is required", i)
		}
		if _, dup := routers[r.Name]; dup {
			return fmt.Errorf("router %q: duplicate name", r.Name)
		}
		if r.Address == "" {
			return fmt.Errorf("router %q: address is required", r.Name)
		}
		if r.Username == "" {
			return fmt.Errorf("router %q: username is required", r.Name)
		}
		if r.PasswordFile == "" && r.PrivateKeyFile == "" {
			return fmt.Errorf("router %q: password_file or private_key_file is required", r.Name)
		}
		if r.KnownHosts == "" && !r.InsecureSkipHostKey {
			return fmt.Errorf("router %q: known_hosts is required (or set insecure_skip_host_key: true)", r.Name)
		}
		if r.Sessions < 1 {
			return fmt.Errorf("router %q: sessions must be at least 1", r.Name)
		}
		if len(r.Links) == 0 {
			return fmt.Errorf("router %q: at least one link is required", r.Name)
		}
		seen := make(map[string]bool, len(r.Links))
		for _, l := range r.Links {
			if l.Name == "" {
				return fmt.Errorf("router %q: every link needs a name", r.Name)
			}
			if seen[l.Name] {
				return fmt.Errorf("router %q: link %q: duplicate name", r.Name, l.Name)
			}
			seen[l.Name] = true
			if l.Source == "" && l.Source6 == "" {
				return fmt.Errorf("router %q: link %q: source or source6 is required", r.Name, l.Name)
			}
			if l.Source != "" {
				if ip := net.ParseIP(l.Source); ip == nil || ip.To4() == nil {
					return fmt.Errorf("router %q: link %q: source %q is not an IPv4 address", r.Name, l.Name, l.Source)
				}
			}
			if l.Source6 != "" {
				if ip := net.ParseIP(l.Source6); ip == nil || ip.To4() != nil {
					return fmt.Errorf("router %q: link %q: source6 %q is not an IPv6 address", r.Name, l.Name, l.Source6)
				}
			}
		}
		routers[r.Name] = r
	}

	for i, tg := range c.Targets {
		if tg.Router == "" {
			if tg.Count != 0 || tg.PacketInterval != 0 || tg.Timeout != 0 || len(tg.Links) != 0 {
				return fmt.Errorf("targets[%d]: count, packet_interval, timeout and links require router", i)
			}
			continue
		}
		r, ok := routers[tg.Router]
		if !ok {
			return fmt.Errorf("targets[%d]: unknown router %q", i, tg.Router)
		}
		if tg.Source != "" {
			return fmt.Errorf("targets[%d]: source cannot be used with router; the source comes from each link", i)
		}
		if tg.Network != "ip" && tg.Network != "ip4" && tg.Network != "ip6" {
			return fmt.Errorf("targets[%d]: network must be one of ip, ip4, ip6 for router targets", i)
		}
		if tg.Count < 1 {
			return fmt.Errorf("targets[%d]: count must be at least 1", i)
		}
		if tg.Interval <= 0 {
			return fmt.Errorf("targets[%d]: interval must be positive", i)
		}
		for _, name := range tg.Links {
			if !slices.ContainsFunc(r.Links, func(l Link) bool { return l.Name == name }) {
				return fmt.Errorf("targets[%d]: router %q has no link %q", i, r.Name, name)
			}
		}
		links := r.SelectLinks(tg.Links)
		if tg.Network == "ip6" && !slices.ContainsFunc(links, func(l Link) bool { return l.Source6 != "" }) {
			return fmt.Errorf("targets[%d]: network is ip6 but router has no source6", i)
		}
		if tg.Network == "ip4" && !slices.ContainsFunc(links, func(l Link) bool { return l.Source != "" }) {
			return fmt.Errorf("targets[%d]: network is ip4 but router has no source", i)
		}
	}
	return nil
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (s *TargetGroup) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*s = DefaultTargetGroup
	type plain TargetGroup
	if err := unmarshal((*plain)(s)); err != nil {
		return err
	}
	// Fold single host into hosts list for backwards compatibility
	if s.Host != "" && len(s.Hosts) == 0 {
		s.Hosts = []string{s.Host}
	}
	if s.Router == "" {
		return nil
	}
	// The local 1s default interval is far too aggressive for SSH, so remote
	// groups get their own default when interval is not set explicitly.
	var raw map[string]interface{}
	if err := unmarshal(&raw); err != nil {
		return err
	}
	if _, ok := raw["interval"]; !ok {
		s.Interval = DefaultRemoteInterval
	}
	if s.Count == 0 {
		s.Count = DefaultRemoteCount
	}
	if s.PacketInterval == 0 {
		s.PacketInterval = DefaultRemotePacketInterval
	}
	if s.Timeout == 0 {
		s.Timeout = DefaultRemoteTimeout
	}
	return nil
}
