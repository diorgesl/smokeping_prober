# RemotePing via Huawei NE8000: plano de implementação

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fazer o smokeping_prober entrar por SSH num Huawei NE8000 e rodar `ping -a <ip-do-link>` para cada destino, uma vez por link de operadora, gravando uma série por link nas métricas que já existem.

**Architecture:** Um pacote novo, `remote/`, com quatro peças: comando e parser do VRP (funções puras), sessão SSH persistente com PTY, scheduler com pool de sessões e o construtor de alvos (grupo de config → um alvo por host × link). O `main.go` passa a preparar, iniciar e parar os schedulers junto com os pingers locais, e o `collector.go` ganha o label `link`, um recorder para resultados remotos e o `requests_total` dos alvos remotos.

**Tech Stack:** Go 1.25+, `golang.org/x/crypto/ssh` (+ `knownhosts`), `prometheus/client_golang` (+ `testutil`), `go.yaml.in/yaml/v2`.

**Spec:** `docs/superpowers/specs/2026-09-24-remote-ping-huawei-design.md`

## Global Constraints

- Módulo `github.com/SuperQ/smokeping_prober`, `go 1.25.0` no `go.mod`. A única dependência nova é `golang.org/x/crypto`, que já está no `go.mod` como indireta (v0.54.0) e passa a direta via `go mod tidy`.
- Imports em três blocos: stdlib, terceiros, `github.com/SuperQ/smokeping_prober/...` (gci). Formatação gofumpt com extra rules (parâmetros adjacentes do mesmo tipo agrupados: `func f(a, b string)`).
- Logs com `slog` em pares chave-valor, mensagem com inicial maiúscula, como o código atual (`logger.Info("Starting prober", "address", ...)`).
- Arquivos `.go` novos começam com o cabeçalho Apache 2.0 do repositório, com `// Copyright 2026 The smokeping_prober Authors` na primeira linha.
- Nomes de métricas novas, exatos: `smokeping_remote_sessions_up{router}`, `smokeping_remote_errors_total{router, reason}` com `reason` ∈ `connect`, `timeout`, `parse`, e `smokeping_remote_jobs_skipped_total{router}`.
- Conjunto base de labels, nesta ordem: `ip, host, source, tos, link`, depois as chaves personalizadas em ordem alfabética. Uma chave personalizada igual a um nome base é descartada.
- Padrões de grupos remotos: `interval` 1m (só quando o campo não aparece no yaml), `count` 10, `packet_interval` 50ms, `timeout` 500ms. `sessions` do roteador: 5.
- Deadline por comando: `count × (timeout + packet_interval) + 5s`. Ctrl-C espera o prompt por 3s. Backoff de reconexão: 1s → 30s, dobrando.
- Falha de SSH, timeout ou parse nunca grava perda: nada vai para `requests_total` nem para o histograma naquele ciclo.
- O yaml atual do usuário (grupos com `host:` único, `labels:`, sem `router:`) carrega e se comporta como hoje.
- Código, comentários, README e CHANGELOG em inglês, como o resto do repositório.
- Toda mensagem de commit termina com a linha `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **Banner de login e pergunta interativa antes do prompt.** O VRP imprime linhas `Info: ...` após o login e, no primeiro acesso de um usuário, pode perguntar `Change now? [Y/N]`. O esperado é ignorar o banner e recusar a conexão com erro claro quando a pergunta aparece, em vez de adotar `[Y/N]` como prompt. Testes: `TestDialSkipsBanner` e `TestDialRejectsInteractiveQuestion` (Task 3).
2. **Saída com CRLF e eco do comando.** Pelo PTY, as linhas chegam com `\r\n` e o comando volta ecoado. O esperado é o parser receber o texto limpo, sem o eco e sem o prompt. Testes: `TestParseOutputCRLF` (Task 2) e as asserções de eco e prompt em `TestDialAndRunPing` (Task 3).
3. **Erro do próprio VRP no lugar da saída do ping** (por exemplo, IP de origem inválido). O esperado é contar `reason="parse"`, logar a primeira linha e não gravar perda. Teste: `TestSchedulerParseErrorIsNotLoss` (Task 4).
4. **Reload com comando em andamento e mudança no conjunto de labels.** O scheduler antigo termina o comando depois que `initMetrics` já trocou as variáveis globais. O esperado é não dar panic de cardinalidade. O recorder guarda os vetores da época em que foi criado. Teste: `TestRemoteRecorderKeepsItsOwnVectors` (Task 6).
5. **Config só com grupos remotos, sem nenhum pinger local.** Hoje `start()` divide `maxInterval` por `len(started)`. O esperado é iniciar sem divisão por zero. Teste: `TestStartRemoteOnly` (Task 6).

---

## Estrutura de arquivos

| arquivo | responsabilidade |
|---|---|
| `config/config.go` (modificar) | tipos `Router`/`Link`, campos remotos em `TargetGroup`, padrões remotos, `Config.Validate()`, `Config.Router()`, `Router.SelectLinks()` |
| `config/config_test.go` (novo) | retrocompatibilidade, padrões, cada regra de validação |
| `remote/huawei.go` | `Job`, `Reply`, `Result`, `BuildCommand`, `CommandDeadline`, `ParseOutput` |
| `remote/huawei_test.go` | saídas reais do NE8000 como fixtures |
| `remote/session.go` | `SSHConfig`, `Session` (`Dial`, `Run`, `Close`), `ErrTimeout`, `ErrClosed` |
| `remote/fakevrp_test.go` | servidor SSH falso que imita o VRP |
| `remote/session_test.go` | login, prompt, eco, deadline, host key |
| `remote/metrics.go` | as três métricas `smokeping_remote_*` |
| `remote/scheduler.go` | `Runner`, `Dialer`, `Recorder`, `Target`, `Scheduler` |
| `remote/scheduler_test.go` | pool, descarte, erros, reconexão, parada |
| `remote/targets.go` | `Resolve`, `BuildTargets`, `NewSSHDialer` |
| `remote/targets_test.go` | expansão host × link, família, arquivos de credencial |
| `collector.go` (modificar) | `labelValues`, `remoteLabelValues`, `remoteRecorder`, alvos remotos no collector |
| `collector_test.go` (novo) | labels, recorder, `requests_total` remoto |
| `main.go` (modificar) | `smokePingers` com schedulers, `prepare`/`start`/`stop`, label `link`, chamadas em `main()` |
| `main_test.go` (novo) | `prepare` separando local e remoto, start só remoto |
| `README.md`, `smokeping_prober.yml`, `CHANGELOG.md` (modificar) | documentação |

---

### Task 0: Ambiente e worktree

**Files:**
- Nenhum arquivo de produto. Copia `docs/superpowers/specs/2026-09-24-remote-ping-huawei-design.md` e este plano para a worktree.

- [ ] **Step 1: Criar a worktree** com a skill `superpowers:using-git-worktrees`, branch `feat/remote-ping-huawei` a partir de `master`. Todos os passos seguintes rodam dentro da worktree.

- [ ] **Step 2: Copiar spec e plano** (estão não rastreados no checkout principal):

```bash
MAIN=/Users/diorgera/Projetos/smokeping_prober
mkdir -p docs/superpowers/specs docs/superpowers/plans
cp "$MAIN/docs/superpowers/specs/2026-09-24-remote-ping-huawei-design.md" docs/superpowers/specs/
cp "$MAIN/docs/superpowers/plans/2026-09-24-remote-ping-huawei.md" docs/superpowers/plans/
```

- [ ] **Step 3: Conferir o Go.** O usuário instalou o Go 1.27.1 em `/usr/local/go/bin` (via `/etc/paths.d/go`). Sessões abertas antes da instalação não têm esse diretório no PATH, então todo bloco de comandos deste plano começa com:

```bash
export PATH=/usr/local/go/bin:$PATH
go version
```

Esperado: `go version go1.27.1 darwin/arm64` (qualquer versão >= 1.25 serve). Se o comando não existir, pare e avise o usuário.

- [ ] **Step 4: Linha de base**

```bash
go build ./... && go test ./...
```

Esperado: build ok, `no test files` em todos os pacotes.

- [ ] **Step 5: Commit**

```bash
git add docs/superpowers
git commit -m "docs: add remote ping design and implementation plan

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 1: Config de roteadores, links e grupos remotos

**Files:**
- Modify: `config/config.go` (bloco `var`/`DefaultTargetGroup`, `Config`, `ReloadConfig`, `TargetGroup`, `TargetGroup.UnmarshalYAML`)
- Test: `config/config_test.go`

**Interfaces:**
- Produces:
  - `type Router struct { Name, Address, Username, PasswordFile, PrivateKeyFile, KnownHosts string; InsecureSkipHostKey bool; Sessions int; Links []Link }`
  - `type Link struct { Name, Source, Source6 string }`
  - `TargetGroup` ganha `Router string`, `Links []string`, `Count int`, `PacketInterval time.Duration`, `Timeout time.Duration`
  - `Config.Routers []Router`
  - `func (c *Config) Validate() error`
  - `func (c *Config) Router(name string) (Router, bool)`
  - `func (r Router) SelectLinks(names []string) []Link`: todos os links quando `names` é vazio, senão os nomeados, na ordem do roteador
  - constantes `DefaultRemoteInterval`, `DefaultRemoteCount`, `DefaultRemotePacketInterval`, `DefaultRemoteTimeout`, `DefaultRouterSessions`

- [ ] **Step 1: Escrever os testes que falham**

`config/config_test.go`:

```go
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
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `go test ./config/`
Expected: FAIL de compilação (`sc.C.Router undefined`, `tg.Router undefined`, `DefaultRouterSessions undefined`).

- [ ] **Step 3: Implementar**

Em `config/config.go`, acrescentar `"net"` e `"slices"` aos imports da stdlib. Logo depois do bloco `var (...)` existente, acrescentar:

```go
// Defaults for target groups that ping through a router.
const (
	DefaultRemoteInterval       = time.Minute
	DefaultRemoteCount          = 10
	DefaultRemotePacketInterval = 50 * time.Millisecond
	DefaultRemoteTimeout        = 500 * time.Millisecond
	DefaultRouterSessions       = 5
)
```

Trocar `type Config struct` por:

```go
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
```

Em `ReloadConfig`, entre o `decoder.Decode` e o `sc.Lock()`:

```go
	if err = c.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
```

Em `TargetGroup`, acrescentar os campos (depois de `Labels`):

```go
	Router         string        `yaml:"router,omitempty"`
	Links          []string      `yaml:"links,omitempty"`
	Count          int           `yaml:"count,omitempty"`
	PacketInterval time.Duration `yaml:"packet_interval,omitempty"`
	Timeout        time.Duration `yaml:"timeout,omitempty"`
```

Trocar `TargetGroup.UnmarshalYAML` por:

```go
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
```

E, no fim do arquivo, `Validate`:

```go
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
		for _, name := range tg.Links {
			if !slices.ContainsFunc(r.Links, func(l Link) bool { return l.Name == name }) {
				return fmt.Errorf("targets[%d]: router %q has no link %q", i, r.Name, name)
			}
		}
		links := r.SelectLinks(tg.Links)
		if tg.Network == "ip6" && !slices.ContainsFunc(links, func(l Link) bool { return l.Source6 != "" }) {
			return fmt.Errorf("targets[%d]: network is ip6 but no selected link of router %q has source6", i, r.Name)
		}
		if tg.Network == "ip4" && !slices.ContainsFunc(links, func(l Link) bool { return l.Source != "" }) {
			return fmt.Errorf("targets[%d]: network is ip4 but no selected link of router %q has source", i, r.Name)
		}
	}
	return nil
}
```

- [ ] **Step 4: Rodar e ver passar**

Run: `go test ./config/ -v`
Expected: PASS em `TestCurrentConfigStillLoads`, `TestRemoteGroupDefaults`, `TestRemoteGroupExplicitValues`, todos os subtestes de `TestValidationErrors` e `TestInvalidReloadKeepsPreviousConfig`. Rodar também `go build ./...` (o `main` continua compilando).

- [ ] **Step 5: Commit**

```bash
git add config/
git commit -m "config: add routers, links and remote target groups

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Comando e parser do Huawei VRP

**Files:**
- Create: `remote/huawei.go`
- Test: `remote/huawei_test.go`

**Interfaces:**
- Produces:
  - `type Job struct { Target string; IPv6 bool; Source string; Count int; PacketInterval, Timeout time.Duration; Size int; ToS uint8 }`
  - `type Reply struct { Seq int; RTT time.Duration; TTL int }`
  - `type Result struct { Sent int; Replies []Reply; Timeouts int }`
  - `func BuildCommand(j Job) string`
  - `func CommandDeadline(j Job) time.Duration`
  - `func ParseOutput(out string) (Result, error)`
  - `var ErrParse error`
  - fixtures de teste do pacote: `ipv4OK`, `ipv4Loss`, `ipv6OK`, `vrpError` (usados nas Tasks 3 e 4)

- [ ] **Step 1: Escrever os testes que falham**

`remote/huawei_test.go` (cabeçalho de licença como na Task 1):

```go
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
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `go test ./remote/`
Expected: FAIL de compilação (`undefined: ParseOutput`, `undefined: Job`...).

- [ ] **Step 3: Implementar** `remote/huawei.go` (cabeçalho de licença):

```go
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
	replyRe   = regexp.MustCompile(`Sequence=(\d+)\s+(?:ttl|hop limit)=(\d+)\s+time\s*=\s*(\d+)\s*ms`)
	sentRe    = regexp.MustCompile(`(\d+)\s+packet\(s\)\s+transmitted`)
	timeoutRe = regexp.MustCompile(`Request time out`)
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
```

- [ ] **Step 4: Rodar e ver passar**

Run: `go test ./remote/ -v`
Expected: PASS em `TestParseOutput` (3 subtestes), `TestParseOutputCRLF`, `TestParseOutputRouterError`, `TestBuildCommand`, `TestCommandDeadline`.

- [ ] **Step 5: Commit**

```bash
git add remote/huawei.go remote/huawei_test.go
git commit -m "remote: build and parse Huawei VRP ping commands

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Sessão SSH persistente

**Files:**
- Create: `remote/session.go`
- Create: `remote/fakevrp_test.go`
- Test: `remote/session_test.go`
- Modify: `go.mod`, `go.sum` (via `go mod tidy`)

**Interfaces:**
- Consumes: `BuildCommand`, `Job`, `ParseOutput`, fixture `ipv4OK` (Task 2)
- Produces:
  - `type SSHConfig struct { Address, Username, Password string; PrivateKey []byte; KnownHostsFile string; InsecureSkipHostKey bool; DialTimeout time.Duration }`
  - `func Dial(ctx context.Context, cfg SSHConfig) (*Session, error)`
  - `func (s *Session) Run(cmd string, timeout time.Duration) (string, error)`: devolve a saída sem eco e sem prompt
  - `func (s *Session) Close() error`
  - `var ErrTimeout, ErrClosed error`. Timeout em que o Ctrl-C recuperou o prompt → `ErrTimeout` com a sessão viva. Sem prompt depois do Ctrl-C → erro que satisfaz `errors.Is` para **os dois**, e a sessão fica fechada. Falha de escrita/leitura → `ErrClosed`.
  - helpers de teste: `startFakeVRP(t, *fakeVRP)`, `(*fakeVRP).knownHosts(t) string`, `(*fakeVRP).commands() []string`

- [ ] **Step 1: Escrever o servidor falso**

`remote/fakevrp_test.go` (cabeçalho de licença):

```go
package remote

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

const fakePrompt = "<rt-test>"

// fakeVRP is an in-process SSH server that behaves like a Huawei VRP user view.
type fakeVRP struct {
	password    string
	banner      string            // printed before the first prompt
	loginText   string            // when set, replaces banner and prompt entirely
	responses   map[string]string // command -> output
	hang        map[string]bool   // commands that only end with Ctrl-C
	ignoreCtrlC bool

	addr    string
	hostKey ssh.PublicKey

	mu   sync.Mutex
	cmds []string
}

func startFakeVRP(t *testing.T, f *fakeVRP) {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	f.hostKey = signer.PublicKey()
	cfg := &ssh.ServerConfig{
		PasswordCallback: func(_ ssh.ConnMetadata, p []byte) (*ssh.Permissions, error) {
			if string(p) == f.password {
				return nil, nil
			}
			return nil, errors.New("access denied")
		},
	}
	cfg.AddHostKey(signer)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	f.addr = ln.Addr().String()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(conn, cfg)
		}
	}()
}

func (f *fakeVRP) knownHosts(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "known_hosts")
	line := knownhosts.Line([]string{knownhosts.Normalize(f.addr)}, f.hostKey)
	if err := os.WriteFile(path, []byte(line+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func (f *fakeVRP) commands() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.cmds...)
}

func (f *fakeVRP) serve(conn net.Conn, cfg *ssh.ServerConfig) {
	_, chans, reqs, err := ssh.NewServerConn(conn, cfg)
	if err != nil {
		conn.Close()
		return
	}
	go ssh.DiscardRequests(reqs)
	for nc := range chans {
		if nc.ChannelType() != "session" {
			nc.Reject(ssh.UnknownChannelType, "only sessions")
			continue
		}
		ch, creqs, err := nc.Accept()
		if err != nil {
			return
		}
		go func() {
			for r := range creqs {
				switch r.Type {
				case "pty-req":
					r.Reply(true, nil)
				case "shell":
					r.Reply(true, nil)
					go f.shell(ch)
				default:
					r.Reply(false, nil)
				}
			}
		}()
	}
}

func (f *fakeVRP) shell(ch ssh.Channel) {
	defer ch.Close()
	if f.loginText != "" {
		io.WriteString(ch, f.loginText)
	} else {
		io.WriteString(ch, f.banner+"\r\n"+fakePrompt)
	}
	var line []byte
	hanging := false
	b := make([]byte, 1)
	for {
		if _, err := ch.Read(b); err != nil {
			return
		}
		switch c := b[0]; {
		case c == 0x03:
			if hanging && !f.ignoreCtrlC {
				hanging = false
				io.WriteString(ch, "\r\n"+fakePrompt)
			}
		case c == '\r' || c == '\n':
			if hanging {
				continue
			}
			cmd := strings.TrimSpace(string(line))
			line = line[:0]
			if cmd == "" {
				continue
			}
			f.mu.Lock()
			f.cmds = append(f.cmds, cmd)
			f.mu.Unlock()
			io.WriteString(ch, cmd+"\r\n") // terminal echo
			if f.hang[cmd] {
				hanging = true
				continue
			}
			out, ok := f.responses[cmd]
			if !ok && cmd == "screen-length 0 temporary" {
				out = "Info: The configuration takes effect on the current user terminal interface only."
			}
			if out != "" {
				io.WriteString(ch, strings.ReplaceAll(out, "\n", "\r\n")+"\r\n")
			}
			io.WriteString(ch, "\r\n"+fakePrompt)
		default:
			if !hanging {
				line = append(line, c)
			}
		}
	}
}
```

- [ ] **Step 2: Escrever os testes que falham**

`remote/session_test.go` (cabeçalho de licença):

```go
package remote

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

var testJob = Job{Target: "1.1.1.1", Source: "201.131.152.1", Count: 3, PacketInterval: ms(50), Timeout: ms(500), Size: 56}

func dialFake(t *testing.T, f *fakeVRP) (*Session, error) {
	t.Helper()
	return Dial(context.Background(), SSHConfig{
		Address:        f.addr,
		Username:       "smokeping",
		Password:       "secret",
		KnownHostsFile: f.knownHosts(t),
		DialTimeout:    2 * time.Second,
	})
}

func TestDialAndRunPing(t *testing.T) {
	cmd := BuildCommand(testJob)
	f := &fakeVRP{password: "secret", responses: map[string]string{cmd: ipv4OK}}
	startFakeVRP(t, f)

	s, err := dialFake(t, f)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer s.Close()

	out, err := s.Run(cmd, 2*time.Second)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if strings.Contains(out, fakePrompt) {
		t.Errorf("output still contains the prompt: %q", out)
	}
	if strings.HasPrefix(out, "ping ") {
		t.Errorf("output still starts with the command echo: %q", out)
	}
	got, err := ParseOutput(out)
	if err != nil {
		t.Fatalf("ParseOutput: %v", err)
	}
	want, _ := ParseOutput(ipv4OK)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
	if cmds := f.commands(); len(cmds) != 2 || cmds[0] != "screen-length 0 temporary" || cmds[1] != cmd {
		t.Errorf("commands = %q", cmds)
	}
}

func TestDialSkipsBanner(t *testing.T) {
	f := &fakeVRP{
		password: "secret",
		banner:   "Info: The max number of VTY users is 21, the number of current VTY users online is 2.\r\n      The current login time is 2026-09-24 10:00:00.",
	}
	startFakeVRP(t, f)
	s, err := dialFake(t, f)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	s.Close()
}

func TestDialRejectsInteractiveQuestion(t *testing.T) {
	f := &fakeVRP{
		password:  "secret",
		loginText: "Warning: The password has expired. Change now? [Y/N]",
	}
	startFakeVRP(t, f)
	_, err := dialFake(t, f)
	if err == nil || !strings.Contains(err.Error(), "interactive question") {
		t.Fatalf("error = %v, want interactive question error", err)
	}
}

func TestDialRejectsUnknownHostKey(t *testing.T) {
	f := &fakeVRP{password: "secret"}
	startFakeVRP(t, f)
	other, err := ssh.NewSignerFromKey(mustEd25519(t))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "known_hosts")
	line := knownhosts.Line([]string{knownhosts.Normalize(f.addr)}, other.PublicKey())
	if err := os.WriteFile(path, []byte(line+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = Dial(context.Background(), SSHConfig{Address: f.addr, Username: "u", Password: "secret", KnownHostsFile: path, DialTimeout: 2 * time.Second})
	if err == nil {
		t.Fatal("expected host key error")
	}
}

func TestDialWrongPassword(t *testing.T) {
	f := &fakeVRP{password: "secret"}
	startFakeVRP(t, f)
	_, err := Dial(context.Background(), SSHConfig{Address: f.addr, Username: "u", Password: "wrong", KnownHostsFile: f.knownHosts(t), DialTimeout: 2 * time.Second})
	if err == nil {
		t.Fatal("expected authentication error")
	}
}

func TestRunTimeoutRecoversWithCtrlC(t *testing.T) {
	cmd := BuildCommand(testJob)
	f := &fakeVRP{password: "secret", hang: map[string]bool{"ping hang": true}, responses: map[string]string{cmd: ipv4OK}}
	startFakeVRP(t, f)
	s, err := dialFake(t, f)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer s.Close()

	if _, err := s.Run("ping hang", 200*time.Millisecond); !errors.Is(err, ErrTimeout) || errors.Is(err, ErrClosed) {
		t.Fatalf("error = %v, want ErrTimeout only", err)
	}
	out, err := s.Run(cmd, 2*time.Second)
	if err != nil {
		t.Fatalf("session unusable after Ctrl-C: %v", err)
	}
	if _, err := ParseOutput(out); err != nil {
		t.Errorf("ParseOutput after recovery: %v", err)
	}
}

func TestRunTimeoutWithoutPromptClosesSession(t *testing.T) {
	f := &fakeVRP{password: "secret", hang: map[string]bool{"ping hang": true}, ignoreCtrlC: true}
	startFakeVRP(t, f)
	s, err := dialFake(t, f)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	_, err = s.Run("ping hang", 200*time.Millisecond)
	if !errors.Is(err, ErrTimeout) || !errors.Is(err, ErrClosed) {
		t.Fatalf("error = %v, want ErrTimeout and ErrClosed", err)
	}
}
```

E, em `fakevrp_test.go`, acrescentar o helper usado acima:

```go
func mustEd25519(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return priv
}
```

- [ ] **Step 3: Rodar e ver falhar**

Run: `go test ./remote/`
Expected: FAIL de compilação (`undefined: Dial`, `undefined: SSHConfig`, `undefined: ErrTimeout`).

- [ ] **Step 4: Implementar** `remote/session.go` (cabeçalho de licença):

```go
package remote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

var (
	// ErrTimeout means a command did not finish before its deadline.
	ErrTimeout = errors.New("command timed out")
	// ErrClosed means the session is gone and must be replaced.
	ErrClosed = errors.New("session closed")
)

const (
	loginTimeout = 15 * time.Second
	setupTimeout = 10 * time.Second
	ctrlCTimeout = 3 * time.Second
)

// promptRe matches a VRP prompt at the end of the output: <name>, [name] or [~name].
var promptRe = regexp.MustCompile(`([<\[][~*]?[^\s<>\[\]]+[>\]])\s*$`)

// SSHConfig describes how to reach a router.
type SSHConfig struct {
	Address             string
	Username            string
	Password            string
	PrivateKey          []byte
	KnownHostsFile      string
	InsecureSkipHostKey bool
	DialTimeout         time.Duration
}

// Session is an interactive VRP shell that runs one command at a time.
type Session struct {
	client    *ssh.Client
	sess      *ssh.Session
	stdin     io.Writer
	chunks    chan []byte
	buf       []byte
	prompt    string
	closeOnce sync.Once
}

// Dial logs into the router, learns its prompt and disables paging.
func Dial(ctx context.Context, cfg SSHConfig) (*Session, error) {
	hostKey, err := hostKeyCallback(cfg)
	if err != nil {
		return nil, err
	}
	auth, err := authMethods(cfg)
	if err != nil {
		return nil, err
	}
	timeout := cfg.DialTimeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	d := net.Dialer{Timeout: timeout}
	conn, err := d.DialContext(ctx, "tcp", cfg.Address)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", cfg.Address, err)
	}
	_ = conn.SetDeadline(time.Now().Add(timeout))
	c, chans, reqs, err := ssh.NewClientConn(conn, cfg.Address, &ssh.ClientConfig{
		User:            cfg.Username,
		Auth:            auth,
		HostKeyCallback: hostKey,
		Timeout:         timeout,
	})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("ssh handshake with %s: %w", cfg.Address, err)
	}
	_ = conn.SetDeadline(time.Time{})
	client := ssh.NewClient(c, chans, reqs)
	s, err := startShell(client)
	if err != nil {
		client.Close()
		return nil, err
	}
	return s, nil
}

func hostKeyCallback(cfg SSHConfig) (ssh.HostKeyCallback, error) {
	if cfg.InsecureSkipHostKey {
		return ssh.InsecureIgnoreHostKey(), nil
	}
	if cfg.KnownHostsFile == "" {
		return nil, errors.New("known_hosts file is required")
	}
	cb, err := knownhosts.New(cfg.KnownHostsFile)
	if err != nil {
		return nil, fmt.Errorf("load known_hosts: %w", err)
	}
	return cb, nil
}

func authMethods(cfg SSHConfig) ([]ssh.AuthMethod, error) {
	var auth []ssh.AuthMethod
	if len(cfg.PrivateKey) > 0 {
		signer, err := ssh.ParsePrivateKey(cfg.PrivateKey)
		if err != nil {
			return nil, fmt.Errorf("parse private key: %w", err)
		}
		auth = append(auth, ssh.PublicKeys(signer))
	}
	if cfg.Password != "" {
		auth = append(auth,
			ssh.Password(cfg.Password),
			ssh.KeyboardInteractive(func(_, _ string, questions []string, _ []bool) ([]string, error) {
				answers := make([]string, len(questions))
				for i := range answers {
					answers[i] = cfg.Password
				}
				return answers, nil
			}),
		)
	}
	if len(auth) == 0 {
		return nil, errors.New("no password or private key")
	}
	return auth, nil
}

func startShell(client *ssh.Client) (*Session, error) {
	sess, err := client.NewSession()
	if err != nil {
		return nil, fmt.Errorf("open session: %w", err)
	}
	stdin, err := sess.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("stdin: %w", err)
	}
	stdout, err := sess.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout: %w", err)
	}
	modes := ssh.TerminalModes{ssh.ECHO: 1, ssh.TTY_OP_ISPEED: 115200, ssh.TTY_OP_OSPEED: 115200}
	// A wide terminal keeps long ping commands from wrapping in the echo.
	if err := sess.RequestPty("vt100", 200, 512, modes); err != nil {
		return nil, fmt.Errorf("request pty: %w", err)
	}
	if err := sess.Shell(); err != nil {
		return nil, fmt.Errorf("start shell: %w", err)
	}
	s := &Session{client: client, sess: sess, stdin: stdin, chunks: make(chan []byte, 64)}
	go s.readLoop(stdout)
	if err := s.learnPrompt(); err != nil {
		s.Close()
		return nil, err
	}
	if _, err := s.Run("screen-length 0 temporary", setupTimeout); err != nil {
		s.Close()
		return nil, fmt.Errorf("disable paging: %w", err)
	}
	return s, nil
}

func (s *Session) readLoop(r io.Reader) {
	defer close(s.chunks)
	for {
		b := make([]byte, 4096)
		n, err := r.Read(b)
		if n > 0 {
			s.chunks <- b[:n]
		}
		if err != nil {
			return
		}
	}
}

func (s *Session) text() string {
	return strings.ReplaceAll(string(s.buf), "\r", "")
}

func (s *Session) learnPrompt() error {
	timer := time.NewTimer(loginTimeout)
	defer timer.Stop()
	for {
		if m := promptRe.FindStringSubmatch(s.text()); m != nil {
			if strings.Contains(strings.ToUpper(m[1]), "Y/N") {
				return fmt.Errorf("router asked an interactive question after login: %q", strings.TrimSpace(s.text()))
			}
			s.prompt = m[1]
			s.buf = s.buf[:0]
			return nil
		}
		select {
		case chunk, ok := <-s.chunks:
			if !ok {
				return fmt.Errorf("%w: during login", ErrClosed)
			}
			s.buf = append(s.buf, chunk...)
		case <-timer.C:
			return fmt.Errorf("%w: no prompt after login", ErrTimeout)
		}
	}
}

// Run sends cmd and returns its output without the command echo and the prompt.
func (s *Session) Run(cmd string, timeout time.Duration) (string, error) {
	s.drain()
	if _, err := io.WriteString(s.stdin, cmd+"\n"); err != nil {
		s.Close()
		return "", fmt.Errorf("%w: write: %v", ErrClosed, err)
	}
	out, err := s.waitPrompt(timeout)
	switch {
	case err == nil:
		return cleanOutput(out, cmd), nil
	case errors.Is(err, ErrTimeout):
		_, _ = io.WriteString(s.stdin, "\x03")
		if _, err := s.waitPrompt(ctrlCTimeout); err != nil {
			s.Close()
			return "", fmt.Errorf("%w: %w: no prompt after Ctrl-C", ErrTimeout, ErrClosed)
		}
		return "", ErrTimeout
	default:
		s.Close()
		return "", err
	}
}

// drain drops output left over from an interrupted command.
func (s *Session) drain() {
	s.buf = s.buf[:0]
	for {
		select {
		case _, ok := <-s.chunks:
			if !ok {
				return
			}
		default:
			return
		}
	}
}

func (s *Session) waitPrompt(timeout time.Duration) (string, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		trimmed := strings.TrimRight(s.text(), " \n")
		if strings.HasSuffix(trimmed, s.prompt) {
			s.buf = s.buf[:0]
			return strings.TrimSuffix(trimmed, s.prompt), nil
		}
		select {
		case chunk, ok := <-s.chunks:
			if !ok {
				return "", ErrClosed
			}
			s.buf = append(s.buf, chunk...)
		case <-timer.C:
			return "", ErrTimeout
		}
	}
}

func cleanOutput(out, cmd string) string {
	lines := strings.Split(out, "\n")
	if len(lines) > 0 && strings.Contains(lines[0], cmd) {
		lines = lines[1:]
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

// Close ends the shell and the SSH connection.
func (s *Session) Close() error {
	var err error
	s.closeOnce.Do(func() {
		_ = s.sess.Close()
		err = s.client.Close()
	})
	return err
}
```

- [ ] **Step 5: Atualizar módulos, rodar e ver passar**

```bash
go mod tidy
go test ./remote/ -v -race
```

Expected: `golang.org/x/crypto` passa para o bloco `require` direto no `go.mod`. PASS em `TestDialAndRunPing`, `TestDialSkipsBanner`, `TestDialRejectsInteractiveQuestion`, `TestDialRejectsUnknownHostKey`, `TestDialWrongPassword`, `TestRunTimeoutRecoversWithCtrlC`, `TestRunTimeoutWithoutPromptClosesSession` e nos testes da Task 2, sem data race.

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum remote/session.go remote/session_test.go remote/fakevrp_test.go
git commit -m "remote: add persistent VRP shell session over SSH

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Scheduler, pool de sessões e métricas de saúde

**Files:**
- Create: `remote/metrics.go`
- Create: `remote/scheduler.go`
- Test: `remote/scheduler_test.go`

**Interfaces:**
- Consumes: `Job`, `Result`, `BuildCommand`, `CommandDeadline`, `ParseOutput`, `ErrTimeout`, `ErrClosed` (Tasks 2 e 3), fixtures `ipv4OK`, `vrpError`
- Produces:
  - `type Runner interface { Run(cmd string, timeout time.Duration) (string, error); Close() error }` (`*Session` satisfaz)
  - `type Dialer func(ctx context.Context) (Runner, error)`
  - `type Recorder interface { Record(t *Target, r Result) }`
  - `type Target struct { Host, Link string; Interval time.Duration; Labels map[string]string; Job Job }` com `func (t *Target) Sent() uint64` e `func (t *Target) AddSent(n int)`
  - `func NewScheduler(router string, sessions int, dial Dialer, targets []*Target, rec Recorder, logger *slog.Logger) *Scheduler`
  - `func (s *Scheduler) Start()`, `func (s *Scheduler) Stop()`, `func (s *Scheduler) Targets() []*Target`
  - métricas de pacote `sessionsUp`, `remoteErrors`, `jobsSkipped` e constantes `reasonConnect`, `reasonTimeout`, `reasonParse`

- [ ] **Step 1: Escrever os testes que falham**

`remote/scheduler_test.go` (cabeçalho de licença):

```go
package remote

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

var nopLogger = slog.New(slog.DiscardHandler)

type fakeRunner struct {
	run      func(cmd string) (string, error)
	delay    time.Duration
	inFlight *atomic.Int32
	closed   atomic.Bool
}

func (f *fakeRunner) Run(cmd string, _ time.Duration) (string, error) {
	if f.inFlight != nil {
		f.inFlight.Add(1)
		defer f.inFlight.Add(-1)
	}
	time.Sleep(f.delay)
	return f.run(cmd)
}

func (f *fakeRunner) Close() error {
	f.closed.Store(true)
	return nil
}

type fakeRecorder struct {
	mu  sync.Mutex
	got map[string]int
}

func (r *fakeRecorder) Record(t *Target, _ Result) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.got == nil {
		r.got = map[string]int{}
	}
	r.got[t.Link]++
}

func (r *fakeRecorder) count(link string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.got[link]
}

func testTargets(interval time.Duration, links ...string) []*Target {
	var ts []*Target
	for _, l := range links {
		ts = append(ts, &Target{Host: "1.1.1.1", Link: l, Interval: interval, Job: testJob})
	}
	return ts
}

func always(out string, err error) func(string) (string, error) {
	return func(string) (string, error) { return out, err }
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestSchedulerRecordsEveryTarget(t *testing.T) {
	rec := &fakeRecorder{}
	dial := func(context.Context) (Runner, error) { return &fakeRunner{run: always(ipv4OK, nil)}, nil }
	targets := testTargets(50*time.Millisecond, "a", "b", "c")
	s := NewScheduler("sched-ok", 2, dial, targets, rec, nopLogger)
	s.Start()
	waitFor(t, "two results per link", func() bool {
		return rec.count("a") >= 2 && rec.count("b") >= 2 && rec.count("c") >= 2
	})
	waitFor(t, "two sessions up", func() bool { return testutil.ToFloat64(sessionsUp.WithLabelValues("sched-ok")) == 2 })
	s.Stop()
	for _, tg := range targets {
		if tg.Sent() < 6 {
			t.Errorf("link %s: sent = %d, want >= 6", tg.Link, tg.Sent())
		}
	}
	if got := testutil.ToFloat64(sessionsUp.WithLabelValues("sched-ok")); got != 0 {
		t.Errorf("sessions_up after Stop = %v, want 0", got)
	}
}

func TestSchedulerSkipsWhilePending(t *testing.T) {
	dial := func(context.Context) (Runner, error) {
		return &fakeRunner{run: always(ipv4OK, nil), delay: 200 * time.Millisecond}, nil
	}
	s := NewScheduler("sched-skip", 1, dial, testTargets(20*time.Millisecond, "a"), &fakeRecorder{}, nopLogger)
	s.Start()
	waitFor(t, "a skipped job", func() bool { return testutil.ToFloat64(jobsSkipped.WithLabelValues("sched-skip")) > 0 })
	s.Stop()
}

func TestSchedulerParseErrorIsNotLoss(t *testing.T) {
	rec := &fakeRecorder{}
	dial := func(context.Context) (Runner, error) { return &fakeRunner{run: always(vrpError, nil)}, nil }
	targets := testTargets(20*time.Millisecond, "a")
	s := NewScheduler("sched-parse", 1, dial, targets, rec, nopLogger)
	s.Start()
	waitFor(t, "a parse error", func() bool {
		return testutil.ToFloat64(remoteErrors.WithLabelValues("sched-parse", reasonParse)) > 0
	})
	s.Stop()
	if rec.count("a") != 0 || targets[0].Sent() != 0 {
		t.Errorf("parse error was recorded: results=%d sent=%d", rec.count("a"), targets[0].Sent())
	}
}

func TestSchedulerTimeoutKeepsSession(t *testing.T) {
	var dials, calls atomic.Int32
	rec := &fakeRecorder{}
	dial := func(context.Context) (Runner, error) {
		dials.Add(1)
		return &fakeRunner{run: func(string) (string, error) {
			if calls.Add(1) == 1 {
				return "", ErrTimeout
			}
			return ipv4OK, nil
		}}, nil
	}
	s := NewScheduler("sched-timeout", 1, dial, testTargets(20*time.Millisecond, "a"), rec, nopLogger)
	s.Start()
	waitFor(t, "a result after the timeout", func() bool { return rec.count("a") > 0 })
	s.Stop()
	if got := testutil.ToFloat64(remoteErrors.WithLabelValues("sched-timeout", reasonTimeout)); got != 1 {
		t.Errorf("timeout errors = %v, want 1", got)
	}
	if dials.Load() != 1 {
		t.Errorf("dials = %d, want 1 (session kept after a recovered timeout)", dials.Load())
	}
}

func TestSchedulerReplacesClosedSession(t *testing.T) {
	var dials atomic.Int32
	rec := &fakeRecorder{}
	dial := func(context.Context) (Runner, error) {
		if dials.Add(1) == 1 {
			return &fakeRunner{run: always("", fmt.Errorf("%w: %w", ErrTimeout, ErrClosed))}, nil
		}
		return &fakeRunner{run: always(ipv4OK, nil)}, nil
	}
	s := NewScheduler("sched-closed", 1, dial, testTargets(20*time.Millisecond, "a"), rec, nopLogger)
	s.Start()
	waitFor(t, "a result from the new session", func() bool { return rec.count("a") > 0 })
	s.Stop()
	if dials.Load() < 2 {
		t.Errorf("dials = %d, want >= 2", dials.Load())
	}
}

func TestSchedulerRetriesDial(t *testing.T) {
	var dials atomic.Int32
	rec := &fakeRecorder{}
	dial := func(context.Context) (Runner, error) {
		if dials.Add(1) <= 2 {
			return nil, errors.New("connection refused")
		}
		return &fakeRunner{run: always(ipv4OK, nil)}, nil
	}
	s := NewScheduler("sched-dial", 1, dial, testTargets(20*time.Millisecond, "a"), rec, nopLogger)
	s.minBackoff = 10 * time.Millisecond
	s.Start()
	waitFor(t, "a result after reconnecting", func() bool { return rec.count("a") > 0 })
	s.Stop()
	if got := testutil.ToFloat64(remoteErrors.WithLabelValues("sched-dial", reasonConnect)); got != 2 {
		t.Errorf("connect errors = %v, want 2", got)
	}
}

func TestSchedulerStopWaitsForRunningJob(t *testing.T) {
	var inFlight atomic.Int32
	runner := &fakeRunner{run: always(ipv4OK, nil), delay: 100 * time.Millisecond, inFlight: &inFlight}
	dial := func(context.Context) (Runner, error) { return runner, nil }
	s := NewScheduler("sched-stop", 1, dial, testTargets(10*time.Millisecond, "a"), &fakeRecorder{}, nopLogger)
	s.Start()
	waitFor(t, "a job in flight", func() bool { return inFlight.Load() == 1 })
	s.Stop()
	if inFlight.Load() != 0 {
		t.Error("Stop returned while a job was still running")
	}
	if !runner.closed.Load() {
		t.Error("Stop did not close the session")
	}
}
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `go test ./remote/`
Expected: FAIL de compilação (`undefined: NewScheduler`, `undefined: sessionsUp`...).

- [ ] **Step 3: Implementar as métricas** em `remote/metrics.go` (cabeçalho de licença):

```go
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
```

- [ ] **Step 4: Implementar o scheduler** em `remote/scheduler.go` (cabeçalho de licença):

```go
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
```

- [ ] **Step 5: Rodar e ver passar**

Run: `go test ./remote/ -v -race`
Expected: PASS em todos os `TestScheduler*` e nos testes das Tasks 2 e 3, sem data race.

- [ ] **Step 6: Commit**

```bash
git add remote/metrics.go remote/scheduler.go remote/scheduler_test.go
git commit -m "remote: schedule pings on a pool of router sessions

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Alvos a partir da config e dialer SSH

**Files:**
- Create: `remote/targets.go`
- Test: `remote/targets_test.go`

**Interfaces:**
- Consumes: `config.TargetGroup`, `config.Router`, `Router.SelectLinks` (Task 1); `Target`, `Job`, `Dialer`, `Runner` (Task 4); `Dial`, `SSHConfig` (Task 3)
- Produces:
  - `func Resolve(host, network string) (net.IP, error)`: um literal decide a família; um nome é resolvido localmente, com preferência por IPv4 quando `network` é `ip`
  - `func BuildTargets(tg config.TargetGroup, r config.Router, resolve func(host, network string) (net.IP, error), logger *slog.Logger) ([]*Target, error)`
  - `func NewSSHDialer(r config.Router, logger *slog.Logger) (Dialer, error)`: lê `password_file`/`private_key_file` na criação

- [ ] **Step 1: Escrever os testes que falham**

`remote/targets_test.go` (cabeçalho de licença):

```go
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
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `go test ./remote/`
Expected: FAIL de compilação (`undefined: BuildTargets`, `undefined: Resolve`, `undefined: NewSSHDialer`).

- [ ] **Step 3: Implementar** `remote/targets.go` (cabeçalho de licença):

```go
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
				},
			})
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
```

- [ ] **Step 4: Rodar e ver passar**

Run: `go test ./remote/ -v -race`
Expected: PASS em `TestBuildTargets*`, `TestResolveLiteralFamilyMismatch`, `TestNewSSHDialerReadsPasswordFile` e em todos os testes anteriores.

- [ ] **Step 5: Commit**

```bash
git add remote/targets.go remote/targets_test.go
git commit -m "remote: expand target groups per link and build SSH dialers

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Integração com collector e main

**Files:**
- Modify: `collector.go` (de `// SmokepingCollector collects metrics...` até o fim do arquivo)
- Modify: `main.go` (`smokePingers`, `sizeOfPrepared`, `start`, `stop`, `prepare`, `buildLabelNamesFromConfig`, as duas chamadas de `prepare` e de `NewSmokepingCollector` em `main()`)
- Test: `collector_test.go`, `main_test.go`

**Interfaces:**
- Consumes: `remote.Target`, `remote.Result`, `remote.Reply`, `remote.Scheduler`, `remote.NewScheduler`, `remote.BuildTargets`, `remote.Resolve`, `remote.NewSSHDialer`, `remote.Recorder`, `remote.Runner` (Tasks 4 e 5); `config.Config.Router` (Task 1)
- Produces:
  - `func labelValues(labelNames []string, base, custom map[string]string) []string`
  - `func remoteLabelValues(labelNames []string, t *remote.Target) []string`
  - `func newRemoteRecorder(labelNames []string, hist *prometheus.HistogramVec) *remoteRecorder` (satisfaz `remote.Recorder`)
  - `func NewSmokepingCollector(probes []probe, remoteTargets []*remote.Target, labelNames []string, pingResponseSeconds prometheus.HistogramVec) *SmokepingCollector`
  - `func (s *smokePingers) prepare(hosts *[]string, interval *time.Duration, privileged *bool, sizeBytes *int, tosField *uint8, rec remote.Recorder) error`
  - `func (s *smokePingers) remoteTargets() []*remote.Target`

- [ ] **Step 1: Escrever os testes que falham**

`collector_test.go` (cabeçalho de licença):

```go
package main

import (
	"reflect"
	"strings"
	"testing"
	"time"

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
	oldHist := initMetrics(oldNames, prometheus.DefBuckets, 1.05)
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
```

`main_test.go` (cabeçalho de licença):

```go
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
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `go test .`
Expected: FAIL de compilação (`undefined: labelValues`, `undefined: newRemoteRecorder`, `sp.preparedRemote undefined`, número errado de argumentos em `NewSmokepingCollector` e `prepare`).

- [ ] **Step 3: Implementar em `collector.go`**

Acrescentar `"github.com/SuperQ/smokeping_prober/remote"` num terceiro bloco de imports. Substituir tudo desde `// SmokepingCollector collects metrics from the probes and their pingers.` até o fim do arquivo por:

```go
// SmokepingCollector collects metrics from the probes and their pingers.
type SmokepingCollector struct {
	probes *[]probe
	remote []*remote.Target

	requestsSent *prometheus.Desc
	labelNames   []string
}

func NewSmokepingCollector(probes []probe, remoteTargets []*remote.Target, labelNames []string, pingResponseSeconds prometheus.HistogramVec) *SmokepingCollector {
	instance := SmokepingCollector{
		requestsSent: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "requests_total"),
			"Number of ping requests sent",
			labelNames,
			nil,
		),
		labelNames: labelNames,
		remote:     remoteTargets,
	}

	instance.updateProbes(probes, pingResponseSeconds)

	return &instance
}

// labelValues orders base and custom label values to match labelNames.
// A name found in neither map gets an empty value.
func labelValues(labelNames []string, base, custom map[string]string) []string {
	vals := make([]string, 0, len(labelNames))
	for _, k := range labelNames {
		if v, ok := base[k]; ok {
			vals = append(vals, v)
			continue
		}
		vals = append(vals, custom[k])
	}
	return vals
}

func (s *SmokepingCollector) buildLabelValues(pr *probe, overrideIP string) []string {
	ip := pr.pinger.IPAddr().String()
	if overrideIP != "" {
		ip = overrideIP
	}
	return labelValues(s.labelNames, map[string]string{
		"ip":     ip,
		"host":   pr.pinger.Addr(),
		"source": pr.pinger.Source,
		"tos":    strconv.Itoa(int(pr.pinger.TrafficClass())),
		"link":   "",
	}, pr.labels)
}

func remoteLabelValues(labelNames []string, t *remote.Target) []string {
	return labelValues(labelNames, map[string]string{
		"ip":     t.Job.Target,
		"host":   t.Host,
		"source": t.Job.Source,
		"tos":    strconv.Itoa(int(t.Job.ToS)),
		"link":   t.Link,
	}, t.Labels)
}

// remoteRecorder writes remote results into the vectors that were current when
// it was created, so a scheduler that is still stopping during a reload never
// writes to vectors with a different label set.
type remoteRecorder struct {
	labelNames []string
	hist       *prometheus.HistogramVec
	ttl        *prometheus.GaugeVec
}

func newRemoteRecorder(labelNames []string, hist *prometheus.HistogramVec) *remoteRecorder {
	return &remoteRecorder{labelNames: labelNames, hist: hist, ttl: pingResponseTTL}
}

func (r *remoteRecorder) Record(t *remote.Target, res remote.Result) {
	vals := remoteLabelValues(r.labelNames, t)
	for _, rep := range res.Replies {
		r.hist.WithLabelValues(vals...).Observe(rep.RTT.Seconds())
	}
	if n := len(res.Replies); n > 0 {
		r.ttl.WithLabelValues(vals...).Set(float64(res.Replies[n-1].TTL))
	}
}
```

Depois, manter `updateProbes` exatamente como está, acrescentando antes da linha final `s.probes = &probes`:

```go
	for _, t := range s.remote {
		// Init remote series to 0s.
		vals := remoteLabelValues(s.labelNames, t)
		pingResponseSeconds.WithLabelValues(vals...)
		pingResponseTTL.WithLabelValues(vals...)
	}
```

Manter `Describe` e, em `Collect`, acrescentar depois do laço existente:

```go
	for _, t := range s.remote {
		ch <- prometheus.MustNewConstMetric(
			s.requestsSent,
			prometheus.CounterValue,
			float64(t.Sent()),
			remoteLabelValues(s.labelNames, t)...,
		)
	}
```

- [ ] **Step 4: Implementar em `main.go`**

Acrescentar `"github.com/SuperQ/smokeping_prober/remote"` ao bloco local de imports (ao lado de `config`).

Trocar `smokePingers`, `sizeOfPrepared`, `start` e `stop` por:

```go
type smokePingers struct {
	started        []probe
	prepared       []probe
	startedRemote  []*remote.Scheduler
	preparedRemote []*remote.Scheduler
	g              *errgroup.Group
	maxInterval    time.Duration
}

func (s *smokePingers) sizeOfPrepared() int {
	n := len(s.prepared)
	for _, sch := range s.preparedRemote {
		n += len(sch.Targets())
	}
	return n
}

func (s *smokePingers) remoteTargets() []*remote.Target {
	var targets []*remote.Target
	for _, sch := range s.startedRemote {
		targets = append(targets, sch.Targets()...)
	}
	return targets
}

func (s *smokePingers) start() {
	if s.sizeOfPrepared() == 0 {
		return
	}
	if err := s.stop(); err != nil {
		logger.Warn("At least one previous pinger failed to run", "err", err)
	}
	s.startedRemote = s.preparedRemote
	s.preparedRemote = nil
	for _, sch := range s.startedRemote {
		sch.Start()
	}
	s.g = new(errgroup.Group)
	s.started = s.prepared
	s.prepared = nil
	if len(s.started) == 0 {
		return
	}
	splay := time.Duration(s.maxInterval.Nanoseconds() / int64(len(s.started)))
	for _, pr := range s.started {
		pinger := pr.pinger
		logger.Info("Starting prober", "address", pinger.Addr(), "interval", pinger.Interval, "size_bytes", pinger.Size, "source_address", pinger.Source)
		s.g.Go(
			func() error {
				err := pinger.Run()
				if err != nil {
					proberErrors.Inc()
					logger.Warn("Pinger exited with error",
						"address", pinger.Addr(),
						"interval", pinger.Interval,
						"size_bytes", pinger.Size,
						"source_address", pinger.Source,
						"err", err,
					)
				}
				return err
			})
		time.Sleep(splay)
	}
}

func (s *smokePingers) stop() error {
	for _, sch := range s.startedRemote {
		sch.Stop()
	}
	s.startedRemote = nil
	if s.g == nil {
		return nil
	}
	if s.started == nil {
		return nil
	}
	for _, pr := range s.started {
		pr.pinger.Stop()
	}
	if err := s.g.Wait(); err != nil {
		return fmt.Errorf("pingers failed: %v", err)
	}
	return nil
}
```

Em `prepare`, mudar a assinatura para acrescentar `rec remote.Recorder` no fim. Logo antes de `for _, targetGroup := range sc.C.Targets {`, declarar:

```go
	remoteTargets := map[string][]*remote.Target{}
```

Dentro do laço, logo depois da checagem de `packetSize` e **antes** da checagem de `targetGroup.Interval > maxInterval` (para intervalos remotos não esticarem o splay local):

```go
		if targetGroup.Router != "" {
			router, _ := sc.C.Router(targetGroup.Router)
			targets, err := remote.BuildTargets(targetGroup, router, remote.Resolve, logger)
			if err != nil {
				return fmt.Errorf("router %q: %w", router.Name, err)
			}
			remoteTargets[router.Name] = append(remoteTargets[router.Name], targets...)
			continue
		}
```

E trocar o final (`s.prepared = probes` / `s.maxInterval = maxInterval` / `return nil`) por:

```go
	schedulers := make([]*remote.Scheduler, 0, len(remoteTargets))
	for _, router := range sc.C.Routers {
		targets := remoteTargets[router.Name]
		if len(targets) == 0 {
			continue
		}
		dial, err := remote.NewSSHDialer(router, logger)
		if err != nil {
			return fmt.Errorf("router %q: %w", router.Name, err)
		}
		logger.Info("Prepared remote prober", "router", router.Name, "address", router.Address, "sessions", router.Sessions, "targets", len(targets))
		schedulers = append(schedulers, remote.NewScheduler(router.Name, router.Sessions, dial, targets, rec, logger))
	}
	s.prepared = probes
	s.preparedRemote = schedulers
	s.maxInterval = maxInterval
	return nil
```

Em `buildLabelNamesFromConfig`, trocar a base e o filtro:

```go
	base := []string{"ip", "host", "source", "tos", "link"}
```

```go
			if slices.Contains(base, k) {
				continue
			}
```

Em `main()`, na carga inicial:

```go
	err = smokePingers.prepare(hosts, interval, privileged, sizeBytes, tosField, newRemoteRecorder(labelNames, pingResponseSeconds))
```

```go
	smokepingCollector = NewSmokepingCollector(smokePingers.started, smokePingers.remoteTargets(), labelNames, *pingResponseSeconds)
```

E no reload:

```go
			err = smokePingers.prepare(hosts, interval, privileged, sizeBytes, tosField, newRemoteRecorder(newLabelNames, pingResponseSeconds))
```

```go
			smokepingCollector = NewSmokepingCollector(smokePingers.started, smokePingers.remoteTargets(), newLabelNames, *pingResponseSeconds)
```

- [ ] **Step 5: Rodar e ver passar**

```bash
go mod tidy
go vet ./...
go test ./... -race
```

Expected: `github.com/prometheus/client_model` passa a direto no `go.mod` (usado no teste). PASS em `TestLabelValues`, `TestRemoteRecorderObserves`, `TestRemoteRecorderKeepsItsOwnVectors`, `TestCollectorRemoteRequestsTotal`, `TestBuildLabelNamesIncludesLink`, `TestPrepareSplitsLocalAndRemote`, `TestStartRemoteOnly` e em todos os testes de `config` e `remote`.

- [ ] **Step 6: Teste de fumaça local**

```bash
go build -o /tmp/smokeping_prober . && /tmp/smokeping_prober --config.file=smokeping_prober.yml --web.listen-address=127.0.0.1:9374 --privileged=false &
sleep 3; curl -s 127.0.0.1:9374/metrics | grep 'smokeping_requests_total'; kill %1
```

Expected: a série de `localhost` aparece com `link=""`. Se o pinger local não subir sem privilégio, o importante é o binário iniciar e expor `/metrics`.

- [ ] **Step 7: Commit**

```bash
git add go.mod go.sum collector.go collector_test.go main.go main_test.go
git commit -m "Wire remote schedulers into the prober with a link label

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Documentação e verificação final

**Files:**
- Modify: `README.md` (nova seção depois de `## Configuration`; tabela de métricas)
- Modify: `smokeping_prober.yml` (exemplo comentado)
- Modify: `CHANGELOG.md` (`master / unreleased`)

- [ ] **Step 1: README.** Depois do parágrafo "The config is read on startup, and can be reloaded..." de `## Configuration`, acrescentar:

````markdown
### Remote ping through a Huawei VRP router

A target group with `router:` is pinged by the router instead of the local host. The prober keeps persistent SSH sessions to the router and runs `ping -a <link source> <target>` once per link, so a router with several uplinks produces one series per uplink, told apart by the `link` label.

```yaml
routers:
- name: ne8k
  address: 10.0.0.1:22
  username: smokeping
  password_file: /etc/smokeping_prober/ne8k.pass  # or private_key_file
  known_hosts: /etc/smokeping_prober/known_hosts  # or insecure_skip_host_key: true (lab only)
  sessions: 5                                     # Default 5
  links:
  - name: isp-a
    source: 192.0.2.1
    source6: 2001:db8::1   # Optional, needed for IPv6 targets
  - name: isp-b
    source: 198.51.100.1

targets:
- host: 8.8.8.8
  router: ne8k
  interval: 1m          # Default 1m for router targets
  count: 10             # Echo requests per run (-c). Default 10
  packet_interval: 50ms # Time between requests (-m). Default 50ms
  timeout: 500ms        # Wait per reply (-t). Default 500ms
  # links: [isp-a]      # Optional subset of the router links
```

Notes:

* The router reports round-trip times in whole milliseconds, so remote histograms have 1 ms resolution.
* `protocol` is ignored and `source` cannot be set for router targets; the source comes from each link.
* Create `known_hosts` with `ssh-keyscan -p 22 10.0.0.1 > known_hosts` and check the fingerprint on the router.
* The SSH user only needs to run `ping` and `screen-length 0 temporary` in user view.
* A failed SSH session, timeout or unexpected output never counts as packet loss. It shows up in `smokeping_remote_errors_total` instead.
````

Na tabela de `## Metrics`, acrescentar as linhas:

```markdown
 smokeping\_remote\_sessions\_up         | Gauge      | SSH sessions connected and ready per router.
 smokeping\_remote\_errors\_total        | Counter    | Remote ping runs with no result, by `reason` (`connect`, `timeout`, `parse`).
 smokeping\_remote\_jobs\_skipped\_total | Counter    | Remote runs dropped because the previous run for the same target and link was still pending.
```

- [ ] **Step 2: Exemplo de config.** No fim de `smokeping_prober.yml`, acrescentar (comentado, para o arquivo continuar carregando sem roteador):

```yaml
# Ping through a Huawei VRP router, once per uplink. See README.md.
# routers:
# - name: ne8k
#   address: 10.0.0.1:22
#   username: smokeping
#   password_file: /etc/smokeping_prober/ne8k.pass
#   known_hosts: /etc/smokeping_prober/known_hosts
#   links:
#   - name: isp-a
#     source: 192.0.2.1
# targets:
# - host: 8.8.8.8
#   router: ne8k
```

- [ ] **Step 3: CHANGELOG.** Trocar a linha `* [FEATURE]` vazia de `master / unreleased` por:

```markdown
* [FEATURE] Ping through Huawei VRP routers over SSH, one series per uplink (`link` label)
```

- [ ] **Step 4: Verificação completa**

```bash
go build ./... && go vet ./... && go test ./... -race
make GO_ONLY=1 SKIP_GOLANGCI_LINT=1
git diff --exit-code -- go.mod go.sum
make lint
```

Expected: tudo passa. `make lint` baixa o golangci-lint na primeira vez. Se ele acusar formatação (gci/gofumpt), rodar `make format`, conferir o diff e repetir. Se `make lint` não conseguir baixar a ferramenta, registrar isso no relatório final em vez de marcar o passo como feito.

- [ ] **Step 5: Commit**

```bash
git add README.md smokeping_prober.yml CHANGELOG.md
git commit -m "docs: document remote ping through Huawei VRP routers

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 6: Teste manual no NE8000 (usuário).** Não é executado pelo agente. Entregar ao usuário este roteiro: criar o `password_file` e o `known_hosts`, adicionar `routers:` e `router: ne8k` a um grupo, rodar com `--log.level=debug`, conferir no log os comandos `ping -c 10 -m 50 -t 500 ... -a <ip do link> 8.8.8.8` e a saída bruta, e em `/metrics` as três séries com `link="..."`, além de `smokeping_remote_sessions_up{router="ne8k"} 5`.
