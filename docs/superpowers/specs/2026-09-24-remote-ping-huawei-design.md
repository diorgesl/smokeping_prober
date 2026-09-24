# RemotePing via Huawei NE8000: design

Data: 2026-09-24
Status: aguardando revisão

## Objetivo

Medir latência e perda de cada destino separadamente por operadora. O roteador de borda (Huawei NE8000 F1A) tem 3 links IP com BGP, e cada um fornece um /32 (e, opcionalmente, um endereço IPv6). O prober entra no roteador por SSH e manda o próprio roteador pingar o destino, uma vez para cada IP de origem. No Grafana, cada destino vira um painel com uma linha por operadora, e dá para ver qual link está com problema.

Considera-se pronto quando, para um destino como `8.8.8.8`, o Prometheus recebe três séries de `smokeping_response_duration_seconds`, diferenciadas pelo label `link`, com os labels personalizados que a config já usa hoje (`category`, `menu`, `title`, `smokeping_name`, `alerts_enabled`).

## Fora do escopo

- Outros fabricantes além do Huawei VRP.
- NQA, SNMP ou NETCONF (o NQA foi descartado porque exigiria configurar ~200 testes no roteador).
- Defaults por roteador para `interval`/`count`/`timeout`. Cada grupo repete os valores, como a config atual já faz.
- Uma série agregada "todos os links".

## Decisões

| tema | decisão |
|---|---|
| acesso | SSH com shell interativo (PTY) e sessões persistentes |
| concorrência | pool de N sessões por roteador (padrão 5) |
| intervalo | configurável por grupo; o recomendado é 1 min |
| parâmetros do ping | `count: 10`, `packet_interval: 50ms`, `timeout: 500ms`, configuráveis por grupo |
| gráfico | séries separadas por link; o agrupamento acontece no Grafana |
| onde fica | modo novo dentro do próprio `smokeping_prober`, não um binário separado |
| IPv6 | suportado quando o link tem `source6` |
| resolução | 1 ms, porque o VRP imprime a latência em ms inteiros |

Com 200 destinos × 3 links = 600 testes por intervalo, e cada teste levando ~0,5 s com o link bom (até ~5,5 s com 100% de perda), 5 sessões fecham uma rodada em 1 a 3 minutos com o link saudável. Quando uma rodada não cabe no intervalo, os jobs atrasados são descartados e contados (ver Erros).

## Configuração

Aparecem uma seção `routers:` nova e campos novos em `targets:`. Um grupo sem `router:` funciona exatamente como hoje, e a config atual do usuário carrega sem mudança.

```yaml
routers:
- name: ne8k
  address: 10.0.0.1:22
  username: smokeping
  password_file: /etc/smokeping_prober/ne8k.pass   # ou private_key_file
  known_hosts: /etc/smokeping_prober/known_hosts
  # insecure_skip_host_key: true                   # só laboratório; loga aviso a cada conexão
  sessions: 5
  links:
  - name: operadora-a
    source: 201.131.152.1
    source6: 2804:194c:1000::155:f0ca:a
  - name: operadora-b
    source: 201.131.152.5
  - name: operadora-c
    source: 201.131.152.9

targets:
  - host: "8.8.8.8"
    router: ne8k
    interval: 1m
    count: 10
    packet_interval: 50ms
    timeout: 500ms
    network: ip4
    size: 56
    tos: 0x00
    # links: [operadora-a, operadora-b]   # opcional; o padrão é todos os links do roteador
    labels:
      category: "DNS"
      smokeping_name: "Google-1-v4"
```

### Campos em grupos remotos

| campo | uso |
|---|---|
| `router` | nome de uma entrada de `routers:`; é ele que torna o grupo remoto |
| `links` | subconjunto opcional de links do roteador |
| `interval` | intervalo entre coletas de cada par (destino, link); padrão 1m em grupos remotos (o 1s dos grupos locais não se aplica) |
| `count` | `-c`; padrão 10 |
| `packet_interval` | `-m`, em ms; padrão 50ms |
| `timeout` | `-t`, em ms; padrão 500ms |
| `size` | `-s`; mesma validação de hoje (24 a 65535) |
| `tos` | `-tos` no IPv4, `-tc` no IPv6 |
| `network` | `ip4`, `ip6` ou `ip` (automático) |
| `protocol` | ignorado; o roteador sempre usa ICMP |
| `source` | não pode ser usado; a origem vem do link. Usar é erro de config |

`count`, `packet_interval` e `timeout` num grupo sem `router:` são erro de config.

### Escolha de família

- `ip4`: usa `link.source`.
- `ip6`: usa `link.source6`.
- `ip`: um literal IPv4 ou IPv6 decide a família. Um hostname é resolvido pelo prober (não pelo roteador), com preferência pelo IPv4, como o modo local já faz.

Um link sem o endereço da família do destino é pulado, com aviso no log. Se nenhum link do grupo tiver o endereço da família, a config é rejeitada.

### Validação

Erros que rejeitam a config no load e no reload (no reload, a config anterior continua rodando):

- `router:` aponta para um nome inexistente.
- `links:` cita um link que o roteador não tem.
- nomes duplicados em `routers:` ou em `links:`.
- `source`/`source6` não são IPs válidos da família certa.
- sem `password_file` nem `private_key_file`.
- sem `known_hosts` e sem `insecure_skip_host_key: true`.
- `sessions` < 1.

## Métricas e labels

O conjunto base de labels passa de `ip, host, source, tos` para `ip, host, source, tos, link`, seguido das chaves personalizadas em ordem, como hoje. Nos pingers locais, `link` fica vazio. Um label personalizado chamado `link` é descartado, como já acontece com os outros nomes base.

Nos testes remotos:

- `host`: o `host` da config.
- `ip`: o IP de destino (literal, ou resolvido pelo prober).
- `source`: o IP de origem do link.
- `link`: o nome do link.
- `tos`: o `tos` do grupo.

Métricas que já existem e passam a receber dados remotos:

| métrica | origem no modo remoto |
|---|---|
| `smokeping_requests_total` | soma de `N packet(s) transmitted` |
| `smokeping_response_duration_seconds` | uma observação por linha de resposta (`time=X ms` → X/1000 s) |
| `smokeping_response_ttl` | `ttl=` (IPv4) ou `hop limit=` (IPv6) da última resposta |

A perda continua sendo `requests_total − response_duration_seconds_count`, como no modo local, e o dashboard e as regras atuais valem sem mudança. `smokeping_response_duplicates_total`, `smokeping_send_errors_total` e `smokeping_receive_errors_total` não recebem dados remotos.

Métricas novas (labels só `router`, e `reason` quando indicado):

| métrica | tipo | significado |
|---|---|---|
| `smokeping_remote_sessions_up{router}` | gauge | sessões SSH conectadas e prontas |
| `smokeping_remote_errors_total{router, reason}` | counter | `reason` ∈ `connect`, `timeout`, `parse` |
| `smokeping_remote_jobs_skipped_total{router}` | counter | jobs descartados porque o anterior do mesmo par ainda estava pendente |

Exemplo de consulta no Grafana (3 linhas, uma por operadora):

```promql
histogram_quantile(0.9, sum by (link, le) (rate(smokeping_response_duration_seconds_bucket{host="8.8.8.8"}[5m])))
```

## Arquitetura

Um pacote novo, `remote/`, com três unidades independentes, e um ajuste pequeno em `main.go`/`collector.go`.

### `remote/huawei.go`: comando e parser (funções puras)

`BuildCommand(job Job) string`:

- IPv4: `ping -c <count> -m <packet_interval_ms> -t <timeout_ms> -s <size> -tos <tos> -a <source> <dst>`
- IPv6: `ping ipv6 -c <count> -m <packet_interval_ms> -t <timeout_ms> -s <size> -tc <tos> -a <source6> <dst>`

`ParseOutput(out string) (Result, error)`, com `Result{Sent int; Replies []Reply{Seq int; RTT time.Duration; TTL int}}`:

- cada ocorrência de `Sequence=N` seguida de `ttl=T` ou `hop limit=T` e de `time=X ms` vira uma `Reply`. No IPv6, `Reply from <ip>` e os dados vêm em linhas separadas, então o parser se ancora em `Sequence=`, e não em `Reply from`.
- `Sent` vem de `N packet(s) transmitted`.
- `Request time out` é contado só para conferir: `Sent == len(Replies) + timeouts`. Se a conta não fechar, o resultado vale mesmo assim e entra um aviso em nível debug.
- sem a linha `packet(s) transmitted` (por exemplo, `Error: ...` do VRP), a função retorna erro, e a primeira linha não vazia entra na mensagem.

### `remote/session.go`: sessão SSH persistente

- `Dial` usa `golang.org/x/crypto/ssh` com senha ou chave e valida a host key por `known_hosts` (`golang.org/x/crypto/ssh/knownhosts`).
- Abre o shell com PTY, lê até o primeiro prompt e aprende o nome do host pelo formato `<nome>`. Aceita também `[nome]` e `[~nome...]` por segurança.
- Envia `screen-length 0 temporary` e espera o prompt.
- `Run(ctx, cmd) (string, error)` escreve o comando, lê até o prompt reaparecer e devolve a saída sem o eco do comando e sem o prompt.
- Deadline por comando: `count × (timeout + packet_interval) + 5s`. Se estourar, manda Ctrl-C (`\x03`) e espera o prompt por 3s. Se o prompt não voltar, fecha a sessão e retorna erro `timeout`.
- Uma sessão com erro fica marcada como morta e é o dono (o scheduler) que decide reconectar.

### `remote/scheduler.go`: pool e agendamento

- Um `Scheduler` por roteador, com N workers, e cada worker é dono de uma sessão.
- O worker reconecta com backoff exponencial (1s → 30s) quando a sessão está morta e atualiza `sessions_up`.
- Cada par (destino, link) é um job com timer próprio de `interval`. A primeira execução de cada job acontece em `i × interval / total_jobs` (splay), para os 600 não dispararem juntos.
- Fila compartilhada entre os workers. Quando o timer de um par dispara e o job anterior desse par ainda está na fila ou rodando, o novo é descartado e `jobs_skipped_total` é incrementado.
- Ao terminar um job, o worker chama um `Recorder` (interface) com o `Result`, ou incrementa `errors_total` em caso de erro. Erro nunca vira perda: o par simplesmente fica sem dados naquele ciclo.
- `Stop()` cancela os timers, esvazia a fila, espera os workers terminarem o comando em andamento (respeitando o deadline) e fecha as sessões.

### Integração com o código atual

- `config/config.go`: novos tipos `Router` e `Link`, `Config.Routers` e os campos novos de `TargetGroup` (`Router`, `Links`, `Count`, `PacketInterval`, `Timeout`), com os defaults e as validações acima.
- `collector.go`: a gravação de métricas a partir de uma resposta vira um recorder usado pelos dois modos. Os callbacks do pro-bing passam a chamar esse recorder. O `SmokepingCollector.Collect()` passa a emitir `requests_total` também para os probes remotos, a partir de um contador atômico que o scheduler atualiza.
- `main.go`: `smokePingers.prepare()` separa os grupos locais (como hoje) dos remotos. `start()`/`stop()` também iniciam e param os schedulers remotos. O caminho de reload continua o mesmo (reler config → recriar métricas → preparar → iniciar → recriar collector), e os pools SSH são sempre recriados no reload.
- `buildLabelNamesFromConfig()` inclui `link` na base.
- `go.mod`: dependência direta de `golang.org/x/crypto`.

## Testes

O repositório ainda não tem testes. Estes serão os primeiros.

- `remote/huawei_test.go`: tabela de `ParseOutput` com as saídas reais do NE8000 coletadas nesta conversa (IPv4 com 5/5, IPv4 3/3, IPv4 com `Request time out` e 2/3, IPv6 com o formato em duas linhas) e uma saída de erro. Tabela de `BuildCommand` para IPv4 e IPv6.
- `remote/session_test.go`: servidor SSH falso em processo (`x/crypto/ssh`) que imita o VRP: prompt `<rt-test>`, aceita `screen-length 0 temporary` e responde `ping` com saídas enlatadas. Casos: login e prompt, comando normal, deadline com Ctrl-C que recupera o prompt, deadline sem prompt (sessão morta), host key desconhecida recusada.
- `remote/scheduler_test.go`: runner falso. Casos: jobs distribuídos entre as N sessões, descarte quando o anterior está pendente, erro sem gravar perda, `Stop()` limpo.
- `config/config_test.go`: a config atual do usuário (sem `router:`) carrega igual. Um grupo remoto válido carrega. Cada regra de validação tem um caso de erro.
- Teste manual no NE8000, feito pelo usuário, com `--log.level=debug`, em que cada comando e a saída bruta aparecem no log.

## Documentação

- README: seção "Remote ping (Huawei VRP)" com exemplo de config, permissões mínimas do usuário SSH e a observação sobre a resolução de 1 ms.
- `smokeping_prober.yml`: exemplo comentado de `routers:`.
- CHANGELOG: `[FEATURE]` em `master / unreleased`.
