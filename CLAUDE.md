# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Prometheus exporter that sends continuous ICMP (or UDP) pings to targets and exposes the RTTs as histograms, in the style of smokeping. It's a single Go binary. Module path: `github.com/SuperQ/smokeping_prober`. The Go version is pinned in `.promu.yml`.

## Commands

The build uses the standard Prometheus `Makefile.common` and runs through `promu`.

```sh
make                          # full pipeline: style, license check, lint, yamllint, unused, build, test
make build                    # build ./smokeping_prober via promu
make test                     # go test over all packages (-race on amd64)
make lint                     # golangci-lint (config in .golangci.yml)
make format                   # go fmt, then golangci-lint fmt (gofumpt/gci/goimports)
make GO_ONLY=1 SKIP_GOLANGCI_LINT=1   # what CI runs, followed by `git diff --exit-code`
go test -run TestName ./...   # single test (there are no *_test.go files yet)
```

Running it locally needs raw-socket privileges for ICMP: `sudo setcap cap_net_raw=+ep ./smokeping_prober`, or run as root. Example: `./smokeping_prober --config.file=smokeping_prober.yml` or `./smokeping_prober host1 host2`. It listens on `:9374` by default.

Import grouping is enforced as standard, then third-party, then `github.com/SuperQ/smokeping_prober`. gofumpt runs with extra rules. Changes that affect users go in `CHANGELOG.md` under `master / unreleased`, using the `[CHANGE]`/`[FEATURE]`/`[ENHANCEMENT]`/`[BUGFIX]` tags.

## Architecture

There are three files, and most of the logic lives in `main` package globals.

- `config/config.go`: `SafeConfig` (an RWMutex around `*Config`) plus YAML types. `TargetGroup.UnmarshalYAML` seeds `DefaultTargetGroup` (1s, `ip`, `icmp`, 56 bytes) before decoding and folds the legacy single `host:` field into `hosts:`. This file also owns the `smokeping_prober_config_last_reload_*` metrics.
- `main.go`: flags (kingpin), `smokePingers` lifecycle, the reload loop, and HTTP handlers (`/metrics`, `/-/healthy`, `POST /-/reload`, landing page via exporter-toolkit).
- `collector.go`: the metric vars and `SmokepingCollector`.

### Targets come from two sources

`smokePingers.prepare()` builds one `probe` (a `*probing.Pinger` from pro-bing, plus extra labels) per host. It does this for the CLI positional hosts, which use the global `--ping.*`/`--privileged` flags, and for every host in each config `TargetGroup`, which uses that group's settings. Config-group ICMP always forces privileged mode. Both sources run side by side.

### Pinger lifecycle is two-phase

`prepare()` fills `prepared`, and `start()` swaps it into `started`. When pingers are already running, `start()` stops them first, then launches each `pinger.Run()` in an `errgroup`. It sleeps `maxInterval / n` between launches to spread out send times. `prepare()` holds `sc.Lock()` while it reads the config.

### Reload rebuilds everything

SIGHUP and `POST /-/reload` both feed one goroutine in `main()`. On every reload it re-reads the YAML, recomputes the label set with `buildLabelNamesFromConfig()`, unregisters and re-creates the `smokeping_response_duration_seconds` HistogramVec, re-prepares and restarts all pingers, and unregisters and re-creates `SmokepingCollector`. This is needed because custom per-target `labels:` can change the label *names*, and a registered metric can't change those in place.

### Metrics and labels

- The label set is always `ip, host, source, tos` followed by the sorted union of all custom label keys across target groups. A target without a given key gets `""`. Custom keys that collide with a base label are dropped.
- `SmokepingCollector.updateProbes()` sets pro-bing callbacks (`OnRecv`, `OnDuplicateRecv`, `OnSendError`, `OnRecvError`) that write straight into package-level metric vecs. It pre-initializes series to zero and calls `Reset()` on each reload. `ip` comes from the received packet's address, so it reflects the address that actually replied.
- `smokeping_requests_total` is the only metric computed at scrape time (`Collect()` reads `pinger.Statistics()`).
- The response histogram is a classic histogram with `--buckets` plus a native histogram. The native part is controlled by the hidden `--native-histogram-factor` flag.
- `smokeping_receive_errors_total` has no labels. Read timeouts are deliberately left out of it.

`dashboard.json` (Grafana) and `example-rules.yml` depend on these metric and label names, so update them if you rename anything.
