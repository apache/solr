<!--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 -->
# Solr Monitoring

This directory provides two ready-to-use monitoring artifacts for Solr that you can drop into
your existing Prometheus + Grafana installation:

| File | Description |
|---|---|
| **`grafana-solr-dashboard.json`** | Grafana dashboard — import directly into Grafana |
| **`prometheus-solr-alerts.yml`** | Prometheus alert rules — reference from your `prometheus.yml` |

These are the **main artifacts**. Everything else in this directory supports their creation
or testing.

| File / Directory | Description |
|---|---|
| `mixin/` | Jsonnet source (single source of truth used to regenerate the artifacts above) |
| `dev/` | **Developer convenience only** — docker-compose stack for testing changes to the artifacts |

> **The dashboard is still a placeholder**, enough to exercise the build pipeline. The real one
> arrives in [SOLR-18534](https://issues.apache.org/jira/browse/SOLR-18534) and OTLP push support
> in [SOLR-18536](https://issues.apache.org/jira/browse/SOLR-18536). The alert rules are complete.

For the full integration guide see the Solr Reference Guide:
**[Monitoring Solr with Prometheus and Grafana](../solr-ref-guide/modules/deployment-guide/pages/monitoring-with-prometheus-and-grafana.adoc)**

---

## Importing the Dashboard

1. Open Grafana → **Dashboards → Import**
2. Upload `grafana-solr-dashboard.json` or paste its contents
3. Select your Prometheus datasource when prompted
4. Click **Import**

The placeholder dashboard has two panels, Request Rate and Heap Used, and two dropdowns:
`datasource` and `interval`.

---

## Loading Alert Rules into Prometheus

Add the alert rules file to your Prometheus configuration:

```yaml
# prometheus.yml
rule_files:
  - /path/to/prometheus-solr-alerts.yml
```

Validate locally before deploying:

```bash
promtool check rules prometheus-solr-alerts.yml
```

The rules live in the group `SolrAlerts`. Every expression carries `solrSelector` and aggregates
`by (instance)`, so a rule alerts and recovers per node:

| Alert | Severity | Fires when | Setting |
|---|---|---|---|
| `SolrHighHeapUsage` | critical | `jvm_memory_used_bytes` / `jvm_memory_committed_bytes` (heap, summed over pools) above 90% for 2m | `heapUsageThreshold` |
| `SolrJvmGcThrashing` | critical | `rate(jvm_gc_duration_seconds_sum)` over 10 s/min for 3m | `gcSecondsPerMinuteThreshold` |
| `SolrLowDiskSpace` | critical | `solr_disk_space_megabytes` usable/total under 15% for 5m | `diskFreeThreshold` |
| `SolrNodeDown` | warning | `up == 0` for 2m | — |
| `SolrHighSearchLatency` | warning | p99 of `solr_core_requests_times_milliseconds` for `/select*`, `internal="false"`, above 1000 ms for 5m | `searchLatencyThresholdMs` |
| `SolrHighErrorRate` | warning | `solr_core_requests_errors_total` / `solr_core_requests_total` over 1% for 5m | `errorRateThreshold` |
| `SolrOverseerQueueBuildup` | warning | `solr_overseer_collection_work_queue_size` over 50 for 5m (SolrCloud only) | `overseerQueueThreshold` |
| `SolrHighMmapRatio` | warning | Index size against `jvm_system_memory_bytes` minus committed heap, under 10% for 5m | `mmapRatioThreshold` |

`rateInterval` sets the window for the `rate()` queries in the GC, latency and error-rate rules.

Two details the expressions depend on. Heap is measured against committed rather than
`jvm_memory_limit_bytes`, because generational ZGC reports the whole `-Xmx` as the limit of both its
young and old pool; committed is a partition of one heap under every collector, and `bin/solr` pins
`-Xms` to `-Xmx`. The request counters and the latency histogram carry `core!=""`, because a handler
with `aggregateNodeLevelMetricsEnabled` emits a node rollup under the same metric name, without the
`core`, `collection` and `shard` attributes.

---

## Customizing

All tunable settings live in `mixin/config.libsonnet`:

```jsonnet
{
  _config:: {
    solrSelector: 'job="solr"',   // label selector identifying Solr targets
    rateInterval: '5m',           // window for rate() queries
    // ... one threshold per alert rule, see the table above
  },
}
```

`solrSelector` must match your Prometheus scrape job, or no panel or rule will match a series.
Edit the file and regenerate, or patch `_config` from your own jsonnet without touching this tree:

```jsonnet
(import 'mixin.libsonnet') + { _config+:: { solrSelector: 'job="my-solr"' } }
```

Every key in `config.libsonnet` is consumed by the dashboard or the alert rules; the file grows as
they do.

---

## Mixin Build

Only contributors who need to **regenerate** the artifacts need build tooling.
Operators can use the pre-generated files directly.

There are two equivalent ways to build:

### Option A — Docker (no local tool installation required)

```bash
cd mixin
./make.sh install   # fetch grafonnet dependency into vendor/
./make.sh all       # regenerate both artifacts
./make.sh check     # validate alert rules
```

`make.sh` automatically builds the `solr-mixin-make:latest` Docker image on first run, with all
tool versions pinned in `mixin/Dockerfile`. Pass `--rebuild` to force a fresh image build
(e.g. after a Dockerfile update).

### Option B — Local tools

Install the required tools once:

| Tool | Purpose | Install |
|---|---|---|
| `jsonnet` (go-jsonnet) | Evaluate jsonnet source | `brew install go-jsonnet` or `go install github.com/google/go-jsonnet/cmd/jsonnet@latest` |
| `jb` (jsonnet-bundler) | Manage dependencies | `brew install jsonnet-bundler` or `go install github.com/jsonnet-bundler/jsonnet-bundler/cmd/jb@latest` |
| `gojsontoyaml` | Convert JSON→YAML for alert rules | `go install github.com/brancz/gojsontoyaml@latest` |
| `promtool` *(optional)* | Validate and unit-test alert rules | `brew install prometheus` |

Then build:

```bash
cd mixin
make install      # fetch grafonnet dependency into vendor/
make all          # regenerate both artifacts
```

A target aborts before touching any output if a tool is missing, and writes through a temp file,
so a failed build cannot leave a half-written artifact behind.

### Makefile targets

Both `./make.sh <target>` and `make <target>` accept the same targets:

| Target | Action |
|---|---|
| `install` | Download jsonnet dependencies (grafonnet) into `vendor/` |
| `dashboards` | Regenerate `grafana-solr-dashboard.json` |
| `alerts` | Regenerate `prometheus-solr-alerts.yml` |
| `all` | Regenerate both outputs |
| `test` | Run the alert rule unit tests in `mixin/tests/` with `promtool test rules` |
| `check` | Run `promtool check rules` on alert rules, then the unit tests |
| `fmt` | Auto-format all `.libsonnet` source files |

`vendor/` is not checked in; `jsonnetfile.lock.json` pins the exact grafonnet revision, so
`make install` reproduces the same dependency tree.

**Commit the regenerated artifacts together with the jsonnet change that produced them** —
nothing in the Gradle build regenerates or verifies them.

`mixin/tests/` holds `promtool test rules` cases that feed synthetic series to the generated rules.
They pin the metric properties the expressions depend on — a ZGC heap reporting `-Xmx` as the limit
of two pools, a ParallelGC heap partitioned across three, a node rollup sharing a core metric name,
and the MiB units of the index-size gauge — so run `make test` after editing `alerts/`.

---

## Developer Convenience Stack

The `dev/` directory contains a docker-compose stack that starts a local Solr + Prometheus
+ Grafana + Alertmanager environment. **This is solely for contributors who want to
visually test changes to the two artifacts above.** It is not a reference deployment
or a recommended production setup, and is excluded from the source release.

```bash
cd dev
./stack.sh                  # bundled Solr nodes + monitoring + traffic generator
./stack.sh --local-solr     # monitoring only, scraping a Solr you already run on :8983
./stack.sh --down           # stop and remove everything
```

Use `stack.sh` rather than `docker compose up` directly: the bundled Solr nodes sit behind the
`bundled-solr` compose profile, so a plain `docker compose up` would start the monitoring services
with no Solr to scrape.

Services: Solr (`:8983`, `:8984`), Prometheus (`:9090`), Grafana (`:3000`), Alertmanager
(`:9093`). Grafana opens straight on the provisioned dashboard with no login — anonymous viewing
is enabled because this stack is local and throwaway; sign in as `admin`/`admin` to edit. The
Prometheus datasource is provisioned too, and Prometheus loads the alert rules. Both artifacts are bind-mounted as single
files, which the containers resolve once at start, so after regenerating one, recreate them:

```bash
docker compose up -d --force-recreate grafana prometheus
```

The monitoring images are pinned for reproducibility. Override `SOLR_IMAGE`, `PROMETHEUS_IMAGE`,
`GRAFANA_IMAGE` or `ALERTMANAGER_IMAGE` to test against other versions:

```bash
GRAFANA_IMAGE=grafana/grafana:latest ./stack.sh
```

`trafficgen/` is a small Python container that indexes and queries continuously, so the panels
show movement. It exists for this stack only.
