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
| **`otel-collector-config.yaml`** | Example OpenTelemetry Collector config — receives Solr's OTLP push and exposes the metrics in Prometheus format under the same metric names as `/solr/admin/metrics`; a starting point to adapt, not a production setup |

These are the **main artifacts**. Everything else in this directory supports their creation
or testing.

| File / Directory | Description |
|---|---|
| `mixin/` | Jsonnet source (single source of truth used to regenerate the artifacts above) |
| `dev/` | **Developer convenience only** — docker-compose stack for testing changes to the artifacts |

> **The dashboard and alert rules are placeholders right now** — a two-panel dashboard and a
> single alert, which exist to prove the build pipeline works end to end. The real dashboard
> arrives in [SOLR-18534](https://issues.apache.org/jira/browse/SOLR-18534) and the alert rules
> in [SOLR-18535](https://issues.apache.org/jira/browse/SOLR-18535).

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

One placeholder rule is included: `SolrNodeDown`, which fires when a Solr target has failed to
scrape for 2 minutes.

---

## Scraping via the OpenTelemetry Collector

Instead of scraping every Solr node, let the nodes push metrics over OTLP to a Collector and
scrape the Collector on `:9464` with `honor_labels: true`. `otel-collector-config.yaml` is an
example to start from (needs the `otel/opentelemetry-collector-contrib` distribution): its
`transform` processor keeps the metric names identical to a direct scrape (the Collector would
otherwise append `_ratio` to dimensionless metrics), and it also forwards the raw stream to an
Aspire Dashboard, which you can drop. Enable the push on each node with the `opentelemetry`
module and `solr.metrics.otlpExporterEnabled=true`; see the Reference Guide page linked above.

---

## Customizing

All tunable settings live in `mixin/config.libsonnet`:

```jsonnet
{
  _config:: {
    solrSelector: 'job="solr"',   // label selector identifying Solr targets
    rateInterval: '5m',           // window for rate() queries
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
| `promtool` *(optional)* | Validate alert rules | `brew install prometheus` |

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
| `check` | Run `promtool check rules` on alert rules |
| `fmt` | Auto-format all `.libsonnet` source files |

`vendor/` is not checked in; `jsonnetfile.lock.json` pins the exact grafonnet revision, so
`make install` reproduces the same dependency tree.

**Commit the regenerated artifacts together with the jsonnet change that produced them** —
nothing in the Gradle build regenerates or verifies them.

---

## Developer Convenience Stack

The `dev/` directory contains a docker-compose stack that starts a local Solr + Prometheus
+ Grafana + Alertmanager + OpenTelemetry Collector + Aspire Dashboard environment. **This is solely for
contributors who want to visually test changes to the artifacts above.** It is not a reference
deployment or a recommended production setup, and is excluded from the source release.

```bash
cd dev
./stack.sh                        # bundled Solr nodes + monitoring + traffic generator
./stack.sh --local-solr           # monitoring only, scraping a Solr you already run on :8983
./stack.sh --metrics-scrape=otel  # Prometheus scrapes the Collector the Solr nodes push to
./stack.sh --down                 # stop and remove everything
```

The bundled Solr nodes always push metrics and traces to the Collector, which forwards both to
Aspire (`:18888`) for a raw view; `--metrics-scrape` (`solr`, the default, or `otel`) only decides
what Prometheus scrapes, and the job is named `solr` either way.
Put `METRICS_SCRAPE=otel` in a `dev/.env` file (gitignored) to change the default.
`./check-otel-parity.sh` diffs the metric names, types and label keys of a Solr node against the
Collector's output, which is how the shipped Collector config is validated.

Use `stack.sh` rather than `docker compose up` directly: the bundled Solr nodes sit behind the
`bundled-solr` compose profile, so a plain `docker compose up` would start the monitoring services
with no Solr to scrape.

Services: Solr (`:8983`, `:8984`), Prometheus (`:9090`), Grafana (`:3000`), Alertmanager
(`:9093`), OpenTelemetry Collector (OTLP in on `:4318`, Prometheus format out on `:9464`),
Aspire Dashboard (`:18888`). Grafana opens straight on the provisioned dashboard with no login — anonymous viewing
is enabled because this stack is local and throwaway; sign in as `admin`/`admin` to edit. The
Prometheus datasource is provisioned too, and Prometheus loads the alert rules. All three artifacts are bind-mounted as single
files, which the containers resolve once at start, so after regenerating or editing one, recreate them:

```bash
docker compose up -d --force-recreate grafana prometheus otel-collector
```

The monitoring images are pinned for reproducibility. Override `SOLR_IMAGE`, `PROMETHEUS_IMAGE`,
`GRAFANA_IMAGE`, `ALERTMANAGER_IMAGE`, `OTEL_COLLECTOR_IMAGE` or `ASPIRE_IMAGE` to test against other
versions:

```bash
GRAFANA_IMAGE=grafana/grafana:latest ./stack.sh
```

`trafficgen/` is a small Python container that indexes and queries continuously, so the panels
show movement. It exists for this stack only.
