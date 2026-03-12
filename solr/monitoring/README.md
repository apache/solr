# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Solr 10.x Monitoring Artifacts

This directory provides two ready-to-use monitoring artifacts for Solr 10.x that you can
drop into your existing Prometheus + Grafana installation:

| File | Description |
|---|---|
| **`grafana-solr-dashboard.json`** | Grafana dashboard — import directly into Grafana |
| **`prometheus-solr-alerts.yml`** | Prometheus alert rules — reference from your `prometheus.yml` |

These are the **main artifacts**. Everything else in this directory supports their creation
or testing.

| File / Directory | Description |
|---|---|
| `otel-collector-solr.yml` | OTel Collector config snippet for the OTLP push path |
| `mixin/` | Jsonnet source (single source of truth used to regenerate the artifacts above) |
| `dev/` | **Developer convenience only** — docker-compose stack for testing changes to the artifacts |

For the full integration guide see the Solr Reference Guide:
**xref:monitoring-with-prometheus-and-grafana.adoc** (deployment-guide module)

---

## Importing the Dashboard

1. Open Grafana → **Dashboards → Import**
2. Upload `grafana-solr-dashboard.json` or paste its contents
3. Select your Prometheus datasource when prompted
4. Click **Import**

The dashboard has 5 sections:
- **Node Overview** — query/indexing rates, search latency, active cores, disk space
- **JVM** — heap usage, GC pauses and rates, threads, CPU utilization
- **SolrCloud** *(collapsed)* — Overseer queues, ZooKeeper ops, shard leaders
- **Index Health** *(collapsed)* — segment counts, index size, merge rates, MMap efficiency
- **Cache Efficiency** *(collapsed)* — filter/query/document cache hit rates and evictions

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

Seven alert rules are included (3 critical, 4 warning). See the reference guide for details.

---

## Prometheus Scrape Configuration

Configure Prometheus to scrape Solr's metrics endpoint:

```yaml
scrape_configs:
  - job_name: solr
    metrics_path: /api/metrics
    static_configs:
      - targets: ['solr-host:8983']
    relabel_configs:
      - target_label: environment
        replacement: prod          # change per environment: dev, staging, prod
      - target_label: cluster
        replacement: cluster-1     # unique name for this Solr cluster
```

---

## Adding `environment` and `cluster` Labels

The dashboard includes `environment` and `cluster` dropdown variables that let you
scope panels to a specific deployment environment (dev/staging/prod) and SolrCloud cluster.
These labels are **operator-supplied** — Solr does not emit them natively.

If labels are absent, both dropdowns default to "All" and panels match all series —
the dashboard works correctly without these labels.

### OTel Collector path

Map OTel resource attributes in `otel-collector-solr.yml`:

```yaml
processors:
  transform/promote_resource_attrs:
    metric_statements:
      - context: datapoint
        statements:
          - set(attributes["environment"], resource.attributes["deployment.environment"])
            where resource.attributes["deployment.environment"] != nil
          - set(attributes["cluster"], resource.attributes["service.namespace"])
            where resource.attributes["service.namespace"] != nil
```

---

## Customizing Label Names

If your organization uses different Prometheus label names (e.g., `deployment_environment`
instead of `environment`), edit `mixin/config.libsonnet`:

```jsonnet
{
  _config:: {
    environmentLabel: 'deployment_environment',  // change to match your label
    clusterLabel: 'service_namespace',           // change to match your label
    // ... other thresholds ...
  },
}
```

Then regenerate the dashboard:

```bash
cd mixin
make all
```

---

## Customizing Alert Thresholds

All thresholds are in `mixin/config.libsonnet`. For example, to lower the heap alert
threshold from 90% to 85%:

```jsonnet
{
  _config:: {
    heapUsageThreshold: 0.85,   // default: 0.9
  },
}
```

Regenerate after editing:

```bash
cd mixin
make all          # regenerates grafana-solr-dashboard.json and prometheus-solr-alerts.yml
make check        # validates alert rules with promtool (optional)
```

---

## Mixin Build

Only contributors who need to **regenerate** the dashboard or alert rules need build tooling.
Operators can use the pre-generated files directly.

There are two equivalent ways to build:

### Option A — Docker (no local tool installation required)

```bash
cd mixin
./make.sh install   # fetch grafonnet dependency into vendor/
./make.sh all       # regenerate both artifacts
./make.sh check     # validate alert rules
```

`make.sh` automatically builds the `solr-mixin-make:latest` Docker image on first run.
Pass `--rebuild` to force a fresh image build (e.g. after a Dockerfile update):

```bash
./make.sh --rebuild all
```

### Option B — Local tools

Install the required tools once:

| Tool | Purpose | Install |
|---|---|---|
| `jsonnet` (go-jsonnet) | Evaluate jsonnet source | `brew install go-jsonnet` or `go install github.com/google/go-jsonnet/cmd/jsonnet@latest` |
| `jb` (jsonnet-bundler) | Manage dependencies | `brew install jsonnet-bundler` or `go install github.com/jsonnet-bundler/jsonnet-bundler/cmd/jb@latest` |
| `gojsontoyaml` | Convert JSON→YAML for alert rules | `go install github.com/brancz/gojsontoyaml@latest` |
| `promtool` *(optional)* | Validate alert rules | `brew install prometheus` or bundled with Prometheus binary download |

Then build:

```bash
cd mixin
make install      # fetch grafonnet dependency into vendor/
make all          # regenerate both artifacts
```

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

---

## Developer Convenience Stack

The `dev/` directory contains a docker-compose stack that starts a local Solr + Prometheus
+ Grafana + Alertmanager environment. **This is solely for contributors who want to
visually test changes to the two artifacts above.** It is not a reference deployment
or a recommended production setup.

```bash
cd dev
docker-compose up -d
```

Services: Solr (`:8983`), Prometheus (`:9090`), Grafana (`:3000`, admin/solr), Alertmanager (`:9093`)

---

## Grafana Marketplace (Post-Merge)

After merging to the Solr main branch, a committer can publish the dashboard to the
Grafana marketplace at https://grafana.com/grafana/dashboards/ for wider discoverability.
Steps:
1. Log in to grafana.com with a Solr/Apache account
2. Upload `grafana-solr-dashboard.json`
3. Set tags: `solr`, `prometheus`, `solr10`
4. Reference the dashboard ID in this README for easy import via dashboard ID lookup
