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

Welcome to Apache Solr Prometheus Exporter
========

Apache Solr Prometheus Exporter (solr-exporter) provides a way for you to expose metrics for Solr to Prometheus.

# Getting Started With Solr Prometheus Exporter

For information on how to get started with solr-exporter please see:
 * [Solr Reference Guide's section on Monitoring Solr with Prometheus and Grafana](https://solr.apache.org/guide/solr/latest/deployment-guide/monitoring-with-prometheus-and-grafana.html)

# Docker

The Solr Prometheus Exporter can be run via the official or local Solr docker image.
Please refer to the `docker` directory's `README.md` for information on building the image
and the [Solr Reference Guide](https://solr.apache.org/guide/solr/latest/deployment-guide/solr-in-docker.html) for information on using the image.

The `solr-exporter` script is available on the path by default in the Docker image, so the Prometheus Exporter can be run using:

```bash
docker run <image> solr-exporter
```

The environment variables and command line arguments that the Prometheus Exporter accepts can be used the same way in the Docker image.
