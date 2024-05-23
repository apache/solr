/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.metrics.prometheus;

import java.util.regex.Pattern;

public interface PrometheusCoreExporterInfo {
  /** Category of prefix Solr Core dropwizard handler metric names */
  enum CoreCategory {
    ADMIN,
    QUERY,
    UPDATE,
    REPLICATION,
    TLOG,
    CACHE,
    SEARCHER,
    HIGHLIGHTER,
    INDEX,
    CORE
  }

  String PROMETHEUS_CLOUD_CORE_REGEX = "^core_(.*)_(shard[0-9]+)_(replica_n[0-9]+)$";
  Pattern PROMETHEUS_CLOUD_CORE_PATTERN = Pattern.compile(PROMETHEUS_CLOUD_CORE_REGEX);
}
