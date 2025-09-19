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
package org.apache.solr.handler.extraction;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.solr.core.SolrCore;

/**
 * Factory for ExtractionBackend instances. Lazily constructs backends by short name (e.g., "local",
 * "dummy") and caches them for reuse.
 */
public class ExtractionBackendFactory {
  private final SolrCore core;
  private final String tikaConfigLoc;
  private final ParseContextConfig parseContextConfig;
  private final String tikaServerUrl;
  private final Map<String, ExtractionBackend> cache = new ConcurrentHashMap<>();

  public ExtractionBackendFactory(
      SolrCore core, String tikaConfigLoc, ParseContextConfig parseContextConfig, String tikaServerUrl) {
    this.core = core;
    this.tikaConfigLoc = tikaConfigLoc;
    this.parseContextConfig = parseContextConfig;
    this.tikaServerUrl = tikaServerUrl;
  }

  /** Returns a backend instance for the given name, creating it if necessary. */
  public ExtractionBackend getBackend(String name) {
    String key = normalize(name);
    return cache.computeIfAbsent(
        key,
        k -> {
          try {
            return create(k);
          } catch (Exception e) {
            throw new RuntimeException("Failed to create extraction backend '" + k + "'", e);
          }
        });
  }

  private String normalize(String name) {
    if (name == null || name.trim().isEmpty()) return LocalTikaExtractionBackend.ID;
    return name.trim().toLowerCase(Locale.ROOT);
  }

  /** Creates a new backend instance for the given normalized name. */
  protected ExtractionBackend create(String normalizedName) throws Exception {
    switch (normalizedName) {
      case LocalTikaExtractionBackend.ID:
        return new LocalTikaExtractionBackend(core, tikaConfigLoc, parseContextConfig);
      case DummyExtractionBackend.ID:
        return new DummyExtractionBackend();
      case TikaServerExtractionBackend.ID:
        return new TikaServerExtractionBackend(tikaServerUrl != null ? tikaServerUrl : "http://localhost:9998");
      default:
        // Fallback to local for unknown names
        return new LocalTikaExtractionBackend(core, tikaConfigLoc, parseContextConfig);
    }
  }
}
