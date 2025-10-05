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

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for ExtractionBackend instances. Lazily constructs backends by short name (e.g., "local",
 * "tikaserver") and caches them for reuse.
 */
public class ExtractionBackendFactory implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final SolrCore core;
  private final String tikaConfigLoc;
  private final ParseContextConfig parseContextConfig;
  private final String tikaServerUrl;
  private final Map<String, ExtractionBackend> cache = new ConcurrentHashMap<>();

  public ExtractionBackendFactory(
      SolrCore core,
      String tikaConfigLoc,
      ParseContextConfig parseContextConfig,
      String tikaServerUrl) {
    this.core = core;
    this.tikaConfigLoc = tikaConfigLoc;
    this.parseContextConfig = parseContextConfig;
    this.tikaServerUrl = tikaServerUrl;
  }

  /** Returns a backend instance for the given name, creating it if necessary. */
  public ExtractionBackend getBackend(String name) {
    return cache.computeIfAbsent(
        name,
        k -> {
          try {
            return create(k);
          } catch (Exception e) {
            throw new SolrException(
                SolrException.ErrorCode.SERVER_ERROR,
                "Failed to create extraction backend '" + k + "'",
                e);
          }
        });
  }

  /** Creates a new backend instance */
  protected ExtractionBackend create(String name) throws Exception {
    return switch (name) {
      case TikaServerExtractionBackend.NAME -> new TikaServerExtractionBackend(
          tikaServerUrl != null ? tikaServerUrl : "http://localhost:9998");
      case LocalTikaExtractionBackend.NAME -> new LocalTikaExtractionBackend(
          core, tikaConfigLoc, parseContextConfig);
      default -> throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Unknown extraction backend: " + name);
    };
  }

  @Override
  public void close() throws IOException {
    for (Map.Entry<String, ExtractionBackend> entry : cache.entrySet()) {
      log.info("Closing backend {}", entry.getKey());
      entry.getValue().close();
    }
  }
}
