/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.jersey;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.solr.core.ConfigSet;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.RefCounted;
import org.glassfish.jersey.server.ApplicationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores Jersey 'ApplicationHandler' instances by an ID or hash derived from their {@link
 * ConfigSet}.
 *
 * <p>ApplicationHandler creation is expensive; caching these objects allows them to be shared by
 * multiple cores with the same configuration.
 */
public class JerseyAppHandlerCache {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<String, RefCounted<ApplicationHandler>> applicationByConfigSetId;

  public JerseyAppHandlerCache() {
    this.applicationByConfigSetId = new ConcurrentHashMap<>();
  }

  /**
   * Return the 'ApplicationHandler' associated with the provided ID, creating it first if
   * necessary.
   *
   * <p>This method is thread-safe by virtue of its delegation to {@link
   * ConcurrentHashMap#computeIfAbsent(Object, Function)} internally.
   *
   * @param effectiveConfigSetId an ID to associate the ApplicationHandler with. Usually created via
   *     {@link #generateIdForConfigSet(ConfigSet)}.
   * @param createApplicationHandler a Supplier producing an ApplicationHandler
   */
  public RefCounted<ApplicationHandler> computeIfAbsent(
      String effectiveConfigSetId, Supplier<ApplicationHandler> createApplicationHandler) {
    final Function<String, RefCounted<ApplicationHandler>> wrapper =
        s -> {
          return new RefCounted<>(createApplicationHandler.get()) {
            @Override
            public void close() {
              log.info(
                  "Removing AppHandler from cache for 'effective configset' [{}]",
                  effectiveConfigSetId);
              applicationByConfigSetId.remove(effectiveConfigSetId);
            }
          };
        };

    final RefCounted<ApplicationHandler> fetched =
        applicationByConfigSetId.computeIfAbsent(effectiveConfigSetId, wrapper);
    fetched.incref();
    return fetched;
  }

  /**
   * Generates a String ID to represent the provided {@link ConfigSet}
   *
   * <p>Relies on {@link SolrConfig#hashCode()} to generate a different ID for each "unique"
   * configset (where "uniqueness" considers various overlays that get applied to the {@link
   * ConfigSet})
   *
   * @see SolrCore#hashCode()
   */
  public static String generateIdForConfigSet(ConfigSet configSet) {
    return configSet.getName() + "-" + configSet.getSolrConfig().hashCode();
  }
}
