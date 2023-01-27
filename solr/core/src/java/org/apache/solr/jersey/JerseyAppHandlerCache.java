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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.solr.core.ConfigSet;
import org.apache.solr.core.SolrConfig;
import org.glassfish.jersey.server.ApplicationHandler;

/**
 * Stores Jersey 'ApplicationHandler' instances by an ID or hash derived from their {@link
 * ConfigSet}.
 *
 * <p>ApplicationHandler creation is expensive; caching these objects allows them to be shared by
 * multiple cores with the same configuration.
 */
public class JerseyAppHandlerCache {

  private final Cache<String, ApplicationHandler> applicationByConfigSetId =
      Caffeine.newBuilder().weakValues().build();

  /**
   * Return the 'ApplicationHandler' associated with the provided ID, creating it first if
   * necessary.
   *
   * <p>This method is thread-safe by virtue of its delegation to {@link Cache#get(Object,
   * Function)} internally.
   *
   * @param effectiveSolrConfigId an ID to associate the ApplicationHandler with. Usually created
   *     via {@link SolrConfig#effectiveId()}.
   * @param createApplicationHandler a Supplier producing an ApplicationHandler
   */
  public ApplicationHandler computeIfAbsent(
      String effectiveSolrConfigId, Supplier<ApplicationHandler> createApplicationHandler) {
    return applicationByConfigSetId.get(effectiveSolrConfigId, k -> createApplicationHandler.get());
  }

  public int size() {
    return applicationByConfigSetId.asMap().size();
  }
}
