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
import org.apache.solr.util.RefCounted;
import org.glassfish.jersey.server.ApplicationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO There's gotta be a generic Collection implementation that does reference-counting
// TODO If I can't find an off-the-shelf replacement of this, revisit thread safety on refcount
// modification
public class JerseyAppHandlerCache {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<String, RefCounted<ApplicationHandler>> applicationByConfigSetId;

  public JerseyAppHandlerCache() {
    this.applicationByConfigSetId = new ConcurrentHashMap<>();
  }

  public RefCounted<ApplicationHandler> computeIfAbsent(
      String effectiveConfigSetId, Supplier<ApplicationHandler> createApplicationHandler) {
    final Function<String, RefCounted<ApplicationHandler>> wrapper =
        s -> {
          return new RefCounted<>(createApplicationHandler.get()) {
            @Override
            public void close() {
              log.info("JEGERLOW: Removing AppHandler from cache for ID {}", effectiveConfigSetId);
              applicationByConfigSetId.remove(effectiveConfigSetId);
            }
          };
        };

    final RefCounted<ApplicationHandler> fetched =
        applicationByConfigSetId.computeIfAbsent(effectiveConfigSetId, wrapper);
    fetched.incref();
    return fetched;
  }

  public static String generateIdForConfigSet(ConfigSet configSet) {
    return configSet.getName() + "-" + configSet.getSolrConfig().hashCode();
  }
}
