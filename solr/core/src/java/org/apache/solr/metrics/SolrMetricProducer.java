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
package org.apache.solr.metrics;

import io.opentelemetry.api.common.Attributes;
import java.io.IOException;

/** Used by objects that expose metrics through {@link SolrMetricManager}. */
public interface SolrMetricProducer extends AutoCloseable {

  /**
   * Unique metric tag identifies components with the same life-cycle, which should be registered /
   * unregistered together. It is in the format of A:B:C, where A is the parent of B is the parent
   * of C and so on. If object "B" is unregistered C also must get unregistered. If object "A" is
   * unregistered B and C also must get unregistered.
   *
   * @param o object to create a tag for
   * @param parentName parent object name, or null if no parent exists
   */
  static String getUniqueMetricTag(Object o, String parentName) {
    String name = o.getClass().getSimpleName() + "@" + Integer.toHexString(o.hashCode());
    if (parentName != null && parentName.contains(name)) {
      throw new RuntimeException(
          "Parent already includes this component! parent=" + parentName + ", this=" + name);
    }
    return parentName == null ? name : parentName + ":" + name;
  }

  /**
   * Legacy entry point. By default, convert the single String scope into a one‐entry Attributes map
   * and delegate to the Attributes version. TODO This will be deprecated for Attributes instead of
   * scope
   */
  default void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    // If someone wants only the String‐based signature, they override this method.
    // By default, we turn it into an Attributes map with a single key “scope”.
    Attributes attrs = Attributes.builder().put("scope", scope).build();
    initializeMetrics(parentContext, attrs);
  }

  /**
   * New preferred entry point, taking a full set of Attributes. Implement this if you want to
   * receive an Attributes object. Otherwise the default is a no‐op (metrics won’t be registered).
   */
  default void initializeMetrics(SolrMetricsContext parentContext, Attributes attributes) {
    // By default, do nothing. Implementors override this if they only need Attributes.
    // If someone overrode the String‐version, it already delegated here.
  }

  /**
   * Implementations should return the context used in {@link #initializeMetrics(SolrMetricsContext,
   * String)} to ensure proper cleanup of metrics at the end of the life-cycle of this component.
   * This should be the child context if one was created, or null if the parent context was used.
   */
  SolrMetricsContext getSolrMetricsContext();

  /**
   * Implementations should always call <code>SolrMetricProducer.super.close()</code> to ensure that
   * metrics with the same life-cycle as this component are properly unregistered. This prevents
   * obscure memory leaks.
   *
   * <p>from: https://docs.oracle.com/javase/8/docs/api/java/lang/AutoCloseable.html While this
   * interface method is declared to throw Exception, implementers are strongly encouraged to
   * declare concrete implementations of the close method to throw more specific exceptions, or to
   * throw no exception at all if the close operation cannot fail.
   */
  @Override
  default void close() throws IOException {
    SolrMetricsContext context = getSolrMetricsContext();
    if (context == null) {
      return;
    } else {
      context.unregister();
    }
  }
}
