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

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.io.IOException;

/** Used by objects that expose metrics through {@link SolrMetricManager}. */
public interface SolrMetricProducer extends AutoCloseable {

  public static final AttributeKey<String> TYPE_ATTR = AttributeKey.stringKey("type");
  public static final AttributeKey<String> CATEGORY_ATTR = AttributeKey.stringKey("category");
  public static final AttributeKey<String> OPERATION_ATTR = AttributeKey.stringKey("ops");

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
   * NOCOMMIT SOLR-17458: The Scope parameter will be removed with Dropwizard
   *
   * <p>{@link Attributes} passed is the base or common set of attributes that should be attached to
   * every metric that will be initialized
   */
  void initializeMetrics(SolrMetricsContext parentContext, Attributes attributes, String scope);

  /**
   * Implementations should return the context used in {@link #initializeMetrics(SolrMetricsContext,
   * Attributes, String)} to ensure proper cleanup of metrics at the end of the life-cycle of this
   * component. This should be the child context if one was created, or null if the parent context
   * was used.
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
