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
  public static final AttributeKey<String> HANDLER_ATTR = AttributeKey.stringKey("handler");
  public static final AttributeKey<String> OPERATION_ATTR = AttributeKey.stringKey("ops");
  public static final AttributeKey<String> RESULT_ATTR = AttributeKey.stringKey("result");
  public static final AttributeKey<String> NAME_ATTR = AttributeKey.stringKey("name");
  public static final AttributeKey<String> PLUGIN_NAME_ATTR = AttributeKey.stringKey("plugin_name");

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
   * Implementation should initialize all metrics to a {@link SolrMetricsContext}
   * Registry/MeterProvider with {@link Attributes} as the common set of attributes that will be
   * attached to every metric that is initialized for that class/component
   *
   * @param parentContext The registry that the component will initialize metrics to
   * @param attributes Base set of attributes that will be bound to all metrics for that component
   */
  void initializeMetrics(SolrMetricsContext parentContext, Attributes attributes);

  /**
   * Implementations should return the context used in {@link #initializeMetrics(SolrMetricsContext,
   * Attributes)} to ensure proper cleanup of metrics at the end of the life-cycle of this
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
    if (context != null) {
      context.unregister();
    }
  }
}
