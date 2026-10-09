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
package org.apache.solr.opentelemetry;

import io.opentelemetry.sdk.testing.junit4.OpenTelemetryRule;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.solr.common.util.RetryUtil;
import org.junit.Assert;

/** Helpers for tests that collect spans via an {@link OpenTelemetryRule}. */
final class TracingTestUtil {

  private TracingTestUtil() {}

  static List<SpanData> getAndClearSpans(OpenTelemetryRule otelRule) {
    return getAndClearSpans(otelRule, 0);
  }

  /**
   * Waits for at least {@code minExpected} spans, then returns and clears the collected spans, most
   * recently finished first.
   */
  static List<SpanData> getAndClearSpans(OpenTelemetryRule otelRule, int minExpected) {
    try {
      RetryUtil.retryUntil(
          "Timed out waiting for " + minExpected + " span(s)",
          250,
          20,
          TimeUnit.MILLISECONDS,
          () -> otelRule.getSpans().size() >= minExpected);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    List<SpanData> result = new ArrayList<>(otelRule.getSpans());
    Collections.reverse(result); // nicer to see spans chronologically
    otelRule.clearSpans();
    return result;
  }

  static boolean isRootSpan(SpanData span) {
    return !span.getParentSpanContext().isValid();
  }

  static String getRootTraceId(List<SpanData> finishedSpans) {
    Assert.assertEquals(1, finishedSpans.stream().filter(TracingTestUtil::isRootSpan).count());
    return finishedSpans.stream()
        .filter(TracingTestUtil::isRootSpan)
        .findFirst()
        .orElseThrow()
        .getSpanContext()
        .getTraceId();
  }
}
