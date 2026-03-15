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
package org.apache.solr.update;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.update.processor.DistributingUpdateProcessorFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class UpdateLogReplayTracingTest extends SolrTestCaseJ4 {
  private static final String FROM_LEADER = DistributedUpdateProcessor.DistribPhase.FROMLEADER.toString();
  private static InMemorySpanExporter spanExporter;
  private static OpenTelemetrySdk openTelemetrySdk;
  private static UpdateLog ulog;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-tlog.xml", "schema-inplace-updates.xml");

    GlobalOpenTelemetry.resetForTest();
    spanExporter = InMemorySpanExporter.create();
    SdkTracerProvider tracerProvider =
        SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
            .build();
    openTelemetrySdk = OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build();
    GlobalOpenTelemetry.set(openTelemetrySdk);

    try (var req = req()) {
      UpdateHandler updateHandler = req.getCore().getUpdateHandler();
      ulog = updateHandler.getUpdateLog();
      assertNotNull("UpdateLog must be enabled for replay tracing tests", ulog);
    }
  }

  @AfterClass
  public static void afterClass() {
    ulog = null;
    if (openTelemetrySdk != null) {
      openTelemetrySdk.close();
      openTelemetrySdk = null;
    }
    if (spanExporter != null) {
      spanExporter.close();
      spanExporter = null;
    }
    GlobalOpenTelemetry.resetForTest();
  }

  @Test
  public void testReplayEmitsParentAndPerLogSpans() throws Exception {
    Span sanitySpan = GlobalOpenTelemetry.getTracer("solr").spanBuilder("sanity.span").startSpan();
    sanitySpan.end();
    assertTrue("Expected test tracer to record spans", spanExporter.getFinishedSpanItems().size() > 0);

    SolrParams replayParams =
        params(
            DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM,
            FROM_LEADER,
            "distrib.from",
            "http://127.0.0.1:8983/solr/test",
            "distrib.inplace.update",
            "false");

    clearIndex();
    ulog.bufferUpdates();
    assertNotNull(addAndGetVersion(sdoc("id", "1", "title_s", "doc-1"), withVersion(replayParams, 1001L)));
    assertNotNull(addAndGetVersion(sdoc("id", "2", "title_s", "doc-2"), withVersion(replayParams, 1002L)));

    spanExporter.reset();
    Future<UpdateLog.RecoveryInfo> replayFuture = ulog.applyBufferedUpdates();
    assertNotNull("Expected buffered replay future", replayFuture);
    UpdateLog.RecoveryInfo recoveryInfo = replayFuture.get(30, TimeUnit.SECONDS);
    assertTrue("Expected replay to process buffered updates", recoveryInfo.adds > 0);

    List<SpanData> spans = new ArrayList<>(spanExporter.getFinishedSpanItems());
    SpanData replaySpan = findSpan(spans, "updatelog.replay");
    SpanData replayLogSpan = findSpan(spans, "updatelog.replay.log");

    assertNotNull("Expected parent replay span. span names=" + spanNames(spans), replaySpan);
    assertNotNull("Expected per-log replay span. span names=" + spanNames(spans), replayLogSpan);
    assertEquals(replaySpan.getSpanContext().getTraceId(), replayLogSpan.getSpanContext().getTraceId());
    assertEquals(replaySpan.getSpanContext().getSpanId(), replayLogSpan.getParentSpanContext().getSpanId());

    assertEquals(
        h.getCore().getName(),
        replaySpan.getAttributes().get(AttributeKey.stringKey("updatelog.replay.core")));
    assertEquals(
        Long.valueOf(1L),
        replaySpan.getAttributes().get(AttributeKey.longKey("updatelog.replay.logs_replayed")));
    assertEquals(
        Long.valueOf(2L),
        replayLogSpan.getAttributes().get(AttributeKey.longKey("updatelog.replay.log_ops")));
  }

  private SolrParams withVersion(SolrParams base, long version) {
    return params(base, "_version_", Long.toString(version));
  }

  private SpanData findSpan(List<SpanData> spans, String spanName) {
    for (SpanData span : spans) {
      if (spanName.equals(span.getName())) {
        return span;
      }
    }
    return null;
  }

  private String spanNames(List<SpanData> spans) {
    List<String> names = new ArrayList<>(spans.size());
    for (SpanData span : spans) {
      names.add(span.getName());
    }
    return names.toString();
  }
}
