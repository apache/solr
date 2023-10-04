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

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.util.tracing.TraceUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDistributedTracing extends SolrCloudTestCase {

  private static final String COLLECTION = "collection1";

  @BeforeClass
  public static void setupCluster() throws Exception {
    // force early init
    CustomTestOtelTracerConfigurator.prepareForTest();

    configureCluster(4)
        .addConfig("config", TEST_PATH().resolve("collection1").resolve("conf"))
        .withSolrXml(TEST_PATH().resolve("solr.xml"))
        .withTraceIdGenerationDisabled()
        .configure();

    assertNotEquals(
        "Expecting active otel, not noop impl",
        TracerProvider.noop(),
        GlobalOpenTelemetry.get().getTracerProvider());

    CollectionAdminRequest.createCollection(COLLECTION, "config", 2, 2)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 2, 4);
  }

  @AfterClass
  public static void afterClass() {
    CustomTestOtelTracerConfigurator.resetForTest();
  }

  @Test
  public void test() throws IOException, SolrServerException {
    // TODO it would be clearer if we could compare the complete Span tree between reality
    // and what we assert it looks like in a structured visual way.

    getAndClearSpans(); // reset
    CloudSolrClient cloudClient = cluster.getSolrClient();

    // Indexing
    cloudClient.add(COLLECTION, sdoc("id", "1"));
    var finishedSpans = getAndClearSpans();
    finishedSpans.removeIf(
        span ->
            span.getAttributes().get(TraceUtils.TAG_HTTP_URL) == null
                || !span.getAttributes().get(TraceUtils.TAG_HTTP_URL).endsWith("/update"));
    assertEquals(2, finishedSpans.size());
    assertOneSpanIsChildOfAnother(finishedSpans);
    // core because cloudClient routes to core
    assertEquals("post:/{core}/update", finishedSpans.get(0).getName());
    assertCoreName(finishedSpans.get(0));

    cloudClient.add(COLLECTION, sdoc("id", "2"));
    cloudClient.add(COLLECTION, sdoc("id", "3"));
    cloudClient.add(COLLECTION, sdoc("id", "4"));
    cloudClient.commit(COLLECTION);
    getAndClearSpans();

    // Searching
    cloudClient.query(COLLECTION, new SolrQuery("*:*"));
    finishedSpans = getAndClearSpans();
    finishedSpans.removeIf(
        span ->
            span.getAttributes().get(TraceUtils.TAG_HTTP_URL) == null
                || !span.getAttributes().get(TraceUtils.TAG_HTTP_URL).endsWith("/select"));
    // one from client to server, 2 for execute query, 2 for fetching documents
    assertEquals(5, finishedSpans.size());
    var parentTraceId = getRootTraceId(finishedSpans);
    for (var span : finishedSpans) {
      if (isRootSpan(span)) {
        continue;
      }
      assertEquals(span.getParentSpanContext().getTraceId(), parentTraceId);
      assertEquals(span.getTraceId(), parentTraceId);
    }
    assertEquals("get:/{core}/select", finishedSpans.get(0).getName());
    assertCoreName(finishedSpans.get(0));
  }

  @Test
  public void testAdminApi() throws Exception {
    getAndClearSpans(); // reset
    CloudSolrClient cloudClient = cluster.getSolrClient();

    cloudClient.request(new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/metrics"));
    var finishedSpans = getAndClearSpans();
    assertEquals("get:/admin/metrics", finishedSpans.get(0).getName());

    CollectionAdminRequest.listCollections(cloudClient);
    finishedSpans = getAndClearSpans();
    assertEquals("list:/admin/collections", finishedSpans.get(0).getName());
  }

  @Test
  public void testV2Api() throws Exception {
    getAndClearSpans(); // reset
    CloudSolrClient cloudClient = cluster.getSolrClient();

    new V2Request.Builder("/collections/" + COLLECTION + "/reload")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload("{}")
        .build()
        .process(cloudClient);
    var finishedSpans = getAndClearSpans();
    assertEquals("post:/collections/{collection}/reload", finishedSpans.get(0).getName());
    assertCollectionName(finishedSpans.get(0));

    new V2Request.Builder("/c/" + COLLECTION + "/update/json")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload("{\n" + " \"id\" : \"9\"\n" + "}")
        .withParams(params("commit", "true"))
        .build()
        .process(cloudClient);
    finishedSpans = getAndClearSpans();
    assertEquals("post:/c/{collection}/update/json", finishedSpans.get(0).getName());
    assertCollectionName(finishedSpans.get(0));

    final V2Response v2Response =
        new V2Request.Builder("/c/" + COLLECTION + "/select")
            .withMethod(SolrRequest.METHOD.GET)
            .withParams(params("q", "id:9"))
            .build()
            .process(cloudClient);
    finishedSpans = getAndClearSpans();
    assertEquals("get:/c/{collection}/select", finishedSpans.get(0).getName());
    assertCollectionName(finishedSpans.get(0));
    assertEquals(1, ((SolrDocumentList) v2Response.getResponse().get("response")).getNumFound());
  }

  /**
   * Best effort test of the apache http client tracing. the test assumes the request uses the http
   * client but there is no way to enforce it, so when the api will be rewritten this test will
   * become obsolete
   */
  @Test
  public void testApacheClient() throws Exception {
    CollectionAdminRequest.ColStatus a1 = CollectionAdminRequest.collectionStatus(COLLECTION);
    CollectionAdminResponse r1 = a1.process(cluster.getSolrClient());
    assertEquals(0, r1.getStatus());
    var finishedSpans = getAndClearSpans();
    var parentTraceId = getRootTraceId(finishedSpans);
    for (var span : finishedSpans) {
      if (isRootSpan(span)) {
        continue;
      }
      assertEquals(span.getParentSpanContext().getTraceId(), parentTraceId);
      assertEquals(span.getTraceId(), parentTraceId);
    }
  }

  private static boolean isRootSpan(SpanData span) {
    return !span.getParentSpanContext().isValid();
  }

  private static void assertCollectionName(SpanData span) {
    assertEquals(COLLECTION, span.getAttributes().get(TraceUtils.TAG_DB));
  }

  private static void assertCoreName(SpanData span) {
    assertTrue(span.getAttributes().get(TraceUtils.TAG_DB).startsWith(COLLECTION + "_"));
  }

  private void assertOneSpanIsChildOfAnother(List<SpanData> finishedSpans) {
    SpanData child = finishedSpans.get(0);
    SpanData parent = finishedSpans.get(1);
    if (isRootSpan(child)) {
      var temp = parent;
      parent = child;
      child = temp;
    }
    assertEquals(child.getParentSpanContext().getTraceId(), parent.getTraceId());
    assertEquals(child.getTraceId(), parent.getTraceId());
  }

  static List<SpanData> getAndClearSpans() {
    InMemorySpanExporter exporter = CustomTestOtelTracerConfigurator.getInMemorySpanExporter();
    List<SpanData> result = new ArrayList<>(exporter.getFinishedSpanItems());
    Collections.reverse(result); // nicer to see spans chronologically
    exporter.reset();
    return result;
  }

  private String getRootTraceId(List<SpanData> finishedSpans) {
    assertEquals(1, finishedSpans.stream().filter(TestDistributedTracing::isRootSpan).count());
    return finishedSpans.stream()
        .filter(TestDistributedTracing::isRootSpan)
        .findFirst()
        .get()
        .getSpanContext()
        .getTraceId();
  }
}
