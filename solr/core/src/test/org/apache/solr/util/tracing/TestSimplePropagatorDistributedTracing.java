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
package org.apache.solr.util.tracing;

import io.opentracing.util.GlobalTracer;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.CloudLegacySolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.SolrCore;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.update.processor.LogUpdateProcessorFactory;
import org.apache.solr.util.LogListener;
import org.apache.solr.util.TimeOut;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressForbidden(reason = "We need to use log4J2 classes directly to test MDC impacts")
public class TestSimplePropagatorDistributedTracing extends SolrCloudTestCase {

  private static final String COLLECTION = "collection1";

  @BeforeClass
  public static void setupCluster() throws Exception {

    configureCluster(4).addConfig("conf", configset("cloud-minimal")).configure();

    TimeOut timeOut = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    timeOut.waitFor(
        "Waiting for GlobalTracer is registered",
        () -> GlobalTracer.get().toString().contains("SimplePropagator"));

    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 2)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 2, 4);
  }

  @Test
  public void testQueryRequest() throws IOException, SolrServerException {
    CloudSolrClient cloudClient = cluster.getSolrClient();
    cloudClient.add(COLLECTION, sdoc("id", "1"));
    cloudClient.add(COLLECTION, sdoc("id", "2"));
    cloudClient.add(COLLECTION, sdoc("id", "3"));

    try (LogListener reqLog = LogListener.info(SolrCore.class.getName() + ".Request")) {

      try (SolrClient testClient = newCloudLegacySolrClient()) {
        // verify all query events have the same auto-generated trace id
        var r1 = testClient.query(COLLECTION, new SolrQuery("*:*"));
        assertEquals(0, r1.getStatus());
        assertSameTraceId(reqLog, null);

        // verify all query events have the same 'custom' trace id
        String traceId = "tidTestQueryRequest1";
        var q = new QueryRequest(new SolrQuery("*:*"));
        q.addHeader(SimplePropagator.TRACE_ID, traceId);
        var r2 = q.process(testClient, COLLECTION);
        assertEquals(0, r2.getStatus());
        assertSameTraceId(reqLog, traceId);
      }

      try (SolrClient testClient = newCloudHttp2SolrClient()) {
        // verify all query events have the same auto-generated trace id
        var r1 = testClient.query(COLLECTION, new SolrQuery("*:*"));
        assertEquals(0, r1.getStatus());
        assertSameTraceId(reqLog, null);

        // verify all query events have the same 'custom' trace id
        String traceId = "tidTestQueryRequest2";
        var q = new QueryRequest(new SolrQuery("*:*"));
        q.addHeader(SimplePropagator.TRACE_ID, traceId);
        var r2 = q.process(testClient, COLLECTION);
        assertEquals(0, r2.getStatus());
        assertSameTraceId(reqLog, traceId);
      }
    }
  }

  @Test
  public void testUpdateRequest() throws IOException, SolrServerException {
    try (LogListener reqLog = LogListener.info(LogUpdateProcessorFactory.class.getName())) {

      try (SolrClient testClient = newCloudLegacySolrClient()) {
        // verify all indexing events have trace id present
        testClient.add(COLLECTION, sdoc("id", "1"));
        testClient.add(COLLECTION, sdoc("id", "3"));
        var queue = reqLog.getQueue();
        assertFalse(queue.isEmpty());
        while (!queue.isEmpty()) {
          var reqEvent = queue.poll();
          String evTraceId = reqEvent.getContextData().getValue(MDCLoggingContext.TRACE_ID);
          assertNotNull(evTraceId);
        }

        // verify all events have the same 'custom' trace id
        String traceId = "tidTestUpdateRequest1";
        UpdateRequest u = new UpdateRequest();
        u.add(sdoc("id", "5"));
        u.add(sdoc("id", "7"));
        u.addHeader(SimplePropagator.TRACE_ID, traceId);
        var r1 = u.process(testClient, COLLECTION);
        assertEquals(0, r1.getStatus());
        assertSameTraceId(reqLog, traceId);
      }

      try (SolrClient testClient = newCloudHttp2SolrClient()) {
        // verify all indexing events have trace id present
        testClient.add(COLLECTION, sdoc("id", "2"));
        testClient.add(COLLECTION, sdoc("id", "4"));
        var queue = reqLog.getQueue();
        assertFalse(queue.isEmpty());
        while (!queue.isEmpty()) {
          var reqEvent = queue.poll();
          String evTraceId = reqEvent.getContextData().getValue(MDCLoggingContext.TRACE_ID);
          assertNotNull(evTraceId);
        }

        // verify all events have the same 'custom' trace id
        String traceId = "tidTestUpdateRequest2";
        UpdateRequest u = new UpdateRequest();
        u.add(sdoc("id", "6"));
        u.add(sdoc("id", "8"));
        u.addHeader(SimplePropagator.TRACE_ID, traceId);
        var r1 = u.process(testClient, COLLECTION);
        assertEquals(0, r1.getStatus());
        assertSameTraceId(reqLog, traceId);
      }
    }
  }

  private void assertSameTraceId(LogListener reqLog, String traceId) {
    var queue = reqLog.getQueue();
    assertFalse(queue.isEmpty());
    if (traceId == null) {
      traceId = queue.poll().getContextData().getValue(MDCLoggingContext.TRACE_ID);
      assertNotNull(traceId);
    }
    while (!queue.isEmpty()) {
      var reqEvent = queue.poll();
      String evTraceId = reqEvent.getContextData().getValue(MDCLoggingContext.TRACE_ID);
      assertEquals(traceId, evTraceId);
    }
  }

  private CloudSolrClient newCloudLegacySolrClient() {
    return new CloudLegacySolrClient.Builder(
            List.of(cluster.getZkServer().getZkAddress()), Optional.empty())
        .build();
  }

  private CloudHttp2SolrClient newCloudHttp2SolrClient() {
    var builder =
        new CloudHttp2SolrClient.Builder(
            List.of(cluster.getZkServer().getZkAddress()), Optional.empty());
    var client = builder.build();
    client.connect();
    return client;
  }
}
