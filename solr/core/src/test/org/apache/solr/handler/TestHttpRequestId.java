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
package org.apache.solr.handler;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.SolrPing;
import org.apache.solr.client.solrj.util.AsyncListener;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.LogListener;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.MDC;

@LogLevel("org.apache.solr.client.solrj.impl.Http2SolrClient=DEBUG")
public class TestHttpRequestId extends SolrJettyTestBase {

  @BeforeClass
  public static void beforeTest() throws Exception {
    createAndStartJetty(legacyExampleCollection1SolrHome());
  }

  @Test
  public void mdcContextTest() {
    String collection = "/collection1";
    BlockingQueue<Runnable> workQueue = new SynchronousQueue<>(false);
    setupClientAndRun(collection, workQueue, 0);
  }

  @Test
  public void mdcContextFailureTest() {
    String collection = "/doesnotexist";
    BlockingQueue<Runnable> workQueue = new SynchronousQueue<>(false);
    setupClientAndRun(collection, workQueue, 0);
  }

  @Test
  public void mdcContextTest2() {
    String collection = "/collection1";
    BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(10, false);
    setupClientAndRun(collection, workQueue, 3);
  }

  @Test
  public void mdcContextFailureTest2() {
    String collection = "/doesnotexist";
    BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(10, false);
    setupClientAndRun(collection, workQueue, 3);
  }

  @SuppressForbidden(reason = "We need to use log4J2 classes directly to test MDC impacts")
  private void setupClientAndRun(
      String collection, BlockingQueue<Runnable> workQueue, int corePoolSize) {
    final String key = "mdcContextTestKey" + System.nanoTime();
    final String value = "TestHttpRequestId" + System.nanoTime();

    AsyncListener<NamedList<Object>> listener =
        new AsyncListener<>() {
          @Override
          public void onSuccess(NamedList<Object> t) {
            assertEquals(value, MDC.get(key));
          }

          @Override
          public void onFailure(Throwable throwable) {
            assertEquals(value, MDC.get(key));
          }
        };

    try (LogListener reqLog =
        LogListener.debug(Http2SolrClient.class).substring("response processing")) {
      // client setup needs to be same as HttpShardHandlerFactory
      ThreadPoolExecutor commExecutor =
          new ExecutorUtil.MDCAwareThreadPoolExecutor(
              corePoolSize,
              Integer.MAX_VALUE,
              1,
              TimeUnit.SECONDS,
              workQueue,
              new SolrNamedThreadFactory("httpShardExecutor"),
              false);
      try (Http2SolrClient client =
          new Http2SolrClient.Builder(getBaseUrl() + collection)
              .withExecutor(commExecutor)
              .build()) {
        MDC.put(key, value);
        client.asyncRequest(new SolrPing(), null, listener);
      } finally {
        ExecutorUtil.shutdownAndAwaitTermination(commExecutor);
        MDC.remove(key);
      }

      // expecting 2 events: success|failed, completed
      Queue<LogEvent> reqLogQueue = reqLog.getQueue();
      assertEquals(2, reqLogQueue.size());
      while (!reqLogQueue.isEmpty()) {
        var reqEvent = reqLogQueue.poll();
        assertEquals(value, reqEvent.getContextData().getValue(key));
      }
    }
  }
}
