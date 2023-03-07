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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
@SuppressForbidden(reason = "We need to use log4J2 classes directly to test MDC impacts")
public class TestHttpRequestId extends SolrJettyTestBase {

  @BeforeClass
  public static void beforeTest() throws Exception {
    createAndStartJetty(legacyExampleCollection1SolrHome());
  }

  @Test
  public void mdcContextTest() throws Exception {
    String collection = "/collection1";
    BlockingQueue<Runnable> workQueue = new SynchronousQueue<Runnable>(false);
    setupClientAndRun(collection, workQueue);
  }

  @Test
  public void mdcContextFailureTest() throws Exception {
    String collection = "/doesnotexist";
    BlockingQueue<Runnable> workQueue = new SynchronousQueue<Runnable>(false);
    setupClientAndRun(collection, workQueue);
  }

  @Test
  public void mdcContextTest2() throws Exception {
    String collection = "/collection1";
    BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<Runnable>(10, false);
    setupClientAndRun(collection, workQueue);
  }

  @Test
  public void mdcContextFailureTest2() throws Exception {
    String collection = "/doesnotexist";
    BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<Runnable>(10, false);
    setupClientAndRun(collection, workQueue);
  }

  private void setupClientAndRun(String collection, BlockingQueue<Runnable> workQueue) {
    String key = "mdcContextTestKey" + System.nanoTime();
    String value = "TestHttpRequestId" + System.nanoTime();

    AsyncListener<NamedList<Object>> listener =
        new AsyncListener<>() {

          @Override
          public void onSuccess(NamedList<Object> t) {
            assertTrue(value, value.equals(MDC.get(key)));
          }

          @Override
          public void onFailure(Throwable throwable) {
            assertTrue(value, value.equals(MDC.get(key)));
          }
        };

    try (LogListener reqLog =
        LogListener.debug(Http2SolrClient.class).substring("response processing")) {

      ThreadPoolExecutor commExecutor = null;
      Http2SolrClient client = null;
      try {
        // client setup needs to be same as HttpShardHandlerFactory
        commExecutor =
            new ExecutorUtil.MDCAwareThreadPoolExecutor(
                3,
                Integer.MAX_VALUE,
                1,
                TimeUnit.SECONDS,
                workQueue,
                new SolrNamedThreadFactory("httpShardExecutor"),
                false);
        client =
            new Http2SolrClient.Builder(jetty.getBaseUrl().toString() + collection)
                .withExecutor(commExecutor)
                .build();

        MDC.put(key, value);
        client.asyncRequest(new SolrPing(), null, listener);

      } finally {
        if (client != null) {
          client.close();
        }
        ExecutorUtil.shutdownAndAwaitTermination(commExecutor);
        MDC.remove(key);
      }

      // expecting 3 events: started, success|failed, completed
      assertEquals(3, reqLog.getQueue().size());
      while (!reqLog.getQueue().isEmpty()) {
        var reqEvent = reqLog.getQueue().poll();
        assertTrue(reqEvent.getContextData().containsKey(key));
        assertEquals(value, reqEvent.getContextData().getValue(key));
      }
    }
  }
}
