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

package org.apache.solr.cloud;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.handler.component.SearchHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestGracefulJettyShutdown extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public void testSingleShardInFlightRequestsDuringShutDown() throws Exception {
    final String collection = getSaferTestName();
    final String handler = "/foo";

    final Semaphore handlerGate = new Semaphore(0);
    final Semaphore handlerSignal = new Semaphore(0);

    final ExecutorService exec = ExecutorUtil.newMDCAwareCachedThreadPool("client-requests");
    final MiniSolrCloudCluster cluster =
        new MiniSolrCloudCluster(1, createTempDir(), JettyConfig.builder().build());
    try {
      assertTrue(
          CollectionAdminRequest.createCollection(collection, "_default", 1, 1)
              .process(cluster.getSolrClient())
              .isSuccess());

      final JettySolrRunner nodeToStop = cluster.getJettySolrRunner(0);

      // register our custom handler with "all" (one) of our SolrCores
      for (String coreName : nodeToStop.getCoreContainer().getLoadedCoreNames()) {
        try (SolrCore core = nodeToStop.getCoreContainer().getCore(coreName)) {
          final BlockingSearchHandler h = new BlockingSearchHandler(handlerGate, handlerSignal);
          h.inform(core);
          core.registerRequestHandler(handler, h);
        }
      }

      final CloudSolrClient cloudClient = cluster.getSolrClient();

      // add a few docs...
      cloudClient.add(collection, sdoc("id", "xxx", "foo_s", "aaa"));
      cloudClient.add(collection, sdoc("id", "yyy", "foo_s", "bbb"));
      cloudClient.add(collection, sdoc("id", "zzz", "foo_s", "aaa"));
      cloudClient.commit(collection);

      final List<Future<QueryResponse>> results = new ArrayList<>(13);

      try (SolrClient jettyClient = nodeToStop.newClient()) {
        final QueryRequest req = new QueryRequest(params("q", "foo_s:aaa"));
        req.setPath(handler);

        // check inflight requests using both clients...
        for (SolrClient client : Arrays.asList(cloudClient, jettyClient)) {
          results.add(
              exec.submit(
                  () -> {
                    return req.process(client, collection);
                  }));
        }

        // wait for our handlers to indicate they have recieved the requests and started processing
        log.info("Waiting for signals from both requests");
        assertTrue(handlerSignal.tryAcquire(2, 300, TimeUnit.SECONDS)); // safety valve

        // stop our node (via executor so it doesn't block) and open the gate for our handlers
        log.info("Stopping jetty node");
        final Future<Boolean> stopped =
            exec.submit(
                () -> {
                  nodeToStop.stop();
                  return true;
                });
        log.info("Releasing gate for requests");
        handlerGate.release(2);
        log.info("Released gate for requests");

        // confirm success of requests
        assertEquals(2, results.size());
        for (Future<QueryResponse> f : results) {
          final QueryResponse rsp = f.get(300, TimeUnit.SECONDS); // safety valve
          assertEquals(2, rsp.getResults().getNumFound());
        }
        assertTrue(stopped.get(300, TimeUnit.SECONDS)); // safety valve
      }

    } finally {
      handlerGate.release(9999999); // safety valve
      handlerSignal.release(9999999); // safety valve
      cluster.shutdown();
      ExecutorUtil.shutdownAndAwaitTermination(exec);
    }
  }

  /**
   * On every request, prior to handling, releases a ticket to it's signal Semaphre, and then blocks
   * until it can acquire a ticket from it's gate semaphore. Has a safety valve that gives up on the
   * gate after 300 seconds
   */
  private static final class BlockingSearchHandler extends SearchHandler {
    private final Semaphore gate;
    private final Semaphore signal;

    public BlockingSearchHandler(final Semaphore gate, final Semaphore signal) {
      super();
      this.gate = gate;
      this.signal = signal;
      super.init(new NamedList<>());
    }

    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
      log.info("Starting request");
      signal.release();
      if (gate.tryAcquire(300, TimeUnit.SECONDS)) {
        super.handleRequestBody(req, rsp);
      } else {
        log.error("Gate safety valve timeout");
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Gate timeout");
      }
      log.info("Finishing request");
    }
  }
}
