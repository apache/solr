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
package org.apache.solr.cloud.api.collections;

import java.sql.Timestamp;

import java.util.Random;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.CachedThreadStatesGaugeSet;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.MetricsServlet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.util.AsyncListener;
import java.util.List;
import java.util.ArrayList;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StopWatch;
import org.apache.solr.common.util.metrics.Metrics;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Slow
@LuceneTestCase.Nightly
public class CreateCollectionsIndexAndRestartTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final int RECOVERY_WAIT = 15;
  public static final int NODE_COUNT = 4;

  private static boolean metricsRegistered = false;
  private static Server server;

  @BeforeClass
  public static void beforeCreateCollectionsIndexAndRestartTest() throws Exception {
    //   / checkInterruptsOnFinish = false;
    //System.setProperty("solr.containerThreadsIdleTimeout", "100");
    // System.setProperty("solr.minContainerThreads", "8");

    System.setProperty("solr.perThreadPoolSize", "16");
    System.setProperty("zookeeper.globalOutstandingLimit", "500");
    System.setProperty("zookeeper.preAllocSize", "64000");


    System.setProperty("solr.default.collection_op_timeout", "540000");
    System.setProperty("solr.enableMetrics", "false");
    System.setProperty("solr.minHttp2ClientThreads", "60");
    System.setProperty("solr.maxHttp2ClientThreads", "3024");


    System.setProperty("solr.enablePublicKeyHandler", "false");
    System.setProperty("zookeeper.nio.numSelectorThreads", "16");
    System.setProperty("zookeeper.nio.numWorkerThreads", "16");
    System.setProperty("zookeeper.commitProcessor.numWorkerThreads", "16");
    System.setProperty("zookeeper.nio.shutdownTimeout", "1000");
   // System.setProperty("zookeeper.nio.sessionlessCnxnTimeout", "30000");

    System.setProperty("zookeeper.admin.enableServer", "false");
    System.setProperty("zookeeper.skipACL", "true");
    System.setProperty("zookeeper.nio.directBufferBytes", String.valueOf(32 << 10));
    System.setProperty("solr.zkclienttimeout", "30000");
    System.setProperty("solr.getleader.looptimeout", "8000");
    System.setProperty("disableCloseTracker", "true");
    System.setProperty("solr.rootSharedThreadPoolCoreSize", "120");
   // System.setProperty("solr.v2RealPath", "false");
    System.clearProperty("solr.v2RealPath");
    System.setProperty("disable.v2.api", "true");


    System.setProperty("lucene.cms.override_spins", "false"); // TODO: detecting spins for every core, every IW#ConcurrentMergeScheduler can be a bit costly, let's detect and cache somehow?

    System.setProperty("useCompoundFile", "false");
    System.clearProperty("solr.tests.maxBufferedDocs");

    System.clearProperty("pkiHandlerPrivateKeyPath");
    System.clearProperty("pkiHandlerPublicKeyPath");


    System.setProperty("solr.enablePublicKeyHandler", "false");

    System.setProperty("solr.v2RealPath", "true");
//    /System.setProperty("zookeeper.forceSync", "no");


    //System.setProperty("solr.clustering.enabled", "false");
    System.setProperty("solr.peerSync.useRangeVersions", String.valueOf(random().nextBoolean()));

    // we need something as a default, at least these are fast
    System.setProperty(SolrTestCaseJ4.USE_NUMERIC_POINTS_SYSPROP, "false");
    System.setProperty("solr.tests.IntegerFieldType", "org.apache.solr.schema.TrieIntField");
    System.setProperty("solr.tests.FloatFieldType", "org.apache.solr.schema.TrieFloatField");
    System.setProperty("solr.tests.LongFieldType", "org.apache.solr.schema.TrieLongField");
    System.setProperty("solr.tests.DoubleFieldType", "org.apache.solr.schema.TrieDoubleField");
    System.setProperty("solr.tests.DateFieldType", "org.apache.solr.schema.TrieDateField");

    System.setProperty("solr.tests.EnumFieldType", "org.apache.solr.schema.EnumFieldType");
    System.setProperty("solr.tests.numeric.dv", "true");

    System.setProperty("solr.tests.ramBufferSizeMB", "64");
   // System.setProperty("solr.tests.ramPerThreadHardLimitMB", "256");

  //  System.clearProperty("solr.tests.mergePolicyFactory");

 //   System.setProperty("solr.mscheduler", "org.apache.lucene.index.ConcurrentMergeScheduler");
//
    //enableReuseOfCryptoKeys();

    if (!metricsRegistered) {
      MetricRegistry metricsRegisty = Metrics.MARKS_METRICS;
      metricsRegisty.register("threads", new CachedThreadStatesGaugeSet(3, TimeUnit.SECONDS));

      CollectorRegistry.defaultRegistry.register(new DropwizardExports(metricsRegisty));
      metricsRegistered = true;
    }

    server = new Server(8989);
    ServletContextHandler context = new ServletContextHandler();
    context.setContextPath("/");
    server.setHandler(context);
    context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
    server.start();
   // server.join();

    useFactory(null);
    configureCluster(NODE_COUNT).addConfig("conf", SolrTestUtil.configset("cloud-minimal")).formatZk(true).configure();
  }

  @AfterClass
  public static void afterCreateCollectionsIndexAndRestartTest() throws Exception {
    shutdownCluster();
    server.stop();
  }

  @After
  public void afterTest() throws Exception {
    log.info("Test is complete, tearing down");
    cluster.getZkServer().writeZkMonLayout("afterTest");
  }

  @Test
  public void start() throws Exception {
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    int collectionCnt = 1;
    int numShards = 4;
    int numReplicas = 4;

    Random rnd = new Random();
    CloudHttp2SolrClient client = cluster.getSolrClient();

//    CollectionAdminRequest.createCollection("warmCollection", "conf", numShards, numReplicas).process(client);
//    CollectionAdminRequest.deleteCollection("warmCollection").process(client);

    StopWatch totalRunStopWatch = new StopWatch(true);
    totalRunStopWatch.start("totalRun");
    List<String> nodes = new ArrayList<>(NODE_COUNT);
    List<JettySolrRunner> jetties = cluster.getJettySolrRunners();
    for (JettySolrRunner runner : jetties) {
      nodes.add(runner.getBaseUrl());
    }

    System.err.println(timestamp.getTime() + " ********* CREATING " + collectionCnt + ' ' + numShards + 'x' + numReplicas  +
        " COLLECTIONS (" + (collectionCnt * numReplicas * numShards) + ") SolrCores on " + NODE_COUNT + " Solr instances");
    StopWatch stopWatch = new StopWatch(true);
    stopWatch.start("Create Collections");

    Set<Future> futures = ConcurrentHashMap.newKeySet(collectionCnt);
    for (int i = 1; i <= collectionCnt; i++) {
      final String collectionName = "testCollection" + i;
      Future<?> future = ParWork.getRootSharedExecutor().submit(() -> {
        try {
          //System.err.println(new Date() + " - Create Collection " + collectionName);
          CollectionAdminRequest.Create request = CollectionAdminRequest.createCollection(collectionName, "conf", numShards, numReplicas);
          request.setBasePath(nodes.get(rnd.nextInt(NODE_COUNT)));
          Http2SolrClient http2Client = client.getHttpClient();
          http2Client.asyncRequest(request, null, new CollectionCreateAsyncListener());

        } catch (Exception e) {
          log.error("Collection create call failed!", e);
        }
      });
      futures.add(future);
    }

    for (Future future : futures) {
      future.get(240, TimeUnit.SECONDS);
    }

    stopWatch.done();
    System.err.println(timestamp.getTime()  + " ********* CREATING " + collectionCnt + " COLLECTIONS DONE, WAITING FOR FULLY ACTIVE STATES: " + stopWatch.getTime() + "ms");
    stopWatch.start("Wait for active states");

    for (int i = 1; i <= collectionCnt; i++) {
      final String collectionName = "testCollection" + i;
      cluster.waitForActiveCollection(collectionName, RECOVERY_WAIT, TimeUnit.SECONDS, false, numShards, numReplicas * numShards, true, false);
    }
    stopWatch.done();
    System.err.println(timestamp.getTime() + " ********* " + collectionCnt + " COLLECTIONS FOUND FULLY ACTIVE: " + stopWatch.getTime() + "ms");
    stopWatch.start("Stop Jetty Instances");
    System.err.println(timestamp.getTime()  + " ********* RANDOMLY RESTARTING JETTY INSTANCES");
    Set<JettySolrRunner> stoppedRunners = ConcurrentHashMap.newKeySet();

    try (ParWork work = new ParWork(this)) {
      for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
        if (random().nextBoolean()) {
          continue;
        }
        System.err.println(timestamp.getTime()+ " ********* STOPPING " + runner.getBaseUrl());
        stoppedRunners.add(runner);
        work.collect("", () -> {
          try {
            runner.stop();
          } catch (Exception e) {
            log.error("", e);
          }
        });
      }
    }

    stopWatch.done();

    System.err.println(timestamp.getTime() + " ********* DONE RANDOMLY STOPPING JETTY INSTANCES: " + stopWatch.getTime() + "ms");
    log.debug(timestamp.getTime()+ " ********* DONE RANDOMLY STOPPING JETTY INSTANCES: " + stopWatch.getTime() + "ms");

    stopWatch.start("Starting Jetty instances");

    try (ParWork work = new ParWork(this)) {
      for (JettySolrRunner runner : stoppedRunners) {
        System.err.println(timestamp.getTime() + " ********* STARTING " + runner.getBaseUrl());
        log.debug(timestamp.getTime() + " ********* STARTING " + runner.getBaseUrl());
        work.collect("", () -> {
          try {
            runner.start();
          } catch (Exception e) {
            log.error("", e);
          }
        });
      }
    }
    stopWatch.done();
    System.err.println(timestamp.getTime() + " ********* DONE STARTING JETTY INSTANCES: " + stopWatch.getTime() + "ms");

//    System.err.println(timestamp.getTime() + " ********* WAITING 5 SECONDS");
//    Thread.sleep(5000);

    System.err.println(timestamp.getTime()+ " ********* WAITING TO SEE " + collectionCnt + " FULLY ACTIVE COLLECTIONS");
    log.debug(timestamp.getTime() +  " ********* WAITING TO SEE " + collectionCnt + " FULLY ACTIVE COLLECTIONS");
    stopWatch.start("Wait for ACTIVE collections");

    for (int i = 1; i <= collectionCnt; i++) {
      final String collectionName = "testCollection" + i;
      cluster.waitForActiveCollection(collectionName, RECOVERY_WAIT, TimeUnit.SECONDS, false, numShards, numShards * numReplicas, false, false);
    }

    stopWatch.done();
    System.err.println(timestamp.getTime() + " ********* " + collectionCnt + " COLLECTIONS FOUND FULLY ACTIVE: " + stopWatch.getTime() + "ms");

    log.debug(timestamp.getTime() + " ********* " + collectionCnt + " COLLECTIONS FOUND FULLY ACTIVE: " + stopWatch.getTime() + "ms");

    totalRunStopWatch.done();

    System.err.println("\n\n *********  \uD83D\uDE0E DONE FULL TEST METHOD: " + totalRunStopWatch.getTime() + "ms  \uD83D\uDE0E *********\n");
  }

  private static class CollectionCreateAsyncListener implements AsyncListener<NamedList<Object>> {
    @Override public void onSuccess(NamedList<Object> objectNamedList, int code, Object context) {

    }

    @Override public void onFailure(Throwable throwable, int code, Object context) {
      log.error("Collection create call failed!", throwable);
    }
  }
}
