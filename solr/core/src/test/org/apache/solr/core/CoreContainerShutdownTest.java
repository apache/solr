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
package org.apache.solr.core;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Shutdown must run its trailing closes even when a close task outlives the closeThreadPool's
 * termination budget and the pool wait throws. Those closes are the only release path for the
 * container's metrics contexts; skipping them leaks the whole metrics tree, and in a test JVM the
 * leaked contexts are then charged to an unrelated suite by the object release tracker.
 */
public class CoreContainerShutdownTest extends SolrTestCase {

  private static final String SOLR_XML =
      "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n"
          + "<solr>\n"
          + "<str name=\"configSetBaseDir\">${configsets:configsets}</str>\n"
          + "</solr>";

  @BeforeClass
  public static void beforeClass() {
    System.setProperty(
        "configsets", SolrTestCaseJ4.getFile("solr/configsets").toAbsolutePath().toString());
  }

  @Test
  public void testShutdownRunsTrailingClosesWhenClosePoolTimesOut() throws Exception {
    Path solrHome = createTempDir();
    CoreContainer cc = new CoreContainer(SolrXmlConfig.fromString(solrHome, SOLR_XML));
    cc.load();
    BlockingShardHandlerFactory blockingFactory = null;
    try {
      assertTrue(
          "the root metrics context is tracked after load",
          ObjectReleaseTracker.getTrackedObjectsOfType(SolrMetricsContext.class)
              .contains(cc.solrMetricsContext));

      CloseTrackingHandler handler = new CloseTrackingHandler();
      cc.getRequestHandlers().put("/close-tracking", handler);

      // Make one close task outlive the pool's whole termination budget (both 60 second
      // awaits), so the pool wait throws the way it did in the incident this test pins down.
      // This makes the test take about two minutes; the wait is the production timeout,
      // not a test sleep.
      blockingFactory = new BlockingShardHandlerFactory(cc.shardHandlerFactory);
      cc.shardHandlerFactory = blockingFactory;

      cc.shutdown();

      assertTrue(
          "container handlers must be closed even when the close pool times out",
          handler.closed.get());
      assertFalse(
          "the root metrics context must be released even when the close pool times out",
          ObjectReleaseTracker.getTrackedObjectsOfType(SolrMetricsContext.class)
              .contains(cc.solrMetricsContext));
    } finally {
      if (blockingFactory != null) {
        blockingFactory.releaseClose();
        // Wait for the blocked close task to finish so the delegate factory is closed
        // before the framework's post-class object release check runs.
        assertTrue(blockingFactory.awaitCloseFinished(30, TimeUnit.SECONDS));
      }
    }
  }

  /** A container handler whose close is observable. */
  private static final class CloseTrackingHandler extends RequestHandlerBase {
    final AtomicBoolean closed = new AtomicBoolean();

    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) {}

    @Override
    public String getDescription() {
      return "test handler that records its close";
    }

    @Override
    public PermissionNameProvider.Name getPermissionName(AuthorizationContext request) {
      return PermissionNameProvider.Name.ALL;
    }

    @Override
    public void close() throws IOException {
      closed.set(true);
      super.close();
    }
  }

  /**
   * Wraps the real shard handler factory with a close that blocks until released, surviving the
   * pool's force-stop interrupt the way a stuck Jetty client close does, and only then delegates.
   */
  private static final class BlockingShardHandlerFactory extends HttpShardHandlerFactory {
    private final ShardHandlerFactory delegate;
    private final CountDownLatch release = new CountDownLatch(1);
    private final CountDownLatch closeFinished = new CountDownLatch(1);

    BlockingShardHandlerFactory(ShardHandlerFactory delegate) {
      this.delegate = delegate;
    }

    @Override
    public void close() {
      try {
        boolean released = false;
        while (!released) {
          try {
            release.await();
            released = true;
          } catch (InterruptedException e) {
            // Keep blocking: the pool's force-stop interrupts its workers, and a close stuck
            // inside the client shutdown survives that interrupt without finishing.
          }
        }
        delegate.close();
      } finally {
        closeFinished.countDown();
      }
    }

    void releaseClose() {
      release.countDown();
    }

    boolean awaitCloseFinished(long timeout, TimeUnit unit) throws InterruptedException {
      return closeFinished.await(timeout, unit);
    }
  }
}
