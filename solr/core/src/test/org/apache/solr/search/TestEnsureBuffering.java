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

package org.apache.solr.search;

import java.util.Map;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.processor.DistributedUpdateProcessor.DistribPhase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

/**
 * Regression for review finding C1: on a recovery RETRY after an errored leader-tlog catch-up
 * replay, {@code LogReplayer}'s {@code finally} has flipped ulog state to ACTIVE while the buffer
 * tlog still holds the un-applied window-forwarded updates ({@code applyLeaderTlogThenBuffer} only
 * drops the buffer on an error-free replay). The old guard
 * ({@code if (state != BUFFERING) bufferUpdates()}) therefore saw ACTIVE and called
 * {@link UpdateLog#bufferUpdates()}, which unconditionally drops the buffer, causing permanent data
 * loss. {@link UpdateLog#ensureBuffering()} must instead PRESERVE an already-present buffer and only
 * start a fresh one when none exists.
 *
 * <p>Not {@code @Nightly} (unlike {@code TestRecovery}) so it runs in the normal suite.
 */
public class TestEnsureBuffering extends SolrTestCaseJ4 {

  private static final String FROM_LEADER = DistribPhase.FROMLEADER.toString();

  @Before
  public void beforeTest() throws Exception {
    useFactory(null);
    initCore("solrconfig-tlog.xml", "schema15.xml");
  }

  @After
  public void afterTest() {
    deleteCore();
  }

  @SuppressWarnings("unchecked")
  private Gauge<Integer> bufferedOpsGauge() {
    SolrMetricManager manager = h.getCoreContainer().getMetricManager();
    SolrCore core = h.getCore();
    MetricRegistry registry = manager.registry(core.getCoreMetricManager().getRegistryName());
    core.close();
    Map<String, Metric> metrics = registry.getMetrics();
    return (Gauge<Integer>) metrics.get("TLOG.buffered.ops");
  }

  @Test
  public void testEnsureBufferingPreservesExistingBuffer() throws Exception {
    SolrQueryRequest req = req();
    try {
      UpdateHandler uhandler = req.getCore().getUpdateHandler();
      UpdateLog ulog = uhandler.getUpdateLog();
      Gauge<Integer> bufferedOps = bufferedOpsGauge();

      clearIndex();
      assertU(commit());

      assertEquals(UpdateLog.State.ACTIVE, ulog.getState());
      ulog.bufferUpdates();
      assertEquals(UpdateLog.State.BUFFERING, ulog.getState());

      // Leader forwards three updates while we BUFFER; they accumulate in the buffer tlog.
      updateJ(jsonAdd(sdoc("id", "C1a", "_version_", "1010")), params(DISTRIB_UPDATE_PARAM, FROM_LEADER));
      updateJ(jsonAdd(sdoc("id", "C1b", "_version_", "1020")), params(DISTRIB_UPDATE_PARAM, FROM_LEADER));
      updateJ(jsonAdd(sdoc("id", "C1c", "_version_", "1030")), params(DISTRIB_UPDATE_PARAM, FROM_LEADER));
      assertEquals("three updates should be buffered", 3, bufferedOps.getValue().intValue());

      // C1 fix: ensureBuffering() preserves the buffer (the old bufferUpdates() guard dropped it).
      ulog.ensureBuffering();
      assertEquals(UpdateLog.State.BUFFERING, ulog.getState());
      assertEquals("ensureBuffering() must NOT drop the buffered updates", 3, bufferedOps.getValue().intValue());

      // Contrast: bufferUpdates() drops the buffer — the legacy behavior that caused C1.
      ulog.bufferUpdates();
      assertEquals(UpdateLog.State.BUFFERING, ulog.getState());
      assertEquals("bufferUpdates() drops the buffer", 0, bufferedOps.getValue().intValue());

      // With no buffer present, ensureBuffering() behaves like a fresh bufferUpdates().
      ulog.ensureBuffering();
      assertEquals(UpdateLog.State.BUFFERING, ulog.getState());
      assertEquals(0, bufferedOps.getValue().intValue());

      ulog.dropBufferedUpdates();
      assertEquals(UpdateLog.State.ACTIVE, ulog.getState());
    } finally {
      req.close();
    }
  }
}
