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
package org.apache.solr.handler.admin;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.util.RetryUtil;
import org.apache.solr.core.SolrCore;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.SolrMetricTestUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class StatsReloadRaceTest extends SolrTestCaseJ4 {

  // to support many times repeating
  static AtomicInteger taskNum = new AtomicInteger();

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml", "schema.xml");

    XmlDoc docs = new XmlDoc();
    for (int i = 0; i < atLeast(10); i++) {
      docs.xml += doc("id", "" + i, "name_s", "" + i);
    }
    assertU(add(docs));
    assertU(commit());
  }

  @Test
  public void testParallelReloadAndStats() throws Exception {

    Random random = random();

    for (int i = 0; i < atLeast(random, 2); i++) {

      int asyncId = taskNum.incrementAndGet();
      h.getCoreContainer()
          .getMultiCoreHandler()
          .handleRequest(
              req(
                  CommonParams.QT,
                  "/admin/cores",
                  CoreAdminParams.ACTION,
                  CoreAdminParams.CoreAdminAction.RELOAD.toString(),
                  CoreAdminParams.CORE,
                  DEFAULT_TEST_CORENAME,
                  "async",
                  "" + asyncId),
              new SolrQueryResponse());
      try (SolrCore core = h.getCoreInc()) {
        boolean isCompleted;
        do {
          if (random.nextBoolean()) {
            requestMetrics(core, true);
          } else {
            requestCoreStatus();
          }

          isCompleted = checkReloadCompletion(asyncId);
        } while (!isCompleted);
        requestMetrics(core, false);
      }
    }
  }

  private void requestCoreStatus() {
    SolrQueryResponse rsp = new SolrQueryResponse();
    h.getCoreContainer()
        .getMultiCoreHandler()
        .handleRequest(
            req(
                CoreAdminParams.ACTION,
                CoreAdminParams.CoreAdminAction.STATUS.toString(),
                "core",
                DEFAULT_TEST_CORENAME),
            rsp);
    assertNull("" + rsp.getException(), rsp.getException());
  }

  private boolean checkReloadCompletion(int asyncId) {
    boolean isCompleted;
    SolrQueryResponse rsp = new SolrQueryResponse();
    h.getCoreContainer()
        .getMultiCoreHandler()
        .handleRequest(
            req(
                CoreAdminParams.ACTION,
                CoreAdminParams.CoreAdminAction.REQUESTSTATUS.toString(),
                CoreAdminParams.REQUESTID,
                "" + asyncId),
            rsp);

    List<Object> statusLog = rsp.getValues().getAll(CoreAdminAction.STATUS.name());

    assertFalse(
        "expect status check w/o error, got:" + statusLog,
        statusLog.contains(CoreAdminHandler.CoreAdminAsyncTracker.FAILED));

    isCompleted = statusLog.contains(CoreAdminHandler.CoreAdminAsyncTracker.COMPLETED);
    return isCompleted;
  }

  private void requestMetrics(SolrCore core, boolean softFail) throws Exception {
    try {
      // this is not guaranteed to exist right away after core reload - there's a
      // small window between core load and before searcher metrics are registered,
      // so we may have to check a few times, and then fail softly if reload is not complete yet
      RetryUtil.retryUntil(
          "solr_searcher_index_version metric not found",
          10,
          500,
          TimeUnit.MILLISECONDS,
          () -> {
            var reader =
                core.getSolrMetricsContext()
                    .getMetricManager()
                    .getPrometheusMetricReader(core.getSolrMetricsContext().getRegistryName());
            if (reader == null) {
              return false;
            }

            var datapoint =
                SolrMetricTestUtils.getGaugeDatapoint(
                    reader,
                    "solr_core_indexsearcher_index_version",
                    SolrMetricTestUtils.newStandaloneLabelsBuilder(core)
                        .label("category", "SEARCHER")
                        .build());

            return datapoint != null;
          });
    } catch (Exception e) {
      if (softFail) {
        return;
      }
      throw e;
    }
  }
}
