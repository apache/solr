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
package org.apache.solr.handler.component;

import java.util.List;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.ConfigRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.QueryLimit;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.ThreadCpuTimer;
import org.junit.After;
import org.junit.BeforeClass;

/** Shard CPU time when a query limit stops a distributed search. */
public class ShardCpuTimeLimitTest extends SolrCloudTestCase {

  private static final String COLLECTION = "limit-cpu";
  private static final String HANDLER = "/fixedcpu";
  private static final String STOP_AT_PARAM = "stopAt";
  private static final String FINISHING = "fixedCpuFinishing";
  private static final long SHARD_CPU_MS = 1000L;
  // GET_TOP_IDS goes to both shards, so the first round of responses reports this much
  private static final long TOP_IDS_CPU_MS = 2 * SHARD_CPU_MS;
  // GET_FIELDS then goes only to the shard holding the one doc
  private static final long GET_FIELDS_CPU_MS = TOP_IDS_CPU_MS + SHARD_CPU_MS;

  /**
   * Reports a fixed shard cpuTime, and marks when the coordinator finishes a stage with responses.
   */
  public static class FixedCpuComponent extends SearchComponent {
    @Override
    public void prepare(ResponseBuilder rb) {}

    @Override
    public void process(ResponseBuilder rb) {
      rb.rsp.getResponseHeader().add(ThreadCpuTimer.CPU_TIME, SHARD_CPU_MS);
    }

    @Override
    public void finishStage(ResponseBuilder rb) {
      if (!rb.finished.isEmpty()) {
        rb.req.getContext().put(FINISHING, true);
      }
    }

    @Override
    public String getDescription() {
      return "fixed cpuTime";
    }
  }

  /** Stops the coordinator at the check named by {@link #STOP_AT_PARAM}. */
  private static class StopAtLimit implements QueryLimit {
    @Override
    public boolean shouldExit() {
      SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
      ResponseBuilder rb = info == null ? null : info.getResponseBuilder();
      if (rb == null || rb.finished == null) {
        return false;
      }
      return switch (rb.req.getParams().get(STOP_AT_PARAM, "")) {
        case "start" -> true;
        case "handleResponses" -> !rb.finished.isEmpty();
        case "finishStage" -> rb.req.getContext().containsKey(FINISHING);
        case "getFields" -> rb.getStage() == ResponseBuilder.STAGE_GET_FIELDS;
        case "afterGetFields" ->
            rb.req.getContext().containsKey(FINISHING) && rb.finished.size() > 1;
        default -> false;
      };
    }

    @Override
    public Object currentValue() {
      return null;
    }
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty(ThreadCpuTimer.ENABLE_CPU_TIME, "true");
    configureCluster(2).addConfig("conf", configset("cloud-dynamic")).configure();
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 1)
        .process(cluster.getSolrClient());
    waitForState("collection", COLLECTION, clusterShape(2, 2));

    cluster
        .getSolrClient()
        .request(
            new ConfigRequest(
                "{'add-searchcomponent': {'name': 'fixedcpu', 'class': '"
                    + FixedCpuComponent.class.getName()
                    + "'}}"),
            COLLECTION);
    cluster
        .getSolrClient()
        .request(
            new ConfigRequest(
                "{'add-requesthandler': {'name': '"
                    + HANDLER
                    + "', 'class': '"
                    + SearchHandler.class.getName()
                    + "', 'first-components': ['fixedcpu']}}"),
            COLLECTION);

    new UpdateRequest().add(sdoc("id", "1")).commit(cluster.getSolrClient(), COLLECTION);
  }

  @After
  public void clearLimit() {
    TestInjection.queryTimeout = null;
  }

  public void testStopInHandleResponses() throws Exception {
    assertShardCpuTime("handleResponses", TOP_IDS_CPU_MS);
  }

  public void testStopInFinishStage() throws Exception {
    assertShardCpuTime("finishStage", TOP_IDS_CPU_MS);
  }

  public void testStopBeforeGetFields() throws Exception {
    assertShardCpuTime("getFields", TOP_IDS_CPU_MS);
  }

  public void testStopAfterGetFields() throws Exception {
    assertShardCpuTime("afterGetFields", GET_FIELDS_CPU_MS);
  }

  public void testStopWithPartialResultsOmitted() throws Exception {
    assertShardCpuTime("handleResponses", false, TOP_IDS_CPU_MS);
  }

  public void testStopBeforeAnyShardResponse() throws Exception {
    SolrQueryResponse rsp = stopAt("start", true);
    assertNull(rsp.getToLog().get(ThreadCpuTimer.CPU_TIME));
  }

  private void assertShardCpuTime(String stopAt, long expected) throws Exception {
    assertShardCpuTime(stopAt, true, expected);
  }

  private void assertShardCpuTime(String stopAt, boolean partialResults, long expected)
      throws Exception {
    SolrQueryResponse rsp = stopAt(stopAt, partialResults);
    assertEquals(List.of(expected), rsp.getResponseHeader().getAll(ThreadCpuTimer.CPU_TIME));
    assertEquals(expected, rsp.getToLog().get(ThreadCpuTimer.CPU_TIME));
  }

  private SolrQueryResponse stopAt(String stopAt, boolean partialResults) throws Exception {
    TestInjection.queryTimeout = new StopAtLimit();
    Replica replica = getCollectionState(COLLECTION).replicaStream().findFirst().orElseThrow();
    try (SolrCore core =
            cluster.getReplicaJetty(replica).getCoreContainer().getCore(replica.getCoreName());
        SolrQueryRequest req =
            new SolrQueryRequestBase(
                core,
                params(
                    "q",
                    "*:*",
                    ShardParams.SHARDS_QT,
                    HANDLER,
                    ShardParams.DISTRIB_SINGLE_PASS,
                    "false",
                    CommonParams.PARTIAL_RESULTS,
                    String.valueOf(partialResults),
                    STOP_AT_PARAM,
                    stopAt)) {}) {
      SolrQueryResponse rsp = new SolrQueryResponse();
      SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp));
      try {
        core.execute(core.getRequestHandler(HANDLER), req, rsp);
      } finally {
        SolrRequestInfo.clearRequestInfo();
      }
      assertEquals(
          SolrQueryResponse.partialResultsStatus(!partialResults),
          rsp.getResponseHeader().get(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY));
      return rsp;
    }
  }
}
