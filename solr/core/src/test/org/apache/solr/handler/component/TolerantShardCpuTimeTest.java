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
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.ConfigRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.util.ThreadCpuTimer;
import org.junit.BeforeClass;

/** CPU time reported by a tolerant distributed search when one shard fails. */
public class TolerantShardCpuTimeTest extends SolrCloudTestCase {

  private static final String COLLECTION = "tolerant-cpu";
  private static final String HANDLER = "/fixedcpu";
  private static final String FAIL_SHARD_PARAM = "failShard";
  private static final String DOC_ID = "1";
  private static final long SHARD_CPU_MS = 1000L;

  /** Reports a fixed shard cpuTime, or fails on the shard named by {@link #FAIL_SHARD_PARAM}. */
  public static class FixedCpuComponent extends SearchComponent {
    @Override
    public void prepare(ResponseBuilder rb) {}

    @Override
    public void process(ResponseBuilder rb) {
      String shard = rb.req.getCore().getCoreDescriptor().getCloudDescriptor().getShardId();
      if (shard.equals(rb.req.getParams().get(FAIL_SHARD_PARAM))) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "failing " + shard);
      }
      rb.rsp.getResponseHeader().add(ThreadCpuTimer.CPU_TIME, SHARD_CPU_MS);
    }

    @Override
    public String getDescription() {
      return "fixed cpuTime";
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
                    + "', 'last-components': ['fixedcpu']}}"),
            COLLECTION);

    new UpdateRequest().add(sdoc("id", DOC_ID)).commit(cluster.getSolrClient(), COLLECTION);
  }

  public void testSinglePass() throws Exception {
    assertCpuTime(true, SHARD_CPU_MS);
  }

  public void testTwoPhase() throws Exception {
    // the live shard gets a GET_TOP_IDS and a GET_FIELDS request
    assertCpuTime(false, 2 * SHARD_CPU_MS);
  }

  private void assertCpuTime(boolean singlePass, long expected) throws Exception {
    DocCollection coll = getCollectionState(COLLECTION);
    String liveShard = coll.getRouter().getTargetSlice(DOC_ID, null, null, null, coll).getName();
    String failShard =
        coll.getSlices().stream()
            .map(Slice::getName)
            .filter(s -> !s.equals(liveShard))
            .findFirst()
            .orElseThrow();

    SolrQuery query = new SolrQuery("*:*");
    query.setRequestHandler(HANDLER);
    query.set(ShardParams.SHARDS_TOLERANT, true);
    query.set(ShardParams.DISTRIB_SINGLE_PASS, singlePass);
    query.set(FAIL_SHARD_PARAM, failShard);

    QueryResponse rsp = cluster.getSolrClient().query(COLLECTION, query);

    assertEquals(1, rsp.getResults().getNumFound());
    assertEquals(Boolean.TRUE, rsp.getHeader().get("partialResults"));
    assertEquals(List.of(expected), rsp.getHeader().getAll(ThreadCpuTimer.CPU_TIME));
  }
}
