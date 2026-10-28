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
package org.apache.solr.search.stats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.response.Group;
import org.apache.solr.client.solrj.response.GroupCommand;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Ignore;

@Ignore("Abstract calls should not executed as test")
public abstract class TestBaseStatsCache extends TestDefaultStatsCache {

  protected abstract String getStatsCacheClassName();

  @Override
  public void distribSetUp() throws Exception {
    super.distribSetUp();
    System.setProperty("solr.statsCache", getStatsCacheClassName());
  }

  @Override
  public void distribTearDown() throws Exception {
    super.distribTearDown();
  }

  // in this case, as the number of shards increases, per-shard scores should
  // remain identical
  @Override
  protected void checkResponse(QueryResponse controlRsp, QueryResponse shardRsp) {
    System.out.println("======================= Control Response =======================");
    System.out.println(controlRsp);
    System.out.println("");
    System.out.println("");
    System.out.println("======================= Shard Response =======================");
    System.out.println("");
    System.out.println(shardRsp);
    SolrDocumentList shardList = shardRsp.getResults();
    SolrDocumentList controlList = controlRsp.getResults();

    assertEquals(controlList.size(), shardList.size());

    assertEquals(controlList.getNumFound(), shardList.getNumFound());
    Iterator<SolrDocument> it = controlList.iterator();
    Iterator<SolrDocument> it2 = shardList.iterator();
    while (it.hasNext()) {
      SolrDocument controlDoc = it.next();
      SolrDocument shardDoc = it2.next();
      assertEquals(controlDoc.getFieldValue("score"), shardDoc.getFieldValue("score"));
    }
  }

  @Override
  protected void checkDistribStatsException() {
    // doing nothing on distrib stats
  }

  // grouped queries must be scored with the same global stats as the ungrouped ones
  @Override
  protected void checkGroupedScores() throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "a_t:one a_t:four");
    params.set("fl", "id,score");
    params.set("group", "true");
    params.set("group.field", "id");
    params.set("group.limit", "1");

    Map<String, Object> controlScores = topScorePerGroup(controlClient.query(params));
    params.set("shards", shards);
    SolrClient client = clients.get(r.nextInt(clients.size()));
    Map<String, Object> shardScores = topScorePerGroup(client.query(params));

    assertFalse(controlScores.isEmpty());
    assertEquals(controlScores, shardScores);
    checkWithinGroupOrder();
  }

  // the main sort has no score, so the within-group score ranking must get global stats too
  private void checkWithinGroupOrder() throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "a_t:two a_t:five");
    params.set("sort", "id asc");
    params.set("fl", "id");
    params.set("group", "true");
    params.set("group.field", "grp_s1");
    params.set("group.sort", "score desc,id asc");
    params.set("group.limit", "1");

    List<String> controlDocs = topDocIdPerGroup(controlClient.query(params));
    params.set("shards", shards);
    SolrClient client = clients.get(r.nextInt(clients.size()));
    List<String> shardDocs = topDocIdPerGroup(client.query(params));

    assertEquals(2, controlDocs.size());
    assertEquals(controlDocs, shardDocs);
  }

  private static Map<String, Object> topScorePerGroup(QueryResponse rsp) {
    Map<String, Object> scores = new HashMap<>();
    GroupCommand command = rsp.getGroupResponse().getValues().get(0);
    for (Group group : command.getValues()) {
      scores.put(group.getGroupValue(), group.getResult().get(0).getFieldValue("score"));
    }
    return scores;
  }

  private static List<String> topDocIdPerGroup(QueryResponse rsp) {
    List<String> docIds = new ArrayList<>();
    GroupCommand command = rsp.getGroupResponse().getValues().get(0);
    for (Group group : command.getValues()) {
      docIds.add(group.getGroupValue() + "=" + group.getResult().get(0).getFieldValue("id"));
    }
    return docIds;
  }
}
