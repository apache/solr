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

import java.util.ArrayList;
import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Directly drives {@link SuggestComponent#finishStage} with fabricated shard responses (no real
 * multi-shard cluster needed) to prove how {@code suggesterStale} is aggregated across shards in
 * SolrCloud: it's a single boolean, true if *any* shard reports *any* suggester as built from an
 * older index version than that shard's own current one - since per-shard/per-core index versions
 * aren't comparable with each other, this is deliberately a yes/no signal rather than raw numbers.
 */
public class SuggestComponentFinishStageStaleFlagTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void testSuggesterStaleIsTrueIfAnyShardIsStale() throws Exception {
    assumeWorkingMockito();
    assertTrue(
        "expected suggesterStale=true when at least one shard reports a stale suggester",
        runFinishStage(shardResponse("shard1", false), shardResponse("shard2", true)));
  }

  @Test
  public void testSuggesterStaleIsFalseWhenEveryShardIsFresh() throws Exception {
    assumeWorkingMockito();
    assertFalse(
        "expected suggesterStale=false when every shard reports a fresh suggester",
        runFinishStage(shardResponse("shard1", false), shardResponse("shard2", false)));
  }

  /** Runs finishStage() with the given canned shard responses and returns the suggesterStale. */
  private boolean runFinishStage(ShardResponse... shardResponses) throws Exception {
    SuggestComponent component = new SuggestComponent();
    SolrQueryRequest req = req("suggest", "true", "suggest.count", "5");
    try {
      SolrQueryResponse rsp = new SolrQueryResponse();
      List<SearchComponent> components = new ArrayList<>();
      components.add(component);
      ResponseBuilder rb = new ResponseBuilder(req, rsp, components);
      rb.setStage(ResponseBuilder.STAGE_GET_FIELDS);

      ShardRequest sreq = new ShardRequest();
      sreq.responses = new ArrayList<>();
      for (ShardResponse shardResponse : shardResponses) {
        sreq.responses.add(shardResponse);
      }
      rb.finished = new ArrayList<>();
      rb.finished.add(sreq);

      component.finishStage(rb);

      Object staleObj = rsp.getValues().get("suggesterStale");
      assertNotNull("expected a suggesterStale field in the response", staleObj);
      return (Boolean) staleObj;
    } finally {
      req.close();
    }
  }

  /** Builds a fake shard response shaped like a real SuggestComponent.process() response. */
  private static ShardResponse shardResponse(String shard, boolean stale) {
    SimpleOrderedMap<Object> suggest = new SimpleOrderedMap<>(); // no suggestions needed for this

    SimpleOrderedMap<Object> versionEntry = new SimpleOrderedMap<>();
    versionEntry.add("builtFromIndexVersion", stale ? 1L : 2L);
    versionEntry.add("currentIndexVersion", 2L);
    versionEntry.add("stale", stale);
    SimpleOrderedMap<Object> indexVersions = new SimpleOrderedMap<>();
    indexVersions.add("mySuggester", versionEntry);

    NamedList<Object> response = new NamedList<>();
    response.add("suggest", suggest);
    response.add("suggesterIndexVersions", indexVersions);

    SolrResponse solrResponse = Mockito.mock(SolrResponse.class);
    Mockito.when(solrResponse.getResponse()).thenReturn(response);

    ShardResponse shardResponse = new ShardResponse();
    shardResponse.setSolrResponse(solrResponse);
    shardResponse.setShard(shard);
    return shardResponse;
  }
}
