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
package org.apache.solr.search.combine;

import java.util.List;
import java.util.Map;
import org.apache.lucene.search.Explanation;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ShardDoc;

/**
 * The TestCombiner class is an extension of QueryAndResponseCombiner that implements custom logic
 * for combining ranked lists using linear sorting of score from all rank lists. This is just for
 * testing purpose which has been used in test suite CombinedQueryComponentTest for e2e testing of
 * Plugin based Combiner approach.
 */
public class TestCombiner extends QueryAndResponseCombiner {

  private int testInt;

  public int getTestInt() {
    return testInt;
  }

  @Override
  public void init(NamedList<?> args) {
    Object kParam = args.get("var1");
    if (kParam != null) {
      this.testInt = Integer.parseInt(kParam.toString());
    }
  }

  @Override
  public List<ShardDoc> combine(Map<String, List<ShardDoc>> shardDocMap, SolrParams solrParams) {
    return List.of();
  }

  @Override
  public SimpleOrderedMap<Explanation> getExplanations(
      String[] queryKeys,
      Map<String, List<ShardDoc>> queriesDocMap,
      List<ShardDoc> combinedQueriesDocs,
      SolrParams solrParams) {
    SimpleOrderedMap<Explanation> docIdsExplanations = new SimpleOrderedMap<>();
    docIdsExplanations.add("combinerDetails", Explanation.match(testInt, "this is test combiner"));
    return docIdsExplanations;
  }
}
