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

package org.apache.solr.client.solrj.response;

import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.response.json.NestableJsonFacet;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

public class NestableJsonFacetTest extends SolrTestCaseJ4 {

  @Test
  public void testParsing() {
    NamedList<Object> list = new NamedList<>();
    list.add("count", 12);
    NamedList<Object> buckets = new NamedList<>();
    buckets.add("val", "Nike");
    NamedList<Object> vals = new NamedList<>();
    vals.add("numBuckets", 10);
    vals.add("allBuckets", countList(12));
    vals.add("before", countList(1));
    vals.add("after", countList(2));
    vals.add("between", countList(9));
    vals.add("buckets", List.of(buckets));
    list.add("test", vals);
    NestableJsonFacet facet = new NestableJsonFacet(list);

    assertEquals(12L, facet.getCount());
    assertEquals(9L, facet.getBucketBasedFacets("test").getBetween());
    list.clear();

    list.add("count", 12L);
    buckets = new NamedList<>();
    buckets.add("val", "Nike");
    vals = new NamedList<>();
    vals.add("numBuckets", 10L);
    vals.add("allBuckets", countList(12L));
    vals.add("before", countList(1L));
    vals.add("after", countList(2L));
    vals.add("between", countList(9L));
    vals.add("buckets", List.of(buckets));
    list.add("test", vals);
    facet = new NestableJsonFacet(list);
    assertEquals(12L, facet.getCount());
    assertEquals(2L, facet.getBucketBasedFacets("test").getAfter());
  }

  private static NamedList<Object> countList(Object count) {
    NamedList<Object> list = new NamedList<>();
    list.add("count", count);
    return list;
  }
}
