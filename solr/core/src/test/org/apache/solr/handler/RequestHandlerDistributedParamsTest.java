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
package org.apache.solr.handler;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.params.ShardParams;

public class RequestHandlerDistributedParamsTest extends SolrTestCaseJ4 {

  private RequestHandlerBase handler = null;

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Before
  public void setUp() {
    handler = new RequestHandlerBase() {
        // empty implementation
        public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {

        }

        public String getDescription() {
          return "test";
        }
    };
  }

  @Test
  public void shouldAppendParamsInNonDistributedRequest() {
    // given
    NamedList<String> appends = new NamedList<>();
    appends.add("foo", "appended");
    NamedList<NamedList<String>> init = new NamedList<>();
    init.add("appends", appends);
    handler.init(init);

    // when
    SolrQueryRequest req = req();
    handler.handleRequest(req, new SolrQueryResponse());

    // then
    assertEquals(req.getParams().get("foo"), "appended");
  }

  @Test
  public void shouldNotAppendParamsInDistributedRequest() {
    // given
    NamedList<String> appends = new NamedList<>();
    appends.add("foo", "appended");
    NamedList<NamedList<String>> init = new NamedList<>();
    init.add("appends", appends);
    handler.init(init);

    // when
    SolrQueryRequest req = req(ShardParams.IS_SHARD, "true");
    handler.handleRequest(req, new SolrQueryResponse());

    // then
    assertNull(req.getParams().get("foo"));
  }
}
