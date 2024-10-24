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
package org.apache.solr.search.function;

import java.util.Collections;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.facet.FacetField;
import org.apache.solr.search.facet.FacetParser;
import org.apache.solr.search.facet.FacetRequest;
import org.junit.BeforeClass;

public class CustomParseHandlerTest extends SolrTestCaseJ4 {

  public static final FacetField FACET_FIELD_STUB = new FacetField();

  static class CustomParseHandler implements FacetParser.ParseHandler {
    @Override
    public Object parse(FacetParser<?> parent, String key, Object args) {
      assertEquals("arg", args);
      return FACET_FIELD_STUB;
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    FacetParser.registerParseHandler("custom", new CustomParseHandler());
  }

  public void testCustomParseHandler() {
    SolrQueryRequest req = req();
    ResponseBuilder rsp =
        new ResponseBuilder(req, new SolrQueryResponse(), Collections.emptyList());

    FacetRequest facetRequest =
        FacetRequest.parse(rsp.req, Map.of("bogus", Map.of("custom", "arg")));
    assertEquals(FACET_FIELD_STUB, facetRequest.getSubFacets().get("bogus"));
  }
}
