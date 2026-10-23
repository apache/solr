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

import org.apache.solr.SolrTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.QueryElevationParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.search.QueryParsing;
import org.junit.Test;

public class QueryElevationComponentParamTest extends SolrTestCase {

  @Test
  public void testElevateQOverridesRequestQuery() {
    SolrParams params = new ModifiableSolrParams().set(QueryElevationParams.ELEVATE_Q, "simple");

    assertEquals(
        "simple", QueryElevationComponent.getElevationQueryString(params, null, "complex query"));
  }

  @Test
  public void testElevateQOverridesLocalParserQuery() {
    SolrParams params = new ModifiableSolrParams().set(QueryElevationParams.ELEVATE_Q, "simple");
    SolrParams localParams = new ModifiableSolrParams().set(QueryParsing.V, "parser query");

    assertEquals(
        "simple",
        QueryElevationComponent.getElevationQueryString(params, localParams, "complex query"));
  }

  @Test
  public void testLocalParserQueryRemainsDefault() {
    SolrParams params = new ModifiableSolrParams();
    SolrParams localParams = new ModifiableSolrParams().set(QueryParsing.V, "parser query");

    assertEquals(
        "parser query",
        QueryElevationComponent.getElevationQueryString(params, localParams, "complex query"));
  }

  @Test
  public void testRequestQueryRemainsDefaultWithoutLocalParams() {
    assertEquals(
        "complex query",
        QueryElevationComponent.getElevationQueryString(
            new ModifiableSolrParams(), null, "complex query"));
  }
}
