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

import java.io.IOException;
import java.util.ArrayList;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CombinerParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

/** The type Combined query search handler test. */
public class CombinedQuerySearchHandlerTest extends SolrTestCaseJ4 {

  /**
   * Before tests.
   *
   * @throws Exception the exception
   */
  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  /** Test combined component init in search components list. */
  @Test
  public void testCombinedComponentInit() {
    SolrCore core = h.getCore();

    try (CombinedQuerySearchHandler handler = new CombinedQuerySearchHandler()) {
      handler.init(new NamedList<>());
      handler.inform(core);
      assertEquals(9, handler.getComponents().size());
      assertEquals(
          core.getSearchComponent(CombinedQueryComponent.COMPONENT_NAME),
          handler.getComponents().getFirst());
    } catch (IOException e) {
      fail("Exception when closing CombinedQuerySearchHandler");
    }
  }

  /** Test combined response buildr type create dynamically. */
  @Test
  public void testCombinedResponseBuilder() {
    SolrQueryRequest request = req("q", "testQuery");
    try (CombinedQuerySearchHandler handler = new CombinedQuerySearchHandler()) {
      assertFalse(
          handler.newResponseBuilder(request, new SolrQueryResponse(), new ArrayList<>())
              instanceof CombinedQueryResponseBuilder);
      request = req("q", "testQuery", CombinerParams.COMBINER, "true");
      assertTrue(
          handler.newResponseBuilder(request, new SolrQueryResponse(), new ArrayList<>())
              instanceof CombinedQueryResponseBuilder);
    } catch (IOException e) {
      fail("Exception when closing CombinedQuerySearchHandler");
    }
  }
}
