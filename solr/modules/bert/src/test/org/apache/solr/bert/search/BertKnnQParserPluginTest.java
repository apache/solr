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
package org.apache.solr.search.neural;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BertKnnQParserPluginTest extends SolrTestCaseJ4 {

  String IDField = "id";
  String vectorField = "vector";

  @Before
  public void prepareIndex() throws Exception {
    initCore("solrconfig-bert.xml", "schema-densevector.xml");
    // TODO: add documents
  }

  @After
  public void cleanUp() {
    // TODO: delete documents
    deleteCore();
  }

  @Test
  public void testBert1() {
    assertQ(req(CommonParams.Q, "{!bert1 f=vector}foobar", "fl", "id"), "//result[@numFound='0']");
  }

  @Test
  public void testBert2() {
    assertQ(req(CommonParams.Q, "{!bert2 f=vector}foobar", "fl", "id"), "//result[@numFound='0']");
  }
}
