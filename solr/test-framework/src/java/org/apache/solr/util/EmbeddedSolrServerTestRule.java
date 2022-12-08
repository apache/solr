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
package org.apache.solr.util;

import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.SolrTestCaseJ4;

public class EmbeddedSolrServerTestRule extends SolrClientTestRule {

  public static final String DEFAULT_CORE_NAME = "collection1";

  @Override
  protected void before() throws Throwable {
    initCore();
  }

  public EmbeddedSolrServerTestRule(String solrHome) {
    //super(solrHome); To Do
  }


  public static void initCore() throws Exception {
    final String home = SolrJettyTestBase.legacyExampleCollection1SolrHome();
    final String config = home + "/" + DEFAULT_CORE_NAME + "/conf/solrconfig.xml";
    final String schema = home + "/" + DEFAULT_CORE_NAME + "/conf/schema.xml";
    SolrTestCaseJ4.initCore(config, schema, home);
  }
}
