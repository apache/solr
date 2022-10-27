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
package org.apache.solr.client.solrj.embedded;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import org.apache.solr.core.SolrCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestEmbeddedSolrServer extends AbstractEmbeddedSolrServerTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  protected EmbeddedSolrServer getSolrCore1() {
    return new EmbeddedSolrServer(cores, "core1");
  }

  public void testGetCoreContainer() {
    assertEquals(cores, ((EmbeddedSolrServer) getSolrCore0()).getCoreContainer());
    assertEquals(cores, (getSolrCore1()).getCoreContainer());
  }

  public void testClose() throws IOException {

    EmbeddedSolrServer solrServer = (EmbeddedSolrServer) getSolrCore0();

    assertEquals(3, cores.getCores().size());
    List<SolrCore> solrCores = new ArrayList<>();
    for (SolrCore solrCore : cores.getCores()) {
      assertFalse(solrCore.isClosed());
      solrCores.add(solrCore);
    }

    solrServer.close();

    assertEquals(3, cores.getCores().size());

    for (SolrCore solrCore : solrCores) {
      assertFalse(solrCore.isClosed());
    }
  }
}
