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

package org.apache.solr.update;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.SolrCore;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Runs {@link PeerSync#sync()} against a replica whose core failed to load, over real HTTP, so the
 * versions request's error travels through the server's response writer and the client's exception
 * handling before {@link PeerSync} decides.
 */
public class PeerSyncCoreInitFailureHttpTest extends SolrTestCaseJ4 {

  private JettySolrRunner jetty;

  @Before
  public void startNodeWithBrokenCore() throws Exception {
    Path solrHome = createTempDir();
    copyMinConf(solrHome.resolve("good"), "name=good", "solrconfig.xml");
    copyMinConf(solrHome.resolve("bad"), "name=bad", "solrconfig.xml");

    Path badSchema = solrHome.resolve("bad").resolve("conf").resolve("schema.xml");
    Files.writeString(
        badSchema,
        Files.readString(badSchema, StandardCharsets.UTF_8).replace("<field", "<filed"),
        StandardCharsets.UTF_8);

    jetty = new JettySolrRunner(solrHome.toString(), JettyConfig.builder().build());
    jetty.start();

    try (HttpJettySolrClient client =
        new HttpJettySolrClient.Builder(jetty.getBaseUrl() + "/good").build()) {
      for (int i = 0; i < 3; i++) {
        client.add(new SolrInputDocument("id", "doc" + i));
      }
    }
  }

  @After
  public void stopNode() throws Exception {
    if (jetty != null) {
      jetty.stop();
    }
  }

  @Test
  public void testCoreInitFailureOnVersionsRequestIsSuccessWhenCantReachIsSuccess()
      throws Exception {
    assertTrue(syncAgainstBrokenCore(true).isSuccess());
  }

  @Test
  public void testCoreInitFailureOnVersionsRequestIsFailureWhenCantReachIsNotSuccess()
      throws Exception {
    assertFalse(syncAgainstBrokenCore(false).isSuccess());
  }

  private PeerSync.PeerSyncResult syncAgainstBrokenCore(boolean cantReachIsSuccess)
      throws Exception {
    try (SolrCore good = jetty.getCoreContainer().getCore("good");
        PeerSync peerSync =
            new PeerSync(
                good, List.of(jetty.getBaseUrl() + "/bad"), 10, cantReachIsSuccess, false, false)) {
      return peerSync.sync();
    }
  }
}
