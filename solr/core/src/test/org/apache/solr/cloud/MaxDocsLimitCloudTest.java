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

package org.apache.solr.cloud;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import org.apache.lucene.index.IndexWriter;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.util.ErrorLogMuter;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * SOLR-6065: on a single-shard collection whose Lucene index is at its document limit, the client
 * receives the clean max-docs error (a 500 explaining the limit), not the generic analysis-error
 * wording. The single-core half of this behavior is covered by {@code
 * DirectUpdateHandlerTest.testMaxDocsExceededGivesClearError}.
 */
public class MaxDocsLimitCloudTest extends SolrCloudTestCase {

  private String collection;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig(
            "config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    collection = getSaferTestName();
  }

  /**
   * Sets Lucene's per-index document limit for the duration of a test. {@code
   * IndexWriter.setMaxDocs} is a test-only setter that Lucene keeps package-private, so it is
   * reached through a method handle. The limit is JVM-wide, so callers must restore {@link
   * IndexWriter#MAX_DOCS} in a finally block.
   */
  private static void setMaxDocs(int maxDocs) throws Exception {
    MethodHandle setter =
        MethodHandles.privateLookupIn(IndexWriter.class, MethodHandles.lookup())
            .findStatic(
                IndexWriter.class, "setMaxDocs", MethodType.methodType(void.class, int.class));
    try {
      setter.invoke(maxDocs);
    } catch (Throwable t) {
      throw new Exception("could not set the Lucene max-docs limit for this test", t);
    }
  }

  @Test
  @SuppressWarnings("try")
  public void testMaxDocsExceededGivesClearErrorToClient() throws Exception {
    setMaxDocs(3);
    try {
      CollectionAdminRequest.createCollection(collection, "config", 1, 1)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(collection, 1, 1);

      Replica leader = getCollectionState(collection).getLeader("shard1");
      try (SolrClient leaderClient = new HttpJettySolrClient.Builder(leader.getBaseUrl()).build()) {
        new UpdateRequest()
            .add("id", "1")
            .add("id", "2")
            .add("id", "3")
            .commit(leaderClient, leader.getCoreName());

        RemoteSolrException e =
            expectThrows(
                RemoteSolrException.class,
                () -> {
                  try (ErrorLogMuter ignored = ErrorLogMuter.regex("maximum number of documents")) {
                    new UpdateRequest().add("id", "4").process(leaderClient, leader.getCoreName());
                  }
                });
        // a full index is a server-side capacity limit, not a malformed request
        assertEquals(500, e.code());
        assertTrue(e.getMessage(), e.getMessage().contains("maximum number of documents"));
        assertFalse(e.getMessage(), e.getMessage().contains("analysis error"));

        // the index must stay usable
        QueryResponse queryResponse =
            new QueryRequest(new SolrQuery("*:*")).process(leaderClient, leader.getCoreName());
        assertEquals(3, queryResponse.getResults().getNumFound());
      }
    } finally {
      setMaxDocs(IndexWriter.MAX_DOCS);
      CollectionAdminRequest.deleteCollection(collection).process(cluster.getSolrClient());
    }
  }
}
