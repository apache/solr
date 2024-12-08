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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cluster.api.SimpleMap;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.handler.LoggingStream;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;

@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40", "Lucene41", "Lucene42", "Lucene45"})
public class UBIComponentDistrQueriesTest extends SolrCloudTestCase {

  private static final String COLLECTIONORALIAS = "collection1";
  private static final int TIMEOUT = DEFAULT_TIMEOUT;
  private static final String id = "id";

  private static boolean useAlias;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(4)
        .addConfig(
            "conf", TEST_PATH().resolve("configsets").resolve("ubi-enabled").resolve("conf"))
        .configure();

    String collection;
    useAlias = false; //random().nextBoolean();
    if (useAlias) {
      collection = COLLECTIONORALIAS + "_collection";
    } else {
      collection = COLLECTIONORALIAS;
    }

    CollectionAdminRequest.createCollection(collection, "conf", 2, 1)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(collection, 2, 2);

    AbstractDistribZkTestBase.waitForRecoveriesToFinish(
        collection, cluster.getZkStateReader(), false, true, TIMEOUT);
    if (useAlias) {
      CollectionAdminRequest.createAlias(COLLECTIONORALIAS, collection)
          .process(cluster.getSolrClient());
    }

    // -------------------

    CollectionAdminRequest.createCollection("ubi_queries",// it seems like a hardcoded name why?
                    "_default", 1, 1)
            .process(cluster.getSolrClient());

    cluster.waitForActiveCollection("ubi_queries", 1, 1);

    AbstractDistribZkTestBase.waitForRecoveriesToFinish(
            "ubi_queries", cluster.getZkStateReader(), false, true, TIMEOUT);
  }

  @Before
  public void cleanIndex() throws Exception {
    new UpdateRequest().deleteByQuery("*:*").commit(cluster.getSolrClient(), COLLECTIONORALIAS);
  }

  @Test
  public void testUBIQueryStream() throws Exception {
    cluster.getSolrClient(COLLECTIONORALIAS).add(List.of(new SolrInputDocument("id", "1", "subject", "aa"),
            new SolrInputDocument("id", "2" /*"two"*/, "subject", "aa"),
            new SolrInputDocument("id", "3", "subject", "aa")));
    cluster.getSolrClient(COLLECTIONORALIAS).commit(true, true);
    QueryResponse queryResponse = cluster.getSolrClient(COLLECTIONORALIAS).query(new MapSolrParams(
            Map.of("q", "aa", "df","subject", "rows", "2", "ubi", "true"
            )));
    String qid = (String) ((SimpleMap<?>) queryResponse.getResponse().get("ubi")).get("query_id");
    assertTrue(qid.length()>10);
    Thread.sleep(10000); // I know what you think of
    // TODO check that ids were recorded
    QueryResponse queryCheck = cluster.getSolrClient("ubi_queries").query(new MapSolrParams(
            Map.of("q", "id:"+qid //doesn't search it why? is it a race?
            )));
    // however I can't see doc ids found there. Shouldn't I ?
    assertEquals(1L, queryCheck.getResults().getNumFound());
    assertEquals(queryCheck.getResults().get(0).get("id"),qid);
  }
}
