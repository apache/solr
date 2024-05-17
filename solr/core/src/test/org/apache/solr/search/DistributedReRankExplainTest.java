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

package org.apache.solr.search;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SolrTestCaseJ4.SuppressSSL
public class DistributedReRankExplainTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int numShards = 2;
  private static final String COLLECTIONORALIAS = "collection1";

  private static final int TIMEOUT = DEFAULT_TIMEOUT;
  private static final String id = "id";

  private static boolean useAlias;

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("managed.schema.mutable", "true");
    String collection = COLLECTIONORALIAS;
    configureCluster(2)
        .addConfig(
            "conf1", TEST_PATH().resolve("configsets").resolve("cloud-managed").resolve("conf"))
        .configure();
    CollectionAdminRequest.createCollection(collection, "conf1", 2, 1)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(collection, 2, 2);

    CloudSolrClient client = cluster.getSolrClient();
    UpdateRequest updateRequest = new UpdateRequest();
    for (int i = 0; i < 100; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(CommonParams.ID, Integer.toString(i));
      doc.addField("test_s", "hello");
      updateRequest.add(doc);
    }
    updateRequest.process(client, COLLECTIONORALIAS);
    client.commit(COLLECTIONORALIAS);
  }

  @Test
  public void testDebugTrue() throws Exception {
    doTestReRankExplain(params(CommonParams.DEBUG_QUERY, "true"));
    doTestReRankExplain(params(CommonParams.DEBUG, "true"));
  }

  @Test
  public void testDebugAll() throws Exception {
    doTestReRankExplain(params(CommonParams.DEBUG, "all"));
  }

  @Test
  public void testDebugResults() throws Exception {
    doTestReRankExplain(params(CommonParams.DEBUG, CommonParams.RESULTS));
  }

  private void doTestReRankExplain(final SolrParams debugParams) throws Exception {
    final String reRankMainScale =
        "{!rerank reRankDocs=10 reRankMainScale=0-10 reRankQuery='test_s:hello'}";
    final String reRankScale =
        "{!rerank reRankDocs=10 reRankScale=0-10 reRankQuery='test_s:hello'}";

    { // multi-pass reRankMainScale
      final QueryResponse queryResponse =
          doQueryAndCommonChecks(
              SolrParams.wrapDefaults(params(CommonParams.RQ, reRankMainScale), debugParams));
      final Map<String, Object> debug = queryResponse.getDebugMap();
      assertNotNull(debug);
      final String explain = debug.get("explain").toString();
      assertThat(explain, containsString("ReRank Scaling effects unkown"));
    }

    { // single-pass reRankMainScale
      final QueryResponse queryResponse =
          doQueryAndCommonChecks(
              SolrParams.wrapDefaults(
                  params(CommonParams.RQ, reRankMainScale, ShardParams.DISTRIB_SINGLE_PASS, "true"),
                  debugParams));
      final Map<String, Object> debug = queryResponse.getDebugMap();
      assertNotNull(debug);
      final String explain = debug.get("explain").toString();
      assertThat(
          explain,
          containsString("5.0101576 = combined scaled first and unscaled second pass score "));
      assertThat(explain, not(containsString("ReRank Scaling effects unkown")));
    }

    { // multi-pass reRankMainScale
      final QueryResponse queryResponse =
          doQueryAndCommonChecks(
              SolrParams.wrapDefaults(params(CommonParams.RQ, reRankScale), debugParams));
      final Map<String, Object> debug = queryResponse.getDebugMap();
      assertNotNull(debug);
      final String explain = debug.get("explain").toString();
      assertThat(explain, containsString("ReRank Scaling effects unkown"));
    }

    { // single-pass reRankMainScale
      final QueryResponse queryResponse =
          doQueryAndCommonChecks(
              SolrParams.wrapDefaults(
                  params(CommonParams.RQ, reRankScale, ShardParams.DISTRIB_SINGLE_PASS, "true"),
                  debugParams));
      final Map<String, Object> debug = queryResponse.getDebugMap();
      assertNotNull(debug);
      final String explain = debug.get("explain").toString();
      assertThat(
          explain,
          containsString("10.005078 = combined unscaled first and scaled second pass score "));
      assertThat(explain, not(containsString("ReRank Scaling effects unkown")));
    }
  }

  private QueryResponse doQueryAndCommonChecks(final SolrParams params) throws Exception {
    final CloudSolrClient client = cluster.getSolrClient();
    final QueryRequest queryRequest =
        new QueryRequest(
            SolrParams.wrapDefaults(
                params, params(CommonParams.Q, "test_s:hello", "fl", "id,test_s,score")));

    final QueryResponse queryResponse = queryRequest.process(client, COLLECTIONORALIAS);
    assertNotNull(queryResponse.getResults().get(0).getFieldValue("test_s"));
    return queryResponse;
  }
}
