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
package org.apache.solr.handler.admin.api;

import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.solr.client.api.model.NodeSystemInfoResponse;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.InputStreamResponseParser;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;

/** Test {@link GetNodeSystemInfo}. */
public class GetNodeSystemInfoTest extends SolrCloudTestCase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-minimal.xml", "schema-minimal.xml");
  }

  public void testGetNodeInfo() throws Exception {
    SolrQueryRequest req = new SolrQueryRequestBase(h.getCore(), new ModifiableSolrParams()) {};
    SolrQueryResponse resp = new SolrQueryResponse();

    GetNodeSystemInfo getter = new GetNodeSystemInfo(req, resp);

    NodeSystemInfoResponse response = getter.getNodeSystemInfo();
    Assert.assertNotNull(response.nodesInfo);
    Assert.assertEquals(1, response.nodesInfo.size());

    NodeSystemInfoResponse.NodeSystemInfo info =
        response.nodesInfo.values().stream().findFirst().orElseThrow();
    Assert.assertTrue(info.coreRoot != null);
    Assert.assertEquals(h.getCoreContainer().getCoreRootDirectory().toString(), info.coreRoot);
    // other validations in NodeSystemInfoProviderTest
  }

  @Ignore
  public void testGetAllNodesInfo() throws Exception {
    // SystemInfoRequestTest ?? SolrJ
    // beforeClass
    configureCluster(2).addConfig("conf", configset("cloud-minimal")).configure();
    CollectionAdminRequest.createCollection(DEFAULT_TEST_COLLECTION_NAME, "conf", 2, 1)
        .process(cluster.getSolrClient());
    URL baseUrl = cluster.getJettySolrRunner(0).getBaseURLV2();
    // test
    HttpGet get = new HttpGet(baseUrl.toString() + "/node/info/system");
    try (CloseableHttpClient client = HttpClientBuilder.create().build();
        CloseableHttpResponse response = client.execute(get)) {
      try (InputStream in = response.getEntity().getContent()) {
        NamedList<Object> nl =
            InputStreamResponseParser.createInputStreamNamedList(
                response.getStatusLine().getStatusCode(), in);
      }
    }

    SolrQueryRequest req =
        new SolrQueryRequestBase(
            h.getCore(), new ModifiableSolrParams(Map.of("nodes", new String[] {"all"}))) {};
    SolrQueryResponse resp = new SolrQueryResponse();
    GetNodeSystemInfo getter = new GetNodeSystemInfo(req, resp);

    NodeSystemInfoResponse response = getter.getNodeSystemInfo();
    Assert.assertNotNull(response.nodesInfo);
    Assert.assertEquals(2, response.nodesInfo.size());

    response
        .nodesInfo
        .entrySet()
        .forEach(
            e -> {
              String key = e.getKey();
              NodeSystemInfoResponse.NodeSystemInfo info = e.getValue();
              Assert.assertEquals(key, info.node);
            });
  }
}
