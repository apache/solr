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

package org.apache.solr.handler.admin;

import static org.hamcrest.Matchers.containsInAnyOrder;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.solr.client.api.model.ZooKeeperStat;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.ZookeeperReadApi;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Integration tests for {@link ZookeeperRead} */
public class ZookeeperReadAPITest extends SolrCloudTestCase {
  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();
  }

  private URL baseUrl;
  private String basezk;
  private String basezkls;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    baseUrl = cluster.getJettySolrRunner(0).getBaseUrl();

    String baseUrlV2 = cluster.getJettySolrRunner(0).getBaseURLV2().toString();
    basezk = baseUrlV2 + "/cluster/zookeeper/data";
    basezkls = baseUrlV2 + "/cluster/zookeeper/children";
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void testZkread() throws Exception {
    try (HttpSolrClient client = new HttpSolrClient.Builder(baseUrl.toString()).build()) {
      final var securityJsonRequest = new ZookeeperReadApi.ReadNode("/security.json");
      final var securityJsonResponse = securityJsonRequest.process(client);
      assertEquals(200, securityJsonResponse.getHttpStatus());
      try (final var stream = securityJsonResponse.getResponseStream()) {
        final var securityJsonContent = IOUtils.toString(stream, StandardCharsets.UTF_8);
        assertNotNull(securityJsonContent);
      }

      final var configListRequest = new ZookeeperReadApi.ListNodes("/configs");
      final var configListResponse = configListRequest.process(client).getParsed();
      assertEquals(
          16, configListResponse.unknownProperties().get("/configs").get("_default").dataLength);
      assertEquals(
          16, configListResponse.unknownProperties().get("/configs").get("conf").dataLength);
      assertEquals(0, configListResponse.stat.version);

      final var configDataRequest = new ZookeeperReadApi.ReadNode("/configs");
      final var configDataResponse = configDataRequest.process(client);
      // /configs exists but has no data, so API returns '200 OK' with empty response body
      assertEquals(200, configDataResponse.getHttpStatus());
      try (final var stream = configDataResponse.getResponseStream()) {
        assertEquals("", IOUtils.toString(stream, StandardCharsets.UTF_8));
      }

      byte[] bytes = new byte[1024 * 5];
      for (int i = 0; i < bytes.length; i++) {
        bytes[i] = (byte) random().nextInt(128);
      }
      try {
        cluster
            .getZkClient()
            .create("/configs/_default/testdata", bytes, CreateMode.PERSISTENT, true);

        final var testDataRequest = new ZookeeperReadApi.ReadNode("/configs/_default/testdata");
        final var testDataResponse = testDataRequest.process(client);
        assertEquals(200, testDataResponse.getHttpStatus());
        try (final var stream = testDataResponse.getResponseStream()) {
          final var foundContents = stream.readAllBytes();
          for (int i = 0; i < foundContents.length; i++) {
            assertEquals(foundContents[i], bytes[i]);
          }
        }
      } finally {
        cluster.getZkClient().delete("/configs/_default/testdata", -1, true);
      }
    }
  }

  @Test
  public void testRequestingDataFromNonexistentNodeReturnsAnError() throws Exception {
    try (HttpSolrClient client = new HttpSolrClient.Builder(baseUrl.toString()).build()) {
      final var missingNodeReq = new ZookeeperReadApi.ReadNode("/configs/_default/nonexistentnode");
      final var missingNodeResponse = missingNodeReq.process(client);
      assertEquals(404, missingNodeResponse.getHttpStatus());

      final var expected =
          expectThrows(
              SolrException.class, () -> missingNodeResponse.getResponseStreamIfSuccessful());
      assertEquals(404, expected.code());
    }
  }

  @Test
  public void testCanListChildNodes() throws Exception {
    try (HttpSolrClient client = new HttpSolrClient.Builder(baseUrl.toString()).build()) {
      final var listDefaultFilesReq = new ZookeeperReadApi.ListNodes("/configs/_default");
      final var listDefaultFilesResponse = listDefaultFilesReq.process(client).getParsed();

      // At the top level, the response contains a key with the value of the specified zkPath
      assertEquals(1, listDefaultFilesResponse.unknownProperties().size());
      assertEquals(
          "/configs/_default",
          listDefaultFilesResponse.unknownProperties().keySet().stream()
              .collect(Collectors.toList())
              .get(0));

      // Under the specified zkPath is a key for each child, with values being that stat for that
      // node.
      // The actual stat values vary a good bit so aren't very useful to assert on, so let's just
      // make sure all of the expected child nodes were found.
      final Map<String, ZooKeeperStat> childStatsByPath =
          listDefaultFilesResponse.unknownProperties().get("/configs/_default");
      assertEquals(6, childStatsByPath.size());
      assertThat(
          childStatsByPath.keySet(),
          containsInAnyOrder(
              "protwords.txt",
              "solrconfig.xml",
              "synonyms.txt",
              "stopwords.txt",
              "managed-schema.xml",
              "lang"));
    }
  }
}
