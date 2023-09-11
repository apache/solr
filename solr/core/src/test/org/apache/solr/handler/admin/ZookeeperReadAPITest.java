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

import static org.apache.solr.common.util.StrUtils.split;
import static org.apache.solr.common.util.Utils.getObjectByPath;
import static org.hamcrest.Matchers.containsInAnyOrder;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URL;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Integration tests for {@link ZookeeperReadAPI} */
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
    basezk = baseUrl.toString().replace("/solr", "/api") + "/cluster/zookeeper/data";
    basezkls = baseUrl.toString().replace("/solr", "/api") + "/cluster/zookeeper/children";
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void testZkread() throws Exception {
    try (HttpSolrClient client = new HttpSolrClient.Builder(baseUrl.toString()).build()) {
      Object o =
          Utils.executeGET(client.getHttpClient(), basezk + "/security.json", Utils.JSONCONSUMER);
      assertNotNull(o);
      o = Utils.executeGET(client.getHttpClient(), basezkls + "/configs", Utils.JSONCONSUMER);
      assertEquals(
          "0",
          String.valueOf(getObjectByPath(o, true, split(":/configs:_default:dataLength", ':'))));
      assertEquals(
          "0", String.valueOf(getObjectByPath(o, true, split(":/configs:conf:dataLength", ':'))));
      assertEquals("0", String.valueOf(getObjectByPath(o, true, split("/stat/version", '/'))));

      o = Utils.executeGET(client.getHttpClient(), basezk + "/configs", Utils.JSONCONSUMER);
      assertTrue(((Map) o).containsKey("zkData"));
      assertEquals("empty", ((Map) o).get("zkData"));

      byte[] bytes = new byte[1024 * 5];
      for (int i = 0; i < bytes.length; i++) {
        bytes[i] = (byte) random().nextInt(128);
      }
      try {
        cluster
            .getZkClient()
            .create("/configs/_default/testdata", bytes, CreateMode.PERSISTENT, true);
        Utils.executeGET(
            client.getHttpClient(),
            basezk + "/configs/_default/testdata",
            is -> {
              byte[] newBytes = new byte[bytes.length];
              is.read(newBytes);
              for (int i = 0; i < newBytes.length; i++) {
                assertEquals(bytes[i], newBytes[i]);
              }
              return null;
            });
      } finally {
        cluster.getZkClient().delete("/configs/_default/testdata", -1, true);
      }
    }
  }

  @Test
  public void testRequestingDataFromNonexistentNodeReturnsAnError() throws Exception {
    try (HttpSolrClient client = new HttpSolrClient.Builder(baseUrl.toString()).build()) {
      final SolrException expected =
          expectThrows(
              SolrException.class,
              () -> {
                Utils.executeGET(
                    client.getHttpClient(),
                    basezk + "/configs/_default/nonexistentnode",
                    Utils.JSONCONSUMER);
              });
      assertEquals(404, expected.code());
    }
  }

  @Test
  public void testCanListChildNodes() throws Exception {
    try (HttpSolrClient client = new HttpSolrClient.Builder(baseUrl.toString()).build()) {
      final ZookeeperReadAPI.ListZkChildrenResponse response =
          Utils.executeGET(
              client.getHttpClient(),
              basezkls + "/configs/_default",
              is -> {
                return new ObjectMapper()
                    .readValue(is, ZookeeperReadAPI.ListZkChildrenResponse.class);
              });

      // At the top level, the response contains a key with the value of the specified zkPath
      assertEquals(1, response.unknownProperties().size());
      assertEquals(
          "/configs/_default",
          response.unknownProperties().keySet().stream().collect(Collectors.toList()).get(0));

      // Under the specified zkPath is a key for each child, with values being that stat for that
      // node.
      // The actual stat values vary a good bit so aren't very useful to assert on, so let's just
      // make sure all of the expected child nodes were found.
      final Map<String, ZookeeperReadAPI.AnnotatedStat> childStatsByPath =
          response.unknownProperties().get("/configs/_default");
      assertEquals(6, childStatsByPath.size());
      MatcherAssert.assertThat(
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
