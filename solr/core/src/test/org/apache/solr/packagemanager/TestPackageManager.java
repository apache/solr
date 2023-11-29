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
package org.apache.solr.packagemanager;

import com.jayway.jsonpath.InvalidPathException;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.SolrZkClient;
import org.junit.Before;
import org.junit.Test;

public class TestPackageManager extends SolrCloudTestCase {

  private static final String COLLECTION_NAME = "collection1";

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    configureCluster(1).addConfig("conf", configset("conf3")).configure();
    CollectionAdminRequest.createCollection(COLLECTION_NAME, "conf", 1, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION_NAME, 1, 1);
  }

  @Test
  public void testWrongVerificationJPathIsThrown() throws IOException {
    SolrZkClient zkClient = cluster.getZkClient();
    URL baseURLV2 = cluster.getJettySolrRunner(0).getBaseURLV2();
    try (Http2SolrClient solrClient = new Http2SolrClient.Builder(baseURLV2.toString()).build()) {
      try (PackageManager manager = new StubPackageManager(solrClient, zkClient)) {
        SolrPackage.Plugin plugin = new SolrPackage.Plugin();
        if (random().nextBoolean()) {
          plugin.type = "cluster";
        }
        plugin.name = "foo";
        plugin.verifyCommand = new SolrPackage.Command();
        plugin.verifyCommand.method = "GET";
        plugin.verifyCommand.path = /*"/api*/
            "/collections/" + COLLECTION_NAME + "/config/requestHandler";
        plugin.verifyCommand.condition = "&[no_quotes_error_jpath]";
        manager.verify(
            new SolrPackageInstance(
                "",
                "",
                "1.0",
                new SolrPackage.Manifest(),
                Collections.singletonList(plugin),
                Collections.emptyMap()),
            Collections.singletonList(COLLECTION_NAME),
            plugin.type.equals("cluster"),
            new String[0]);
        fail();
      } catch (InvalidPathException e) {
        assertTrue(e.getMessage().contains("&[no_quotes_error_jpath]"));
      }
    }
  }

  private static class StubPackageManager extends PackageManager {
    public StubPackageManager(Http2SolrClient solrClient, SolrZkClient zkClient) {
      super(
          solrClient,
          SolrCloudTestCase.cluster.getJettySolrRunners().get(0).getBaseUrl().toString(),
          zkClient.getZkServerAddress());
    }

    @Override
    Map<String, String> getPackageParams(String packageName, String collection) {
      return Collections.emptyMap();
    }
  }
}
