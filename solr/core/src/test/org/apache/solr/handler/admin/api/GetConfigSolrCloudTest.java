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

import static org.apache.solr.core.CoreContainer.ALLOW_PATHS_SYSPROP;

import org.apache.solr.client.api.model.IndexType;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigApi;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.util.ExternalPaths;
import org.junit.BeforeClass;
import org.junit.Test;

/** HTTP tests for fetching the full config via the collection-scoped v2 path. */
public class GetConfigSolrCloudTest extends SolrCloudTestCase {

  private static final String COLLECTION_NAME = "configApiTestCollection";

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty(ALLOW_PATHS_SYSPROP, ExternalPaths.SERVER_HOME.toAbsolutePath().toString());
    configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();
    CollectionAdminRequest.createCollection(COLLECTION_NAME, "conf", 1, 1)
        .process(cluster.getSolrClient());
  }

  @Test
  public void testGetConfigFromCollection() throws Exception {
    var request = new ConfigApi.GetConfig(IndexType.COLLECTION, COLLECTION_NAME);
    var response = request.process(cluster.getSolrClient());

    assertNotNull(response);
    assertNull(response.error);
    assertNotNull(response.config);
    assertTrue(response.config.containsKey("luceneMatchVersion"));
    assertTrue(response.config.containsKey("updateHandler"));
    assertTrue(response.config.containsKey("query"));
    assertTrue(response.config.containsKey("requestHandler"));
  }

  @Test
  public void testGetOverlayFromCollection() throws Exception {
    var request = new ConfigApi.GetOverlay(IndexType.COLLECTION, COLLECTION_NAME);
    var response = request.process(cluster.getSolrClient());

    assertNotNull(response);
    assertNull(response.error);
    assertNotNull(response.overlay);
    assertTrue(response.overlay.containsKey("znodeVersion"));
    assertEquals(-1, response.overlay.get("znodeVersion"));
  }

  @Test
  public void testGetZnodeVersionFromCollection() throws Exception {
    var request = new ConfigApi.GetZnodeVersion(IndexType.COLLECTION, COLLECTION_NAME);
    var response = request.process(cluster.getSolrClient());

    assertNotNull(response);
    assertNull(response.error);
    assertNotNull(response.znodeVersion);
    assertTrue(response.znodeVersion.overlay >= -1);
    assertTrue(response.znodeVersion.params >= -1);
  }
}
