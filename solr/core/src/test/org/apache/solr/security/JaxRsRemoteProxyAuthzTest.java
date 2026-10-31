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
package org.apache.solr.security;

import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericV2SolrRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.Utils;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.BeforeClass;
import org.junit.Test;

public class JaxRsRemoteProxyAuthzTest extends SolrCloudTestCase {
  private static final String PASSWORD = "pwd";
  private static final String COLLECTION = "proxied";

  @BeforeClass
  public static void setupCluster() throws Exception {
    String hash = Sha256AuthenticationProvider.getSaltedHashedValue(PASSWORD);
    Map<String, Object> security =
        Map.of(
            "authentication",
            Map.of(
                "blockUnknown",
                true,
                "class",
                "solr.BasicAuthPlugin",
                "credentials",
                Map.of("admin", hash, "monitor", hash, "reader", hash)),
            "authorization",
            Map.of(
                "class",
                "solr.RuleBasedAuthorizationPlugin",
                "permissions",
                List.of(
                    Map.of("name", "config-read", "role", "monitor"),
                    Map.of("name", "read", "role", "reader"),
                    Map.of("name", "all", "role", "admin")),
                "user-role",
                Map.of("admin", "admin", "monitor", "monitor", "reader", "reader")));
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .withSecurityJson(Utils.toJSONString(security))
        .configure();
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1)
            .setCreateNodeSet(cluster.getJettySolrRunner(0).getNodeName());
    create.setBasicAuthCredentials("admin", PASSWORD);
    create.process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 1, 1);
  }

  private static int selectViaNodeWithoutReplica(String user) throws Exception {
    JettySolrRunner node = cluster.getJettySolrRunner(1);
    assertTrue(node.getCoreContainer().getAllCoreNames().isEmpty());
    GenericV2SolrRequest req =
        new GenericV2SolrRequest(SolrRequest.METHOD.GET, "/collections/" + COLLECTION + "/select");
    req.setBasicAuthCredentials(user, PASSWORD);
    try {
      req.process(node.getSolrClient());
      return 200;
    } catch (RemoteSolrException e) {
      return e.code();
    }
  }

  @Test
  public void testProxiedSelectAllowedForReader() throws Exception {
    assertEquals(200, selectViaNodeWithoutReplica("reader"));
  }

  @Test
  public void testProxiedSelectForbiddenWithoutReadPermission() throws Exception {
    assertEquals(403, selectViaNodeWithoutReplica("monitor"));
  }
}
