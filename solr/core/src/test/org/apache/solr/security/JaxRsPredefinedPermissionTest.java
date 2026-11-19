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
import org.apache.solr.client.solrj.request.LoggingApi;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.Utils;
import org.junit.BeforeClass;
import org.junit.Test;

public class JaxRsPredefinedPermissionTest extends SolrCloudTestCase {
  private static final String PASSWORD = "pwd";

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
                    Map.of("name", "all", "role", "admin")),
                "user-role",
                Map.of("admin", "admin", "monitor", "monitor", "reader", "reader")));
    configureCluster(1).withSecurityJson(Utils.toJSONString(security)).configure();
  }

  private static void listLoggers(String user) throws Exception {
    LoggingApi.ListAllLoggersAndLevels req = new LoggingApi.ListAllLoggersAndLevels();
    req.setBasicAuthCredentials(user, PASSWORD);
    req.process(cluster.getJettySolrRunner(0).getSolrClient());
  }

  @Test
  public void testPredefinedPermissionAllowedDespiteAllRule() throws Exception {
    listLoggers("monitor");
  }

  @Test
  public void testUserWithoutPermissionForbidden() {
    RemoteSolrException e = expectThrows(RemoteSolrException.class, () -> listLoggers("reader"));
    assertEquals(403, e.code());
  }
}
