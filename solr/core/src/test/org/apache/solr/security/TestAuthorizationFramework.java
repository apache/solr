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

import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseUtil;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.cloud.SolrCloudBridgeTestCase;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LuceneTestCase.Slow
public class TestAuthorizationFramework extends SolrCloudBridgeTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeTestAuthorizationFramework() throws Exception {
    disableReuseOfCryptoKeys();
  }

  @Before
  public void uploadSecurityJson() throws Exception {
    byte[] data = "{\"authorization\":{\"class\":\"org.apache.solr.security.MockAuthorizationPlugin\"}}".getBytes(StandardCharsets.UTF_8);
    SolrZkClient zkClient = cluster.getSolrClient().getZkStateReader().getZkClient();
    if (zkClient.exists(ZkStateReader.SOLR_SECURITY_CONF_PATH, true)) {
      zkClient.setData(ZkStateReader.SOLR_SECURITY_CONF_PATH, data, true);
    } else {
      zkClient.create(ZkStateReader.SOLR_SECURITY_CONF_PATH, data, CreateMode.PERSISTENT, true);
    }
  }

  @After
  public void cleanupAuthPlugin() {
    MockAuthorizationPlugin.denyUsers.clear();
    MockAuthorizationPlugin.protectedResources.clear();
  }

  @Test
  public void authorizationFrameworkTest() throws Exception {
    MockAuthorizationPlugin.denyUsers.add("user1");

    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString();
    verifySecurityStatus(baseUrl + "/admin/authorization", "authorization/class", MockAuthorizationPlugin.class.getName(), 20);
    log.info("Starting test");
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    // This should work fine.
    cloudClient.query(params);
    MockAuthorizationPlugin.protectedResources.add("/select");

    // This user is blacklisted in the mock. The request should return a 403.
    params.add("uname", "user1");
    SolrTestCaseUtil.expectThrows(Exception.class, () -> cloudClient.query(params));
    log.info("Ending test");
  }

  public static void verifySecurityStatus(String url, String objPath, Object expected, int count) throws Exception {
    int lastSlash = url.lastIndexOf('/');
    String path = url.substring(lastSlash);
    String base = url.substring(0, lastSlash);
    boolean success = false;
    String s = null;
    List<String> hierarchy = StrUtils.splitSmart(objPath, '/');
    try (Http2SolrClient client = new Http2SolrClient.Builder(base).build()) {
      for (int i = 0; i < count; i++) {
        try {
          GenericSolrRequest req = new GenericSolrRequest(SolrRequest.METHOD.GET, path, null);
          NamedList<Object> resp = client.request(req);
          s = Utils.toJSONString(resp.asMap(16));
          Map m = (Map) Utils.fromJSONString(s);
          Object actual = Utils.getObjectByPath(m, true, hierarchy);
          if (expected instanceof Predicate) {
            if (((Predicate) expected).test(actual)) { success = true; break; }
          } else if (Objects.equals(String.valueOf(actual), expected)) {
            success = true; break;
          }
        } catch (Exception e) {
          // retry
        }
        Thread.sleep(50);
      }
    }
    assertTrue("No match for " + objPath + " = " + expected + ", full response = " + s, success);
  }
}
