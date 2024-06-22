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
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.apache.solr.client.solrj.impl.CloudLegacySolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAuthorizationFramework extends AbstractFullDistribZkTestBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final byte[] SECURITY_JSON =
      "{\"authorization\":{\"class\":\"org.apache.solr.security.MockAuthorizationPlugin\"}}"
          .getBytes(StandardCharsets.UTF_8);

  static final int TIMEOUT = 10000;

  @Override
  public void distribSetUp() throws Exception {
    super.distribSetUp();
    try (ZkStateReader zkStateReader =
        new ZkStateReader(zkServer.getZkAddress(), TIMEOUT, TIMEOUT)) {
      zkStateReader
          .getZkClient()
          .create(
              ZkStateReader.SOLR_SECURITY_CONF_PATH, SECURITY_JSON, CreateMode.PERSISTENT, true);
    }
  }

  @Test
  public void authorizationFrameworkTest() throws Exception {
    MockAuthorizationPlugin.denyUsers.add("user1");

    waitForThingsToLevelOut(10, TimeUnit.SECONDS);
    String baseUrl = jettys.get(0).getBaseUrl().toString();
    verifySecurityStatus(
        ((CloudLegacySolrClient) cloudClient).getHttpClient(),
        baseUrl + "/admin/authorization",
        "authorization/class",
        s -> MockAuthorizationPlugin.class.getName().equals(s),
        20);

    // This should work fine.
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    cloudClient.query(params);

    // This user is disallowed in the mock. The request should return a 403.
    MockAuthorizationPlugin.protectedResources.add("/select");
    params.add("uname", "user1");
    expectThrows(Exception.class, () -> cloudClient.query(params));
  }

  @Override
  public void distribTearDown() throws Exception {
    MockAuthorizationPlugin.protectedResources.clear();
    MockAuthorizationPlugin.denyUsers.clear();
    super.distribTearDown();
  }

  public static void verifySecurityStatus(
      HttpClient cl, String url, String objPath, Predicate<Object> expected, int count)
      throws Exception {
    String s = null;
    List<String> hierarchy = StrUtils.splitSmart(objPath, '/');
    for (int i = 0; i < count; i++) {
      HttpGet get = new HttpGet(url);
      s =
          EntityUtils.toString(
              cl.execute(get, HttpClientUtil.createNewHttpClientRequestContext()).getEntity());
      Map<?, ?> m = (Map<?, ?>) Utils.fromJSONString(s);

      Object actual = Utils.getObjectByPath(m, true, hierarchy);
      if (expected.test(actual)) {
        return;
      }
      Thread.sleep(50);
    }
    fail("No match for " + objPath + " = " + expected + ", full response = " + s);
  }
}
