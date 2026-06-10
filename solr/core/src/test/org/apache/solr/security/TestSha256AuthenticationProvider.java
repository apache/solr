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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.CommandOperation;
import org.junit.Test;

public class TestSha256AuthenticationProvider extends SolrTestCaseJ4 {
  public void testAuthenticate() {
    Sha256AuthenticationProvider zkAuthenticationProvider = new Sha256AuthenticationProvider();
    zkAuthenticationProvider.init(createConfigMap("ignore", "me"));

    String pwd = "Friendly";
    String user = "marcus";
    Map<String, Object> latestConf = createConfigMap(user, pwd);
    Map<String, Object> params = Map.of(user, pwd);
    Map<String, Object> result =
        zkAuthenticationProvider.edit(
            latestConf, List.of(new CommandOperation("set-user", params)));
    zkAuthenticationProvider = new Sha256AuthenticationProvider();
    zkAuthenticationProvider.init(result);

    assertTrue(zkAuthenticationProvider.authenticate(user, pwd));
    assertFalse(zkAuthenticationProvider.authenticate(user, "WrongPassword"));
    assertFalse(zkAuthenticationProvider.authenticate("unknownuser", "WrongPassword"));
  }

  public void testBasicAuthCommands() throws IOException {
    try (BasicAuthPlugin basicAuthPlugin = new BasicAuthPlugin()) {
      basicAuthPlugin.init(createConfigMap("ignore", "me"));

      Map<String, Object> latestConf = createConfigMap("solr", "SolrRocks");

      CommandOperation blockUnknown =
          new CommandOperation("set-property", Map.of("blockUnknown", true));
      basicAuthPlugin.edit(latestConf, List.of(blockUnknown));
      assertEquals(Boolean.TRUE, latestConf.get("blockUnknown"));
      basicAuthPlugin.init(latestConf);
      assertTrue(basicAuthPlugin.getBlockUnknown());
      blockUnknown = new CommandOperation("set-property", Map.of("blockUnknown", false));
      basicAuthPlugin.edit(latestConf, List.of(blockUnknown));
      assertEquals(Boolean.FALSE, latestConf.get("blockUnknown"));
      basicAuthPlugin.init(latestConf);
      assertFalse(basicAuthPlugin.getBlockUnknown());
    }
  }

  public void testBasicAuthWithCredentials() throws IOException {
    try (BasicAuthPlugin basicAuthPlugin = new BasicAuthPlugin()) {
      Map<String, Object> config =
          createConfigMap(
              "solr",
              "IV0EHq1OnNrj6gvRCwvFwTrZ1+z1oBbnQdiVC3otuq0= Ndd7LKvVBAaZIF0QAVi1ekCfAJXr1GGfLtRUXhgrF8c=");
      basicAuthPlugin.init(config);
      assertTrue(basicAuthPlugin.authenticate("solr", "SolrRocks"));
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testBasicAuthUserNotFound() throws IOException {
    try (BasicAuthPlugin basicAuthPlugin = new BasicAuthPlugin()) {
      Map<String, Object> config = createConfigMap(null, null);
      basicAuthPlugin.init(config);
    }
  }

  public void testBasicAuthDeleteFinalUser() throws IOException {
    try (BasicAuthPlugin basicAuthPlugin = new BasicAuthPlugin()) {
      Map<String, Object> config =
          createConfigMap(
              "solr",
              "IV0EHq1OnNrj6gvRCwvFwTrZ1+z1oBbnQdiVC3otuq0= Ndd7LKvVBAaZIF0QAVi1ekCfAJXr1GGfLtRUXhgrF8c=");
      basicAuthPlugin.init(config);
      assertTrue(basicAuthPlugin.authenticate("solr", "SolrRocks"));

      CommandOperation deleteUser = new CommandOperation("delete-user", "solr");
      assertFalse(deleteUser.hasError());
      basicAuthPlugin.edit(config, Arrays.asList(deleteUser));
      assertTrue(deleteUser.hasError());
      assertTrue(
          deleteUser
              .getErrors()
              .contains(Sha256AuthenticationProvider.CANNOT_DELETE_LAST_USER_ERROR));
    }
  }

  public void testAuthenticateRejectsUsernameEqualPassword() {
    // Simulate a credential store that has the username's own hash as the password
    // (e.g. set up before this policy was in effect) and verify authenticate() still rejects it.
    String user = "alice";
    String hashedValue = Sha256AuthenticationProvider.getSaltedHashedValue(user);
    Map<String, Object> config = new HashMap<>();
    Map<String, String> credentials = new HashMap<>();
    credentials.put(user, hashedValue);
    config.put("credentials", credentials);

    Sha256AuthenticationProvider provider = new Sha256AuthenticationProvider();
    provider.init(config);
    assertFalse(
        "authenticate() must reject username==password even when hash matches",
        provider.authenticate(user, user));
  }

  public void testSetUserRejectsUsernameEqualPassword() {
    Sha256AuthenticationProvider provider = new Sha256AuthenticationProvider();
    provider.init(createConfigMap("ignore", "me"));
    Map<String, Object> latestConf = createConfigMap("ignore", "me");
    String user = "bob";
    CommandOperation cmd = new CommandOperation("set-user", Map.of(user, user));
    provider.edit(latestConf, List.of(cmd));
    assertTrue("set-user should report an error when username==password", cmd.hasError());
  }

  private Map<String, Object> createConfigMap(String user, String pw) {
    Map<String, Object> config = new HashMap<>();
    Map<String, String> credentials = new HashMap<>();
    if (user != null) {
      credentials.put(user, pw);
    }
    config.put("credentials", credentials);
    return config;
  }
}
