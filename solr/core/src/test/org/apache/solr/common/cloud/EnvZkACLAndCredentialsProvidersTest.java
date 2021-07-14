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
package org.apache.solr.common.cloud;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.cloud.AbstractZkTestCase;
import org.apache.solr.cloud.VMParamsZkACLAndCredentialsProvidersTest;
import org.junit.Test;

import static org.apache.solr.common.cloud.EnvAllAndReadonlyDigestZkACLProvider.ZK_READ_ACL_PASSWORD;
import static org.apache.solr.common.cloud.EnvAllAndReadonlyDigestZkACLProvider.ZK_READ_ACL_USERNAME;
import static org.apache.solr.common.cloud.EnvSingleSetCredentialsDigestZkCredentialsProvider.ZK_ALL_ACL_PASSWORD;
import static org.apache.solr.common.cloud.EnvSingleSetCredentialsDigestZkCredentialsProvider.ZK_ALL_ACL_USERNAME;

public class EnvZkACLAndCredentialsProvidersTest extends VMParamsZkACLAndCredentialsProvidersTest {

  @Test
  public void testAllCredentialsFromEnv() throws Exception {
    useAllCredentialsFromEnv();

    try (SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT)) {
      doTest(zkClient,
          true, true, true, true, true,
          true, true, true, true, true);
    }
  }

  @Test
  public void testReadonlyCredentialsFromEnv() throws Exception {
    useReadonlyCredentialsFromEnv();

    try (SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT)) {
      doTest(zkClient,
          true, true, false, false, false,
          false, false, false, false, false);
    }
  }

  private void useAllCredentialsFromEnv() {
    clearSecuritySystemProperties();

    System.setProperty(SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME, EnvSingleSetCredentialsDigestZkCredentialsProvider.class.getName());

    Map<String, String> testEnv = ImmutableMap.of(ZK_ALL_ACL_USERNAME, "connectAndAllACLUsername",
        ZK_ALL_ACL_PASSWORD, "connectAndAllACLPassword");
    EnvSingleSetCredentialsDigestZkCredentialsProvider.setEnvVars(testEnv);
  }

  private void useReadonlyCredentialsFromEnv() {
    clearSecuritySystemProperties();

    System.setProperty(SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME, EnvSingleSetCredentialsDigestZkCredentialsProvider.class.getName());

    Map<String, String> testEnv =
        ImmutableMap.of(ZK_ALL_ACL_USERNAME, "readonlyACLUsername", ZK_ALL_ACL_PASSWORD, "readonlyACLPassword");
    EnvSingleSetCredentialsDigestZkCredentialsProvider.setEnvVars(testEnv);
  }

  @Override
  protected void setSecuritySystemProperties() {
    System.setProperty(SolrZkClient.ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME, EnvAllAndReadonlyDigestZkACLProvider.class.getName());
    System.setProperty(SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME, EnvSingleSetCredentialsDigestZkCredentialsProvider.class.getName());

    Map<String, String> testEnv = ImmutableMap.of(ZK_ALL_ACL_USERNAME, "connectAndAllACLUsername",
        ZK_ALL_ACL_PASSWORD, "connectAndAllACLPassword",
        ZK_READ_ACL_USERNAME, "readonlyACLUsername", ZK_READ_ACL_PASSWORD, "readonlyACLPassword");
    EnvSingleSetCredentialsDigestZkCredentialsProvider.setEnvVars(testEnv);
    EnvAllAndReadonlyDigestZkACLProvider.setEnvVars(testEnv);
  }
}
