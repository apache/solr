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

package org.apache.solr.core;

import java.util.Map;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DigestZkACLProvider;
import org.apache.solr.common.cloud.DigestZkCredentialsProvider;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.VMParamsZkCredentialsInjector;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.security.MockSolrMetricsContextFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class InternalSolrClientCacheTest extends SolrCloudTestCase {
  private static final Map<String, String> sysProps =
      Map.of(
          SolrZkClient.ZK_CREDENTIALS_INJECTOR_CLASS_NAME_VM_PARAM_NAME,
          VMParamsZkCredentialsInjector.class.getName(),
          SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME,
          DigestZkCredentialsProvider.class.getName(),
          SolrZkClient.ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME,
          DigestZkACLProvider.class.getName(),
          VMParamsZkCredentialsInjector.DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME,
          "admin-user",
          VMParamsZkCredentialsInjector.DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME,
          "pass",
          VMParamsZkCredentialsInjector.DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME,
          "read-user",
          VMParamsZkCredentialsInjector.DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME,
          "pass");

  @BeforeClass
  public static void before() throws Exception {
    sysProps.forEach(System::setProperty);
    configureCluster(1)
        .formatZkServer(true)
        .addConfig(
            "conf", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @AfterClass
  public static void after() {
    sysProps.keySet().forEach(System::clearProperty);
  }

  @After
  public void afterEach() {
    System.clearProperty("solr.allow-external-clusters");
  }

  @Test
  public void testSelfClusterShouldBeAllowed() {
    final SolrMetricsContext metricsContext = MockSolrMetricsContextFactory.create();
    try (HttpSolrClientProvider httpSolrClientProvider =
        new HttpSolrClientProvider(null, metricsContext)) {
      try (SolrClientCache cache =
          new InternalSolrClientCache(
              httpSolrClientProvider.getSolrClient(), getZookeeperSolrConnection())) {
        CloudSolrClient cloudSolrClient = cache.getCloudSolrClient(getZookeeperSolrConnection());
        Assert.assertEquals(1, cloudSolrClient.getClusterStateProvider().getLiveNodes().size());
      }
    }
  }

  @Test
  public void testNotRegisteredClusterShouldBeAllowedWhenPropertySpecified() {
    System.setProperty("solr.allow-external-clusters", "true");
    final SolrMetricsContext metricsContext = MockSolrMetricsContextFactory.create();
    try (HttpSolrClientProvider httpSolrClientProvider =
        new HttpSolrClientProvider(null, metricsContext)) {
      try (SolrClientCache cache =
          new InternalSolrClientCache(
              httpSolrClientProvider.getSolrClient(), getZookeeperSolrConnection())) {
        // Trying to connect via HTTP, which the cache knows nothing about.
        CloudSolrClient cloudSolrClient = cache.getCloudSolrClient(getHttpSolrConnection());
        Assert.assertEquals(1, cloudSolrClient.getClusterStateProvider().getLiveNodes().size());
      }
    }
  }

  @Test
  public void testExternalClusterShouldBeProhibited() {
    final SolrMetricsContext metricsContext = MockSolrMetricsContextFactory.create();
    try (HttpSolrClientProvider httpSolrClientProvider =
        new HttpSolrClientProvider(null, metricsContext)) {
      try (SolrClientCache cache =
          new InternalSolrClientCache(
              httpSolrClientProvider.getSolrClient(), getZookeeperSolrConnection())) {
        expectThrows(
            SolrException.class,
            () ->
                cache.getCloudSolrClient(
                    CloudSolrClient.CloudSolrClientConnection.parse("test:2181")));
      }
    }
  }
}
