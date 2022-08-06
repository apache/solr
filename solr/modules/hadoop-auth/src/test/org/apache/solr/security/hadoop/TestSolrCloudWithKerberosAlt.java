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
package org.apache.solr.security.hadoop;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import java.lang.invoke.MethodHandles;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.util.BadZookeeperThreadsFilter;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadLeakFilters(
    defaultFilters = true,
    filters = {
      SolrIgnoredThreadsFilter.class,
      QuickPatchThreadsFilter.class,
      BadZookeeperThreadsFilter.class // Zookeeper login leaks TGT renewal threads
    })
@ThreadLeakLingering(linger = 10000) // minikdc has some lingering threads
public class TestSolrCloudWithKerberosAlt extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int numShards = 1;
  private static final int numReplicas = 1;
  private static final int nodeCount = numShards * numReplicas;
  private static final String configName = "solrCloudCollectionConfig";
  private static final String collectionName = "testkerberoscollection";

  private KerberosTestServices kerberosTestServices;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    kerberosTestServices = KerberosUtils.setupMiniKdc(createTempDir());

    System.setProperty("authenticationPlugin", "org.apache.solr.security.hadoop.KerberosPlugin");
    boolean enableDt = random().nextBoolean();
    log.info("Enable delegation token: {}", enableDt);
    System.setProperty("solr.kerberos.delegation.token.enabled", Boolean.toString(enableDt));

    configureCluster(nodeCount).addConfig(configName, configset("cloud-minimal")).configure();
  }

  @Test
  public void testBasics() throws Exception {
    testCollectionCreateSearchDelete();
    // sometimes run a second test e.g. to test collection create-delete-create scenario
    if (random().nextBoolean()) testCollectionCreateSearchDelete();
  }

  private void testCollectionCreateSearchDelete() throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(collectionName, configName, numShards, numReplicas)
        .process(client);

    cluster.waitForActiveCollection(collectionName, numShards, numShards * numReplicas);

    // modify/query collection
    new UpdateRequest().add("id", "1").commit(client, collectionName);
    QueryResponse rsp = client.query(collectionName, new SolrQuery("*:*"));
    assertEquals(1, rsp.getResults().getNumFound());

    // delete the collection we created earlier
    CollectionAdminRequest.deleteCollection(collectionName).process(client);

    AbstractDistribZkTestBase.waitForCollectionToDisappear(
        collectionName, ZkStateReader.from(client), true, 330);
  }

  @Override
  public void tearDown() throws Exception {
    System.clearProperty("authenticationPlugin");
    System.clearProperty("solr.kerberos.delegation.token.enabled");

    KerberosUtils.cleanupMiniKdc(kerberosTestServices);

    super.tearDown();
  }
}
