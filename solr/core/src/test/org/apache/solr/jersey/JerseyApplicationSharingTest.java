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

package org.apache.solr.jersey;

import java.util.Map;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests ensuring that Jersey apps are shared between cores as expected.
 *
 * <p>Jersey applications should be shared by any cores on the same node that have the same
 * "effective configset" (i.e. the same configset content and any overlays or relevant configuration
 * properties)
 */
public class JerseyApplicationSharingTest extends SolrCloudTestCase {

  private static final String collection = "collection1";
  private static final String confDir = collection + "/conf";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf1", configset("cloud-minimal"))
        .addConfig("conf2", configset("cloud-minimal"))
        .configure();
  }

  @Test
  public void testMultipleCoresWithSameConfigsetShareApplication() throws Exception {
    final SolrClient solrClient = cluster.getSolrClient();

    // No applications should be in the cache to start
    assertJerseyAppCacheHasSize(0);

    // All replicas for the created collection should share a single Jersey ApplicationHandler entry
    // in the cache
    final CollectionAdminRequest.Create coll1Create =
        CollectionAdminRequest.createCollection("coll1", "conf1", 2, 2);
    assertEquals(0, coll1Create.process(solrClient).getStatus());
    assertJerseyAppCacheHasSize(1);

    // A new collection using the same configset will also share the existing cached Jersey
    // ApplicationHandler
    final CollectionAdminRequest.Create coll2Create =
        CollectionAdminRequest.createCollection("coll2", "conf1", 2, 2);
    assertEquals(0, coll2Create.process(solrClient).getStatus());
    assertJerseyAppCacheHasSize(1);

    // Using a different configset WILL cause a new Jersey ApplicationHandler to be used (total
    // cache-count = 2)
    final CollectionAdminRequest.Create coll3Create =
        CollectionAdminRequest.createCollection("coll3", "conf2", 2, 2);
    assertEquals(0, coll3Create.process(solrClient).getStatus());
    assertJerseyAppCacheHasSize(2);

    // Modifying properties that affect a configset will also cause a new Jersey ApplicationHandler
    // to be created (total cache-count = 3)
    final CollectionAdminRequest.Create coll4Create =
        CollectionAdminRequest.createCollection("coll4", "conf1", 2, 2);
    coll4Create.setProperties(
        Map.of(
            "solr.commitwithin.softcommit",
            "false")); // Set any collection property used in the cloud-minimal configset
    assertEquals(0, coll4Create.process(solrClient).getStatus());
    assertJerseyAppCacheHasSize(3);
  }

  private void assertJerseyAppCacheHasSize(int expectedSize) {
    assertEquals(
        expectedSize,
        cluster.getJettySolrRunners().get(0).getCoreContainer().getJerseyAppHandlerCache().size());
  }
}
