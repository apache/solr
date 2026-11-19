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
package org.apache.solr.cli;

import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

public class ConnectionOptionsTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).configure();
  }

  @Test
  public void testResolveZkHostIgnoresEnvWhenSolrUrlProvided() throws Exception {
    String solrUrl = cluster.getJettySolrRunner(0).getBaseUrl().toString();

    System.setProperty("zkHost", "other-cluster:2181/solr");
    System.setProperty("solr.connection", "other-cluster:2181/solr");

    ConnectionOptions opts = new ConnectionOptions();
    opts.solrUrl = solrUrl;

    String resolved = ConnectionOptions.resolveZkHost(opts, solrUrl, null);
    assertNotNull(resolved);
    assertFalse(
        "HTTP CLI target must not pick up a conflicting zkHost/sys property",
        resolved.contains("other-cluster"));
  }
}
