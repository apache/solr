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
package org.apache.solr.util;

import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MultipleManagedSchemasTest extends SolrCloudTestCase {
  @Before
  public void setUp() throws Exception {
    super.setUp();
    SolrCLI.FAKE_EXIT = true;
    configureCluster(1).configure();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
    SolrCLI.FAKE_EXIT = false;
    super.tearDown();
  }

  @Test
  public void testSameCollectionNameWithMultipleSchemas() throws Exception {
    String solrUrl = cluster.getJettySolrRunner(0).getBaseUrl().toString();
    String configsets = TEST_PATH().resolve("configsets").toString();

    SolrCLI.main(new String[] { "create_collection",
        "-name", "COLL1",
        "-solrUrl", solrUrl,
        "-confdir", "_default",
        "-configsetsDir", configsets
    });
    SolrCLI.main(new String[] { "delete",
        "-name", "COLL1",
        "-solrUrl", solrUrl,
    }); // Also deleted the configset from ZK
    SolrCLI.main(new String[] { "create_collection",
        "-name", "COLL1",
        "-solrUrl", solrUrl,
        "-confdir", "cloud-managed",
        "-configsetsDir", configsets
    });
  }
}
