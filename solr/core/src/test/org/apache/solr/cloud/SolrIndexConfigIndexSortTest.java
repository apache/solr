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

package org.apache.solr.cloud;

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.junit.Before;
import org.junit.Test;

public class SolrIndexConfigIndexSortTest extends SolrCloudTestCase {

  private static String COLLECTION;

  @Before
  public void setUp() throws Exception {
    super.setUp();

    // decide collection name ...
    COLLECTION = "collection"+(1+random().nextInt(100)) ;

    // create and configure cluster
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();

    // create an empty collection
    CollectionAdminRequest
    .createCollection(COLLECTION, "conf", 1, 1)
    .processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    cluster.waitForActiveCollection(COLLECTION, 1, 1);
  }

  @Test
  public void testStuff() throws Exception {
    // TODO
  }

}
