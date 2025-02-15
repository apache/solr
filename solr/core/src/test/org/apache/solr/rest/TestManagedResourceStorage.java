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
package org.apache.solr.rest;

import java.nio.file.Path;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.cloud.AbstractZkTestCase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.rest.ManagedResourceStorage.ZooKeeperStorageIO;
import org.junit.Test;

/** Depends on ZK for testing ZooKeeper backed storage logic. */
@LuceneTestCase.Nightly
public class TestManagedResourceStorage extends AbstractZkTestCase {

  /** Runs persisted managed resource creation and update tests on Zookeeper storage. */
  @Test
  public void testZkBasedJsonStorage() throws Exception {
    // test using ZooKeeper
    assertTrue("Not using ZooKeeper", h.getCoreContainer().isZooKeeperAware());
    Path instanceDir = createTempDir("zk-storage");
    try (SolrResourceLoader loader = new SolrResourceLoader(instanceDir)) {
      ZooKeeperStorageIO zkStorageIO = new ZooKeeperStorageIO(zkServer.getZkClient(), "/test");
      zkStorageIO.configure(loader, new NamedList<>());
      TestManagedFileStorage.doStorageTests(loader, zkStorageIO);
    }
  }
}
