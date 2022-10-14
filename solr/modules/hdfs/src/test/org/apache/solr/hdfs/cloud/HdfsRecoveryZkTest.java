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
package org.apache.solr.hdfs.cloud;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.LuceneTestCase.Nightly;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.cloud.AbstractRecoveryZkTestBase;
import org.apache.solr.hdfs.util.BadHdfsThreadsFilter;
import org.junit.AfterClass;
import org.junit.BeforeClass;

@Nightly
@LuceneTestCase.AwaitsFix(bugUrl = "SOLR-15405")
@ThreadLeakFilters(
    defaultFilters = true,
    filters = {
      SolrIgnoredThreadsFilter.class,
      QuickPatchThreadsFilter.class,
      BadHdfsThreadsFilter.class // hdfs currently leaks thread(s)
    })
@ThreadLeakLingering(
    linger = 1000) // Wait at least 1 second for Netty GlobalEventExecutor to shutdown
public class HdfsRecoveryZkTest extends AbstractRecoveryZkTestBase {
  private static MiniDFSCluster dfsCluster;

  @BeforeClass
  public static void setupClass() throws Exception {
    dfsCluster = HdfsTestUtil.setupClass(createTempDir().toFile().getAbsolutePath());

    cluster.uploadConfigSet(configset("cloud-hdfs"), "conf");
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    try {
      shutdownCluster(); // need to close before the MiniDFSCluster
    } finally {
      try {
        HdfsTestUtil.teardownClass(dfsCluster);
      } finally {
        dfsCluster = null;
      }
    }
  }
}
