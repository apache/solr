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

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.util.RevertDefaultThreadHandlerRule;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@ThreadLeakLingering(linger = 10)
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "Solr logs to JUL")
public class ZkTestServerTest extends SolrTestCaseJ4 {

  @ClassRule
  public static TestRule solrClassRules =
      RuleChain.outerRule(new SystemPropertiesRestoreRule())
          .around(new RevertDefaultThreadHandlerRule());

  @Test
  public void testEmbeddedZkServerSetsMaxCnxnsDefaultWhenUnset() throws Exception {
    final String priorValue = System.getProperty(SolrZkServer.ZK_MAX_CNXNS_PROPERTY);
    ZkTestServer zkServer = null;
    try {
      System.clearProperty(SolrZkServer.ZK_MAX_CNXNS_PROPERTY);
      zkServer = new ZkTestServer(createTempDir("zkData"));
      zkServer.run(false);
      assertEquals(
          SolrZkServer.ZK_MAX_CNXNS_DEFAULT,
          System.getProperty(SolrZkServer.ZK_MAX_CNXNS_PROPERTY));
    } finally {
      if (zkServer != null) {
        zkServer.shutdown();
      }
      if (priorValue == null) {
        System.clearProperty(SolrZkServer.ZK_MAX_CNXNS_PROPERTY);
      } else {
        System.setProperty(SolrZkServer.ZK_MAX_CNXNS_PROPERTY, priorValue);
      }
    }
  }
}
