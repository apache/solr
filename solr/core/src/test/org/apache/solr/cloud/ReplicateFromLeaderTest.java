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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrConfig;
import org.junit.Test;

public class ReplicateFromLeaderTest extends SolrTestCaseJ4 {

  @Test
  public void determineTestPollIntervalString() {
    SolrConfig.UpdateHandlerInfo updateHandlerInfo =
        new SolrConfig.UpdateHandlerInfo(
            "solr.DirectUpdateHandler2", -1, 60000, -1, true, -1, -1, false, "0:0:56");
    String pollInterval = ReplicateFromLeader.determinePollInterval(updateHandlerInfo);
    assertEquals("00:00:01", pollInterval);
  }

  @Test
  public void determinePollIntervalString() {
    // disable jetty test mode
    System.clearProperty("jetty.testMode");

    SolrConfig.UpdateHandlerInfo updateHandlerInfo =
        new SolrConfig.UpdateHandlerInfo(
            "solr.DirectUpdateHandler2", -1, 15000, -1, true, -1, 60000, false, null);
    String pollInterval = ReplicateFromLeader.determinePollInterval(updateHandlerInfo);
    assertEquals("0:0:7", pollInterval);

    updateHandlerInfo =
        new SolrConfig.UpdateHandlerInfo(
            "solr.DirectUpdateHandler2", -1, 60000, -1, true, -1, 15000, false, null);
    pollInterval = ReplicateFromLeader.determinePollInterval(updateHandlerInfo);
    assertEquals("0:0:30", pollInterval);

    updateHandlerInfo =
        new SolrConfig.UpdateHandlerInfo(
            "solr.DirectUpdateHandler2", -1, 15000, -1, false, -1, 60000, false, null);
    pollInterval = ReplicateFromLeader.determinePollInterval(updateHandlerInfo);
    assertEquals("0:0:30", pollInterval);

    updateHandlerInfo =
        new SolrConfig.UpdateHandlerInfo(
            "solr.DirectUpdateHandler2", -1, 60000, -1, false, -1, 15000, false, null);
    pollInterval = ReplicateFromLeader.determinePollInterval(updateHandlerInfo);
    assertEquals("0:0:30", pollInterval);

    updateHandlerInfo =
        new SolrConfig.UpdateHandlerInfo(
            "solr.DirectUpdateHandler2", -1, -1, -1, false, -1, 60000, false, null);
    pollInterval = ReplicateFromLeader.determinePollInterval(updateHandlerInfo);
    assertEquals("0:0:30", pollInterval);

    updateHandlerInfo =
        new SolrConfig.UpdateHandlerInfo(
            "solr.DirectUpdateHandler2", -1, 15000, -1, false, -1, -1, false, null);
    pollInterval = ReplicateFromLeader.determinePollInterval(updateHandlerInfo);
    assertEquals("0:0:7", pollInterval);

    updateHandlerInfo =
        new SolrConfig.UpdateHandlerInfo(
            "solr.DirectUpdateHandler2", -1, 60000, -1, true, -1, -1, false, null);
    pollInterval = ReplicateFromLeader.determinePollInterval(updateHandlerInfo);
    assertEquals("0:0:30", pollInterval);

    updateHandlerInfo =
        new SolrConfig.UpdateHandlerInfo(
            "solr.DirectUpdateHandler2", -1, 60000, -1, true, -1, -1, false, "0:0:56");
    pollInterval = ReplicateFromLeader.determinePollInterval(updateHandlerInfo);
    assertEquals("0:0:56", pollInterval);

    final SolrConfig.UpdateHandlerInfo illegalUpdateHandlerInfo =
        new SolrConfig.UpdateHandlerInfo(
            "solr.DirectUpdateHandler2",
            -1,
            60000,
            -1,
            true,
            -1,
            -1,
            false,
            "garbage-unfortunately");
    assertThrows(
        SolrException.class,
        () -> ReplicateFromLeader.determinePollInterval(illegalUpdateHandlerInfo));
  }
}
