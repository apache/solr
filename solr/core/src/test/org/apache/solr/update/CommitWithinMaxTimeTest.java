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
package org.apache.solr.update;

import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** A commitWithin longer than autoCommit maxTime must not delay the hard commit (SOLR-18692). */
public class CommitWithinMaxTimeTest extends SolrTestCaseJ4 {

  private static final long MAX_TIME_MS = 1000;
  private static final long COMMIT_WITHIN_MS = TimeUnit.HOURS.toMillis(1);

  private CommitTracker hardCommitTracker;

  @Before
  public void setup() throws Exception {
    System.setProperty("solr.ulog", "solr.UpdateLog");
    initCore("solrconfig-tlog.xml", "schema.xml");
    DirectUpdateHandler2 updateHandler = (DirectUpdateHandler2) h.getCore().getUpdateHandler();

    // Route commitWithin to the hard commit tracker, as a TLOG replica does
    updateHandler.setCommitWithinSoftCommit(false);

    updateHandler.softCommitTracker.setTimeUpperBound(-1);
    updateHandler.softCommitTracker.setDocsUpperBound(-1);
    updateHandler.softCommitTracker.setTLogFileSizeUpperBound(-1);

    hardCommitTracker = updateHandler.commitTracker;
    hardCommitTracker.setOpenSearcher(false);
    hardCommitTracker.setTimeUpperBound(MAX_TIME_MS);
    hardCommitTracker.setDocsUpperBound(-1);
    hardCommitTracker.setTLogFileSizeUpperBound(-1);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    deleteCore();
  }

  @Test
  public void testCommitWithinLongerThanMaxTimeCommitsAtMaxTime() throws Exception {
    assertEquals(0, hardCommitTracker.getCommitCount());

    assertU(add(doc("id", "1"), "commitWithin", Long.toString(COMMIT_WITHIN_MS)));

    new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME)
        .waitFor(
            "No hard commit within 30s although autoCommit maxTime is "
                + MAX_TIME_MS
                + "ms; commitWithin of "
                + COMMIT_WITHIN_MS
                + "ms was used instead",
            () -> hardCommitTracker.getCommitCount() > 0);
  }
}
