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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.index.IndexWriter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.TimeOut;
import org.junit.BeforeClass;
import org.junit.Test;

/** Tests {@link DirectUpdateHandler2} with update log enabled. */
@LogLevel("org.apache.solr.update=INFO")
public class DirectUpdateHandlerWithUpdateLogTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("solr.updateHandler", SpyingUpdateHandler.class.getName());
    initCore("solrconfig.xml", "schema11.xml");
  }

  @Test
  public void testShouldCommitHook() throws Exception {
    // Given a core.
    SolrCore core = h.getCore();
    assertNotNull(core);
    SpyingUpdateHandler updater = (SpyingUpdateHandler) core.getUpdateHandler();
    updater.shouldCommitCallCount.set(0);

    // When we add a doc and commit.
    assertU(adoc("id", "1"));
    assertU(commit());
    // Then the shouldCommit hook is called.
    assertEquals(1, updater.shouldCommitCallCount.get());

    // When we add a doc and soft commit.
    assertU(adoc("id", "2"));
    assertU(commit("softCommit", "true"));
    // Then the shouldCommit hook is not called.
    assertEquals(1, updater.shouldCommitCallCount.get());
    // And when we commit.
    assertU(commit());
    // Then the shouldCommit hook is called.
    assertEquals(2, updater.shouldCommitCallCount.get());

    // When we commit with no updates (empty commit).
    assertU(commit());
    // Then the shouldCommit hook is called (may commit only user metadata).
    assertEquals(3, updater.shouldCommitCallCount.get());

    // When we add a doc, do not commit, and close the IndexWriter.
    assertU(adoc("id", "3"));
    h.close();
    // Then the shouldCommit hook is called.
    new TimeOut(10, TimeUnit.SECONDS, TimeSource.NANO_TIME)
        .waitFor(
            "Timeout waiting for should commit hook",
            () -> updater.shouldCommitCallCount.get() == 4);
  }

  public static class SpyingUpdateHandler extends DirectUpdateHandler2 {

    final AtomicInteger shouldCommitCallCount = new AtomicInteger();

    public SpyingUpdateHandler(SolrCore core) {
      super(core);
    }

    public SpyingUpdateHandler(SolrCore core, UpdateHandler updateHandler) {
      super(core, updateHandler);
    }

    @Override
    protected boolean shouldCommit(CommitUpdateCommand cmd, IndexWriter writer) throws IOException {
      shouldCommitCallCount.incrementAndGet();
      return super.shouldCommit(cmd, writer);
    }
  }
}
