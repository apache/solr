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
package org.apache.solr.schema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.file.PathUtils;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.MergeTrigger;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.index.LogDocMergePolicyFactory;
import org.apache.solr.util.RestTestBase;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * A Schema API add-field must not block indexing while a long merge is running. A core reload
 * replaces the IndexWriter, and closing the old one waits for running merges while holding the lock
 * every update needs. A merge that never finishes on its own stands in for a slow one, such as
 * merging large HNSW vector segments.
 */
public class TestSchemaChangeDuringMerge extends RestTestBase {
  private static volatile CountDownLatch mergeStarted;
  private static volatile CountDownLatch writerClosing;
  private static volatile CountDownLatch releaseMerges;
  private static volatile ExecutorService mergeExecutor;

  private ExecutorService executor;

  @Before
  public void before() throws Exception {
    mergeStarted = new CountDownLatch(1);
    writerClosing = new CountDownLatch(1);
    releaseMerges = new CountDownLatch(1);
    mergeExecutor =
        ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("blockingMerge"));
    executor = ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("client"));

    Path tmpSolrHome = createTempDir();
    PathUtils.copyDirectory(TEST_HOME(), tmpSolrHome);
    System.setProperty("managed.schema.mutable", "true");
    System.setProperty("solr.index.updatelog.enabled", "false");
    System.setProperty("solr.tests.mergeScheduler", BlockingMergeScheduler.class.getName());
    System.setProperty("solr.tests.mergePolicyFactory", LogDocMergePolicyFactory.class.getName());
    createJettyAndHarness(tmpSolrHome, "solrconfig-managed-schema.xml", "schema-rest.xml");
  }

  @After
  public void after() throws Exception {
    releaseMerges.countDown();
    ExecutorUtil.shutdownAndAwaitTermination(executor);
    solrTestRule.reset();
    ExecutorUtil.shutdownAndAwaitTermination(mergeExecutor);
    mergeExecutor = null;
  }

  @Test
  public void testIndexingIsNotBlockedByAddFieldDuringMerge() throws Exception {
    // LogDocMergePolicy merges once mergeFactor (10) segments of the same level exist.
    for (int i = 0; i < 10; i++) {
      getSolrClient().add(sdoc("id", "seg" + i));
      getSolrClient().commit();
    }
    assertTrue("merge did not start", mergeStarted.await(30, TimeUnit.SECONDS));
    // Core creation already closed a writer that initialized the empty index.
    writerClosing = new CountDownLatch(1);

    Future<?> addField =
        executor.submit(
            () ->
                new SchemaRequest.AddField(Map.of("name", "new_field", "type", "string"))
                    .process(getSolrClient()));

    TimeOut timeOut = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    timeOut.waitFor(
        "add-field neither returned nor started closing the IndexWriter",
        () -> addField.isDone() || writerClosing.getCount() == 0);

    Future<?> update = executor.submit(() -> getSolrClient().add(sdoc("id", "during-merge")));
    try {
      update.get(10, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      fail(
          "Indexing is blocked until the running merge finishes: add-field reloads the core, "
              + "which waits for merges while closing the old IndexWriter");
    }
    addField.get(30, TimeUnit.SECONDS);
  }

  /** Holds every merge until the test releases it. */
  public static class BlockingMergeScheduler extends MergeScheduler {
    @Override
    public void merge(MergeSource mergeSource, MergeTrigger trigger) {
      if (trigger == MergeTrigger.CLOSING) {
        writerClosing.countDown();
      }
      MergePolicy.OneMerge merge;
      while ((merge = mergeSource.getNextMerge()) != null) {
        MergePolicy.OneMerge oneMerge = merge;
        mergeExecutor.execute(
            () -> {
              mergeStarted.countDown();
              try {
                releaseMerges.await();
              } catch (InterruptedException ignored) {
                // the merge must still run, or closing the IndexWriter hangs
              }
              try {
                mergeSource.merge(oneMerge);
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            });
      }
    }
  }
}
