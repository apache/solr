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

package org.apache.solr.zero.metadata;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.zero.client.ZeroFile;
import org.apache.solr.zero.process.CorePusher;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for in the context of deciding to delete no longer needed Zero store files. This
 * doesn't test the actual deletes but the application of the appropriate delete strategy.
 */
public class FileDeleteStrategyTest {

  /**
   * Recently deleted files should not be removed from the Zero store. Files deleted long ago are
   * removed.
   */
  @Test
  public void doNotRemoveFreshDeletes() {

    final AtomicInteger attemptedDeleteEnqueues = new AtomicInteger(0);
    final Set<ZeroFile.ToDelete> enqueuedFilesToDelete = new HashSet<>();

    // Create an instance of CorePushPull that considers that deleted file at a timestamp before
    // 10000 should be hard deleted, and those at or after this timestamp should not, and for which
    // deletes always work. Also implement a collector to track which deletes have been called.
    CorePusher deleteBeforeDate10000 =
        new CorePusher(null, null, null, null, null) {

          @Override
          protected boolean okForHardDelete(ZeroFile.ToDelete file) {
            return file.getDeletedAt().isBefore(Instant.ofEpochMilli(10000L));
          }

          @Override
          protected boolean enqueueForDelete(Set<ZeroFile.ToDelete> zeroFiles) {
            attemptedDeleteEnqueues.incrementAndGet();
            enqueuedFilesToDelete.addAll(zeroFiles);
            // enqueue always succeeds (assuming physical delete eventually happens, not tested
            // here)
            return true;
          }
        };

    // Now create a metadata builder with a set of files to delete and verify the right ones are
    // deleted.
    ZeroStoreShardMetadata shardMetadata = new ZeroStoreShardMetadata(0L);

    // file 1 deleted at 5000 (should be removed)
    ZeroFile.ToDelete zeroFile1 = newZeroFileToDelete("solrFile1.xxx", Instant.ofEpochMilli(5000L));
    // file 2 deleted at 15000 (should NOT be removed)
    ZeroFile.ToDelete zeroFile2 =
        newZeroFileToDelete("solrFile2.xxx", Instant.ofEpochMilli(15000L));
    // file 3 deleted at 1000 (should be removed)
    ZeroFile.ToDelete zeroFile3 = newZeroFileToDelete("solrFile3.xxx", Instant.ofEpochMilli(1000L));
    // file 4 deleted at 1000000000 (should not be removed)
    ZeroFile.ToDelete zeroFile4 =
        newZeroFileToDelete("solrFile4.xxx", Instant.ofEpochMilli(1000000000L));
    // file 5 deleted at 1000000000 (should not be removed)
    ZeroFile.ToDelete zeroFile5 =
        newZeroFileToDelete("solrFile5.xxx", Instant.ofEpochMilli(1000000000L));

    shardMetadata.addFileToDelete(zeroFile1);
    shardMetadata.addFileToDelete(zeroFile2);
    shardMetadata.addFileToDelete(zeroFile3);
    shardMetadata.addFileToDelete(zeroFile4);
    shardMetadata.addFileToDelete(zeroFile5);

    // Call the delete code
    deleteBeforeDate10000.enqueueForHardDelete(shardMetadata);

    // Verify delete enqueue attempted only on the two files to hard delete
    Assert.assertEquals(
        "Expected one delete enqueue attempts", 1, attemptedDeleteEnqueues.intValue());
    Assert.assertEquals("Expected two files enqueued for deleted", 2, enqueuedFilesToDelete.size());
    Assert.assertTrue(
        "solrFile1.xxx should have been enqueued for delete",
        enqueuedFilesToDelete.contains(zeroFile1));
    Assert.assertTrue(
        "solrFile3.xxx should have been enqueued for delete",
        enqueuedFilesToDelete.contains(zeroFile3));

    // Verify the metadata file got updated in the deleted files section.
    Set<String> remainingFilesToDelete = new HashSet<>();
    for (ZeroFile.ToDelete zftd : shardMetadata.getZeroFilesToDelete()) {
      remainingFilesToDelete.add(zftd.getZeroFileName());
    }

    Assert.assertEquals(
        "Zero shard metadata should have only three remaining files to delete",
        3,
        remainingFilesToDelete.size());
    Assert.assertTrue(
        "zeroFile2 should still be present in files to delete",
        remainingFilesToDelete.contains("solrFile2.xxx"));
    Assert.assertTrue(
        "zeroFile4 should still be present in files to delete",
        remainingFilesToDelete.contains("solrFile4.xxx"));
    Assert.assertTrue(
        "zeroFile5 should still be present in files to delete",
        remainingFilesToDelete.contains("solrFile5.xxx"));
  }

  /** When delete enqueue fails, no files are removed. */
  @Test
  public void noDeleteWhenEnqueueFails() {

    // Create an instance of CorePushPull that considers all deleted files should be hard deleted
    // but have delete enqueue always fail...
    CorePusher deleteButFailToEnqueue =
        new CorePusher(null, null, null, null, null) {

          @Override
          protected boolean okForHardDelete(ZeroFile.ToDelete file) {
            // All files should be hard deleted...
            return true;
          }

          @Override
          protected boolean enqueueForDelete(Set<ZeroFile.ToDelete> zeroFiles) {
            // ... but enqueue always fails
            return false;
          }
        };

    // Now create a metadata builder with a set of files to delete
    ZeroStoreShardMetadata shardMetadata = new ZeroStoreShardMetadata(0L);

    shardMetadata.addFileToDelete(
        newZeroFileToDelete("solrFile1.xyz", Instant.ofEpochMilli(123456L)));
    shardMetadata.addFileToDelete(
        newZeroFileToDelete("solrFile2.abc", Instant.ofEpochMilli(234567L)));
    shardMetadata.addFileToDelete(
        newZeroFileToDelete("solrFile3.uvw", Instant.ofEpochMilli(987654321L)));

    // Call the delete code
    deleteButFailToEnqueue.enqueueForHardDelete(shardMetadata);

    // Now verify no files have been removed from the delete section of the shard metadata since all
    // enqueues failed. (note it happens that we try only one enqueue then give up)

    Assert.assertEquals(
        "shard metadata should still have its three files to delete",
        3,
        shardMetadata.getZeroFilesToDelete().size());
  }

  private ZeroFile.ToDelete newZeroFileToDelete(String zeroFileName, Instant deletedAt) {
    return new ZeroFile.ToDelete("zeroCollectionName", "zeroShardName", zeroFileName, deletedAt);
  }
}
