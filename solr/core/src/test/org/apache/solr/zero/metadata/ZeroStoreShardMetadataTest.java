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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.solr.zero.client.ZeroFile;
import org.junit.Assert;
import org.junit.Test;

/** Unit tests for {@link ZeroStoreShardMetadata} */
public class ZeroStoreShardMetadataTest extends Assert {

  private static final long GENERATION_NUMBER = 456;
  private static final long FILE_SIZE = 123000;
  private static final long CHECKSUM = 100;

  /** Test vuilding a shard metadata referencing no segment files */
  @Test
  public void buildShardMetadataNoFiles() {
    ZeroStoreShardMetadata shardMetadata = new ZeroStoreShardMetadata(GENERATION_NUMBER);

    assertTrue(
        "metadata should have no files unless explicitly added",
        shardMetadata.getZeroFiles().isEmpty());
    assertEquals(
        "metadata should have specified generation",
        GENERATION_NUMBER,
        shardMetadata.getGeneration());
  }

  @Test
  public void buildShardMetadataWithFile() {
    ZeroStoreShardMetadata shardMetadata = new ZeroStoreShardMetadata(GENERATION_NUMBER);
    shardMetadata.addFile(newZeroFileWithLocal("solrFilename", FILE_SIZE, CHECKSUM));

    assertEquals(
        "metadata should have specified generation",
        GENERATION_NUMBER,
        shardMetadata.getGeneration());
    assertEquals(
        "metadata should have the correct number of added files",
        1,
        shardMetadata.getZeroFiles().size());

    // we have a single single, retrieve it
    ZeroFile.WithLocal localFile = shardMetadata.getZeroFiles().iterator().next();
    assertEquals(
        "Zero store file should have correct solr filename",
        "solrFilename",
        localFile.getSolrFileName());
    assertEquals(
        "Zero store file should have correct Zero store filename",
        "solrFilename.xxx",
        localFile.getZeroFileName());
    assertEquals(
        "Zero store file should have correct file size", FILE_SIZE, localFile.getFileSize());
    assertEquals("Zero store file should have correct checksum", CHECKSUM, localFile.getChecksum());
  }

  @Test
  public void jsonShardMetadataNoFiles() throws Exception {
    ZeroStoreShardMetadata shardMetadata = new ZeroStoreShardMetadata(666L);

    verifyToJsonAndBack(shardMetadata);
  }

  @Test
  public void jsonShardMetadataFile() throws Exception {
    ZeroStoreShardMetadata shardMetadata = new ZeroStoreShardMetadata(777L);
    shardMetadata.addFile(newZeroFileWithLocal("solrFilename", 123000L, 100L));
    verifyToJsonAndBack(shardMetadata);
  }

  @Test
  public void jsonShardMetadataMultiFiles() throws Exception {
    ZeroStoreShardMetadata shardMetadata = new ZeroStoreShardMetadata(123L);
    Set<ZeroFile.WithLocal> files =
        new HashSet<>(
            Arrays.asList(
                newZeroFileWithLocal("solrFilename11", 1234L, 100L),
                newZeroFileWithLocal("solrFilename21", 2345L, 200L),
                newZeroFileWithLocal("solrFilename31", 3456L, 200L),
                newZeroFileWithLocal("solrFilename41", 4567L, 400L)));
    for (ZeroFile.WithLocal f : files) {
      shardMetadata.addFile(f);
    }

    verifyToJsonAndBack(shardMetadata);

    assertEquals(
        "ZeroStoreShardMetadata should have generation specified to builder",
        123L,
        shardMetadata.getGeneration());

    // Files are not necessarily in the same order
    assertEquals(
        "ZeroStoreShardMetadata should have file set specified to builder",
        files,
        shardMetadata.getZeroFiles());
  }

  private void verifyToJsonAndBack(ZeroStoreShardMetadata b) throws Exception {
    String json = b.toJson();
    ZeroStoreShardMetadata b2 = ZeroStoreShardMetadata.fromJson(json);
    assertEquals(
        "Conversion from ZeroStoreShardMetadata to json and back expected to return an equal object",
        b,
        b2);
  }

  private ZeroFile.WithLocal newZeroFileWithLocal(
      String solrFileName, long fileSize, long checksum) {
    return new ZeroFile.WithLocal(
        "myCollectionNameJsonTest",
        "myShardNameJsonTest",
        solrFileName,
        ZeroMetadataController.buildFileName(solrFileName, "xxx"),
        fileSize,
        checksum);
  }
}
