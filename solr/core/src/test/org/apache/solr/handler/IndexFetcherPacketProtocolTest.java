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
package org.apache.solr.handler;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.FastInputStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.handler.admin.api.CoreReplication;
import org.apache.solr.handler.admin.api.ReplicationAPIBase;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the replication packet protocol between DirectoryFileStream (sender) and
 * IndexFetcher.FileFetcher.fetchPackets (receiver).
 *
 * <p>The packet protocol is:
 *
 * <ul>
 *   <li>Data packet: {@code int(size) + long(checksum) + byte[size]}
 *   <li>EOF marker: {@code int(0)} alone (no checksum)
 * </ul>
 *
 * <p>These tests verify correct handling of:
 *
 * <ul>
 *   <li>Files that are exact multiples of PACKET_SZ (1 MB) - triggers zero-length data packets
 *   <li>Files of arbitrary sizes - boundary conditions and edge cases
 *   <li>Checksum verification and error handling
 * </ul>
 */
public class IndexFetcherPacketProtocolTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final int PACKET_SZ = ReplicationAPIBase.PACKET_SZ;
  private static final int NO_CONTENT = 1;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("solr.security.allow.urls.enabled", "false");
    initCore("solrconfig.xml", "schema.xml");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    System.clearProperty("solr.security.allow.urls.enabled");
  }

  // Tests for files that are exact multiples of PACKET_SZ
  @Test
  public void testExactlyOnePacketSize() throws Exception {
    assertFetchPacketsSuccess(PACKET_SZ, "test-1mb.bin");
  }

  @Test
  public void testExactlyTwoPacketSizes() throws Exception {
    assertFetchPacketsSuccess(2 * PACKET_SZ, "test-2mb.bin");
  }

  @Test
  public void testExactlyThreePacketSizes() throws Exception {
    assertFetchPacketsSuccess(3 * PACKET_SZ, "test-3mb.bin");
  }

  @Test
  public void testExactly63PacketSizes() throws Exception {
    assertFetchPacketsSuccess(63 * PACKET_SZ, "test-63mb.bin");
  }

  // Tests for files that are not exact multiples of PACKET_SZ
  @Test
  public void testEmptyFile() throws Exception {
    assertFetchPacketsSuccess(0, "test-empty.bin");
  }

  @Test
  public void testSingleByte() throws Exception {
    assertFetchPacketsSuccess(1, "test-1byte.bin");
  }

  @Test
  public void testSmallFile100Bytes() throws Exception {
    assertFetchPacketsSuccess(100, "test-100bytes.bin");
  }

  @Test
  public void testSmallFile100KB() throws Exception {
    assertFetchPacketsSuccess(100 * 1024, "test-100kb.bin");
  }

  @Test
  public void testHalfPacketSize() throws Exception {
    assertFetchPacketsSuccess(PACKET_SZ / 2, "test-512kb.bin");
  }

  @Test
  public void testJustUnderOnePacketSize() throws Exception {
    assertFetchPacketsSuccess(PACKET_SZ - 1, "test-under-1mb.bin");
  }

  @Test
  public void testJustOverOnePacketSize() throws Exception {
    assertFetchPacketsSuccess(PACKET_SZ + 1, "test-over-1mb.bin");
  }

  @Test
  public void testOneAndHalfPacketSizes() throws Exception {
    assertFetchPacketsSuccess(PACKET_SZ + PACKET_SZ / 2, "test-1.5mb.bin");
  }

  @Test
  public void testJustUnderTwoPacketSizes() throws Exception {
    assertFetchPacketsSuccess(2 * PACKET_SZ - 1, "test-under-2mb.bin");
  }

  @Test
  public void testJustOverTwoPacketSizes() throws Exception {
    assertFetchPacketsSuccess(2 * PACKET_SZ + 1, "test-over-2mb.bin");
  }

  @Test
  public void testArbitrarySize() throws Exception {
    assertFetchPacketsSuccess(PACKET_SZ + 500000, "test-arbitrary.bin");
  }

  // Additional tests for comprehensive coverage

  @Test
  public void testMultipleExactPacketSizeFiles() throws Exception {
    // Ensures zero-length packet handling works correctly in succession
    for (int i = 1; i <= 5; i++) {
      assertFetchPacketsSuccess(i * PACKET_SZ, "test-" + i + "mb-successive.bin");
    }
  }

  @Test
  public void testChecksumMismatchReturnsError() throws Exception {
    int fileSize = 1024;
    String fileName = "test-checksum-mismatch.bin";

    byte[] expectedContent = createDeterministicContent(fileSize);
    byte[] streamBytes = serializeFileToPacketStream(expectedContent, fileName);

    // Corrupt the checksum bytes (bytes 4-11 after packet size)
    // Stream format: int(size) + long(checksum) + data
    if (streamBytes.length > 12) {
      streamBytes[5] ^= 0xFF; // Flip bits in checksum
    }

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    int result = invokeFetchPackets(streamBytes, output, fileName, fileSize);

    assertEquals("fetchPackets should return 1 for checksum mismatch", 1, result);
  }

  @Test
  public void testLargeFileTriggersBufferBehavior() throws Exception {
    // Test with file larger than initial buffer allocation
    // Initial buffer is min(fileSize, PACKET_SZ), so for 5MB file, buffer starts at 1MB
    // This ensures the code handles multi-packet transfers correctly
    assertFetchPacketsSuccess(5 * PACKET_SZ + 12345, "test-large-5mb.bin");
  }

  private void assertFetchPacketsSuccess(int fileSize, String fileName) throws Exception {
    log.info("Testing file transfer: {} ({} bytes)", fileName, fileSize);

    byte[] expectedContent = createDeterministicContent(fileSize);
    byte[] streamBytes = serializeFileToPacketStream(expectedContent, fileName);
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    int result = invokeFetchPackets(streamBytes, output, fileName, fileSize);

    int expectedResult = (fileSize == 0) ? NO_CONTENT : 0;
    assertEquals("fetchPackets return code mismatch", expectedResult, result);
    assertEquals("Output size should match input size", fileSize, output.size());
    assertArrayEquals(
        "Output content should match expected for " + fileName,
        expectedContent,
        output.toByteArray());

    log.info("Successfully verified transfer of {} ({} bytes)", fileName, fileSize);
  }

  private byte[] createDeterministicContent(int size) {
    byte[] content = new byte[size];
    for (int i = 0; i < size; i++) {
      content[i] = (byte) (i % 256);
    }
    return content;
  }

  /** Writes file content to a directory and serializes it using the replication packet protocol. */
  private byte[] serializeFileToPacketStream(byte[] content, String fileName) throws Exception {
    DirectoryFactory directoryFactory = h.getCore().getDirectoryFactory();
    Directory dir =
        directoryFactory.get(
            h.getCore().getNewIndexDir(),
            DirectoryFactory.DirContext.DEFAULT,
            h.getCore().getSolrConfig().indexConfig.lockType);

    try {
      if (content.length > 0) {
        try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
          out.writeBytes(content, content.length);
        }
      }

      CoreReplication replicationAPI =
          new CoreReplication(
              h.getCore(),
              new SolrQueryRequestBase(h.getCore(), new ModifiableSolrParams()) {},
              new SolrQueryResponse());

      MethodHandles.Lookup lookup =
          MethodHandles.privateLookupIn(ReplicationAPIBase.class, MethodHandles.lookup());
      Class<?> directoryFileStreamClass =
          Class.forName(ReplicationAPIBase.class.getName() + "$DirectoryFileStream");
      MethodHandle doFetchFileHandle =
          lookup.findVirtual(
              ReplicationAPIBase.class,
              "doFetchFile",
              MethodType.methodType(
                  directoryFileStreamClass,
                  String.class,
                  String.class,
                  String.class,
                  String.class,
                  boolean.class,
                  boolean.class,
                  double.class,
                  Long.class));

      Object stream;
      try {
        stream =
            doFetchFileHandle.invoke(
                replicationAPI, fileName, "file", null, null, false, true, 0.0, null);
      } catch (Exception e) {
        throw e;
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      Method writeMethod = stream.getClass().getMethod("write", java.io.OutputStream.class);
      writeMethod.invoke(stream, baos);
      return baos.toByteArray();
    } finally {
      if (content.length > 0) {
        try {
          dir.deleteFile(fileName);
        } catch (Exception e) {
          // ignored
        }
      }
      directoryFactory.release(dir);
    }
  }

  /**
   * Deserializes packet stream bytes using FileFetcher.fetchPackets and returns the result code.
   */
  private int invokeFetchPackets(
      byte[] streamBytes, ByteArrayOutputStream output, String fileName, long expectedSize)
      throws Exception {

    Class<?> fileFetcherClass = null;
    for (Class<?> innerClass : IndexFetcher.class.getDeclaredClasses()) {
      if (innerClass.getSimpleName().equals("FileFetcher")) {
        fileFetcherClass = innerClass;
        break;
      }
    }
    assertNotNull("FileFetcher inner class should exist", fileFetcherClass);

    IndexFetcher indexFetcher = createIndexFetcher();

    try {
      Map<String, Object> fileDetails = new HashMap<>();
      fileDetails.put("name", fileName);
      fileDetails.put("size", expectedSize);

      Object mockFileInterface = createMockFileInterface(output);

      MethodHandles.Lookup lookup =
          MethodHandles.privateLookupIn(fileFetcherClass, MethodHandles.lookup());
      MethodHandle ctorHandle =
          lookup.findConstructor(
              fileFetcherClass,
              MethodType.methodType(
                  void.class,
                  IndexFetcher.class,
                  getFileInterfaceClass(),
                  Map.class,
                  String.class,
                  String.class,
                  long.class));

      Object fileFetcher;
      try {
        fileFetcher =
            ctorHandle.invoke(indexFetcher, mockFileInterface, fileDetails, fileName, "file", 0L);
      } catch (Exception e) {
        throw e;
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }

      MethodHandle fetchPacketsHandle =
          lookup.findVirtual(
              fileFetcherClass,
              "fetchPackets",
              MethodType.methodType(int.class, FastInputStream.class));

      FastInputStream fis = new FastInputStream(new ByteArrayInputStream(streamBytes));
      try {
        return (int) fetchPacketsHandle.invoke(fileFetcher, fis);
      } catch (Exception e) {
        throw e;
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }
    } finally {
      indexFetcher.destroy();
    }
  }

  private IndexFetcher createIndexFetcher() throws Exception {
    NamedList<Object> initArgs = new NamedList<>();
    initArgs.add("leaderUrl", "http://localhost:8983/solr/collection1");
    return new IndexFetcher(initArgs, null, h.getCore());
  }

  private Class<?> getFileInterfaceClass() throws Exception {
    for (Class<?> innerClass : IndexFetcher.class.getDeclaredClasses()) {
      if (innerClass.getSimpleName().equals("FileInterface")) {
        return innerClass;
      }
    }
    throw new AssertionError("FileInterface not found");
  }

  /** Creates a mock FileInterface that captures written bytes to the provided output stream. */
  private Object createMockFileInterface(ByteArrayOutputStream output) throws Exception {
    Class<?> fileInterfaceClass = getFileInterfaceClass();

    return java.lang.reflect.Proxy.newProxyInstance(
        fileInterfaceClass.getClassLoader(),
        new Class<?>[] {fileInterfaceClass},
        (proxy, method, args) -> {
          switch (method.getName()) {
            case "write":
              byte[] buf = (byte[]) args[0];
              int packetSize = (Integer) args[1];
              output.write(buf, 0, packetSize);
              return null;
            case "sync":
            case "close":
            case "delete":
              return null;
            default:
              throw new UnsupportedOperationException("Unexpected method: " + method.getName());
          }
        });
  }
}
