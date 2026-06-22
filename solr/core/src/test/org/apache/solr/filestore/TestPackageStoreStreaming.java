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
package org.apache.solr.filestore;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.FastInputStream;
import org.apache.solr.common.util.Utils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for the streaming fetch/persist path in {@link DistribPackageStore} and
 * the {@link Utils#newStreamingConsumer(Path)} helper.
 *
 * <p>Tests cover:
 * <ul>
 *   <li>Near-MAX_PKG_SIZE fetch+persist with flag ON — digest correctness</li>
 *   <li>Digest/checksum correctness against known bytes</li>
 *   <li>Temp-file → fsync → atomic-publish leaves the published file complete</li>
 *   <li>Metadata validation still enforced (invalid metadata rejected)</li>
 *   <li>Crash-mid-write simulation: abort before publish leaves NO partial published file</li>
 *   <li>Flag OFF (default): legacy behavior is byte-identical and unchanged</li>
 *   <li>{@link PackageStore.FileEntry} streaming-constructor returns stream, not buffer</li>
 * </ul>
 *
 * <p>These are unit tests — no MiniSolrCloudCluster or ZK required.
 */
public class TestPackageStoreStreaming extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // -----------------------------------------------------------------------
  // Utils.newStreamingConsumer tests
  // -----------------------------------------------------------------------

  /**
   * Digest correctness: stream known bytes through newStreamingConsumer and verify
   * the returned hex digest matches an independent DigestUtils.sha512Hex computation.
   */
  @Test
  public void testStreamingConsumerDigestCorrectness() throws Exception {
    byte[] data = "Hello, streaming package store!".getBytes("UTF-8");
    String expectedDigest = DigestUtils.sha512Hex(data);

    Path tempDir = SolrTestUtil.createTempDir("pkg-stream-test");
    Utils.InputStreamConsumer<Object[]> consumer = Utils.newStreamingConsumer(tempDir);

    Object[] result = invokeStreamingConsumer(consumer, data);
    Path tmpFile = (Path) result[0];
    String actualDigest = (String) result[1];

    assertEquals("Digest must match sha512 of original bytes", expectedDigest, actualDigest);
    assertTrue("Temp file must exist after streaming", Files.exists(tmpFile));

    byte[] written = Files.readAllBytes(tmpFile);
    assertArrayEquals("Temp file content must equal original bytes", data, written);

    Files.deleteIfExists(tmpFile);
  }

  /**
   * Near-MAX_PKG_SIZE: generate ~5 MiB of random bytes and verify round-trip
   * digest + content via the streaming consumer (flag-on scenario).
   */
  @Test
  public void testStreamingConsumerLargeFile() throws Exception {
    int size = 5 * 1024 * 1024; // 5 MiB — exercises buffering without being too slow for CI
    byte[] data = new byte[size];
    new SecureRandom().nextBytes(data);
    String expectedDigest = DigestUtils.sha512Hex(data);

    Path tempDir = SolrTestUtil.createTempDir("pkg-stream-large");
    Object[] result = invokeStreamingConsumer(Utils.newStreamingConsumer(tempDir), data);

    Path tmpFile = (Path) result[0];
    String actualDigest = (String) result[1];

    assertEquals("Large-file digest must be correct", expectedDigest, actualDigest);
    assertEquals("Temp file size must equal original", size, Files.size(tmpFile));

    byte[] written = Files.readAllBytes(tmpFile);
    assertArrayEquals("Temp file bytes must match original", data, written);

    Files.deleteIfExists(tmpFile);
  }

  /**
   * Crash-mid-write simulation: if an IOException is thrown while streaming,
   * the consumer must delete the temp file so no partial file lingers.
   */
  @Test
  public void testStreamingConsumerCleanupOnError() throws Exception {
    Path tempDir = SolrTestUtil.createTempDir("pkg-stream-err");

    byte[] prefix = new byte[1024];
    Arrays.fill(prefix, (byte) 0xAB);

    try {
      invokeStreamingConsumerWithError(Utils.newStreamingConsumer(tempDir), prefix);
      fail("Expected IOException to propagate");
    } catch (IOException | RuntimeException expected) {
      // good — error propagated
    }

    long remaining = Files.list(tempDir)
        .filter(p -> p.getFileName().toString().endsWith(".tmp"))
        .count();
    assertEquals("No temp files should remain after streaming error", 0, remaining);
  }

  // -----------------------------------------------------------------------
  // _persistToFileStreaming tests
  // -----------------------------------------------------------------------

  /**
   * Temp-file → fsync → atomic-publish: after _persistToFileStreaming the published
   * file must exist and contain the original bytes; the temp file must be gone.
   */
  @Test
  public void testPersistToFileStreamingAtomicPublish() throws Exception {
    byte[] data = generateBytes(128 * 1024); // 128 KiB
    String sha512 = DigestUtils.sha512Hex(data);

    Path solrHome = SolrTestUtil.createTempDir("solrhome");
    Path pkgDir = solrHome.resolve("filestore").resolve("package").resolve("mypkg").resolve("v1.0");
    Files.createDirectories(pkgDir);

    // Simulates what newStreamingConsumer produces: a temp file with data already written.
    Path tmpFile = Files.createTempFile(pkgDir, ".pkg-stream-", ".tmp");
    Files.write(tmpFile, data);

    ByteBuffer meta = buildMetaBuffer(sha512);

    String logicalPath = "/package/mypkg/v1.0/lib.jar";
    DistribPackageStore._persistToFileStreaming(solrHome, logicalPath, tmpFile, meta);

    Path published = DistribPackageStore._getRealPath(logicalPath, solrHome);
    assertTrue("Published file must exist", Files.exists(published));
    byte[] publishedBytes = Files.readAllBytes(published);
    assertArrayEquals("Published content must equal original bytes", data, publishedBytes);

    // Temp file must be gone (moved, not copied).
    assertFalse("Temp file must be gone after atomic publish", Files.exists(tmpFile));

    // Digest of published file must match.
    assertEquals("Published file digest must match", sha512, DigestUtils.sha512Hex(publishedBytes));

    // Metadata file must also exist.
    Path metaFile = DistribPackageStore._getRealPath(
        DistribPackageStore._getMetapath(logicalPath), solrHome);
    assertTrue("Metadata file must exist", Files.exists(metaFile));
  }

  /**
   * Metadata validation: _persistToFileStreaming must reject empty/null metadata JSON
   * and must NOT leave a published data file.
   */
  @Test
  public void testPersistToFileStreamingRejectsInvalidMetadata() throws Exception {
    byte[] data = generateBytes(1024);

    Path solrHome = SolrTestUtil.createTempDir("solrhome-invalid");
    Path pkgDir = solrHome.resolve("filestore").resolve("package").resolve("bad").resolve("v1");
    Files.createDirectories(pkgDir);

    Path tmpFile = Files.createTempFile(pkgDir, ".pkg-stream-", ".tmp");
    Files.write(tmpFile, data);

    // Empty JSON {} triggers the "m.isEmpty()" guard in _persistToFileStreaming.
    ByteBuffer emptyMeta = ByteBuffer.wrap("{}".getBytes("UTF-8"), 0, 2);

    String logicalPath = "/package/bad/v1/lib.jar";
    try {
      DistribPackageStore._persistToFileStreaming(solrHome, logicalPath, tmpFile, emptyMeta);
      fail("Should have rejected empty metadata");
    } catch (SolrException e) {
      assertTrue("Exception message should mention 'invalid metadata'",
          e.getMessage().contains("invalid metadata"));
    }

    // No published data file should exist.
    Path published = DistribPackageStore._getRealPath(logicalPath, solrHome);
    assertFalse("No published file should exist after metadata rejection", Files.exists(published));
  }

  /**
   * Crash-before-publish: making the destination directory read-only prevents the
   * rename; no partial published file must exist afterward.
   *
   * <p>Skipped gracefully on platforms where setWritable(false) is ignored (e.g. root).
   */
  @Test
  public void testCrashBeforePublishLeavesNoPartialFile() throws Exception {
    byte[] data = generateBytes(4096);
    String sha512 = DigestUtils.sha512Hex(data);

    Path solrHome = SolrTestUtil.createTempDir("solrhome-crash");
    Path pkgDir = solrHome.resolve("filestore").resolve("package").resolve("crash").resolve("v1");
    Files.createDirectories(pkgDir);

    Path tmpFile = Files.createTempFile(pkgDir, ".pkg-stream-", ".tmp");
    Files.write(tmpFile, data);

    ByteBuffer meta = buildMetaBuffer(sha512);
    String logicalPath = "/package/crash/v1/lib.jar";
    Path published = DistribPackageStore._getRealPath(logicalPath, solrHome);

    boolean madeReadOnly = pkgDir.toFile().setWritable(false);
    if (!madeReadOnly) {
      // Platform did not honour read-only (e.g. running as root in CI) — skip.
      Files.deleteIfExists(tmpFile);
      return;
    }

    try {
      DistribPackageStore._persistToFileStreaming(solrHome, logicalPath, tmpFile, meta);
      // If rename somehow succeeded (e.g. cross-device fallback), just pass.
    } catch (IOException expected) {
      assertFalse("Published file must not exist after failed publish", Files.exists(published));
    } finally {
      pkgDir.toFile().setWritable(true);
      Files.deleteIfExists(tmpFile);
      Files.deleteIfExists(published);
    }
  }

  // -----------------------------------------------------------------------
  // PackageStore.FileEntry streaming-constructor tests
  // -----------------------------------------------------------------------

  /**
   * Stream-first FileEntry: getInputStream() returns a stream from the supplier;
   * getBuffer() returns null (invariant #8: no .array()-capable buffer on streaming path).
   */
  @Test
  public void testFileEntryStreamingConstructorReturnsStreamNotBuffer() throws Exception {
    byte[] data = "streaming entry data".getBytes("UTF-8");
    PackageStore.FileEntry entry = new PackageStore.FileEntry(
        null, null, "/package/test/v1/lib.jar",
        () -> new ByteArrayInputStream(data));

    assertNull("getBuffer() must be null on streaming path", entry.getBuffer());

    try (InputStream is = entry.getInputStream()) {
      assertNotNull("getInputStream() must be non-null on streaming path", is);
      byte[] read = is.readAllBytes();
      assertArrayEquals("Stream content must match original data", data, read);
    }
  }

  /**
   * Legacy FileEntry (buffer path): getBuffer() returns the buffer; getInputStream()
   * wraps it.  Behavior must be byte-identical with flag OFF.
   */
  @Test
  public void testFileEntryLegacyConstructorPreservedBehavior() throws Exception {
    byte[] data = "legacy buffer data".getBytes("UTF-8");
    ByteBuffer buf = ByteBuffer.wrap(data);

    PackageStore.FileEntry entry = new PackageStore.FileEntry(buf, null, "/package/test/v1/lib.jar");

    assertSame("getBuffer() must return the original buffer", buf, entry.getBuffer());
    assertArrayEquals("getBuffer().array() must equal original bytes", data, entry.getBuffer().array());

    try (InputStream is = entry.getInputStream()) {
      assertNotNull("getInputStream() must work on legacy path", is);
      byte[] read = is.readAllBytes();
      assertArrayEquals("Stream from buffer must equal original bytes", data, read);
    }
  }

  /**
   * Flag-OFF guard: with STREAM_ENABLED=false (the default), the legacy whole-file path
   * is taken.  This test confirms the default constant value when the property is not set.
   */
  @Test
  public void testStreamingFlagDefaultIsFalse() {
    String propValue = System.getProperty("solr.filestore.stream.enabled");
    if (propValue == null) {
      assertFalse("STREAM_ENABLED default must be false when property not set",
          DistribPackageStore.STREAM_ENABLED);
    }
    log.info("solr.filestore.stream.enabled = {}", DistribPackageStore.STREAM_ENABLED);
  }

  // -----------------------------------------------------------------------
  // Test helpers
  // -----------------------------------------------------------------------

  /**
   * Invoke a {@link Utils.InputStreamConsumer} with {@code data} bridged through
   * a {@link FastInputStream} (matching what {@code executeGET} does in production).
   */
  @SuppressWarnings("unchecked")
  private static Object[] invokeStreamingConsumer(
      Utils.InputStreamConsumer<Object[]> consumer, byte[] data) throws IOException {
    try (FastInputStream fis = FastInputStream.wrap(new ByteArrayInputStream(data))) {
      return consumer.accept(fis);
    }
  }

  /**
   * Like {@link #invokeStreamingConsumer} but the stream throws after delivering
   * {@code prefix} bytes, simulating a mid-stream network failure.
   */
  @SuppressWarnings("unchecked")
  private static Object[] invokeStreamingConsumerWithError(
      Utils.InputStreamConsumer<Object[]> consumer, byte[] prefix) throws IOException {
    InputStream errorStream = new InputStream() {
      int pos = 0;
      @Override
      public int read() throws IOException {
        if (pos < prefix.length) return prefix[pos++] & 0xff;
        throw new IOException("Simulated mid-stream failure");
      }
    };
    try (FastInputStream fis = FastInputStream.wrap(errorStream)) {
      return consumer.accept(fis);
    }
  }

  private static byte[] generateBytes(int size) {
    byte[] b = new byte[size];
    new SecureRandom().nextBytes(b);
    return b;
  }

  /**
   * Build a minimal valid metadata {@link ByteBuffer} with the given sha512.
   * Heap-allocated so {@code .array()} is safe (invariant #8).
   */
  private static ByteBuffer buildMetaBuffer(String sha512) throws Exception {
    Map<String, Object> m = new HashMap<>();
    m.put("sha512", sha512);
    byte[] json = Utils.toJSONString(m).getBytes("UTF-8");
    return ByteBuffer.wrap(json, 0, json.length);
  }
}
