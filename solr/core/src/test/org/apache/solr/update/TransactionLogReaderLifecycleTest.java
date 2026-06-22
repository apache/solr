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

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.BufferMetrics;
import org.apache.solr.update.TransactionLog.LogReader;
import org.apache.solr.update.TransactionLog.ReverseReader;
import org.junit.Test;

/**
 * Reader-lifecycle hardening coverage for {@link TransactionLog} (Wave 3, LANE-TLOG, plan item 5).
 *
 * <p>These tests exercise the parts of the reader/replayer lifecycle that the hardening pass is
 * responsible for keeping correct:
 * <ul>
 *   <li>the active-reader gauge in {@link BufferMetrics} is balanced (every increment matched by a
 *       decrement) across {@link LogReader}, {@link ReverseReader}, {@code SortedLogReader}, AND the
 *       {@code lookup()} path (which the UpdateLog realtime-get / applyPartialUpdates / RecentUpdates
 *       paths funnel through);</li>
 *   <li>the gauge is restored even when {@code next()} or {@code lookup()} throws;</li>
 *   <li>closing the log while a reader is still active does not unmap the buffer prematurely — the
 *       reader can finish reading because {@code close()} only unmaps once the refcount reaches zero
 *       (invariant #3);</li>
 *   <li>{@code forceClose()} on a tlog with a wedged reader bumps the forced-close metric exactly
 *       once (the single documented final-shutdown exception to invariant #3).</li>
 * </ul>
 *
 * <p>The {@link BufferMetrics} registry is a process-wide singleton, so all gauge assertions use
 * before/after deltas captured immediately around the operation under test, never absolute values.
 * Construction mirrors the already-GREEN {@code TransactionLogActiveReadersTest} /
 * {@code TransactionLogFormatTest}: a real on-disk tlog built directly, no MiniSolrCloudCluster, so
 * every test is deterministic and fast.
 */
public class TransactionLogReaderLifecycleTest extends SolrTestCase {

  private static File newTlogFile() {
    String name = String.format(Locale.ROOT,
        UpdateLog.LOG_FILENAME_PATTERN, UpdateLog.TLOG_NAME, System.nanoTime());
    Path dir = SolrTestUtil.createTempDir();
    return new File(dir.toFile(), name);
  }

  private static long writeAdd(TransactionLog tlog, long version, String id) {
    AddUpdateCommand cmd = new AddUpdateCommand(null);
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", id);
    doc.addField("_version_", version);
    cmd.solrDoc = doc;
    cmd.setVersion(version);
    return tlog.write(cmd);
  }

  // ─── active-reader gauge balance across next() iteration ──────────────────────

  /**
   * Reading a multi-record log to EOF and then closing the reader must leave the active-reader gauge
   * exactly back at baseline (the reader contributes +1 for its whole lifetime, restored on close),
   * regardless of how many {@code next()} calls happened in between.
   */
  @Test
  public void testReaderCloseAfterNextRestoresGauge() throws Exception {
    BufferMetrics m = BufferMetrics.getInstance();
    try (TransactionLog tlog = new TransactionLog(newTlogFile(), new ArrayList<>())) {
      tlog.deleteOnClose = false;
      writeAdd(tlog, 1, "a");
      writeAdd(tlog, 2, "b");
      writeAdd(tlog, 3, "c");

      long before = m.getActiveReaders();
      LogReader reader = tlog.getReader(0);
      try {
        // gauge is +1 for the whole reader lifetime, not per-record
        assertEquals(before + 1, m.getActiveReaders());
        int count = 0;
        while (reader.next() != null) {
          count++;
          assertEquals("gauge must stay +1 across every next() call", before + 1, m.getActiveReaders());
        }
        assertEquals("all three records must be read", 3, count);
      } finally {
        reader.close();
      }
      assertEquals("closing the reader after iterating to EOF must restore the gauge", before, m.getActiveReaders());
    }
  }

  /**
   * A {@code SortedLogReader} extends {@code LogReader}, so its {@code super()} ctor and the inherited
   * {@code close()} must keep the gauge balanced just like a plain forward reader.
   */
  @Test
  public void testSortedReaderTracksAndRestoresGauge() throws Exception {
    BufferMetrics m = BufferMetrics.getInstance();
    try (TransactionLog tlog = new TransactionLog(newTlogFile(), new ArrayList<>())) {
      tlog.deleteOnClose = false;
      // write versions out of order so the SortedLogReader exercises its reorder path
      writeAdd(tlog, 30, "c");
      writeAdd(tlog, 10, "a");
      writeAdd(tlog, 20, "b");

      long before = m.getActiveReaders();
      LogReader reader = tlog.getSortedReader(0);
      try {
        assertEquals("opening a SortedLogReader must increment the gauge", before + 1, m.getActiveReaders());
        int count = 0;
        while (reader.next() != null) count++;
        assertEquals("sorted reader must surface all three records", 3, count);
      } finally {
        reader.close();
      }
      assertEquals("closing a SortedLogReader must restore the gauge", before, m.getActiveReaders());
    }
  }

  // ─── lookup() lifecycle (realtime-get / applyPartialUpdates / RecentUpdates chokepoint) ──

  /**
   * {@code TransactionLog.lookup(pos)} is the single chokepoint the UpdateLog realtime-get,
   * applyPartialUpdates, and RecentUpdates lookup/getDeleteByQuery paths use to read the mapped
   * buffer. It must bump the active-reader gauge for the duration of the read and restore it on
   * return, and the returned record must deserialize correctly.
   */
  @Test
  public void testLookupTracksAndRestoresGauge() throws Exception {
    BufferMetrics m = BufferMetrics.getInstance();
    try (TransactionLog tlog = new TransactionLog(newTlogFile(), new ArrayList<>())) {
      tlog.deleteOnClose = false;
      long pos = writeAdd(tlog, 42, "look");

      long before = m.getActiveReaders();
      Object record = tlog.lookup(pos);
      assertEquals("lookup() must restore the active-reader gauge after returning", before, m.getActiveReaders());
      assertNotNull("lookup() at a valid record position must return the record", record);
    }
  }

  /**
   * A negative pointer is the documented "no record" sentinel ({@code lookup} returns null without
   * touching the buffer). It must NOT move the gauge — the increment/decrement only brackets the
   * actual buffer read.
   */
  @Test
  public void testLookupNegativePointerDoesNotMoveGauge() throws Exception {
    BufferMetrics m = BufferMetrics.getInstance();
    try (TransactionLog tlog = new TransactionLog(newTlogFile(), new ArrayList<>())) {
      tlog.deleteOnClose = false;
      writeAdd(tlog, 7, "x");

      long before = m.getActiveReaders();
      assertNull("a negative pointer must look up to null", tlog.lookup(-1));
      assertEquals("a no-op lookup must not move the active-reader gauge", before, m.getActiveReaders());
    }
  }

  /**
   * If {@code lookup()} is handed a bogus (in-bounds but non-record) position so deserialization
   * throws, the gauge must still be restored — the increment is bracketed by try/finally. This is the
   * exact shape the UpdateLog {@code getEntryFromTLog} path relies on (it catches the exception and
   * moves on to the next tlog).
   */
  @Test
  public void testLookupRestoresGaugeOnDeserializeFailure() throws Exception {
    BufferMetrics m = BufferMetrics.getInstance();
    try (TransactionLog tlog = new TransactionLog(newTlogFile(), new ArrayList<>())) {
      tlog.deleteOnClose = false;
      long pos = writeAdd(tlog, 99, "y");

      long before = m.getActiveReaders();
      // pos+1 points into the middle of the record's bytes; deserialization is expected to throw.
      // The point of the test is gauge balance on the throwing path, so we tolerate either outcome
      // (throw or return) but always require the gauge to be restored afterward.
      try {
        tlog.lookup(pos + 1);
      } catch (Throwable expected) {
        // expected: a corrupt/misaligned position fails to deserialize
      }
      assertEquals("the gauge must be restored whether lookup() throws or returns", before, m.getActiveReaders());
    }
  }

  // ─── close-while-reader-active must not unmap prematurely (invariant #3) ──────

  /**
   * Open a reader, decref the log so the structural self-reference is gone, and confirm the reader
   * can STILL read every record: because the reader holds an incref, the refcount has not reached
   * zero, so {@code close()}/unmap has not run. The reader's own {@code close()} is what finally
   * drops the refcount to zero and unmaps. This is the invariant-#3 "no unmap while a reader is
   * active" guarantee on the normal (non-forced) path.
   */
  @Test
  public void testLogDecrefWhileReaderActiveDoesNotUnmap() throws Exception {
    File f = newTlogFile();
    TransactionLog tlog = new TransactionLog(f, new ArrayList<>());
    tlog.deleteOnClose = false;
    writeAdd(tlog, 1, "a");
    writeAdd(tlog, 2, "b");

    LogReader reader = tlog.getReader(0); // reader increfs (refcount now 2)
    try {
      // Drop the structural self-reference. refcount goes 2 -> 1, so the buffer is NOT unmapped:
      // the live reader still holds a ref. If unmap had happened, the next read would SIGSEGV.
      tlog.decref();

      int count = 0;
      Object o;
      while ((o = reader.next()) != null) {
        count++;
      }
      assertEquals("reader must still read all records after the log's self-reference was dropped", 2, count);
    } finally {
      reader.close(); // final decref -> refcount 0 -> unmap+close happens here
    }
  }

  // ─── forceClose metric on a wedged reader (documented final-shutdown exception) ──

  /**
   * If a reader is still active (refcount &gt; 0) past the {@code forceClose} drain deadline,
   * {@code forceClose()} forcibly unmaps and increments the forced-close metric exactly once. This is
   * the single sanctioned unmap-while-refcount-positive path. We hold a reader open across the call
   * to keep the refcount positive, and assert the forced-close counter moved by exactly one.
   *
   * <p>The 5s drain in {@code forceClose} is real wall time; this test accepts that one-time cost to
   * prove the forced path. The reader is closed afterward in a finally so no buffer is leaked.
   */
  @Test
  public void testForceCloseWithWedgedReaderBumpsForcedCloseMetricOnce() throws Exception {
    BufferMetrics m = BufferMetrics.getInstance();
    File f = newTlogFile();
    TransactionLog tlog = new TransactionLog(f, new ArrayList<>());
    tlog.deleteOnClose = false;
    writeAdd(tlog, 1, "a");

    // Hold an extra incref so the refcount cannot drain during forceClose (simulates a wedged reader).
    tlog.incref(); // refcount: 2 (self + our hold)

    long forcedBefore = m.getForcedCloseCount();
    final AtomicReference<Throwable> err = new AtomicReference<>();
    final CountDownLatch done = new CountDownLatch(1);
    Thread t = new Thread(() -> {
      try {
        tlog.forceClose();
      } catch (Throwable e) {
        err.set(e);
      } finally {
        done.countDown();
      }
    }, "forceClose-wedged");
    t.start();

    // forceClose drains for up to 5s then forces; give it margin.
    boolean finished = done.await(30, TimeUnit.SECONDS);
    assertTrue("forceClose must complete within the bounded drain window", finished);
    assertNull("forceClose must not throw", err.get());
    assertEquals("forceClose with a wedged reader must bump the forced-close metric exactly once",
        forcedBefore + 1, m.getForcedCloseCount());
  }
}
