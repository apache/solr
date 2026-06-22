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

import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.BufferMetrics;
import org.apache.solr.update.TransactionLog.LogReader;
import org.apache.solr.update.TransactionLog.ReverseReader;
import org.junit.Test;

/**
 * End-to-end coverage for the {@code BufferMetrics} active-reader gauge wiring in
 * {@link TransactionLog}. Invariant #3 (no unmap while an active reader/replayer exists) relies on
 * the tlog refcount; this gauge gives that lifecycle external observability. These tests open real
 * {@link LogReader}/{@link ReverseReader} instances and assert the gauge moves with them.
 *
 * <p>The registry is a process-wide singleton, so the assertions use before/after deltas taken
 * immediately around each open/close rather than absolute values, keeping them independent of any
 * readers other tests (or the tlog's own internals) may have outstanding.
 */
public class TransactionLogActiveReadersTest extends SolrTestCase {

  private TransactionLog newTlog() {
    String tlogFileName =
        String.format(Locale.ROOT, UpdateLog.LOG_FILENAME_PATTERN, UpdateLog.TLOG_NAME, Long.MAX_VALUE);
    Path path = SolrTestUtil.createTempDir();
    File logFile = new File(path.toFile(), tlogFileName);
    return new TransactionLog(logFile, new ArrayList<>());
  }

  private void writeDoc(TransactionLog tlog, long v) {
    AddUpdateCommand cmd = new AddUpdateCommand(null);
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", Long.toString(v));
    doc.addField("_version_", v);
    cmd.solrDoc = doc;
    cmd.setVersion(v);
    tlog.write(cmd);
  }

  /** A forward {@link LogReader} bumps the active-reader gauge while open and restores it on close. */
  @Test
  public void testForwardReaderTracksActiveReaders() throws Exception {
    BufferMetrics m = BufferMetrics.getInstance();
    try (TransactionLog tlog = newTlog()) {
      writeDoc(tlog, 1000);

      long before = m.getActiveReaders();
      LogReader reader = tlog.getReader(0);
      try {
        assertEquals("opening a LogReader must increment the active-reader gauge", before + 1, m.getActiveReaders());
        assertNotNull(reader.next());
      } finally {
        reader.close();
      }
      assertEquals("closing a LogReader must restore the active-reader gauge", before, m.getActiveReaders());
    }
  }

  /** A {@link ReverseReader} (FSReverseReader) bumps the gauge while open and restores it on close. */
  @Test
  public void testReverseReaderTracksActiveReaders() throws Exception {
    BufferMetrics m = BufferMetrics.getInstance();
    try (TransactionLog tlog = newTlog()) {
      writeDoc(tlog, 2000);

      long before = m.getActiveReaders();
      ReverseReader reader = tlog.getReverseReader();
      try {
        assertEquals("opening a ReverseReader must increment the active-reader gauge", before + 1, m.getActiveReaders());
        assertNotNull(reader.next());
      } finally {
        reader.close();
      }
      assertEquals("closing a ReverseReader must restore the active-reader gauge", before, m.getActiveReaders());
    }
  }

  /** Concurrently open readers each contribute exactly one to the gauge and unwind cleanly. */
  @Test
  public void testMultipleConcurrentReadersCountIndependently() throws Exception {
    BufferMetrics m = BufferMetrics.getInstance();
    try (TransactionLog tlog = newTlog()) {
      writeDoc(tlog, 3000);
      writeDoc(tlog, 3001);

      long before = m.getActiveReaders();
      LogReader r1 = tlog.getReader(0);
      LogReader r2 = tlog.getReader(0);
      ReverseReader r3 = tlog.getReverseReader();
      try {
        assertEquals("three open readers -> gauge +3", before + 3, m.getActiveReaders());
      } finally {
        r1.close();
        assertEquals(before + 2, m.getActiveReaders());
        r2.close();
        assertEquals(before + 1, m.getActiveReaders());
        r3.close();
      }
      assertEquals("all readers closed -> gauge back to baseline", before, m.getActiveReaders());
    }
  }
}
