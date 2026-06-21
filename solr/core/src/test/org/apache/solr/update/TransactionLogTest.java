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
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.update.TransactionLog.LogReader;
import org.apache.solr.update.TransactionLog.ReverseReader;
import org.junit.Test;

public class TransactionLogTest extends SolrTestCase {

  @Test
  public void testBigLastAddSize() {
    String tlogFileName = String.format(Locale.ROOT, UpdateLog.LOG_FILENAME_PATTERN, UpdateLog.TLOG_NAME,
        Long.MAX_VALUE);
    Path path = SolrTestUtil.createTempDir();
    File logFile = new File(path.toFile(), tlogFileName);
    try (TransactionLog transactionLog = new TransactionLog(logFile, new ArrayList<>())) {
      transactionLog.lastAddSize = 2000000000;
      AddUpdateCommand updateCommand = new AddUpdateCommand(null);
      updateCommand.solrDoc = new SolrInputDocument();
      transactionLog.write(updateCommand);
    }
  }

  @Test
  public void testReverseReaderConcurrentWrites() throws Exception {
    String tlogFileName = String.format(Locale.ROOT, UpdateLog.LOG_FILENAME_PATTERN, UpdateLog.TLOG_NAME,
        Long.MAX_VALUE);
    Path path = SolrTestUtil.createTempDir();
    File logFile = new File(path.toFile(), tlogFileName);
    final int numThreads = 4;
    final int perThread = 200;
    final int total = numThreads * perThread;
    try (TransactionLog tlog = new TransactionLog(logFile, new ArrayList<>())) {
      Thread[] threads = new Thread[numThreads];
      final java.util.concurrent.atomic.AtomicLong ver = new java.util.concurrent.atomic.AtomicLong(1000);
      for (int t = 0; t < numThreads; t++) {
        threads[t] = new Thread(() -> {
          for (int i = 0; i < perThread; i++) {
            AddUpdateCommand cmd = new AddUpdateCommand(null);
            SolrInputDocument doc = new SolrInputDocument();
            long v = ver.incrementAndGet();
            doc.addField("id", Long.toString(v));
            doc.addField("_version_", v);
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < (int) (v % 17); j++) sb.append("abcdefg");
            doc.addField("payload_s", sb.toString());
            cmd.solrDoc = doc;
            cmd.setVersion(v);
            tlog.write(cmd);
          }
        });
      }
      for (Thread th : threads) th.start();
      for (Thread th : threads) th.join();

      int listRecords = 0;
      ReverseReader reader = tlog.getReverseReader();
      try {
        Object o;
        while ((o = reader.next()) != null) {
          if (o instanceof java.util.List) listRecords++;
        }
      } finally {
        reader.close();
      }
      assertEquals("reverse reader did not return all add records (concurrent)", total, listRecords);
    }
  }

  @Test
  public void testReverseReaderManyRecords() throws IOException {
    // Regression for the reverse-reader false-EOF bug: DirectMemBufferedInputStream.read()
    // returned a signed byte, so any 0xFF byte in a record's varint (e.g. tlog version 4095,
    // whose encoding contains 0xFF) was read as -1 and misinterpreted as EOF by
    // JavaBinCodec.read(), truncating RecentUpdates.getVersions() and breaking PeerSync.
    String tlogFileName = String.format(Locale.ROOT, UpdateLog.LOG_FILENAME_PATTERN, UpdateLog.TLOG_NAME,
        Long.MAX_VALUE);
    Path path = SolrTestUtil.createTempDir();
    File logFile = new File(path.toFile(), tlogFileName);
    int numDocs = 120;
    try (TransactionLog tlog = new TransactionLog(logFile, new ArrayList<>())) {
      for (int i = 0; i < numDocs; i++) {
        AddUpdateCommand cmd = new AddUpdateCommand(null);
        SolrInputDocument doc = new SolrInputDocument();
        long v = 4000 + i;
        doc.addField("id", Long.toString(v));
        doc.addField("_version_", v);
        cmd.solrDoc = doc;
        cmd.setVersion(v);
        tlog.write(cmd);
      }

      int listRecords = 0;
      ReverseReader reader = tlog.getReverseReader();
      try {
        Object o;
        while ((o = reader.next()) != null) {
          if (o instanceof java.util.List) listRecords++;
        }
      } finally {
        reader.close();
      }
      assertEquals("reverse reader did not return all add records", numDocs, listRecords);
    }
  }

  @Test
  public void testUUID() throws IOException, InterruptedException {
    String tlogFileName = String.format(Locale.ROOT, UpdateLog.LOG_FILENAME_PATTERN, UpdateLog.TLOG_NAME,
        Long.MAX_VALUE);
    Path path = SolrTestUtil.createTempDir();
    File logFile = new File(path.toFile(), tlogFileName);
    UUID uuid = UUID.randomUUID();
    try (TransactionLog tlog = new TransactionLog(logFile, new ArrayList<>())) {
      tlog.deleteOnClose = false;
      AddUpdateCommand updateCommand = new AddUpdateCommand(null);

      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("uuid", uuid);
      updateCommand.solrDoc = doc;

      tlog.write(updateCommand);
    }

    try (TransactionLog tlog = new TransactionLog(logFile, new ArrayList<>(), true)) {
      LogReader reader = tlog.getReader(0);
      Object entry = reader.next();
      assertNotNull(entry);
      SolrInputDocument doc = (SolrInputDocument) ((List<?>) entry).get(2);
      assertEquals(uuid, (UUID) doc.getFieldValue("uuid"));
    }
  }
}
