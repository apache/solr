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

import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Lightweight micro-harness for the Agrona buffer-hardening hot paths.
 *
 * <p><b>Gating:</b> all test methods call {@link #assumeBenchEnabled()} which skips unless
 * {@code -Dsolr.bench=true} is set. This means the class compiles and lives in the normal
 * test source tree but is NEVER executed during a normal CI run (all methods are skipped).
 *
 * <p><b>Running:</b>
 * <pre>
 *   /home/markmiller/solr-ref/gradlew :solr:core:test \
 *       --tests "org.apache.solr.update.BufferHardeningBenchmarks" \
 *       -Dsolr.bench=true
 * </pre>
 *
 * <p>Results appear at ERROR log level (INFO is suppressed for o.a.s.* in the test log4j
 * config) so they show up in the gradle console.
 *
 * <p><b>What each benchmark measures:</b>
 * <ul>
 *   <li>Elapsed wall-clock nanos converted to throughput (ops/s) and per-op latency.</li>
 *   <li>Heap allocation delta via {@code Runtime.totalMemory() - freeMemory()} before/after.
 *       This is a lower bound — GC noise can make it negative; treat it as order-of-magnitude.
 *       Run with {@code -Xmx512m} for less noise.</li>
 *   <li>p50/p95/p99 latency from per-op nanoTime samples stored in a pre-allocated
 *       {@code long[]} to avoid allocation during measurement.</li>
 * </ul>
 *
 * <p><b>Benchmark inventory (real vs stubbed):</b>
 * <ul>
 *   <li>REAL: benchTlogAddThroughput, benchTlogDeleteThroughput, benchTlogDbqThroughput,
 *       benchTlogCommitThroughput, benchTlogLookupLatency, benchReverseReaderScan,
 *       benchJavabinEncodeDecode</li>
 *   <li>STUBBED (compile-only): benchRecoveryReplay, benchRecentUpdatesReverseScan,
 *       benchByteBuffersDataOutputWrite — each stub documents exactly what it would need.</li>
 * </ul>
 */
public class BufferHardeningBenchmarks extends SolrTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** Number of ops for write-throughput benchmarks. */
  private static final int WRITE_OPS = 10_000;
  /** Number of ops for commit-throughput benchmark (keep lower — each commit appends END_MESSAGE). */
  private static final int COMMIT_OPS = 500;
  /** Number of ops for lookup/read benchmarks. */
  private static final int READ_OPS = 1_000;
  /** Warmup fraction: run 1/WARMUP_FRACTION of total ops first to let JIT settle. */
  private static final int WARMUP_FRACTION = 5;

  // ─── gating ────────────────────────────────────────────────────────────────

  @BeforeClass
  public static void checkSysProp() {
    if (!Boolean.getBoolean("solr.bench")) {
      log.error("BENCH: benchmarks skipped (set -Dsolr.bench=true to run)");
    }
  }

  private static void assumeBenchEnabled() {
    Assume.assumeTrue("Set -Dsolr.bench=true to run benchmarks", Boolean.getBoolean("solr.bench"));
  }

  // ─── tlog setup helpers ────────────────────────────────────────────────────

  private static File newTlogFile() {
    String name = String.format(Locale.ROOT,
        UpdateLog.LOG_FILENAME_PATTERN, UpdateLog.TLOG_NAME, System.nanoTime());
    Path dir = SolrTestUtil.createTempDir();
    return new File(dir.toFile(), name);
  }

  private static AddUpdateCommand makeAdd(long version) {
    AddUpdateCommand cmd = new AddUpdateCommand(null);
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", Long.toString(version));
    doc.addField("_version_", version);
    doc.addField("text_s", "The quick brown fox jumped over the lazy dog. version=" + version);
    cmd.solrDoc = doc;
    cmd.setVersion(version);
    return cmd;
  }

  private static DeleteUpdateCommand makeDelete(long version) {
    DeleteUpdateCommand cmd = new DeleteUpdateCommand(null);
    cmd.id = Long.toString(version);
    cmd.setVersion(version);
    return cmd;
  }

  private static CommitUpdateCommand makeCommit(long version) {
    CommitUpdateCommand cmd = new CommitUpdateCommand(null, false);
    cmd.setVersion(version);
    return cmd;
  }

  // ─── latency statistics ────────────────────────────────────────────────────

  /**
   * Returns p50/p95/p99 nanos from the first {@code count} entries of {@code samples}.
   * Sorts a copy of the array; original is unchanged.
   */
  private static long[] percentiles(long[] samples, int count) {
    long[] s = Arrays.copyOf(samples, count);
    Arrays.sort(s);
    return new long[]{
        s[(int) (count * 0.50)],
        s[(int) (count * 0.95)],
        s[Math.min((int) (count * 0.99), count - 1)]
    };
  }

  private static long heapUsedBytes() {
    Runtime rt = Runtime.getRuntime();
    return rt.totalMemory() - rt.freeMemory();
  }

  // ─── REAL benchmark 1: tlog ADD throughput ─────────────────────────────────

  /**
   * Measures {@link TransactionLog#write(AddUpdateCommand)} throughput.
   *
   * <p>Covers: the hot write path — serialize into an Agrona ExpandableBuffer, acquire
   * fosLock, ensureCapacity (possibly remapping), putBytes to the MappedResizeableBuffer.
   *
   * <p>Run command:
   * <pre>
   *   /home/markmiller/solr-ref/gradlew :solr:core:test \
   *       --tests "org.apache.solr.update.BufferHardeningBenchmarks.benchTlogAddThroughput" \
   *       -Dsolr.bench=true
   * </pre>
   */
  @Test
  public void benchTlogAddThroughput() throws Exception {
    assumeBenchEnabled();
    int ops = WRITE_OPS;
    int warmup = ops / WARMUP_FRACTION;
    long[] samples = new long[ops];

    try (TransactionLog tlog = new TransactionLog(newTlogFile(), new ArrayList<>())) {
      for (int i = 0; i < warmup; i++) {
        tlog.write(makeAdd(i));
      }

      long heapBefore = heapUsedBytes();
      long wallStart = System.nanoTime();
      for (int i = 0; i < ops; i++) {
        long t0 = System.nanoTime();
        tlog.write(makeAdd(warmup + i));
        samples[i] = System.nanoTime() - t0;
      }
      long wallElapsed = System.nanoTime() - wallStart;
      long heapAfter = heapUsedBytes();

      long[] pct = percentiles(samples, ops);
      log.error("BENCH tlogAdd: ops={} throughput={} ops/s  p50={}ns p95={}ns p99={}ns  heapDelta={}KB",
          ops,
          String.format(Locale.ROOT, "%,.0f", (double) ops / wallElapsed * 1e9),
          pct[0], pct[1], pct[2],
          (heapAfter - heapBefore) / 1024);
    }
  }

  // ─── REAL benchmark 2: tlog DELETE throughput ──────────────────────────────

  /**
   * Measures {@link TransactionLog#writeDelete(DeleteUpdateCommand)} throughput.
   *
   * <p>Run command:
   * <pre>
   *   /home/markmiller/solr-ref/gradlew :solr:core:test \
   *       --tests "org.apache.solr.update.BufferHardeningBenchmarks.benchTlogDeleteThroughput" \
   *       -Dsolr.bench=true
   * </pre>
   */
  @Test
  public void benchTlogDeleteThroughput() throws Exception {
    assumeBenchEnabled();
    int ops = WRITE_OPS;
    int warmup = ops / WARMUP_FRACTION;
    long[] samples = new long[ops];

    try (TransactionLog tlog = new TransactionLog(newTlogFile(), new ArrayList<>())) {
      for (int i = 0; i < warmup; i++) {
        tlog.writeDelete(makeDelete(i));
      }

      long heapBefore = heapUsedBytes();
      long wallStart = System.nanoTime();
      for (int i = 0; i < ops; i++) {
        long t0 = System.nanoTime();
        tlog.writeDelete(makeDelete(warmup + i));
        samples[i] = System.nanoTime() - t0;
      }
      long wallElapsed = System.nanoTime() - wallStart;
      long heapAfter = heapUsedBytes();

      long[] pct = percentiles(samples, ops);
      log.error("BENCH tlogDelete: ops={} throughput={} ops/s  p50={}ns p95={}ns p99={}ns  heapDelta={}KB",
          ops,
          String.format(Locale.ROOT, "%,.0f", (double) ops / wallElapsed * 1e9),
          pct[0], pct[1], pct[2],
          (heapAfter - heapBefore) / 1024);
    }
  }

  // ─── REAL benchmark 3: tlog DELETE_BY_QUERY throughput ─────────────────────

  /**
   * Measures {@link TransactionLog#writeDeleteByQuery(DeleteUpdateCommand)} throughput.
   *
   * <p>Run command:
   * <pre>
   *   /home/markmiller/solr-ref/gradlew :solr:core:test \
   *       --tests "org.apache.solr.update.BufferHardeningBenchmarks.benchTlogDbqThroughput" \
   *       -Dsolr.bench=true
   * </pre>
   */
  @Test
  public void benchTlogDbqThroughput() throws Exception {
    assumeBenchEnabled();
    int ops = WRITE_OPS;
    int warmup = ops / WARMUP_FRACTION;
    long[] samples = new long[ops];

    try (TransactionLog tlog = new TransactionLog(newTlogFile(), new ArrayList<>())) {
      for (int i = 0; i < warmup; i++) {
        DeleteUpdateCommand cmd = new DeleteUpdateCommand(null);
        cmd.query = "type_s:foo";
        cmd.setVersion(i);
        tlog.writeDeleteByQuery(cmd);
      }

      long heapBefore = heapUsedBytes();
      long wallStart = System.nanoTime();
      for (int i = 0; i < ops; i++) {
        DeleteUpdateCommand cmd = new DeleteUpdateCommand(null);
        cmd.query = "type_s:foo AND id:" + (warmup + i);
        cmd.setVersion(warmup + i);
        long t0 = System.nanoTime();
        tlog.writeDeleteByQuery(cmd);
        samples[i] = System.nanoTime() - t0;
      }
      long wallElapsed = System.nanoTime() - wallStart;
      long heapAfter = heapUsedBytes();

      long[] pct = percentiles(samples, ops);
      log.error("BENCH tlogDbq: ops={} throughput={} ops/s  p50={}ns p95={}ns p99={}ns  heapDelta={}KB",
          ops,
          String.format(Locale.ROOT, "%,.0f", (double) ops / wallElapsed * 1e9),
          pct[0], pct[1], pct[2],
          (heapAfter - heapBefore) / 1024);
    }
  }

  // ─── REAL benchmark 4: tlog COMMIT throughput ──────────────────────────────

  /**
   * Measures {@link TransactionLog#writeCommit(CommitUpdateCommand)} throughput.
   *
   * <p>Each commit is preceded by one ADD to approximate realistic usage (a tlog
   * with only commits would be pathological).
   *
   * <p>Run command:
   * <pre>
   *   /home/markmiller/solr-ref/gradlew :solr:core:test \
   *       --tests "org.apache.solr.update.BufferHardeningBenchmarks.benchTlogCommitThroughput" \
   *       -Dsolr.bench=true
   * </pre>
   */
  @Test
  public void benchTlogCommitThroughput() throws Exception {
    assumeBenchEnabled();
    int ops = COMMIT_OPS;
    int warmup = ops / WARMUP_FRACTION;
    long[] samples = new long[ops];

    try (TransactionLog tlog = new TransactionLog(newTlogFile(), new ArrayList<>())) {
      for (int i = 0; i < warmup; i++) {
        tlog.write(makeAdd(i));
        tlog.writeCommit(makeCommit(i));
      }

      long heapBefore = heapUsedBytes();
      long wallStart = System.nanoTime();
      for (int i = 0; i < ops; i++) {
        tlog.write(makeAdd(warmup + i));
        long t0 = System.nanoTime();
        tlog.writeCommit(makeCommit(warmup + i));
        samples[i] = System.nanoTime() - t0;
      }
      long wallElapsed = System.nanoTime() - wallStart;
      long heapAfter = heapUsedBytes();

      long[] pct = percentiles(samples, ops);
      log.error("BENCH tlogCommit: ops={} throughput={} ops/s  p50={}ns p95={}ns p99={}ns  heapDelta={}KB",
          ops,
          String.format(Locale.ROOT, "%,.0f", (double) ops / wallElapsed * 1e9),
          pct[0], pct[1], pct[2],
          (heapAfter - heapBefore) / 1024);
    }
  }

  // ─── REAL benchmark 5: tlog lookup latency ─────────────────────────────────

  /**
   * Measures {@link TransactionLog#lookup(long)} random-position latency.
   *
   * <p>Captures positions from {@link TransactionLog#write} for READ_OPS records,
   * shuffles them with a fixed seed, then measures per-lookup time. Covers the RTG
   * (real-time-get by version pointer) hot path.
   *
   * <p>Run command:
   * <pre>
   *   /home/markmiller/solr-ref/gradlew :solr:core:test \
   *       --tests "org.apache.solr.update.BufferHardeningBenchmarks.benchTlogLookupLatency" \
   *       -Dsolr.bench=true
   * </pre>
   */
  @Test
  public void benchTlogLookupLatency() throws Exception {
    assumeBenchEnabled();
    int ops = READ_OPS;
    long[] positions = new long[ops];
    long[] samples = new long[ops];

    try (TransactionLog tlog = new TransactionLog(newTlogFile(), new ArrayList<>())) {
      for (int i = 0; i < ops; i++) {
        positions[i] = tlog.write(makeAdd(i + 1));
      }

      // Shuffle positions with a fixed seed for repeatability across runs.
      java.util.Random rng = new java.util.Random(0xdeadbeefL);
      for (int i = ops - 1; i > 0; i--) {
        int j = rng.nextInt(i + 1);
        long tmp = positions[i]; positions[i] = positions[j]; positions[j] = tmp;
      }

      int warmup = ops / WARMUP_FRACTION;
      for (int i = 0; i < warmup; i++) {
        tlog.lookup(positions[i % ops]);
      }

      long heapBefore = heapUsedBytes();
      long wallStart = System.nanoTime();
      for (int i = 0; i < ops; i++) {
        long t0 = System.nanoTime();
        Object result = tlog.lookup(positions[i]);
        samples[i] = System.nanoTime() - t0;
        if (result == null) throw new AssertionError("null lookup at pos=" + positions[i]);
      }
      long wallElapsed = System.nanoTime() - wallStart;
      long heapAfter = heapUsedBytes();

      long[] pct = percentiles(samples, ops);
      log.error("BENCH tlogLookup: ops={} throughput={} ops/s  p50={}ns p95={}ns p99={}ns  heapDelta={}KB",
          ops,
          String.format(Locale.ROOT, "%,.0f", (double) ops / wallElapsed * 1e9),
          pct[0], pct[1], pct[2],
          (heapAfter - heapBefore) / 1024);
    }
  }

  // ─── REAL benchmark 6: FSReverseReader full reverse scan ───────────────────

  /**
   * Measures {@link TransactionLog#getReverseReader()} full reverse scan over 5,000 ADDs.
   *
   * <p>Exercises the {@code RecentUpdates} / PeerSync path: reads the tlog backward to
   * collect recent version numbers. One warmup scan is done before measurement.
   *
   * <p>Run command:
   * <pre>
   *   /home/markmiller/solr-ref/gradlew :solr:core:test \
   *       --tests "org.apache.solr.update.BufferHardeningBenchmarks.benchReverseReaderScan" \
   *       -Dsolr.bench=true
   * </pre>
   */
  @Test
  public void benchReverseReaderScan() throws Exception {
    assumeBenchEnabled();
    int recordCount = 5_000;

    try (TransactionLog tlog = new TransactionLog(newTlogFile(), new ArrayList<>())) {
      for (int i = 0; i < recordCount; i++) {
        tlog.write(makeAdd(i + 1));
      }

      // Warmup: one full reverse scan.
      {
        TransactionLog.ReverseReader warmupReader = tlog.getReverseReader();
        try {
          int wc = 0;
          while (warmupReader.next() != null) wc++;
          if (wc == 0) throw new AssertionError("warmup reverse reader returned no records");
        } finally {
          warmupReader.close();
        }
      }

      long heapBefore = heapUsedBytes();
      long wallStart = System.nanoTime();
      int scanned = 0;
      TransactionLog.ReverseReader reader = tlog.getReverseReader();
      try {
        while (reader.next() != null) {
          scanned++;
        }
      } finally {
        reader.close();
      }
      long wallElapsed = System.nanoTime() - wallStart;
      long heapAfter = heapUsedBytes();

      log.error("BENCH reverseReaderScan: records={} scanned={} elapsed={}ms throughput={} records/s  heapDelta={}KB",
          recordCount, scanned,
          wallElapsed / 1_000_000,
          String.format(Locale.ROOT, "%,.0f", (double) scanned / wallElapsed * 1e9),
          (heapAfter - heapBefore) / 1024);

      if (scanned != recordCount) {
        throw new AssertionError("Expected " + recordCount + " records, got " + scanned);
      }
    }
  }

  // ─── REAL benchmark 7: JavaBin encode/decode ───────────────────────────────

  /**
   * Measures {@link JavaBinCodec} marshal + unmarshal throughput for a 10-doc NamedList
   * response.
   *
   * <p>Covers the hot path used by HTTP/2 streaming: {@code ConcurrentUpdateHttp2SolrClient}
   * marshals a batch into a pooled Agrona buffer via {@code JavaBinCodec.marshal()}. A plain
   * {@link ByteArrayOutputStream} is used here to keep the benchmark fully self-contained
   * (no Agrona dependency required).
   *
   * <p>Run command:
   * <pre>
   *   /home/markmiller/solr-ref/gradlew :solr:core:test \
   *       --tests "org.apache.solr.update.BufferHardeningBenchmarks.benchJavabinEncodeDecode" \
   *       -Dsolr.bench=true
   * </pre>
   */
  @Test
  public void benchJavabinEncodeDecode() throws Exception {
    assumeBenchEnabled();
    int ops = 2_000;
    int warmup = ops / WARMUP_FRACTION;
    long[] encodeSamples = new long[ops];
    long[] decodeSamples = new long[ops];

    NamedList<Object> response = buildResponsePayload(10);

    // Pre-encode once to get a stable byte[] size for capacity hints.
    byte[] sizeProbe;
    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
      new JavaBinCodec().marshal(response, baos);
      sizeProbe = baos.toByteArray();
    }
    int payloadBytes = sizeProbe.length;

    // Warmup
    for (int i = 0; i < warmup; i++) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(payloadBytes);
      new JavaBinCodec().marshal(response, baos);
      new JavaBinCodec().unmarshal(baos.toByteArray());
    }

    long heapBefore = heapUsedBytes();
    long wallStart = System.nanoTime();
    for (int i = 0; i < ops; i++) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(payloadBytes);
      long t0 = System.nanoTime();
      new JavaBinCodec().marshal(response, baos);
      encodeSamples[i] = System.nanoTime() - t0;

      byte[] bytes = baos.toByteArray();
      long t1 = System.nanoTime();
      Object decoded = new JavaBinCodec().unmarshal(bytes);
      decodeSamples[i] = System.nanoTime() - t1;

      if (decoded == null) throw new AssertionError("null decode at op=" + i);
    }
    long wallElapsed = System.nanoTime() - wallStart;
    long heapAfter = heapUsedBytes();

    long[] encPct = percentiles(encodeSamples, ops);
    long[] decPct = percentiles(decodeSamples, ops);
    log.error("BENCH javabinEncode: ops={} payloadBytes={} throughput={} ops/s  p50={}ns p95={}ns p99={}ns",
        ops, payloadBytes,
        String.format(Locale.ROOT, "%,.0f", (double) ops / wallElapsed * 1e9),
        encPct[0], encPct[1], encPct[2]);
    log.error("BENCH javabinDecode: ops={} payloadBytes={} throughput={} ops/s  p50={}ns p95={}ns p99={}ns  heapDelta={}KB",
        ops, payloadBytes,
        String.format(Locale.ROOT, "%,.0f", (double) ops / wallElapsed * 1e9),
        decPct[0], decPct[1], decPct[2],
        (heapAfter - heapBefore) / 1024);
  }

  /** Builds a NamedList resembling a Solr query response with {@code docCount} SolrInputDocuments. */
  private static NamedList<Object> buildResponsePayload(int docCount) {
    NamedList<Object> response = new NamedList<>();
    NamedList<Object> header = new NamedList<>();
    header.add("status", 0);
    header.add("QTime", 42);
    response.add("responseHeader", header);

    List<SolrInputDocument> docs = new ArrayList<>(docCount);
    for (int i = 0; i < docCount; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "doc-" + i);
      doc.addField("_version_", (long) (1000 + i));
      doc.addField("title_t", "The quick brown fox document number " + i + " with extra text.");
      doc.addField("category_s", "cat-" + (i % 5));
      doc.addField("score", (float) i * 0.1f);
      docs.add(doc);
    }
    response.add("docs", docs);
    return response;
  }

  // ─── STUBBED benchmark A: recovery replay throughput ───────────────────────

  /**
   * STUB — not runnable in isolation.
   *
   * <p>What it would measure: {@link UpdateLog} recovery replay throughput —
   * {@code applyBufferedUpdates()} and {@code copyOverOldUpdates()} from a pre-built
   * tlog chain into a fresh {@link org.apache.solr.search.SolrIndexSearcher}.
   *
   * <p>Why stubbed: recovery replay requires a fully initialised {@code SolrCore} with an
   * {@code IndexWriter}, a live {@code UpdateLog}, and an {@code IndexSchema}. Constructing
   * these in isolation requires {@code SolrTestCaseJ4.initCore()} or
   * {@code MiniSolrCloudCluster}, which is incompatible with a dependency-free
   * micro-harness. The replay path also requires a functioning {@code RequestHandlerBase}
   * and update-processor chain.
   *
   * <p>To enable: extend {@code SolrTestCaseJ4}, call
   * {@code initCore("solrconfig.xml","schema.xml")}, populate the tlog via
   * {@code DirectUpdateHandler2}, close and reopen the core, then time
   * {@code UpdateLog.waitForRecovery()}.
   */
  @Test
  public void benchRecoveryReplay() {
    assumeBenchEnabled();
    log.error("BENCH recoveryReplay: STUBBED — requires SolrCore + UpdateLog + IndexWriter fixture. See javadoc.");
  }

  // ─── STUBBED benchmark B: RecentUpdates reverse scan ──────────────────────

  /**
   * STUB — not runnable in isolation.
   *
   * <p>What it would measure: {@link UpdateLog.RecentUpdates} reverse-scan throughput —
   * the time to call {@code ulog.getRecentUpdates()} which opens up to
   * {@code numRecordsToKeep} LogReaders in reverse order to build the in-memory version
   * map used by PeerSync.
   *
   * <p>Why stubbed: {@code UpdateLog.getRecentUpdates()} internally calls
   * {@code tlog.getSortedReader()} (SortedLogReader) which depends on the UpdateLog's
   * {@code versionInfo} and {@code map} structures, initialised only by
   * {@code UpdateLog.init(UpdateHandler, SolrCore)}. That chain requires a live
   * {@code SolrCore}. Additionally, {@code UpdateLog} reads its tlog file list from a
   * directory managed by {@code SolrCore.getDataDir()}.
   *
   * <p>To enable: extend {@code SolrTestCaseJ4}, call {@code initCore()}, write records
   * via the normal update handler, then time
   * {@code h.getCore().getUpdateHandler().getUpdateLog().getRecentUpdates()}.
   */
  @Test
  public void benchRecentUpdatesReverseScan() {
    assumeBenchEnabled();
    log.error("BENCH recentUpdatesReverseScan: STUBBED — requires SolrCore + UpdateLog. See javadoc.");
  }

  // ─── STUBBED benchmark C: ByteBuffersDataOutput write throughput ────────────

  /**
   * STUB — not runnable in the current codebase state.
   *
   * <p>What it would measure: {@link org.apache.solr.filestore.ByteBuffersDataOutput}
   * sequential write throughput for 4 KB, 64 KB, and 1 MiB payloads, including the
   * direct/heap threshold crossover controlled by
   * {@code solr.bytebuffersdir.directThreshold}.
   *
   * <p>Why stubbed: the fork's {@code ByteBuffersDataOutput} write path is intentionally
   * gutted — the block-allocation machinery is commented out, {@code currentBlock = EMPTY}
   * (capacity 0), so any non-zero {@code writeByte()} call throws
   * {@code IndexOutOfBoundsException}. Resurrecting it is out of scope for the
   * buffer-hardening phase (CLAUDE.md Wave-2 closeout note).
   *
   * <p>To enable: restore the block-allocation machinery in
   * {@code ByteBuffersDataOutput.allocateNewBlock()}, then call
   * {@code writeByte()} / {@code writeBytes()} in a loop and time it.
   */
  @Test
  public void benchByteBuffersDataOutputWrite() {
    assumeBenchEnabled();
    log.error("BENCH byteBuffersDataOutputWrite: STUBBED — ByteBuffersDataOutput write path is intentionally gutted (CLAUDE.md Wave-2 closeout). Cannot benchmark until resurrected.");
  }
}
