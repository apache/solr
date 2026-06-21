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
package org.apache.solr;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.*;
import org.apache.lucene.search.AssertingIndexSearcher;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.AssertingSimilarity;
import org.apache.lucene.search.similarities.RandomSimilarity;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.FileSwitchDirectory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.store.RawDirectoryWrapper;
import org.apache.lucene.util.*;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.util.RandomizeSSL;
import org.apache.solr.util.SSLTestConfig;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SolrTestUtil {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final boolean VERBOSE = false;

  public SolrTestUtil() {
  }

  /**
   * Special hook for sanity checking if any tests trigger failures when an
   * Assumption failure occures in a {@link Before} method
   *
   * @lucene.internal
   */
  //  @Before
  //  public void checkSyspropForceBeforeAssumptionFailure() {
  //    // ant test -Dargs="-Dtests.force.assumption.failure.before=true"
  //    final String PROP = "tests.force.assumption.failure.before";
  //    assumeFalse(PROP + " == true",
  //                systemPropertyAsBoolean(PROP, false));
  //  }
  public static String TEST_HOME() {
    return getFile("solr/collection1").getParent();
  }

  public static Path TEST_PATH() {
    return getFile("solr/collection1").getParentFile().toPath();
  }

  /**
   * Gets a resource from the context classloader as {@link File}. This method should only be used,
   * if a real file is needed. To get a stream, code should prefer
   * {@link Class#getResourceAsStream} using {@code this.getClass()}.
   */
  public static File getFile(String name) {
    final URL url = SolrTestCase.class.getClassLoader().getResource(name.replace(File.separatorChar, '/'));
    if (url != null) {
      try {
        return new File(url.toURI());
      } catch (Exception e) {
        ParWork.propagateInterrupt(e);
        throw new RuntimeException("Resource was found on classpath, but cannot be resolved to a " + "normal file (maybe it is part of a JAR file): " + name);
      }
    }
    final File file = new File(name);
    if (file.exists()) {
      return file;
    }
    throw new RuntimeException("Cannot find resource in classpath or in file-system (relative to CWD): " + name);
  }

  /**
   * Return the name of the currently executing test case.
   */
  public static String getTestName() {
    return SolrTestCase.threadAndTestNameRule.testMethodName;
  }

  public static SSLTestConfig buildSSLConfig() {
    Class<?> targetClass = RandomizedContext.current().getTargetClass();
    final SolrTestCase.AlwaysUseSSL alwaysUseSSL = targetClass.getAnnotation(SolrTestCase.AlwaysUseSSL.class);
//    if (!LuceneTestCase.TEST_NIGHTLY && alwaysUseSSL == null) {
//      return new SSLTestConfig();
//    }
    // MRM TODO: whats up with SSL in nightly tests and http2 client?
    if (alwaysUseSSL == null) {
      return new SSLTestConfig();
    }

    RandomizeSSL.SSLRandomizer sslRandomizer = RandomizeSSL.SSLRandomizer.getSSLRandomizerForClass(targetClass);

    if (Constants.MAC_OS_X) {
      // see SOLR-9039
      // If a solution is found to remove this, please make sure to also update
      // TestMiniSolrCloudClusterSSL.testSslAndClientAuth as well.
      sslRandomizer = new RandomizeSSL.SSLRandomizer(sslRandomizer.ssl, 0.0D, (sslRandomizer.debug + " w/ MAC_OS_X supressed clientAuth"));
    }

    SSLTestConfig result = sslRandomizer.createSSLTestConfig();
    if (log.isInfoEnabled()) {
      log.info("Randomized ssl ({}) and clientAuth ({}) via: {}", result.isSSLMode(), result.isClientAuthMode(), sslRandomizer.debug);
    }
    return result;
  }

  /**
   * Creates an empty, temporary folder (when the name of the folder is of no importance).
   *
   * @see #createTempDir(String)
   */
  public static Path createTempDir() {
    return createTempDir("tempDir");
  }

  /**
   * Creates an empty, temporary folder with the given name prefix under the
   * test class's getBaseTempDirForTestClass().
   *
   * <p>The folder will be automatically removed after the
   * test class completes successfully. The test should close any file handles that would prevent
   * the folder from being removed.
   */
  public static Path createTempDir(String prefix) {
//    try {
//      Path testDir = Paths.get("/data3-ext3", prefix + "-Solr-Test");
//      while (Files.exists(testDir)) {
//        try {
//          Files.walk(testDir).sorted(Comparator.reverseOrder()).forEach(new CoreContainer.FileConsumer());
//        } catch (NoSuchFileException | UncheckedIOException e) {
//
//        }
//      }
//      Files.deleteIfExists(testDir);
//      return Files.createDirectory(testDir);
//    } catch (IOException e) {
//      log.error("tmp dir create failed", e);
//      throw new RuntimeException(e);
//    }

    return SolrTestCase.tempFilesCleanupRule.createTempDir(prefix);
  }

  /**
   * Creates an empty file with the given prefix and suffix under the
   * test class's getBaseTempDirForTestClass().
   *
   * <p>The file will be automatically removed after the
   * test class completes successfully. The test should close any file handles that would prevent
   * the folder from being removed.
   */
  public static Path createTempFile(String prefix, String suffix) throws IOException {
    return SolrTestCase.tempFilesCleanupRule.createTempFile(prefix, suffix);
  }

  /**
   * Creates an empty temporary file.
   *
   * @see #createTempFile(String, String)
   */
  public static Path createTempFile() throws IOException {
    return createTempFile("tempFile", ".tmp");
  }

  public static Path configset(String name) {
    return TEST_PATH().resolve("configsets").resolve(name).resolve("conf");
  }

  public static void wait(Thread thread) {

    if (thread.getName().startsWith(ParWork.ROOT_EXEC_PREFIX) && thread.getState() == Thread.State.TIMED_WAITING) {
      return;
    }
    if (thread.getState() == Thread.State.TERMINATED) {
      return; // change to true if we go back to test framework thread checking
    }

    int cnt = 0;
    do {
      if (thread.getName().startsWith(ParWork.ROOT_EXEC_PREFIX) && thread.getState() == Thread.State.TIMED_WAITING) {
        return;
      }
      if (log.isDebugEnabled()) {
        log.debug("waiting on {} {}", thread.getName(), thread.getState());
      }
      thread.interrupt();
      try {
        thread.join(5);
      } catch (InterruptedException e) {

      }
    } while (cnt++ < 20);

  }


  /** Create a new searcher over the reader. This searcher might randomly use threads. */
  public static IndexSearcher newSearcher(IndexReader r) {
    return newSearcher(r, true);
  }

  /** Create a new searcher over the reader. This searcher might randomly use threads. */
  public static IndexSearcher newSearcher(IndexReader r, boolean maybeWrap) {
    return newSearcher(r, maybeWrap, true);
  }

  /**
   * Create a new searcher over the reader. This searcher might randomly use threads. if <code>
   * maybeWrap</code> is true, this searcher might wrap the reader with one that returns null for
   * getSequentialSubReaders. If <code>wrapWithAssertions</code> is true, this searcher might be an
   * {@link AssertingIndexSearcher} instance.
   */
  public static IndexSearcher newSearcher(
          IndexReader r, boolean maybeWrap, boolean wrapWithAssertions) {
    Random random = SolrTestCase.random();
    if (usually()) {
      if (maybeWrap) {
        try {
          r = maybeWrapReader(r);
        } catch (IOException e) {
          Rethrow.rethrow(e);
        }
      }
      // TODO: this whole check is a coverage hack, we should move it to tests for various
      // filterreaders.
      // ultimately whatever you do will be checkIndex'd at the end anyway.
      if (random.nextInt(500) == 0 && r instanceof LeafReader) {
        // TODO: not useful to check DirectoryReader (redundant with checkindex)
        // but maybe sometimes run this on the other crazy readers maybeWrapReader creates?
        try {
          TestUtil.checkReader(r);
        } catch (IOException e) {
          Rethrow.rethrow(e);
        }
      }
      final IndexSearcher ret;
      if (wrapWithAssertions) {
        ret =
                random.nextBoolean()
                        ? new AssertingIndexSearcher(random, r)
                        : new AssertingIndexSearcher(random, r.getContext());
      } else {
        ret = random.nextBoolean() ? new IndexSearcher(r) : new IndexSearcher(r.getContext());
      }
      ret.setSimilarity(new AssertingSimilarity(new RandomSimilarity(SolrTestCase.random())));
      return ret;
    } else {
      int threads = 0;
      final ThreadPoolExecutor ex;
      if (r.getReaderCacheHelper() == null || random.nextBoolean()) {
        ex = null;
      } else {
        threads = TestUtil.nextInt(random, 1, 8);
        ex =
                new ThreadPoolExecutor(
                        threads,
                        threads,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<Runnable>(),
                        new NamedThreadFactory("LuceneTestCase"));
        // uncomment to intensify LUCENE-3840
        // ex.prestartAllCoreThreads();
      }
      if (ex != null) {
        if (VERBOSE) {
          System.out.println(
                  "NOTE: newSearcher using ExecutorService with " + threads + " threads");
        }
        r.getReaderCacheHelper()
                .addClosedListener(cacheKey -> TestUtil.shutdownExecutorService(ex));
      }
      IndexSearcher ret;
      if (wrapWithAssertions) {
        ret =
                random.nextBoolean()
                        ? new AssertingIndexSearcher(random, r, ex)
                        : new AssertingIndexSearcher(random, r.getContext(), ex);
      } else if (random.nextBoolean()) {
        int maxDocPerSlice = 1 + random.nextInt(100000);
        int maxSegmentsPerSlice = 1 + random.nextInt(20);
        ret =
                new IndexSearcher(r, ex) {
                  @Override
                  protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
                    return slices(leaves, maxDocPerSlice, maxSegmentsPerSlice);
                  }
                };
      } else {
        ret =
                random.nextBoolean() ? new IndexSearcher(r, ex) : new IndexSearcher(r.getContext(), ex);
      }
      ret.setSimilarity(new AssertingSimilarity(new RandomSimilarity(SolrTestCase.random())));
      ret.setQueryCachingPolicy(LuceneTestCase.MAYBE_CACHE_POLICY);
      return ret;
    }
  }

  public static IndexReader wrapReader(IndexReader r) throws IOException {
    Random random = SolrTestCase.random();

    for (int i = 0, c = random.nextInt(6) + 1; i < c; i++) {
      switch (random.nextInt(5)) {
        case 0:
          // will create no FC insanity in atomic case, as ParallelLeafReader has own cache key:
          if (VERBOSE) {
            System.out.println(
                    "NOTE: LuceneTestCase.wrapReader: wrapping previous reader="
                            + r
                            + " with ParallelLeaf/CompositeReader");
          }
          r =
                  (r instanceof LeafReader)
                          ? new ParallelLeafReader((LeafReader) r)
                          : new ParallelCompositeReader((CompositeReader) r);
          break;
        case 1:
          if (r instanceof LeafReader) {
            final LeafReader ar = (LeafReader) r;
            final List<String> allFields = new ArrayList<>();
            for (FieldInfo fi : ar.getFieldInfos()) {
              allFields.add(fi.name);
            }
            Collections.shuffle(allFields, random);
            final int end = allFields.isEmpty() ? 0 : random.nextInt(allFields.size());
            final Set<String> fields = new HashSet<>(allFields.subList(0, end));
            // will create no FC insanity as ParallelLeafReader has own cache key:
            if (VERBOSE) {
              System.out.println(
                      "NOTE: LuceneTestCase.wrapReader: wrapping previous reader="
                              + r
                              + " with ParallelLeafReader");
            }
            r =
                    new ParallelLeafReader(
                            new FieldFilterLeafReader(ar, fields, false),
                            new FieldFilterLeafReader(ar, fields, true));
          }
          break;
        case 2:
          // Häckidy-Hick-Hack: a standard Reader will cause FC insanity, so we use
          // QueryUtils' reader with a fake cache key, so insanity checker cannot walk
          // along our reader:
          if (VERBOSE) {
            System.out.println(
                    "NOTE: LuceneTestCase.wrapReader: wrapping previous reader="
                            + r
                            + " with AssertingLeaf/DirectoryReader");
          }
          if (r instanceof LeafReader) {
            r = new AssertingLeafReader((LeafReader) r);
          } else if (r instanceof DirectoryReader) {
            r = new AssertingDirectoryReader((DirectoryReader) r);
          }
          break;
        case 3:
          if (VERBOSE) {
            System.out.println(
                    "NOTE: LuceneTestCase.wrapReader: wrapping previous reader="
                            + r
                            + " with MismatchedLeaf/DirectoryReader");
          }
          if (r instanceof LeafReader) {
            r = new MismatchedLeafReader((LeafReader) r, random);
          } else if (r instanceof DirectoryReader) {
            r = new MismatchedDirectoryReader((DirectoryReader) r, random);
          }
          break;
        case 4:
          if (VERBOSE) {
            System.out.println(
                    "NOTE: LuceneTestCase.wrapReader: wrapping previous reader="
                            + r
                            + " with MergingCodecReader");
          }
          if (r instanceof CodecReader) {
            r = new MergingCodecReader((CodecReader) r);
          } else if (r instanceof DirectoryReader) {
            boolean allLeavesAreCodecReaders = true;
            for (LeafReaderContext ctx : r.leaves()) {
              if (ctx.reader() instanceof CodecReader == false) {
                allLeavesAreCodecReaders = false;
                break;
              }
            }
            if (allLeavesAreCodecReaders) {
              r = new MergingDirectoryReaderWrapper((DirectoryReader) r);
            }
          }
          break;
        default:
          LuceneTestCase.fail("should not get here");
      }
    }

    if (VERBOSE) {
      System.out.println("wrapReader wrapped: " + r);
    }

    return r;
  }

  /** Sometimes wrap the IndexReader as slow, parallel or filter reader (or combinations of that) */
  public static IndexReader maybeWrapReader(IndexReader r) throws IOException {
    if (rarely(SolrTestCase.random())) {
      r = wrapReader(r);
    }
    return r;
  }


  public static IndexWriterConfig newIndexWriterConfig(Analyzer a) {
    return newIndexWriterConfig(SolrTestCase.random(), a);
  }

  public static IndexWriterConfig newIndexWriterConfig(Random r, Analyzer a) {
    IndexWriterConfig c = new IndexWriterConfig(a);
    c.setSimilarity(new AssertingSimilarity(new RandomSimilarity(SolrTestCase.random())));
//    if (VERBOSE) {
//      // Even though TestRuleSetupAndRestoreClassEnv calls
//      // InfoStream.setDefault, we do it again here so that
//      // the PrintStreamInfoStream.messageID increments so
//      // that when there are separate instances of
//      // IndexWriter created we see "IW 0", "IW 1", "IW 2",
//      // ... instead of just always "IW 0":
//      c.setInfoStream(
//              new org.apache.lucene.util.TestRuleSetupAndRestoreClassEnv.ThreadNameFixingPrintStreamInfoStream(System.out));
//    }

    if (rarely(r)) {
      c.setMergeScheduler(new SerialMergeScheduler());
    } else if (rarely(r)) {
      ConcurrentMergeScheduler cms;
      if (r.nextBoolean()) {
        cms = new ConcurrentMergeScheduler();
      } else {
        cms =
                new ConcurrentMergeScheduler() {
                  @Override
                  protected synchronized boolean maybeStall(MergeSource mergeSource) {
                    return true;
                  }
                };
      }
      int maxThreadCount = TestUtil.nextInt(r, 1, 4);
      int maxMergeCount = TestUtil.nextInt(r, maxThreadCount, maxThreadCount + 4);
      cms.setMaxMergesAndThreads(maxMergeCount, maxThreadCount);
      if (SolrTestCase.random().nextBoolean()) {
        cms.disableAutoIOThrottle();
        //assertFalse(cms.getAutoIOThrottle());
      }
      cms.setForceMergeMBPerSec(10 + 10 * SolrTestCase.random().nextDouble());
      c.setMergeScheduler(cms);
    } else {
      // Always use consistent settings, else CMS's dynamic (SSD or not)
      // defaults can change, hurting reproducibility:
      ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();

      // Only 1 thread can run at once (should maybe help reproducibility),
      // with up to 3 pending merges before segment-producing threads are
      // stalled:
      cms.setMaxMergesAndThreads(3, 1);
      c.setMergeScheduler(cms);
    }

    if (r.nextBoolean()) {
      if (rarely(r)) {
        // crazy value
        c.setMaxBufferedDocs(TestUtil.nextInt(r, 2, 15));
      } else {
        // reasonable value
        c.setMaxBufferedDocs(TestUtil.nextInt(r, 16, 1000));
      }
    }

    c.setMergePolicy(newMergePolicy(r));

    //avoidPathologicalMerging(c);

    if (rarely(r)) {
      c.setMergedSegmentWarmer(new SimpleMergedSegmentWarmer(c.getInfoStream()));
    }
    c.setUseCompoundFile(r.nextBoolean());
    c.setReaderPooling(r.nextBoolean());
    if (rarely(r)) {
      c.setCheckPendingFlushUpdate(false);
    }

    if (rarely(r)) {
      c.setIndexWriterEventListener(new MockIndexWriterEventListener());
    }

    c.setMaxFullFlushMergeWaitMillis(SolrTestUtil.rarely(SolrTestCase.random()) ? atLeast(1000) : atLeast(200));
    return c;
  }

  public static String getSaferTestName() {
    // test names can hold additional info, like the test seed
    // only take to first space
    String testName = SolrTestCase.threadAndTestNameRule.testMethodName;
    int index = testName.indexOf(' ');
    if (index > 0) {
      testName = testName.substring(0, index);
    }
    return testName;
  }

  public static boolean compareSolrDocumentList(Object expected, Object actual) {
    if (!(expected instanceof SolrDocumentList) || !(actual instanceof SolrDocumentList)) {
      return false;
    }

    if (expected == actual) {
      return true;
    }

    SolrDocumentList list1 = (SolrDocumentList) expected;
    SolrDocumentList list2 = (SolrDocumentList) actual;

    if (list1.getMaxScore() == null) {
      if (list2.getMaxScore() != null) {
        return false;
      }
    } else if (list2.getMaxScore() == null) {
      return false;
    } else {
      if (Float.compare(list1.getMaxScore(), list2.getMaxScore()) != 0 || list1.getNumFound() != list2.getNumFound() || list1.getStart() != list2.getStart()) {
        return false;
      }
    }
    final long numFound = list1.getNumFound();
    for (int i = 0; i < numFound; i++) {
      if (!compareSolrDocument(list1.get(i), list2.get(i))) {
        return false;
      }
    }
    return true;
  }

  public static boolean compareSolrDocument(Object expected, Object actual) {

    if (!(expected instanceof SolrDocument) || !(actual instanceof SolrDocument)) {
      return false;
    }

    if (expected == actual) {
      return true;
    }

    SolrDocument solrDocument1 = (SolrDocument) expected;
    SolrDocument solrDocument2 = (SolrDocument) actual;

    if (solrDocument1.getFieldNames().size() != solrDocument2.getFieldNames().size()) {
      return false;
    }

    Iterator<String> iter1 = solrDocument1.getFieldNames().iterator();
    Iterator<String> iter2 = solrDocument2.getFieldNames().iterator();

    if (iter1.hasNext()) {
      String key1 = iter1.next();
      String key2 = iter2.next();

      Object val1 = solrDocument1.getFieldValues(key1);
      Object val2 = solrDocument2.getFieldValues(key2);

      if (!key1.equals(key2) || !val1.equals(val2)) {
        return false;
      }
    }

    if (solrDocument1.getChildDocuments() == null && solrDocument2.getChildDocuments() == null) {
      return true;
    }
    if (solrDocument1.getChildDocuments() == null || solrDocument2.getChildDocuments() == null) {
      return false;
    } else if (solrDocument1.getChildDocuments().size() != solrDocument2.getChildDocuments().size()) {
      return false;
    } else {
      Iterator<SolrDocument> childDocsIter1 = solrDocument1.getChildDocuments().iterator();
      Iterator<SolrDocument> childDocsIter2 = solrDocument2.getChildDocuments().iterator();
      while (childDocsIter1.hasNext()) {
        if (!compareSolrDocument(childDocsIter1.next(), childDocsIter2.next())) {
          return false;
        }
      }
      return true;
    }
  }

  public static IndexableField newTextField(String value, String foo_bar_bar_bar_bar, Field.Store no) {
    return LuceneTestCase.newTextField(value, foo_bar_bar_bar_bar, no);
  }

  public static IndexableField newStringField(String value, String bar, Field.Store yes) {
    return LuceneTestCase.newStringField(value, bar, yes);
  }

  public static int atLeast(int i) {
    return LuceneTestCase.atLeast(i);
  }

  public static IndexWriterConfig newIndexWriterConfig() {
    return newIndexWriterConfig(new MockAnalyzer(SolrTestCase.random()));
  }


  public static abstract class StopableThread extends Thread {
    public abstract void stopThread();
  }

  public static abstract class HorridGC extends Thread {
    public abstract void stopHorridGC();
    public abstract void waitForThreads(int ms) throws InterruptedException;
  }

  public static HorridGC horridGC() {

    //          try {
    //            Thread.sleep(random().nextInt(10000));
    //          } catch (InterruptedException e) {
    //            throw new RuntimeException();
    //          }
    int delay = 10 + SolrTestCase.random().nextInt(2000);
    StopableThread thread1 = new StopableThread() {
      volatile boolean stop = false;

      @Override
      public void run() {
        try {
          Thread.sleep(delay);
        } catch (InterruptedException e) {
          ParWork.propagateInterrupt(e);
          throw new RuntimeException();
        }

        double sideEffect = 0;
        for (int i = 0; i < 60000; i++) {
          if (stop) {
            return;
          }
          sideEffect = slowpoke(599999L);
          if (stop) {
            return;
          }
          //          try {
          //            Thread.sleep(random().nextInt(10000));
          //          } catch (InterruptedException e) {
          //            throw new RuntimeException();
          //          }
        }
        System.out.println("result = " + sideEffect);
      }

      @Override
      public void stopThread() {
        this.stop = true;
      }
    };
    thread1.start();

    // trigger stop-the-world
    StopableThread thread2 = new StopableThread() {
      volatile boolean stop = false;

      @Override
      public void run() {
        try {
          Thread.sleep(delay);
        } catch (InterruptedException e) {
          ParWork.propagateInterrupt(e);
          throw new RuntimeException();
        }
        int cnt = 0;
        long timestamp = System.nanoTime();
        while (cnt++ <= 350) {
          System.out.println("Delay " + (TimeUnit.MILLISECONDS.convert(System.nanoTime() - timestamp, TimeUnit.NANOSECONDS)) + " " + cnt);

          timestamp = System.nanoTime();
          // trigger stop-the-world
          System.gc();
          if (stop) {
            return;
          }
          //          try {
          //            Thread.sleep(SolrTestCase.random().nextInt(3));
          //          } catch (InterruptedException e) {
          //            ParWork.propagateInterrupt(e);
          //            throw new RuntimeException();
          //          }
        }
      }

      @Override
      public void stopThread() {
        this.stop = true;
      }
    };
    thread2.start();
   return new HorridGC() {
     @Override
     public void stopHorridGC() {
       thread1.stopThread();
       thread2.stopThread();
     }

     @Override
     public void waitForThreads(int ms) throws InterruptedException {
       thread1.join(ms);
     }
   };
  }

  public static double slowpoke(long iterations) {
    double d = 0;
    for (int j = 1; j < iterations; j++) {
      d += Math.log(Math.E * j);
    }
    return d;
  }

  public static boolean usually(Random random) {
    return !rarely(random);
  }

  public static boolean usually() {
    return usually(SolrTestCase.random());
  }

  public static boolean rarely(Random random) {
    int p = LuceneTestCase.TEST_NIGHTLY ? 10 : 1;
    p += (p * Math.log(LuceneTestCase.RANDOM_MULTIPLIER));
    int min = 100 - Math.min(p, 50); // never more than 50
    return random.nextInt(100) >= min;
  }

  /** Returns true, if MMapDirectory supports unmapping on this platform (required for Windows), or if we are not on Windows. */
  public static boolean hasWorkingMMapOnWindows() {
    return !Constants.WINDOWS || MMapDirectory.UNMAP_SUPPORTED;
  }

  /** Filesystem-based {@link Directory} implementations. */
  private static final List<String> FS_DIRECTORIES = Arrays.asList(
      "NIOFSDirectory",
      // NIOFSDirectory as replacement for MMapDirectory if unmapping is not supported on Windows (to make randomization stable):
      hasWorkingMMapOnWindows() ? "MMapDirectory" : "NIOFSDirectory"
  );

  /** Returns a new FSDirectory instance over the given file, which must be a folder. */
  public static BaseDirectoryWrapper newFSDirectory(Path f, LockFactory lf, Random random) {
    return newFSDirectory(f, lf, rarely(random));
  }

  /** Returns a new FSDirectory instance over the given file, which must be a folder. */
  public static BaseDirectoryWrapper newFSDirectory(Path f, LockFactory lf) {
    return newFSDirectory(f, lf, SolrTestCase.random());
  }


  private static BaseDirectoryWrapper newFSDirectory(Path f, LockFactory lf, boolean bare) {
    String fsdirClass = LuceneTestCase.TEST_DIRECTORY;
    if (fsdirClass.equals("random")) {
      fsdirClass = RandomPicks.randomFrom(SolrTestCase.random(), FS_DIRECTORIES);
    }

    Class<? extends FSDirectory> clazz;
    try {
      try {
        clazz = CommandLineUtil.loadFSDirectoryClass(fsdirClass);
      } catch (ClassCastException e) {
        // TEST_DIRECTORY is not a sub-class of FSDirectory, so draw one at random
        fsdirClass = RandomPicks.randomFrom(SolrTestCase.random(), FS_DIRECTORIES);
        clazz = CommandLineUtil.loadFSDirectoryClass(fsdirClass);
      }

      Directory fsdir = newFSDirectoryImpl(clazz, f, lf);
      BaseDirectoryWrapper wrapped = wrapDirectory(SolrTestCase.random(), fsdir, bare, true);
      return wrapped;
    } catch (Exception e) {
      Rethrow.rethrow(e);
      throw null; // dummy to prevent compiler failure
    }
  }

  private static Directory newFSDirectoryImpl(Class<? extends FSDirectory> clazz, Path path, LockFactory lf) throws IOException {
    FSDirectory d = null;
    try {
      d = CommandLineUtil.newFSDirectory(clazz, path, lf);
    } catch (ReflectiveOperationException e) {
      Rethrow.rethrow(e);
    }
    return d;
  }

  private static Directory newFileSwitchDirectory(Random random, Directory dir1, Directory dir2) {
    List<String> fileExtensions =
        Arrays.asList("fdt", "fdx", "tim", "tip", "si", "fnm", "pos", "dii", "dim", "nvm", "nvd", "dvm", "dvd");
    Collections.shuffle(fileExtensions, random);
    fileExtensions = fileExtensions.subList(0, 1 + random.nextInt(fileExtensions.size()));
    return new FileSwitchDirectory(new HashSet<>(fileExtensions), dir1, dir2, true);
  }

  static Directory newDirectoryImpl(Random random, String clazzName) {
    return newDirectoryImpl(random, clazzName, FSLockFactory.getDefault());
  }

  /** All {@link Directory} implementations. */
  private static final List<String> CORE_DIRECTORIES;
  static {
    CORE_DIRECTORIES = new ArrayList<>(FS_DIRECTORIES);
    CORE_DIRECTORIES.add(ByteBuffersDirectory.class.getSimpleName());
  }


  static Directory newDirectoryImpl(Random random, String clazzName, LockFactory lf) {
    if (clazzName.equals("random")) {
      if (rarely(random)) {
        clazzName = RandomPicks.randomFrom(random, CORE_DIRECTORIES);
      } else if (rarely(random)) {
        String clazzName1 = rarely(random)
            ? RandomPicks.randomFrom(random, CORE_DIRECTORIES)
            : ByteBuffersDirectory.class.getName();
        String clazzName2 = rarely(random)
            ? RandomPicks.randomFrom(random, CORE_DIRECTORIES)
            : ByteBuffersDirectory.class.getName();
        return newFileSwitchDirectory(random,
            newDirectoryImpl(random, clazzName1, lf),
            newDirectoryImpl(random, clazzName2, lf));
      } else {
        clazzName = ByteBuffersDirectory.class.getName();
      }
    }

    try {
      final Class<? extends Directory> clazz = CommandLineUtil.loadDirectoryClass(clazzName);
      // If it is a FSDirectory type, try its ctor(Path)
      if (FSDirectory.class.isAssignableFrom(clazz)) {
        final Path dir = createTempDir("index-" + clazzName);
        return newFSDirectoryImpl(clazz.asSubclass(FSDirectory.class), dir, lf);
      }

      // See if it has a Path/LockFactory ctor even though it's not an
      // FSDir subclass:
      try {
        Constructor<? extends Directory> pathCtor = clazz.getConstructor(Path.class, LockFactory.class);
        final Path dir = createTempDir("index");
        return pathCtor.newInstance(dir, lf);
      } catch (NoSuchMethodException nsme) {
        // Ignore
      }

      // the remaining dirs are no longer filesystem based, so we must check that the passedLockFactory is not file based:
      if (!(lf instanceof FSLockFactory)) {
        // try ctor with only LockFactory
        try {
          return clazz.getConstructor(LockFactory.class).newInstance(lf);
        } catch (NoSuchMethodException nsme) {
          // Ignore
        }
      }

      // try empty ctor
      return clazz.getConstructor().newInstance();
    } catch (Exception e) {
      Rethrow.rethrow(e);
      throw null; // dummy to prevent compiler failure
    }
  }

  /**
   * Returns a new Directory instance. Use this when the test does not
   * care about the specific Directory implementation (most tests).
   * <p>
   * The Directory is wrapped with {@link BaseDirectoryWrapper}.
   * this means usually it will be picky, such as ensuring that you
   * properly close it and all open files in your test. It will emulate
   * some features of Windows, such as not allowing open files to be
   * overwritten.
   */
  public static BaseDirectoryWrapper newDirectory() {
    return newDirectory(SolrTestCase.random());
  }

  /**
   * Returns a new Directory instance, using the specified random.
   * See {@link #newDirectory()} for more information.
   */
  public static BaseDirectoryWrapper newDirectory(Random r) {
    return wrapDirectory(r, newDirectoryImpl(r, LuceneTestCase.TEST_DIRECTORY), rarely(r), false);
  }

  /**
   * Returns a new Directory instance, using the specified random
   * with contents copied from the provided directory. See
   * {@link #newDirectory()} for more information.
   */
  public static BaseDirectoryWrapper newDirectory(Random r, Directory d) throws IOException {
    Directory impl = newDirectoryImpl(r, LuceneTestCase.TEST_DIRECTORY);
    for (String file : d.listAll()) {
      if (file.startsWith(IndexFileNames.SEGMENTS) || IndexFileNames.CODEC_FILE_PATTERN.matcher(file).matches()) {
        impl.copyFrom(d, file, file, LuceneTestCase.newIOContext(r));
      }
    }
    return wrapDirectory(r, impl, rarely(r), false);
  }

  private static BaseDirectoryWrapper wrapDirectory(Random random, Directory directory, boolean bare, boolean filesystem) {
    // IOContext randomization might make NRTCachingDirectory make bad decisions, so avoid
    // using it if the user requested a filesystem directory.
    if (rarely(random) && !bare && filesystem == false) {
      directory = new NRTCachingDirectory(directory, random.nextDouble(), random.nextDouble());
    }

    if (bare) {
      BaseDirectoryWrapper base = new RawDirectoryWrapper(directory);
      return base;
    } else {
      MockDirectoryWrapper mock = new MockDirectoryWrapper(random, directory);

      mock.setThrottling(LuceneTestCase.TEST_THROTTLING);

      return mock;
    }
  }

  public static MergePolicy newMergePolicy() {
    return newMergePolicy(SolrTestCase.random());
  }


  public static MergePolicy newMergePolicy(Random r) {
    return newMergePolicy(r, true);
  }

  public static MergePolicy newMergePolicy(Random r, boolean includeMockMP) {
    if (includeMockMP && rarely(r)) {
      return new MockRandomMergePolicy(r);
    } else if (r.nextBoolean()) {
      return newTieredMergePolicy(r);
    }
    // MRM TODO: need time stuff setup correctly with our SolrTestCase.classEnvRule, not LuceneTestCase
    //    else if (rarely(r) ) {
    //      return newAlcoholicMergePolicy(r, classEnvRule.timeZone);
    //    }
    return newLogMergePolicy(r);
  }

  public static LogMergePolicy newLogMergePolicy(Random r) {
    LogMergePolicy logmp = r.nextBoolean() ? new LogDocMergePolicy() : new LogByteSizeMergePolicy();
    logmp.setCalibrateSizeByDeletes(r.nextBoolean());
    if (rarely(r)) {
      logmp.setMergeFactor(TestUtil.nextInt(r, 2, 9));
    } else {
      logmp.setMergeFactor(TestUtil.nextInt(r, 10, 50));
    }
    configureRandom(r, logmp);
    return logmp;
  }

  private static void configureRandom(Random r, MergePolicy mergePolicy) {
    if (r.nextBoolean()) {
      mergePolicy.setNoCFSRatio(0.1 + r.nextDouble()*0.8);
    } else {
      mergePolicy.setNoCFSRatio(r.nextBoolean() ? 1.0 : 0.0);
    }

    if (rarely(r)) {
      mergePolicy.setMaxCFSSegmentSizeMB(0.2 + r.nextDouble() * 2.0);
    } else {
      mergePolicy.setMaxCFSSegmentSizeMB(Double.POSITIVE_INFINITY);
    }
  }

  public static TieredMergePolicy newTieredMergePolicy(Random r) {
    TieredMergePolicy tmp = new TieredMergePolicy();
    if (rarely(r)) {
      tmp.setMaxMergeAtOnce(TestUtil.nextInt(r, 2, 9));
    } else {
      tmp.setMaxMergeAtOnce(TestUtil.nextInt(r, 10, 50));
    }
    if (rarely(r)) {
      tmp.setMaxMergedSegmentMB(0.2 + r.nextDouble() * 2.0);
    } else {
      tmp.setMaxMergedSegmentMB(10 + r.nextDouble() * 100);
    }
    tmp.setFloorSegmentMB(0.2 + r.nextDouble() * 2.0);
    tmp.setForceMergeDeletesPctAllowed(0.0 + r.nextDouble() * 30.0);
    if (rarely(r)) {
      tmp.setSegmentsPerTier(TestUtil.nextInt(r, 2, 20));
    } else {
      tmp.setSegmentsPerTier(TestUtil.nextInt(r, 10, 50));
    }
    configureRandom(r, tmp);
    tmp.setDeletesPctAllowed(20 + r.nextDouble() * 30);
    return tmp;
  }

  public static MergePolicy newLogMergePolicy(boolean useCFS) {
    MergePolicy logmp = newLogMergePolicy();
    logmp.setNoCFSRatio(useCFS ? 1.0 : 0.0);
    return logmp;
  }

  public static MergePolicy newLogMergePolicy(boolean useCFS, int mergeFactor) {
    LogMergePolicy logmp = newLogMergePolicy();
    logmp.setNoCFSRatio(useCFS ? 1.0 : 0.0);
    logmp.setMergeFactor(mergeFactor);
    return logmp;
  }

  public static MergePolicy newLogMergePolicy(int mergeFactor) {
    LogMergePolicy logmp = newLogMergePolicy();
    logmp.setMergeFactor(mergeFactor);
    return logmp;
  }

  public static LogMergePolicy newLogMergePolicy() {
    return newLogMergePolicy(SolrTestCase.random());
  }

}