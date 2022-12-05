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
package org.apache.solr.hdfs.cloud;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.hdfs.util.BadHdfsThreadsFilter;
import org.apache.solr.hdfs.util.HdfsRecoverLeaseFileSystemUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@ThreadLeakFilters(
    defaultFilters = true,
    filters = {
      SolrIgnoredThreadsFilter.class,
      QuickPatchThreadsFilter.class,
      BadHdfsThreadsFilter.class // hdfs currently leaks thread(s)
    })
@ThreadLeakLingering(
    linger = 1000) // Wait at least 1 second for Netty GlobalEventExecutor to shutdown
public class HdfsRecoverLeaseTest extends SolrTestCaseJ4 {

  private static MiniDFSCluster dfsCluster;

  @BeforeClass
  public static void beforeClass() throws Exception {
    dfsCluster = HdfsTestUtil.setupClass(createTempDir().toFile().getAbsolutePath(), false);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    try {
      HdfsTestUtil.teardownClass(dfsCluster);
    } finally {
      dfsCluster = null;
    }
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void testBasic() throws IOException {
    long startRecoverLeaseSuccessCount =
        HdfsRecoverLeaseFileSystemUtils.RECOVER_LEASE_SUCCESS_COUNT.get();

    URI uri = dfsCluster.getURI();
    Path path = new Path(uri);
    Configuration conf = HdfsTestUtil.getClientConfiguration(dfsCluster);
    FileSystem fs1 = FileSystem.get(path.toUri(), conf);
    Path testFile = new Path(uri + "/testfile");
    FSDataOutputStream out = fs1.create(testFile);

    out.write(5);
    out.hflush();
    out.close();

    HdfsRecoverLeaseFileSystemUtils.recoverFileLease(fs1, testFile, conf, () -> false);
    assertEquals(
        0,
        HdfsRecoverLeaseFileSystemUtils.RECOVER_LEASE_SUCCESS_COUNT.get()
            - startRecoverLeaseSuccessCount);

    fs1.close();

    FileSystem fs2 = FileSystem.get(path.toUri(), conf);
    Path testFile2 = new Path(uri + "/testfile2");
    FSDataOutputStream out2 = fs2.create(testFile2);

    if (random().nextBoolean()) {
      int cnt = random().nextInt(100);
      for (int i = 0; i < cnt; i++) {
        out2.write(random().nextInt(20000));
      }
      out2.hflush();
    }

    // closing the fs will close the file it seems
    // fs2.close();

    FileSystem fs3 = FileSystem.get(path.toUri(), conf);

    HdfsRecoverLeaseFileSystemUtils.recoverFileLease(fs3, testFile2, conf, () -> false);
    assertEquals(
        1,
        HdfsRecoverLeaseFileSystemUtils.RECOVER_LEASE_SUCCESS_COUNT.get()
            - startRecoverLeaseSuccessCount);

    fs3.close();
    fs2.close();
  }

  @Test
  @SuppressWarnings("DoNotCall")
  public void testMultiThreaded() throws Exception {
    long startRecoverLeaseSuccessCount =
        HdfsRecoverLeaseFileSystemUtils.RECOVER_LEASE_SUCCESS_COUNT.get();

    final URI uri = dfsCluster.getURI();
    final Path path = new Path(uri);
    final Configuration conf = HdfsTestUtil.getClientConfiguration(dfsCluster);

    // n threads create files
    class WriterThread extends Thread {
      private final FileSystem fs;
      private final int id;

      public WriterThread(int id) {
        this.id = id;
        try {
          fs = FileSystem.get(path.toUri(), conf);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void run() {
        Path testFile = new Path(uri + "/file-" + id);
        FSDataOutputStream out;
        try {
          out = fs.create(testFile);

          if (random().nextBoolean()) {
            int cnt = random().nextInt(100);
            for (int i = 0; i < cnt; i++) {
              out.write(random().nextInt(20000));
            }
            out.hflush();
          }
        } catch (IOException e) {
          throw new RuntimeException();
        }
      }

      public void close() throws IOException {
        fs.close();
      }

      public int getFileId() {
        return id;
      }
    }

    class RecoverThread extends Thread {
      private final FileSystem fs;
      private final int id;

      public RecoverThread(int id) {
        this.id = id;
        try {
          fs = FileSystem.get(path.toUri(), conf);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void run() {
        Path testFile = new Path(uri + "/file-" + id);
        try {
          HdfsRecoverLeaseFileSystemUtils.recoverFileLease(fs, testFile, conf, () -> false);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      public void close() throws IOException {
        fs.close();
      }
    }

    Set<WriterThread> writerThreads = new HashSet<>();
    Set<RecoverThread> recoverThreads = new HashSet<>();

    int threadCount = 3;
    for (int i = 0; i < threadCount; i++) {
      WriterThread wt = new WriterThread(i);
      writerThreads.add(wt);
      // should be wt.start();
      wt.run();
    }

    for (WriterThread wt : writerThreads) {
      wt.join();
    }

    Thread.sleep(2000);

    for (WriterThread wt : writerThreads) {
      RecoverThread rt = new RecoverThread(wt.getFileId());
      recoverThreads.add(rt);
      // should be rt.start();
      rt.run();
    }

    for (WriterThread wt : writerThreads) {
      wt.close();
    }

    for (RecoverThread rt : recoverThreads) {
      rt.close();
    }

    assertEquals(
        threadCount,
        HdfsRecoverLeaseFileSystemUtils.RECOVER_LEASE_SUCCESS_COUNT.get()
            - startRecoverLeaseSuccessCount);
  }
}
