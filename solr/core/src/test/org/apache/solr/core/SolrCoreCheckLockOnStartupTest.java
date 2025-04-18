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
package org.apache.solr.core;

import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrCoreCheckLockOnStartupTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    System.setProperty("solr.directoryFactory", "org.apache.solr.core.NIOFSDirectoryFactory");
    // test tests native and simple in the same jvm in the same exact directory:
    // the file will remain after the native test (it cannot safely be deleted without the risk of
    // deleting another guys lock) it's ok, these aren't "compatible" anyway: really this test
    // should not re-use the same directory at all.
    Files.deleteIfExists(initAndGetDataDir().resolve("index").resolve(IndexWriter.WRITE_LOCK_NAME));
  }

  @Test
  public void testSimpleLockErrorOnStartup() throws Exception {

    Directory directory =
        newFSDirectory(initAndGetDataDir().resolve("index"), SimpleFSLockFactory.INSTANCE);
    // creates a new IndexWriter without releasing the lock yet
    IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig(null));

    ignoreException("locked");
    try {
      System.setProperty("solr.tests.lockType", DirectoryFactory.LOCK_TYPE_SIMPLE);
      // opening a new core on the same index
      initCore("solrconfig-basic.xml", "schema.xml");
      if (checkForCoreInitException(LockObtainFailedException.class)) return;
      fail("Expected " + LockObtainFailedException.class.getSimpleName());
    } finally {
      System.clearProperty("solr.tests.lockType");
      unIgnoreException("locked");
      indexWriter.close();
      directory.close();
      deleteCore();
    }
  }

  @Test
  public void testNativeLockErrorOnStartup() throws Exception {

    Path indexDir = initAndGetDataDir().resolve("index");
    if (log.isInfoEnabled()) {
      log.info("Acquiring lock on {}", indexDir);
    }
    Directory directory = newFSDirectory(indexDir, NativeFSLockFactory.INSTANCE);
    // creates a new IndexWriter without releasing the lock yet
    IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig(null));

    ignoreException("locked");
    try {
      System.setProperty("solr.tests.lockType", DirectoryFactory.LOCK_TYPE_NATIVE);
      // opening a new core on the same index
      initCore("solrconfig-basic.xml", "schema.xml");
      if (checkForCoreInitException(LockObtainFailedException.class)) return;
      fail("Expected " + LockObtainFailedException.class.getSimpleName());
    } finally {
      System.clearProperty("solr.tests.lockType");
      unIgnoreException("locked");
      indexWriter.close();
      directory.close();
      deleteCore();
    }
  }

  private boolean checkForCoreInitException(Class<? extends Exception> clazz) {
    for (Map.Entry<String, CoreContainer.CoreLoadFailure> entry :
        h.getCoreContainer().getCoreInitFailures().entrySet()) {
      for (Throwable t = entry.getValue().exception; t != null; t = t.getCause()) {
        if (clazz.isInstance(t)) return true;
      }
    }
    return false;
  }
}
