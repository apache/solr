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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Sort;
import org.apache.solr.cloud.RecoveryStrategy;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The state in this class can be easily shared between SolrCores across
 * SolrCore reloads.
 * 
 */
public abstract class SolrCoreState {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public final LongAdder reloads = new LongAdder();
  public final LongAdder successReloads = new LongAdder();

  protected volatile boolean closed = false;
  private final ReentrantLock updateLock = new ReentrantLock(false);
  private final ReentrantLock reloadLock = new ReentrantLock(false);
  
  public ReentrantLock getUpdateLock() {
    return updateLock;
  }
  
  public ReentrantLock getReloadLock() {
    return reloadLock;
  }

  /**
   * Marks that this core reached ACTIVE without running a full recovery (e.g. it was already in sync
   * with the leader at registration). Implementations should clear any "recovering after startup"
   * state so that a subsequent recovery uses the core's current recent versions rather than the
   * (possibly empty) at-startup snapshot — otherwise PeerSync gets "no frame of reference" and falls
   * back to a full index replication. Default is a no-op.
   */
  public void recoveredWithoutFullRecovery() {}

  protected final AtomicInteger solrCoreStateRefCnt = new AtomicInteger(1);

  public void increfSolrCoreState() {
    int refCnt = solrCoreStateRefCnt.updateAndGet(operand -> operand == 0 ? 0 : ++operand);
    log.debug("incref'd SolrCoreState refCnt={}", refCnt);
  }
  
  public boolean decrefSolrCoreState(IndexWriterCloser closer) {
    boolean close = false;

    int refCnt = solrCoreStateRefCnt.updateAndGet(operand -> operand == 0 ? 0 : --operand);

    log.debug("decref'd SolrCoreState refCnt={}", refCnt);

    if (refCnt == 0) {
      closed = true;
      close = true;
    }

    if (close) {
      try {
        if (log.isDebugEnabled()) log.debug("Closing SolrCoreState");
        close(closer);
      } catch (Exception e) {
        log.error("Error closing SolrCoreState", e);
      }
    }
    return close;
  }
  
  public abstract Lock getCommitLock();
  
  /**
   * Force the creation of a new IndexWriter using the settings from the given
   * SolrCore.
   * 
   * @param rollback close IndexWriter if false, else rollback
   * @throws IOException If there is a low-level I/O error.
   */
  public abstract void newIndexWriter(SolrCore core, boolean rollback, boolean createIndex) throws IOException;

  public abstract void newIndexWriter(SolrCore core, boolean rollback) throws IOException;
  
  /**
   * Expert method that closes the IndexWriter - you must call {@link #openIndexWriter(SolrCore)}
   * in a finally block after calling this method.
   * 
   * @param core that the IW belongs to
   * @param rollback true if IW should rollback rather than close
   * @throws IOException If there is a low-level I/O error.
   */
  public abstract void closeIndexWriter(SolrCore core, boolean rollback) throws IOException;
  
  /**
   * Expert method that opens the IndexWriter - you must call {@link #closeIndexWriter(SolrCore, boolean)}
   * first, and then call this method in a finally block.
   * 
   * @param core that the IW belongs to
   * @throws IOException If there is a low-level I/O error.
   */
  public abstract void openIndexWriter(SolrCore core) throws IOException;

  /**
   * Get the current IndexWriter. If a new IndexWriter must be created, use the
   * settings from the given {@link SolrCore}.
   *
   * @throws IOException If there is a low-level I/O error.
   */
  public abstract RefCounted<IndexWriter> getIndexWriter(SolrCore core) throws IOException;

  /**
   * Get the current IndexWriter. If a new IndexWriter must be created, use the
   * settings from the given {@link SolrCore}.
   * 
   * @throws IOException If there is a low-level I/O error.
   */
  public abstract RefCounted<IndexWriter> getIndexWriter(SolrCore core, boolean createIndex) throws IOException;
  
  /**
   * Rollback the current IndexWriter. When creating the new IndexWriter use the
   * settings from the given {@link SolrCore}.
   * 
   * @throws IOException If there is a low-level I/O error.
   */
  public abstract void rollbackIndexWriter(SolrCore core) throws IOException;
  
  /**
   * Get the current Sort of the current IndexWriter's MergePolicy..
   *
   * @throws IOException If there is a low-level I/O error.
   */
  public abstract Sort getMergePolicySort() throws IOException;

  /**
   * @return the {@link DirectoryFactory} that should be used.
   */
  public abstract DirectoryFactory getDirectoryFactory();

  /**
   * @return the {@link org.apache.solr.cloud.RecoveryStrategy.Builder} that should be used.
   */
  public abstract RecoveryStrategy.Builder getRecoveryStrategyBuilder();


  public interface IndexWriterCloser {
    void closeWriter(IndexWriter writer) throws IOException;
  }

  public abstract void doRecovery(SolrCore core, String source, Replica leader);

  public abstract void doRecovery(CoreContainer cc, CoreDescriptor cd, String source, Replica leader);
  
  public abstract void cancelRecovery();

  public abstract boolean isRecoverying();

  public abstract void cancelRecovery(boolean wait, boolean prepForClose);

  public abstract void close(IndexWriterCloser closer);

  public abstract boolean getLastReplicateIndexSuccess();

  public abstract void setLastReplicateIndexSuccess(boolean success);

  public static class CoreIsClosedException extends AlreadyClosedException {
    
    public CoreIsClosedException() {
      super();
    }
    
    public CoreIsClosedException(String s) {
      super(s);
    }
  }

  public Throwable getTragicException() throws IOException {
    RefCounted<IndexWriter> ref = getIndexWriter(null, false);
    if (ref == null) return null;
    try {
      return ref.get().getTragicException();
    } finally {
      ref.decref();
    }
  }
}
