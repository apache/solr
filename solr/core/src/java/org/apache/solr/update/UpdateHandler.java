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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrEventListener;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <code>UpdateHandler</code> handles requests to change the index (adds, deletes, commits,
 * optimizes, etc).
 *
 * @since solr 0.9
 */
public abstract class UpdateHandler implements SolrInfoBean {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected final SolrCore core;

  protected final SchemaField idField;
  protected final FieldType idFieldType;

  protected List<SolrEventListener> commitCallbacks =
      Collections.synchronizedList(new ArrayList<>());
  protected List<SolrEventListener> softCommitCallbacks =
      Collections.synchronizedList(new ArrayList<>());
  protected List<SolrEventListener> optimizeCallbacks =
      Collections.synchronizedList(new ArrayList<>());

  protected final UpdateLog ulog;

  protected SolrMetricsContext solrMetricsContext;

  private void parseEventListeners() {
    for (PluginInfo info : core.getSolrConfig().getPluginInfos(SolrEventListener.class.getName())) {
      String event = info.attributes.get("event");
      if ("postCommit".equals(event)) {
        SolrEventListener obj = core.createEventListener(info);
        commitCallbacks.add(obj);
        log.info("added SolrEventListener for postCommit: {}", obj);
      } else if ("postOptimize".equals(event)) {
        SolrEventListener obj = core.createEventListener(info);
        optimizeCallbacks.add(obj);
        log.info("added SolrEventListener for postOptimize: {}", obj);
      }
    }
  }

  /** Call the {@link SolrCoreAware#inform(SolrCore)} on all the applicable registered listeners. */
  public void informEventListeners(SolrCore core) {
    for (SolrEventListener listener : commitCallbacks) {
      if (listener instanceof SolrCoreAware) {
        ((SolrCoreAware) listener).inform(core);
      }
    }
    for (SolrEventListener listener : optimizeCallbacks) {
      if (listener instanceof SolrCoreAware) {
        ((SolrCoreAware) listener).inform(core);
      }
    }
  }

  protected void callPostCommitCallbacks() {
    for (SolrEventListener listener : commitCallbacks) {
      listener.postCommit();
    }
  }

  protected void callPostSoftCommitCallbacks() {
    for (SolrEventListener listener : softCommitCallbacks) {
      listener.postSoftCommit();
    }
  }

  protected void callPostOptimizeCallbacks() {
    for (SolrEventListener listener : optimizeCallbacks) {
      listener.postCommit();
    }
  }

  public UpdateHandler(SolrCore core) {
    this(core, null, true);
  }

  public UpdateHandler(SolrCore core, UpdateLog updateLog) {
    this(core, updateLog, true);
  }

  /**
   * Subclasses should call this ctor, with `initUlog=false` and should then, as the last action in
   * the subclass ctor, call {@link #initUlog(boolean)}.
   *
   * <p>NOTE: as an abstract class, if subclasses are supposed to always call this with
   * `initUlog=false`, we could simply never init ulog in this method, and avoid the extra arg. But
   * the arg is present for 3 reasons:
   *
   * <ol>
   *   <li>for backward compatibility with subclasses (plugins) that may have called {@link
   *       UpdateHandler} ctor with the assumption that {@link #ulog} <i>will</i> be initialized
   *   <li>to force subclass implementations to be aware that they must init {@link #ulog}
   *   <li>because it's likely that deferring ulog init until the last action of the top-level ctor
   *       is actually unnecessary (see below)
   * </ol>
   *
   * <p>As noted in a comment in {@link DirectUpdateHandler2#DirectUpdateHandler2(SolrCore,
   * UpdateHandler)}, it's unclear why we are advised to defer ulog init until the last action of
   * the top-level ctor, as opposed to simply delegating init to the base-class {@link
   * UpdateHandler} ctor. If we were to follow this approach, this "extra-arg" ctor could be removed
   * in favor of {@link #UpdateHandler(SolrCore, UpdateLog)}, initializing any non-null {@link
   * #ulog} (and removing the {@link #initUlog(boolean)} helper method as well).
   */
  public UpdateHandler(SolrCore core, UpdateLog updateLog, boolean initUlog) {
    this.core = core;
    idField = core.getLatestSchema().getUniqueKeyField();
    idFieldType = idField != null ? idField.getType() : null;
    parseEventListeners();
    PluginInfo ulogPluginInfo = core.getSolrConfig().getPluginInfo(UpdateLog.class.getName());

    // If this replica doesn't require a transaction log, don't create it
    boolean skipUpdateLog =
        core.getCoreDescriptor().getCloudDescriptor() != null
            && !core.getCoreDescriptor()
                .getCloudDescriptor()
                .getReplicaType()
                .requireTransactionLog;
    if (updateLog == null
        && ulogPluginInfo != null
        && ulogPluginInfo.isEnabled() // useless; `getPluginInfo()` returns null if !enabled
        && !skipUpdateLog) {
      DirectoryFactory dirFactory = core.getDirectoryFactory();

      // if the update log class is not defined in the plugin info / solrconfig.xml
      // (like <updateLog class="${solr.ulog:solr.UpdateLog}"> )
      // we fall back use the one which is the default for the given directory factory
      ulog =
          ulogPluginInfo.className == null
              ? dirFactory.newDefaultUpdateLog()
              : core.getResourceLoader().newInstance(ulogPluginInfo, UpdateLog.class, true);

      if (log.isInfoEnabled()) {
        log.info("Using UpdateLog implementation: {}", ulog.getClass().getName());
      }
      ulog.init(ulogPluginInfo);

      if (initUlog) {
        initUlog(true);
      }
    } else {
      ulog = updateLog;
      if (updateLog != null && initUlog) {
        initUlog(false);
      }
    }
  }

  /**
   * Helper method to init {@link #ulog}. As discussed in the javadocs for {@link
   * #UpdateHandler(SolrCore, UpdateLog, boolean)}, this should be called as the last action of each
   * top-level ctor.
   *
   * @param closeOnError if the calling context is responsible for creating {@link #ulog}, then we
   *     should respond to an init failure by closing {@link #ulog}, and this param should be set to
   *     <code>true</code>. If the calling context is <i>not</i> responsible for creating {@link
   *     #ulog}, then references exist elsewhere and we should not close on init error (set this
   *     param to <code>false</code>).
   */
  protected final void initUlog(boolean closeOnError) {
    try {
      ulog.init(this, core);
    } catch (Throwable t) {
      if (closeOnError) {
        ulog.close(false, false);
      }
      throw t;
    }
  }

  /**
   * Called when the Writer should be opened again - eg when replication replaces all of the index
   * files.
   *
   * @param rollback IndexWriter if true else close
   * @throws IOException If there is a low-level I/O error.
   */
  public abstract void newIndexWriter(boolean rollback) throws IOException;

  public abstract SolrCoreState getSolrCoreState();

  public abstract int addDoc(AddUpdateCommand cmd) throws IOException;

  public abstract void delete(DeleteUpdateCommand cmd) throws IOException;

  public abstract void deleteByQuery(DeleteUpdateCommand cmd) throws IOException;

  public abstract int mergeIndexes(MergeIndexesCommand cmd) throws IOException;

  public abstract void commit(CommitUpdateCommand cmd) throws IOException;

  public abstract void rollback(RollbackUpdateCommand cmd) throws IOException;

  public abstract UpdateLog getUpdateLog();

  /**
   * NOTE: this function is not thread safe. However, it is safe to call within the <code>
   * inform( SolrCore core )</code> function for <code>SolrCoreAware</code> classes. Outside <code>
   * inform</code>, this could potentially throw a ConcurrentModificationException
   *
   * @see SolrCoreAware
   */
  public void registerCommitCallback(SolrEventListener listener) {
    commitCallbacks.add(listener);
  }

  /**
   * NOTE: this function is not thread safe. However, it is safe to call within the <code>
   * inform( SolrCore core )</code> function for <code>SolrCoreAware</code> classes. Outside <code>
   * inform</code>, this could potentially throw a ConcurrentModificationException
   *
   * @see SolrCoreAware
   */
  public void registerSoftCommitCallback(SolrEventListener listener) {
    softCommitCallbacks.add(listener);
  }

  /**
   * NOTE: this function is not thread safe. However, it is safe to call within the <code>
   * inform( SolrCore core )</code> function for <code>SolrCoreAware</code> classes. Outside <code>
   * inform</code>, this could potentially throw a ConcurrentModificationException
   *
   * @see SolrCoreAware
   */
  public void registerOptimizeCallback(SolrEventListener listener) {
    optimizeCallbacks.add(listener);
  }

  public abstract void split(SplitIndexCommand cmd) throws IOException;

  @Override
  public Category getCategory() {
    return Category.UPDATE;
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }
}
