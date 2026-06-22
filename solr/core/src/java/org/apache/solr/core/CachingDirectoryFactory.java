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

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.jctools.maps.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link DirectoryFactory} impl base class for caching Directory instances
 * per path. Most DirectoryFactory implementations will want to extend this
 * class and simply implement {@link DirectoryFactory#create(String, LockFactory, DirContext)}.
 * <p>
 * This is an expert class and these API's are subject to change.
 */
public abstract class CachingDirectoryFactory extends DirectoryFactory {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final boolean DEBUG_GET_RELEASE = false;

  protected final Map<String, Directory> byPathCache = new NonBlockingHashMap<>();

  protected final Map<Directory, String> byDirCache = new NonBlockingHashMap<>();

  protected final Map<Directory, List<CloseListener>> closeListeners = new NonBlockingHashMap<>();

  private volatile Double maxWriteMBPerSecFlush;

  private volatile Double maxWriteMBPerSecMerge;

  private volatile Double maxWriteMBPerSecRead;

  private volatile Double maxWriteMBPerSecDefault;

  private volatile boolean closed;

  public interface CloseListener {
    public void postClose();

    public void preClose();
  }

  @Override
  public void addCloseListener(Directory dir, CloseListener closeListener) {
    List<CloseListener> listeners = closeListeners.computeIfAbsent(dir, k -> new ArrayList<>());
    listeners.add(closeListener);

    closeListeners.put(dir, listeners);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.solr.core.DirectoryFactory#close()
   */
  @Override
  public void close() throws IOException {
    log.debug("Closing CachingDirectoryFactory");
    // Set closed first so a concurrent get() observes it and refuses to insert a new
    // Directory. Note: without a shared lock there is a residual race where a get()
    // already past the closed check can insert after the forEach below; this fork
    // deliberately removed the refcount/lock machinery, so we accept that narrow
    // window rather than reintroduce coarse locking.
    closed = true;
    byPathCache.forEach((s, directory) -> closeWithListeners(directory));
    byPathCache.clear();
    byDirCache.clear();
    closeListeners.clear();
  }

  /**
   * Closes the given Directory, invoking any registered {@link CloseListener}s around the
   * close (pre/post) and removing its listener registration. The closeListeners map was
   * previously populated by {@link #addCloseListener} but never consumed, so registered
   * listeners never fired; this is the single place that drains them.
   */
  private void closeWithListeners(Directory directory) {
    List<CloseListener> listeners = closeListeners.remove(directory);
    if (listeners != null) {
      for (CloseListener listener : listeners) {
        try {
          listener.preClose();
        } catch (Exception e) {
          log.error("Error executing preClose for directory {}", directory, e);
        }
      }
    }
    IOUtils.closeQuietly(directory);
    if (listeners != null) {
      for (CloseListener listener : listeners) {
        try {
          listener.postClose();
        } catch (Exception e) {
          log.error("Error executing postClose for directory {}", directory, e);
        }
      }
    }
  }

  @Override
  public boolean exists(String path) throws IOException {
    if (log.isTraceEnabled()) log.trace("exists(String path={}) - start", path);

    // back compat behavior
    File dirFile = new File(path);
    boolean returnboolean = dirFile.canRead() && dirFile.list().length > 0;

    if (log.isTraceEnabled()) log.trace("exists(String) - end");

    return returnboolean;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.solr.core.DirectoryFactory#get(java.lang.String,
   * java.lang.String, boolean)
   */
  @Override
  public final Directory get(String path, DirContext dirContext, String rawLockType)
          throws IOException {
    if (log.isTraceEnabled()) log.trace("get(String path={}, DirContext dirContext={}, String rawLockType={}) - start", path, dirContext, rawLockType);

    // Refuse to create/serve a Directory from a factory that is closing or closed,
    // otherwise we would insert into byPathCache after close() has already drained it
    // (leaking the freshly-created Directory and its held write lock).
    if (closed) {
      throw new AlreadyClosedException("CachingDirectoryFactory is closed");
    }

    String fullPath = normalize(path);

    return byPathCache.computeIfAbsent(fullPath, dir -> {
      try {
        Directory directory = create(fullPath, createLockFactory(rawLockType), dirContext);
        byDirCache.put(directory, fullPath);
        return directory;
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }

    });
  }



  @Override
  public void init(NamedList args) {
    if (log.isTraceEnabled()) log.trace("init(NamedList args={}) - start", args);

    maxWriteMBPerSecFlush = (Double) args.get("maxWriteMBPerSecFlush");
    maxWriteMBPerSecMerge = (Double) args.get("maxWriteMBPerSecMerge");
    maxWriteMBPerSecRead = (Double) args.get("maxWriteMBPerSecRead");
    maxWriteMBPerSecDefault = (Double) args.get("maxWriteMBPerSecDefault");

    // override global config
    if (args.get(SolrXmlConfig.SOLR_DATA_HOME) != null) {
      dataHomePath = Paths.get((String) args.get(SolrXmlConfig.SOLR_DATA_HOME));
    }
    if (dataHomePath != null) {
      log.info(SolrXmlConfig.SOLR_DATA_HOME + "={}", dataHomePath);
    }

    if (log.isTraceEnabled()) log.trace("init(NamedList) - end");
  }


  @Override
  public void remove(Directory dir) throws IOException {
    String fullPath = byDirCache.remove(dir);
    if (fullPath != null) {
      byPathCache.remove(fullPath);
    } else {
      // dir was not tracked in byDirCache (e.g. created outside the cache, or the
      // mapping was already removed). We are about to close it, so make sure no
      // byPathCache entry still points at this Directory; otherwise a subsequent
      // get() would return a CLOSED Directory.
      byPathCache.values().removeIf(cached -> cached == dir);
    }
    closeWithListeners(dir);
    if (fullPath != null) {
      remove(fullPath);
    }
  }

  @Override public void release(String path) throws IOException {
    // Normalize the key exactly as get() does, otherwise an un-normalized key (e.g. with
    // a trailing slash) misses the cached entry and leaks the Directory.
    String fullPath = normalize(path);
    Directory dir = byPathCache.remove(fullPath);
    if (dir != null) {
      byDirCache.remove(dir);
    }
    // NOTE: the Directory is intentionally NOT closed here. This fork removed the
    // refcount machinery, so release() has no way to know whether another caller still
    // holds this Directory; closing it would risk a use-after-close and would drop the
    // Lucene write lock out from under a live IndexWriter. Residual risk: the evicted
    // Directory (and its held write lock) is not closed until remove()/close() runs.
    // Closing here safely would require reinstating per-holder accounting, which is out
    // of scope for this fix.
  }


  @Override
  public String normalize(String path) throws IOException {
    if (log.isTraceEnabled()) log.trace("normalize(String path={}) - start", path);


    path = stripTrailingSlash(path);

    return path;
  }

  protected static String stripTrailingSlash(String path) {
    if (log.isTraceEnabled()) log.trace("stripTrailingSlash(String path={}) - start", path);

    if (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }

    return path;
  }

  /**
   * Method for inspecting the cache
   *
   * @return paths in the cache which have not been marked "done"
   */
  public Set<String> getLivePaths() {

    return byPathCache.keySet();
  }

  @Override
  protected boolean deleteOldIndexDirectory(String oldDirPath) throws IOException {
    if (log.isTraceEnabled()) log.trace("deleteOldIndexDirectory(String oldDirPath={}) - start", oldDirPath);

    Set<String> livePaths = getLivePaths();
    if (livePaths.contains(oldDirPath)) {
      log.warn("Cannot delete directory {} as it is still being referenced in the cache!", oldDirPath);
      return false;
    }

    return super.deleteOldIndexDirectory(oldDirPath);
  }

}
