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
    closed = true;
    byPathCache.forEach((s, directory) -> org.apache.solr.common.util.IOUtils.closeQuietly(directory));
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
    IOUtils.closeQuietly(dir);
    byPathCache.remove(fullPath);
    if (fullPath != null) {
      remove(fullPath);
    }
  }

  @Override public void release(String fullPath) throws IOException {
    Directory dir = byPathCache.remove(fullPath);
    byDirCache.remove(dir);
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
