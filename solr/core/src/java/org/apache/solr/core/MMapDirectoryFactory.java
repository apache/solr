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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.function.BiPredicate;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Directly provide MMapDirectory instead of relying on {@link
 * org.apache.lucene.store.FSDirectory#open}.
 *
 * <p>Can set the following parameters:
 *
 * <ul>
 *   <li>maxChunkSize -- The Max chunk size. See {@link MMapDirectory#MMapDirectory(Path,
 *       LockFactory, long)}
 *   <li>preload -- Whether to load each index file into the OS page cache when that file is opened.
 *   <li>preloadExtensions -- Comma separated file extensions, such as {@code .vex,.veb}. Only files
 *       whose name ends with a listed extension are loaded into the OS page cache when those files
 *       are opened. Takes precedence over {@code preload}.
 * </ul>
 */
public class MMapDirectoryFactory extends StandardDirectoryFactory {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  boolean preload;
  private long maxChunk;
  private Set<String> preloadExtensions = Set.of();

  @Override
  public void init(NamedList<?> args) {
    super.init(args);
    SolrParams params = args.toSolrParams();
    maxChunk = params.getLong("maxChunkSize", MMapDirectory.DEFAULT_MAX_CHUNK_SIZE);
    if (maxChunk <= 0) {
      throw new IllegalArgumentException("maxChunk must be greater than 0");
    }
    if (params.get("unmap") != null) {
      log.warn(
          "It is no longer possible to configure unmapping of index files on DirectoryFactory level in solrconfig.xml.");
      log.warn(
          "To disable unmapping, pass -Dorg.apache.lucene.store.MMapDirectory.enableUnmapHack=false on Solr's command line.");
    }
    preload = params.getBool("preload", false); // default turn-off
    preloadExtensions = parsePreloadExtensions(params.get("preloadExtensions"));
    if (preload && !preloadExtensions.isEmpty()) {
      log.warn("Ignoring preload=true because preloadExtensions takes precedence over it.");
    }
  }

  private static Set<String> parsePreloadExtensions(String value) {
    if (value == null) {
      return Set.of();
    }
    Set<String> extensions = new HashSet<>();
    for (String extension : value.split(",")) {
      extension = extension.trim().toLowerCase(Locale.ROOT);
      if (!extension.isEmpty()) {
        extensions.add(extension.startsWith(".") ? extension : "." + extension);
      }
    }
    return Set.copyOf(extensions);
  }

  @VisibleForTesting
  BiPredicate<String, IOContext> preloadPredicate() {
    if (preloadExtensions.isEmpty()) {
      return preload ? MMapDirectory.ALL_FILES : MMapDirectory.NO_FILES;
    }
    Set<String> extensions = preloadExtensions;
    return (name, ioContext) -> {
      int dot = name.lastIndexOf('.');
      return dot >= 0 && extensions.contains(name.substring(dot).toLowerCase(Locale.ROOT));
    };
  }

  @Override
  protected Directory create(String path, LockFactory lockFactory) throws IOException {
    MMapDirectory mapDirectory = new MMapDirectory(Path.of(path), lockFactory, maxChunk);
    mapDirectory.setPreload(preloadPredicate());
    return mapDirectory;
  }
}
