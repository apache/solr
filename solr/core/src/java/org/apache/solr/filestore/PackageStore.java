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

package org.apache.solr.filestore;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.solr.common.MapWriter;
import org.apache.solr.filestore.PackageStoreAPI.MetaData;
import org.apache.zookeeper.server.ByteBufferInputStream;

/**
 * The interface to be implemented by any package store provider
 * * @lucene.experimental
 */
public interface PackageStore {

  /**
   * Store a file into the filestore. This should ensure that it is replicated
   * across all nodes in the cluster
   */
  void put(FileEntry fileEntry) throws IOException;

  /**
   * read file content from a given path
   */
  void get(String path, Consumer<FileEntry> filecontent, boolean getMissing) throws IOException;

  /**
   * Fetch a resource from another node
   * internal API
   */
  boolean fetch(String path, String from) throws IOException;

  List<FileDetails> list(String path, Predicate<String> predicate) throws IOException;

  /** Sync a local file to all nodes. All the nodes are asked to pull the file from this node
   */
  void syncToAllNodes(String path) throws IOException;

  /**
   * get the real path on filesystem
   */
  Path getRealpath(String path);

  /**
   * The type of the resource
   */
  FileType getType(String path, boolean fetchMissing) throws IOException;

  /**Get all the keys in the package store. The data is a .DER file content
   */
  Map<String,byte[]> getKeys() throws IOException;

  /**Refresh the files in a path. May be this node does not have all files
   * @param path the path to be refreshed.
   */
  void refresh(String path);

  public class FileEntry {
    final ByteBuffer buf;
    final MetaData meta;
    final String path;
    /**
     * Stream supplier for the streaming path ({@code solr.filestore.stream.enabled=true}).
     * When non-null, {@link #getInputStream()} delegates to this supplier and
     * {@link #getBuffer()} returns {@code null}.  This avoids whole-file heap buffering.
     * Small metadata files always use the {@code buf} path so that {@code .array()} callers
     * are never broken (invariant #8).
     */
    private final Supplier<InputStream> streamSupplier;

    FileEntry(ByteBuffer buf, MetaData meta, String path) {
      this.buf = buf;
      this.meta = meta;
      this.path = path;
      this.streamSupplier = null;
    }

    /**
     * Streaming-path constructor.  {@code buf} may be {@code null} when the file is accessed
     * via {@code streamSupplier}.  {@code streamSupplier} must return a fresh, open
     * {@link InputStream} each time it is called.
     */
    FileEntry(ByteBuffer buf, MetaData meta, String path, Supplier<InputStream> streamSupplier) {
      this.buf = buf;
      this.meta = meta;
      this.path = path;
      this.streamSupplier = streamSupplier;
    }

    public String getPath() {
      return path;
    }

    /**
     * Returns an {@link InputStream} over the file content.
     * On the streaming path ({@code streamSupplier} non-null) this opens a fresh stream each
     * time; callers must close it.  On the legacy path a {@link ByteBufferInputStream} wrapping
     * the in-memory buffer is returned.  Returns {@code null} when no data source is available.
     */
    public InputStream getInputStream() {
      if (streamSupplier != null) return streamSupplier.get();
      if (buf != null) return new ByteBufferInputStream(buf);
      return null;
    }

    /**
     * Returns the in-memory {@link ByteBuffer} for files fetched on the legacy
     * (whole-file heap) path, or {@code null} on the streaming path.
     *
     * <p>Small metadata files always use this path and callers may safely call
     * {@code .array()} on the returned buffer (invariant #8).  Do NOT call
     * {@code .array()} on a buffer that came from a direct-buffer path.
     */
    public ByteBuffer getBuffer() {
      return buf;
    }

    public MetaData getMetaData() {
      return meta;
    }
  }

  enum FileType {
    FILE, DIRECTORY, NOFILE, METADATA
  }

  interface FileDetails extends MapWriter {

    MetaData getMetaData();

    Date getTimeStamp();

    long size();

    boolean isDir();


  }


}
