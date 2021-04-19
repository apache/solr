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

package org.apache.solr.blob;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.solr.common.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class BlobDirectory extends FilterDirectory {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final IOContext SYNC_IO_CONTEXT = new IOContext();

  private final String blobDirPath;
  private final BlobPusher blobPusher;
  /**
   * Map of {@link BlobFileSupplier} for each file created by this directory. Keys are file names.
   * Each {@link BlobFileSupplier} keeps a reference to the {@link IndexOutput} created for the
   * file, to provide the checksums on {@link #sync(Collection)}. But it is able to free earlier the
   * reference each time an {@link IndexOutput} is closed, by getting the checksum at that time.
   */
  private final Map<String, BlobFileSupplier> blobFileSupplierMap = new HashMap<>();
  private final Set<String> synchronizedFileNames = new HashSet<>();
  private final Collection<String> deletedFileNames = new ArrayList<>();
  private final Object lock = new Object();
  private volatile boolean isOpen;

  public BlobDirectory(Directory delegate, String blobDirPath, BlobPusher blobPusher) {
    super(delegate);
    this.blobDirPath = blobDirPath;
    this.blobPusher = blobPusher;
  }

  @Override
  public void deleteFile(String name) throws IOException {
    log.debug("deleteFile {}", name);
    in.deleteFile(name);
    synchronized (lock) {
      deletedFileNames.add(name);
    }
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    log.debug("createOutput {}", name);
    IndexOutput indexOutput = in.createOutput(name, context);
    BlobFileSupplier blobFileSupplier = new BlobFileSupplier(indexOutput);
    synchronized (lock) {
      blobFileSupplierMap.put(name, blobFileSupplier);
    }
    return new BlobIndexOutput(indexOutput, blobFileSupplier);
  }

  // createTempOutput(): We don't track tmp files since they are not synced.

  @Override
  public void sync(Collection<String> names) throws IOException {
    log.debug("sync {}", names);
    in.sync(names);
    synchronized (lock) {
      synchronizedFileNames.addAll(names);
    }
  }

  @Override
  public void rename(String source, String dest) throws IOException {
    log.debug("rename {} to {}", source, dest);
    in.rename(source, dest);
    synchronized (lock) {
      // Rename the corresponding BlobFile.
      BlobFileSupplier blobFileSupplier = blobFileSupplierMap.remove(source);
      if (blobFileSupplier != null) {
        blobFileSupplier.rename(source, dest);
        blobFileSupplierMap.put(dest, blobFileSupplier);
      }
      // Rename the tracked synchronized file.
      if (synchronizedFileNames.remove(source)) {
        synchronizedFileNames.add(dest);
      }
    }
  }

  @Override
  public void syncMetaData() throws IOException {
    log.debug("syncMetaData");
    in.syncMetaData();
    syncToRepository();
  }

  private void syncToRepository() throws IOException {
    log.debug("File names to sync {}", synchronizedFileNames);

    Collection<BlobFile> writes;
    Collection<String> deletes;
    synchronized (lock) {
      writes = new ArrayList<>(synchronizedFileNames.size());
      for (String fileName : synchronizedFileNames) {
        BlobFileSupplier blobFileSupplier = blobFileSupplierMap.get(fileName);
        if (blobFileSupplier != null) {
          // Only sync files that were synced since this directory was released.
          // Previous files don't need to be synced.
          blobFileSupplier.freeIndexOutput();
          writes.add(blobFileSupplier.getBlobFile());
        }
      }
      synchronizedFileNames.clear();
      deletes = new ArrayList<>(deletedFileNames);
      deletedFileNames.clear();
    }

    log.debug("Sync to repository writes={} deleted={}", writes, deletes);
    blobPusher.push(blobDirPath, writes, this::openInputStream, deletes);
  }

  private InputStream openInputStream(BlobFile blobFile) throws IOException {
    return new IndexInputInputStream(in.openInput(blobFile.fileName(), SYNC_IO_CONTEXT));
  }

  public void release() {
    log.debug("release");
    synchronized (lock) {
      blobFileSupplierMap.clear();
      synchronizedFileNames.clear();
      deletedFileNames.clear();
    }
  }

  // obtainLock(): We get the delegate Directory lock.

  @Override
  public void close() {
    log.debug("close");
    isOpen = false;
    IOUtils.closeQuietly(in);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + in.toString() + ")";
  }

  @Override
  protected void ensureOpen() throws AlreadyClosedException {
    if (!isOpen) {
      throw new AlreadyClosedException("This Directory is closed");
    }
  }

  /**
   * Delegating {@link IndexOutput} that hooks the {@link #close()} method to compute the checksum.
   * The goal is to free the reference to the delegate {@link IndexOutput} when it is closed because
   * we only need it to get the checksum.
   */
  private static class BlobIndexOutput extends FilterIndexOutput {

    private final BlobFileSupplier blobFileSupplier;

    BlobIndexOutput(IndexOutput delegate, BlobFileSupplier blobFileSupplier) {
      super("Blob " + delegate.toString(), delegate.getName(), delegate);
      this.blobFileSupplier = blobFileSupplier;
    }

    @Override
    public void close() throws IOException {
      blobFileSupplier.freeIndexOutput();
      // TODO
      //  It could be possible to start pushing the file asynchronously with BlobPusher at this time,
      //  provided that the file size is larger than a threshold (to avoid sending small intermediate
      //  files that could be removed later before the sync, e.g. merge-on-commit). We would have to
      //  wait for the completion of the push in the BlobPusher when sync()/syncMetadata() is called.
      //  Other option: intercept the calls to the write methods, buffer written data, and start
      //  pushing to BlobPusher earlier than the call to this close method.
      super.close();
    }
  }

  /**
   * Supplies the length and checksum of a file created in this directory. Keeps a reference to the
   * file {@link IndexOutput} to be able to get its final length and checksum. However we try to
   * free the reference as soon as we can (when the {@link IndexOutput} is closed so we know the
   * content is final).
   */
  private static class BlobFileSupplier {

    IndexOutput indexOutput;
    String name;
    BlobFile blobFile;

    BlobFileSupplier(IndexOutput indexOutput) {
      this.indexOutput = indexOutput;
      name = indexOutput.getName();
    }

    void rename(String source, String dest) {
      assert name.equals(source);
      name = dest;
      if (blobFile != null) {
        blobFile = new BlobFile(name, blobFile.size(), blobFile.checksum());
        assert indexOutput == null;
      }
    }

    /**
     * Frees the reference to the {@link IndexOutput}.
     * Creates the {@link BlobFile} if it has not been created yet.
     */
    void freeIndexOutput() throws IOException {
      if (indexOutput != null) {
        blobFile = new BlobFile(name, indexOutput.getFilePointer(), indexOutput.getChecksum());
        indexOutput = null;
      }
    }

    /**
     * Gets the {@link BlobFile}. {@link #freeIndexOutput()} must have been called before.
     */
    BlobFile getBlobFile() {
      assert indexOutput == null : "IndexOutput must be freed before";
      assert blobFile != null;
      return blobFile;
    }
  }
}
