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

package org.apache.solr.core.backup.repository;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.io.file.PathUtils;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.DirectoryFactory;

/**
 * A concrete implementation of {@linkplain BackupRepository} interface supporting backup/restore of
 * Solr indexes to a local file-system. (Note - This can even be used for a shared file-system if it
 * is exposed via a local file-system interface e.g. NFS).
 */
public class LocalFileSystemRepository implements BackupRepository {

  private NamedList<?> config = null;

  private Path baseLocation;

  @Override
  public void init(NamedList<?> args) {
    this.config = args;
    baseLocation =
        Optional.<String>ofNullable(getConfigProperty(CoreAdminParams.BACKUP_LOCATION))
            .map(Path::of)
            .or(
                () -> Optional.ofNullable(System.getProperty("solr.backups.path")).map(Path::of)
            )
            .orElseThrow(
                () ->
                    new SolrException(
                        SolrException.ErrorCode.SERVER_ERROR,
                        "Required init param "
                            + CoreAdminParams.BACKUP_LOCATION
                            + " was not provided."));
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getConfigProperty(String name) {
    return (T) this.config.get(name);
  }

  /**
   * This method returns the location where the backup should be stored (or restored from). An error
   * will be thrown if the given directory is outside of the repository's base location.
   *
   * @param override The location parameter supplied by the user.
   * @return If <code>override</code> is not null then return the same value Otherwise return the
   *     default configuration value for the {@linkplain CoreAdminParams#BACKUP_LOCATION} parameter.
   * @throws SolrException if the given path cannot be resolved as a subdirectory of the
   *     repository's base location
   */
  @Override
  public String getBackupLocation(String override) {
    if (override == null) {
      return baseLocation.toString();
    }
    URI overrideUri = null;
    try {
      overrideUri = new URI(override);
      if (!overrideUri.isAbsolute()) {
        overrideUri = Path.of(override).toUri();
      }
    } catch (URISyntaxException ex) {
      overrideUri = Path.of(override).toUri();
    }
    Path overridePath = Path.of(overrideUri.getPath());
    if (!overridePath.isAbsolute()) {
      overridePath = baseLocation.resolve(overridePath);
    }
    if (!overridePath.startsWith(baseLocation)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Path "
              + override
              + " cannot be used to store backups, since it is not contained within the configured base backup location: "
              + baseLocation);
    }
    return overridePath.toString();
  }

  @Override
  public URI createURI(String location) {
    Objects.requireNonNull(location);

    URI result = null;
    try {
      result = new URI(location);
      if (!result.isAbsolute()) {
        result = Path.of(location).toUri();
      }
    } catch (URISyntaxException ex) {
      result = Path.of(location).toUri();
    }

    return result;
  }

  @Override
  public URI resolve(URI baseUri, String... pathComponents) {
    if (pathComponents.length <= 0) {
      throw new IllegalArgumentException("pathComponents.length must be greater than 0.");
    }

    Path result = Path.of(baseUri);
    for (String pathComponent : pathComponents) {
      try {
        result = result.resolve(pathComponent);
      } catch (Exception e) {
        // unlikely to happen
        throw new RuntimeException(e);
      }
    }

    return result.toUri();
  }

  @Override
  public void createDirectory(URI path) throws IOException {
    Path p = Path.of(path);
    if (!Files.exists(p, LinkOption.NOFOLLOW_LINKS)) {
      Files.createDirectory(p);
    }
  }

  @Override
  public void deleteDirectory(URI path) throws IOException {
    PathUtils.deleteDirectory(Path.of(path));
  }

  @Override
  public boolean exists(URI path) throws IOException {
    return Files.exists(Path.of(path));
  }

  @Override
  public IndexInput openInput(URI dirPath, String fileName, IOContext ctx) throws IOException {
    try (FSDirectory dir = new NIOFSDirectory(Path.of(dirPath), NoLockFactory.INSTANCE)) {
      return dir.openInput(fileName, ctx);
    }
  }

  @Override
  public OutputStream createOutput(URI path) throws IOException {
    return Files.newOutputStream(Path.of(path));
  }

  @Override
  public String[] listAll(URI dirPath) throws IOException {
    // It is better to check the existence of the directory first since
    // creating a FSDirectory will create a corresponds folder if the directory does not exist
    if (!exists(dirPath)) {
      return new String[0];
    }

    try (FSDirectory dir = new NIOFSDirectory(Path.of(dirPath), NoLockFactory.INSTANCE)) {
      return dir.listAll();
    }
  }

  @Override
  public PathType getPathType(URI path) throws IOException {
    return Files.isDirectory(Path.of(path)) ? PathType.DIRECTORY : PathType.FILE;
  }

  @Override
  public void copyIndexFileFrom(
      Directory sourceDir, String sourceFileName, URI destDir, String destFileName)
      throws IOException {
    try (FSDirectory dir = new NIOFSDirectory(Path.of(destDir), NoLockFactory.INSTANCE)) {
      copyIndexFileFrom(sourceDir, sourceFileName, dir, destFileName);
    }
  }

  @Override
  public void copyIndexFileTo(
      URI sourceDir, String sourceFileName, Directory dest, String destFileName)
      throws IOException {
    try (FSDirectory dir = new NIOFSDirectory(Path.of(sourceDir), NoLockFactory.INSTANCE)) {
      dest.copyFrom(dir, sourceFileName, destFileName, DirectoryFactory.IOCONTEXT_NO_CACHE);
    }
  }

  @Override
  public void delete(URI path, Collection<String> files) throws IOException {
    if (files.isEmpty()) return;

    try (FSDirectory dir = new NIOFSDirectory(Path.of(path), NoLockFactory.INSTANCE)) {
      for (String file : files) {
        try {
          dir.deleteFile(file);
        } catch (NoSuchFileException ignore) {

        }
      }
    }
  }

  @Override
  public void close() throws IOException {}
}
