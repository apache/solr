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
package org.apache.solr.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.solr.common.SolrException;

/**
 * @since solr 1.3
 */
public class VersionedFile {
  /* Open the latest version of a file... fileName if that exists, or
   * the last fileName.* after being sorted lexicographically.
   * Older versions of the file are deleted (and queued for deletion if
   * that fails).
   * TODO SOLR-8282 dirName should be a Path instead of string
   */
  public static InputStream getLatestFile(String dirName, String fileName) throws IOException {
    Collection<Path> oldFiles = null;
    final String prefix = fileName + '.';
    Path f = Path.of(dirName, fileName);
    InputStream is = null;

    // there can be a race between checking for a file and opening it...
    // the user may have just put a new version in and deleted an old version.
    // try multiple times in a row.
    for (int retry = 0; retry < 10 && is == null; retry++) {
      try {
        if (!Files.exists(f)) {
          Path dir = Path.of(dirName);
          List<Path> fileList;

          try (Stream<Path> files = Files.list(dir)) {
            fileList =
                files
                    .filter((file) -> file.getFileName().toString().startsWith(prefix))
                    .sorted()
                    .toList();
          } catch (IOException e) {
            throw new SolrException(
                SolrException.ErrorCode.SERVER_ERROR, "Unable to list files in " + dir, e);
          }

          f = dir.resolve(fileList.getLast());
          oldFiles = new ArrayList<>();
          for (int i = 0; i < fileList.size() - 1; i++) {
            oldFiles.add(dir.resolve(fileList.get(i)));
          }
        }

        is = Files.newInputStream(f);
      } catch (Exception e) {
        // swallow exception for now
      }
    }

    // allow exception to be thrown from the final try.
    if (is == null) {
      is = Files.newInputStream(f);
    }

    // delete old files only after we have successfully opened the newest
    if (oldFiles != null) {
      delete(oldFiles);
    }

    return is;
  }

  private static final Set<Path> deleteList = new HashSet<>();

  private static synchronized void delete(Collection<Path> files) {
    synchronized (deleteList) {
      deleteList.addAll(files);
      List<Path> deleted = new ArrayList<>();
      for (Path df : deleteList) {
        try {
          try {
            Files.deleteIfExists(df);
          } catch (IOException cause) {
            // TODO: should this class care if a file couldn't be deleted?
            // this just emulates previous behavior, where only SecurityException would be handled.
          }
          // deleteList.remove(df);
          deleted.add(df);
        } catch (SecurityException e) {
          if (!Files.exists(df)) {
            deleted.add(df);
          }
        }
      }
      deleteList.removeAll(deleted);
    }
  }
}
