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
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.io.FileExistsException;

/** */
public class FileUtils {

  public static boolean fileExists(String filePathString) {
    return Files.exists(Path.of(filePathString));
  }

  // Files.createDirectories has odd behavior if the path is a symlink and it already exists
  // _even if it's a symlink to a directory_.
  //
  // oddly, if the path to be created just contains a symlink in intermediate levels,
  // Files.createDirectories works just fine.
  //
  // This works around that issue
  public static Path createDirectories(Path path) throws IOException {
    if (Files.exists(path) && Files.isSymbolicLink(path)) {
      Path real = path.toRealPath();
      if (Files.isDirectory(real)) return real;
      throw new FileExistsException(
          "Tried to create a directory at to an existing non-directory symlink: "
              + path.toString());
    }
    return Files.createDirectories(path);
  }

  /**
   * Checks whether a child path falls under a particular parent
   *
   * <p>Useful for validating user-provided relative paths, which generally aren't expected to
   * "escape" a given parent/root directory. Parent and child paths are "normalized" by {@link
   * Path#normalize()}. This removes explicit backtracking (e.g. "../") though it will not resolve
   * symlinks if any are present in the provided Paths, so some forms of parent "escape" remain
   * undetected. Paths needn't exist as a file or directory for comparison purposes.
   *
   * <p>Note, this method does not consult the file system
   *
   * @param parent the path of a 'parent' node. Path must be syntactically valid but needn't exist.
   * @param potentialChild the path of a potential child. Typically obtained via:
   *     parent.resolve(relativeChildPath). Path must be syntactically valid but needn't exist.
   * @return true if 'potentialChild' nests under the provided 'parent', false otherwise.
   */
  public static boolean isPathAChildOfParent(Path parent, Path potentialChild) {
    final var normalizedParent = parent.toAbsolutePath().normalize();
    final var normalizedChild = potentialChild.toAbsolutePath().normalize();

    return normalizedChild.startsWith(normalizedParent)
        && !normalizedChild.equals(normalizedParent);
  }
}
