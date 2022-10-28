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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public class DirectoryUtil {
  private DirectoryUtil() {}

  /**
   * Recursively copy the contents of one directory into another. For example:
   *
   * <pre>
   *   copyDirectory(Path.of("src"), Path.of("dst"))
   * </pre>
   *
   * will copy the contents of src directly into dst. This will not create a new "src" folder inside
   * of dst.
   */
  public static void copyDirectoryContents(final Path source, final Path destination)
      throws IOException {
    assert source.getFileSystem().equals(destination.getFileSystem());

    Files.walkFileTree(
        source,
        new SimpleFileVisitor<>() {
          private Path resolveTarget(Path other) {
            return destination.resolve(source.relativize(other));
          }

          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
              throws IOException {
            Files.createDirectories(resolveTarget(dir));
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            Files.copy(file, resolveTarget(file));
            return FileVisitResult.CONTINUE;
          }
        });
  }
}
