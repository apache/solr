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
package org.apache.solr.common.util;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IOUtils {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public static Exception closeQuietly(Closeable closeable) {
    Exception returnException = null;
    try {
      if (closeable != null) {
        closeable.close();
      }
    } catch (Exception e) {
      log.error("Error while closing", e);
      returnException = e;
    }

    return returnException;
  }

  public static void closeQuietly(AutoCloseable closeable) {
    try {
      if (closeable != null) {
        closeable.close();
      }
    } catch (Exception e) {
      log.error("Error while closing", e);
    }
  }

  public static void deleteDirectory(String path)  {
    deleteDirectory(Paths.get(path));
  }

  public static void deleteDirectory(Path path) {
    List<Path> files = new ArrayList<>(32);
    try {

      Files.walkFileTree(path, new FileVisitor<>() {
        @Override public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
          return FileVisitResult.CONTINUE;
        }

        @Override public FileVisitResult postVisitDirectory(Path dir, IOException impossible) throws IOException {
          files.add(dir);
          return FileVisitResult.CONTINUE;
        }

        @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          files.add(file);
          return FileVisitResult.CONTINUE;
        }

        @Override public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
          files.add(file);
          return FileVisitResult.CONTINUE;
        }
      });
    } catch (IOException impossible) {
      throw new AssertionError("visitor threw exception", impossible);
    }
    files.sort(Comparator.reverseOrder());

    files.forEach(file -> {
      try {
        Files.deleteIfExists(file);
      }  catch (NoSuchFileException e) {
        // ignore
      } catch (IOException e) {
        log.warn("WARN: could not delete file:{} {}", path, e.getMessage());
      }
    });
  }
}
