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

package org.apache.solr.cli;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Edits Solr shell and Windows include files without changing unrelated settings. */
final class IncludeFileEditor {
  enum Format {
    SHELL(Pattern.compile("\\s*(?:export\\s+)?([A-Za-z_][A-Za-z0-9_]*)\\s*=.*"), "# "),
    WINDOWS(Pattern.compile("\\s*(?i:set)\\s+\"?([A-Za-z_][A-Za-z0-9_]*)\\s*=.*"), "REM ");

    private final Pattern assignment;
    private final String comment;

    Format(Pattern assignment, String comment) {
      this.assignment = assignment;
      this.comment = comment;
    }

    private String variable(String line) {
      var matcher = assignment.matcher(line);
      if (!matcher.matches()) return null;
      return this == WINDOWS ? matcher.group(1).toUpperCase(Locale.ROOT) : matcher.group(1);
    }
  }

  private IncludeFileEditor() {}

  static String update(
      String original, Map<String, String> settings, Set<String> replaced, Format format) {
    String newline = newline(original, format);
    StringBuilder output = new StringBuilder();
    for (String line : original.split("\\R")) {
      String variable = format.variable(line);
      if (variable == null || !replaced.contains(variable)) output.append(line).append(newline);
    }
    settings.forEach(
        (key, value) -> {
          if (!key.matches("[A-Za-z_][A-Za-z0-9_]*")
              || value.indexOf('\n') >= 0
              || value.indexOf('\r') >= 0
              || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(
                "Include-file settings must be single-line variable assignments.");
          }
          if (format == Format.WINDOWS) {
            output.append("set ").append(key).append('=').append(value);
          } else {
            output.append(key).append("='").append(value.replace("'", "'\"'\"'")).append("'");
          }
          output.append(newline);
        });
    return output.toString();
  }

  static String disable(String original, Set<String> settings, Format format) {
    String newline = newline(original, format);
    StringBuilder output = new StringBuilder();
    boolean changed = false;
    for (String line : original.split("\\R")) {
      String variable = format.variable(line);
      if (variable != null && settings.contains(variable)) {
        output.append(format.comment);
        changed = true;
      }
      output.append(line).append(newline);
    }
    return changed ? output.toString() : original;
  }

  private static String newline(String original, Format format) {
    return original.contains("\r\n") || (original.isEmpty() && format == Format.WINDOWS)
        ? "\r\n"
        : "\n";
  }

  static void write(Path path, byte[] data) throws IOException {
    if (!Files.exists(path)) {
      if (path.getFileSystem().supportedFileAttributeViews().contains("posix")) {
        Files.createFile(
            path,
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
      } else {
        Files.createFile(path);
      }
    }
    Files.write(path, data);
  }
}
