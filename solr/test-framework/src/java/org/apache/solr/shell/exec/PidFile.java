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

package org.apache.solr.shell.exec;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * The {@code solr-<port>.pid} files {@code bin/solr} writes into {@code SOLR_PID_DIR}.
 *
 * <p>These are the harness' source of truth for which nodes exist. A detached Solr node is
 * re-parented the moment its launching shell exits — {@code nohup} on Unix, {@code start /B} on
 * Windows — so it is not reliably a descendant of anything the harness started, and the process
 * tree cannot be used to find it.
 */
public final class PidFile {

  private static final Pattern NAME = Pattern.compile("solr-(\\d+)\\.pid");

  private final Path path;
  private final int port;

  private PidFile(Path path, int port) {
    this.path = path;
    this.port = port;
  }

  /** Every pid file in {@code pidDir}, or an empty list if it is not a directory. */
  public static List<PidFile> listIn(Path pidDir) throws IOException {
    if (!Files.isDirectory(pidDir)) {
      return List.of();
    }
    List<PidFile> found = new ArrayList<>();
    try (Stream<Path> files = Files.list(pidDir)) {
      for (Path file : files.toList()) {
        Matcher m = NAME.matcher(file.getFileName().toString());
        if (m.matches()) {
          found.add(new PidFile(file, Integer.parseInt(m.group(1))));
        }
      }
    }
    return found;
  }

  /** The pid file for a port, whether or not it exists. */
  public static Path pathFor(Path pidDir, int port) {
    return pidDir.resolve("solr-" + port + ".pid");
  }

  public Path path() {
    return path;
  }

  public int port() {
    return port;
  }

  /** The PID it names, empty when the file is gone, blank or unparseable. */
  public OptionalLong pid() {
    try {
      String text = Files.readString(path, StandardCharsets.UTF_8).trim();
      return text.isEmpty() ? OptionalLong.empty() : OptionalLong.of(Long.parseLong(text));
    } catch (IOException | NumberFormatException e) {
      return OptionalLong.empty();
    }
  }

  /** Whether the process it names is still running. A file alone does not mean a live node. */
  public boolean isProcessAlive() {
    OptionalLong pid = pid();
    return pid.isPresent()
        && ProcessHandle.of(pid.getAsLong()).map(ProcessHandle::isAlive).orElse(false);
  }

  @Override
  public String toString() {
    return path.toString();
  }
}
