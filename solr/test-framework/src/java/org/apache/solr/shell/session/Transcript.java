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

package org.apache.solr.shell.session;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Map;
import org.apache.solr.shell.exec.CommandResult;

/**
 * The record of what a session ran. Written to {@code transcript.log} in the session's work dir as
 * it goes, and kept in memory so a failing test can print it and copy it into the snapshot.
 */
public final class Transcript {

  private final Path file;
  private final StringBuilder buffer = new StringBuilder();

  public Transcript(Path file) {
    this.file = file;
  }

  /** Appends one command, preceded by the environment changes relative to the base environment. */
  public synchronized void record(CommandResult result, Map<String, String> envDiff) {
    StringBuilder entry = new StringBuilder();
    entry.append("--- ").append(Instant.now()).append(" ---\n");
    if (!envDiff.isEmpty()) {
      entry.append("env     : ").append(envDiff).append('\n');
    }
    entry.append(result.transcriptEntry()).append('\n');
    append(entry.toString());
  }

  /** Appends a free-form note, for things the harness itself did. */
  public synchronized void note(String message) {
    append("--- " + Instant.now() + " --- " + message + "\n\n");
  }

  private void append(String text) {
    buffer.append(text);
    try {
      Files.createDirectories(file.getParent());
      Files.writeString(
          file, text, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    } catch (IOException e) {
      // The in-memory copy is what tests actually report with; losing the file is not worth
      // failing a test over.
      buffer
          .append("[could not write transcript to ")
          .append(file)
          .append(": ")
          .append(e)
          .append("]\n");
    }
  }

  public Path file() {
    return file;
  }

  public synchronized String text() {
    return buffer.toString();
  }
}
