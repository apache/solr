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

package org.apache.solr.cats.exec;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * A shell a command line can be handed to. One process per command; no interactive session.
 *
 * <p>The quoting rules are the whole of an implementation: how a line becomes an argv the platform
 * will run, and how one argument is spelled so the shell passes it through verbatim. The two the
 * harness ships are {@link Shells#BASH} and {@link Shells#CMD}; a project whose tests must also run
 * under another shell implements this and {@linkplain Shells#register registers} it.
 */
public interface Shell {

  /** This shell's name, lowercase, as {@code EnabledOnShell} and {@code cats.shell} spell it. */
  String id();

  /**
   * Builds the argv for running {@code line}, writing any helper script into {@code scriptDir}.
   *
   * @param scriptName a per-command name, unique within {@code scriptDir}
   */
  List<String> processArgs(String line, Path scriptDir, String scriptName) throws IOException;

  /** Quotes a single argument so this shell passes it through verbatim. */
  String quote(String arg);

  /**
   * Quotes a single argument for a line a human will read and paste, rather than one the harness
   * will run.
   *
   * <p>Separate from {@link #quote} because that one may refuse what it cannot express exactly — a
   * {@code "} under {@code cmd.exe}, which is most of what a JSON request body is made of. This
   * never throws: a transcript must not be the thing that fails a test. The cost is that under
   * {@code cmd.exe} a value containing a quote, a caret or a {@code %} is approximated, so treat
   * such a line as a starting point.
   */
  default String quoteForDisplay(String arg) {
    return quote(arg);
  }

  /** Joins an argv into one command line for this shell. */
  default String joinArgs(List<String> argv) {
    StringBuilder sb = new StringBuilder();
    for (String arg : argv) {
      if (sb.length() > 0) {
        sb.append(' ');
      }
      sb.append(quote(arg));
    }
    return sb.toString();
  }
}
