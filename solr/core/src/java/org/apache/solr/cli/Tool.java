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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public interface Tool {
  /**
   * Defines the interface to a Solr tool that can be run from this command-line app.
   *
   * @deprecated Only used by the commons-cli parser. Tools built on picocli take their name from
   *     the {@code @Command} annotation instead.
   */
  @Deprecated
  String getName();

  /**
   * Provides a Usage string to display in help output. Defaults to auto generating usage string.
   * Override for custom string
   *
   * @return The custom usage string or 'null' to auto generate (default)
   * @deprecated Only used by the commons-cli parser. Picocli generates the synopsis from the
   *     command's annotations.
   */
  @Deprecated
  default String getUsage() {
    return null;
  }

  /**
   * Optional header to display before the options in help output. Defaults to 'List of options:'
   *
   * @deprecated Only used by the commons-cli parser. Picocli uses {@code @Command(header = ...)}.
   */
  @Deprecated
  default String getHeader() {
    return "List of options:";
  }

  /**
   * Optional footer to display after the options in help output. Defaults to a link to reference
   * guide
   *
   * @deprecated Only used by the commons-cli parser. Picocli uses {@code @Command(footer = ...)}.
   */
  @Deprecated
  default String getFooter() {
    return "\nPlease see the Reference Guide for more tools documentation: https://solr.apache.org/guide/solr/latest/deployment-guide/solr-control-script-reference.html";
  }

  /** Return non-null runtime of the tool. */
  ToolRuntime getRuntime();

  /**
   * Retrieve the {@link Options} supported by this tool.
   *
   * @return The {@link Options} this tool supports.
   * @deprecated Only used by the commons-cli parser. Picocli tools declare their options as
   *     annotated fields instead.
   */
  @Deprecated
  Options getOptions();

  /**
   * Runs the tool against an already-parsed commons-cli command line.
   *
   * @deprecated Implement {@link ToolBase#callTool()} instead, which picocli invokes.
   */
  @Deprecated
  int runTool(CommandLine cli) throws Exception;
}
