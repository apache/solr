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

import picocli.CommandLine;

/**
 * Picocli mixin that adds a {@code --help} option to a command without also adding {@code
 * --version}. Prefer this over {@code mixinStandardHelpOptions = true} on subcommands, since {@code
 * --version} is only meaningful on the top-level {@code bin/solr} command.
 */
public class HelpMixin {
  @CommandLine.Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "Print this help message and exit.")
  public boolean help;
}
