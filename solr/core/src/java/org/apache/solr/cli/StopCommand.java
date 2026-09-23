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
 * This class is currently only used for printing CLI usage. The stop logic is currently handled in
 * start script.
 */
@CommandLine.Command(
    name = "stop",
    description = "Stops Solr.",
    footerHeading = "%nExamples:%n",
    footer = {
      "  # Stop Solr running on the default port",
      "  bin/solr stop -p 8983",
      "",
      "  # Stop all running Solr instances on this host",
      "  bin/solr stop --all"
    })
public class StopCommand {

  @CommandLine.Mixin HelpMixin helpMixin;

  @CommandLine.Option(
      names = {"-p", "--port"},
      description =
          "Specify the port the Solr HTTP listener is bound to.\n"
              + "The STOP_PORT is derived as ($SOLR_PORT-1000).")
  String port;

  @CommandLine.Option(
      names = {"-k", "--key"},
      description = "Stop key; default is solrrocks",
      defaultValue = "solrrocks")
  String key;

  @CommandLine.Option(
      names = "--all",
      description = "Find and stop all running Solr servers on this host")
  boolean all;

  @CommandLine.Option(
      names = {"--verbose"},
      description = "Enable verbose mode.")
  boolean verbose;
}
