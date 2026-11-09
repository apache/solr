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
 * This class is currently only used for printing CLI usage. The restart logic is currently handled
 * in the start script, which parses the same options as {@code start} plus the stop key.
 */
@CommandLine.Command(
    name = "restart",
    description = "Restarts Solr.",
    footerHeading = "%nExamples:%n",
    footer = {
      "  # Restart Solr running on the default port",
      "  bin/solr restart",
      "",
      "  # Restart Solr running on a custom port with 2g heap",
      "  bin/solr restart -p 8984 -m 2g"
    })
public class RestartCommand extends StartCommand {

  @CommandLine.Option(
      names = {"-k", "--key"},
      description = "Stop key; default is solrrocks",
      defaultValue = "solrrocks")
  String key;

  /** The bin/solr script performs the restart; this command only documents its options. */
  @Override
  public Integer call() {
    return 0;
  }
}
