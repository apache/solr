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

import static org.apache.solr.cli.SolrCLI.print;

import java.io.PrintStream;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

/** Supports zk help information in the bin/solr script. */
public class ZkToolHelp extends ToolBase {

  public ZkToolHelp() {
    this(CLIO.getOutStream());
  }

  public ZkToolHelp(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public List<Option> getOptions() {
    return List.of(
        Option.builder()
            .longOpt("print-short-zk-usage")
            .desc("Invokes the short summary help for zk commands.")
            .build(),
        SolrCLI.OPTION_VERBOSE);
  }

  @Override
  public String getName() {
    return "zk-tool-help";
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    SolrCLI.raiseLogLevelUnlessVerbose(cli);

    if (cli.hasOption("print-short-zk-usage")) {
      String scriptCommand = cli.getArgs()[0];
      print(
          "You must invoke this subcommand using the zk command.   bin/solr zk "
              + scriptCommand
              + ".");
    }
  }
}
