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
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/** Supports zk help information in the bin/solr script. */
public class ZkToolHelp extends ToolBase {

  private static final Option PRINT_ZK_SUBCOMMAND_OPTION =
      Option.builder()
          .longOpt("print-zk-subcommand-usage")
          .desc("Reminds user to prepend zk to invoke the command.")
          .build();

  private static final Option PRINT_LONG_ZK_USAGE_OPTION =
      Option.builder()
          .longOpt("print-long-zk-usage")
          .desc("Invokes the detailed help for zk commands.")
          .build();

  public ZkToolHelp() {
    this(CLIO.getOutStream());
  }

  public ZkToolHelp(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public Options getOptions() {
    return super.getOptions()
        .addOption(PRINT_ZK_SUBCOMMAND_OPTION)
        .addOption(PRINT_LONG_ZK_USAGE_OPTION);
  }

  @Override
  public String getName() {
    return "zk-tool-help";
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    if (cli.hasOption(PRINT_ZK_SUBCOMMAND_OPTION)) {
      String scriptCommand = cli.getArgs()[0];
      print(
          "You must invoke this subcommand using the zk command.   bin/solr zk "
              + scriptCommand
              + ".");
    }
    if (cli.hasOption(PRINT_LONG_ZK_USAGE_OPTION)) {
      print("usage:");
      print(new ZkLsTool().getUsage());
      print(new ZkCpTool().getUsage());
      print(new ZkMvTool().getUsage());

      print(new ConfigSetUploadTool().getUsage());
      print(new ConfigSetDownloadTool().getUsage());
      print(new ZkMkrootTool().getUsage());
      print(new LinkConfigTool().getUsage());
      print(new UpdateACLTool().getUsage());
      print("");
      print(
          "Pass --help or -h after any COMMAND to see command-specific usage information such as: ./solr zk ls --help");
    }
  }
}
