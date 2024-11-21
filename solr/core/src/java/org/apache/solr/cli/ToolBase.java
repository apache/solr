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

import java.io.PrintStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.solr.util.StartupLoggingUtils;

public abstract class ToolBase implements Tool {

  private boolean verbose = false;
  protected PrintStream stdout;

  protected ToolBase() {
    this(CLIO.getOutStream());
  }

  protected ToolBase(PrintStream stdout) {
    this.stdout = stdout;
  }

  /** Is this tool being run in a verbose mode? */
  protected boolean isVerbose() {
    return verbose;
  }

  protected void echoIfVerbose(final String msg) {
    if (verbose) {
      echo(msg);
    }
  }

  protected void echo(final String msg) {
    stdout.println(msg);
  }

  @Override
  public Options getOptions() {
    return new Options()
        .addOption(CommonCLIOptions.HELP_OPTION)
        .addOption(CommonCLIOptions.VERBOSE_OPTION);
  }

  /**
   * Provides the two ways of connecting to Solr for CLI Tools
   *
   * @return OptionGroup validates that only one option is supplied by the caller.
   */
  public OptionGroup getConnectionOptions() {
    OptionGroup optionGroup = new OptionGroup();
    optionGroup.addOption(CommonCLIOptions.SOLR_URL_OPTION);
    optionGroup.addOption(CommonCLIOptions.ZK_HOST_OPTION);
    return optionGroup;
  }

  @Override
  public int runTool(CommandLine cli) throws Exception {
    verbose = cli.hasOption(CommonCLIOptions.VERBOSE_OPTION);
    raiseLogLevelUnlessVerbose();

    int toolExitStatus = 0;
    try {
      runImpl(cli);
    } catch (Exception exc) {
      // since this is a CLI, spare the user the stacktrace
      String excMsg = exc.getMessage();
      if (excMsg != null) {
        CLIO.err("\nERROR: " + excMsg + "\n");
        if (verbose) {
          exc.printStackTrace(CLIO.getErrStream());
        }
        toolExitStatus = 1;
      } else {
        throw exc;
      }
    }
    return toolExitStatus;
  }

  private void raiseLogLevelUnlessVerbose() {
    if (!verbose) {
      StartupLoggingUtils.changeLogLevel("WARN");
    }
  }

  public abstract void runImpl(CommandLine cli) throws Exception;
}
