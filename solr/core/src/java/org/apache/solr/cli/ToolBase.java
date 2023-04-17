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

public abstract class ToolBase implements Tool {

  protected PrintStream stdout;
  protected boolean verbose = false;

  protected ToolBase() {
    this(CLIO.getOutStream());
  }

  protected ToolBase(PrintStream stdout) {
    this.stdout = stdout;
  }

  protected void echoIfVerbose(final String msg, CommandLine cli) {
    if (cli.hasOption(SolrCLI.OPTION_VERBOSE.getOpt())) {
      echo(msg);
    }
  }

  protected void echo(final String msg) {
    stdout.println(msg);
  }

  @Override
  public int runTool(CommandLine cli) throws Exception {
    verbose = cli.hasOption(SolrCLI.OPTION_VERBOSE.getOpt());

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

  public abstract void runImpl(CommandLine cli) throws Exception;
}
