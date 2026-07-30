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

import org.apache.solr.common.util.SuppressForbidden;

/**
 * An implementation of this class is specified when executing {@link ToolBase} to access
 * environment specific methods (mostly to differentiate test from non-test executions for now).
 *
 * @see ToolBase
 */
public abstract class ToolRuntime {

  public abstract void print(String message);

  public abstract void println(String message);

  /** Print an error message, highlighted in red on terminals. */
  public void printError(String message) {
    println(CLIUtils.RED + message + CLIUtils.RESET);
  }

  /** Print a success message, highlighted in green on terminals. */
  public void printSuccess(String message) {
    println(CLIUtils.GREEN + message + CLIUtils.RESET);
  }

  /** Invokes {@link System#exit(int)} to force the JVM to immediately quit. */
  @SuppressForbidden(reason = "That's the only method in CLI code where we allow to exit the JVM")
  public void exit(int status) {
    try {
      System.exit(status);
    } catch (SecurityException secExc) {
      if (status != 0) throw new RuntimeException("SolrCLI failed to exit with status " + status);
    }
  }
}
