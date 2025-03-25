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

import static org.apache.solr.cli.SolrCLI.findTool;
import static org.apache.solr.cli.SolrCLI.parseCmdLine;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CLITestHelper {

  /**
   * Run a tool with all parameters, as specified in the command line. First parameter must be the
   * tool name.
   *
   * @param args Complete command line.
   * @param clazz Expected class name of tool implementation.
   */
  public static int runTool(String[] args, Class<?> clazz) throws Exception {
    ToolRuntime runtime = new TestingRuntime(false);
    return runTool(args, runtime, clazz);
  }

  /**
   * Run a tool with all parameters, as specified in the command line. First parameter must be the
   * tool name.
   *
   * @param args Complete command line.
   * @param runtime The runtime implementation specified to the tool. In tests, an instance of
   *     {@link TestingRuntime} should be typically used for additional checks.
   * @param clazz Expected class name of tool implementation.
   */
  public static int runTool(String[] args, ToolRuntime runtime, Class<?> clazz) throws Exception {
    Tool tool = findTool(args, runtime);

    // Check the tool actual class
    assertEquals(clazz, tool.getClass());

    CommandLine cli = parseCmdLine(tool, args);
    return tool.runTool(cli);
  }

  /**
   * Runtime for test with additional validations. Mostly, we ensure tests never call {@link
   * System#exit(int)}.
   */
  public static class TestingRuntime extends ToolRuntime {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final StringWriter writer;
    private final PrintWriter printer;

    public TestingRuntime(boolean captureOutput) {
      if (captureOutput) {
        writer = new StringWriter();
        printer = new PrintWriter(writer);
      } else {
        writer = null;
        printer = null;
      }
    }

    @Override
    public void print(String message) {
      // This logs with a full line, while prod code prints to System.out with no new line char
      log.info(message);

      if (printer != null) {
        printer.print(message);
      }
    }

    @Override
    public void println(String message) {
      log.info(message);

      if (printer != null) {
        printer.println(message);
      }
    }

    public void clearOutput() {
      if (printer == null) {
        fail("TestingRuntime was created without capturing output");
      }

      printer.flush();
      writer.getBuffer().setLength(0);
    }

    public String getOutput() {
      if (printer == null) {
        fail("TestingRuntime was created without capturing output");
      }

      printer.flush();
      return writer.toString();
    }

    public Reader getReader() {
      if (printer == null) {
        fail("TestingRuntime was created without capturing output");
      }

      return new StringReader(writer.toString());
    }

    /**
     * Do not allow to exit the JMV in unit tests!
     *
     * @param status JVM return code.
     */
    @Override
    public void exit(int status) {
      throw new RuntimeException("exit() not allowed in tests");
    }
  }
}
