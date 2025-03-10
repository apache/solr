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

import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import org.apache.commons.cli.CommandLine;

public class CLITestHelper {

  /**
   * Run a tool with all parameters, as specified in the command line. Fist parameter must be the
   * tool name.
   *
   * @param args Complete command line.
   * @param clazz Expected class name of tool implementation.
   */
  public static int runTool(String[] args, Class<?> clazz) throws Exception {
    ToolRuntime runtime = new ValidatingRuntime();
    return runTool(args, runtime, clazz);
  }

  /**
   * Run a tool with all parameters, as specified in the command line. Fist parameter must be the
   * tool name.
   *
   * @param args Complete command line.
   * @param runtime The runtime implementation specified to the tool. In test, should be usually a
   *     test specific implementation for additional checks.
   * @param clazz Expected class name of tool implementation.
   */
  public static int runTool(String[] args, ToolRuntime runtime, Class<?> clazz) throws Exception {
    Tool tool = findTool(args, runtime);

    // Check the tool actual class
    assertEquals(clazz, tool.getClass());

    CommandLine cli = parseCmdLine(tool, args);
    return tool.runTool(cli);
  }

  public static class BufferingRuntime extends ValidatingRuntime {

    private final StringWriter writer;
    private final PrintWriter printer;

    public BufferingRuntime() {
      writer = new StringWriter();
      printer = new PrintWriter(writer);
    }

    @Override
    public void print(String message) {
      printer.print(message);
    }

    @Override
    public void println(String message) {
      printer.println(message);
    }

    public void clearOutput() {
      printer.flush();
      writer.getBuffer().setLength(0);
    }

    public String getOutput() {
      printer.flush();
      return writer.toString();
    }

    public Reader getReader() {
      return new StringReader(writer.toString());
    }
  }

  /**
   * Runtime for test with additional validations.
   */
  public static class ValidatingRuntime extends DefaultToolRuntime {}
}
