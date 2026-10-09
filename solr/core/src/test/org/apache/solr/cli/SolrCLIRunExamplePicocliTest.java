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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import org.junit.Test;
import picocli.CommandLine;

/** Runs all {@link TestSolrCLIRunExample} tests through the picocli invocation path. */
public class SolrCLIRunExamplePicocliTest extends TestSolrCLIRunExample {

  @Override
  protected int runTool(RunExampleTool tool, String[] args) throws Exception {
    return new CommandLine(tool)
        .setDefaultValueProvider(new CliDefaultValueProvider())
        .execute(args);
  }

  private static Object parsedValue(CommandLine cmd, String option) {
    return cmd.getCommandSpec().findOption(option).getValue();
  }

  private CommandLine parse(String... extra) {
    CommandLine cmd =
        new CommandLine(new RunExampleTool(new CLITestHelper.TestingRuntime(false)))
            .setDefaultValueProvider(new CliDefaultValueProvider());
    String[] base = {"-e", "techproducts", "--server-dir", "/tmp/server"};
    String[] all = new String[base.length + extra.length];
    System.arraycopy(base, 0, all, 0, base.length);
    System.arraycopy(extra, 0, all, base.length, extra.length);
    cmd.parseArgs(all);
    return cmd;
  }

  @Test
  public void testPortAndZkHostDefaultsComeFromTheProperties() {
    System.setProperty("solr.port.listen", "7777");
    System.setProperty("zkHost", "zk.example:2181");
    CommandLine cmd = parse();
    assertEquals(7777, parsedValue(cmd, "--port"));
    assertEquals("zk.example:2181", parsedValue(cmd, "--zk-host"));
  }

  @Test
  public void testPortDefaultsTo8983WithoutTheProperty() {
    assertEquals(8983, parsedValue(parse(), "--port"));
  }

  @Test
  public void testScriptInputsIsTheOptionTheScriptPasses() {
    assertEquals("1,2", parsedValue(parse("--script-inputs", "1,2"), "--script-inputs"));
  }

  private static RunExampleTool tool(CommandLine cmd) {
    return (RunExampleTool) cmd.getCommand();
  }

  @Test
  public void testDashDArgumentsAreKeptAsExtraArguments() {
    // what bin/solr start -e techproducts -Dcustom.prop=1 forwards to run_example
    CommandLine cmd = parse("some-arg", "-Dcustom.prop=1", "-Dother=2");
    assertArrayEquals(
        new String[] {"some-arg", "-Dcustom.prop=1", "-Dother=2"}, tool(cmd).picocliExtraArgs());
  }

  @Test
  public void testDashDValueOfJvmOptsIsNotAnExtraArgument() {
    CommandLine cmd = parse("--jvm-opts", "-Dcustom.prop=helloworld");
    assertArrayEquals(new String[0], tool(cmd).picocliExtraArgs());
  }

  @Test
  public void testOtherUnknownOptionsAreStillAUsageError() {
    CommandLine cmd = parse("--no-such-option", "-Dcustom.prop=1");
    assertEquals(List.of("--no-such-option"), tool(cmd).unknownOptions());

    // nothing runs: the message and usage go to stderr and the exit code is picocli's usage error
    StringWriter err = new StringWriter();
    CommandLine run =
        new CommandLine(new RunExampleTool(new CLITestHelper.TestingRuntime(false)))
            .setDefaultValueProvider(new CliDefaultValueProvider())
            .setErr(new PrintWriter(err));
    assertEquals(
        2, run.execute("-e", "techproducts", "--server-dir", "/tmp/server", "--no-such-option"));
    assertTrue(err.toString(), err.toString().contains("Unknown option: '--no-such-option'"));
    assertTrue(err.toString(), err.toString().contains("Usage"));
  }
}
