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
    try {
      CommandLine cmd = parse();
      assertEquals(7777, parsedValue(cmd, "--port"));
      assertEquals("zk.example:2181", parsedValue(cmd, "--zk-host"));
    } finally {
      System.clearProperty("solr.port.listen");
      System.clearProperty("zkHost");
    }
    assertEquals(8983, parsedValue(parse(), "--port"));
  }

  @Test
  public void testScriptInputsIsTheOptionTheScriptPasses() {
    assertEquals("1,2", parsedValue(parse("--script-inputs", "1,2"), "--script-inputs"));
  }
}
