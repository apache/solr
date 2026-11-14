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
package org.apache.solr.cli.tools.cluster;

import java.util.Arrays;
import org.apache.solr.cli.CLITestHelper;
import org.apache.solr.cli.CliDefaultValueProvider;
import org.apache.solr.cli.ToolBase;
import org.apache.solr.cli.ToolRuntime;
import org.junit.Test;
import picocli.CommandLine;

/**
 * Runs all {@link HealthcheckToolTest} tests through the picocli invocation path.
 *
 * <p>All {@code @Test} methods are inherited; only the invocation strategy is overridden.
 */
public class HealthcheckToolPicocliTest extends HealthcheckToolTest {

  @Override
  protected int runTool(String[] args, Class<? extends ToolBase> clazz) throws Exception {
    // args[0] is the tool name used by commons-cli dispatch; strip it for picocli.
    String[] toolArgs = Arrays.copyOfRange(args, 1, args.length);
    ToolRuntime runtime = new CLITestHelper.TestingRuntime(false);
    ToolBase tool = clazz.getDeclaredConstructor(ToolRuntime.class).newInstance(runtime);
    return new CommandLine(tool)
        .setDefaultValueProvider(new CliDefaultValueProvider())
        .execute(toolArgs);
  }

  @Test
  public void testHealthcheckWithSolrConnectionProperty() throws Exception {
    // SOLR_CONNECTION reaches the JVM as the solr.connection property
    System.setProperty("solr.connection", getHttpSolrConnection().toString());
    String[] args = new String[] {"healthcheck", "-c", "bob"};
    assertEquals(0, runTool(args, HealthcheckTool.class));
  }
}
