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

import java.util.Arrays;

/**
 * Runs all {@link ZkSubcommandsTest} tests through the picocli invocation path.
 *
 * <p>All {@code @Test} methods are inherited from {@link ZkSubcommandsTest}. Only the tool
 * invocation strategy is overridden here to use {@code picocli.CommandLine.execute()} instead of
 * the commons-cli path.
 */
public class ZkSubcommandsPicocliTest extends ZkSubcommandsTest {

  @Override
  protected int runTool(String[] args, Class<? extends ToolBase> clazz) throws Exception {
    // args[0] is the tool/subcommand name used by commons-cli dispatch; strip it for picocli.
    String[] toolArgs = Arrays.copyOfRange(args, 1, args.length);
    ToolBase tool = clazz.getDeclaredConstructor().newInstance();
    return new picocli.CommandLine(tool).execute(toolArgs);
  }

  @Override
  protected int runTool(
      String[] args, CLITestHelper.TestingRuntime runtime, Class<? extends ToolBase> clazz)
      throws Exception {
    String[] toolArgs = Arrays.copyOfRange(args, 1, args.length);
    ToolBase tool = clazz.getDeclaredConstructor(ToolRuntime.class).newInstance(runtime);
    return new picocli.CommandLine(tool).execute(toolArgs);
  }
}
