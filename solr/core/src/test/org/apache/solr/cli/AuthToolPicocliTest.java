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
import picocli.CommandLine;

/**
 * Runs all {@link AuthToolTest} tests through the picocli invocation path.
 *
 * <p>All {@code @Test} methods are inherited from {@link AuthToolTest}. Only the tool invocation
 * strategy is overridden here to use {@code picocli.CommandLine.execute()} instead of the
 * commons-cli path.
 */
public class AuthToolPicocliTest extends AuthToolTest {

  @Override
  protected int runTool(String[] args, Class<? extends ToolBase> clazz) throws Exception {
    // args[0] is the tool name used by commons-cli dispatch; strip it for picocli.
    String[] toolArgs = Arrays.copyOfRange(args, 1, args.length);
    // Use a TestingRuntime so runtime.exit() cannot terminate the test JVM
    ToolRuntime runtime = new CLITestHelper.TestingRuntime(false);
    ToolBase tool = clazz.getDeclaredConstructor(ToolRuntime.class).newInstance(runtime);
    return new CommandLine(tool)
        .setDefaultValueProvider(new CliDefaultValueProvider())
        .execute(toolArgs);
  }
}
