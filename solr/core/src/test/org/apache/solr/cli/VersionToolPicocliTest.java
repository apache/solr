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
 * Runs all {@link VersionToolTest} tests through the picocli invocation path.
 *
 * <p>All {@code @Test} methods are inherited; only the invocation strategy is overridden.
 */
public class VersionToolPicocliTest extends VersionToolTest {

  @Override
  protected String runVersionTool(String[] toolArgs) throws Exception {
    // toolArgs[0] is the tool name used by commons-cli dispatch; strip it for picocli.
    String[] args = Arrays.copyOfRange(toolArgs, 1, toolArgs.length);
    CLITestHelper.TestingRuntime runtime = new CLITestHelper.TestingRuntime(true);
    VersionTool tool = new VersionTool(runtime);
    new CommandLine(tool).setDefaultValueProvider(new CliDefaultValueProvider()).execute(args);
    return runtime.getOutput();
  }
}
