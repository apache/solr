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

import org.apache.commons.cli.CommandLine;
import org.apache.solr.packagemanager.PackageManager;
import org.apache.solr.packagemanager.RepositoryManager;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.ParentCommand;

/** Shared picocli wiring for {@code bin/solr package <subcommand> leaves} */
abstract class PackageSubCommand extends ToolBase {

  @FunctionalInterface
  interface PackageAction {
    void run(PackageManager packageManager, RepositoryManager repositoryManager) throws Exception;
  }

  @ParentCommand PackageTool packageTool;

  @Mixin CredentialsOptions credentialsOptions;

  @ArgGroup(exclusive = true, multiplicity = "0..1")
  ConnectionOptions connectionOptions;

  PackageSubCommand() {
    super(new DefaultToolRuntime());
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    throw new UnsupportedOperationException(
        getName() + " is implemented via the commons-cli path under PackageTool");
  }

  final int runWithManagers(PackageAction action) throws Exception {
    String credentials =
        credentialsOptions.credentials != null
            ? credentialsOptions.credentials
            : packageTool.credentialsOptions != null
                ? packageTool.credentialsOptions.credentials
                : null;

    packageTool.runWithManagers(
        connectionOptions != null ? connectionOptions : packageTool.connectionOptions,
        credentials,
        action);
    return 0;
  }
}
