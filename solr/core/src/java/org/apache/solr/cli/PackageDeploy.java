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

import static org.apache.solr.cli.SolrCLI.printRed;

/** Supports package deploy command in the bin/solr script. */
@SuppressWarnings("UnnecessarilyFullyQualified")
@picocli.CommandLine.Command(
    name = "deploy",
    description = "PackageDeploy an installed package to collections or at cluster level.",
    exitCodeListHeading = "%nExit Codes:%n",
    exitCodeList = {
      "0: Operation completed successfully.",
      "1: Operation failed; check output for details."
    },
    footerHeading = "%nExamples:%n",
    footer = {
      "  # PackageDeploy a package to a collection",
      "  bin/solr package deploy mypkg:1.0.0 --collections myCollection -y",
      "",
      "  # Update an existing deployment",
      "  bin/solr package deploy mypkg --update --collections myCollection -y"
    })
public class PackageDeploy extends PackageSubCommand {

  @picocli.CommandLine.Parameters(
      index = "0",
      arity = "1",
      paramLabel = "PACKAGE[:VERSION]",
      description = "Package name, optionally with :version.")
  private String packageNameAndVersion;

  @picocli.CommandLine.Option(
      names = {"--cluster"},
      description = "Specifies that this action should affect cluster-level plugins only.")
  private boolean cluster;

  @picocli.CommandLine.Option(
      names = {"--collections"},
      paramLabel = "COLLECTIONS",
      description =
          "Specifies that this action should affect plugins for the given collections only, excluding cluster level plugins.")
  private String collections;

  @picocli.CommandLine.Option(
      names = {"-p", "--param"},
      paramLabel = "PARAMS",
      description = "List of parameters to be used with deploy command.")
  private String[] params;

  @picocli.CommandLine.Option(
      names = {"--update"},
      description = "If a deployment is an update over a previous deployment.")
  private boolean update;

  @picocli.CommandLine.Option(
      names = {"-y", "--no-prompt"},
      description = "Don't prompt for input; accept all default choices, defaults to false.")
  private boolean noPrompt;

  @Override
  public int callTool() throws Exception {
    if (!cluster && collections == null) {
      printRed(
          "Either specify --cluster to deploy cluster level plugins or --collections <list-of-collections> to deploy collection level plugins");
      return 1;
    }
    return runWithManagers(
        (packageManager, repositoryManager) ->
            packageTool.deploy(
                packageManager,
                packageNameAndVersion,
                cluster,
                collections,
                params,
                update,
                noPrompt));
  }

  @Override
  public String getName() {
    return "deploy";
  }
}
