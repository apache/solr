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

/** Supports package undeploy command in the bin/solr script. */
@SuppressWarnings("UnnecessarilyFullyQualified")
@picocli.CommandLine.Command(
    name = "undeploy",
    description = "Undeploy a package from specified collection(s) or at cluster level.",
    exitCodeListHeading = "%nExit Codes:%n",
    exitCodeList = {
      "0: Operation completed successfully.",
      "1: Operation failed; check output for details."
    },
    footerHeading = "%nExamples:%n",
    footer = {
      "  # Undeploy a package from a collection",
      "  bin/solr package undeploy mypkg --collections myCollection"
    })
public class Undeploy extends PackageSubCommand {

  @picocli.CommandLine.Parameters(
      index = "0",
      arity = "1",
      paramLabel = "PACKAGE",
      description = "Package name")
  private String packageName;

  @picocli.CommandLine.Option(
      names = {"--cluster"},
      description = "Specifies that this action should affect cluster-level plugins only.")
  private boolean cluster;

  @picocli.CommandLine.Option(
      names = {"--collections"},
      paramLabel = "COLLECTIONS",
      description =
          "Collections on which this package needs to be undeployed from, excluding cluster level plugins")
  private String collections;

  @Override
  public int callTool() throws Exception {
    if (!cluster && collections == null) {
      printRed(
          "Either specify --cluster to undeploy cluster level plugins or --collections <list-of-collections> to undeploy collection level plugins");
      return 1;
    }
    return runWithManagers(
        (packageManager, repositoryManager) ->
            packageTool.undeploy(packageManager, packageName, cluster, collections));
  }

  @Override
  public String getName() {
    return "undeploy";
  }
}
