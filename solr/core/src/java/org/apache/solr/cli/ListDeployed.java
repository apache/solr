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

/** Supports package list-deployed command in the bin/solr script. */
@SuppressWarnings("UnnecessarilyFullyQualified")
@picocli.CommandLine.Command(
    name = "list-deployed",
    description =
        "Print packages deployed on a collection, or collections where a package is deployed.",
    exitCodeListHeading = "%nExit Codes:%n",
    exitCodeList = {
      "0: Operation completed successfully.",
      "1: Operation failed; check output for details."
    },
    footerHeading = "%nExamples:%n",
    footer = {
      "  # List packages deployed on a collection",
      "  bin/solr package list-deployed -c myCollection",
      "",
      "  # List collections where a package is deployed",
      "  bin/solr package list-deployed mypkg"
    })
public class ListDeployed extends PackageSubCommand {

  @picocli.CommandLine.Option(
      names = {"-c", "--collection"},
      paramLabel = "COLLECTION",
      description = "The collection to apply the package to, not required.")
  private String collection;

  @picocli.CommandLine.Parameters(
      index = "0",
      arity = "0..1",
      paramLabel = "PACKAGE",
      description = "Package name; lists collections where this package is deployed.")
  private String packageName;

  @Override
  public int callTool() throws Exception {
    if (collection == null && packageName == null) {
      printRed("Either -c/--collection <collection> or a package name is required.");
      return 1;
    }

    return runWithManagers(
        (packageManager, repositoryManager) -> {
          if (collection != null) {
            packageTool.listPackagesDeployedOnCollection(packageManager, collection);
          } else {
            packageTool.listCollectionsWithPackageDeployed(packageManager, packageName);
          }
        });
  }

  @Override
  public String getName() {
    return "list-deployed";
  }
}
