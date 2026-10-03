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

/** Supports package add-repo command in the bin/solr script. */
@SuppressWarnings("UnnecessarilyFullyQualified")
@picocli.CommandLine.Command(
    name = "add-repo",
    description = "Add a package repository to Solr.",
    exitCodeListHeading = "%nExit Codes:%n",
    exitCodeList = {
      "0: Operation completed successfully.",
      "1: Operation failed; check output for details."
    },
    footerHeading = "%nExamples:%n",
    footer = {
      "  # Add a package repository",
      "  bin/solr package add-repo myrepo https://my.repo.example/repo",
    })
public class AddRepo extends PackageSubCommand {

  @picocli.CommandLine.Parameters(
      index = "0",
      arity = "1",
      paramLabel = "REPOSITORY-NAME",
      description = "Name of the package repository.")
  private String repoName;

  @picocli.CommandLine.Parameters(
      index = "1",
      arity = "1",
      paramLabel = "REPOSITORY-URL",
      description = "URL of the package repository.")
  private String repoUrl;

  @Override
  public int callTool() throws Exception {
    return runWithManagers(((packageManager, repositoryManager) -> packageTool.addRepo(repositoryManager, repoName, repoUrl)));
  }

  @Override
  public String getName() {
    return "add-repo";
  }
}
