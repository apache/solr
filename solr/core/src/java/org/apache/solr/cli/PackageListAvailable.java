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

/** Supports package list-available command in the bin/solr script. */
@SuppressWarnings("UnnecessarilyFullyQualified")
@picocli.CommandLine.Command(
    name = "list-available",
    description = "Print a list of packages available in the repositories.",
    exitCodeListHeading = "%nExit Codes:%n",
    exitCodeList = {
      "0: Operation completed successfully.",
      "1: Operation failed; check output for details."
    },
    footerHeading = "%nExamples:%n",
    footer = {
      "  # List packages available from configured repositories",
      "  bin/solr package list-available",
    })
public class PackageListAvailable extends PackageSubCommand {

  @Override
  public int callTool() throws Exception {
    return runWithManagers(
        (packageManager, repositoryManager) -> packageTool.listAvailable(repositoryManager));
  }

  @Override
  public String getName() {
    return "list-available";
  }
}
