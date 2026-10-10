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

package org.apache.solr.cli.tools.pkg;

import java.nio.file.Path;

/** Supports package add-key command in the bin/solr script. */
@SuppressWarnings("UnnecessarilyFullyQualified")
@picocli.CommandLine.Command(
    name = "add-key",
    description = "Add a trusted key to Solr.",
    exitCodeListHeading = "%nExit Codes:%n",
    exitCodeList = {
      "0: Operation completed successfully.",
      "1: Operation failed; check output for details."
    },
    footerHeading = "%nExamples:%n",
    footer = {
      "  # Add a trusted public key",
      "  bin/solr package add-key /path/to/public.key.der",
    })
public class PackageAddKey extends PackageSubCommand {

  @picocli.CommandLine.Parameters(
      index = "0",
      arity = "1",
      paramLabel = "KEY-FILE",
      description = "Path to a file containing the trusted key.")
  private Path keyFile;

  @Override
  public int callTool() throws Exception {
    return runWithManagers(
        ((packageManager, repositoryManager) -> packageTool.addKey(repositoryManager, keyFile)));
  }

  @Override
  public String getName() {
    return "add-key";
  }
}
