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

import picocli.CommandLine;

/** Where a package (un)deploy applies: cluster-level plugins or the given collections. */
public class PackageTargetOptions {
  @CommandLine.Option(
      names = {"--cluster"},
      description = "Specifies that this action should affect cluster-level plugins only.")
  public boolean cluster;

  @CommandLine.Option(
      names = {"--collections"},
      paramLabel = "COLLECTIONS",
      description =
          "Specifies that this action should affect plugins for the given collections only, excluding cluster level plugins.")
  public String collections;

  /** The error to print when neither target was given, or null if one was. */
  String missingTargetMessage(String verb) {
    if (cluster || collections != null) {
      return null;
    }
    return "Either specify --cluster to "
        + verb
        + " cluster level plugins or --collections <list-of-collections> to "
        + verb
        + " collection level plugins";
  }
}
