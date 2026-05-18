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

/**
 * Picocli mixin providing common configset options shared by upconfig and downconfig sub-commands.
 *
 * <p>Use {@code @CommandLine.Mixin ConfigSetOptions configSetOpts} in a command class to inherit
 * these options.
 */
public class ConfigSetOptions {

  @CommandLine.Option(
      names = {"-n", "--conf-name"},
      description = {
        "Configset name in ZooKeeper.",
        "Name of the configuration set under the \"/configs\" ZooKeeper node.",
        "",
        "You can see available configuration sets in the Admin UI via the Cloud screens. Choose Cloud → Tree → configs to see them.",
        "",
        "If a pre-existing configuration set is specified, it will be overwritten in ZooKeeper.",
        "",
        "**Example:** `-n myconfig`"
      },
      required = true)
  public String confName;

  @CommandLine.Option(
      names = {"-d", "--conf-dir"},
      description = "Local directory with configs.",
      required = true,
      paramLabel = "DIR")
  public String confDir;
}
