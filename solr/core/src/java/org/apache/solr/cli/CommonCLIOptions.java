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

import org.apache.commons.cli.Option;

public final class CommonCLIOptions {

  private CommonCLIOptions() {}

  public static final Option VERBOSE_OPTION =
      Option.builder("v").longOpt("verbose").desc("Enable verbose command output.").build();

  public static final Option HELP_OPTION =
      Option.builder("h").longOpt("help").desc("Print this message.").build();

  public static final Option ZK_HOST_OPTION =
      Option.builder("z")
          .longOpt("zk-host")
          .hasArg()
          .argName("HOST")
          .desc(
              "Zookeeper connection string; unnecessary if ZK_HOST is defined in solr.in.sh; otherwise, defaults to "
                  + DefaultValues.ZK_HOST
                  + '.')
          .build();

  public static final Option SOLR_URL_OPTION =
      Option.builder("s")
          .longOpt("solr-url")
          .hasArg()
          .argName("HOST")
          .desc(
              "Base Solr URL, which can be used to determine the zk-host if that's not known; defaults to: "
                  + CLIUtils.getDefaultSolrUrl()
                  + '.')
          .build();

  public static final Option RECURSIVE_OPTION =
      Option.builder("r").longOpt("recursive").desc("Apply the command recursively.").build();

  public static final Option CREDENTIALS_OPTION =
      Option.builder("u")
          .longOpt("credentials")
          .hasArg()
          .argName("credentials")
          .desc(
              "Credentials in the format username:password. Example: --credentials solr:SolrRocks")
          .build();

  public static final class DefaultValues {

    private DefaultValues() {}

    public static final String ZK_HOST = "localhost:9983";

    public static final String DEFAULT_CONFIG_SET = "_default";
  }
}
