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

import java.util.Locale;
import org.apache.commons.cli.DeprecatedAttributes;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.solr.common.util.EnvUtils;

public final class CommonCLIOptions {

  private CommonCLIOptions() {}

  public static final Option VERBOSE_OPTION = Option.builder("v")
      .longOpt("verbose")
      .required(false)
      .desc("Enable verbose command output.")
      .build();

  public static final Option HELP_OPTION = Option.builder("h")
      .longOpt("help")
      .required(false)
      .desc("Print this message.")
      .build();

  /**
   * @deprecated Use {@link CommonCLIOptions#ZK_HOST_OPTION} instead.
   */
  @Deprecated(since = "9.7")
  private static final Option ZK_HOST_OPTION_DEP =
      Option.builder("zkHost")
          .longOpt("zkHost")
          .deprecated(
              DeprecatedAttributes.builder()
                  .setForRemoval(true)
                  .setSince("9.7")
                  .setDescription("Use --zk-host instead")
                  .get())
          .argName("HOST")
          .hasArg()
          .required(false)
          .desc(
              "Zookeeper connection string; unnecessary if ZK_HOST is defined in solr.in.sh; otherwise, defaults to "
                  + DefaultValues.ZK_HOST
                  + '.')
          .build();

  public static final Option ZK_HOST_OPTION = Option.builder("z")
      .longOpt("zk-host")
      .argName("HOST")
      .hasArg()
      .required(false)
      .desc(
          "Zookeeper connection string; unnecessary if ZK_HOST is defined in solr.in.sh; otherwise, defaults to "
              + DefaultValues.ZK_HOST
              + '.')
      .build();

  /**
   * Zookeeper host option group that supports the deprecated and the new option of zk-host.
   *
   * @deprecated Use {@link CommonCLIOptions#ZK_HOST_OPTION} instead.
   */
  @Deprecated(since = "9.7")
  public static final OptionGroup ZK_HOST_OPTION_GROUP = new OptionGroup()
      .addOption(ZK_HOST_OPTION_DEP)
      .addOption(ZK_HOST_OPTION);

  /**
   * @deprecated Use {@link CommonCLIOptions#SOLR_URL_OPTION} instead.
   */
  @Deprecated(since = "9.7", forRemoval = true)
  private static final Option SOLR_URL_OPTION_DEP =
      Option.builder("solrUrl")
          .longOpt("solrUrl")
          .deprecated(
              DeprecatedAttributes.builder()
                  .setForRemoval(true)
                  .setSince("9.7")
                  .setDescription("Use --solr-url instead")
                  .get())
          .argName("HOST")
          .hasArg()
          .required(false)
          .desc(
              "Base Solr URL, which can be used to determine the zk-host if that's not known; defaults to: "
                  + DefaultValues.getDefaultSolrUrl()
                  + '.')
          .build();

  /**
   * @deprecated Use {@link CommonCLIOptions#SOLR_URL_OPTION} instead.
   */
  @Deprecated(since = "9.7", forRemoval = true)
  private static final Option SOLR_URL_OPTION_DEP2 =
      Option.builder("url")
          .longOpt("solr-url")
          .argName("HOST")
          .hasArg()
          .required(false)
          .desc(
              "Base Solr URL, which can be used to determine the zk-host if that's not known; defaults to: "
                  + DefaultValues.getDefaultSolrUrl()
                  + '.')
          .build();

  public static final Option SOLR_URL_OPTION =
      Option.builder("s")
          .longOpt("solr-url")
          .argName("HOST")
          .hasArg()
          .required(false)
          .desc(
              "Base Solr URL, which can be used to determine the zk-host if that's not known; defaults to: "
                  + DefaultValues.getDefaultSolrUrl()
                  + '.')
          .build();

  /**
   * Solr URL option group that supports deprecated and none options for providing the Solr URL.
   *
   * @deprecated Use {@link CommonCLIOptions#SOLR_URL_OPTION} instead.
   */
  @Deprecated(since = "9.8")
  public static final OptionGroup SOLR_URL_OPTION_GROUP = new OptionGroup()
      .addOption(SOLR_URL_OPTION_DEP)
      .addOption(SOLR_URL_OPTION_DEP2);

  public static final Option RECURSE_OPTION =
      Option.builder("r")
          .longOpt("recurse")
          .required(false)
          .desc("Apply the command recursively.")
          .build();

  public static final Option CREDENTIALS_OPTION = Option.builder("u")
      .longOpt("credentials")
      .argName("credentials")
      .hasArg()
      .required(false)
      .desc("Credentials in the format username:password. Example: --credentials solr:SolrRocks")
      .build();

  public static final class DefaultValues {

    private DefaultValues() {}

    public static final String ZK_HOST = "localhost:9983";

    public static String getDefaultSolrUrl() {
      // note that ENV_VAR syntax (and the env vars too) are mapped to env.var sys props
      String scheme = EnvUtils.getProperty("solr.url.scheme", "http");
      String host = EnvUtils.getProperty("solr.tool.host", "localhost");
      String port = EnvUtils.getProperty("jetty.port", "8983"); // from SOLR_PORT env
      return String.format(Locale.ROOT, "%s://%s:%s", scheme.toLowerCase(Locale.ROOT), host, port);
    }
  }
}
