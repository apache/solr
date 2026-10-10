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

import java.lang.reflect.Field;
import org.apache.solr.common.util.EnvUtils;
import picocli.CommandLine;

/**
 * Fills picocli options from the environment (solr.in.sh variables, system properties) when they
 * are not given on the command line, keyed by the option's long name.
 */
public class CliDefaultValueProvider implements CommandLine.IDefaultValueProvider {

  @Override
  public String defaultValue(CommandLine.Model.ArgSpec argSpec) throws Exception {
    if (!(argSpec instanceof CommandLine.Model.OptionSpec option)) {
      return null;
    }
    return switch (option.longestName()) {
      case "--zk-host" -> EnvUtils.getProperty("zkHost");
      case "--solr-connection" -> EnvUtils.getProperty("solr.connection");
      // Only the shared option means a base URL; a tool's own --solr-url (ApiTool's full endpoint
      // URL) must not be filled from SOLR_URL
      case "--solr-url" ->
          declaredIn(option, ConnectionOptions.class) ? EnvUtils.getProperty("solr.url") : null;
      // Must match CLIUtils.getDefaultSolrUrl(), which reads solr.port.listen
      case "--port" -> EnvUtils.getProperty("solr.port.listen", "8983");
      case "--max-wait-secs" -> EnvUtils.getProperty("solr.max.wait.seconds", "0");
      default -> null;
    };
  }

  private static boolean declaredIn(CommandLine.Model.OptionSpec option, Class<?> holder) {
    return option.userObject() instanceof Field field && field.getDeclaringClass() == holder;
  }
}
