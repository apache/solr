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

import org.apache.solr.common.util.EnvUtils;
import picocli.CommandLine;

/** Provides default values for CLI arguments. */
public class CliDefaultValueProvider implements CommandLine.IDefaultValueProvider {
  @Override
  public String defaultValue(CommandLine.Model.ArgSpec argSpec) throws Exception {
    return switch (argSpec.paramLabel()) {
      case "<zkHost>" -> EnvUtils.getProperty("zkHost");
      case "<solrUrl>" -> EnvUtils.getProperty("solr.url");
      case "<port>" -> EnvUtils.getProperty("solr.port", "8983");
      case "<maxWaitSecs>" -> EnvUtils.getProperty("solr.max.wait.seconds", "0");
      default -> null;
    };
  }
}
