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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.solr.client.api.util.SolrVersion;
import org.apache.solr.client.solrj.request.SystemInfoRequest;
import org.apache.solr.client.solrj.response.SystemInfoResponse;

/**
 * Supports version command in the bin/solr script.
 *
 * <p>Prints the client (CLI) version. When a connection target ({@code -s}/{@code
 * --solr-connection}, {@code --solr-url}, or {@code --zk-host}) is provided, also prints the
 * version of the remote Solr server.
 */
public class VersionTool extends ToolBase {

  public VersionTool(ToolRuntime runtime) {
    super(runtime);
  }

  @Override
  public String getName() {
    return "version";
  }

  @Override
  public Options getOptions() {
    return super.getOptions()
        .addOption(CommonCLIOptions.CREDENTIALS_OPTION)
        .addOptionGroup(getConnectionOptions());
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    echo("Client version: " + SolrVersion.LATEST);

    if (CLIUtils.hasConnectionOption(cli)) {
      String solrUrl = CLIUtils.normalizeSolrUrl(cli);
      String credentials = cli.getOptionValue(CommonCLIOptions.CREDENTIALS_OPTION);
      try (var solrClient = CLIUtils.getSolrClient(solrUrl, credentials)) {
        SystemInfoResponse sysResponse = new SystemInfoRequest().process(solrClient);
        echo("Server version: " + sysResponse.getSolrImplVersion());
      }
    }
  }
}
