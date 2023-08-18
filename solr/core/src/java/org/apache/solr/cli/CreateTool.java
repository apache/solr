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

import static org.apache.solr.common.params.CommonParams.SYSTEM_INFO_PATH;

import java.io.PrintStream;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.util.NamedList;

public class CreateTool extends ToolBase {

  public CreateTool() {
    this(CLIO.getOutStream());
  }

  public CreateTool(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public String getName() {
    return "create";
  }

  @Override
  public List<Option> getOptions() {
    return SolrCLI.CREATE_COLLECTION_OPTIONS;
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    SolrCLI.raiseLogLevelUnlessVerbose(cli);
    String solrUrl = cli.getOptionValue("solrUrl", SolrCLI.getDefaultSolrUrl());
    if (!solrUrl.endsWith("/")) solrUrl += "/";

    ToolBase tool;
    try (var solrClient = SolrCLI.getSolrClient(solrUrl)) {
      NamedList<Object> systemInfo =
          solrClient.request(new GenericSolrRequest(SolrRequest.METHOD.GET, SYSTEM_INFO_PATH));
      if ("solrcloud".equals(systemInfo.get("mode"))) {
        tool = new CreateCollectionTool(stdout);
      } else {
        tool = new CreateCoreTool(stdout);
      }
      tool.runImpl(cli);
    }
  }
} // end CreateTool class
