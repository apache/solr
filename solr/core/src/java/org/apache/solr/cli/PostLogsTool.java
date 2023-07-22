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

import java.io.PrintStream;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

public class PostLogsTool extends ToolBase {
  /** Get the status of a Solr server. */
  public PostLogsTool() {
    this(CLIO.getOutStream());
  }

  public PostLogsTool(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public String getName() {
    return "postlogs";
  }

  @Override
  public List<Option> getOptions() {
    return List.of(
        Option.builder("url")
            .longOpt("url")
            .argName("ADDRESS")
            .hasArg()
            .required(true)
            .desc("Address of the collection, example http://localhost:8983/solr/collection1/.")
            .build(),
        Option.builder("rootdir")
            .longOpt("rootdir")
            .argName("DIRECTORY")
            .hasArg()
            .required(true)
            .desc("All files found at or below the root directory will be indexed.")
            .build());
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    String url = cli.getOptionValue("url");
    String rootDir = cli.getOptionValue("rootdir");
    SolrLogPostTool solrLogPostTool = new SolrLogPostTool();
    solrLogPostTool.runCommand(url, rootDir);
  }
}
