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

import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

public class PostTool extends ToolBase {

  public PostTool() {
    this(CLIO.getOutStream());
  }

  public PostTool(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public String getName() {
    return "post";
  }

  @Override
  public List<Option> getOptions() {
    return List.of(
        Option.builder("url")
            .argName("url")
            .hasArg()
            .required(true)
            .desc("<base Solr update URL>")
            .build(),
        Option.builder("commit").required(false).desc("Issue a commit at end of post").build(),
        Option.builder("optimize").required(false).desc("Issue an optimize at end of post").build(),
        Option.builder("mode")
            .argName("mode")
            .hasArg(true)
            .required(false)
            .desc("Files crawls files, web crawls website. default: files.")
            .build(),
        Option.builder("recursive")
            .argName("recursive")
            .hasArg(true)
            .required(false)
            .desc("For web crawl, how deep to go. default: 1")
            .build(),
        Option.builder("delay")
            .argName("delay")
            .hasArg(true)
            .required(false)
            .desc(
                "If recursive then delay will be the wait time between posts.  default: 10 for web, 0 for files")
            .build(),
        Option.builder("type")
            .argName("content-type")
            .hasArg(true)
            .required(false)
            .desc("default: application/json")
            .build(),
        Option.builder("filetypes")
            .argName("<type>[,<type>,...]")
            .hasArg(true)
            .required(false)
            .desc("default: " + SimplePostTool.DEFAULT_FILE_TYPES)
            .build(),
        Option.builder("params")
            .argName("<key>=<value>[&<key>=<value>...]")
            .hasArg(true)
            .required(false)
            .desc("values must be URL-encoded; these pass through to Solr update request")
            .build(),
        Option.builder("out")
            .required(false)
            .desc("sends Solr response outputs to console")
            .build(),
        Option.builder("format")
            .required(false)
            .desc(
                "sends application/json content as Solr commands to /update instead of /update/json/docs")
            .build());
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    SolrCLI.raiseLogLevelUnlessVerbose(cli);

    String url = cli.getOptionValue("url");
    URL solrUrl = new URL(url);

    String mode = SimplePostTool.DEFAULT_DATA_MODE;
    if (cli.hasOption("mode")) {
      mode = cli.getOptionValue("mode");
    }
    boolean auto = true;
    String type = null;
    if (cli.hasOption("type")) {
      type = cli.getOptionValue("type");
    }
    String format =
        cli.hasOption("format")
            ? SimplePostTool.FORMAT_SOLR
            : ""; // i.e not solr formatted json commands

    String fileTypes = SimplePostTool.DEFAULT_FILE_TYPES;
    if (cli.hasOption("filetypes")) {
      fileTypes = cli.getOptionValue("filetypes");
    }

    int defaultDelay = (mode.equals((SimplePostTool.DATA_MODE_WEB)) ? 10 : 0);
    int delay = Integer.parseInt(cli.getOptionValue("delay", String.valueOf(defaultDelay)));
    int recursive = Integer.parseInt(cli.getOptionValue("recursive", "1"));

    OutputStream out = cli.hasOption("out") ? CLIO.getOutStream() : null;
    boolean commit = cli.hasOption("commit");
    boolean optimize = cli.hasOption("optimize");

    String[] args = cli.getArgs();

    SimplePostTool spt =
        new SimplePostTool(
            mode, solrUrl, auto, type, format, recursive, delay, fileTypes, out, commit, optimize,
            args);

    spt.execute();
  }
}
