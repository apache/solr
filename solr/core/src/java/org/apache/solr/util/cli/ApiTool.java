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
package org.apache.solr.util.cli;

import java.io.PrintStream;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.util.CLIO;
import org.apache.solr.util.SolrCLI;
import org.noggit.CharArr;
import org.noggit.JSONWriter;

/** Used to send an arbitrary HTTP request to a Solr API endpoint. */
public class ApiTool extends ToolBase {

  public ApiTool() {
    this(CLIO.getOutStream());
  }

  public ApiTool(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public String getName() {
    return "api";
  }

  @Override
  public Option[] getOptions() {
    return new Option[] {
      Option.builder("get")
          .argName("URL")
          .hasArg()
          .required(true)
          .desc("Send a GET request to a Solr API endpoint.")
          .build()
    };
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    String getUrl = cli.getOptionValue("get");
    if (getUrl != null) {
      Map<String, Object> json = SolrCLI.getJson(getUrl);

      // pretty-print the response to stdout
      CharArr arr = new CharArr();
      new JSONWriter(arr, 2).write(json);
      echo(arr.toString());
    }
  }
}
