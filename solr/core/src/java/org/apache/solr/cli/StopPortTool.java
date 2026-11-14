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
import org.apache.commons.cli.Options;
import picocli.CommandLine;

/**
 * Prints the Jetty stop port of the Solr node listening on a given port, read from the {@code
 * -DSTOP.PORT} argument of the running process.
 *
 * <p>Helper for the {@code bin/solr} stop logic, which needs each node's own stop port rather than
 * one value for the whole invocation. Hidden because it is of no use outside the scripts.
 */
@CommandLine.Command(
    name = "stop-port",
    description = "Prints the Jetty stop port of the Solr node running on the given port.",
    hidden = true)
public class StopPortTool extends ToolBase {

  @CommandLine.Option(
      names = {"-p", "--port"},
      required = true,
      description = "Port the Solr HTTP listener of the node is bound to.")
  private Integer port;

  /**
   * @deprecated Only used by the commons-cli parser; the picocli path declares this as an annotated
   *     field.
   */
  @Deprecated
  private static final Option PORT_OPTION =
      Option.builder("p")
          .longOpt("port")
          .hasArg()
          .argName("PORT")
          .type(Integer.class)
          .required()
          .desc("Port the Solr HTTP listener of the node is bound to.")
          .get();

  public StopPortTool() {
    this(new DefaultToolRuntime());
  }

  public StopPortTool(ToolRuntime runtime) {
    super(runtime);
  }

  @Override
  public String getName() {
    return "stop-port";
  }

  @Override
  public Options getOptions() {
    return super.getOptions().addOption(PORT_OPTION);
  }

  @Override
  public void runImpl(org.apache.commons.cli.CommandLine cli) throws Exception {
    port = cli.getParsedOptionValue(PORT_OPTION);
    echo(String.valueOf(resolveStopPort()));
  }

  @Override
  public int callTool() {
    echo(String.valueOf(resolveStopPort()));
    return 0;
  }

  private int resolveStopPort() {
    return new SolrProcessManager()
        .processForPort(port)
        .orElseThrow(
            () -> new IllegalArgumentException("No Solr node found running on port " + port))
        .stopPort();
  }
}
