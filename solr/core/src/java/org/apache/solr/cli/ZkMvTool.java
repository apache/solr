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

import static org.apache.solr.packagemanager.PackageUtils.format;

import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Locale;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Supports zk mv command in the bin/solr script. */
public class ZkMvTool extends ToolBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public ZkMvTool() {
    this(CLIO.getOutStream());
  }

  public ZkMvTool(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public List<Option> getOptions() {
    return List.of(
        SolrCLI.OPTION_RECURSE,
        SolrCLI.OPTION_SOLRURL,
        SolrCLI.OPTION_SOLRURL_DEPRECATED,
        SolrCLI.OPTION_ZKHOST,
        SolrCLI.OPTION_ZKHOST_DEPRECATED,
        SolrCLI.OPTION_CREDENTIALS,
        SolrCLI.OPTION_VERBOSE);
  }

  @Override
  public String getName() {
    return "mv";
  }

  @Override
  public String getUsage() {
    return "bin/solr zk mv [-r <recurse>] [-s <HOST>] [-u <credentials>] [-v] [-z <HOST>] source destination";
  }

  @Override
  public String getHeader() {
    StringBuilder sb = new StringBuilder();
    format(sb, "mv moves (renames) znodes on Zookeeper.");
    format(sb, "");
    format(sb, "<src>, <dest> : Zookeeper nodes, the 'zk:' prefix is optional.");
    format(sb, "If <dest> ends with '/', then <dest> will be a parent znode");
    format(sb, "and the last element of the <src> path will be appended.");
    format(sb, "Zookeeper nodes CAN have data, so moving a single file to a parent znode");
    format(sb, "will overlay the data on the parent Znode so specifying the trailing slash");
    format(sb, "is important.");
    format(sb, "\nList of options:");
    return sb.toString();
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    SolrCLI.raiseLogLevelUnlessVerbose(cli);
    String zkHost = SolrCLI.getZkHost(cli);

    try (SolrZkClient zkClient = SolrCLI.getSolrZkClient(cli, zkHost)) {
      echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...", cli);
      String src = cli.getArgs()[0];
      String dst = cli.getArgs()[1];

      if (src.toLowerCase(Locale.ROOT).startsWith("file:")
          || dst.toLowerCase(Locale.ROOT).startsWith("file:")) {
        throw new SolrServerException(
            "mv command operates on znodes and 'file:' has been specified.");
      }
      String source = src;
      if (src.toLowerCase(Locale.ROOT).startsWith("zk")) {
        source = src.substring(3);
      }

      String dest = dst;
      if (dst.toLowerCase(Locale.ROOT).startsWith("zk")) {
        dest = dst.substring(3);
      }

      echo("Moving Znode " + source + " to " + dest + " on ZooKeeper at " + zkHost);
      zkClient.moveZnode(source, dest);
    } catch (Exception e) {
      log.error("Could not complete mv operation for reason: ", e);
      throw (e);
    }
  }
}
