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
import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.client.solrj.impl.SolrZkClientTimeout;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.Compressor;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.ZLibCompressor;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrXmlConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Supports zk cp command in the bin/solr script. */
public class ZkCpTool extends ToolBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public ZkCpTool() {
    this(CLIO.getOutStream());
  }

  public ZkCpTool(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public List<Option> getOptions() {
    return List.of(
        Option.builder("src")
            .argName("src")
            .hasArg()
            .required(true)
            .desc("Source file or directory, may be local or a Znode.")
            .build(),
        Option.builder("dst")
            .argName("dst")
            .hasArg()
            .required(true)
            .desc("Destination of copy, may be local or a Znode.")
            .build(),
        Option.builder()
            .longOpt("solr-home")
            .argName("DIR")
            .hasArg()
            .required(false)
            .desc("Required to look up configuration for compressing state.json.")
            .build(),
        SolrCLI.OPTION_RECURSE,
        SolrCLI.OPTION_ZKHOST,
        SolrCLI.OPTION_VERBOSE);
  }

  @Override
  public String getName() {
    return "cp";
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    SolrCLI.raiseLogLevelUnlessVerbose(cli);
    String zkHost = SolrCLI.getZkHost(cli);
    if (zkHost == null) {
      throw new IllegalStateException(
          "Solr at "
              + cli.getOptionValue("solrUrl")
              + " is running in standalone server mode, cp can only be used when running in SolrCloud mode.\n");
    }

    echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...", cli);
    String src = cli.getOptionValue("src");
    String dst = cli.getOptionValue("dst");
    Boolean recurse = Boolean.parseBoolean(cli.getOptionValue("recurse"));
    echo("Copying from '" + src + "' to '" + dst + "'. ZooKeeper at " + zkHost);

    boolean srcIsZk = src.toLowerCase(Locale.ROOT).startsWith("zk:");
    boolean dstIsZk = dst.toLowerCase(Locale.ROOT).startsWith("zk:");

    String srcName = src;
    if (srcIsZk) {
      srcName = src.substring(3);
    } else if (srcName.toLowerCase(Locale.ROOT).startsWith("file:")) {
      srcName = srcName.substring(5);
    }

    String dstName = dst;
    if (dstIsZk) {
      dstName = dst.substring(3);
      if (!dstName.startsWith("/")) {
        dstName = "/" + dstName;
      }
    } else {
      if (dstName.toLowerCase(Locale.ROOT).startsWith("file:")) {
        dstName = dstName.substring(5);
      }
    }

    int minStateByteLenForCompression = -1;
    Compressor compressor = new ZLibCompressor();

    if (dstIsZk) {
      String solrHome = cli.getOptionValue("solr-home");
      if (StrUtils.isNullOrEmpty(solrHome)) {
        solrHome = System.getProperty("solr.home");
      }

      if (solrHome != null) {
        echoIfVerbose("Using SolrHome: " + solrHome, cli);
        try {
          // Be aware that if you start Solr and pass in some variables via -D like
          // solr start -DminStateByteLenForCompression=0 -c, this logic will not
          // know about the -DminStateByteLenForCompression and only return the
          // version set in the solr.xml.  So you must edit solr.xml directly.
          Path solrHomePath = Paths.get(solrHome);
          Properties props = new Properties();
          props.put(SolrXmlConfig.ZK_HOST, zkHost);
          NodeConfig nodeConfig = NodeConfig.loadNodeConfig(solrHomePath, props);
          minStateByteLenForCompression =
              nodeConfig.getCloudConfig().getMinStateByteLenForCompression();
          String stateCompressorClass = nodeConfig.getCloudConfig().getStateCompressorClass();
          if (StrUtils.isNotNullOrEmpty(stateCompressorClass)) {
            Class<? extends Compressor> compressionClass =
                Class.forName(stateCompressorClass).asSubclass(Compressor.class);
            compressor = compressionClass.getDeclaredConstructor().newInstance();
          }
        } catch (SolrException e) {
          // Failed to load solr.xml
          throw new IllegalStateException(
              "Failed to load solr.xml from ZK or SolrHome, put/get operations on compressed data will use data as is. If your intention is to read and de-compress data or compress and write data, then solr.xml must be accessible.");
        } catch (ClassNotFoundException
            | NoSuchMethodException
            | InstantiationException
            | IllegalAccessException
            | InvocationTargetException e) {
          throw new IllegalStateException(
              "Unable to find or instantiate compression class: " + e.getMessage());
        }
      }
    }
    if (minStateByteLenForCompression > -1) {
      echoIfVerbose("Compression of state.json has been enabled", cli);
    }

    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkHost)
            .withTimeout(SolrZkClientTimeout.DEFAULT_ZK_CLIENT_TIMEOUT, TimeUnit.MILLISECONDS)
            .withStateFileCompression(minStateByteLenForCompression, compressor)
            .build()) {

      zkClient.zkTransfer(srcName, srcIsZk, dstName, dstIsZk, recurse);

    } catch (Exception e) {
      log.error("Could not complete the zk operation for reason: ", e);
      throw (e);
    }
  }
}
