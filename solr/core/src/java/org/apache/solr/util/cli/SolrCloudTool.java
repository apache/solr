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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.client.solrj.impl.CloudLegacySolrClient;
import org.apache.solr.util.SolrCLI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Optional;

/**
 * Helps build SolrCloud aware tools by initializing a CloudSolrClient instance before running the
 * tool.
 */
public abstract class SolrCloudTool extends ToolBase {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    protected SolrCloudTool(PrintStream stdout) {
        super(stdout);
    }

    @Override
    public Option[] getOptions() {
        return SolrCLI.cloudOptions;
    }

    @Override
    public void runImpl(CommandLine cli) throws Exception {
        SolrCLI.raiseLogLevelUnlessVerbose(cli);
        String zkHost = cli.getOptionValue(SolrCLI.OPTION_ZKHOST.getOpt(), SolrCLI.ZK_HOST);

        log.debug("Connecting to Solr cluster: {}", zkHost);
        try (var cloudSolrClient =
                     new CloudLegacySolrClient.Builder(Collections.singletonList(zkHost), Optional.empty())
                             .build()) {

            cloudSolrClient.connect();
            runCloudTool(cloudSolrClient, cli);
        }
    }

    /**
     * Runs a SolrCloud tool with CloudSolrClient initialized
     */
    protected abstract void runCloudTool(CloudLegacySolrClient cloudSolrClient, CommandLine cli)
            throws Exception;
}
