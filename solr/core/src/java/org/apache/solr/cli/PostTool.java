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
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostTool extends ToolBase {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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
                Option.builder("commit")
                        .required(false)
                        .desc("Issue a commit at end of post")
                        .build(),
                Option.builder("optimize")
                        .required(false)
                        .desc("Issue an optimize at end of post")
                        .build(),
                Option.builder("recursive")
                        .required(false)
                        .desc("default: 1")
                        .build(),
                Option.builder("delay")
                        .argName("seconds")
                        .hasArg(true)
                        .required(false)
                        .desc("default: 10 for web, 0 for files")
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
                        .desc("default: xml,json,jsonl,csv,pdf,doc,docx,ppt,pptx,xls,xlsx,odt,odp,ods,ott,otp,ots,rtf,htm,html,txt,log")
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
                        .desc("sends application/json content as Solr commands to /update instead of /update/json/docs")
                        .build());

    }

    @Override
    public void runImpl(CommandLine cli) throws Exception {
        SolrCLI.raiseLogLevelUnlessVerbose(cli);

        String url = cli.getOptionValue("url");
        URL solrUrl = new URL(url);

        //Map<String,String> props = Collections.singletonMap("-Dauto", "yes");
        String mode = SimplePostTool.DEFAULT_DATA_MODE;
        boolean auto = true;
        String type = SimplePostTool.DEFAULT_CONTENT_TYPE;
        if (cli.hasOption("type")){
            type = cli.getOptionValue("type");
        }
        String format = cli.hasOption("format") ? "solr": ""; // i.e not solr formatted json commands
        int delay = 0;
        String fileTypes ="";
        int recursive = 0;
        String r = cli.getOptionValue("recursive","1");
        try {
            recursive = Integer.parseInt(r);
        } catch (Exception e) {
            if (cli.hasOption("recursive")) {
                recursive = SimplePostTool.DATA_MODE_WEB.equals(mode) ? 1 : 999;
            }
        }


        OutputStream out = cli.hasOption("out") ? CLIO.getOutStream() : null;
        Boolean commit = cli.hasOption("commit");
        Boolean optimize = cli.hasOption("optimize");;

        //String[] args = {"fake_to_pass_check_in_execute_method"};
        String[] args = cli.getArgs();

        System.out.println("ERIC HERE ARE ARGS");
        System.out.println(cli.getArgs().toString());
        //SimplePostTool spt2 = SimplePostTool.parseArgsAndInit(cli.getArgs());
        SimplePostTool spt = new SimplePostTool(mode, solrUrl, auto, type, format, recursive, delay, fileTypes, out, commit, optimize, args);

        spt.execute();
    }



}
