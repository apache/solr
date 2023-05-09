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

import static org.apache.solr.packagemanager.PackageUtils.print;
import static org.apache.solr.packagemanager.PackageUtils.printGreen;

import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.exec.OS;

public class PostTool extends ToolBase {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public PostTool() {
        this(CLIO.getOutStream());
    }

    public PostTool(PrintStream stdout) {
        super(stdout);
    }

    private static final boolean isWindows = (OS.isFamilyDOS() || OS.isFamilyWin9x() || OS.isFamilyWindows());
    private static final String THIS_SCRIPT = (isWindows ? "bin\\solr.cmd post" : "bin/solr post");

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
                Option.builder("host")
                        .argName("host")
                        .hasArg()
                        .required(false)
                        .desc("<host> (default: localhost)")
                        .build(),
                Option.builder("p")
                        .argName("port")
                        .hasArg()
                        .required(false)
                        .desc("Number of shards; default is 1")
                        //.withLongOpt("port")
                        .build(),
                Option.builder("commit")
                        .argName("yes|no")
                        .hasArg(true)
                        .required(false)
                        .desc("Whether to commit at end of post (default: yes)")
                        .build(),
                Option.builder("h")
                        .argName("help")
                        .hasArg(false)
                        .required(false)
                        .desc("prints tool help")
                        //.withLongOpt("help")
                        .build(),
                Option.builder("recursive")
                        .argName("depth")
                        .hasArg(true)
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
                        .desc("default: application/xml")
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
                        .argName("yes|no")
                        .hasArg(true)
                        .required(false)
                        .desc("default: no; yes outputs Solr response to console")
                        .build(),
                Option.builder("format")
                        .argName("solr")
                        .hasArg(true)
                        .required(false)
                        .desc("sends application/json content as Solr commands to /update instead of /update/json/docs")
                        .build());

    }

    @Override
    public void runImpl(CommandLine cli) throws Exception {
        SolrCLI.raiseLogLevelUnlessVerbose(cli);

        String solrUrl = cli.getOptionValue("url");

        Map<String,String> props = Collections.singletonMap("-Dauto", "yes");
        boolean recursive = false;
        String[] args = cli.getArgs();
        String mode = SimplePostTool.DEFAULT_DATA_MODE;
        boolean auto = true;
        String type = SimplePostTool.DEFAULT_CONTENT_TYPE;
        String format = ""; // i.e not solr formatted json commands
        int delay = 0;
        String fileTypes ="";
        OutputStream out;
        Boolean commit = false;
        Boolean optimize = false;

        // this is a weird way to pass it in!
        System.setProperty("url",solrUrl);

        //SimplePostTool spt = new SimplePostTool(mode, solrUrl, auto, type, format, recursive, delay, fileTypes, out, commit, optimize, args);
        SimplePostTool spt = SimplePostTool.parseArgsAndInit(cli.getArgs());
        spt.execute();
    }
    


}
