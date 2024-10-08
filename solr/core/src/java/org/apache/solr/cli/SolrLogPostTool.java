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

/**
 * A command line tool for indexing Solr logs in the out-of-the-box log format.
 *
 * @deprecated Please use {@link PostLogsTool} that is exposed as 'bin/solr postlogs'.
 */
@Deprecated(since = "9.4")
public class SolrLogPostTool {

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      CLIO.out("");
      CLIO.out("postlogs is a simple tool for indexing Solr logs.");
      CLIO.out("");
      CLIO.out("parameters:");
      CLIO.out("");
      CLIO.out("-- baseUrl: Example http://localhost:8983/solr/collection1");
      CLIO.out("-- rootDir: All files found at or below the root will be indexed.");
      CLIO.out("");
      CLIO.out(
          "Sample syntax 1: ./bin/postlogs http://localhost:8983/solr/collection1 /user/foo/logs/solr.log");
      CLIO.out(
          "Sample syntax 2: ./bin/postlogs http://localhost:8983/solr/collection1 /user/foo/logs");
      CLIO.out("");
      return;
    }
    String baseUrl = args[0];
    String root = args[1];
    PostLogsTool postLogsTool = new PostLogsTool();
    postLogsTool.runCommand(baseUrl, root);
  }
}
