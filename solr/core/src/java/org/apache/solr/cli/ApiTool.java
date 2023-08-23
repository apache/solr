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
import java.net.URI;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.JsonMapResponseParser;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.noggit.CharArr;
import org.noggit.JSONWriter;

public class ApiTool extends ToolBase {
  /** Used to send an arbitrary HTTP request to a Solr API endpoint. */
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
  public List<Option> getOptions() {
    return List.of(
        Option.builder("get")
            .argName("URL")
            .hasArg()
            .required(true)
            .desc("Send a GET request to a Solr API endpoint.")
            .build());
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    String response = null;
    String getUrl = cli.getOptionValue("get");
    if (getUrl != null) {
      response = callGet(getUrl);
    }
    if (response != null) {
      // pretty-print the response to stdout
      echo(response);
    }
  }

  protected String callGet(String url) throws Exception {
    URI uri = new URI(url.replace("+", "%20"));
    String solrUrl = getSolrUrlFromUri(uri);
    String path = uri.getPath();
    try (var solrClient = SolrCLI.getSolrClient(solrUrl)) {
      // For path parameter we need the path without the root so from the second / char
      // (because root can be configured)
      // E.g URL is http://localhost:8983/solr/admin/info/system path is
      // /solr/admin/info/system and the path without root is /admin/info/system
      var req =
          new GenericSolrRequest(
              SolrRequest.METHOD.GET,
              path.substring(path.indexOf("/", path.indexOf("/") + 1)),
              getSolrParamsFromUri(uri) // .add("indent", "true")
              );
      // Using the "smart" solr parsers won't work, because they decode into Solr objects.
      // When trying to re-write into JSON, the JSONWriter doesn't have the right info to print it
      // correctly.
      // All we want to do is pass the JSON response to the user, so do that.
      req.setResponseParser(new JsonMapResponseParser());
      NamedList<Object> response = solrClient.request(req);
      // pretty-print the response to stdout
      CharArr arr = new CharArr();
      new JSONWriter(arr, 2).write(response.asMap());
      return arr.toString();
    }
  }

  /**
   * Get Solr base url with port if present and root from URI
   *
   * @param uri Full Solr URI (e.g. http://localhost:8983/solr/admin/info/system)
   * @return Solr base url with port and root (from above example http://localhost:8983/solr)
   */
  public static String getSolrUrlFromUri(URI uri) {
    return uri.getScheme() + "://" + uri.getAuthority() + "/" + uri.getPath().split("/")[1];
  }

  public static ModifiableSolrParams getSolrParamsFromUri(URI uri) {
    ModifiableSolrParams paramsMap = new ModifiableSolrParams();
    String[] params = uri.getQuery() == null ? new String[] {} : uri.getQuery().split("&");
    for (String param : params) {
      String[] paramSplit = param.split("=");
      paramsMap.add(paramSplit[0], paramSplit[1]);
    }
    return paramsMap;
  }
}
