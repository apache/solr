package org.apache.solr.util.cli;

import java.io.PrintStream;
import java.net.URI;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.CLIO;
import org.apache.solr.util.SolrCLI;
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
    String getUrl = cli.getOptionValue("get");
    if (getUrl != null) {
      getUrl = getUrl.replace("+", "%20");
      URI uri = new URI(getUrl);
      String solrUrl = SolrCLI.getSolrUrlFromUri(uri);
      String path = uri.getPath();
      try (var solrClient = SolrCLI.getSolrClient(solrUrl)) {
        NamedList<Object> response =
            solrClient.request(
                // For path parameter we need the path without the root so from the second / char
                // (because root can be configured)
                // E.g URL is http://localhost:8983/solr/admin/info/system path is
                // /solr/admin/info/system and the path without root is /admin/info/system
                new GenericSolrRequest(
                    SolrRequest.METHOD.GET,
                    path.substring(path.indexOf("/", path.indexOf("/") + 1)),
                    SolrCLI.getSolrParamsFromUri(uri)));

        // pretty-print the response to stdout
        CharArr arr = new CharArr();
        new JSONWriter(arr, 2).write(response.asMap());
        echo(arr.toString());
      }
    }
  }
}
