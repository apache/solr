package org.apache.solr.util.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.util.CLIO;
import org.apache.solr.util.SolrCLI;

import java.io.PrintStream;
import java.util.Map;

public class CreateTool extends ToolBase {

    public CreateTool() {
        this(CLIO.getOutStream());
    }

    public CreateTool(PrintStream stdout) {
        super(stdout);
    }

    @Override
    public String getName() {
        return "create";
    }

    @Override
    public Option[] getOptions() {
        return SolrCLI.CREATE_COLLECTION_OPTIONS;
    }

    @Override
    public void runImpl(CommandLine cli) throws Exception {
        SolrCLI.raiseLogLevelUnlessVerbose(cli);
        String solrUrl = cli.getOptionValue("solrUrl", SolrCLI.DEFAULT_SOLR_URL);
        if (!solrUrl.endsWith("/")) solrUrl += "/";

        String systemInfoUrl = solrUrl + "admin/info/system";
        CloseableHttpClient httpClient = SolrCLI.getHttpClient();

        ToolBase tool;
        try {
            Map<String, Object> systemInfo = SolrCLI.getJson(httpClient, systemInfoUrl, 2, true);
            if ("solrcloud".equals(systemInfo.get("mode"))) {
                tool = new CreateCollectionTool(stdout);
            } else {
                tool = new CreateCoreTool(stdout);
            }
            tool.runImpl(cli);
        } finally {
            SolrCLI.closeHttpClient(httpClient);
        }
    }
}
