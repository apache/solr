package org.apache.solr.util.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.util.CLIO;
import org.apache.solr.util.SolrCLI;
import org.noggit.CharArr;
import org.noggit.JSONWriter;

import java.io.PrintStream;
import java.util.Map;

/**
 * Used to send an arbitrary HTTP request to a Solr API endpoint.
 */
public class ApiTool extends ToolBase {

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
    public Option[] getOptions() {
        return new Option[]{
                Option.builder("get")
                        .argName("URL")
                        .hasArg()
                        .required(true)
                        .desc("Send a GET request to a Solr API endpoint.")
                        .build()
        };
    }

    @Override
    public void runImpl(CommandLine cli) throws Exception {
        String getUrl = cli.getOptionValue("get");
        if (getUrl != null) {
            Map<String, Object> json = SolrCLI.getJson(getUrl);

            // pretty-print the response to stdout
            CharArr arr = new CharArr();
            new JSONWriter(arr, 2).write(json);
            echo(arr.toString());
        }
    }
} // end ApiTool class
