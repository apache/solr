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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.solr.client.solrj.io.Lang;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.PushBackStream;
import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.handler.CatStream;

/** Supports stream command in the bin/solr script. */
public class StreamTool extends ToolBase {

  public StreamTool() {
    this(CLIO.getOutStream());
  }

  public StreamTool(PrintStream stdout) {
    super(stdout);
  }

  private final SolrClientCache solrClientCache = new SolrClientCache();

  @Override
  public String getName() {
    return "stream";
  }

  @Override
  public String getUsage() {
    // Specify that the last argument is the streaming expression
    return "bin/solr stream [--array-delimiter <CHARACTER>] [-c <NAME>] [--delimiter <CHARACTER>] [--execution <ENVIRONMENT>] [--fields\n"
        + "       <FIELDS>] [-h] [--header] [-s <HOST>] [-u <credentials>] [-v] [-z <HOST>]  <streaming expression OR stream_file.expr>\n";
  }

  private static final Option EXECUTION_OPTION =
      Option.builder()
          .longOpt("execution")
          .hasArg()
          .argName("ENVIRONMENT")
          .desc(
              "Execution environment is either 'local' (i.e CLI process) or via a 'remote' Solr server. Default environment is 'remote'.")
          .build();

  private static final Option COLLECTION_OPTION =
      Option.builder("c")
          .longOpt("name")
          .argName("NAME")
          .hasArg()
          .desc(
              "Name of the specific collection to execute expression on if the execution is set to 'remote'. Required for 'remote' execution environment.")
          .build();

  private static final Option FIELDS_OPTION =
      Option.builder()
          .longOpt("fields")
          .argName("FIELDS")
          .hasArg()
          .desc(
              "The fields in the tuples to output. Defaults to fields in the first tuple of result set.")
          .build();

  private static final Option HEADER_OPTION =
      Option.builder().longOpt("header").desc("Specify to include a header line.").build();

  private static final Option DELIMITER_OPTION =
      Option.builder()
          .longOpt("delimiter")
          .argName("CHARACTER")
          .hasArg()
          .desc("The output delimiter. Default to using three spaces.")
          .build();
  private static final Option ARRAY_DELIMITER_OPTION =
      Option.builder()
          .longOpt("array-delimiter")
          .argName("CHARACTER")
          .hasArg()
          .desc("The delimiter multi-valued fields. Default to using a pipe (|) delimiter.")
          .build();

  @Override
  public Options getOptions() {

    return super.getOptions()
        .addOption(EXECUTION_OPTION)
        .addOption(COLLECTION_OPTION)
        .addOption(FIELDS_OPTION)
        .addOption(HEADER_OPTION)
        .addOption(DELIMITER_OPTION)
        .addOption(ARRAY_DELIMITER_OPTION)
        .addOption(CommonCLIOptions.CREDENTIALS_OPTION)
        .addOptionGroup(getConnectionOptions());
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  public void runImpl(CommandLine cli) throws Exception {

    String expressionArgument = cli.getArgs()[0];
    String execution = cli.getOptionValue(EXECUTION_OPTION, "remote");
    String arrayDelimiter = cli.getOptionValue(ARRAY_DELIMITER_OPTION, "|");
    String delimiter = cli.getOptionValue(DELIMITER_OPTION, "   ");
    boolean includeHeaders = cli.hasOption(HEADER_OPTION);
    String[] outputHeaders = getOutputFields(cli);

    LineNumberReader bufferedReader = null;
    String expr;
    try {
      Reader inputStream =
          expressionArgument.toLowerCase(Locale.ROOT).endsWith(".expr")
              ? new InputStreamReader(
                  new FileInputStream(expressionArgument), Charset.defaultCharset())
              : new StringReader(expressionArgument);

      bufferedReader = new LineNumberReader(inputStream);
      expr = StreamTool.readExpression(bufferedReader, cli.getArgs());
      echoIfVerbose("Running Expression: " + expr);
    } finally {
      if (bufferedReader != null) {
        bufferedReader.close();
      }
    }

    PushBackStream pushBackStream;
    if (execution.equalsIgnoreCase("local")) {
      pushBackStream = doLocalMode(cli, expr);
    } else {
      pushBackStream = doRemoteMode(cli, expr);
    }

    try {
      pushBackStream.open();

      if (outputHeaders == null) {

        Tuple tuple = pushBackStream.read();

        if (!tuple.EOF) {
          outputHeaders = getHeadersFromFirstTuple(tuple);
        }

        pushBackStream.pushBack(tuple);
      }

      if (includeHeaders) {
        StringBuilder headersOut = new StringBuilder();
        if (outputHeaders != null) {
          for (int i = 0; i < outputHeaders.length; i++) {
            if (i > 0) {
              headersOut.append(delimiter);
            }
            headersOut.append(outputHeaders[i]);
          }
        }
        CLIO.out(headersOut.toString());
      }

      while (true) {
        Tuple tuple = pushBackStream.read();
        if (tuple.EOF) {
          break;
        } else {
          StringBuilder outLine = new StringBuilder();
          if (outputHeaders != null) {
            for (int i = 0; i < outputHeaders.length; i++) {
              if (i > 0) {
                outLine.append(delimiter);
              }

              Object o = tuple.get(outputHeaders[i]);
              if (o != null) {
                if (o instanceof List outfields) {
                  outLine.append(listToString(outfields, arrayDelimiter));
                } else {
                  outLine.append(o);
                }
              }
            }
          }
          CLIO.out(outLine.toString());
        }
      }
    } finally {
      pushBackStream.close();
      solrClientCache.close();
    }

    echoIfVerbose("StreamTool -- Done.");
  }

  /**
   * Runs a streaming expression in the local process of the CLI.
   *
   * <p>Running locally means that parallelization support or those expressions requiring access to
   * internal Solr capabilities will not function.
   *
   * @param cli The CLI invoking the call
   * @param expr The streaming expression to be parsed and in the context of the CLI process
   * @return A connection to the streaming expression that receives Tuples as they are emitted
   *     locally.
   */
  private PushBackStream doLocalMode(CommandLine cli, String expr) throws Exception {
    String zkHost = CLIUtils.getZkHost(cli);

    echoIfVerbose("Connecting to ZooKeeper at " + zkHost);
    solrClientCache.setBasicAuthCredentials(
        cli.getOptionValue(CommonCLIOptions.CREDENTIALS_OPTION));
    solrClientCache.getCloudSolrClient(zkHost);

    TupleStream stream;
    PushBackStream pushBackStream;

    StreamExpression streamExpression = StreamExpressionParser.parse(expr);
    StreamFactory streamFactory = new StreamFactory();

    // stdin is ONLY available in the local mode, not in the remote mode as it
    // requires access to System.in
    streamFactory.withFunctionName("stdin", StandardInStream.class);

    // LocalCatStream extends CatStream and disables the Solr cluster specific
    // logic about where to read data from.
    streamFactory.withFunctionName("cat", LocalCatStream.class);

    streamFactory.withDefaultZkHost(zkHost);

    Lang.register(streamFactory);

    stream = streamFactory.constructStream(streamExpression);

    pushBackStream = new PushBackStream(stream);

    // Now we can run the stream and return the results.
    StreamContext streamContext = new StreamContext();
    streamContext.setSolrClientCache(solrClientCache);

    // Output the headers
    pushBackStream.setStreamContext(streamContext);

    return pushBackStream;
  }

  /**
   * Runs a streaming expression on a Solr collection via the /stream end point and returns the
   * results to the CLI. Requires a collection to be specified to send the expression to.
   *
   * <p>Running remotely allows you to use all the standard Streaming Expression capabilities as the
   * expression is running in a Solr environment.
   *
   * @param cli The CLI invoking the call
   * @param expr The streaming expression to be parsed and run remotely
   * @return A connection to the streaming expression that receives Tuples as they are emitted from
   *     Solr /stream.
   */
  private PushBackStream doRemoteMode(CommandLine cli, String expr) throws Exception {

    String solrUrl = CLIUtils.normalizeSolrUrl(cli);
    if (!cli.hasOption(COLLECTION_OPTION)) {
      throw new IllegalStateException(
          "You must provide --name COLLECTION with --execution remote parameter.");
    }
    String collection = cli.getOptionValue(COLLECTION_OPTION);

    if (expr.toLowerCase(Locale.ROOT).contains("stdin(")) {
      throw new IllegalStateException(
          "The stdin() expression is only usable with --worker local set up.");
    }

    final SolrStream solrStream =
        new SolrStream(solrUrl + "/solr/" + collection, params("qt", "/stream", "expr", expr));

    String credentials = cli.getOptionValue(CommonCLIOptions.CREDENTIALS_OPTION);
    if (credentials != null) {
      String username = credentials.split(":")[0];
      String password = credentials.split(":")[1];
      solrStream.setCredentials(username, password);
    }
    return new PushBackStream(solrStream);
  }

  private static ModifiableSolrParams params(String... params) {
    if (params.length % 2 != 0) throw new RuntimeException("Params length should be even");
    ModifiableSolrParams msp = new ModifiableSolrParams();
    for (int i = 0; i < params.length; i += 2) {
      msp.add(params[i], params[i + 1]);
    }
    return msp;
  }

  public static class StandardInStream extends TupleStream implements Expressible {

    private BufferedReader reader;
    private InputStream inputStream = System.in;
    private boolean doClose = false;

    public StandardInStream() {}

    public StandardInStream(StreamExpression expression, StreamFactory factory)
        throws IOException {}

    @Override
    public List<TupleStream> children() {
      return null;
    }

    public void setInputStream(InputStream inputStream) {
      this.inputStream = inputStream;
      this.doClose = true;
    }

    @Override
    public void open() {
      reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
    }

    @Override
    public void close() throws IOException {
      if (doClose) {
        inputStream.close();
      }
    }

    @Override
    public Tuple read() throws IOException {
      String line = reader.readLine();
      Map<String, ?> map = new HashMap<>();
      Tuple tuple = new Tuple(map);
      if (line != null) {
        tuple.put("line", line);
        tuple.put("file", "cat");
      } else {
        tuple.put("EOF", "true");
      }
      return tuple;
    }

    @Override
    public void setStreamContext(StreamContext context) {}

    @Override
    public StreamExpression toExpression(StreamFactory factory) {
      return null;
    }

    @Override
    public Explanation toExplanation(StreamFactory factory) {
      return null;
    }

    @Override
    public StreamComparator getStreamSort() {
      return null;
    }
  }

  static String[] getOutputFields(CommandLine cli) {
    if (cli.hasOption(FIELDS_OPTION)) {

      String fl = cli.getOptionValue(FIELDS_OPTION);
      String[] flArray = fl.split(",");
      String[] outputHeaders = new String[flArray.length];

      for (int i = 0; i < outputHeaders.length; i++) {
        outputHeaders[i] = flArray[i].trim();
      }

      return outputHeaders;

    } else {
      return null;
    }
  }

  public static class LocalCatStream extends CatStream {

    public LocalCatStream(StreamExpression expression, StreamFactory factory) throws IOException {
      super(expression, factory);
    }

    public LocalCatStream(String commaDelimitedFilepaths, int maxLines) {
      super(commaDelimitedFilepaths, maxLines);
    }

    @Override
    public void setStreamContext(StreamContext context) {
      // LocalCatStream inherently has no Solr core to pull from the context
    }

    @Override
    protected List<CrawlFile> validateAndSetFilepathsInSandbox() {
      // The nature of LocalCatStream is that we are not limited to the sandboxed "userfiles"
      // directory
      // the way the CatStream does.

      final List<CrawlFile> crawlSeeds = new ArrayList<>();
      for (String crawlRootStr : commaDelimitedFilepaths.split(",")) {
        Path crawlRootPath = Paths.get(crawlRootStr).normalize();

        if (!Files.exists(crawlRootPath)) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "file/directory to stream doesn't exist: " + crawlRootStr);
        }

        crawlSeeds.add(new CrawlFile(crawlRootStr, crawlRootPath));
      }

      return crawlSeeds;
    }
  }

  @SuppressWarnings({"rawtypes"})
  static String[] getHeadersFromFirstTuple(Tuple tuple) {
    Set fields = tuple.getFields().keySet();
    String[] outputHeaders = new String[fields.size()];
    int i = -1;
    for (Object o : fields) {
      outputHeaders[++i] = o.toString();
    }
    Arrays.sort(outputHeaders);
    return outputHeaders;
  }

  @SuppressWarnings({"rawtypes"})
  static String listToString(List values, String internalDelim) {
    StringBuilder buf = new StringBuilder();
    for (Object value : values) {
      if (buf.length() > 0) {
        buf.append(internalDelim);
      }

      buf.append(value.toString());
    }

    return buf.toString();
  }

  static String readExpression(LineNumberReader bufferedReader, String[] args) throws IOException {

    StringBuilder exprBuff = new StringBuilder();

    boolean comment = false;
    while (true) {
      String line = bufferedReader.readLine();
      if (line == null) {
        break;
      }

      if (line.trim().indexOf("/*") == 0) {
        comment = true;
        continue;
      }

      if (line.trim().contains("*/")) {
        comment = false;
        continue;
      }

      if (comment || line.trim().startsWith("#") || line.trim().startsWith("//")) {
        continue;
      }

      // Substitute parameters

      if (line.length() > 0) {
        for (int i = 1; i < args.length; i++) {
          String arg = args[i];
          line = line.replace("$" + i, arg);
        }
      }

      exprBuff.append(line);
    }

    return exprBuff.toString();
  }
}
