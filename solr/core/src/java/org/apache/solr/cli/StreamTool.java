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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
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
import org.apache.solr.common.params.ModifiableSolrParams;

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
  public List<Option> getOptions() {
    return List.of(
        SolrCLI.OPTION_ZKHOST,
        SolrCLI.OPTION_SOLRURL,
        Option.builder()
            .longOpt("workers")
            .hasArg()
            .required(false)
            .desc("Workers are either 'local' or 'solr'. Default is 'solr'")
            .build(),
        Option.builder("c")
            .longOpt("name")
            .argName("NAME")
            .hasArg()
            .desc(
                "Name of the collection to execute on if workers are 'solr'.  Required for 'solr' worker.")
            .build(),
        Option.builder("f")
            .longOpt("fields")
            .argName("FIELDS")
            .hasArg()
            .required(false)
            .desc(
                "The fields in the tuples to output. (defaults to fields in the first tuple of result set).")
            .build(),
        Option.builder()
            .longOpt("header")
            .required(false)
            .desc("Whether or not to include a header line. (default=false)")
            .build(),
        Option.builder()
            .longOpt("delimiter")
            .argName("CHARACTER")
            .hasArg()
            .required(false)
            .desc("The output delimiter. (default=tab).")
            .build(),
        Option.builder()
            .longOpt("array-delimiter")
            .argName("CHARACTER")
            .hasArg()
            .required(false)
            .desc("The delimiter multi-valued fields. (default=|)")
            .build(),
        SolrCLI.OPTION_CREDENTIALS);
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  public void runImpl(CommandLine cli) throws Exception {
    SolrCLI.raiseLogLevelUnlessVerbose(cli);

    String expressionArgument = cli.getArgs()[0];
    String worker = cli.getOptionValue("workers", "solr");

    String arrayDelimiter = cli.getOptionValue("array-delimiter", "|");
    String delimiter = cli.getOptionValue("delimiter", "   ");
    boolean includeHeaders = cli.hasOption("header");
    String[] outputHeaders = getOutputFields(cli);

    LineNumberReader bufferedReader = null;
    String expr;
    try {
      Reader inputStream =
          expressionArgument.toLowerCase(Locale.ROOT).endsWith("expr")
              ? new InputStreamReader(
                  new FileInputStream(expressionArgument), Charset.defaultCharset())
              : new StringReader(expressionArgument);

      bufferedReader = new LineNumberReader(inputStream);
      expr = StreamTool.readExpression(bufferedReader, cli.getArgs());
      echoIfVerbose("Running Expression: " + expr, cli);
    } finally {
      if (bufferedReader != null) {
        bufferedReader.close();
      }
    }

    PushBackStream pushBackStream;
    if (worker.equalsIgnoreCase("local")) {
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
                if (o instanceof List) {
                  List outfields = (List) o;
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

      if (pushBackStream != null) {
        pushBackStream.close();
      }

      solrClientCache.close();
    }

    if (verbose) {
      CLIO.err("StreamTool -- Done.");
      CLIO.err("");
    }
  }

  public PushBackStream doLocalMode(CommandLine cli, String expr) throws Exception {
    String zkHost = SolrCLI.getZkHost(cli);

    echoIfVerbose("Connecting to ZooKeeper at " + zkHost, cli);
    solrClientCache.getCloudSolrClient(zkHost);
    solrClientCache.setBasicAuthCredentials(cli.getOptionValue(SolrCLI.OPTION_CREDENTIALS));

    TupleStream stream;
    PushBackStream pushBackStream;

    StreamExpression streamExpression = StreamExpressionParser.parse(expr);
    StreamFactory streamFactory = new StreamFactory();

    streamFactory.withDefaultZkHost(zkHost);

    Lang.register(streamFactory);

    streamFactory.withFunctionName("stdin", StandardInStream.class);
    stream = StreamTool.constructStream(streamFactory, streamExpression);

    pushBackStream = new PushBackStream(stream);

    // Now we can run the stream and return the results.
    StreamContext streamContext = new StreamContext();
    streamContext.setSolrClientCache(solrClientCache);
    System.setProperty("COMMAND_LINE_EXPRESSION", "true");

    // Output the headers
    pushBackStream.setStreamContext(streamContext);

    return pushBackStream;
  }

  public PushBackStream doRemoteMode(CommandLine cli, String expr) throws Exception {

    String solrUrl = SolrCLI.normalizeSolrUrl(cli);
    if (!cli.hasOption("name")) {
      throw new IllegalStateException(
          "You must provide --name COLLECTION with --worker solr parameter.");
    }
    String collection = cli.getOptionValue("name");

    final SolrStream solrStream =
        new SolrStream(solrUrl + "/solr/" + collection, params("qt", "/stream", "expr", expr));

    String credentials = cli.getOptionValue(SolrCLI.OPTION_CREDENTIALS.getLongOpt());
    if (credentials != null) {
      String username = credentials.split(":")[0];
      String password = credentials.split(":")[1];
      solrStream.setCredentials(username, password);
    }
    return new PushBackStream(solrStream);
  }

  public static ModifiableSolrParams params(String... params) {
    if (params.length % 2 != 0) throw new RuntimeException("Params length should be even");
    ModifiableSolrParams msp = new ModifiableSolrParams();
    for (int i = 0; i < params.length; i += 2) {
      msp.add(params[i], params[i + 1]);
    }
    return msp;
  }

  /* Slurps a stream into a List */
  protected static List<Tuple> getTuples(final TupleStream tupleStream) throws IOException {
    List<Tuple> tuples = new ArrayList<>();
    try (tupleStream) {
      // log.trace("TupleStream: {}", tupleStream);
      System.out.println("TupleStream: {}" + tupleStream);
      tupleStream.open();
      for (Tuple t = tupleStream.read(); !t.EOF; t = tupleStream.read()) {
        // if (log.isTraceEnabled()) {
        //  log.trace("Tuple: {}", t.getFields());
        System.out.println("Tuple:" + t.getFields());
        // }
        tuples.add(t);
      }
    }
    return tuples;
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

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Tuple read() throws IOException {
      String line = reader.readLine();
      HashMap map = new HashMap();
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

  public static String[] getOutputFields(CommandLine cli) {
    if (cli.hasOption("fields")) {

      String fl = cli.getOptionValue("fields");
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

  @SuppressWarnings({"rawtypes"})
  public static String[] getHeadersFromFirstTuple(Tuple tuple) {
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
  public static String listToString(List values, String internalDelim) {
    StringBuilder buf = new StringBuilder();
    for (Object value : values) {
      if (buf.length() > 0) {
        buf.append(internalDelim);
      }

      buf.append(value.toString());
    }

    return buf.toString();
  }

  public static TupleStream constructStream(
      StreamFactory streamFactory, StreamExpression streamExpression) throws IOException {
    return streamFactory.constructStream(streamExpression);
  }

  public static String readExpression(LineNumberReader bufferedReader, String[] args)
      throws IOException {

    StringBuilder exprBuff = new StringBuilder();

    boolean comment = false;
    while (true) {
      String line = bufferedReader.readLine();
      if (line == null) {
        break;
      }

      if (line.indexOf("/*") == 0) {
        comment = true;
        continue;
      }

      if (line.indexOf("*/") == 0) {
        comment = false;
        continue;
      }

      if (comment || line.startsWith("#") || line.startsWith("//")) {
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
