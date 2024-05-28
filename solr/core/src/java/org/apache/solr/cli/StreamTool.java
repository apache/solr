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
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/** Supports stream command in the bin/solr script. */
public class StreamTool extends ToolBase {

  // not sure this is right!!!
  public static final String OUTPUT_FIELDS = "fields";

  public StreamTool() {
    this(CLIO.getOutStream());
  }

  public StreamTool(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public String getName() {
    return "stream";
  }

  @Override
  public List<Option> getOptions() {
    return List.of(
        SolrCLI.OPTION_ZKHOST,
        SolrCLI.OPTION_SOLRURL,
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
        SolrCLI.OPTION_CREDENTIALS,
        SolrCLI.OPTION_VERBOSE);
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  public void runImpl(CommandLine cli) throws Exception {
    SolrCLI.raiseLogLevelUnlessVerbose(cli);

    String zkHost = SolrCLI.getZkHost(cli);
    String[] outputHeaders;
    String arrayDelimiter = cli.getOptionValue("array-delimiter", "|");
    String delimiter = cli.getOptionValue("delimiter", "   ");
    boolean includeHeaders = cli.hasOption("header");
    // not sure I need verbose however.
    verbose = cli.hasOption(SolrCLI.OPTION_VERBOSE.getOpt());

    System.out.println("HERE WE GO");
    System.out.print(cli.getArgList());
    System.out.println(cli.getArgs());

    for (var arg : cli.getArgList()) {
      System.out.println("Printing from getArgList" + arg);
    }

    for (var arg : cli.getArgs()) {
      System.out.println("Printing from getArgs" + arg);
    }

    String expressionArgument = cli.getArgs()[0];
    // Http2SolrClient solrClient = (Http2SolrClient) SolrCLI.getSolrClient(cli);
    // SolrClientCache solrClientCache = new SolrClientCache(solrClient);
    SolrClientCache solrClientCache = new SolrClientCache();
    TupleStream stream;
    PushBackStream pushBackStream = null;
    LineNumberReader bufferedReader = null;

    // likewise not sure...
//    PrintStream filterOut =
//        new PrintStream(System.err) {
//          @Override
//          public void println(String l) {
//            if (!l.startsWith("SLF4J")) {
//              super.println(l);
//            }
//          }
//        };
//
//    System.setErr(filterOut);

    try {
      // Read from the commandline either the file param or the actual expression
      Reader inputStream =
          expressionArgument.toLowerCase(Locale.ROOT).endsWith("expr")
              ? new InputStreamReader(
                  new FileInputStream(expressionArgument), Charset.defaultCharset())
              : new StringReader(expressionArgument);

      bufferedReader = new LineNumberReader(inputStream);
      String expr = readExpression(bufferedReader, cli.getArgs());
      echoIfVerbose("Running Expression: " + expr, cli);
      StreamExpression streamExpression = StreamExpressionParser.parse(expr);
      StreamFactory streamFactory = new StreamFactory();

      streamFactory.withDefaultZkHost(zkHost);

      Lang.register(streamFactory);

      streamFactory.withFunctionName("stdin", StandardInStream.class);
      stream = constructStream(streamFactory, streamExpression);

      outputHeaders = getOutputFields(cli);

      pushBackStream = new PushBackStream(stream);

      solrClientCache = new SolrClientCache();

      // unsure of need for this.
      if (zkHost != null) {
        echoIfVerbose("Connecting to ZooKeeper at " + zkHost, cli);
        solrClientCache.getCloudSolrClient(zkHost);
      }

      // Now we can run the stream and return the results.
      StreamContext streamContext = new StreamContext();
      streamContext.setSolrClientCache(solrClientCache);
      System.setProperty("COMMAND_LINE_EXPRESSION", "true");
      // Output the headers

      if (verbose) {
        CLIO.err("");
        CLIO.err("CLI -- Running Expression: " + expr + " ...");
        CLIO.err("");
      }

      pushBackStream.setStreamContext(streamContext);

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
        for (int i = 0; i < outputHeaders.length; i++) {
          if (i > 0) {
            headersOut.append(delimiter);
          }
          headersOut.append(outputHeaders[i]);
        }
        CLIO.out(headersOut.toString());
      }

      while (true) {
        Tuple tuple = pushBackStream.read();
        if (tuple.EOF) {
          break;
        } else {
          StringBuilder outLine = new StringBuilder();
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
          CLIO.out(outLine.toString());
        }
      }
    } finally {

      if (verbose) {
        CLIO.err("");
        CLIO.err("CLI -- Expression complete");
        CLIO.err("CLI -- Cleaning up resources ...");
      }

      if (pushBackStream != null) {
        pushBackStream.close();
      }

      if (bufferedReader != null) {
        bufferedReader.close();
      }

      if (solrClientCache != null) {
        solrClientCache.close();
      }

      if (verbose) {
        CLIO.err("CLI -- Done.");
        CLIO.err("");
      }
    }
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
          System.out.println("arg:" + arg);
          line = line.replace("$" + i, arg);
        }
      }

      exprBuff.append(line);
    }

    return exprBuff.toString();
  }
}
