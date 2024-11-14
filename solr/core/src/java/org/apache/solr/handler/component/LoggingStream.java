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
package org.apache.solr.handler.component;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.noggit.CharArr;
import org.noggit.JSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends tuples emitted by a wrapped {@link TupleStream} as writes to a log file. The log file will
 * be created in the "userfiles" directory and formatted in the JSON w/ Lines format.
 *
 * <p>I really want to call this the DogStream, as it matches the CatStream.
 *
 * <p>Is this generically useful to be added to the streaming jar and Lang?
 *
 * <p>WriterStream? LoggingStream? FileoutputStream? JsonOutputStream? LoggingStream??
 *
 * @since 9.8.0
 */
public class LoggingStream extends TupleStream implements Expressible {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // field name in summary tuple for #docs updated in batch
  public static String BATCH_LOGGED_FIELD_NAME = "batchLogged";

  private Path chroot;

  /**
   * The name of the log file that should be written to. This will be in the same directory that the
   * CatStream is allowed to write to.
   */
  private String filepath;

  private int updateBatchSize;

  private int batchNumber;
  private long totalDocsIndex;
  // private PushBackStream tupleSource;
  private TupleStream tupleSource;
  private List<SolrInputDocument> documentBatch = new ArrayList<>();

  private OutputStream fos;
  private final CharArr charArr = new CharArr(1024 * 2);
  JSONWriter jsonWriter = new JSONWriter(charArr, -1);
  private Writer writer;

  public LoggingStream(StreamExpression expression, StreamFactory factory) throws IOException {

    filepath = factory.getValueOperand(expression, 0);
    if (filepath == null) {
      throw new IllegalArgumentException("No filepath provided to log stream to");
    }
    final String filepathWithoutSurroundingQuotes = stripSurroundingQuotesIfTheyExist(filepath);
    if (StrUtils.isNullOrEmpty(filepathWithoutSurroundingQuotes)) {
      throw new IllegalArgumentException("No filepath provided to stream");
    }

    this.filepath = filepathWithoutSurroundingQuotes;

    // Extract underlying TupleStream.
    List<StreamExpression> streamExpressions =
        factory.getExpressionOperandsRepresentingTypes(
            expression, Expressible.class, TupleStream.class);
    if (1 != streamExpressions.size()) {
      throw new IOException(
          String.format(
              Locale.ROOT,
              "Invalid expression %s - expecting a single stream but found %d",
              expression,
              streamExpressions.size()));
    }
    StreamExpression sourceStreamExpression = streamExpressions.get(0);
    init(filepathWithoutSurroundingQuotes, factory.constructStream(sourceStreamExpression));
  }

  public LoggingStream(String filepath, TupleStream tupleSource) throws IOException {

    init(filepath, tupleSource);
  }

  private void init(String filepath, TupleStream tupleSource) {
    this.filepath = filepath;
    this.tupleSource = tupleSource;
  }

  /** The name of the file being updated */
  protected String getFilePath() {
    return filepath;
  }

  @Override
  public void open() throws IOException {
    Path filePath = chroot.resolve(filepath).normalize();
    if (!filePath.startsWith(chroot)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "file to log to must be under " + chroot);
    }

    fos = new FileOutputStream(filePath.toFile(), true);
    writer = new OutputStreamWriter(fos, StandardCharsets.UTF_8);

    tupleSource.open();
  }

  @Override
  public Tuple read() throws IOException {

    Tuple tuple = tupleSource.read();
    if (tuple.EOF) {

      return tuple;
    } else {
      // tupleSource.pushBack(tuple);
      uploadBatchToCollection(tuple);
      // return createBatchSummaryTuple(b);
    }

    // uploadBatchToCollection(documentBatch);
    // int b = documentBatch.size();
    // documentBatch.clear();
    int b = 1;
    return createBatchSummaryTuple(b);
  }

  @Override
  public void close() throws IOException {
    if (writer != null) {
      writer.flush();
    }
    if (fos != null) {
      fos.flush();
      fos.close();
    }
    tupleSource.close();
  }

  @Override
  public StreamComparator getStreamSort() {
    return tupleSource.getStreamSort();
  }

  @Override
  public List<TupleStream> children() {
    ArrayList<TupleStream> sourceList = new ArrayList<>(1);
    sourceList.add(tupleSource);
    return sourceList;
  }

  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException {
    return toExpression(factory, true);
  }

  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams)
      throws IOException {
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    expression.addParameter(filepath);

    if (includeStreams) {
      if (tupleSource != null) {
        expression.addParameter(((Expressible) tupleSource).toExpression(factory));
      } else {
        throw new IOException(
            "This LoggingStream contains a non-expressible TupleStream - it cannot be converted to an expression");
      }
    } else {
      expression.addParameter("<stream>");
    }

    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    StreamExplanation explanation = new StreamExplanation(getStreamNodeId() + "-datastore");

    explanation.setFunctionName(String.format(Locale.ROOT, "logging (%s)", filepath));
    explanation.setImplementingClass(this.getClass().getName());
    explanation.setExpressionType(ExpressionType.DATASTORE);
    explanation.setExpression("Log tuples into " + filepath);

    // child is a datastore so add it at this point
    StreamExplanation child = new StreamExplanation(getStreamNodeId().toString());
    child.setFunctionName(String.format(Locale.ROOT, factory.getFunctionName(getClass())));
    child.setImplementingClass(getClass().getName());
    child.setExpressionType(ExpressionType.DATASTORE);
    child.setExpression(toExpression(factory, false).toString());
    child.addChild(tupleSource.toExplanation(factory));

    explanation.addChild(child);

    return explanation;
  }

  @Override
  public void setStreamContext(StreamContext context) {
    Object solrCoreObj = context.get("solr-core");
    if (solrCoreObj == null || !(solrCoreObj instanceof SolrCore)) {
      throw new SolrException(
          SolrException.ErrorCode.INVALID_STATE,
          "StreamContext must have SolrCore in solr-core key");
    }
    final SolrCore core = (SolrCore) context.get("solr-core");

    this.chroot = core.getCoreContainer().getUserFilesPath();
    if (!Files.exists(chroot)) {
      try {
        Files.createDirectories(this.chroot);
      } catch (IOException ioe) {
        throw new SolrException(
            SolrException.ErrorCode.INVALID_STATE,
            chroot + " directory used to load files must exist but and couldn't be created!");
      }
    }

    // Pass down the stream context.
    this.tupleSource.setStreamContext(context);
  }

  //    private SolrInputDocument convertTupleTJson(Tuple tuple) {
  //        SolrInputDocument doc = new SolrInputDocument();
  //        for (String field : tuple.getFields().keySet()) {
  //
  //            if (!(field.equals(CommonParams.VERSION_FIELD) )) {
  //                Object value = tuple.get(field);
  //                if (value instanceof List) {
  //                    addMultivaluedField(doc, field, (List<?>) value);
  //                } else {
  //                    doc.addField(field, value);
  //                }
  //            }
  //        }
  //        log.debug("Tuple [{}] was converted into SolrInputDocument [{}].", tuple, doc);
  //        jsonWriter
  //        return doc;
  //    }

  private void addMultivaluedField(SolrInputDocument doc, String fieldName, List<?> values) {
    for (Object value : values) {
      doc.addField(fieldName, value);
    }
  }

  /**
   * This method will be called on every batch of tuples consumed, after converting each tuple in
   * that batch to a Solr Input Document.
   */
  protected void uploadBatchToCollection(Tuple doc) throws IOException {
    charArr.reset();
    //        doc.toMap()
    //        Map<String, Object> m =doc.toMap(<String, Object>)
    //        doc.forEach(
    //                (s, field) -> {
    //                    if (s.equals("_version_") || s.equals("_roor_")) return;
    //                    if (field instanceof List) {
    //                        if (((List<?>) field).size() == 1) {
    //                            field = ((List<?>) field).get(0);
    //                        }
    //                    }
    //                    field = constructDateStr(field);
    //                    if (field instanceof List) {
    //                        List<?> list = (List<?>) field;
    //                        if (hasdate(list)) {
    //                            ArrayList<Object> listCopy = new ArrayList<>(list.size());
    //                            for (Object o : list) listCopy.add(constructDateStr(o));
    //                            field = listCopy;
    //                        }
    //                    }
    //                    m.put(s, field);
    //                });
    // jsonWriter.write(m);
    jsonWriter.write(doc);
    writer.write(charArr.getArray(), charArr.getStart(), charArr.getEnd());
    writer.append('\n');
  }

  private Tuple createBatchSummaryTuple(int batchSize) {
    assert batchSize > 0;
    Tuple tuple = new Tuple();
    this.totalDocsIndex += batchSize;
    ++batchNumber;
    tuple.put(BATCH_LOGGED_FIELD_NAME, batchSize);
    tuple.put("totalIndexed", this.totalDocsIndex);
    tuple.put("batchNumber", batchNumber);
    // if (coreName != null) {
    //     tuple.put("worker", coreName);
    // }
    return tuple;
  }

  private String stripSurroundingQuotesIfTheyExist(String value) {
    if (value.length() < 2) return value;
    if ((value.startsWith("\"") && value.endsWith("\""))
        || (value.startsWith("'") && value.endsWith("'"))) {
      return value.substring(1, value.length() - 1);
    }

    return value;
  }
}
