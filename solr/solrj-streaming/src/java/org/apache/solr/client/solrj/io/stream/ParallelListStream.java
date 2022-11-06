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

package org.apache.solr.client.solrj.io.stream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;

public class ParallelListStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;
  private TupleStream[] streams;
  private TupleStream currentStream;
  private int streamIndex;

  public ParallelListStream(TupleStream... streams) throws IOException {
    init(streams);
  }

  public ParallelListStream(StreamExpression expression, StreamFactory factory) throws IOException {
    List<StreamExpression> streamExpressions =
        factory.getExpressionOperandsRepresentingTypes(
            expression, Expressible.class, TupleStream.class);
    TupleStream[] streams = new TupleStream[streamExpressions.size()];
    for (int idx = 0; idx < streamExpressions.size(); ++idx) {
      streams[idx] = factory.constructStream(streamExpressions.get(idx));
    }

    init(streams);
  }

  private void init(TupleStream... tupleStreams) {
    this.streams = tupleStreams;
  }

  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException {
    return toExpression(factory, true);
  }

  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams)
      throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    if (includeStreams) {
      for (TupleStream stream : streams) {
        expression.addParameter(((Expressible) stream).toExpression(factory));
      }
    }
    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    StreamExplanation explanation = new StreamExplanation(getStreamNodeId().toString());
    explanation.setFunctionName(factory.getFunctionName(this.getClass()));
    explanation.setImplementingClass(this.getClass().getName());
    explanation.setExpressionType(ExpressionType.STREAM_DECORATOR);
    explanation.setExpression(toExpression(factory, false).toString());
    for (TupleStream stream : streams) {
      explanation.addChild(stream.toExplanation(factory));
    }

    return explanation;
  }

  @Override
  public void setStreamContext(StreamContext context) {
    for (TupleStream stream : streams) {
      stream.setStreamContext(context);
    }
  }

  @Override
  public List<TupleStream> children() {
    List<TupleStream> l = new ArrayList<TupleStream>();
    for (TupleStream stream : streams) {
      l.add(stream);
    }
    return l;
  }

  @Override
  public Tuple read() throws IOException {
    while (true) {
      if (currentStream == null) {
        if (streamIndex < streams.length) {
          currentStream = streams[streamIndex];
        } else {
          return Tuple.EOF();
        }
      }

      Tuple tuple = currentStream.read();
      if (tuple.EOF) {
        currentStream.close();
        currentStream = null;
        ++streamIndex;
      } else {
        return tuple;
      }
    }
  }

  @Override
  public void close() throws IOException {}

  @Override
  public void open() throws IOException {
    openStreams();
  }

  private void openStreams() throws IOException {
    ExecutorService service =
        ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("ParallelListStream"));
    try {
      List<Future<StreamIndex>> futures = new ArrayList<>();
      int i = 0;
      for (TupleStream tupleStream : streams) {
        StreamOpener so = new StreamOpener(new StreamIndex(tupleStream, i++));
        Future<StreamIndex> future = service.submit(so);
        futures.add(future);
      }

      try {
        for (Future<StreamIndex> f : futures) {
          StreamIndex streamIndex = f.get();
          this.streams[streamIndex.getIndex()] = streamIndex.getTupleStream();
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    } finally {
      service.shutdown();
    }
  }

  protected static class StreamOpener implements Callable<StreamIndex> {

    private StreamIndex streamIndex;

    public StreamOpener(StreamIndex streamIndex) {
      this.streamIndex = streamIndex;
    }

    @Override
    public StreamIndex call() throws Exception {
      streamIndex.getTupleStream().open();
      return streamIndex;
    }
  }

  protected static class StreamIndex {
    private TupleStream tupleStream;
    private int index;

    public StreamIndex(TupleStream tupleStream, int index) {
      this.tupleStream = tupleStream;
      this.index = index;
    }

    public int getIndex() {
      return this.index;
    }

    public TupleStream getTupleStream() {
      return this.tupleStream;
    }
  }

  /** Return the stream sort - ie, the order in which records are returned */
  @Override
  public StreamComparator getStreamSort() {
    return null;
  }

  @Override
  public int getCost() {
    return 0;
  }
}
