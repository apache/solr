package org.apache.solr.client.solrj.io.stream;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.*;

import java.io.IOException;
import java.util.*;

public class MapStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  private TupleStream stream;
  private StreamContext streamContext;


  private Map<String, String> mappings;

  public MapStream(TupleStream stream, Map<String, String> mappings){
    this.stream = stream;
    this.mappings = mappings;
  }

  public MapStream(StreamExpression expression, StreamFactory factory) throws IOException {
    List<StreamExpression> streamExpressions =
            factory.getExpressionOperandsRepresentingTypes(
                    expression, Expressible.class, TupleStream.class);
    if (1 != streamExpressions.size()) {
      throw new IOException(
              String.format(
                      Locale.ROOT,
                      "Invalid expression %s - expecting single stream but found %d (must be TupleStream types)",
                      expression,
                      streamExpressions.size()));
    }

    stream = factory.constructStream(streamExpressions.get(0));

    List<StreamExpressionParameter> mapFieldExpressions =
            factory.getOperandsOfType(expression, StreamExpressionValue.class);
    if (0 == mapFieldExpressions.size()) {
      throw new IOException(
              String.format(
                      Locale.ROOT,
                      "Invalid expression %s - expecting at least one select field but found %d",
                      expression,
                      streamExpressions.size()));
    }

    mappings = new HashMap<>();
    mapFieldExpressions.forEach(
        op -> {
          String value = ((StreamExpressionValue) op).getValue().trim();
          if (value.length() > 2 && value.startsWith("\"") && value.endsWith("\"")) {
            value = value.substring(1, value.length() - 1);
          }
          String[] parts = value.split("=");
          mappings.put(parts[0].trim(), parts[1].trim());
        });
  }

  @Override
  public void setStreamContext(StreamContext context) {
    this.streamContext = context;
    this.stream.setStreamContext(context);
  }

  @Override
  public List<TupleStream> children() {
    List<TupleStream> l = new ArrayList<>();
    l.add(stream);
    return l;
  }

  @Override
  public void open() throws IOException {
    stream.open();
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }

  @Override
  public Tuple read() throws IOException {
    Tuple original = stream.read();
    if (original.EOF)
      return original;
    final Tuple workingToReturn = new Tuple();
    original.getFields().forEach((k,v) ->
            workingToReturn.put(k,mappings.containsKey(k) ? new Tuple(mappings.get(k),v) : v));
    return workingToReturn;
  }

  @Override
  public StreamComparator getStreamSort() {
    return null;
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return toExpression(factory, true);
  }

  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams)
      throws IOException {
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    if (includeStreams) {
      // stream
      if (stream instanceof Expressible) {
        expression.addParameter(((Expressible) stream).toExpression(factory));
      } else {
        throw new IOException(
            "This SelectStream contains a non-expressible TupleStream - it cannot be converted to an expression");
      }
    } else {
      expression.addParameter("<stream>");
    }
    mappings.forEach((k, v) -> expression.addParameter(k + "=" + v));

    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return new StreamExplanation(getStreamNodeId().toString())
            .withChildren(new Explanation[] {stream.toExplanation(factory)})
            .withFunctionName(factory.getFunctionName(this.getClass()))
            .withImplementingClass(this.getClass().getName())
            .withExpressionType(Explanation.ExpressionType.STREAM_DECORATOR)
            .withExpression(toExpression(factory, false).toString());
  }
}
