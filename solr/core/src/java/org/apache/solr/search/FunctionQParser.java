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
package org.apache.solr.search;

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.ConstKnnByteVectorValueSource;
import org.apache.lucene.queries.function.valuesource.ConstKnnFloatValueSource;
import org.apache.lucene.queries.function.valuesource.ConstValueSource;
import org.apache.lucene.queries.function.valuesource.DoubleConstValueSource;
import org.apache.lucene.queries.function.valuesource.LiteralValueSource;
import org.apache.lucene.queries.function.valuesource.VectorValueSource;
import org.apache.lucene.search.Query;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.facet.AggValueSource;
import org.apache.solr.search.function.FieldNameValueSource;

/**
 * Does "function query" parsing of function-call like strings, producing a {@link ValueSource}. As
 * this implements {@link QParser}, we produce a {@link Query}, but more often {@link
 * #parseAsValueSource()} is called instead.
 *
 * @see ValueSourceParser
 */
public class FunctionQParser extends QParser {

  public static final int FLAG_CONSUME_DELIMITER = 0x01; // consume delimiter after parsing arg
  public static final int FLAG_IS_AGG = 0x02;
  // When a field name is encountered, use the placeholder FieldNameValueSource instead of resolving
  // to a real ValueSource
  public static final int FLAG_USE_FIELDNAME_SOURCE = 0x04;

  // When the flag is set, vector parsing use byte encoding, otherwise float encoding is used
  public static final int FLAG_PARSE_VECTOR_BYTE_ENCODING = 0x08;
  public static final int FLAG_DEFAULT = FLAG_CONSUME_DELIMITER;

  /**
   * @lucene.internal
   */
  public StrParser sp;

  @Deprecated private boolean parseMultipleSources = false;
  private boolean parseToEnd = true;

  public FunctionQParser(
      String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
    setFlags(FLAG_DEFAULT);
    setString(qstr);
    if (localParams != null && localParams.getPrimitiveBool("multiple")) {
      setParseMultipleSources(true);
    }
  }

  /**
   * Parses the string to a {@link ValueSource}. Typically, this is not used, however.
   *
   * @see QParser#parseAsValueSource()
   */
  public static ValueSource parseAsValueSource(String string, SolrQueryRequest request)
      throws SyntaxError {
    return getParser(string, FunctionQParserPlugin.NAME, request).parseAsValueSource();
  }

  @Override
  public void setString(String s) {
    super.setString(s);
    if (s != null) {
      sp = new StrParser(s);
    }
  }

  @Deprecated
  public void setParseMultipleSources(boolean parseMultipleSources) {
    this.parseMultipleSources = parseMultipleSources;
  }

  /**
   * Parse multiple comma separated value sources encapsulated into a {@link VectorValueSource} when
   * {@link #getQuery()} or {@link #parseAsValueSource()} is called.
   *
   * @deprecated this is only needed for an unusual use-case and seems hard to support
   */
  @Deprecated
  public boolean getParseMultipleSources() {
    return parseMultipleSources;
  }

  public void setParseToEnd(boolean parseToEnd) {
    this.parseToEnd = parseToEnd;
  }

  /** throw exception if there is extra stuff at the end of the parsed valuesource(s). */
  public boolean getParseToEnd() {
    return parseToEnd;
  }

  @Override
  public Query parse() throws SyntaxError {
    return new FunctionQuery(parseAsValueSource());
  }

  /**
   * Parses as a ValueSource, not a Query. <em>NOT</em> intended to be called by {@link
   * ValueSourceParser#parse(FunctionQParser)}; it's intended for general code that has a {@link
   * QParser} but actually wants to parse a ValueSource.
   *
   * @return A {@link VectorValueSource} for multiple VS, otherwise just the single VS.
   */
  @Override
  public ValueSource parseAsValueSource() throws SyntaxError {
    ValueSource vs = null;
    List<ValueSource> lst = null;

    for (; ; ) {
      ValueSource valsource = parseValueSource(getFlags() & ~FLAG_CONSUME_DELIMITER);
      sp.eatws();
      if (!parseMultipleSources) {
        vs = valsource;
        break;
      } else {
        if (lst != null) {
          lst.add(valsource);
        } else {
          vs = valsource;
        }
      }

      // check if there is a "," separator
      if (sp.peek() != ',') break;

      consumeArgumentDelimiter();

      if (lst == null) {
        lst = new ArrayList<>(2);
        lst.add(valsource);
      }
    }

    if (parseToEnd && sp.pos < sp.end) {
      throw new SyntaxError("Unexpected text after function: " + sp.val.substring(sp.pos, sp.end));
    }

    if (lst != null) {
      vs = new VectorValueSource(lst);
    }
    return vs;
  }

  /**
   * Are there more arguments in the argument list being parsed?
   *
   * @return whether more args exist
   */
  public boolean hasMoreArguments() throws SyntaxError {
    int ch = sp.peek();
    /* determine whether the function is ending with a paren or end of str */
    return (!(ch == 0 || ch == ')'));
  }

  /*
   * TODO: Doc
   */
  public String parseId() throws SyntaxError {
    String value = parseArg();
    if (argWasQuoted()) {
      throw new SyntaxError("Expected identifier instead of quoted string:" + value);
    } else if (value == null) {
      throw new SyntaxError("Expected identifier instead of 'null' for function " + sp);
    }
    return value;
  }

  /**
   * Parse a float.
   *
   * @return Float
   */
  public Float parseFloat() throws SyntaxError {
    String str = parseArg();
    if (argWasQuoted()) throw new SyntaxError("Expected float instead of quoted string:" + str);
    try {
      return Float.parseFloat(str);
    } catch (NumberFormatException | NullPointerException e) {
      throw new SyntaxError("Expected float instead of '" + str + "' for function " + sp);
    }
  }

  /**
   * Parse a Double
   *
   * @return double
   */
  public double parseDouble() throws SyntaxError {
    String str = parseArg();
    if (argWasQuoted()) throw new SyntaxError("Expected double instead of quoted string:" + str);
    try {
      return Double.parseDouble(str);
    } catch (NumberFormatException | NullPointerException e) {
      throw new SyntaxError("Expected double instead of '" + str + "' for function " + sp);
    }
  }

  /**
   * Parse an integer
   *
   * @return An int
   */
  public int parseInt() throws SyntaxError {
    String str = parseArg();
    if (argWasQuoted()) throw new SyntaxError("Expected integer instead of quoted string:" + str);
    try {
      return Integer.parseInt(str);
    } catch (NumberFormatException | NullPointerException e) {
      throw new SyntaxError("Expected integer instead of '" + str + "' for function " + sp);
    }
  }

  private boolean argWasQuoted;

  public boolean argWasQuoted() {
    return argWasQuoted;
  }

  public String parseArg() throws SyntaxError {
    argWasQuoted = false;

    sp.eatws();
    char ch = sp.peek();
    String val = null;
    switch (ch) {
      case ')':
        return null;
      case '$':
        sp.pos++;
        String param = sp.getId();
        val = getParam(param);
        break;
      case '\'':
      case '"':
        val = sp.getQuotedString();
        argWasQuoted = true;
        break;
      default:
        // read unquoted literal ended by whitespace ',' or ')'
        // there is no escaping.
        int valStart = sp.pos;
        for (; ; ) {
          if (sp.pos >= sp.end) {
            throw new SyntaxError(
                "Missing end to unquoted value starting at " + valStart + " str='" + sp.val + "'");
          }
          char c = sp.val.charAt(sp.pos);
          if (c == ')' || c == ',' || Character.isWhitespace(c)) {
            val = sp.val.substring(valStart, sp.pos);
            break;
          }
          sp.pos++;
        }
    }

    sp.eatws();
    consumeArgumentDelimiter();
    return val;
  }

  public List<Number> parseVector(VectorEncoding encoding) throws SyntaxError {
    ArrayList<Number> values = new ArrayList<>();
    char initChar = sp.val.charAt(sp.pos);
    if (initChar != '[') {
      throw new SyntaxError("Missing parenthesis at the beginning of vector ");
    }
    sp.pos += 1;
    boolean valueExpected = true;
    while (sp.pos < sp.end) {
      char ch = sp.val.charAt(sp.pos);
      if (Character.isWhitespace(ch)) {
        sp.pos++;
      } else if ((ch >= '0' && ch <= '9') || ch == '.' || ch == '+' || ch == '-') {
        switch (encoding) {
          case BYTE:
            values.add(sp.getByte());
            break;
          case FLOAT32:
            values.add(sp.getFloat());
            break;
          default:
            throw new SyntaxError("Unexpected vector encoding: " + encoding);
        }
        valueExpected = false;
      } else if (ch == ',') {
        if (valueExpected) {
          throw new SyntaxError("Unexpected vector encoding: " + encoding);
        }
        sp.pos++;
        valueExpected = true;
      } else if (ch == ']' && !valueExpected) {
        break;
      } else {
        throw new SyntaxError("Unexpected " + ch + " at position " + sp.pos);
      }
    }
    if (sp.pos >= sp.end) {
      throw new SyntaxError("Missing parenthesis at the end of vector");
    }
    sp.pos++;
    return values;
  }

  /**
   * Parse a list of ValueSource. Must be the final set of arguments to a ValueSource.
   *
   * @return List&lt;ValueSource&gt;
   */
  public List<ValueSource> parseValueSourceList() throws SyntaxError {
    return parseValueSourceList(getFlags() | FLAG_CONSUME_DELIMITER);
  }

  /**
   * Parse a list of ValueSource. Must be the final set of arguments to a ValueSource.
   *
   * @param flags - customize parsing behavior
   * @return List&lt;ValueSource&gt;
   */
  public List<ValueSource> parseValueSourceList(int flags) throws SyntaxError {
    List<ValueSource> sources = new ArrayList<>(3);
    while (hasMoreArguments()) {
      sources.add(parseValueSource(flags));
    }
    return sources;
  }

  /** Parse an individual ValueSource. */
  public ValueSource parseValueSource() throws SyntaxError {
    /* consume the delimiter afterward for an external call to parseValueSource */
    return parseValueSource(getFlags() | FLAG_CONSUME_DELIMITER);
  }

  /*
   * TODO: Doc
   */
  public Query parseNestedQuery() throws SyntaxError {
    Query nestedQuery;

    if (sp.opt("$")) {
      String param = sp.getId();
      String qstr = getParam(param);
      qstr = qstr == null ? "" : qstr;
      nestedQuery = subQuery(qstr, null).getQuery();

      // nestedQuery would be null when de-referenced query value is not specified
      // Ex: query($qq) in request with no qq param specified
      if (nestedQuery == null) {
        throw new SyntaxError(
            "Missing param " + param + " while parsing function '" + sp.val + "'");
      }
    } else {
      int start = sp.pos;
      String v = sp.val;

      String qs = v;
      ModifiableSolrParams nestedLocalParams = new ModifiableSolrParams();
      int end = QueryParsing.parseLocalParams(qs, start, nestedLocalParams, getParams());

      QParser sub;

      if (end > start) {
        if (nestedLocalParams.get(QueryParsing.V) != null) {
          // value specified directly in local params... so the end of the
          // query should be the end of the local params.
          sub = subQuery(qs.substring(start, end), null);
        } else {
          // value here is *after* the local params... ask the parser.
          sub = subQuery(qs, null);
          // int subEnd = sub.findEnd(')');
          // TODO.. implement functions to find the end of a nested query
          throw new SyntaxError(
              "Nested local params must have value in v parameter.  got '" + qs + "'");
        }
      } else {
        throw new SyntaxError(
            "Nested function query must use $param or {!v=value} forms. got '" + qs + "'");
      }

      sp.pos += end - start; // advance past nested query
      nestedQuery = sub.getQuery();
      // handling null check on nestedQuery separately, so that proper error can be returned
      // one case this would be possible when v is specified but v's value is empty or has only
      // spaces
      if (nestedQuery == null) {
        throw new SyntaxError("Nested function query returned null for '" + sp.val + "'");
      }
    }
    consumeArgumentDelimiter();

    return nestedQuery;
  }

  /**
   * Parse an individual value source.
   *
   * @param doConsumeDelimiter whether to consume a delimiter following the ValueSource
   */
  protected ValueSource parseValueSource(boolean doConsumeDelimiter) throws SyntaxError {
    return parseValueSource(
        doConsumeDelimiter
            ? (getFlags() | FLAG_CONSUME_DELIMITER)
            : (getFlags() & ~FLAG_CONSUME_DELIMITER));
  }

  protected ValueSource parseValueSource(int flags) throws SyntaxError {
    ValueSource valueSource;

    int ch = sp.peek();
    if ((ch >= '0' && ch <= '9') || ch == '.' || ch == '+' || ch == '-') {
      Number num = sp.getNumber();
      if (num instanceof Long) {
        valueSource = new ValueSourceParser.LongConstValueSource(num.longValue());
      } else if (num instanceof Double) {
        valueSource = new DoubleConstValueSource(num.doubleValue());
      } else {
        // shouldn't happen
        valueSource = new ConstValueSource(num.floatValue());
      }
    } else if (ch == '"' || ch == '\'') {
      valueSource = new LiteralValueSource(sp.getQuotedString());
    } else if (ch == '[') {
      valueSource = parseConstVector(flags);
    } else if (ch == '$') {
      sp.pos++;
      String param = sp.getId();
      String val = getParam(param);
      if (val == null) {
        throw new SyntaxError(
            "Missing param " + param + " while parsing function '" + sp.val + "'");
      }

      if ((flags & FLAG_USE_FIELDNAME_SOURCE) != 0 && req.getSchema().getFieldOrNull(val) != null) {
        // Don't try to create a ValueSource for the field, just use a placeholder.
        // this handles the case like x=myfunc($qq)&qq=something
        valueSource = new FieldNameValueSource(val);
      } else {
        QParser subParser = subQuery(val, "func");
        if (subParser instanceof FunctionQParser subFunc) {
          subFunc.setParseMultipleSources(true);
          subFunc.setFlags(flags);
        }
        valueSource = subParser.parseAsValueSource();
      }

      /*
      // dereference *simple* argument (i.e., can't currently be a function)
       // In the future we could support full function dereferencing via a stack of ValueSource (or StringParser) objects
      ch = val.length()==0 ? '\0' : val.charAt(0);

      if (ch>='0' && ch<='9'  || ch=='.' || ch=='+' || ch=='-') {
        StrParser sp = new StrParser(val);
        Number num = sp.getNumber();
        if (num instanceof Long) {
          valueSource = new LongConstValueSource(num.longValue());
        } else if (num instanceof Double) {
          valueSource = new DoubleConstValueSource(num.doubleValue());
        } else {
          // shouldn't happen
          valueSource = new ConstValueSource(num.floatValue());
        }
      } else if (ch == '"' || ch == '\'') {
        StrParser sp = new StrParser(val);
        val = sp.getQuotedString();
        valueSource = new LiteralValueSource(val);
      } else {
        if (val.length()==0) {
          valueSource = new LiteralValueSource(val);
        } else {
          String id = val;
          SchemaField f = req.getSchema().getField(id);
          valueSource = f.getType().getValueSource(f, this);
        }
      }
      */

    } else {

      String id = sp.getId();
      if (sp.opt("(")) {
        // a function... look it up.
        ValueSourceParser argParser = req.getCore().getValueSourceParser(id);
        if (argParser == null) {
          throw new SyntaxError("Unknown function " + id + " in FunctionQuery(" + sp + ")");
        }
        valueSource = argParser.parse(this);
        sp.expect(")");
      } else {
        if ("true".equals(id)) {
          valueSource = ValueSourceParser.BoolConstValueSource.TRUE;
        } else if ("false".equals(id)) {
          valueSource = ValueSourceParser.BoolConstValueSource.FALSE;
        } else {
          if ((flags & FLAG_USE_FIELDNAME_SOURCE) != 0) {
            // Don't try to create a ValueSource for the field, just use a placeholder.
            valueSource = new FieldNameValueSource(id);
          } else {
            SchemaField f = req.getSchema().getField(id);
            valueSource = f.getType().getValueSource(f, this);
          }
        }
      }
    }

    if ((flags & FLAG_CONSUME_DELIMITER) != 0) {
      consumeArgumentDelimiter();
    }

    return valueSource;
  }

  public ValueSource parseConstVector(int flags) throws SyntaxError {

    VectorEncoding encoding =
        (flags & FLAG_PARSE_VECTOR_BYTE_ENCODING) != 0
            ? VectorEncoding.BYTE
            : VectorEncoding.FLOAT32;
    var vector = parseVector(encoding);

    switch (encoding) {
      case BYTE:
        byte[] byteVector = new byte[vector.size()];
        for (int i = 0; i < vector.size(); ++i) {
          byteVector[i] = vector.get(i).byteValue();
        }
        return new ConstKnnByteVectorValueSource(byteVector);
      case FLOAT32:
        float[] floatVector = new float[vector.size()];
        for (int i = 0; i < vector.size(); ++i) {
          floatVector[i] = vector.get(i).floatValue();
        }
        return new ConstKnnFloatValueSource(floatVector);
    }

    throw new SyntaxError("wrong vector encoding:" + encoding);
  }

  /**
   * @lucene.experimental
   */
  public AggValueSource parseAgg(int flags) throws SyntaxError {
    String origId = sp.getId();
    AggValueSource vs = null;
    boolean hasParen;

    if ("agg".equals(origId)) {
      hasParen = sp.opt("(");
      vs = parseAgg(flags | FLAG_IS_AGG);
    } else {
      // parse as an aggregation...
      String id = origId.startsWith("agg_") ? origId : "agg_" + origId;
      hasParen = sp.opt("(");

      ValueSourceParser argParser = req.getCore().getValueSourceParser(id);
      if (argParser == null) {
        argParser = req.getCore().getValueSourceParser(origId);
        if (argParser == null) {
          throw new SyntaxError("Unknown aggregation '" + origId + "' in input (" + sp + ")");
        } else {
          throw new SyntaxError(
              "Expected multi-doc aggregation from '"
                  + origId
                  + "' but got per-doc function in input ("
                  + sp
                  + ")");
        }
      }

      ValueSource vv = argParser.parse(this);
      if (!(vv instanceof AggValueSource)) {
        throw new SyntaxError(
            "Expected multi-doc aggregation from '"
                + origId
                + "' but got ("
                + vv
                + ") in ("
                + sp
                + ")");
      }
      vs = (AggValueSource) vv;
    }

    if (hasParen) {
      sp.expect(")");
    }

    if ((flags & FLAG_CONSUME_DELIMITER) != 0) {
      consumeArgumentDelimiter();
    }

    return vs;
  }

  /**
   * Consume an argument delimiter (a comma) from the token stream. Only consumes if more arguments
   * should exist (no ending parens or end of string).
   *
   * @return whether a delimiter was consumed
   */
  protected boolean consumeArgumentDelimiter() throws SyntaxError {
    /* if a list of args is ending, don't expect the comma */
    if (hasMoreArguments()) {
      sp.expect(",");
      return true;
    }

    return false;
  }
}
