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
package org.apache.solr.client.solrj.io.stream.expr;

import java.util.Objects;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;

/** Provides a named parameter */
public class StreamExpressionNamedParameter implements StreamExpressionParameter {
  private String name;
  private StreamExpressionParameter parameter;

  public StreamExpressionNamedParameter(String name) {
    this.name = name;
  }

  public StreamExpressionNamedParameter(String name, String parameter) {
    this.name = name;
    setParameter(parameter);
  }

  public StreamExpressionNamedParameter(String name, StreamExpressionParameter parameter) {
    this.name = name;
    setParameter(parameter);
  }

  public String getName() {
    return this.name;
  }

  public void setName(String name) {
    if (null == name || 0 == name.length()) {
      throw new IllegalArgumentException("Null or empty name is not allowed is not allowed.");
    }

    this.name = name;
  }

  public StreamExpressionParameter getParameter() {
    return this.parameter;
  }

  public void setParameter(StreamExpressionParameter parameter) {
    this.parameter = parameter;
  }

  public StreamExpressionNamedParameter withParameter(StreamExpressionParameter parameter) {
    setParameter(parameter);
    return this;
  }

  public void setParameter(String parameter) {
    this.parameter = new StreamExpressionValue(parameter);
  }

  public StreamExpressionNamedParameter withParameter(String parameter) {
    setParameter(parameter);
    return this;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(name);
    sb.append("=");

    // check if we require quoting
    boolean requiresQuote = false;
    if (parameter instanceof StreamExpressionValue) {
      String value = ((StreamExpressionValue) parameter).getValue();
      requiresQuote = !StreamExpressionParser.wordToken(value);
    }

    if (requiresQuote) {
      sb.append("\"");
      sb.append(
          ESCAPE_QUOTES
              .matcher(parameter.toString())
              .replaceAll(StreamExpressionNamedParameter::escapeQuoteMatch));
      sb.append("\"");
    } else {
      sb.append(parameter.toString());
    }

    return sb.toString();
  }

  /**
   * single literal doublequote, possibly already escaped by a preceding single literal backslash
   */
  private static final Pattern ESCAPE_QUOTES = Pattern.compile("(\\\\)?\"");

  private static String escapeQuoteMatch(MatchResult m) {
    if (m.start(1) == -1) {
      // the quote was _not_ already escaped, so replace it with a simple escaped quote
      // (single literal doublequote preceded by a single literal backslash), for a
      // final result: `\"`.
      return "\\\\\"";
    } else {
      // the quote was already escaped, so escape it at the _Solr syntax_ level by
      // preceding the existing `\"` with an extra literal backslash `\\`, for final
      // result: `\\\"`.
      return "\\\\\\\\\\\\\"";
    }
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof StreamExpressionNamedParameter)) {
      return false;
    }

    StreamExpressionNamedParameter check = (StreamExpressionNamedParameter) other;

    if (null == this.name && null != check.name) {
      return false;
    }
    if (null != this.name && null == check.name) {
      return false;
    }

    if (null != this.name && !this.name.equals(check.name)) {
      return false;
    }

    return this.parameter.equals(check.parameter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }
}
