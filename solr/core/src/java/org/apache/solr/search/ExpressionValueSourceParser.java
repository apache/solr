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

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.expressions.Bindings;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;

/**
 * A ValueSource parser configured with a pre-compiled expression that can then be evaluated at
 * request time. It's powered by the Lucene Expressions module, which is a subset of JavaScript.
 */
public class ExpressionValueSourceParser extends ValueSourceParser {

  public static final String SCORE_KEY = "score-name"; // TODO get rid of this?  Why have it?
  public static final String EXPRESSION_KEY = "expression";

  private Expression expression;
  private String scoreKey;
  private int numPositionalArgs = 0; // Number of positional arguments in the expression

  @Override
  public void init(NamedList<?> args) {
    initConfiguredExpression(args);
    initScoreKey(args);
    super.init(args);
  }

  /** Checks for optional scoreKey override */
  private void initScoreKey(NamedList<?> args) {
    scoreKey = Optional.ofNullable((String) args.remove(SCORE_KEY)).orElse(SolrReturnFields.SCORE);
  }

  /** Parses the pre-configured expression */
  private void initConfiguredExpression(NamedList<?> args) {
    String expressionStr =
        Optional.ofNullable((String) args.remove(EXPRESSION_KEY))
            .orElseThrow(
                () ->
                    new SolrException(
                        SERVER_ERROR, EXPRESSION_KEY + " must be configured with an expression"));

    // Find the highest positional argument in the expression
    Pattern pattern = Pattern.compile("\\$(\\d+)");
    Matcher matcher = pattern.matcher(expressionStr);
    while (matcher.find()) {
      int argNum = Integer.parseInt(matcher.group(1));
      numPositionalArgs = Math.max(numPositionalArgs, argNum);
    }

    // TODO add way to register additional functions
    try {
      this.expression = JavascriptCompiler.compile(expressionStr);
    } catch (ParseException e) {
      throw new SolrException(
          SERVER_ERROR, "Unable to parse javascript expression: " + expressionStr, e);
    }
  }

  // TODO: support dynamic expressions:  expr("foo * bar / 32")  ??

  @Override
  public ValueSource parse(FunctionQParser fp) throws SyntaxError {
    assert null != fp;

    // Parse positional arguments if any
    List<DoubleValuesSource> positionalArgs = new ArrayList<>();
    for (int i = 0; i < numPositionalArgs; i++) {
      ValueSource vs = fp.parseValueSource();
      positionalArgs.add(vs.asDoubleValuesSource());
    }

    IndexSchema schema = fp.getReq().getSchema();
    SolrBindings b = new SolrBindings(scoreKey, schema, positionalArgs);
    return ValueSource.fromDoubleValuesSource(expression.getDoubleValuesSource(b));
  }

  /**
   * A bindings class that uses schema fields to resolve variables.
   *
   * @lucene.internal
   */
  public static class SolrBindings extends Bindings {
    private final String scoreKey;
    private final IndexSchema schema;
    private final List<DoubleValuesSource> positionalArgs;

    /**
     * @param scoreKey The binding name that should be used to represent the score, may be null
     * @param schema IndexSchema for field bindings
     * @param positionalArgs List of positional arguments
     */
    public SolrBindings(
        String scoreKey, IndexSchema schema, List<DoubleValuesSource> positionalArgs) {
      this.scoreKey = scoreKey;
      this.schema = schema;
      this.positionalArgs = positionalArgs != null ? positionalArgs : new ArrayList<>();
    }

    @Override
    public DoubleValuesSource getDoubleValuesSource(String key) {
      assert null != key;

      if (Objects.equals(scoreKey, key)) {
        return DoubleValuesSource.SCORES;
      }

      // Check for positional arguments like $1, $2, etc.
      if (key.startsWith("$")) {
        try {
          int position = Integer.parseInt(key.substring(1));
          return positionalArgs.get(position - 1); // Convert to 0-based index
        } catch (RuntimeException e) {
          throw new IllegalArgumentException("Not a valid positional argument: " + key, e);
        }
      }

      SchemaField field = schema.getFieldOrNull(key);
      if (null != field) {
        return field.getType().getValueSource(field, null).asDoubleValuesSource();
      }

      throw new IllegalArgumentException("No binding or schema field for key: " + key);
    }
  }
}
