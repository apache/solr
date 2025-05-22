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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
 * A ValueSource parser that can be configured with (named) pre-complied Expressions that can then
 * be evaluated at request time.
 */
public class ExpressionValueSourceParser extends ValueSourceParser {

  // TODO: more docs, example configs, example usage

  public static final String SCORE_KEY = "score-name";
  public static final String EXPRESSIONS_KEY = "expressions";

  private Map<String, Expression> expressions;
  private String scoreKey = SolrReturnFields.SCORE;

  @Override
  public void init(NamedList<?> args) {
    initConfiguredExpression(args);
    initScoreKey(args);
    super.init(args);
  }

  /** Checks for optional scoreKey override */
  private void initScoreKey(NamedList<?> args) {
    final int scoreIdx = args.indexOf(SCORE_KEY, 0);
    if (scoreIdx < 0) {
      // if it's not in the list at all, use the existing default...
      return;
    }

    Object arg = args.remove(scoreIdx);
    // null is valid if they want to prevent default binding
    scoreKey = (null == arg) ? null : arg.toString();
  }

  /** Parses the pre-configured expressions */
  private void initConfiguredExpression(NamedList<?> args) {
    Object arg = args.remove(EXPRESSIONS_KEY);
    if (!(arg instanceof NamedList)) {
      // :TODO: null arg may be ok if we want to later support dynamic expressions
      throw new SolrException(
          SERVER_ERROR, EXPRESSIONS_KEY + " must be configured as a list of named expressions");
    }
    @SuppressWarnings("unchecked")
    NamedList<String> input = (NamedList<String>) arg;
    Map<String, Expression> expr = new HashMap<>();
    for (Map.Entry<String, String> item : input) {
      String key = item.getKey();
      try {
        Expression val = JavascriptCompiler.compile(item.getValue());
        expr.put(key, val);
      } catch (ParseException e) {
        throw new SolrException(
            SERVER_ERROR, "Unable to parse javascript expression: " + item.getValue(), e);
      }
    }

    // TODO: should we support mapping positional func args to names in bindings?
    //
    // ie: ...
    // <lst name="expressions">
    //   <lst name="my_expr">
    //     <str name="expression">foo * bar / baz</str>
    //     <arr name="positional-bindings">
    //       <str>baz</str>
    //       <str>bar</str>
    //     </arr>
    //   </lst>
    //   <str name="foo">32</foo>
    //   ...
    // </lst>
    //  and then:  "expr(my_expr,42,56)" == "32 * 56 / 42"

    exceptionIfCycles(expr);
    this.expressions = Collections.unmodifiableMap(expr);
  }

  @Override
  public ValueSource parse(FunctionQParser fp) throws SyntaxError {
    assert null != fp;

    String key = fp.parseArg();
    // TODO: allow function params for overriding bindings?
    // TODO: support dynamic expressions:  expr("foo * bar / 32")  ??

    IndexSchema schema = fp.getReq().getSchema();

    SolrBindings b = new SolrBindings(scoreKey, expressions, schema);
    return ValueSource.fromDoubleValuesSource(b.getDoubleValuesSource(key));
  }

  /** Validates that a Map of named expressions does not contain any cycles. */
  public static void exceptionIfCycles(Map<String, Expression> expressions) {

    // TODO: there's probably a more efficient way to do this
    // TODO: be nice to just return the shortest cycles (ie: b->a->b instead of x->a->b->a)

    List<String> cycles = new ArrayList<>(expressions.size());
    Set<String> checkedKeys = new LinkedHashSet<>();

    for (String key : expressions.keySet()) {
      Set<String> visitedKeys = new LinkedHashSet<>();
      String cycle = makeCycleErrString(key, expressions, visitedKeys, checkedKeys);
      if (null != cycle) {
        cycles.add(cycle);
      }
    }
    if (0 < cycles.size()) {
      throw new SolrException(
          SERVER_ERROR,
          "At least "
              + cycles.size()
              + " cycles detected in configured expressions: ["
              + String.join("], [", cycles)
              + "]");
    }
  }

  /**
   * Recursively checks for cycles, returning null if none found - otherwise the string is a
   * representation of the cycle found (may not be the shortest cycle) This method recursively adds
   * to visitedKeys and checkedKeys
   */
  private static String makeCycleErrString(
      String key,
      Map<String, Expression> expressions,
      Set<String> visitedKeys,
      Set<String> checkedKeys) {
    if (checkedKeys.contains(key)) {
      return null;
    }
    if (visitedKeys.contains(key)) {
      checkedKeys.add(key);
      return String.join("=>", visitedKeys) + "=>" + key;
    }
    visitedKeys.add(key);

    Expression expr = expressions.get(key);
    if (null != expr) {
      for (String var : expr.variables) {
        assert null != var;

        String err = makeCycleErrString(var, expressions, visitedKeys, checkedKeys);
        if (null != err) {
          return err;
        }
      }
    }

    checkedKeys.add(key);
    return null;
  }

  /**
   * A bindings class that recognizes pre-configured named expression and uses schema fields to
   * resolve variables that have not been configured as expressions.
   *
   * @lucene.internal
   */
  public static class SolrBindings extends Bindings {
    private final String scoreKey;
    private final Map<String, Expression> expressions;
    private final IndexSchema schema;

    /**
     * @param scoreKey The binding name that should be used to represent the score, may be null
     * @param expressions Mapping of expression names that will be consulted as the primary
     *     bindings, get() should return null if key is not bound to an expression (will be used
     *     read only, will *not* be defensively copied, must not contain cycles)
     * @param schema IndexSchema for field bindings
     */
    public SolrBindings(String scoreKey, Map<String, Expression> expressions, IndexSchema schema) {
      this.scoreKey = scoreKey;
      this.expressions = expressions;
      this.schema = schema;
      // :TODO: add support for request time bindings (function args)
    }

    @Override
    public DoubleValuesSource getDoubleValuesSource(String key) {
      assert null != key;

      if (Objects.equals(scoreKey, key)) {
        return DoubleValuesSource.SCORES;
      }

      Expression expr = expressions.get(key);
      if (null != expr) {
        try {
          return expr.getDoubleValuesSource(this);
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException(
              "Invalid binding for key '" + key + "' transative binding problem: " + e.getMessage(),
              e);
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
