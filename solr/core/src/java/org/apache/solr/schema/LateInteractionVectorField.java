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
package org.apache.solr.schema;

import static java.util.Optional.ofNullable;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.lucene.document.LateInteractionField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.LateInteractionFloatValuesSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.StrParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.uninverting.UninvertingReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** nocommit: jdocs */
public class LateInteractionVectorField extends FieldType {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String VECTOR_DIMENSION = "vectorDimension";
  public static final String SIMILARITY_FUNCTION = "similarityFunction";
  public static final VectorSimilarityFunction DEFAULT_SIMILARITY =
      VectorSimilarityFunction.EUCLIDEAN;

  private static final int MUST_BE_TRUE = DOC_VALUES;
  private static final int MUST_BE_FALSE = MULTIVALUED | TOKENIZED | INDEXED | UNINVERTIBLE;

  private static String MUST_BE_TRUE_MSG =
      " fields require these properties to be true: " + propertiesToString(MUST_BE_TRUE);
  private static String MUST_BE_FALSE_MSG =
      " fields require these properties to be false: " + propertiesToString(MUST_BE_FALSE);

  private int dimension;
  private VectorSimilarityFunction similarityFunction;

  // nocommit: pre-emptively add ScoreFunction opt?
  // nocommit: if we don't add it now, write a test to fail if/when new options added to
  // ScoreFunction enum

  public LateInteractionVectorField() {
    super();
  }

  @Override
  public void init(IndexSchema schema, Map<String, String> args) {
    this.dimension =
        ofNullable(args.get(VECTOR_DIMENSION))
            .map(Integer::parseInt)
            .orElseThrow(
                () ->
                    new SolrException(
                        SolrException.ErrorCode.SERVER_ERROR,
                        VECTOR_DIMENSION + " is a mandatory parameter"));
    args.remove(VECTOR_DIMENSION);

    try {
      this.similarityFunction =
          ofNullable(args.get(SIMILARITY_FUNCTION))
              .map(value -> VectorSimilarityFunction.valueOf(value.toUpperCase(Locale.ROOT)))
              .orElse(DEFAULT_SIMILARITY);
    } catch (IllegalArgumentException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          SIMILARITY_FUNCTION + " not recognized: " + args.get(SIMILARITY_FUNCTION));
    }
    args.remove(SIMILARITY_FUNCTION);

    // By the time this method is called, FieldType.setArgs has already set "typical" defaults,
    // and parsed the users explicit options.
    // We need to override those defaults, and error if the user asked for nonesense

    this.properties |= MUST_BE_TRUE;
    this.properties &= ~MUST_BE_FALSE;
    if (on(trueProperties, MUST_BE_FALSE)) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, getClass().getSimpleName() + MUST_BE_FALSE_MSG);
    }
    if (on(falseProperties, MUST_BE_TRUE)) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, getClass().getSimpleName() + MUST_BE_TRUE_MSG);
    }

    super.init(schema, args);
  }

  public int getDimension() {
    return dimension;
  }

  public VectorSimilarityFunction getSimilarityFunction() {
    return similarityFunction;
  }

  public DoubleValuesSource getMultiVecSimilarityValueSource(
      final SchemaField f, final String vecStr) throws SyntaxError {
    // nocommit: use ScoreFunction here if we add it
    return new LateInteractionFloatValuesSource(
        f.getName(), stringToMultiFloatVector(dimension, vecStr), getSimilarityFunction());
  }

  @Override
  protected void checkSupportsDocValues() {
    // No-Op: always supported
  }

  @Override
  protected boolean enableDocValuesByDefault() {
    return true;
  }

  @Override
  public void checkSchemaField(final SchemaField field) throws SolrException {
    super.checkSchemaField(field);
    if (field.multiValued()) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          getClass().getSimpleName() + " fields can not be multiValued: " + field.getName());
    }
    if (field.indexed()) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          getClass().getSimpleName() + " fields can not be indexed: " + field.getName());
    }

    if (!field.hasDocValues()) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          getClass().getSimpleName() + " fields must have docValues: " + field.getName());
    }
  }

  /** Not supported: We override createFields. so this should never be called */
  @Override
  public IndexableField createField(SchemaField field, Object value) {
    throw new IllegalStateException("This method should never be called in expected operation");
  }

  @Override
  public List<IndexableField> createFields(SchemaField field, Object value) {
    try {
      final ArrayList<IndexableField> fields = new ArrayList<>(2);

      if (!CharSequence.class.isInstance(value)) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            getClass().getSimpleName() + " fields require string input: " + field.getName());
      }
      final String valueString = value.toString();

      final float[][] multiVec = stringToMultiFloatVector(dimension, valueString);
      fields.add(new LateInteractionField(field.getName(), multiVec));

      if (field.stored()) {
        fields.add(new StoredField(field.getName(), valueString));
      }

      return fields;
    } catch (SyntaxError | RuntimeException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Error while creating field '" + field + "' from value '" + value + "'",
          e);
    }
  }

  // nocommit: 1/2 public methods that refer to float[][] explicitly
  // nocommit: maybe refactor into an abstraction in case lucene supports byte/int/etc later?
  /**
   * nocommit: jdocs, note input must not be null, dimension must be positive
   *
   * @lucene.experimental
   */
  public static float[][] stringToMultiFloatVector(final int dimension, final String input)
      throws SyntaxError {

    assert 0 < dimension;
    final int lastIndex = dimension - 1;

    final List<float[]> result = new ArrayList<>(7);
    final StrParser sp = new StrParser(input);
    sp.expect("["); // outer array

    while (sp.pos < sp.end) {
      sp.expect("[");
      final float[] entry = new float[dimension];
      for (int i = 0; i < dimension; i++) {
        final int preFloatPos = sp.pos;
        try {
          entry[i] = sp.getFloat();
        } catch (NumberFormatException e) {
          throw new SyntaxError(
              "Expected float at position " + preFloatPos + " in '" + input + "'", e);
        }
        if (i < lastIndex) {
          sp.expect(",");
        }
      }

      sp.expect("]");
      result.add(entry);

      if (',' != sp.peek()) {
        // no more entries in outer array
        break;
      }
      sp.expect(",");
    }
    sp.expect("]"); // outer array

    sp.eatws();
    if (sp.pos < sp.end) {
      throw new SyntaxError("Unexpected text at position " + sp.pos + " in '" + input + "'");
    }
    return result.toArray(new float[result.size()][]);
  }

  // nocommit: 1/2 public methods that refer to float[][] explicitly
  // nocommit: maybe refactor into an abstraction in case lucene supports byte/int/etc later?
  /**
   * nocommit: jdocs, note input must not be null(s), dimensions must be positive
   *
   * @lucene.experimental
   */
  public static String multiFloatVectorToString(final float[][] input) {
    assert null != input && 0 < input.length;
    final StringBuilder out =
        new StringBuilder(input.length * 89 /* prime, smallish, ~4 verbose floats */);
    out.append("[");
    for (int i = 0; i < input.length; i++) {
      final float[] currentVec = input[i];
      assert 0 < currentVec.length;
      out.append("[");
      for (int x = 0; x < currentVec.length; x++) {
        out.append(currentVec[x]);
        out.append(",");
      }
      out.replace(out.length() - 1, out.length(), "]");
      out.append(",");
    }
    out.replace(out.length() - 1, out.length(), "]");
    return out.toString();
  }

  @Override
  public String toExternal(IndexableField f) {
    String val = f.stringValue();
    if (val == null) {
      val = multiFloatVectorToString(LateInteractionField.decode(f.binaryValue()));
    }
    return val;
  }

  @Override
  public UninvertingReader.Type getUninversionType(SchemaField sf) {
    return null;
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    writer.writeStr(name, toExternal(f), false);
  }
  
  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    return multiFloatVectorToString(LateInteractionField.decode(term));
  }

  /** Not supported */
  @Override
  public Query getPrefixQuery(QParser parser, SchemaField sf, String termStr) {
    throw new SolrException(
        SolrException.ErrorCode.BAD_REQUEST,
        getClass().getSimpleName() + " not supported for prefix queries.");
  }

  /** Not supported */
  @Override
  public ValueSource getValueSource(SchemaField field, QParser parser) {
    throw new SolrException(
        SolrException.ErrorCode.BAD_REQUEST,
        getClass().getSimpleName() + " not supported for function queries.");
  }

  /** Not supported */
  @Override
  public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
    throw new SolrException(
        SolrException.ErrorCode.BAD_REQUEST,
        "nocommit: better error msgs citing value source parser once it exists");
  }

  /** Not Supported */
  @Override
  public Query getRangeQuery(
      QParser parser,
      SchemaField field,
      String part1,
      String part2,
      boolean minInclusive,
      boolean maxInclusive) {
    throw new SolrException(
        SolrException.ErrorCode.BAD_REQUEST,
        getClass().getSimpleName() + " not supported for range queries.");
  }

  /** Not Supported */
  @Override
  public Query getSetQuery(QParser parser, SchemaField field, Collection<String> externalVals) {
    throw new SolrException(
        SolrException.ErrorCode.BAD_REQUEST,
        getClass().getSimpleName() + " not supported for set queries.");
  }

  /** Not Supported */
  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    throw new SolrException(
        SolrException.ErrorCode.BAD_REQUEST,
        getClass().getSimpleName() + " not supported for sorting.");
  }
}
