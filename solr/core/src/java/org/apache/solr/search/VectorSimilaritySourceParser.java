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

import static org.apache.solr.common.SolrException.ErrorCode;
import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;

import java.util.Arrays;
import java.util.Locale;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.ByteVectorSimilarityFunction;
import org.apache.lucene.queries.function.valuesource.FloatVectorSimilarityFunction;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.DenseVectorField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;

/**
 * This class provides implementation for two variants for parsing function query vectorSimilarity
 * which is used to calculate the similarity between two vectors.
 */
public class VectorSimilaritySourceParser extends ValueSourceParser {
  @Override
  public ValueSource parse(FunctionQParser fp) throws SyntaxError {

    final String arg1Str = fp.parseArg();
    if (arg1Str == null || !fp.hasMoreArguments())
      throw new SolrException(
          BAD_REQUEST, "Invalid number of arguments. Please provide either two or four arguments.");

    final String arg2Str = peekIsConstVector(fp) ? null : fp.parseArg();
    if (fp.hasMoreArguments() && arg2Str != null) {
      return handle4ArgsVariant(fp, arg1Str, arg2Str);
    }
    return handle2ArgsVariant(fp, arg1Str, arg2Str);
  }

  /**
   * returns true if and only if the next argument is a constant vector, taking into consideration
   * that the next (literal) argument may be a param reference
   */
  private boolean peekIsConstVector(final FunctionQParser fp) throws SyntaxError {
    final char rawPeek = fp.sp.peek();
    if ('[' == rawPeek) {
      return true;
    }
    if ('$' == rawPeek) {
      final int savedPos = fp.sp.pos;
      try {
        final String rawParam = fp.parseArg();
        return ((null != rawParam) && ('[' == (new StrParser(rawParam)).peek()));
      } finally {
        fp.sp.pos = savedPos;
      }
    }
    return false;
  }

  private static int buildVectorEncodingFlag(final VectorEncoding vectorEncoding) {
    return FunctionQParser.FLAG_DEFAULT
        | FunctionQParser.FLAG_CONSUME_DELIMITER
        | (vectorEncoding.equals(VectorEncoding.BYTE)
            ? FunctionQParser.FLAG_PARSE_VECTOR_BYTE_ENCODING
            : 0);
  }

  /** Expects to find args #3 and #4 (two vector ValueSources) still in the function parser */
  private ValueSource handle4ArgsVariant(FunctionQParser fp, String vecEncStr, String vecSimFuncStr)
      throws SyntaxError {
    final var vectorEncoding = enumValueOrBadRequest(VectorEncoding.class, vecEncStr);
    final var vectorSimilarityFunction =
        enumValueOrBadRequest(VectorSimilarityFunction.class, vecSimFuncStr);
    final int vectorEncodingFlag = buildVectorEncodingFlag(vectorEncoding);
    final ValueSource v1 = fp.parseValueSource(vectorEncodingFlag);
    final ValueSource v2 = fp.parseValueSource(vectorEncodingFlag);
    return createSimilarityFunction(vectorSimilarityFunction, vectorEncoding, v1, v2);
  }

  /**
   * If <code>field2Name</code> is null, then expects to find a constant vector as the only
   * remaining arg in the function parser.
   */
  private ValueSource handle2ArgsVariant(FunctionQParser fp, String field1Name, String field2Name)
      throws SyntaxError {

    final SchemaField field1 = fp.req.getSchema().getField(field1Name);
    final DenseVectorField field1Type = requireVectorType(field1);

    final var vectorEncoding = field1Type.getVectorEncoding();
    final var vectorSimilarityFunction = field1Type.getSimilarityFunction();

    final ValueSource v1 = field1Type.getValueSource(field1, fp);
    final ValueSource v2;

    if (null == field2Name) {
      final int vectorEncodingFlag = buildVectorEncodingFlag(vectorEncoding);
      v2 = fp.parseValueSource(vectorEncodingFlag);

    } else {
      final SchemaField field2 = fp.req.getSchema().getField(field2Name);
      final DenseVectorField field2Type = requireVectorType(field2);
      if (vectorEncoding != field2Type.getVectorEncoding()
          || vectorSimilarityFunction != field2Type.getSimilarityFunction()) {
        throw new SolrException(
            BAD_REQUEST,
            String.format(
                Locale.ROOT,
                "Invalid arguments: vector field %s and vector field %s must have the same vectorEncoding and similarityFunction",
                field1.getName(),
                field2.getName()));
      }
      v2 = field2Type.getValueSource(field2, fp);
    }
    return createSimilarityFunction(vectorSimilarityFunction, vectorEncoding, v1, v2);
  }

  private ValueSource createSimilarityFunction(
      VectorSimilarityFunction functionName,
      VectorEncoding vectorEncoding,
      ValueSource v1,
      ValueSource v2)
      throws SyntaxError {
    switch (vectorEncoding) {
      case FLOAT32:
        return new FloatVectorSimilarityFunction(functionName, v1, v2);
      case BYTE:
        return new ByteVectorSimilarityFunction(functionName, v1, v2);
      default:
        throw new SyntaxError("Invalid vector encoding: " + vectorEncoding);
    }
  }

  private DenseVectorField requireVectorType(final SchemaField field) throws SyntaxError {
    final FieldType fieldType = field.getType();
    if (fieldType instanceof DenseVectorField) {
      return (DenseVectorField) field.getType();
    }
    throw new SolrException(
        BAD_REQUEST,
        String.format(
            Locale.ROOT,
            "Type mismatch: Expected [%s], but found a different field type for field: [%s]",
            DenseVectorField.class.getSimpleName(),
            field.getName()));
  }

  /**
   * Helper method that returns the correct Enum instance for the <code>arg</code> String, or throws
   * a {@link ErrorCode#BAD_REQUEST} with specifics on the "Invalid argument"
   */
  private static <T extends Enum<T>> T enumValueOrBadRequest(
      final Class<T> enumClass, final String arg) throws SolrException {
    assert null != enumClass;
    try {
      return Enum.valueOf(enumClass, arg);
    } catch (IllegalArgumentException | NullPointerException e) {
      throw new SolrException(
          BAD_REQUEST,
          String.format(
              Locale.ROOT,
              "Invalid argument: %s is not a valid %s. Expected one of %s",
              arg,
              enumClass.getSimpleName(),
              Arrays.toString(enumClass.getEnumConstants())));
    }
  }
}
