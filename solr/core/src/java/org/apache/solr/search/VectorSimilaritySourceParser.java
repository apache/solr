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

import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;

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
    final boolean constVec = '[' == fp.sp.peek();
    final String arg2Str = constVec ? null : fp.parseArg();
    if (fp.hasMoreArguments() && arg2Str != null) return handle4ArgsVariant(fp, arg1Str, arg2Str);
    return handle2ArgsVariant(fp, arg1Str, arg2Str, constVec);
  }

  private ValueSource handle4ArgsVariant(FunctionQParser fp, String arg1Str, String arg2Str)
      throws SyntaxError {
    final var vectorEncoding = VectorEncoding.valueOf(arg1Str);
    final var vectorSimilarityFunction = VectorSimilarityFunction.valueOf(arg2Str);
    int vectorEncodingFlag =
        vectorEncoding.equals(VectorEncoding.BYTE)
            ? FunctionQParser.FLAG_PARSE_VECTOR_BYTE_ENCODING
            : 0;
    final ValueSource v1 =
        fp.parseValueSource(
            FunctionQParser.FLAG_DEFAULT
                | FunctionQParser.FLAG_CONSUME_DELIMITER
                | vectorEncodingFlag);
    final ValueSource v2 =
        fp.parseValueSource(
            FunctionQParser.FLAG_DEFAULT
                | FunctionQParser.FLAG_CONSUME_DELIMITER
                | vectorEncodingFlag);
    return createSimilarityFunction(vectorSimilarityFunction, vectorEncoding, v1, v2);
  }

  private ValueSource handle2ArgsVariant(
      FunctionQParser fp, String arg1Str, String arg2Str, boolean constVec) throws SyntaxError {
    final SchemaField field1 = fp.req.getSchema().getField(arg1Str);
    requireVectorType(field1.getType());
    final DenseVectorField field1Type = (DenseVectorField) field1.getType();
    final var vectorEncoding = field1Type.getVectorEncoding();
    final var vectorSimilarityFunction = field1Type.getSimilarityFunction();
    int vectorEncodingFlag =
        vectorEncoding.equals(VectorEncoding.BYTE)
            ? FunctionQParser.FLAG_PARSE_VECTOR_BYTE_ENCODING
            : 0;
    final ValueSource v1 = field1Type.getValueSource(field1, fp);
    final ValueSource v2;
    if (constVec) {
      v2 =
          fp.parseValueSource(
              FunctionQParser.FLAG_DEFAULT
                  | FunctionQParser.FLAG_CONSUME_DELIMITER
                  | vectorEncodingFlag);
    } else {
      final SchemaField field2 = fp.req.getSchema().getField(arg2Str);
      requireVectorType(field2.getType());
      v2 = field2.getType().getValueSource(field2, fp);
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

  private void requireVectorType(FieldType fieldType) throws SyntaxError {
    if (!(fieldType instanceof DenseVectorField)) {
      throw new SolrException(
          BAD_REQUEST,
          String.format(
              Locale.ROOT,
              "Type mismatch: Expected [%s], but found a different field type.",
              DenseVectorField.class.getSimpleName()));
    }
  }
}
