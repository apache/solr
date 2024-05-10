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
package org.apache.solr.search.function;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.queries.function.valuesource.ConstKnnByteVectorValueSource;
import org.apache.lucene.queries.function.valuesource.ConstKnnFloatValueSource;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.junit.Assert;
import org.junit.Test;

public class TestDenseVectorValueSourceParser {

  @Test
  public void floatVectorParsing_shouldReturnConstKnnFloatValueSource()
      throws SyntaxError, IOException {
    FunctionQParser qp = new FunctionQParser("[1, 2,3,4]", null, null, null);
    var valueSource = qp.parseConstVector(0);
    Assert.assertEquals(ConstKnnFloatValueSource.class, valueSource.getClass());
    var floatVectorValueSource = (ConstKnnFloatValueSource) valueSource;

    float[] expected = {1.f, 2.f, 3.f, 4.f};
    Assert.assertArrayEquals(
        expected, floatVectorValueSource.getValues(null, null).floatVectorVal(0), 0.1f);
  }

  @Test
  public void byteVectorParsing_shouldReturnConstKnnByterValueSource()
      throws SyntaxError, IOException {
    FunctionQParser qp = new FunctionQParser("[1, 2,3, 4]", null, null, null);
    var valueSource = qp.parseConstVector(FunctionQParser.FLAG_PARSE_VECTOR_BYTE_ENCODING);
    Assert.assertEquals(ConstKnnByteVectorValueSource.class, valueSource.getClass());

    var byteVectorValueSource = (ConstKnnByteVectorValueSource) valueSource;

    byte[] expected = {1, 2, 3, 4};
    Assert.assertArrayEquals(
        expected, byteVectorValueSource.getValues(null, null).byteVectorVal(0));
  }

  @Test
  public void byteVectorParsing_ValuesOutsideByteBoundaries_shouldRaiseAnException() {
    FunctionQParser qp = new FunctionQParser("[1,2,3,170]", null, null, null);
    Assert.assertThrows(
        NumberFormatException.class,
        () -> qp.parseConstVector(FunctionQParser.FLAG_PARSE_VECTOR_BYTE_ENCODING));
  }

  @Test
  public void byteVectorParsing_NonIntegerValues_shouldRaiseAnException() {
    FunctionQParser qp = new FunctionQParser("[1,2,3.2,4]", null, null, null);
    Assert.assertThrows(
        NumberFormatException.class,
        () -> qp.parseConstVector(FunctionQParser.FLAG_PARSE_VECTOR_BYTE_ENCODING));
  }

  @Test
  public void byteVectorParsing_WrongSyntaxForVector_shouldRaiseAnException() {

    var testCases = List.of("<1,2,3.2,4>", "[1,,2,3,4]", "[1,2,3,4,5", "[1,2,3,4,,]", "1,2,3,4]");

    for (String testCase : testCases) {
      FunctionQParser qp = new FunctionQParser(testCase, null, null, null);
      Assert.assertThrows(
          SyntaxError.class,
          () -> qp.parseConstVector(FunctionQParser.FLAG_PARSE_VECTOR_BYTE_ENCODING));
    }
  }
}
