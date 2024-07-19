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
package org.apache.solr.search.neural;

import static org.apache.solr.search.neural.KnnQParser.DEFAULT_TOP_K;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KnnQParserTest extends SolrTestCaseJ4 {
  String IDField = "id";
  String vectorField = "vector";
  String vectorField2 = "vector2";
  String vectorFieldByteEncoding = "vector_byte_encoding";

  @Before
  public void prepareIndex() throws Exception {
    /* vectorDimension="4" similarityFunction="cosine" */
    initCore("solrconfig_codec.xml", "schema-densevector.xml");

    List<SolrInputDocument> docsToIndex = this.prepareDocs();
    for (SolrInputDocument doc : docsToIndex) {
      assertU(adoc(doc));
    }

    assertU(commit());
  }

  private List<SolrInputDocument> prepareDocs() {
    int docsCount = 13;
    List<SolrInputDocument> docs = new ArrayList<>(docsCount);
    for (int i = 1; i < docsCount + 1; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(IDField, i);
      docs.add(doc);
    }

    docs.get(0)
        .addField(vectorField, Arrays.asList(1f, 2f, 3f, 4f)); // cosine distance vector1= 1.0
    docs.get(1)
        .addField(
            vectorField, Arrays.asList(1.5f, 2.5f, 3.5f, 4.5f)); // cosine distance vector1= 0.998
    docs.get(2)
        .addField(
            vectorField,
            Arrays.asList(7.5f, 15.5f, 17.5f, 22.5f)); // cosine distance vector1= 0.992
    docs.get(3)
        .addField(
            vectorField, Arrays.asList(1.4f, 2.4f, 3.4f, 4.4f)); // cosine distance vector1= 0.999
    docs.get(4)
        .addField(vectorField, Arrays.asList(30f, 22f, 35f, 20f)); // cosine distance vector1= 0.862
    docs.get(5)
        .addField(vectorField, Arrays.asList(40f, 1f, 1f, 200f)); // cosine distance vector1= 0.756
    docs.get(6)
        .addField(vectorField, Arrays.asList(5f, 10f, 20f, 40f)); // cosine distance vector1= 0.970
    docs.get(7)
        .addField(
            vectorField, Arrays.asList(120f, 60f, 30f, 15f)); // cosine distance vector1= 0.515
    docs.get(8)
        .addField(
            vectorField, Arrays.asList(200f, 50f, 100f, 25f)); // cosine distance vector1= 0.554
    docs.get(9)
        .addField(
            vectorField, Arrays.asList(1.8f, 2.5f, 3.7f, 4.9f)); // cosine distance vector1= 0.997
    docs.get(10)
        .addField(vectorField2, Arrays.asList(1f, 2f, 3f, 4f)); // cosine distance vector2= 1
    docs.get(11)
        .addField(
            vectorField2,
            Arrays.asList(7.5f, 15.5f, 17.5f, 22.5f)); // cosine distance vector2= 0.992
    docs.get(12)
        .addField(
            vectorField2, Arrays.asList(1.5f, 2.5f, 3.5f, 4.5f)); // cosine distance vector2= 0.998

    docs.get(0).addField(vectorFieldByteEncoding, Arrays.asList(1, 2, 3, 4));
    docs.get(1).addField(vectorFieldByteEncoding, Arrays.asList(2, 2, 1, 4));
    docs.get(2).addField(vectorFieldByteEncoding, Arrays.asList(1, 2, 1, 2));
    docs.get(3).addField(vectorFieldByteEncoding, Arrays.asList(7, 2, 1, 3));
    docs.get(4).addField(vectorFieldByteEncoding, Arrays.asList(19, 2, 4, 4));
    docs.get(5).addField(vectorFieldByteEncoding, Arrays.asList(19, 2, 4, 4));
    docs.get(6).addField(vectorFieldByteEncoding, Arrays.asList(18, 2, 4, 4));
    docs.get(7).addField(vectorFieldByteEncoding, Arrays.asList(8, 3, 2, 4));

    return docs;
  }

  @After
  public void cleanUp() {
    clearIndex();
    deleteCore();
  }

  @Test
  public void incorrectTopK_shouldThrowException() {
    String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";

    assertQEx(
        "String topK should throw Exception",
        "For input string: \"string\"",
        req(CommonParams.Q, "{!knn f=vector topK=string}" + vectorToSearch, "fl", "id"),
        SolrException.ErrorCode.BAD_REQUEST);

    assertQEx(
        "Double topK should throw Exception",
        "For input string: \"4.5\"",
        req(CommonParams.Q, "{!knn f=vector topK=4.5}" + vectorToSearch, "fl", "id"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void topKMissing_shouldReturnDefaultTopK() {
    String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";

    assertQ(
        req(CommonParams.Q, "{!knn f=vector}" + vectorToSearch, "fl", "id"),
        "//result[@numFound='" + DEFAULT_TOP_K + "']",
        "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='4']",
        "//result/doc[3]/str[@name='id'][.='2']",
        "//result/doc[4]/str[@name='id'][.='10']",
        "//result/doc[5]/str[@name='id'][.='3']",
        "//result/doc[6]/str[@name='id'][.='7']",
        "//result/doc[7]/str[@name='id'][.='5']",
        "//result/doc[8]/str[@name='id'][.='6']",
        "//result/doc[9]/str[@name='id'][.='9']",
        "//result/doc[10]/str[@name='id'][.='8']");
  }

  @Test
  public void topK_shouldReturnOnlyTopKResults() {
    String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";

    assertQ(
        req(CommonParams.Q, "{!knn f=vector topK=5}" + vectorToSearch, "fl", "id"),
        "//result[@numFound='5']",
        "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='4']",
        "//result/doc[3]/str[@name='id'][.='2']",
        "//result/doc[4]/str[@name='id'][.='10']",
        "//result/doc[5]/str[@name='id'][.='3']");

    assertQ(
        req(CommonParams.Q, "{!knn f=vector topK=3}" + vectorToSearch, "fl", "id"),
        "//result[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='4']",
        "//result/doc[3]/str[@name='id'][.='2']");
  }

  @Test
  public void incorrectVectorFieldType_shouldThrowException() {
    String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";

    assertQEx(
        "Incorrect vector field type should throw Exception",
        "only DenseVectorField is compatible with Vector Query Parsers",
        req(CommonParams.Q, "{!knn f=id topK=10}" + vectorToSearch, "fl", "id"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void undefinedVectorField_shouldThrowException() {
    String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";

    assertQEx(
        "Undefined vector field should throw Exception",
        "undefined field: \"notExistent\"",
        req(CommonParams.Q, "{!knn f=notExistent topK=10}" + vectorToSearch, "fl", "id"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void missingVectorField_shouldThrowException() {
    String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";

    assertQEx(
        "missing vector field should throw Exception",
        "the Dense Vector field 'f' is missing",
        req(CommonParams.Q, "{!knn topK=10}" + vectorToSearch, "fl", "id"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void correctVectorField_shouldSearchOnThatField() {
    String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";

    assertQ(
        req(CommonParams.Q, "{!knn f=vector2 topK=5}" + vectorToSearch, "fl", "id"),
        "//result[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='11']",
        "//result/doc[2]/str[@name='id'][.='13']",
        "//result/doc[3]/str[@name='id'][.='12']");
  }

  @Test
  public void highDimensionFloatVectorField_shouldSearchOnThatField() {
    int highDimension = 2048;
    List<SolrInputDocument> docsToIndex = this.prepareHighDimensionFloatVectorsDocs(highDimension);
    for (SolrInputDocument doc : docsToIndex) {
      assertU(adoc(doc));
    }
    assertU(commit());

    float[] highDimensionalityQueryVector = new float[highDimension];
    for (int i = 0; i < highDimension; i++) {
      highDimensionalityQueryVector[i] = i;
    }
    String vectorToSearch = Arrays.toString(highDimensionalityQueryVector);

    assertQ(
        req(CommonParams.Q, "{!knn f=2048_float_vector topK=1}" + vectorToSearch, "fl", "id"),
        "//result[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.='1']");
  }

  @Test
  public void highDimensionByteVectorField_shouldSearchOnThatField() {
    int highDimension = 2048;
    List<SolrInputDocument> docsToIndex = this.prepareHighDimensionByteVectorsDocs(highDimension);
    for (SolrInputDocument doc : docsToIndex) {
      assertU(adoc(doc));
    }
    assertU(commit());

    byte[] highDimensionalityQueryVector = new byte[highDimension];
    for (int i = 0; i < highDimension; i++) {
      highDimensionalityQueryVector[i] = (byte) (i % 127);
    }
    String vectorToSearch = Arrays.toString(highDimensionalityQueryVector);

    assertQ(
        req(CommonParams.Q, "{!knn f=2048_byte_vector topK=1}" + vectorToSearch, "fl", "id"),
        "//result[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.='1']");
  }

  private List<SolrInputDocument> prepareHighDimensionFloatVectorsDocs(int highDimension) {
    int docsCount = 13;
    String field = "2048_float_vector";
    List<SolrInputDocument> docs = new ArrayList<>(docsCount);

    for (int i = 1; i < docsCount + 1; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(IDField, i);
      docs.add(doc);
    }

    for (int i = 0; i < docsCount; i++) {
      List<Integer> highDimensionalityVector = new ArrayList<>();
      for (int j = i * highDimension; j < highDimension; j++) {
        highDimensionalityVector.add(j);
      }
      docs.get(i).addField(field, highDimensionalityVector);
    }
    Collections.reverse(docs);
    return docs;
  }

  private List<SolrInputDocument> prepareHighDimensionByteVectorsDocs(int highDimension) {
    int docsCount = 13;
    String field = "2048_byte_vector";
    List<SolrInputDocument> docs = new ArrayList<>(docsCount);

    for (int i = 1; i < docsCount + 1; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(IDField, i);
      docs.add(doc);
    }

    for (int i = 0; i < docsCount; i++) {
      List<Integer> highDimensionalityVector = new ArrayList<>();
      for (int j = i * highDimension; j < highDimension; j++) {
        highDimensionalityVector.add(j % 127);
      }
      docs.get(i).addField(field, highDimensionalityVector);
    }
    Collections.reverse(docs);
    return docs;
  }

  @Test
  public void vectorByteEncodingField_shouldSearchOnThatField() {
    String vectorToSearch = "[2, 2, 1, 3]";

    assertQ(
        req(CommonParams.Q, "{!knn f=vector_byte_encoding topK=2}" + vectorToSearch, "fl", "id"),
        "//result[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='2']",
        "//result/doc[2]/str[@name='id'][.='3']");

    vectorToSearch = "[8, 3, 2, 4]";

    assertQ(
        req(CommonParams.Q, "{!knn f=vector_byte_encoding topK=2}" + vectorToSearch, "fl", "id"),
        "//result[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='8']",
        "//result/doc[2]/str[@name='id'][.='4']");
  }

  @Test
  public void vectorByteEncodingField_shouldRaiseExceptionIfQueryUsesFloatVectors() {
    String vectorToSearch = "[8.3, 4.3, 2.1, 4.1]";

    assertQEx(
        "incorrect vector element: '8.3'. The expected format is:'[b1,b2..b3]' where each element b is a byte (-128 to 127)",
        "incorrect vector element: '8.3'. The expected format is:'[b1,b2..b3]' where each element b is a byte (-128 to 127)",
        req(CommonParams.Q, "{!knn f=vector_byte_encoding topK=10}" + vectorToSearch, "fl", "id"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void
      vectorByteEncodingField_shouldRaiseExceptionWhenQueryContainsValuesOutsideByteValueRange() {
    String vectorToSearch = "[1, -129, 3, 5]";

    assertQEx(
        "incorrect vector element: ' -129'. The expected format is:'[b1,b2..b3]' where each element b is a byte (-128 to 127)",
        "incorrect vector element: ' -129'. The expected format is:'[b1,b2..b3]' where each element b is a byte (-128 to 127)",
        req(CommonParams.Q, "{!knn f=vector_byte_encoding topK=10}" + vectorToSearch, "fl", "id"),
        SolrException.ErrorCode.BAD_REQUEST);

    vectorToSearch = "[1, 3, 156, 5]";

    assertQEx(
        "incorrect vector element: ' 156'. The expected format is:'[b1,b2..b3]' where each element b is a byte (-128 to 127)",
        "incorrect vector element: ' 156'. The expected format is:'[b1,b2..b3]' where each element b is a byte (-128 to 127)",
        req(CommonParams.Q, "{!knn f=vector_byte_encoding topK=10}" + vectorToSearch, "fl", "id"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void missingVectorToSearch_shouldThrowException() {
    assertQEx(
        "missing vector to search should throw Exception",
        "the Dense Vector value 'v' to search is missing",
        req(CommonParams.Q, "{!knn f=vector topK=10}", "fl", "id"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void incorrectVectorToSearchDimension_shouldThrowException() {
    String vectorToSearch = "[2.0, 4.4, 3.]";
    assertQEx(
        "missing vector to search should throw Exception",
        "incorrect vector dimension. The vector value has size 3 while it is expected a vector with size 4",
        req(CommonParams.Q, "{!knn f=vector topK=10}" + vectorToSearch, "fl", "id"),
        SolrException.ErrorCode.BAD_REQUEST);

    vectorToSearch = "[2.0, 4.4,,]";
    assertQEx(
        "incorrect vector to search should throw Exception",
        "incorrect vector dimension. The vector value has size 2 while it is expected a vector with size 4",
        req(CommonParams.Q, "{!knn f=vector topK=10}" + vectorToSearch, "fl", "id"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void incorrectVectorToSearch_shouldThrowException() {
    String vectorToSearch = "2.0, 4.4, 3.5, 6.4";
    assertQEx(
        "incorrect vector to search should throw Exception",
        "incorrect vector format. The expected format is:'[f1,f2..f3]' where each element f is a float",
        req(CommonParams.Q, "{!knn f=vector topK=10}" + vectorToSearch, "fl", "id"),
        SolrException.ErrorCode.BAD_REQUEST);

    vectorToSearch = "[2.0, 4.4, 3.5, 6.4";
    assertQEx(
        "incorrect vector to search should throw Exception",
        "incorrect vector format. The expected format is:'[f1,f2..f3]' where each element f is a float",
        req(CommonParams.Q, "{!knn f=vector topK=10}" + vectorToSearch, "fl", "id"),
        SolrException.ErrorCode.BAD_REQUEST);

    vectorToSearch = "2.0, 4.4, 3.5, 6.4]";
    assertQEx(
        "incorrect vector to search should throw Exception",
        "incorrect vector format. The expected format is:'[f1,f2..f3]' where each element f is a float",
        req(CommonParams.Q, "{!knn f=vector topK=10}" + vectorToSearch, "fl", "id"),
        SolrException.ErrorCode.BAD_REQUEST);

    vectorToSearch = "[2.0, 4.4, 3.5, stringElement]";
    assertQEx(
        "incorrect vector to search should throw Exception",
        "incorrect vector element: ' stringElement'. The expected format is:'[f1,f2..f3]' where each element f is a float",
        req(CommonParams.Q, "{!knn f=vector topK=10}" + vectorToSearch, "fl", "id"),
        SolrException.ErrorCode.BAD_REQUEST);

    vectorToSearch = "[2.0, 4.4, , ]";
    assertQEx(
        "incorrect vector to search should throw Exception",
        "incorrect vector element: ' '. The expected format is:'[f1,f2..f3]' where each element f is a float",
        req(CommonParams.Q, "{!knn f=vector topK=10}" + vectorToSearch, "fl", "id"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void correctQuery_shouldRankBySimilarityFunction() {
    String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";

    assertQ(
        req(CommonParams.Q, "{!knn f=vector topK=10}" + vectorToSearch, "fl", "id"),
        "//result[@numFound='10']",
        "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='4']",
        "//result/doc[3]/str[@name='id'][.='2']",
        "//result/doc[4]/str[@name='id'][.='10']",
        "//result/doc[5]/str[@name='id'][.='3']",
        "//result/doc[6]/str[@name='id'][.='7']",
        "//result/doc[7]/str[@name='id'][.='5']",
        "//result/doc[8]/str[@name='id'][.='6']",
        "//result/doc[9]/str[@name='id'][.='9']",
        "//result/doc[10]/str[@name='id'][.='8']");
  }

  @Test
  public void knnQueryUsedInFilter_shouldFilterResultsBeforeTheQueryExecution() {
    String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";
    assertQ(
        req(
            CommonParams.Q,
            "id:(3 4 9 2)",
            "fq",
            "{!knn f=vector topK=4}" + vectorToSearch,
            "fl",
            "id"),
        "//result[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='2']",
        "//result/doc[2]/str[@name='id'][.='4']");
  }

  @Test
  public void knnQueryUsedInFilters_shouldFilterResultsBeforeTheQueryExecution() {
    String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";

    // topK=4 -> 1,4,2,10
    assertQ(
        req(
            CommonParams.Q,
            "id:(3 4 9 2)",
            "fq",
            "{!knn f=vector topK=4}" + vectorToSearch,
            "fq",
            "id:(4 20 9)",
            "fl",
            "id"),
        "//result[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.='4']");
  }

  @Test
  public void knnQueryUsedInFiltersWithPreFilter_shouldFilterResultsBeforeTheQueryExecution() {
    String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";

    // topK=4 w/localparam preFilter -> 1,4,7,9
    assertQ(
        req(
            CommonParams.Q,
            "id:(3 4 9 2)",
            "fq",
            "{!knn f=vector topK=4 preFilter='id:(1 4 7 8 9)'}" + vectorToSearch,
            "fq",
            "id:(4 20 9)",
            "fl",
            "id"),
        "//result[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='4']",
        "//result/doc[2]/str[@name='id'][.='9']");
  }

  @Test
  public void knnQueryUsedInFilters_rejectIncludeExclude() {
    String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";

    for (String fq :
        Arrays.asList(
            "{!knn f=vector topK=5 includeTags=xxx}" + vectorToSearch,
            "{!knn f=vector topK=5 excludeTags=xxx}" + vectorToSearch)) {
      assertQEx(
          "fq={!knn...} incompatible with include/exclude localparams",
          "used as a filter does not support",
          req("q", "*:*", "fq", fq),
          SolrException.ErrorCode.BAD_REQUEST);
    }
  }

  @Test
  public void knnQueryAsSubQuery() {
    final SolrParams common = params("fl", "id", "vec", "[1.0, 2.0, 3.0, 4.0]");
    final String filt = "id:(2 4 7 9 8 20 3)";

    // When knn parser is a subquery, it should not pre-filter on any global fq params
    // topK -> 1,4,2,10,3 -> fq -> 4,2,3
    assertQ(
        req(common, "fq", filt, "q", "*:* AND {!knn f=vector topK=5 v=$vec}"),
        "//result[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='4']",
        "//result/doc[2]/str[@name='id'][.='2']",
        "//result/doc[3]/str[@name='id'][.='3']");
    // topK -> 1,4,2,10,3 + '8' -> fq -> 4,2,3,8
    assertQ(
        req(common, "fq", filt, "q", "id:8^=0.01 OR {!knn f=vector topK=5 v=$vec}"),
        "//result[@numFound='4']",
        "//result/doc[1]/str[@name='id'][.='4']",
        "//result/doc[2]/str[@name='id'][.='2']",
        "//result/doc[3]/str[@name='id'][.='3']",
        "//result/doc[4]/str[@name='id'][.='8']");
  }

  @Test
  public void knnQueryAsSubQuery_withPreFilter() {
    final SolrParams common = params("fl", "id", "vec", "[1.0, 2.0, 3.0, 4.0]");
    final String filt = "id:(2 4 7 9 8 20 3)";

    // knn subquery should still accept `preFilter` local param
    // filt -> topK -> 4,2,3,7,9
    assertQ(
        req(common, "q", "*:* AND {!knn f=vector topK=5 preFilter='" + filt + "' v=$vec}"),
        "//result[@numFound='5']",
        "//result/doc[1]/str[@name='id'][.='4']",
        "//result/doc[2]/str[@name='id'][.='2']",
        "//result/doc[3]/str[@name='id'][.='3']",
        "//result/doc[4]/str[@name='id'][.='7']",
        "//result/doc[5]/str[@name='id'][.='9']");

    // it should not pre-filter on any global fq params
    // filt -> topK -> 4,2,3,7,9 -> fq -> 3,9
    assertQ(
        req(
            common,
            "fq",
            "id:(1 9 20 3 5 6 8)",
            "q",
            "*:* AND {!knn f=vector topK=5 preFilter='" + filt + "' v=$vec}"),
        "//result[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='3']",
        "//result/doc[2]/str[@name='id'][.='9']");
    // filt -> topK -> 4,2,3,7,9 + '8' -> fq -> 8,3,9
    assertQ(
        req(
            common,
            "fq",
            "id:(1 9 20 3 5 6 8)",
            "q",
            "id:8^=100 OR {!knn f=vector topK=5 preFilter='" + filt + "' v=$vec}"),
        "//result[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='8']",
        "//result/doc[2]/str[@name='id'][.='3']",
        "//result/doc[3]/str[@name='id'][.='9']");
  }

  @Test
  public void knnQueryAsSubQuery_rejectIncludeExclude() {
    final SolrParams common = params("fl", "id", "vec", "[1.0, 2.0, 3.0, 4.0]");

    for (String knn :
        Arrays.asList(
            "{!knn f=vector topK=5 includeTags=xxx v=$vec}",
            "{!knn f=vector topK=5 excludeTags=xxx v=$vec}")) {
      assertQEx(
          "knn as subquery incompatible with include/exclude localparams",
          "used as a sub-query does not support",
          req(common, "q", "*:* OR " + knn),
          SolrException.ErrorCode.BAD_REQUEST);
    }
  }

  @Test
  public void knnQueryWithFilterQuery_singlePreFilterEquivilence() {
    final String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";
    final SolrParams common = params("fl", "id");

    // these requests should be equivalent
    final String filt = "id:(1 2 7 20)";
    for (SolrQueryRequest req :
        Arrays.asList(
            req(common, "q", "{!knn f=vector topK=10}" + vectorToSearch, "fq", filt),
            req(common, "q", "{!knn f=vector preFilter=\"" + filt + "\" topK=10}" + vectorToSearch),
            req(
                common,
                "q",
                "{!knn f=vector preFilter=$my_filt topK=10}" + vectorToSearch,
                "my_filt",
                filt))) {
      assertQ(
          req,
          "//result[@numFound='3']",
          "//result/doc[1]/str[@name='id'][.='1']",
          "//result/doc[2]/str[@name='id'][.='2']",
          "//result/doc[3]/str[@name='id'][.='7']");
    }
  }

  @Test
  public void knnQueryWithFilterQuery_multiPreFilterEquivilence() {
    final String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";
    final SolrParams common = params("fl", "id");

    // these requests should be equivalent
    final String fx = "id:(3 4 9 2 1 )"; // 1 & 10 dropped from intersection
    final String fy = "id:(3 4 9 2 10)";
    for (SolrQueryRequest req :
        Arrays.asList(
            req(common, "q", "{!knn f=vector topK=4}" + vectorToSearch, "fq", fx, "fq", fy),
            req(
                common,
                "q",
                "{!knn f=vector preFilter=\""
                    + fx
                    + "\" preFilter=\""
                    + fy
                    + "\" topK=4}"
                    + vectorToSearch),
            req(
                common,
                "q",
                "{!knn f=vector preFilter=$fx preFilter=$fy topK=4}" + vectorToSearch,
                "fx",
                fx,
                "fy",
                fy),
            req(
                common,
                "q",
                "{!knn f=vector preFilter=$multi_filt topK=4}" + vectorToSearch,
                "multi_filt",
                fx,
                "multi_filt",
                fy))) {
      assertQ(
          req,
          "//result[@numFound='4']",
          "//result/doc[1]/str[@name='id'][.='4']",
          "//result/doc[2]/str[@name='id'][.='2']",
          "//result/doc[3]/str[@name='id'][.='3']",
          "//result/doc[4]/str[@name='id'][.='9']");
    }
  }

  @Test
  public void knnQueryWithPreFilter_rejectIncludeExclude() {
    final String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";

    assertQEx(
        "knn preFilter localparm incompatible with include/exclude localparams",
        "does not support combining preFilter localparam with either",
        // shouldn't matter if global fq w/tag even exists, usage is an error
        req("q", "{!knn f=vector preFilter='id:1' includeTags=xxx}" + vectorToSearch),
        SolrException.ErrorCode.BAD_REQUEST);
    assertQEx(
        "knn preFilter localparm incompatible with include/exclude localparams",
        "does not support combining preFilter localparam with either",
        // shouldn't matter if global fq w/tag even exists, usage is an error
        req("q", "{!knn f=vector preFilter='id:1' excludeTags=xxx}" + vectorToSearch),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void knnQueryWithFilterQuery_preFilterLocalParamOverridesGlobalFilters() {
    final String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";

    // trivial case: empty preFilter localparam means no pre-filtering
    assertQ(
        req(
            "q", "{!knn f=vector preFilter='' topK=5}" + vectorToSearch,
            "fq", "-id:4",
            "fl", "id"),
        "//result[@numFound='4']",
        "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']",
        "//result/doc[3]/str[@name='id'][.='10']",
        "//result/doc[4]/str[@name='id'][.='3']");

    // localparam prefiltering, global fqs applied independently
    assertQ(
        req(
            "q", "{!knn f=vector preFilter='id:(3 4 9 2 7 8)' topK=5}" + vectorToSearch,
            "fq", "-id:4",
            "fl", "id"),
        "//result[@numFound='4']",
        "//result/doc[1]/str[@name='id'][.='2']",
        "//result/doc[2]/str[@name='id'][.='3']",
        "//result/doc[3]/str[@name='id'][.='7']",
        "//result/doc[4]/str[@name='id'][.='9']");
  }

  @Test
  public void knnQueryWithFilterQuery_localParamIncludeExcludeTags() {
    final String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";
    final SolrParams common =
        params(
            "fl", "id",
            "fq", "{!tag=xx,aa}id:(5 6 7 8 9 10)",
            "fq", "{!tag=yy,aa}id:(1 2 3 4 5 6 7)");

    // These req's are equivalent: pre-filter everything
    // So only 7,6,5 are viable for topK=5
    for (SolrQueryRequest req :
        Arrays.asList(
            // default behavior is all fq's pre-filter,
            req(common, "q", "{!knn f=vector topK=5}" + vectorToSearch),
            // diff ways of explicitly requesting both fq params
            req(common, "q", "{!knn f=vector includeTags=aa topK=5}" + vectorToSearch),
            req(
                common,
                "q",
                "{!knn f=vector includeTags=aa excludeTags='' topK=5}" + vectorToSearch),
            req(
                common,
                "q",
                "{!knn f=vector includeTags=aa excludeTags=bogus topK=5}" + vectorToSearch),
            req(
                common,
                "q",
                "{!knn f=vector includeTags=xx includeTags=yy topK=5}" + vectorToSearch),
            req(common, "q", "{!knn f=vector includeTags=xx,yy,bogus topK=5}" + vectorToSearch))) {
      assertQ(
          req,
          "//result[@numFound='3']",
          "//result/doc[1]/str[@name='id'][.='7']",
          "//result/doc[2]/str[@name='id'][.='5']",
          "//result/doc[3]/str[@name='id'][.='6']");
    }
  }

  @Test
  public void knnQueryWithFilterQuery_localParamsDisablesAllPreFiltering() {
    final String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";
    final SolrParams common =
        params(
            "fl", "id",
            "fq", "{!tag=xx,aa}id:(5 6 7 8 9 10)",
            "fq", "{!tag=yy,aa}id:(1 2 3 4 5 6 7)");

    // These req's are equivalent: pre-filter nothing
    // So 1,4,2,10,3,7 are the topK=6
    // Only 7 matches both of the the regular fq params
    for (SolrQueryRequest req :
        Arrays.asList(
            // explicit local empty preFilter
            req(common, "q", "{!knn f=vector preFilter='' topK=6}" + vectorToSearch),
            // diff ways of explicitly including none of the global fq params
            req(common, "q", "{!knn f=vector includeTags='' topK=6}" + vectorToSearch),
            req(common, "q", "{!knn f=vector includeTags=bogus topK=6}" + vectorToSearch),
            // diff ways of explicitly excluding all of the global fq params
            req(common, "q", "{!knn f=vector excludeTags=aa topK=6}" + vectorToSearch),
            req(
                common,
                "q",
                "{!knn f=vector includeTags=aa excludeTags=aa topK=6}" + vectorToSearch),
            req(
                common,
                "q",
                "{!knn f=vector includeTags=aa excludeTags=xx,yy topK=6}" + vectorToSearch),
            req(
                common,
                "q",
                "{!knn f=vector includeTags=xx,yy excludeTags=aa topK=6}" + vectorToSearch),
            req(common, "q", "{!knn f=vector excludeTags=xx,yy topK=6}" + vectorToSearch),
            req(common, "q", "{!knn f=vector excludeTags=aa topK=6}" + vectorToSearch),
            req(
                common,
                "q",
                "{!knn f=vector excludeTags=xx excludeTags=yy topK=6}" + vectorToSearch),
            req(
                common,
                "q",
                "{!knn f=vector excludeTags=xx excludeTags=yy,bogus topK=6}" + vectorToSearch),
            req(common, "q", "{!knn f=vector excludeTags=xx,yy,bogus topK=6}" + vectorToSearch))) {
      assertQ(req, "//result[@numFound='1']", "//result/doc[1]/str[@name='id'][.='7']");
    }
  }

  @Test
  public void knnQueryWithFilterQuery_localParamCombinedIncludeExcludeTags() {
    final String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";
    final SolrParams common =
        params(
            "fl", "id",
            "fq", "{!tag=xx,aa}id:(5 6 7 8 9 10)",
            "fq", "{!tag=yy,aa}id:(1 2 3 4 5 6 7)");

    // These req's are equivalent: prefilter only the 'yy' fq
    // So 1,4,2,3,7 are in the topK=5.
    // Only 7 matches the regular 'xx' fq param
    for (SolrQueryRequest req :
        Arrays.asList(
            // diff ways of only using the 'yy' filter
            req(common, "q", "{!knn f=vector includeTags=yy,bogus topK=5}" + vectorToSearch),
            req(
                common,
                "q",
                "{!knn f=vector includeTags=yy excludeTags='' topK=5}" + vectorToSearch),
            req(common, "q", "{!knn f=vector excludeTags=xx,bogus topK=5}" + vectorToSearch),
            req(
                common,
                "q",
                "{!knn f=vector includeTags=yy excludeTags=xx topK=5}" + vectorToSearch),
            req(
                common,
                "q",
                "{!knn f=vector includeTags=aa excludeTags=xx topK=5}" + vectorToSearch))) {
      assertQ(req, "//result[@numFound='1']", "//result/doc[1]/str[@name='id'][.='7']");
    }
  }

  @Test
  public void knnQueryWithMultiSelectFaceting_excludeTags() {
    // NOTE: faceting on id is not very realistic,
    // but it confirms what we care about re:filters w/o needing extra fields.
    final String facet_xpath = "//lst[@name='facet_fields']/lst[@name='id']/int";
    final String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";

    final SolrParams common =
        params(
            "fl", "id",
            "indent", "true",
            "q", "{!knn f=vector topK=5 excludeTags=facet_click v=$vec}",
            "vec", vectorToSearch,
            // mimicing "inStock:true"
            "fq", "-id:(2 3)",
            "facet", "true",
            "facet.mincount", "1",
            "facet.field", "{!ex=facet_click}id");

    // initial query, with basic pre-filter and facet counts
    assertQ(
        req(common),
        "//result[@numFound='5']",
        "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='4']",
        "//result/doc[3]/str[@name='id'][.='10']",
        "//result/doc[4]/str[@name='id'][.='7']",
        "//result/doc[5]/str[@name='id'][.='5']",
        "*[count(" + facet_xpath + ")=5]",
        facet_xpath + "[@name='1'][.='1']",
        facet_xpath + "[@name='4'][.='1']",
        facet_xpath + "[@name='10'][.='1']",
        facet_xpath + "[@name='7'][.='1']",
        facet_xpath + "[@name='5'][.='1']");

    // drill down on a single facet constraint
    // multi-select means facet counts shouldn't change
    // (this proves the knn isn't pre-filtering on the 'facet_click' fq)
    assertQ(
        req(common, "fq", "{!tag=facet_click}id:(4)"),
        "//result[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.='4']",
        "*[count(" + facet_xpath + ")=5]",
        facet_xpath + "[@name='1'][.='1']",
        facet_xpath + "[@name='4'][.='1']",
        facet_xpath + "[@name='10'][.='1']",
        facet_xpath + "[@name='7'][.='1']",
        facet_xpath + "[@name='5'][.='1']");

    // drill down on an additional facet constraint
    // multi-select means facet counts shouldn't change
    // (this proves the knn isn't pre-filtering on the 'facet_click' fq)
    assertQ(
        req(common, "fq", "{!tag=facet_click}id:(4 5)"),
        "//result[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='4']",
        "//result/doc[2]/str[@name='id'][.='5']",
        "*[count(" + facet_xpath + ")=5]",
        facet_xpath + "[@name='1'][.='1']",
        facet_xpath + "[@name='4'][.='1']",
        facet_xpath + "[@name='10'][.='1']",
        facet_xpath + "[@name='7'][.='1']",
        facet_xpath + "[@name='5'][.='1']");
  }

  @Test
  public void knnQueryWithCostlyFq_shouldPerformKnnSearchWithPostFilter() {
    String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";

    assertQ(
        req(
            CommonParams.Q,
            "{!knn f=vector topK=10}" + vectorToSearch,
            "fq",
            "{!frange cache=false l=0.99}$q",
            "fl",
            "*,score"),
        "//result[@numFound='5']",
        "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='4']",
        "//result/doc[3]/str[@name='id'][.='2']",
        "//result/doc[4]/str[@name='id'][.='10']",
        "//result/doc[5]/str[@name='id'][.='3']");
  }

  @Test
  public void knnQueryWithFilterQueries_shouldPerformKnnSearchWithPreFiltersAndPostFilters() {
    String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";

    assertQ(
        req(
            CommonParams.Q,
            "{!knn f=vector topK=4}" + vectorToSearch,
            "fq",
            "id:(3 4 9 2)",
            "fq",
            "{!frange cache=false l=0.99}$q",
            "fl",
            "id"),
        "//result[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='4']",
        "//result/doc[2]/str[@name='id'][.='2']");
  }

  @Test
  public void knnQueryWithNegativeFilterQuery_shouldPerformKnnSearchInPreFilteredResults() {
    String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";
    assertQ(
        req(CommonParams.Q, "{!knn f=vector topK=4}" + vectorToSearch, "fq", "-id:4", "fl", "id"),
        "//result[@numFound='4']",
        "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']",
        "//result/doc[3]/str[@name='id'][.='10']",
        "//result/doc[4]/str[@name='id'][.='3']");
  }

  /**
   * See {@link org.apache.solr.search.ReRankQParserPlugin.ReRankQueryRescorer#combine(float,
   * boolean, float)}} for more details.
   */
  @Test
  public void knnQueryAsRerank_shouldAddSimilarityFunctionScore() {
    String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";

    assertQ(
        req(
            CommonParams.Q,
            "id:(3 4 9 2)",
            "rq",
            "{!rerank reRankQuery=$rqq reRankDocs=4 reRankWeight=1}",
            "rqq",
            "{!knn f=vector topK=4}" + vectorToSearch,
            "fl",
            "id"),
        "//result[@numFound='4']",
        "//result/doc[1]/str[@name='id'][.='4']",
        "//result/doc[2]/str[@name='id'][.='2']",
        "//result/doc[3]/str[@name='id'][.='3']",
        "//result/doc[4]/str[@name='id'][.='9']");
  }
}
