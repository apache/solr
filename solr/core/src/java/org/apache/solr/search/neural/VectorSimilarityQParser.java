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

import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.ByteVectorSimilarityQuery;
import org.apache.lucene.search.FloatVectorSimilarityQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.DenseVectorField;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.vector.DenseVectorParser;

public class VectorSimilarityQParser extends AbstractVectorQParserBase {

  // retrieve the top results based on the distance similarity function thresholds
  static final String MIN_RETURN = "minReturn";
  static final String MIN_TRAVERSE = "minTraverse";

  static final float DEFAULT_MIN_TRAVERSE = Float.NEGATIVE_INFINITY;

  public VectorSimilarityQParser(
      String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  @Override
  public Query parse() throws SyntaxError {
    final String fieldName = getFieldName();
    final SchemaField schemaField = req.getCore().getLatestSchema().getField(fieldName);
    final DenseVectorField denseVectorType = getCheckedFieldType(schemaField);
    final String vectorToSearch = getVectorToSearch();
    final float minTraverse = localParams.getFloat(MIN_TRAVERSE, DEFAULT_MIN_TRAVERSE);
    final Float minReturn = localParams.getFloat(MIN_RETURN);
    if (null == minReturn) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          MIN_RETURN + " is required to use Vector Similarity QParser");
    }

    final DenseVectorParser vectorBuilder =
        denseVectorType.getVectorBuilder(vectorToSearch, DenseVectorParser.BuilderPhase.QUERY);

    final VectorEncoding vectorEncoding = denseVectorType.getVectorEncoding();
    switch (vectorEncoding) {
      case FLOAT32:
        return new FloatVectorSimilarityQuery(
            fieldName, vectorBuilder.getFloatVector(), minTraverse, minReturn, getFilterQuery());
      case BYTE:
        return new ByteVectorSimilarityQuery(
            fieldName, vectorBuilder.getByteVector(), minTraverse, minReturn, getFilterQuery());
      default:
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Unexpected state. Vector Encoding: " + vectorEncoding);
    }
  }
}
