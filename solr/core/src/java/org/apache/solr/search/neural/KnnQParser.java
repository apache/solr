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

import java.util.Optional;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.PatienceKnnVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.DenseVectorField;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SyntaxError;

public class KnnQParser extends AbstractVectorQParserBase {

  // retrieve the top K results based on the distance similarity function
  protected static final String TOP_K = "topK";
  protected static final int DEFAULT_TOP_K = 10;

  // parameters for PatienceKnnVectorQuery, a version of knn vector query that exits early when HNSW
  // queue
  // saturates over a {@code #saturationThreshold} for more than {@code #patience} times.
  protected static final String EARLY_TERMINATION = "earlyTermination";
  protected static final boolean DEFAULT_EARLY_TERMINATION = false;
  protected static final String SATURATION_THRESHOLD = "saturationThreshold";
  protected static final String PATIENCE = "patience";

  public KnnQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  @Override
  public Query parse() throws SyntaxError {
    final SchemaField schemaField = req.getCore().getLatestSchema().getField(getFieldName());
    final DenseVectorField denseVectorType = getCheckedFieldType(schemaField);
    final String vectorToSearch = getVectorToSearch();
    final int topK = localParams.getInt(TOP_K, DEFAULT_TOP_K);

    return wrapWithPatienceIfEarlyTerminationEnabled(
        denseVectorType.getKnnVectorQuery(
            schemaField.getName(), vectorToSearch, topK, getFilterQuery()));
  }

  protected Query wrapWithPatienceIfEarlyTerminationEnabled(Query knnQuery) {
    final Double saturationThreshold =
        Optional.ofNullable(localParams.get(SATURATION_THRESHOLD))
            .map(KnnQParser::validateSaturationThreshold)
            .orElse(null);

    final Integer patience =
        Optional.ofNullable(localParams.get(PATIENCE))
            .map(KnnQParser::validatePatience)
            .orElse(null);

    final boolean useCustomParams = (saturationThreshold != null && patience != null);
    if ((saturationThreshold == null) != (patience == null)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Parameters 'saturationThreshold' and 'patience' must both be provided, or neither.");
    }

    final boolean earlyTerminationEnabled =
        localParams.getBool(EARLY_TERMINATION, DEFAULT_EARLY_TERMINATION) || useCustomParams;

    if (!earlyTerminationEnabled) {
      return knnQuery;
    }

    return switch (knnQuery) {
      case KnnFloatVectorQuery knnFloatQuery -> useCustomParams
          ? PatienceKnnVectorQuery.fromFloatQuery(knnFloatQuery, saturationThreshold, patience)
          : PatienceKnnVectorQuery.fromFloatQuery(knnFloatQuery);
      case KnnByteVectorQuery knnByteQuery -> useCustomParams
          ? PatienceKnnVectorQuery.fromByteQuery(knnByteQuery, saturationThreshold, patience)
          : PatienceKnnVectorQuery.fromByteQuery(knnByteQuery);
      default -> throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "earlyTermination enabled but this is not a Knn*VectorQuery: " + knnQuery.getClass());
    };
  }

  private static Double validateSaturationThreshold(String value) {
    if (value == null) {
      return null;
    }
    try {
      double parsedValue = Double.parseDouble(value);
      if (Double.isNaN(parsedValue)
          || Double.isInfinite(parsedValue)
          || parsedValue <= 0.0
          || parsedValue >= 1.0) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Invalid saturationThreshold value: must be a double between 0.0 and 1.0 (exclusive), got "
                + parsedValue);
      }
      return parsedValue;
    } catch (NumberFormatException e) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Invalid saturationThreshold value: not a valid double, got " + value,
          e);
    }
  }

  private static Integer validatePatience(String value) {
    if (value == null) {
      return null;
    }
    try {
      int parsedValue = Integer.parseInt(value);
      if (parsedValue < 7) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Invalid patience value: must be an integer >= 7, got " + parsedValue);
      }
      return parsedValue;
    } catch (NumberFormatException e) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Invalid patience value: not a valid integer, got " + value,
          e);
    }
  }
}
