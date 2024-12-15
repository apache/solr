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

import org.apache.lucene.search.Query;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.DenseVectorField;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SyntaxError;

public class KnnQParser extends AbstractVectorQParserBase {

  // retrieve the top K results based on the distance similarity function
  static final String TOP_K = "topK";
  static final int DEFAULT_TOP_K = 10;

  public KnnQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  @Override
  public Query parse() throws SyntaxError {
    final SchemaField schemaField = req.getCore().getLatestSchema().getField(getFieldName());
    final DenseVectorField denseVectorType = getCheckedFieldType(schemaField);
    final String vectorToSearch = getVectorToSearch();
    final int topK = localParams.getInt(TOP_K, DEFAULT_TOP_K);

    return denseVectorType.getKnnVectorQuery(
        schemaField.getName(), vectorToSearch, topK, getFilterQuery(), getSeedQuery());
  }
}
