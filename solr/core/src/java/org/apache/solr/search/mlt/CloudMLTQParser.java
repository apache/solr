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
package org.apache.solr.search.mlt;

import static org.apache.solr.common.params.CommonParams.ID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QueryParsing;

public class CloudMLTQParser extends SimpleMLTQParser {

  public CloudMLTQParser(
      String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  @Override
  public Query parse() {
    String id = localParams.get(QueryParsing.V);
    // Do a Real Time Get for the document
    SolrDocument doc = getDocument(id);
    if (doc == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Error completing MLT request. Could not fetch " + "document with id [" + id + "]");
    }
    try {
      final Query docIdQuery = createIdQuery(req.getSchema().getUniqueKeyField().getName(), id);
      return parseMLTQuery(() -> getFieldsFromDoc(doc), (mlt) -> likeDoc(mlt, doc), docIdQuery);
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Bad Request", e);
    }
  }

  protected Query likeDoc(MoreLikeThis moreLikeThis, SolrDocument doc) throws IOException {
    Map<String, Collection<Object>> filteredDocument = new HashMap<>();

    for (String field : moreLikeThis.getFieldNames()) {
      Collection<Object> fieldValues = doc.getFieldValues(field);
      if (fieldValues != null) {
        Collection<Object> values = new ArrayList<>();
        for (Object val : fieldValues) {
          if (val instanceof IndexableField) {
            values.add(((IndexableField) val).stringValue());
          } else {
            values.add(val);
          }
        }
        filteredDocument.put(field, values);
      }
    }
    return moreLikeThis.like(filteredDocument);
  }

  protected String[] getFieldsFromDoc(SolrDocument doc) {
    ArrayList<String> fields = new ArrayList<>();
    for (String field : doc.getFieldNames()) {
      // Only use fields that are stored and have an explicit analyzer.
      // This makes sense as the query uses tf/idf/.. for query construction.
      // We might want to relook and change this in the future though.
      SchemaField f = req.getSchema().getFieldOrNull(field);
      if (f != null && f.stored() && f.getType().isExplicitAnalyzer()) {
        fields.add(field);
      }
    }
    return fields.toArray(new String[0]);
  }

  private SolrDocument getDocument(String id) {
    SolrCore core = req.getCore();
    SolrQueryResponse rsp = new SolrQueryResponse();
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(ID, id);

    SolrQueryRequestBase request = new SolrQueryRequestBase(core, params) {};

    core.getRequestHandler("/get").handleRequest(request, rsp);
    NamedList<?> response = rsp.getValues();

    return (SolrDocument) response.get("doc");
  }
}
