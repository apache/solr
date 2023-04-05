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

import java.io.IOException;
import java.io.StringReader;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CollectionUtil;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SyntaxError;

public class MLTContentQParserPlugin extends QParserPlugin {
  public static final String NAME = "mlt_content";

  @Override
  public QParser createParser(
      String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new AbstractMLTQParser(qstr, localParams, params, req) {
      @Override
      public Query parse() throws SyntaxError {
        String content = localParams.get(QueryParsing.V);
        try {
          return parseMLTQuery(this::getFieldsFromSchema, (mlt) -> likeContent(mlt, content));
        } catch (IOException e) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST, "Error completing MLT request" + e.getMessage());
        }
      }
    };
  }

  protected Query likeContent(MoreLikeThis moreLikeThis, String content) throws IOException {
    final String[] fieldNames = moreLikeThis.getFieldNames();
    if (fieldNames.length == 1) {
      return moreLikeThis.like(fieldNames[0], new StringReader(content));
    } else {
      Collection<Object> streamValue = Collections.singleton(content);
      Map<String, Collection<Object>> multifieldDoc = CollectionUtil.newHashMap(fieldNames.length);
      for (String field : fieldNames) {
        multifieldDoc.put(field, streamValue);
      }
      return moreLikeThis.like(multifieldDoc);
    }
  }
}
