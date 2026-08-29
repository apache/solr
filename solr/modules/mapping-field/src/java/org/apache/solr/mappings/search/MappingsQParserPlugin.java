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
package org.apache.solr.mappings.search;

import java.lang.invoke.MethodHandles;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.MappingType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SyntaxError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plugin to allow searching in mapping fields keys and values.
 *
 * <p>Mappings Query Parser Syntax: q={!mappings f=my_mapping_field key="some key" value=123}
 */
public class MappingsQParserPlugin extends QParserPlugin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** Name of the 'mappings' QParserPlugin */
  public static final String NAME = "mappings";

  /** mappings QPArser: Local param 'field' */
  public static final String FIELD_PARAM = QueryParsing.F;

  /** mappings QPArser: Local param 'key' */
  public static final String KEY_PARAM = "key";

  /** mappings QPArser: Local param 'value' */
  public static final String VALUE_PARAM = "value";

  @Override
  public QParser createParser(
      String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new MappingsQParser(qstr, localParams, params, req);
  }

  /** Parser for 'mappings' query */
  public class MappingsQParser extends QParser {

    public MappingsQParser(
        String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
      super(qstr, localParams, params, req);
    }

    @Override
    public Query parse() throws SyntaxError {
      if (log.isDebugEnabled()) {
        log.debug("Parse local params: {}", localParams.toQueryString());
      }
      String searchField = localParams.get(FIELD_PARAM);
      String keySearch = localParams.get(KEY_PARAM);
      String valueSearch = localParams.get(VALUE_PARAM);

      if (keySearch == null && valueSearch == null) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Mappings query must specify 'key' and/or 'value'.");
      }

      if (searchField == null || searchField.equals("*")) {
        return searchAnyMapping(keySearch, valueSearch);

      } else if (!(req.getSchema().getFieldType(searchField) instanceof MappingType)) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Mappings query 'f' parameter must specify the name of a MappingType field.");
      }

      return searchRequiredMapping(searchField, keySearch, valueSearch);
    }

    /** Search or the key and/or value where the mapping exists */
    private Query searchRequiredMapping(String searchField, String keySearch, String valueSearch) {
      SchemaField sf = req.getSchema().getField(searchField);
      FieldType ft = req.getSchema().getFieldType(searchField);

      BooleanQuery.Builder boolQuery = new BooleanQuery.Builder();

      MappingType mappingType = (MappingType) ft;
      Query mappingQuery = ft.getExistenceQuery(this, sf);
      boolQuery.add(mappingQuery, BooleanClause.Occur.MUST);

      addSubQuery(boolQuery, BooleanClause.Occur.MUST, mappingType.getKeyField(sf), keySearch);
      addSubQuery(boolQuery, BooleanClause.Occur.MUST, mappingType.getValueField(sf), valueSearch);

      BooleanQuery query = boolQuery.build();
      if (log.isDebugEnabled()) {
        log.debug("Parsed query: {}", query.toString());
      }
      return query;
    }

    /** Search for key and/or value in any mapping */
    private Query searchAnyMapping(String keySearch, String valueSearch) {
      // all mappings to create key and value fields
      BooleanQuery.Builder boolQuery = new BooleanQuery.Builder();
      req.getSchema().getFields().entrySet().stream()
          .filter(e -> (e.getValue().getType() instanceof MappingType))
          .forEach(
              e -> {
                String mappingField = e.getKey();
                SchemaField sf = req.getSchema().getField(mappingField);
                FieldType ft = req.getSchema().getFieldType(mappingField);
                MappingType mappingType = (MappingType) ft;

                addSubQuery(
                    boolQuery, BooleanClause.Occur.SHOULD, mappingType.getKeyField(sf), keySearch);
                addSubQuery(
                    boolQuery,
                    BooleanClause.Occur.SHOULD,
                    mappingType.getValueField(sf),
                    valueSearch);
              });

      BooleanQuery query = boolQuery.build();
      if (log.isDebugEnabled()) {
        log.debug("Parsed query: {}", query.toString());
      }
      return query;
    }

    /** create and add a sub-query, for key or value */
    private void addSubQuery(
        BooleanQuery.Builder boolQuery,
        BooleanClause.Occur occur,
        SchemaField field,
        String search) {
      if (log.isDebugEnabled()) {
        log.debug("Create sub-query for: {}", search);
      }
      if (search != null) {
        if (search.equals("*")) {
          Query existQ = field.getType().getExistenceQuery(this, field);
          boolQuery.add(existQ, occur);
          return;
        } else {
          if (search.startsWith("[") && search.endsWith("]")) {
            String decoded =
                URLDecoder.decode(search.substring(1, search.length() - 1), StandardCharsets.UTF_8);
            String[] parts = decoded.split("TO");
            String min = parts[0].trim();
            String max = parts[1].trim();
            Query rangeQ =
                field
                    .getType()
                    .getRangeQuery(
                        this,
                        field,
                        "*".equals(min) ? null : min,
                        "*".equals(max) ? null : max,
                        true,
                        true);
            boolQuery.add(rangeQ, occur);
            return;
          } else if (search.contains("*") || search.contains("?")) {
            Query termQ = new WildcardQuery(new Term(field.getName(), search));
            boolQuery.add(termQ, occur);
            return;
          } else {
            Query termQ = field.getType().getFieldTermQuery(this, field, search);
            boolQuery.add(termQ, occur);
          }
        }
      }
    }
  }
}
