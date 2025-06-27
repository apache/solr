/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.solr.savedsearch.update;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.InvertableType;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.monitor.MonitorQuery;
import org.apache.lucene.monitor.Presearcher;
import org.apache.lucene.monitor.QueryDecomposer;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.savedsearch.SavedSearchDataValues;
import org.apache.solr.savedsearch.SavedSearchDataValues.QueryDisjunct;
import org.apache.solr.savedsearch.SimpleQueryParser;
import org.apache.solr.savedsearch.search.ReverseSearchComponent;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.apache.solr.util.plugin.SolrCoreAware;

public class SavedSearchUpdateProcessorFactory extends UpdateRequestProcessorFactory
    implements SolrCoreAware {

  private QueryDecomposer queryDecomposer;
  private Presearcher presearcher;

  @Override
  public UpdateRequestProcessor getInstance(
      SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    return new SavedSearchUpdateRequestProcessor(next, req.getCore(), queryDecomposer, presearcher);
  }

  @Override
  public void inform(SolrCore core) {
    ReverseSearchComponent rsc =
        (ReverseSearchComponent)
            core.getSearchComponents().get(ReverseSearchComponent.COMPONENT_NAME);
    presearcher = rsc.getPresearcher();
    queryDecomposer = rsc.getQueryDecomposer();
  }

  static class SavedSearchUpdateRequestProcessor extends UpdateRequestProcessor {

    private final SolrCore core;
    private final IndexSchema indexSchema;
    private final QueryDecomposer queryDecomposer;
    private final Presearcher presearcher;
    private final Set<String> allowedFieldNames;
    private final SchemaField queryIdField;
    private final SchemaField monitorQueryField;

    public SavedSearchUpdateRequestProcessor(
        UpdateRequestProcessor next,
        SolrCore core,
        QueryDecomposer queryDecomposer,
        Presearcher presearcher) {
      super(next);
      this.core = core;
      this.indexSchema = core.getLatestSchema();
      this.queryDecomposer = queryDecomposer;
      this.presearcher = presearcher;
      this.queryIdField = indexSchema.getField(SavedSearchDataValues.QUERY_ID);
      this.monitorQueryField = indexSchema.getField(SavedSearchDataValues.MONITOR_QUERY);
      this.allowedFieldNames =
          Set.of(
              SavedSearchDataValues.MONITOR_QUERY,
              indexSchema.getUniqueKeyField().getName(),
              CommonParams.VERSION_FIELD);
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      var solrInputDocument = cmd.getSolrInputDocument();
      String idFieldName = indexSchema.getUniqueKeyField().getName();
      var queryId = (String) solrInputDocument.getFieldValue(idFieldName);
      var queryFieldValue = solrInputDocument.getFieldValue(SavedSearchDataValues.MONITOR_QUERY);
      if (queryFieldValue == null) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Document is missing mandatory "
                + SavedSearchDataValues.MONITOR_QUERY
                + " field which is required by "
                + getClass().getSimpleName()
                + ".");
      }
      var unsupportedFields =
          solrInputDocument.getFieldNames().stream()
              .filter(name -> !allowedFieldNames.contains(name))
              .collect(Collectors.joining(", "));
      if (!unsupportedFields.isEmpty()) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Request contains fields not supported by "
                + getClass().getSimpleName()
                + ": "
                + unsupportedFields);
      }
      List<SolrInputDocument> children =
          Optional.of(queryFieldValue)
              .filter(String.class::isInstance)
              .map(String.class::cast)
              .map(
                  queryStr ->
                      new MonitorQuery(
                          queryId, SimpleQueryParser.parse(queryStr, core), queryStr, Map.of()))
              .stream()
              .flatMap(this::decompose)
              .map(this::toSolrInputDoc)
              .collect(Collectors.toList());
      if (children.isEmpty()) {
        throw new SolrException(
            SolrException.ErrorCode.INVALID_STATE, "Query could not be decomposed");
      }
      SolrInputDocument firstChild = children.get(0);
      if (solrInputDocument.hasChildDocuments()) {
        solrInputDocument.getChildDocuments().clear();
      }
      solrInputDocument.addChildDocuments(children.stream().skip(1).collect(Collectors.toList()));
      for (var firstChildField : firstChild) {
        solrInputDocument.setField(firstChildField.getName(), firstChildField.getValue());
      }
      super.processAdd(cmd);
    }

    private Stream<Document> decompose(MonitorQuery monitorQuery) {
      return QueryDisjunct.decompose(monitorQuery, queryDecomposer).stream()
          .map(
              disjunct -> {
                Document doc =
                    presearcher.indexQuery(disjunct.getMatchQuery(), monitorQuery.getMetadata());
                doc.add(indexSchema.getUniqueKeyField().createField(disjunct.getId()));
                doc.add(queryIdField.createField(disjunct.getQueryId()));
                doc.add(monitorQueryField.createField(monitorQuery.getQueryString()));
                return doc;
              });
    }

    private SolrInputDocument toSolrInputDoc(Document subDocument) {
      SolrInputDocument out = new SolrInputDocument();
      for (var f : subDocument.getFields()) {
        Object existing = out.get(f.name());
        if (existing == null) {
          SchemaField sf = indexSchema.getFieldOrNull(f.name());
          if (sf != null && sf.multiValued()) {
            List<Object> vals = new ArrayList<>();
            if (f.fieldType().docValuesType() == DocValuesType.SORTED_NUMERIC) {
              vals.add(sf.getType().toObject(f));
            } else {
              vals.add(materialize(f));
            }
            out.setField(f.name(), vals);
          } else {
            out.setField(f.name(), materialize(f));
          }
        } else {
          out.addField(f.name(), materialize(f));
        }
      }
      return out;
    }

    private static Object materialize(IndexableField in) {
      if (in instanceof Field && ((Field) in).tokenStreamValue() != null) {
        return new DerivedSkipTLogField((Field) in);
      }
      if (in.numericValue() != null) {
        return in.numericValue();
      }
      if (in.binaryValue() != null) {
        return in.binaryValue();
      }
      if (in.stringValue() != null) {
        return in.stringValue();
      }
      throw new IllegalArgumentException("could not clone field " + in.name());
    }

    private static class DerivedSkipTLogField
        implements IndexableField, JavaBinCodec.ObjectResolver {

      private final Field ref;

      public DerivedSkipTLogField(Field ref) {
        this.ref = ref;
      }

      @Override
      public Object resolve(Object o, JavaBinCodec codec) {
        return "";
      }

      @Override
      public String name() {
        return ref.name();
      }

      @Override
      public IndexableFieldType fieldType() {
        return ref.fieldType();
      }

      @Override
      public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
        return ref.tokenStream(analyzer, reuse);
      }

      @Override
      public BytesRef binaryValue() {
        return ref.binaryValue();
      }

      @Override
      public String stringValue() {
        return ref.stringValue();
      }

      @Override
      public CharSequence getCharSequenceValue() {
        return ref.getCharSequenceValue();
      }

      @Override
      public Reader readerValue() {
        return ref.readerValue();
      }

      @Override
      public Number numericValue() {
        return ref.numericValue();
      }

      @Override
      public StoredValue storedValue() {
        return null;
      }

      @Override
      public InvertableType invertableType() {
        return InvertableType.TOKEN_STREAM;
      }
    }
  }
}
