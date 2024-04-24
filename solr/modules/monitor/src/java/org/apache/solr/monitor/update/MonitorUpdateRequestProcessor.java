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

package org.apache.solr.monitor.update;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.apache.lucene.monitor.MonitorFields;
import org.apache.lucene.monitor.MonitorQuery;
import org.apache.lucene.monitor.Presearcher;
import org.apache.lucene.monitor.QCEVisitor;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.core.SolrCore;
import org.apache.solr.monitor.MonitorConstants;
import org.apache.solr.monitor.MonitorSchemaFields;
import org.apache.solr.monitor.SimpleQueryParser;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;

public class MonitorUpdateRequestProcessor extends UpdateRequestProcessor {

  private final SolrCore core;
  private final IndexSchema indexSchema;
  private final Presearcher presearcher;
  private final MonitorSchemaFields monitorSchemaFields;

  public MonitorUpdateRequestProcessor(
      UpdateRequestProcessor next, SolrCore core, Presearcher presearcher) {
    super(next);
    this.core = core;
    this.indexSchema = core.getLatestSchema();
    this.presearcher = presearcher;
    this.monitorSchemaFields = new MonitorSchemaFields(indexSchema);
  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    var solrInputDocument = cmd.getSolrInputDocument();
    var queryId =
        (String) solrInputDocument.getFieldValue(indexSchema.getUniqueKeyField().getName());
    var queryFieldValue = solrInputDocument.getFieldValue(MonitorFields.MONITOR_QUERY);
    if (queryFieldValue != null) {
      var payload =
          Optional.ofNullable(solrInputDocument.getFieldValue(MonitorFields.PAYLOAD))
              .map(Object::toString)
              .orElse(null);
      List<SolrInputDocument> children =
          Optional.of(queryFieldValue)
              .filter(String.class::isInstance)
              .map(String.class::cast)
              .map(
                  queryStr ->
                      new MonitorQuery(
                          queryId, SimpleQueryParser.parse(queryStr, core), queryStr, Map.of()))
              .stream()
              .flatMap(monitorQuery -> decompose(monitorQuery, payload))
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
      if (solrInputDocument.hasChildDocuments()) {
        solrInputDocument
            .getChildDocuments()
            .forEach(
                child ->
                    solrInputDocument.forEach(
                        field -> {
                          if (!MonitorFields.RESERVED_MONITOR_FIELDS.contains(field.getName())) {
                            child.addField(field.getName(), field.getValue());
                          }
                        }));
        solrInputDocument
            .getChildDocuments()
            .forEach(
                child ->
                    child.setField(
                        indexSchema.getUniqueKeyField().getName(),
                        child.getFieldValue(MonitorFields.CACHE_ID)));
      }
      copyFirstChildToParent(solrInputDocument, firstChild);
    }
    super.processAdd(cmd);
  }

  private void copyFirstChildToParent(SolrInputDocument parent, SolrInputDocument firstChild) {
    parent.setField(
        indexSchema.getUniqueKeyField().getName(),
        firstChild.getFieldValue(MonitorFields.CACHE_ID));
    for (var firstBornInputField : firstChild) {
      parent.setField(firstBornInputField.getName(), firstBornInputField.getValue());
    }
  }

  private Stream<Document> decompose(MonitorQuery monitorQuery, String payload) {
    return QCEVisitor.decompose(monitorQuery, MonitorConstants.QUERY_DECOMPOSER).stream()
        .map(
            qce -> {
              Document doc =
                  presearcher.indexQuery(qce.getMatchQuery(), monitorQuery.getMetadata());
              doc.add(monitorSchemaFields.getQueryId().createField(qce.getQueryId()));
              doc.add(monitorSchemaFields.getCacheId().createField(qce.getCacheId()));
              doc.add(
                  monitorSchemaFields.getMonitorQuery().createField(monitorQuery.getQueryString()));
              if (payload != null) {
                doc.add(monitorSchemaFields.getPayload().createField(payload));
              }
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

  private static class DerivedSkipTLogField implements IndexableField, JavaBinCodec.ObjectResolver {

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
