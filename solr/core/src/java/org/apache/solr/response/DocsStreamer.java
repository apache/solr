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
package org.apache.solr.response;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrDocumentFetcher;
import org.apache.solr.search.SolrReturnFields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This streams SolrDocuments from a DocList and applies transformer */
public class DocsStreamer implements Iterator<SolrDocument> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final Object FAILED_STORED_VALUE = new Object();

  private final ResultContext rctx;
  private final SolrDocumentFetcher docFetcher; // a collaborator of SolrIndexSearcher
  private final DocList docs;

  private final DocTransformer transformer;
  private final DocIterator docIterator;

  private final SolrReturnFields solrReturnFields;

  private int idx = -1;

  public DocsStreamer(ResultContext rctx) {
    this.rctx = rctx;
    this.docs = rctx.getDocList();
    transformer = rctx.getReturnFields().getTransformer();
    docIterator = this.docs.iterator();
    docFetcher = rctx.getDocFetcher();
    solrReturnFields = (SolrReturnFields) rctx.getReturnFields();

    if (transformer != null) {
      transformer.setContext(rctx);
    }
  }

  public int currentIndex() {
    return idx;
  }

  @Override
  public boolean hasNext() {
    return docIterator.hasNext();
  }

  @Override
  public SolrDocument next() {
    int id = docIterator.nextDoc();
    idx++;
    SolrDocument sdoc = docFetcher.solrDoc(id, solrReturnFields);

    if (transformer != null) {
      try {
        transformer.transform(sdoc, id, docIterator);
      } catch (IOException e) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR, "Error applying transformer", e);
      }
    }
    return sdoc;
  }

  /**
   * Converts the specified <code>Document</code> into a <code>SolrDocument</code>.
   *
   * <p>The use of {@link ReturnFields} can be important even when it was already used to retrieve
   * the {@link Document} from {@link SolrDocumentFetcher} because the Document may have been cached
   * with more fields then are desired.
   *
   * @param doc <code>Document</code> to be converted, must not be null
   * @param schema <code>IndexSchema</code> containing the field/fieldType details for the index the
   *     <code>Document</code> came from, must not be null.
   * @param fields <code>ReturnFields</code> instance that can be use to limit the set of fields
   *     that will be converted, must not be null
   */
  public static SolrDocument convertLuceneDocToSolrDoc(
      Document doc, final IndexSchema schema, final ReturnFields fields) {
    // TODO move to SolrDocumentFetcher ?  Refactor to also call
    // docFetcher.decorateDocValueFields(...) ?
    assert null != doc;
    assert null != schema;
    assert null != fields;

    // can't just use fields.wantsField(String)
    // because that doesn't include extra fields needed by transformers
    final Set<String> fieldNamesNeeded = fields.getLuceneFieldNames();

    JavaBinResponseWriter.MaskCharSeqSolrDocument masked = null;
    final SolrDocument out =
        ResultContext.READASBYTES.get() == null
            ? new SolrDocument()
            : (masked = new JavaBinResponseWriter.MaskCharSeqSolrDocument());

    // NOTE: it would be tempting to try and optimize this to loop over fieldNamesNeeded when it's
    // smaller then the IndexableField[] in the Document -- but that's actually *less* effecient
    // since Document.getFields(String) does a full (internal) iteration over the full
    // IndexableField[]. see SOLR-11891
    for (IndexableField f : doc.getFields()) {
      final String fname = f.name();
      if (null == fieldNamesNeeded || fieldNamesNeeded.contains(fname)) {
        // Make sure multivalued fields are represented as lists
        Object existing = masked == null ? out.get(fname) : masked.getRaw(fname);
        if (existing == null) {
          SchemaField sf = schema.getFieldOrNull(fname);
          if (sf != null && sf.multiValued()) {
            List<Object> vals = new ArrayList<>();
            vals.add(f);
            out.setField(fname, vals);
          } else {
            out.setField(fname, f);
          }
        } else {
          out.addField(fname, f);
        }
      }
    }
    return out;
  }

  @Override
  public void remove() { // do nothing
  }

  /**
   * Replace Lucene {@link IndexableField} values on a {@link SolrDocument} (and nested / child
   * documents) with the SolrJ-native objects that clients see after JavaBin deserialization. The
   * serialized JavaBin path already converts stored values through JavaBinResponseWriter.Resolver;
   * this exists for the EmbeddedSolrServer streaming path, whose codec hands documents straight to
   * the callback, so {@code queryAndStreamResponse} matches {@code query} / {@code HttpSolrClient}.
   *
   * <p>A stored value that cannot be converted is logged and omitted; conversion continues for the
   * remaining values and fields.
   *
   * <p>Do not call this from {@link #convertLuceneDocToSolrDoc}; JSON/XML writers and some
   * transformers still expect stored fields as {@link IndexableField}.
   *
   * @see #getValue(SchemaField, IndexableField)
   */
  public static SolrDocument externalizeStoredValues(SolrDocument doc, IndexSchema schema) {
    if (doc == null || schema == null) {
      return doc;
    }
    List<String> failedFields = null;
    for (Iterator<Map.Entry<String, Object>> it = doc.iterator(); it.hasNext(); ) {
      Map.Entry<String, Object> entry = it.next();
      Object val = entry.getValue();
      Object converted = externalizeValue(val, schema);
      if (FAILED_STORED_VALUE.equals(converted)) {
        // The document's entry iterator does not support remove(), so collect the
        // failed fields and remove them from the document after the loop instead.
        if (failedFields == null) {
          failedFields = new ArrayList<>();
        }
        failedFields.add(entry.getKey());
      } else if (!Objects.equals(converted, val)) {
        entry.setValue(converted);
      }
    }
    if (failedFields != null) {
      for (String failedField : failedFields) {
        doc.remove(failedField);
      }
    }
    List<SolrDocument> children = doc.getChildDocuments();
    if (children != null) {
      for (SolrDocument child : children) {
        externalizeStoredValues(child, schema);
      }
    }
    return doc;
  }

  private static Object externalizeValue(Object val, IndexSchema schema) {
    if (val instanceof IndexableField f) {
      try {
        return getValue(schema.getFieldOrNull(f.name()), f);
      } catch (Exception | AssertionError e) {
        // AssertionError: point field types (IntPointField, DatePointField, ...) throw it
        // from toObject when a stored value predates a change to a point type.
        log.warn("Error reading a field : {}", f, e);
        return FAILED_STORED_VALUE;
      }
    }
    if (val instanceof SolrDocument nested) {
      return externalizeStoredValues(nested, schema);
    }
    if (val instanceof Collection<?> coll) {
      List<Object> out = new ArrayList<>(coll.size());
      boolean changed = false;
      for (Object item : coll) {
        Object converted = externalizeValue(item, schema);
        if (FAILED_STORED_VALUE.equals(converted)) {
          changed = true;
          continue;
        }
        changed |= !Objects.equals(converted, item);
        out.add(converted);
      }
      return changed ? (out.isEmpty() ? FAILED_STORED_VALUE : out) : val;
    }
    return val;
  }

  public static Object getValue(SchemaField sf, IndexableField f) {
    FieldType ft = null;
    if (sf != null) {
      ft = sf.getType();
    }

    if (ft == null) { // handle fields not in the schema
      BytesRef bytesRef = f.binaryValue();
      if (bytesRef != null) {
        if (bytesRef.offset == 0 && bytesRef.length == bytesRef.bytes.length) {
          return bytesRef.bytes;
        } else {
          final byte[] bytes = new byte[bytesRef.length];
          System.arraycopy(bytesRef.bytes, bytesRef.offset, bytes, 0, bytesRef.length);
          return bytes;
        }
      } else {
        return f.stringValue();
      }
    } else {
      if (ft instanceof FieldType.ExternalizeStoredValuesAsObjects) {
        return ft.toObject(f);
      } else {
        return ft.toExternal(f);
      }
    }
  }
}
