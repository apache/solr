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
import java.io.Writer;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.FastWriter;
import org.apache.solr.common.util.TextWriter;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocList;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrReturnFields;

/** Base class for text-oriented response writers. */
public abstract class TextResponseWriter implements TextWriter {

  protected final FastWriter writer;
  protected final IndexSchema schema;
  protected final SolrQueryRequest req;
  protected final SolrQueryResponse rsp;

  // the default set of fields to return for each document
  protected ReturnFields returnFields;

  protected int level;
  protected boolean doIndent;

  protected Calendar cal; // reusable calendar instance

  /**
   * A signal object that must be used to differentiate from <code>null</code> in strict object
   * equality checks against {@link #rawReturnFields}, in order to determine the appropriate context
   * in which to write raw field values.
   */
  private static final ReturnFields NO_RAW_FIELDS = new SolrReturnFields();

  private final TextResponseWriter rawShim;
  private final Set<String> rawFields;
  private final ReturnFields rawReturnFields;

  public TextResponseWriter(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) {
    this.writer = writer == null ? null : FastWriter.wrap(writer);
    this.schema = req.getSchema();
    this.req = req;
    this.rsp = rsp;
    String indent = req.getParams().get("indent");
    if (null == indent || !("off".equals(indent) || "false".equals(indent))) {
      doIndent = true;
    }
    returnFields = rsp.getReturnFields();
    if (req.getParams().getBool(CommonParams.OMIT_HEADER, false)) rsp.removeResponseHeader();
    DocTransformer rootDocTransformer = returnFields.getTransformer();
    Collection<String> rawFields;
    if (rootDocTransformer == null || (rawFields = rootDocTransformer.getRawFields()).isEmpty()) {
      this.rawFields = null;
      this.rawShim = null;
      this.rawReturnFields = NO_RAW_FIELDS;
    } else {
      this.rawFields =
          rawFields.size() == 1
              ? Collections.singleton(rawFields.iterator().next())
              : new HashSet<>(rawFields);
      this.rawShim = new RawShimTextResponseWriter(this);
      this.rawReturnFields = returnFields;
    }
  }
  // only for test purposes
  TextResponseWriter(Writer writer, boolean indent) {
    this.writer = writer == null ? null : FastWriter.wrap(writer);
    this.schema = null;
    this.req = null;
    this.rsp = null;
    returnFields = null;
    this.doIndent = indent;
    this.rawShim = null;
    this.rawFields = null;
    this.rawReturnFields = null;
  }

  /**
   * NOTE: strict object equality check against {@link #rawReturnFields}; see javadocs for {@link
   * #NO_RAW_FIELDS}
   */
  protected final boolean shouldWriteRaw(String fname, ReturnFields returnFields) {
    return rawReturnFields == returnFields && rawFields.contains(fname);
  }

  /** done with this ResponseWriter... make sure any buffers are flushed to writer */
  @Override
  public void close() throws IOException {
    if (writer != null) writer.flushBuffer();
  }

  @Override
  public boolean doIndent() {
    return doIndent;
  }

  /** returns the Writer that the response is being written to */
  @Override
  public Writer getWriter() {
    return writer;
  }

  //
  // Functions to manipulate the current logical nesting level.
  // Any indentation will be partially based on level.
  //
  public void setLevel(int level) {
    this.level = level;
  }

  @Override
  public int level() {
    return level;
  }

  @Override
  public int incLevel() {
    return ++level;
  }

  @Override
  public int decLevel() {
    return --level;
  }

  @Override
  public TextResponseWriter setIndent(boolean doIndent) {
    this.doIndent = doIndent;
    return this;
  }

  @Override
  public final void writeVal(String name, Object val, boolean raw) throws IOException {

    // if there get to be enough types, perhaps hashing on the type
    // to get a handler might be faster (but types must be exact to do that...)
    //    (see a patch on LUCENE-3041 for inspiration)

    // go in order of most common to least common, however some of the more general types like Map
    // belong towards the end
    if (val == null) {
      writeNull(name);
      return;
    }

    if (val instanceof IndexableField) {
      IndexableField f = (IndexableField) val;
      SchemaField sf = schema.getFieldOrNull(f.name());
      if (sf != null) {
        sf.getType().write(raw ? rawShim : this, name, f);
      } else if (raw) {
        writeStrRaw(name, f.stringValue());
      } else {
        writeStr(name, f.stringValue(), true);
      }
    } else if (val instanceof Document) {
      SolrDocument doc =
          DocsStreamer.convertLuceneDocToSolrDoc((Document) val, schema, returnFields);
      writeSolrDocument(name, doc, returnFields, 0);
    } else if (val instanceof SolrDocument) {
      writeSolrDocument(name, (SolrDocument) val, returnFields, 0);
    } else if (val instanceof ResultContext) {
      // requires access to IndexReader
      writeDocuments(name, (ResultContext) val);
    } else if (val instanceof DocList) {
      // Should not happen normally
      ResultContext ctx = new BasicResultContext((DocList) val, returnFields, null, null, req);
      writeDocuments(name, ctx);
      // }
      // else if (val instanceof DocSet) {
      // how do we know what fields to read?
      // todo: have a DocList/DocSet wrapper that
      // restricts the fields to write...?
    } else if (val instanceof SolrDocumentList) {
      writeSolrDocumentList(name, (SolrDocumentList) val, returnFields);
    } else if (val instanceof BytesRef) {
      BytesRef arr = (BytesRef) val;
      writeByteArr(name, arr.bytes, arr.offset, arr.length);
    } else {
      TextWriter.super.writeVal(name, val, raw);
    }
  }
  // names are passed when writing primitives like writeInt to allow many different
  // types of formats, including those where the name may come after the value (like
  // some XML formats).

  public abstract void writeStartDocumentList(
      String name, long start, int size, long numFound, Float maxScore, Boolean numFoundExact)
      throws IOException;

  public abstract void writeSolrDocument(
      String name, SolrDocument doc, ReturnFields fields, int idx) throws IOException;

  public abstract void writeEndDocumentList() throws IOException;

  // Assume each SolrDocument is already transformed
  public final void writeSolrDocumentList(String name, SolrDocumentList docs, ReturnFields fields)
      throws IOException {
    writeStartDocumentList(
        name,
        docs.getStart(),
        docs.size(),
        docs.getNumFound(),
        docs.getMaxScore(),
        docs.getNumFoundExact());
    for (int i = 0; i < docs.size(); i++) {
      writeSolrDocument(null, docs.get(i), fields, i);
    }
    writeEndDocumentList();
  }

  public final void writeDocuments(String name, ResultContext res) throws IOException {
    DocList ids = res.getDocList();
    Iterator<SolrDocument> docsStreamer = res.getProcessedDocuments();
    writeStartDocumentList(
        name,
        ids.offset(),
        ids.size(),
        ids.matches(),
        res.wantsScores() ? ids.maxScore() : null,
        ids.hitCountRelation() == TotalHits.Relation.EQUAL_TO);

    int idx = 0;
    while (docsStreamer.hasNext()) {
      writeSolrDocument(null, docsStreamer.next(), res.getReturnFields(), idx);
      idx++;
    }
    writeEndDocumentList();
  }
}
