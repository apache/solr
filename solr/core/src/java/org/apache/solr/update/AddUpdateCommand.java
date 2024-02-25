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
package org.apache.solr.update;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;

/**
 * An {@link UpdateCommand} for adding or updating one document. Technically more than one Lucene
 * documents may be involved in the event of nested documents.
 */
public class AddUpdateCommand extends UpdateCommand {

  /** Higher level SolrInputDocument, normally used to construct the Lucene Document(s) to index. */
  public SolrInputDocument solrDoc;

  /**
   * This is the version of a document, previously indexed, on which the current update depends on.
   * This version could be that of a previous in-place update or a full update. A negative value
   * here, e.g. -1, indicates that this add update does not depend on a previous update.
   */
  public long prevVersion = -1;

  public boolean overwrite = true;

  /** The term to use to delete an existing document (for dedupe). (optional) */
  public Term updateTerm;

  public int commitWithin = -1;

  public boolean isLastDocInBatch = false;

  private BytesRef indexedId;
  private String indexedIdStr;
  private String selfOrNestedDocIdStr;

  public AddUpdateCommand(SolrQueryRequest req) {
    super(req);
  }

  @Override
  public String name() {
    return "add";
  }

  /** Reset state to reuse this object with a different document in the same request */
  public void clear() {
    solrDoc = null;
    indexedId = null;
    indexedIdStr = null;
    selfOrNestedDocIdStr = null;
    updateTerm = null;
    isLastDocInBatch = false;
    version = 0;
    prevVersion = -1;
  }

  public SolrInputDocument getSolrInputDocument() {
    return solrDoc;
  }

  /**
   * Creates and returns a lucene Document for in-place update. The SolrInputDocument itself may be
   * modified, which will be reflected in the update log. Any changes made to the returned Document
   * will not be reflected in the SolrInputDocument, or future calls to this method.
   */
  Document makeLuceneDocForInPlaceUpdate() {
    // perhaps this should move to UpdateHandler or DocumentBuilder?
    assert isInPlaceUpdate();
    if (req.getSchema().isUsableForChildDocs()
        && solrDoc.getField(IndexSchema.ROOT_FIELD_NAME) == null) {
      solrDoc.setField(IndexSchema.ROOT_FIELD_NAME, getIndexedIdStr());
    }
    final boolean forInPlaceUpdate = true;
    final boolean ignoreNestedDocs = false; // throw an exception if found
    return DocumentBuilder.toDocument(solrDoc, req.getSchema(), forInPlaceUpdate, ignoreNestedDocs);
  }

  /**
   * Returns the indexed ID that we route this document on. Typically, this is for the unique key of
   * the document but for a nested document, it's the ID of the root.
   *
   * @return possibly null if there's no uniqueKey field
   */
  public String getIndexedIdStr() {
    extractIdsIfNeeded();
    return indexedIdStr;
  }

  /**
   * Returns the indexed ID that we route this document on. Typically, this is for the unique key of
   * the document but for a nested document, it's the ID of the root.
   *
   * <p>BytesRef should be treated as immutable. It will not be re-used/modified for additional
   * docs.
   *
   * @return possibly null if there's no uniqueKey field
   */
  public BytesRef getIndexedId() {
    extractIdsIfNeeded();
    return indexedId;
  }

  /**
   * Returns the ID of the doc itself, possibly different from {@link #getIndexedIdStr()} which
   * points to the root doc.
   *
   * @return possibly null if there's no uniqueKey field
   */
  public String getSelfOrNestedDocIdStr() {
    extractIdsIfNeeded();
    return selfOrNestedDocIdStr;
  }

  /** The ID for logging purposes. */
  public String getPrintableId() {
    if (req == null) {
      return "(uninitialized)"; // in tests?
    }
    extractIdsIfNeeded();
    if (indexedIdStr == null) {
      return "(null)";
    } else if (indexedIdStr.equals(selfOrNestedDocIdStr)) {
      return indexedIdStr;
    } else {
      return selfOrNestedDocIdStr + " (root=" + indexedIdStr + ")";
    }
  }

  private void extractIdsIfNeeded() {
    if (indexedId != null) {
      return;
    }
    IndexSchema schema = req.getSchema();
    SchemaField sf = schema.getUniqueKeyField();
    if (sf != null) {
      if (solrDoc != null) {
        SolrInputField field = solrDoc.getField(sf.getName());
        // check some uniqueKey constraints
        int count = field == null ? 0 : field.getValueCount();
        if (count == 0) {
          if (overwrite) {
            throw new SolrException(
                SolrException.ErrorCode.BAD_REQUEST,
                "Document is missing mandatory uniqueKey field: " + sf.getName());
          }
        } else if (count > 1) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Document contains multiple values for uniqueKey field: " + field);
        } else {
          this.selfOrNestedDocIdStr = field.getFirstValue().toString();
          // the root might be in _root_ field; if not then uniqueKeyField.
          this.indexedIdStr =
              (String) solrDoc.getFieldValue(IndexSchema.ROOT_FIELD_NAME); // or here
          if (this.indexedIdStr == null) {
            this.indexedIdStr = selfOrNestedDocIdStr;
          }
          indexedId = schema.indexableUniqueKey(indexedIdStr);
        }
      }
    }
  }

  @VisibleForTesting
  public void setIndexedId(BytesRef indexedId) {
    this.indexedId = indexedId;
    this.indexedIdStr = indexedId.utf8ToString();
    this.selfOrNestedDocIdStr = indexedIdStr;
  }

  /**
   * Computes the final flattened Lucene docs, possibly generating them on-demand (on iteration).
   * The SolrInputDocument itself may be modified, which will be reflected in the update log. This
   * should only be called once. Any changes made to the returned Document(s) will not be reflected
   * in the SolrInputDocument, or future calls to this method.
   */
  Iterable<Document> makeLuceneDocs() {
    // perhaps this should move to UpdateHandler or DocumentBuilder?
    assert !isInPlaceUpdate() : "We don't expect this to happen."; // but should "work"?
    if (!req.getSchema().isUsableForChildDocs()) {
      // note if the doc is nested despite this, we'll throw an exception elsewhere
      final boolean forInPlaceUpdate = false;
      final boolean ignoreNestedDocs = false; // throw an exception if found
      Document doc =
          DocumentBuilder.toDocument(solrDoc, req.getSchema(), forInPlaceUpdate, ignoreNestedDocs);
      return Collections.singleton(doc);
    }

    List<SolrInputDocument> all = flatten(solrDoc);

    final String rootId = getIndexedIdStr();
    final SolrInputField versionSif = solrDoc.get(CommonParams.VERSION_FIELD);

    for (SolrInputDocument sdoc : all) {
      addRootField(sdoc, rootId);
      if (versionSif != null) {
        addVersionField(sdoc, versionSif);
      }
      // TODO: if possible concurrent modification exception (if SolrInputDocument not cloned and is
      // being forwarded to replicas) then we could add this field to the generated lucene document
      // instead.
    }

    return () ->
        all.stream().map(sdoc -> DocumentBuilder.toDocument(sdoc, req.getSchema())).iterator();
  }

  private void addRootField(SolrInputDocument sdoc, String rootId) {
    sdoc.setField(IndexSchema.ROOT_FIELD_NAME, rootId);
  }

  private void addVersionField(SolrInputDocument sdoc, SolrInputField versionSif) {
    // Reordered delete-by-query assumes all documents have a version, see SOLR-10114
    // all docs in hierarchy should have the same version.
    // Either fetch the version from the root doc or compute it and propagate it.
    sdoc.put(CommonParams.VERSION_FIELD, versionSif);
  }

  private List<SolrInputDocument> flatten(SolrInputDocument root) {
    List<SolrInputDocument> unwrappedDocs = new ArrayList<>();
    flattenAnonymous(unwrappedDocs, root, true);
    flattenLabelled(unwrappedDocs, root, true);
    unwrappedDocs.add(root);

    return unwrappedDocs;
  }

  /** Extract all child documents from parent that are saved in fields */
  private void flattenLabelled(
      List<SolrInputDocument> unwrappedDocs, SolrInputDocument currentDoc, boolean isRoot) {
    for (SolrInputField field : currentDoc.values()) {
      Object value = field.getFirstValue();
      // check if value is a childDocument
      if (value instanceof SolrInputDocument) {
        Object val = field.getValue();
        if (!(val instanceof Collection)) {
          flattenLabelled(unwrappedDocs, ((SolrInputDocument) val));
          continue;
        }
        @SuppressWarnings({"unchecked"})
        Collection<SolrInputDocument> childrenList = ((Collection) val);
        for (SolrInputDocument child : childrenList) {
          flattenLabelled(unwrappedDocs, child);
        }
      }
    }

    if (!isRoot) unwrappedDocs.add(currentDoc);
  }

  private void flattenLabelled(
      List<SolrInputDocument> unwrappedDocs, SolrInputDocument currentDoc) {
    if (currentDoc.hasChildDocuments()) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Anonymous child docs can only hang from others or the root: " + currentDoc);
    }
    flattenLabelled(unwrappedDocs, currentDoc, false);
  }

  /** Extract all anonymous child documents from parent. */
  private void flattenAnonymous(
      List<SolrInputDocument> unwrappedDocs, SolrInputDocument currentDoc, boolean isRoot) {
    List<SolrInputDocument> children = currentDoc.getChildDocuments();
    if (children != null) {
      for (SolrInputDocument child : children) {
        flattenAnonymous(unwrappedDocs, child);
      }
    }

    if (!isRoot) unwrappedDocs.add(currentDoc);
  }

  private void flattenAnonymous(
      List<SolrInputDocument> unwrappedDocs, SolrInputDocument currentDoc) {
    flattenAnonymous(unwrappedDocs, currentDoc, false);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(super.toString());
    sb.append(",id=").append(getPrintableId());
    if (!overwrite) sb.append(",overwrite=").append(overwrite);
    if (commitWithin != -1) sb.append(",commitWithin=").append(commitWithin);
    sb.append('}');
    return sb.toString();
  }

  /**
   * Is this add update an in-place update? An in-place update is one where only docValues are
   * updated, and a new document is not indexed.
   */
  public boolean isInPlaceUpdate() {
    return (prevVersion >= 0);
  }
}
