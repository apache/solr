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

package org.apache.solr.update.processor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.RealTimeGetComponent;
import org.apache.solr.handler.component.RealTimeGetComponent.Resolution;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;

/**
 * An implementation of {@link UpdateRequestProcessor} which computes a hash of field values, and
 * uses this hash to reject/accept document updates.
 *
 * <ul>
 *   <li>When no corresponding document with same id exists (create), the computed hash is added to
 *       the document.
 *   <li>When a previous document exists (update), a new hash is computed from the incoming field
 *       values and compared with the stored hash.
 * </ul>
 *
 * <p>Depending on {#dropSameDocuments} value, this processor may drop or accept document updates.
 * This implementation can be used for monitoring or dropping no-op updates (updates that do not
 * change the Solr document content).
 *
 * <p>Note: the hash is computed using {@link Lookup3Signature} and must be stored in a field with
 * docValues enabled for retrieval.
 *
 * @see Lookup3Signature
 */
public class ContentHashVersionProcessor extends UpdateRequestProcessor {
  private final SchemaField hashField;
  private final SolrQueryResponse rsp;
  private final SolrCore core;
  private final Predicate<SolrInputField> includedFields; // Matcher for included fields in hash
  private final Predicate<SolrInputField> excludedFields; // Matcher for excluded fields from hash
  private boolean dropSameDocuments;
  private int sameCount = 0;
  private int differentCount = 0;

  public ContentHashVersionProcessor(
      Predicate<SolrInputField> hashIncludedFields,
      Predicate<SolrInputField> hashExcludedFields,
      String hashFieldName,
      boolean dropSameDocuments,
      SolrQueryRequest req,
      SolrQueryResponse rsp,
      UpdateRequestProcessor next) {
    super(next);
    this.core = req.getCore();

    IndexSchema schema = core.getLatestSchema();
    this.hashField = schema.getField(hashFieldName);
    this.dropSameDocuments = dropSameDocuments;
    this.rsp = rsp;
    this.includedFields = hashIncludedFields;
    this.excludedFields = hashExcludedFields;
  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    SolrInputDocument newDoc = cmd.getSolrInputDocument();
    byte[] newHash = computeDocHash(newDoc);
    newDoc.setField(hashField.getName(), newHash);

    if (!isHashAcceptable(cmd.getIndexedId(), newHash)) {
      return;
    }
    super.processAdd(cmd);
  }

  @Override
  public void finish() throws IOException {
    try {
      super.finish();
    } finally {
      if (sameCount + differentCount > 0) {
        if (dropSameDocuments) {
          rsp.addToLog("contentHash.duplicatesDropped", sameCount);
        } else {
          rsp.addToLog("contentHash.duplicatesDetected", sameCount);
        }
      }
    }
  }

  private boolean isHashAcceptable(BytesRef indexedDocId, byte[] newHash) throws IOException {
    assert null != indexedDocId;

    Optional<byte[]> oldDocHash = getOldDocHash(indexedDocId);
    if (oldDocHash.isPresent()) {
      if (Arrays.equals(newHash, oldDocHash.get())) {
        sameCount++;
        return !dropSameDocuments;
      } else {
        differentCount++;
        return true;
      }
    }
    return true; // Doc not found
  }

  /** Retrieves the hash value from the old document identified by the given ID. */
  private Optional<byte[]> getOldDocHash(BytesRef indexedDocId) throws IOException {
    SolrInputDocument oldDoc =
        RealTimeGetComponent.getInputDocument(
            core, indexedDocId, indexedDocId, null, Set.of(hashField.getName()), Resolution.DOC);
    if (oldDoc == null) {
      return Optional.empty();
    }
    Object o = oldDoc.getFieldValue(hashField.getName());
    if (o instanceof byte[] bytes) {
      return Optional.of(bytes);
    } else if (o instanceof ByteBuffer buf) {
      byte[] bytes = new byte[buf.remaining()];
      buf.duplicate().get(bytes);
      return Optional.of(bytes);
    }
    return Optional.empty();
  }

  byte[] computeDocHash(SolrInputDocument doc) {
    final Signature sig = new Lookup3Signature();

    // Stream field names, filter, sort, and process in a single pass
    doc.values().stream()
        .filter(includedFields) // Keep fields that match 'included fields' matcher
        .filter(excludedFields.negate()) // Exclude fields that match 'excluded fields' matcher
        .sorted(
            Comparator.comparing(
                SolrInputField
                    ::getName)) // Sort to ensure consistent field order across different doc field
        // orders
        .forEach(
            inputField -> {
              sig.add(inputField.getName());
              Object o = inputField.getValue();
              if (o instanceof Collection) {
                for (Object oo : (Collection<?>) o) {
                  sig.add(String.valueOf(oo));
                }
              } else {
                sig.add(String.valueOf(o));
              }
            });

    return sig.getSignature();
  }
}
