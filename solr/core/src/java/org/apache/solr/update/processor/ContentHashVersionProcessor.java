package org.apache.solr.update.processor;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.RealTimeGetComponent;
import org.apache.solr.handler.component.RealTimeGetComponent.Resolution;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TextField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.UpdateCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

/**
 * An implementation of {@link UpdateRequestProcessor} which computes a hash of selected doc values, and uses this hash
 * value to reject/accept doc updates.
 * <ul>
 * <li>When no corresponding doc with same id exists (create), computed hash is added to the document.</li>
 * <li>When a previous doc exists (update), a new hash is computed using new version values and compared with old hash.</li>
 * </ul>
 * Depending on {#discardSameDocuments} value, this processor may reject or accept doc update.
 * This implementation can be used for monitoring or rejecting no-op updates (updates that do not change Solr document).
 * <p>
 * Note: hash is computed using {@link Lookup3Signature}.
 * </p>
 * @see Lookup3Signature
 */
public class ContentHashVersionProcessor extends UpdateRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final SchemaField hashField;
    private final SolrQueryResponse rsp;
    private final SolrCore core;
    private final Predicate<String> includedFields; // Matcher for included fields in hash
    private final Predicate<String> excludedFields; // Matcher for excluded fields from hash
    private OldDocProvider oldDocProvider = new DefaultDocProvider();
    private boolean discardSameDocuments;
    private int sameCount = 0;
    private int differentCount = 0;
    private int unknownCount = 0;

    public ContentHashVersionProcessor(Predicate<String> hashIncludedFields, Predicate<String> hashExcludedFields, String hashFieldName, SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
        super(next);
        this.core = req.getCore();
        this.hashField = new SchemaField(hashFieldName, new TextField());
        this.rsp = rsp;
        this.includedFields = hashIncludedFields;
        this.excludedFields = hashExcludedFields;
    }

    public void processAdd(AddUpdateCommand cmd) throws IOException {
        SolrInputDocument newDoc = cmd.getSolrInputDocument();
        String newHash = computeDocHash(newDoc);
        newDoc.setField(hashField.getName(), newHash);
        int i = 0;

        if (!validateHash(cmd.getIndexedId(), newHash)) {
            return;
        }

        while (true) {
            logOverlyFailedRetries(i, cmd);
            try {
                super.processAdd(cmd);
                return;
            } catch (SolrException e) {
                if (e.code() != 409) {
                    throw e;
                }
                ++i;
            }
        }
    }

    @Override
    public void finish() throws IOException {
        try {
            super.finish();
        } finally {
            rsp.addToLog("numAddsExisting", sameCount + differentCount + unknownCount);
            rsp.addToLog("numAddsExistingWithIdentical", sameCount);
            rsp.addToLog("numAddsExistingUnknown", unknownCount);
        }
    }

    private static void logOverlyFailedRetries(int i, UpdateCommand cmd) {
        if ((i & 255) == 255) {
            log.warn("Unusual number of optimistic concurrency retries: retries={} cmd={}", i, cmd);
        }
    }

    void setOldDocProvider(OldDocProvider oldDocProvider) {
        this.oldDocProvider = oldDocProvider;
    }

    void setDiscardSameDocuments(boolean discardSameDocuments) {
        this.discardSameDocuments = discardSameDocuments;
    }

    private boolean validateHash(BytesRef indexedDocId, String newHash) throws IOException {
        assert null != indexedDocId;

        var docFoundAndOldUserVersions = getOldUserVersionsFromStored(indexedDocId);
        if (docFoundAndOldUserVersions.found) {
            String oldHash = docFoundAndOldUserVersions.oldHash; // No hash: might want to keep track of these too
            if (oldHash == null) {
                unknownCount++;
                return true;
            } else if (Objects.equals(newHash, oldHash)) {
                sameCount++;
                return !discardSameDocuments;
            } else {
                differentCount++;
                return true;
            }
        }
        return true; // Doc not found
    }

    private DocFoundAndOldUserAndSolrVersions getOldUserVersionsFromStored(BytesRef indexedDocId) throws IOException {
        SolrInputDocument oldDoc = oldDocProvider.getDocument(core, hashField.getName(), indexedDocId);
        return null == oldDoc ? DocFoundAndOldUserAndSolrVersions.NOT_FOUND : getUserVersionAndSolrVersionFromDocument(oldDoc);
    }

    private DocFoundAndOldUserAndSolrVersions getUserVersionAndSolrVersionFromDocument(SolrInputDocument oldDoc) {
        Object o = oldDoc.getFieldValue(hashField.getName());
        if (o != null) {
            return new DocFoundAndOldUserAndSolrVersions(o.toString());
        }
        return new DocFoundAndOldUserAndSolrVersions();
    }

    public String computeDocHash(SolrInputDocument doc) {
        List<String> docIncludedFieldNames = doc.getFieldNames().stream()
                .filter(includedFields) // Keep fields that match 'included fields' matcher...
                .filter(excludedFields.negate()) // ...and exclude fields that match 'excluded fields' matcher
                .sorted() // Sort to ensure consistent field order across different doc field orders
                .toList();

        final Signature sig = new Lookup3Signature();
        for (String fieldName : docIncludedFieldNames) {
            sig.add(fieldName);
            Object o = doc.getFieldValue(fieldName);
            if (o instanceof Collection) {
                for (Object oo : (Collection<?>) o) {
                    sig.add(String.valueOf(oo));
                }
            } else {
                sig.add(String.valueOf(o));
            }
        }

        // Signature, depending on implementation, may return 8-byte or 16-byte value
        byte[] signature = sig.getSignature();
        return Base64.getEncoder().encodeToString(signature); // Makes a base64 hash out of signature value
    }

    interface OldDocProvider {
        SolrInputDocument getDocument(SolrCore core, String hashField, BytesRef indexedDocId) throws IOException;
    }

    private static class DefaultDocProvider implements OldDocProvider {
        @Override
        public SolrInputDocument getDocument(SolrCore core, String hashField, BytesRef indexedDocId) throws IOException {
            return RealTimeGetComponent.getInputDocument(core, indexedDocId, indexedDocId, null, Set.of(hashField), Resolution.PARTIAL);
        }
    }

    private static class DocFoundAndOldUserAndSolrVersions {
        private static final DocFoundAndOldUserAndSolrVersions NOT_FOUND = new DocFoundAndOldUserAndSolrVersions();
        private final boolean found;

        public String oldHash;

        private DocFoundAndOldUserAndSolrVersions() {
            this.found = false;
        }
        private DocFoundAndOldUserAndSolrVersions(String oldHash) {
            this.found = true;
            this.oldHash = oldHash;
        }

    }
}
