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
package org.apache.solr.handler.admin.api;

import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.Version;
import org.apache.solr.client.api.model.UpgradeCoreIndexRequestBody;
import org.apache.solr.client.api.model.UpgradeCoreIndexResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.index.LatestVersionMergePolicy;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocValuesIteratorCache;
import org.apache.solr.search.SolrDocumentFetcher;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the UPGRADECOREINDEX CoreAdmin action, which upgrades an existing core's index
 * in-place by reindexing documents from segments created by older Lucene versions.
 *
 * <p>This action is intended for user-managed or standalone installations when upgrading Solr
 * across major versions.
 *
 * <p>The upgrade process:
 *
 * <ol>
 *   <li>Temporarily installs {@link LatestVersionMergePolicy} to prevent older-version segments
 *       from participating in merges during reindexing.
 *   <li>Iterates each segment whose {@code minVersion} is older than the current Lucene major
 *       version. For each live document, rebuilds a {@link SolrInputDocument} from stored fields,
 *       decorates it with non-stored DocValues fields (excluding copyField targets), and re-adds it
 *       through Solr's update pipeline.
 *   <li>Commits the changes and validates that no older-format segments remain.
 *   <li>Restores the original merge policy.
 * </ol>
 *
 * <p><strong>Important:</strong> Only fields that are stored or have DocValues enabled can be
 * preserved during upgrade. Fields that are neither stored, nor docValues-enabled or copyField
 * targets, will lose their data.
 *
 * @see LatestVersionMergePolicy
 * @see UpgradeCoreIndexRequestBody
 * @see UpgradeCoreIndexResponse
 */
public class UpgradeCoreIndex extends CoreAdminAPIBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public enum CoreIndexUpgradeStatus {
    UPGRADE_SUCCESSFUL,
    ERROR,
    NO_UPGRADE_NEEDED;
  }

  private static final int RETRY_COUNT_FOR_SEGMENT_DELETION = 5;

  public UpgradeCoreIndex(
      CoreContainer coreContainer,
      CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker,
      SolrQueryRequest req,
      SolrQueryResponse rsp) {
    super(coreContainer, coreAdminAsyncTracker, req, rsp);
  }

  @Override
  public boolean isExpensive() {
    return true;
  }

  public UpgradeCoreIndexResponse upgradeCoreIndex(
      String coreName, UpgradeCoreIndexRequestBody requestBody) throws Exception {
    ensureRequiredParameterProvided("coreName", coreName);

    final UpgradeCoreIndexResponse response =
        instantiateJerseyResponse(UpgradeCoreIndexResponse.class);

    return handlePotentiallyAsynchronousTask(
        response,
        coreName,
        requestBody.async,
        "upgrade-index",
        () -> performUpgrade(coreName, requestBody, response));
  }

  private UpgradeCoreIndexResponse performUpgrade(
      String coreName, UpgradeCoreIndexRequestBody requestBody, UpgradeCoreIndexResponse response) {
    {
      try (SolrCore core = coreContainer.getCore(coreName)) {

        // Set LatestVersionMergePolicy to prevent older segments from
        // participating in merges while we reindex. This is to prevent any older version
        // segments from
        // merging with any newly formed segments created due to reindexing and undoing the work
        // we are doing.
        RefCounted<IndexWriter> iwRef = null;
        MergePolicy originalMergePolicy = null;
        int numSegmentsEligibleForUpgrade = 0, numSegmentsUpgraded = 0;
        try {
          iwRef = core.getSolrCoreState().getIndexWriter(core);
          IndexWriter iw = iwRef.get();

          originalMergePolicy = iw.getConfig().getMergePolicy();
          iw.getConfig()
              .setMergePolicy(new LatestVersionMergePolicy(iw.getConfig().getMergePolicy()));

          RefCounted<SolrIndexSearcher> searcherRef = core.getSearcher();
          try {
            List<LeafReaderContext> leafContexts = searcherRef.get().getTopReaderContext().leaves();
            DocValuesIteratorCache dvICache = new DocValuesIteratorCache(searcherRef.get());

            UpdateRequestProcessorChain updateProcessorChain =
                getUpdateProcessorChain(core, requestBody.updateChain);

            for (LeafReaderContext lrc : leafContexts) {
              if (!shouldUpgradeSegment(lrc)) {
                continue;
              }
              numSegmentsEligibleForUpgrade++;
              processSegment(lrc, updateProcessorChain, core, searcherRef.get(), dvICache);
              numSegmentsUpgraded++;
            }

            if (numSegmentsEligibleForUpgrade == 0) {
              response.core = coreName;
              response.upgradeStatus = CoreIndexUpgradeStatus.NO_UPGRADE_NEEDED.toString();
              response.numSegmentsEligibleForUpgrade = 0;
              return response;
            }
          } catch (Exception e) {
            log.error("Error while processing core: [{}}]", coreName, e);
            throw new CoreAdminAPIBaseException(e);
          } finally {
            // important to decrement searcher ref count after use since we obtained it via the
            // SolrCore.getSearcher() method
            searcherRef.decref();
          }

          try {
            doCommit(core);
          } catch (IOException e) {
            throw new CoreAdminAPIBaseException(e);
          }

          boolean indexUpgraded = isIndexUpgraded(core);

          if (!indexUpgraded) {
            log.error(
                "Validation failed for core '{}'. Some data is still present in the older (<{}.x) Lucene index format.",
                coreName,
                Version.LATEST.major);
            throw new CoreAdminAPIBaseException(
                new SolrException(
                    SolrException.ErrorCode.SERVER_ERROR,
                    "Validation failed for core '"
                        + coreName
                        + "'. Some data is still present in the older (<"
                        + Version.LATEST.major
                        + ".x) Lucene index format."));
          }

          response.core = coreName;
          response.upgradeStatus = CoreIndexUpgradeStatus.UPGRADE_SUCCESSFUL.toString();
          response.numSegmentsEligibleForUpgrade = numSegmentsEligibleForUpgrade;
          response.numSegmentsUpgraded = numSegmentsUpgraded;
        } catch (Exception ioEx) {
          throw new CoreAdminAPIBaseException(ioEx);

        } finally {
          // Restore original merge policy
          if (iwRef != null) {
            IndexWriter iw = iwRef.get();
            if (originalMergePolicy != null) {
              iw.getConfig().setMergePolicy(originalMergePolicy);
            }
            iwRef.decref();
          }
        }
      }

      return response;
    }
  }

  private boolean shouldUpgradeSegment(LeafReaderContext lrc) {
    Version segmentMinVersion = null;

    LeafReader leafReader = lrc.reader();
    leafReader = FilterLeafReader.unwrap(leafReader);

    SegmentCommitInfo si = ((SegmentReader) leafReader).getSegmentInfo();
    segmentMinVersion = si.info.getMinVersion();

    return (segmentMinVersion == null || segmentMinVersion.major < Version.LATEST.major);
  }

  @SuppressWarnings({"rawtypes"})
  private UpdateRequestProcessorChain getUpdateProcessorChain(
      SolrCore core, String requestedUpdateChain) {

    // Try explicitly requested chain first
    if (requestedUpdateChain != null) {
      UpdateRequestProcessorChain resolvedChain =
          core.getUpdateProcessingChain(requestedUpdateChain);
      if (resolvedChain != null) {
        return resolvedChain;
      }
      throw new SolrException(
          BAD_REQUEST,
          "Requested update chain '"
              + requestedUpdateChain
              + "' not found for core "
              + core.getName());
    }

    // Try to find chain configured in /update handler
    String updateChainName = null;
    SolrRequestHandler reqHandler = core.getRequestHandler("/update");

    NamedList initArgs = ((RequestHandlerBase) reqHandler).getInitArgs();

    if (initArgs != null) {
      // Check invariants first
      Object invariants = initArgs.get("invariants");
      if (invariants instanceof NamedList) {
        updateChainName = (String) ((NamedList) invariants).get(UpdateParams.UPDATE_CHAIN);
      }

      // Check defaults if not found in invariants
      if (updateChainName == null) {
        Object defaults = initArgs.get("defaults");
        if (defaults instanceof NamedList) {
          updateChainName = (String) ((NamedList) defaults).get(UpdateParams.UPDATE_CHAIN);
        }
      }
    }

    // default chain is returned if updateChainName is null
    return core.getUpdateProcessingChain(updateChainName);
  }

  private boolean isIndexUpgraded(SolrCore core) throws IOException {

    try (FSDirectory dir = FSDirectory.open(Path.of(core.getIndexDir()));
        IndexReader reader = DirectoryReader.open(dir)) {

      List<LeafReaderContext> leaves = reader.leaves();
      if (leaves == null || leaves.isEmpty()) {
        // no segments to process/validate
        return true;
      }

      for (LeafReaderContext lrc : leaves) {
        LeafReader leafReader = lrc.reader();
        leafReader = FilterLeafReader.unwrap(leafReader);
        if (leafReader instanceof SegmentReader) {
          SegmentReader segmentReader = (SegmentReader) leafReader;
          SegmentCommitInfo si = segmentReader.getSegmentInfo();
          Version segMinVersion = si.info.getMinVersion();
          if (segMinVersion == null || segMinVersion.major != Version.LATEST.major) {
            log.warn(
                "isIndexUpgraded(): Core: {}, Segment [{}] is still at minVersion [{}] and is not updated to the latest version [{}]; numLiveDocs: [{}]",
                core.getName(),
                si.info.name,
                (segMinVersion == null ? 6 : segMinVersion.major),
                Version.LATEST.major,
                segmentReader.numDocs());
            return false;
          }
        }
      }
      return true;
    } catch (Exception e) {
      log.error("Error while opening segmentInfos for core [{}]", core.getName(), e);
      throw e;
    }
  }

  private void doCommit(SolrCore core) throws IOException {
    try (LocalSolrQueryRequest req = new LocalSolrQueryRequest(core, new ModifiableSolrParams())) {
      CommitUpdateCommand cmd = new CommitUpdateCommand(req, false);
      core.getUpdateHandler().commit(cmd);
    } catch (IOException ioEx) {
      log.warn("Error committing on core [{}] during index upgrade", core.getName(), ioEx);
      throw ioEx;
    }
  }

  private void processSegment(
      LeafReaderContext leafReaderContext,
      UpdateRequestProcessorChain processorChain,
      SolrCore core,
      SolrIndexSearcher solrIndexSearcher,
      DocValuesIteratorCache dvICache)
      throws Exception {

    Exception exceptionToThrow = null;
    int numDocsProcessed = 0;

    String coreName = core.getName();
    IndexSchema indexSchema = core.getLatestSchema();

    LeafReader leafReader = FilterLeafReader.unwrap(leafReaderContext.reader());
    SegmentReader segmentReader = (SegmentReader) leafReader;
    final String segmentName = segmentReader.getSegmentName();
    Bits bits = segmentReader.getLiveDocs();
    SolrInputDocument solrDoc = null;
    UpdateRequestProcessor processor = null;
    LocalSolrQueryRequest solrRequest = null;
    SolrDocumentFetcher docFetcher = solrIndexSearcher.getDocFetcher();
    try {
      // Exclude copy field targets to avoid duplicating values on reindex
      Set<String> nonStoredDVFields = docFetcher.getNonStoredDVsWithoutCopyTargets();
      solrRequest = new LocalSolrQueryRequest(core, new ModifiableSolrParams());

      SolrQueryResponse rsp = new SolrQueryResponse();
      processor = processorChain.createProcessor(solrRequest, rsp);
      StoredFields storedFields = segmentReader.storedFields();
      for (int luceneDocId = 0; luceneDocId < segmentReader.maxDoc(); luceneDocId++) {
        if (bits != null && !bits.get(luceneDocId)) {
          continue;
        }

        Document doc = storedFields.document(luceneDocId);
        solrDoc = toSolrInputDocument(doc, indexSchema);

        docFetcher.decorateDocValueFields(
            solrDoc, leafReaderContext.docBase + luceneDocId, nonStoredDVFields, dvICache);
        solrDoc.removeField("_version_");
        AddUpdateCommand currDocCmd = new AddUpdateCommand(solrRequest);
        currDocCmd.solrDoc = solrDoc;
        processor.processAdd(currDocCmd);
        numDocsProcessed++;
      }
    } catch (Exception e) {
      log.error("Error while processing segment [{}] in core [{}]", segmentName, coreName, e);
      exceptionToThrow = e;
    } finally {
      if (processor != null) {
        try {
          processor.finish();
        } catch (Exception e) {
          log.error(
              "Exception during processor.finish() for segment [{}] in core [{}]",
              segmentName,
              coreName,
              e);
          if (exceptionToThrow == null) {
            exceptionToThrow = e;
          } else {
            exceptionToThrow.addSuppressed(e);
          }
        }
        try {
          processor.close();
        } catch (Exception e) {
          log.error(
              "Exception while closing update processor for segment [{}] in core [{}]",
              segmentName,
              coreName,
              e);
          if (exceptionToThrow == null) {
            exceptionToThrow = e;
          } else {
            exceptionToThrow.addSuppressed(e);
          }
        }
      }
      if (solrRequest != null) {
        try {
          solrRequest.close();
        } catch (Exception e) {
          if (exceptionToThrow == null) {
            exceptionToThrow = e;
          } else {
            exceptionToThrow.addSuppressed(e);
          }
        }
      }
    }

    log.info(
        "End processing segment : {}, core: {} docs processed: {}",
        segmentName,
        coreName,
        numDocsProcessed);

    if (exceptionToThrow != null) {
      throw exceptionToThrow;
    }
  }

  /*
   * Convert a lucene Document to a SolrInputDocument
   */
  protected SolrInputDocument toSolrInputDocument(
      org.apache.lucene.document.Document doc, IndexSchema schema) {
    SolrInputDocument out = new SolrInputDocument();
    for (IndexableField f : doc.getFields()) {
      String fname = f.name();
      SchemaField sf = schema.getFieldOrNull(f.name());
      Object val = null;
      if (sf != null) {
        if ((!sf.hasDocValues() && !sf.stored()) || schema.isCopyFieldTarget(sf)) {
          continue;
        }
        val = sf.getType().toObject(f);
      } else {
        val = f.stringValue();
        if (val == null) {
          val = f.numericValue();
        }
        if (val == null) {
          val = f.binaryValue();
        }
        if (val == null) {
          val = f;
        }
      }
      out.addField(fname, val);
    }
    return out;
  }
}
