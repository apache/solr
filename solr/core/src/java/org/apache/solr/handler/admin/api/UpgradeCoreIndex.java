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
import java.util.List;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.Directory;
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
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.index.LatestVersionMergePolicy;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.DocValuesIteratorCache;
import org.apache.solr.search.SolrDocumentFetcher;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DocumentBuilder;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the UPGRADECOREINDEX CoreAdmin action, which upgrades an existing core's index
 * in-place by reindexing documents from segments belonging to older Lucene versions, so that they
 * get written into latest version segments.
 *
 * <p>The upgrade process:
 *
 * <ul>
 *   <li>Temporarily installs {@link LatestVersionMergePolicy} to prevent older-version segments
 *       from participating in merges during reindexing.
 *   <li>Iterates each segment whose {@code minVersion} is older than the current Lucene major
 *       version. For each live document, rebuilds a {@link SolrInputDocument} from stored fields,
 *       decorates it with non-stored DocValues fields (excluding copyField targets), and re-adds it
 *       through Solr's update pipeline.
 *   <li>Commits the changes and validates that no older-format segments remain.
 *   <li>Restores the original merge policy.
 * </ul>
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

    try (SolrCore core = coreContainer.getCore(coreName)) {
      return performUpgradeImpl(core, requestBody, response);
    }
  }

  private UpgradeCoreIndexResponse performUpgradeImpl(
      SolrCore core, UpgradeCoreIndexRequestBody requestBody, UpgradeCoreIndexResponse response) {

    RefCounted<IndexWriter> iwRef = null;
    MergePolicy originalMergePolicy = null;
    int numSegmentsEligibleForUpgrade = 0, numSegmentsUpgraded = 0;
    String coreName = core.getName();
    try {
      iwRef = core.getSolrCoreState().getIndexWriter(core);
      IndexWriter iw = iwRef.get();

      RefCounted<SolrIndexSearcher> searcherRef = core.getSearcher();
      try {
        // Check for nested documents before processing - we don't support them
        if (indexContainsNestedDocs(searcherRef.get())) {
          throw new SolrException(
              BAD_REQUEST,
              "UPGRADECOREINDEX does not support indexes containing nested documents. "
                  + " Consider reindexing your data "
                  + "from the original source.");
        }

        /* Set LatestVersionMergePolicy to prevent older segments from
        participating in merges while we reindex. This is to prevent any older version
        segments from
        merging with any newly formed segments created due to reindexing and undoing the work
        we are doing. */
        originalMergePolicy = iw.getConfig().getMergePolicy();
        iw.getConfig()
            .setMergePolicy(
                new LatestVersionMergePolicy(
                    iw.getConfig().getMergePolicy())); // prevent older segments from merging

        List<LeafReaderContext> leafContexts = searcherRef.get().getIndexReader().leaves();
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
        // important to decrement searcher ref count after use since we obtained it via
        // SolrCore.getSearcher()
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
      // Avoid double-wrapping if already a CoreAdminAPIBaseException
      if (ioEx instanceof CoreAdminAPIBaseException) {
        throw (CoreAdminAPIBaseException) ioEx;
      }
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

    return response;
  }

  private boolean shouldUpgradeSegment(LeafReaderContext lrc) {
    Version segmentMinVersion = null;

    LeafReader leafReader = lrc.reader();
    leafReader = FilterLeafReader.unwrap(leafReader);

    SegmentCommitInfo si = ((SegmentReader) leafReader).getSegmentInfo();
    segmentMinVersion = si.info.getMinVersion();

    return (segmentMinVersion == null || segmentMinVersion.major < Version.LATEST.major);
  }

  private boolean indexContainsNestedDocs(SolrIndexSearcher searcher) throws IOException {
    IndexSchema schema = searcher.getSchema();

    // First check if schema supports nested docs
    if (!schema.isUsableForChildDocs()) {
      return false;
    }

    // Check if _root_ field has fewer unique values than documents with that field.
    // This indicates multiple docs share the same _root_ (i.e., child docs exist)
    IndexReader reader = searcher.getIndexReader();
    for (LeafReaderContext leaf : reader.leaves()) {
      Terms terms = leaf.reader().terms(IndexSchema.ROOT_FIELD_NAME);
      if (terms != null) {
        long uniqueRootValues = terms.size();
        int docsWithRoot = terms.getDocCount();

        if (uniqueRootValues == -1 || uniqueRootValues < docsWithRoot) {
          return true; // Codec doesn't store number of terms (so a safe fallback), or multiple docs
          // share same _root_ (aka nested docs exist)
        }
      }
    }
    return false;
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

    Directory dir =
        core.getDirectoryFactory()
            .get(
                core.getIndexDir(),
                DirectoryFactory.DirContext.DEFAULT,
                core.getSolrConfig().indexConfig.lockType);

    try (IndexReader reader = DirectoryReader.open(dir)) {
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
    } finally {
      if (dir != null) {
        core.getDirectoryFactory().release(dir);
      }
    }
  }

  private void doCommit(SolrCore core) throws IOException {
    try (LocalSolrQueryRequest req = new LocalSolrQueryRequest(core, new ModifiableSolrParams())) {
      CommitUpdateCommand cmd = new CommitUpdateCommand(req, false); // optimize=false
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

    String coreName = core.getName();
    IndexSchema indexSchema = core.getLatestSchema();

    LeafReader leafReader = leafReaderContext.reader();
    Bits liveDocs = leafReader.getLiveDocs();
    SolrDocumentFetcher docFetcher = solrIndexSearcher.getDocFetcher();

    // Exclude copy field targets to avoid duplicating values on reindex
    Set<String> nonStoredDVFields = docFetcher.getNonStoredDVsWithoutCopyTargets();

    try (LocalSolrQueryRequest solrRequest =
        new LocalSolrQueryRequest(core, new ModifiableSolrParams())) {
      SolrQueryResponse rsp = new SolrQueryResponse();
      UpdateRequestProcessor processor = processorChain.createProcessor(solrRequest, rsp);
      try {
        StoredFields storedFields = leafReader.storedFields();
        for (int luceneDocId = 0; luceneDocId < leafReader.maxDoc(); luceneDocId++) {
          if (liveDocs != null && !liveDocs.get(luceneDocId)) {
            continue;
          }
          Document doc = storedFields.document(luceneDocId);
          SolrInputDocument solrDoc = DocumentBuilder.toSolrInputDocument(doc, indexSchema);

          docFetcher.decorateDocValueFields(
              solrDoc, leafReaderContext.docBase + luceneDocId, nonStoredDVFields, dvICache);

          AddUpdateCommand currDocCmd = new AddUpdateCommand(solrRequest);
          currDocCmd.solrDoc = solrDoc;
          processor.processAdd(currDocCmd);
        }
      } finally {
        // finish() must be called before close() to flush pending operations
        processor.finish();
        processor.close();
      }
    }
  }
}
