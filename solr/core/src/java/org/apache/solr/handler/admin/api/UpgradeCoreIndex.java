package org.apache.solr.handler.admin.api;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Paths;
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
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.api.model.UpgradeCoreIndexRequestBody;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.index.LatestVersionFilterMergePolicy;
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
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpgradeCoreIndex extends CoreAdminAPIBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /*
   * The re-indexing status at any point of time for a particular core.
   * DEFAULT - This is the default status, meaning it is yet to be processed and checked if the version is LATEST
   * for this core
   * REINDEXING_ACTIVE - This is set at the start of the re-indexing operation
   * PROCESSED - This is set at the end of the re-indexing operation if there are no errors
   * ERROR - This is set if there is any error in any segment. This core will be retried CORE_ERROR_RETRIES number
   * of
   * times
   * CORRECTVERSION - This is set if the core is already at the correct version
   */
  public enum CoreReindexingStatus {
    DEFAULT,
    REINDEXING_ACTIVE,
    REINDEXING_PAUSED,
    PROCESSED,
    ERROR,
    CORRECTVERSION;
  }

  /*
   * The state that a single ReindexingThread would be in. This is set to START_REINDEXING by CPUMonitorTask
   * thread
   * START_REINDEXING - CPUMonitorTask checks if current CPU usage is below given threshold and sets this state
   * WAITING - CPUMonitorTask checks if the CPU usage is above given threshold and sets this state to put
   * the CVReindexingTask thread in a waiting state. Note that ReindexingTask thread run() will still be checked
   * in this case. This can also be set when all cores have processed and there are no pending cores.
   * STOP_REINDEXING - CPUMonitorTask checks if all cores are processed with the status between {CORRECTVERSION,
   * ERROR}
   * and if all cores are processed then sets it to STOP_REINDEXING
   *
   */
  public enum ReindexingThreadState {
    START_REINDEXING,
    STOP_REINDEXING,
    WAITING;
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

  public SolrJerseyResponse upgradeCoreIndex(
      String coreName, UpgradeCoreIndexRequestBody requestBody) throws Exception {
    ensureRequiredParameterProvided("coreName", coreName);
    SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);

    return handlePotentiallyAsynchronousTask(
        response,
        coreName,
        requestBody.async,
        "upgrade-index",
        () -> {
          try (SolrCore core = coreContainer.getCore(coreName)) {

            log.info("Received UPGRADECOREINDEX request for core: {}", core.getName());
            CoreReindexingStatus coreRxStatus = CoreReindexingStatus.REINDEXING_ACTIVE;

            // Set LatestVersionFilterMergePolicy to prevent older segments from
            // participating in merges while we reindex.
            // This must be done inside the async lambda to ensure it stays in effect
            // for the duration of the reindexing operation.
            RefCounted<IndexWriter> iwRef = null;
            MergePolicy originalMergePolicy = null;
            try {
              iwRef = core.getSolrCoreState().getIndexWriter(core);
              if (iwRef != null) {
                IndexWriter iw = iwRef.get();
                if (iw != null) {
                  originalMergePolicy = iw.getConfig().getMergePolicy();
                  iw.getConfig()
                      .setMergePolicy(
                          new LatestVersionFilterMergePolicy(iw.getConfig().getMergePolicy()));
                }
              }

              RefCounted<SolrIndexSearcher> ssearcherRef = core.getSearcher();
              try {
                List<LeafReaderContext> leafContexts =
                    ssearcherRef.get().getTopReaderContext().leaves();
                DocValuesIteratorCache dvICache = new DocValuesIteratorCache(ssearcherRef.get());

                UpdateRequestProcessorChain updateProcessorChain =
                    getUpdateProcessorChain(core, requestBody.updateChain);

                for (LeafReaderContext lrc : leafContexts) {
                  if (!shouldUpgradeSegment(lrc)) {
                    continue;
                  }

                  boolean success =
                      processSegment(lrc, updateProcessorChain, core, ssearcherRef.get(), dvICache);

                  if (!success) {
                    coreRxStatus = CoreReindexingStatus.ERROR;
                    break;
                  }
                }
              } catch (Exception e) {
                log.error("Error while processing core: {}", coreName, e);
                coreRxStatus = CoreReindexingStatus.ERROR;
              } finally {
                // important to decrement searcher ref count after use since we obtained it via the
                // SolrCore.getSearcher() method
                ssearcherRef.decref();
              }

              // TO-DO
              // Prepare SolrjerseyResponse if coreRxStatus==ERROR at this point

              doCommit(core);
              try {
                // giving some time for 0 doc segments to clear up
                Thread.sleep(10000);
              } catch (InterruptedException ie) {
                /* We don't need to preserve the interrupt here.
                Otherwise, we may get immediately interrupted when we try to call sleep() during validation in the next steps.
                And we are almost done anyway.
                 */
              }

              boolean indexUpgraded = isIndexUpgraded(core);

              /*
              There is a delay observed sometimes between when a commit happens and
              when the segment (with zero live docs) gets cleared. So adding a validation check with retries.
              */
              for (int i = 0; i < RETRY_COUNT_FOR_SEGMENT_DELETION && !indexUpgraded; i++) {
                try {
                  doCommit(core);
                  Thread.sleep(10000);
                  indexUpgraded = isIndexUpgraded(core);

                } catch (InterruptedException ie) {
                  Thread.currentThread().interrupt();
                }
                if (Thread.currentThread().isInterrupted()) {
                  break;
                }
              }

              if (!indexUpgraded) {
                log.error(
                    "Validation failed for core '{}'. Some older version segments still remain (likely despite 100% deleted docs).",
                    coreName);
                coreRxStatus = CoreReindexingStatus.ERROR;
              }
            } catch (IOException ioEx) {
              // TO-DO
              // Throw exception wrapped in CoreAdminAPIBaseException
            } finally {
              // Restore original merge policy
              if (iwRef != null) {
                IndexWriter iw = iwRef.get();
                if (iw != null && originalMergePolicy != null) {
                  iw.getConfig().setMergePolicy(originalMergePolicy);
                }
                iwRef.decref();
              }
            }
          }
          return response;
        });
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
      } else {
        log.error(
            "UPGRADECOREINDEX:: Requested update chain {} not found for core {}",
            requestedUpdateChain,
            core.getName());
        // TO-DO
        // Throw exception wrapped in CoreAdminAPIBaseException
      }
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

  private boolean isIndexUpgraded(SolrCore core) {

    try (FSDirectory dir = FSDirectory.open(Paths.get(core.getIndexDir()));
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
      log.error(
          "Error while opening segmentInfos for core: {}, exception: {}",
          core.getName(),
          e.toString());
    }
    return false;
  }

  private void doCommit(SolrCore core) {
    RefCounted<IndexWriter> iwRef = null;
    try {
      iwRef = core.getSolrCoreState().getIndexWriter(null);
      if (iwRef != null) {
        IndexWriter iw = iwRef.get();

        if (iw != null) {
          iw.commit();
        } else {
          log.warn("IndexWriter for core {} is null", core.getName());
        }
      } else {
        log.warn("IWRef for core {} is null", core.getName());
      }
    } catch (IOException ioEx) {
      log.warn(
          String.format("Error commiting on core {} during index upgrade", core.getName()), ioEx);
    } finally {
      if (iwRef != null) {
        iwRef.decref();
      }
    }
  }

  private boolean processSegment(
      LeafReaderContext leafReaderContext,
      UpdateRequestProcessorChain processorChain,
      SolrCore core,
      SolrIndexSearcher solrIndexSearcher,
      DocValuesIteratorCache dvICache) {

    boolean success = false;
    int numDocsProcessed = 0;

    String coreName = core.getName();
    IndexSchema indexSchema = core.getLatestSchema();

    LeafReader leafReader = FilterLeafReader.unwrap(leafReaderContext.reader());
    SegmentReader segmentReader = (SegmentReader) leafReader;
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
      success = true;
    } catch (Exception e) {
      log.error("Error in CvReindexingTask process() : {}", e.toString());

    } finally {
      if (processor != null) {
        try {
          processor.finish();
        } catch (Exception e) {
          log.error("Exception while doing finish processor.finish() : {}", e.toString());

        } finally {
          try {
            processor.close();
          } catch (IOException e) {
            log.error("Exception while closing processor: {}", e.toString());
          }
        }
      }
      if (solrRequest != null) {
        solrRequest.close();
      }
    }

    log.info(
        "End processing segment : {}, core: {} docs processed: {}",
        segmentReader.getSegmentName(),
        coreName,
        numDocsProcessed);

    return success;
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
